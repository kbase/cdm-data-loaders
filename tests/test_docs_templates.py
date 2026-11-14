"""Tests for ``docs_macros``, Jinja2 documentation templates, and drift verification across ``docs/`` and ``README.md``."""

import ast
import functools
import importlib
import re
import sys
import textwrap
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

import jinja2
import pytest
import yaml
from pydantic import AliasChoices
from zensical.extensions.macros import MacroEnv, _load_module

from cdm_data_loaders.coercion.pyarrow import coerce_record_for_pyarrow
from cdm_data_loaders.core.settings import CdmDataLoadersBase
from cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline import (
    run_jsonlines_with_validation_pipeline,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from docs_macros import define_env
from docs_macros.__main__ import main as docs_macros_cli_main
from docs_macros.cli_context import (
    CliEntryDoc,
    discover_api_modules,
    load_cli_entries,
)
from docs_macros.constants import (
    DOCS_DIR,
    GITHUB_BLOB_BASE,
    REPO_ROOT,
    SCHEMAS_SUBDIR,
    SITE_URL_BASE,
    SRC_ROOT,
    TEMPLATES_DIR,
)
from docs_macros.env import (
    GENERATED_MACRO_STUB_RE,
    DocsGenerationPaths,
    build_generated_docs_plan,
    clean_generated_docs,
    create_jinja_env,
    discover_spec_names,
    render_bundled_schemas,
    render_cli_reference,
    render_converter_spec,
    render_macro_stub,
    render_pipeline_spec,
    render_reference_table,
    sync_generated_docs,
)
from docs_macros.formatting import (
    CodeBlockContext,
    code_fence_for_text,
    escape_table_cell,
    module_api_url,
    module_blob_url,
    repo_blob_url,
    slugify_anchor,
)
from docs_macros.schemas_context import (
    discover_schema_assets,
    is_published_jsonschema,
    resolve_schema_file_metadata,
)
from docs_macros.spec_context import (
    CONVERTER_SPECS,
    PIPELINE_SPECS,
    ErrorFixtureRefContext,
    ErrorRowContext,
    ErrorSectionContext,
    build_converter_spec_context,
    build_pipeline_spec_context,
    discover_converter_specs,
)
from docs_macros.tables_context import TABLE_TEMPLATE_REGISTRY
from scripts.gen_pyarrow_schema import check_or_write_files
from tests.cdm_data_loaders.pipelines.reference.pipeline import ReferencePipelineSettings

pytestmark = pytest.mark.docs

MIN_EXPECTED_CLIS: Final[int] = 16
EXPECTED_TEMPLATE_COUNT: Final[int] = 15
EXPECTED_MACRO_STUB_COUNT: Final[int] = 13
MIN_MARKDOWN_TABLE_LINES: Final[int] = 3
ZENSICAL_PATH: Final[Path] = REPO_ROOT / "zensical.toml"
INDEX_PATH: Final[Path] = DOCS_DIR / "index.md"
README_PATH: Final[Path] = REPO_ROOT / "README.md"
FRONT_MATTER_RE: Final[re.Pattern[str]] = re.compile(r"\A---\n(.*?)\n---\n?(.*)\Z", re.DOTALL)
FENCED_BLOCK_RE: Final[re.Pattern[str]] = re.compile(r"(`{3,}).*?\1", re.DOTALL)
PYTHON_FENCE_RE: Final[re.Pattern[str]] = re.compile(r"(`{3,})(?:python|py)(?:[ \t][^\n]*)?\n(.*?)\1", re.DOTALL)
SHELL_FENCE_RE: Final[re.Pattern[str]] = re.compile(
    r"(`{3,})(?:bash|sh|shell|console)(?:[ \t][^\n]*)?\n(.*?)\1",
    re.DOTALL,
)
CDL_ENV_VAR_RE: Final[re.Pattern[str]] = re.compile(r"\bCDL_[A-Z0-9_]+\b")
INDEX_MD_LINK_RE: Final[re.Pattern[str]] = re.compile(r"\]\(([a-zA-Z0-9_-]+\.md)(?:#[^)]*)?\)")
INDEX_H2_HEADING_RE: Final[re.Pattern[str]] = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)
README_DOCS_LINK_RE: Final[re.Pattern[str]] = re.compile(r"\]\(docs/([a-zA-Z0-9_-]+\.md)(?:#[^)]*)?\)")
README_SITE_URL_RE: Final[re.Pattern[str]] = re.compile(
    r"\]\((https://kbase\.github\.io/cdm-data-loaders/(?:#[a-zA-Z0-9_-]+)?)\)"
)
MERMAID_FENCE_RE: Final[re.Pattern[str]] = re.compile(r"(`{3,})mermaid(?:[ \t][^\n]*)?\n(.*?)\1", re.DOTALL)
CLASS_DIAGRAM_RE: Final[re.Pattern[str]] = re.compile(r"^\s*classDiagram\b", re.MULTILINE)
HIERARCHY_CHAIN_RE: Final[re.Pattern[str]] = re.compile(r"\*\*Settings hierarchy\*\*:|\*\*MRO Chain:\*\*")
GITHUB_BLOB_LINK_RE: Final[re.Pattern[str]] = re.compile(
    r"\]\(https://github\.com/kbase/cdm-data-loaders/blob/([^/]+)/([^)#\s]+)(?:#[^)]*)?\)"
)
SHELL_STOP_TOKENS: Final[frozenset[str]] = frozenset({"|", "||", "&&", ";", ">", ">>", "<"})
EXPECTED_NAV_SECTIONS: Final[tuple[str, ...]] = (
    "Home",
    "User Guides",
    "Specifications",
    "Developer Guides",
    "API and Schema Reference",
    "About These Docs",
)
EXPECTED_PRIMARY_COLORS: Final[tuple[str, ...]] = (
    "red",
    "pink",
    "purple",
    "deep-purple",
    "indigo",
    "blue",
    "light-blue",
    "cyan",
    "teal",
    "green",
    "light-green",
    "lime",
    "yellow",
    "amber",
    "orange",
    "deep-orange",
    "brown",
    "grey",
    "blue-grey",
)
EXPECTED_ACCENT_COLORS: Final[tuple[str, ...]] = (
    "red",
    "pink",
    "purple",
    "deep-purple",
    "indigo",
    "blue",
    "light-blue",
    "cyan",
    "teal",
    "green",
    "light-green",
    "lime",
    "yellow",
    "amber",
    "orange",
    "deep-orange",
)
SWATCH_BUTTON_RE: Final[re.Pattern[str]] = re.compile(r'data-md-color-(primary|accent)="([a-z-]+)"')
CSS_COLOR_LITERAL_RE: Final[re.Pattern[str]] = re.compile(r"(?<!&)#[0-9a-fA-F]{3,8}\b|\b(?:rgb|hsl)a?\(")
EXPECTED_README_SITE_URLS: Final[tuple[str, ...]] = (
    f"{SITE_URL_BASE}/",
    f"{SITE_URL_BASE}/#user-guides",
    f"{SITE_URL_BASE}/#specifications",
    f"{SITE_URL_BASE}/#developer-guides",
    f"{SITE_URL_BASE}/#api-and-schema-reference",
)


def _excluded_doc_names(zensical_path: Path = ZENSICAL_PATH) -> set[str]:
    """Return the set of excluded entries listed under ``[project].exclude_docs`` in ``zensical.toml``."""
    if not zensical_path.exists():
        return set()
    zensical_data = tomllib.loads(zensical_path.read_text(encoding="utf-8"))
    project_data = zensical_data.get("project", {})
    raw_exclude = project_data.get("exclude_docs", "") or ""
    return {line.strip().lstrip("/").rstrip("/") for line in str(raw_exclude).splitlines() if line.strip()}


@functools.cache
def _macro_jinja_env() -> jinja2.Environment:
    """Load ``docs_macros`` via Zensical's ``_load_module`` into a strict Jinja2 macro environment."""
    macro_env = MacroEnv(conf={"root_dir": str(REPO_ROOT), "docs_dir": "docs"})
    _load_module(macro_env, "docs_macros", REPO_ROOT)
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=jinja2.StrictUndefined,
        autoescape=jinja2.select_autoescape(enabled_extensions=(), default_for_string=False, default=False),
    )
    env.globals.update(macro_env.variables)
    env.globals.update(macro_env.macros)
    env.filters.update(macro_env.filters)
    return env


def materialize_markdown_text(raw_text: str) -> str:
    """Expand Zensical ``render_macros: true`` front-matter Markdown text via ``docs_macros``."""
    fm_match = FRONT_MATTER_RE.match(raw_text)
    if fm_match is None:
        return raw_text
    meta = yaml.safe_load(fm_match.group(1)) or {}
    if not isinstance(meta, dict) or meta.get("render_macros") is not True:
        return raw_text
    body = fm_match.group(2)
    return _macro_jinja_env().from_string(body).render()


def materialize_markdown_file(md_path: Path) -> str:
    """Read ``md_path`` and expand Zensical ``render_macros: true`` front-matter pages via ``docs_macros``."""
    return materialize_markdown_text(md_path.read_text(encoding="utf-8"))


@functools.cache
def materialized_default_corpus() -> tuple[tuple[str, str], ...]:
    """Build and cache ``(label, rendered_markdown)`` pairs for ``README.md``, hand-written ``docs/*.md``, and generated stubs."""
    excluded = _excluded_doc_names()
    by_label: dict[str, str] = {"README.md": materialize_markdown_file(README_PATH)}
    for md_path in sorted(p for p in DOCS_DIR.glob("*.md") if p.name not in excluded):
        label = md_path.relative_to(REPO_ROOT).as_posix()
        by_label[label] = materialize_markdown_file(md_path)
    for rel_path, stub_text in build_generated_docs_plan().items():
        if rel_path.suffix == ".md" and rel_path.name not in excluded:
            by_label[f"docs/{rel_path.as_posix()}"] = materialize_markdown_text(stub_text)
    return tuple(by_label.items())


def _resolve_markdown_sources(
    md_paths: Sequence[Path] | None,
    repo_root: Path = REPO_ROOT,
) -> Sequence[tuple[str, str]]:
    """Return ``(label, markdown_text)`` pairs from explicit ``md_paths`` or the cached materialized corpus."""
    if md_paths is None:
        return materialized_default_corpus()
    pairs: list[tuple[str, str]] = []
    for md_path in md_paths:
        label = md_path.relative_to(repo_root).as_posix() if md_path.is_relative_to(repo_root) else md_path.name
        pairs.append((label, materialize_markdown_file(md_path)))
    return pairs


def _collect_nav_md_entries(nav_node: object) -> set[str]:
    """Collect ``.md`` file references from a parsed ``zensical.toml`` ``[project].nav`` structure."""
    found: set[str] = set()
    if isinstance(nav_node, str):
        if nav_node.endswith(".md"):
            found.add(nav_node)
    elif isinstance(nav_node, list):
        for item in nav_node:
            found.update(_collect_nav_md_entries(item))
    elif isinstance(nav_node, dict):
        for value in nav_node.values():
            found.update(_collect_nav_md_entries(value))
    return found


def _heading_slug(heading_text: str) -> str:
    """Convert a Markdown heading string into its lowercase hyphenated URL anchor slug.

    :param heading_text: Raw Markdown heading text without leading ``#`` markers.
    :type heading_text: str
    :return: Lowercase hyphen-delimited anchor slug.
    :rtype: str
    """
    return re.sub(r"[^a-z0-9]+", "-", heading_text.lower()).strip("-")


def check_zensical_nav_and_index(
    docs_dir: Path = DOCS_DIR,
    zensical_path: Path = ZENSICAL_PATH,
    index_path: Path = INDEX_PATH,
    readme_path: Path = README_PATH,
    *,
    include_generated: bool | None = None,
) -> list[str]:
    """Verify ``docs/*.md`` parity across ``zensical.toml`` and ``docs/index.md`` and website links in ``README.md``.

    :param docs_dir: Documentation directory containing Markdown pages.
    :type docs_dir: Path
    :param zensical_path: Path to ``zensical.toml``.
    :type zensical_path: Path
    :param index_path: Path to ``docs/index.md``.
    :type index_path: Path
    :param readme_path: Path to ``README.md``.
    :type readme_path: Path
    :param include_generated: Whether to include planned autogenerated Markdown stubs.
    :type include_generated: bool | None
    :return: List of validation error messages (empty when in sync).
    :rtype: list[str]
    """
    zensical_data = tomllib.loads(zensical_path.read_text(encoding="utf-8"))
    project_data = zensical_data.get("project", {})
    raw_exclude = project_data.get("exclude_docs", "") or ""
    excluded_names = {line.strip().lstrip("/").rstrip("/") for line in str(raw_exclude).splitlines() if line.strip()}

    docs_files = {p.name for p in docs_dir.glob("*.md") if p.name not in excluded_names}
    use_generated = (docs_dir == DOCS_DIR) if include_generated is None else include_generated
    if use_generated:
        docs_files |= {
            rel_path.name
            for rel_path in build_generated_docs_plan()
            if rel_path.suffix == ".md" and rel_path.name not in excluded_names
        }
    nav_entries = _collect_nav_md_entries(project_data.get("nav", []))

    non_index_docs = docs_files - {"index.md"}
    index_stripped = FENCED_BLOCK_RE.sub("", index_path.read_text(encoding="utf-8"))
    readme_stripped = FENCED_BLOCK_RE.sub("", readme_path.read_text(encoding="utf-8"))

    index_links = set(INDEX_MD_LINK_RE.findall(index_stripped))
    index_h2_slugs = {_heading_slug(heading) for heading in INDEX_H2_HEADING_RE.findall(index_stripped)}
    readme_rel_doc_links = sorted(set(README_DOCS_LINK_RE.findall(readme_stripped)))
    readme_site_urls = set(README_SITE_URL_RE.findall(readme_stripped))
    readme_unknown_anchors = sorted(
        url.split("#", maxsplit=1)[1]
        for url in readme_site_urls
        if "#" in url and url.split("#", maxsplit=1)[1] not in index_h2_slugs
    )

    return [
        *(f"docs/{name} is missing from zensical.toml nav" for name in sorted(docs_files - nav_entries)),
        *(f"zensical.toml nav entry '{name}' does not exist in docs/" for name in sorted(nav_entries - docs_files)),
        *(f"docs/{name} is not linked in docs/index.md" for name in sorted(non_index_docs - index_links)),
        *(f"docs/index.md links to non-existent 'docs/{name}'" for name in sorted(index_links - docs_files)),
        *(
            f"README.md is missing top-level docs website link '{url}'"
            for url in EXPECTED_README_SITE_URLS
            if url not in readme_site_urls
        ),
        *(
            f"README.md docs website anchor '#{anchor}' does not match any H2 heading in docs/index.md"
            for anchor in readme_unknown_anchors
        ),
        *(
            f"README.md links to relative 'docs/{name}' instead of the published docs website"
            for name in readme_rel_doc_links
        ),
    ]


def check_no_class_hierarchy_diagrams(
    md_paths: Sequence[Path] | None = None,
) -> list[str]:
    """Verify no Mermaid ``classDiagram`` blocks or settings/MRO inheritance chains appear in documentation.

    :param md_paths: Optional explicit Markdown paths to check (defaults to the full materialized corpus).
    :type md_paths: Sequence[Path] | None
    :return: List of violation messages (empty when compliant).
    :rtype: list[str]
    """
    errors: list[str] = []
    for label, raw_text in _resolve_markdown_sources(md_paths):
        if any(CLASS_DIAGRAM_RE.search(match.group(2)) for match in MERMAID_FENCE_RE.finditer(raw_text)):
            errors.append(f"{label}: contains forbidden Mermaid classDiagram block")
        stripped = FENCED_BLOCK_RE.sub("", raw_text)
        if HIERARCHY_CHAIN_RE.search(stripped) is not None:
            errors.append(f"{label}: contains forbidden class hierarchy or MRO chain listing")
    return errors


def check_github_repo_blob_links(
    md_paths: Sequence[Path] | None = None,
    repo_root: Path = REPO_ROOT,
) -> list[str]:
    """Verify every repository ``blob/develop`` link in hand-written and rendered docs resolves to a real file."""
    errors: list[str] = []
    for label, raw_text in _resolve_markdown_sources(md_paths, repo_root=repo_root):
        stripped = FENCED_BLOCK_RE.sub("", raw_text)
        for branch, rel_target in sorted(set(GITHUB_BLOB_LINK_RE.findall(stripped))):
            if branch != "develop":
                errors.append(f"{label}: GitHub blob link uses branch '{branch}' instead of 'develop' ('{rel_target}')")
            if not (repo_root / rel_target).exists():
                errors.append(f"{label}: GitHub blob link target '{rel_target}' does not exist in repository")
    return errors


def _discover_settings_classes() -> dict[str, type[CdmDataLoadersBase]]:
    """Import all package modules and return all ``CdmDataLoadersBase`` subclasses keyed by class name."""
    for mod_doc in discover_api_modules():
        importlib.import_module(mod_doc.module_path)

    found: dict[str, type[CdmDataLoadersBase]] = {}
    stack = list(CdmDataLoadersBase.__subclasses__())
    while stack:
        cls = stack.pop()
        found[cls.__name__] = cls
        stack.extend(cls.__subclasses__())
    return found


def _valid_settings_kwargs(settings_cls: type[CdmDataLoadersBase]) -> set[str]:
    """Return all valid keyword argument names (field names and string validation aliases) for ``settings_cls``."""
    valid: set[str] = set()
    for field_name, field_info in settings_cls.model_fields.items():
        valid.add(field_name)
        if isinstance(field_info.alias, str):
            valid.add(field_info.alias)
        if isinstance(field_info.validation_alias, str):
            valid.add(field_info.validation_alias)
        elif isinstance(field_info.validation_alias, AliasChoices):
            valid.update(choice for choice in field_info.validation_alias.choices if isinstance(choice, str))
    return valid


def _verify_import_from_node(node: ast.ImportFrom, label: str, block_line: int) -> list[str]:
    """Verify a single ``from cdm_data_loaders... import ...`` AST node resolves to real attributes."""
    if not node.module or not node.module.startswith("cdm_data_loaders"):
        return []
    line = block_line + node.lineno - 1
    try:
        mod = importlib.import_module(node.module)
    except Exception as exc:  # noqa: BLE001
        return [f"{label}:{line}: cannot import module '{node.module}' ({exc})"]

    errors: list[str] = []
    for alias in node.names:
        if hasattr(mod, alias.name):
            continue
        try:
            importlib.import_module(f"{node.module}.{alias.name}")
        except Exception:  # noqa: BLE001
            errors.append(f"{label}:{line}: cannot import '{alias.name}' from '{node.module}'")
    return errors


def check_markdown_python_blocks(
    md_paths: Sequence[Path] | None = None,
    settings_classes: Mapping[str, type[CdmDataLoadersBase]] | None = None,
) -> list[str]:
    """Verify ``cdm_data_loaders`` imports and settings kwargs inside hand-written and rendered Python blocks."""
    resolved_settings = settings_classes if settings_classes is not None else _discover_settings_classes()
    settings_kwargs_map = {name: _valid_settings_kwargs(cls) for name, cls in resolved_settings.items()}
    errors: list[str] = []

    for label, text in _resolve_markdown_sources(md_paths):
        for match in PYTHON_FENCE_RE.finditer(text):
            block = textwrap.dedent(match.group(2))
            block_line = text[: match.start()].count("\n") + 2
            try:
                tree = ast.parse(block)
            except SyntaxError as exc:
                if "cdm_data_loaders" in block:
                    errors.append(f"{label}:{block_line}: SyntaxError in Python block ({exc.msg})")
                continue

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    line = block_line + node.lineno - 1
                    for alias in node.names:
                        if alias.name.startswith("cdm_data_loaders"):
                            try:
                                importlib.import_module(alias.name)
                            except Exception as exc:  # noqa: BLE001
                                errors.append(f"{label}:{line}: cannot import module '{alias.name}' ({exc})")
                elif isinstance(node, ast.ImportFrom):
                    errors.extend(_verify_import_from_node(node, label, block_line))
                elif isinstance(node, ast.Call):
                    callee_name = (
                        node.func.id
                        if isinstance(node.func, ast.Name)
                        else node.func.attr
                        if isinstance(node.func, ast.Attribute)
                        else None
                    )
                    if callee_name in settings_kwargs_map:
                        line = block_line + node.lineno - 1
                        valid_kwargs = settings_kwargs_map[callee_name]
                        errors.extend(
                            f"{label}:{line}: unknown field '{kw.arg}' passed to {callee_name}(...)"
                            for kw in node.keywords
                            if kw.arg is not None and kw.arg not in valid_kwargs
                        )

    return errors


def _build_valid_cli_flags_and_env_vars(
    cli_entries: Sequence[CliEntryDoc],
    settings_classes: Mapping[str, type[CdmDataLoadersBase]],
) -> tuple[dict[str, set[str]], set[str]]:
    """Build per-CLI valid flag sets and the global set of valid ``CDL_*`` environment variables."""
    cli_flags: dict[str, set[str]] = {}
    valid_env_vars: set[str] = set()

    for entry in cli_entries:
        flags = {"-h", "--help"}
        for field_doc in entry.fields:
            flags.add(field_doc.cli_flag)
            flags.add(f"--{field_doc.name}")
            for shortcut in field_doc.shortcuts:
                flags.add(shortcut)
                flags.add(shortcut.replace("_", "-"))
            valid_env_vars.add(field_doc.env_var)
        cli_flags[entry.cli_name] = flags

    for settings_cls in settings_classes.values():
        env_prefix = str(settings_cls.model_config.get("env_prefix", "CDL_"))
        for field_name in settings_cls.model_fields:
            valid_env_vars.add(f"{env_prefix}{field_name.upper()}")

    return cli_flags, valid_env_vars


def _extract_cli_invocation_flags(tokens: list[str], cli_names: set[str]) -> tuple[str, list[str]] | None:
    """If ``tokens`` invokes a registered CLI via ``uv run``, ``pipeline``, or container entrypoint, return its flags."""
    for idx, token in enumerate(tokens):
        if token not in cli_names:
            continue
        prev = tokens[idx - 1] if idx > 0 else ""
        if idx == 0 or prev in {"run", "pipeline", "cdm-data-loaders"} or prev.endswith(":latest"):
            flags: list[str] = []
            for arg in tokens[idx + 1 :]:
                if arg in SHELL_STOP_TOKENS:
                    break
                if arg.startswith("-"):
                    flags.append(arg.split("=", maxsplit=1)[0])
            return token, flags
    return None


def check_markdown_cli_and_env_refs(
    md_paths: Sequence[Path] | None = None,
    cli_entries: Sequence[CliEntryDoc] | None = None,
    settings_classes: Mapping[str, type[CdmDataLoadersBase]] | None = None,
) -> list[str]:
    """Verify CLI flags in shell code blocks and ``CDL_*`` environment variables across hand-written and rendered docs."""
    resolved_entries = list(cli_entries) if cli_entries is not None else load_cli_entries()
    resolved_settings = settings_classes if settings_classes is not None else _discover_settings_classes()
    cli_flags_map, valid_env_vars = _build_valid_cli_flags_and_env_vars(resolved_entries, resolved_settings)
    cli_names = set(cli_flags_map)
    errors: list[str] = []

    for label, text in _resolve_markdown_sources(md_paths):
        for match in SHELL_FENCE_RE.finditer(text):
            block = match.group(2)
            block_line = text[: match.start()].count("\n") + 2
            joined_block = re.sub(r"\\\s*\n(?:\s*>\s*)?", " ", block)
            for raw_line in joined_block.splitlines():
                line_str = raw_line.strip().lstrip(">").strip()
                if not line_str or line_str.startswith("#"):
                    continue
                invocation = _extract_cli_invocation_flags(line_str.split(), cli_names)
                if invocation is None:
                    continue
                cli_name, used_flags = invocation
                valid_flags = cli_flags_map[cli_name]
                errors.extend(
                    f"{label}:{block_line}: invalid flag '{flag}' for CLI '{cli_name}'"
                    for flag in used_flags
                    if flag not in valid_flags
                )

        for env_match in CDL_ENV_VAR_RE.finditer(text):
            env_var = env_match.group(0)
            if env_var == "CDL_":
                continue
            if env_var not in valid_env_vars:
                line = text[: env_match.start()].count("\n") + 1
                errors.append(f"{label}:{line}: unknown environment variable '{env_var}'")

    return errors


def test_jinja_templates_inventory_and_strict_macro_registration_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify all 15 .md.j2 templates exist, compile cleanly, and register macros without mutating docs/."""
    template_files = sorted(p.relative_to(TEMPLATES_DIR).as_posix() for p in TEMPLATES_DIR.rglob("*.md.j2"))
    assert len(template_files) == EXPECTED_TEMPLATE_COUNT
    env = create_jinja_env()
    for rel_template in template_files:
        compiled = env.get_template(rel_template)
        assert compiled.name == rel_template

    before_docs_entries = sorted(p.relative_to(DOCS_DIR).as_posix() for p in DOCS_DIR.rglob("*"))
    for mod_key in [k for k in sys.modules if k == "docs_macros" or k.startswith("docs_macros.")]:
        monkeypatch.delitem(sys.modules, mod_key)

    macro_env = MacroEnv(conf={"root_dir": str(REPO_ROOT), "docs_dir": "docs"})
    _load_module(macro_env, "docs_macros", REPO_ROOT)
    assert set(macro_env.macros) == {
        "render_cli_reference",
        "render_bundled_schemas",
        "render_converter_spec",
        "render_pipeline_spec",
        "render_reference_table",
    }
    assert set(macro_env.filters) == {"escape_cell", "code_fence"}
    define_env(macro_env)
    after_docs_entries = sorted(p.relative_to(DOCS_DIR).as_posix() for p in DOCS_DIR.rglob("*"))
    assert after_docs_entries == before_docs_entries


def test_all_macro_stub_pages_render_via_zensical_loader_pass(tmp_path: Path) -> None:
    """Verify all 13 planned render_macros: true stub pages render non-empty Markdown with H1 headings."""
    plan = build_generated_docs_plan()
    md_stubs = {rel_path: text for rel_path, text in plan.items() if rel_path.suffix == ".md"}
    assert len(md_stubs) == EXPECTED_MACRO_STUB_COUNT

    staged_docs = tmp_path / "docs"
    sync_res = sync_generated_docs(DocsGenerationPaths(docs_dir=staged_docs))
    assert len(sync_res.expected_paths) == len(plan)

    for rel_path, stub_text in md_stubs.items():
        assert FRONT_MATTER_RE.match(stub_text) is not None
        rendered = materialize_markdown_text(stub_text)
        assert rendered.strip().startswith("# "), f"{rel_path.name} did not render an H1 title"
        assert "{{" not in rendered or "```" in rendered
        assert materialize_markdown_file(staged_docs / rel_path) == rendered


def test_load_cli_entries_and_render_cli_reference_pass() -> None:
    """Verify all registered pipeline CLIs are introspected and rendered by pages/cli_reference.md.j2."""
    entries = load_cli_entries()
    cli_names = [entry.cli_name for entry in entries]
    assert len(entries) >= MIN_EXPECTED_CLIS
    assert "jsonlines_extract" in cli_names
    assert "uniprot" in cli_names
    assert "xsv" in cli_names

    rendered = render_cli_reference(entries)
    assert "# CLI Reference" in rendered
    assert "## `jsonlines_extract`" in rendered
    assert "`--dataset-name`" in rendered
    assert "`CDL_DATASET_NAME`" in rendered

    stub_text = build_generated_docs_plan()[Path("cli_reference.md")]
    assert "render_cli_reference()" in stub_text
    assert materialize_markdown_text(stub_text).strip() == rendered.strip()


def test_discover_and_render_bundled_schemas_and_published_jsonschemas_pass() -> None:
    """Verify schema asset discovery, inline collapsible rendering, no schema_source dir, and byte-equal published JSON."""
    modules = discover_api_modules()
    module_paths = {mod.module_path for mod in modules}
    assert "cdm_data_loaders" in module_paths
    assert "cdm_data_loaders.core.settings" in module_paths
    assert "cdm_data_loaders.pipelines.core" in module_paths

    assert not (DOCS_DIR / "schema_source").exists()

    assets = discover_schema_assets()
    asset_paths = {asset.rel_path.as_posix() for asset in assets}
    assert "schemas/uniprot/uniprot.xsd" in asset_paths
    assert "schemas/uniref/uniref.xsd" in asset_paths
    assert "schemas/xsv_config/0.1.0/schema.json" in asset_paths
    assert all(not asset.rel_path.name.endswith(".py") for asset in assets)
    assert repo_blob_url(REPO_ROOT / "zensical.toml") == f"{GITHUB_BLOB_BASE}/zensical.toml"

    rendered = render_bundled_schemas(assets)
    assert "# Bundled Schemas and XSD Assets" in rendered
    assert "## Schema Source Files" in rendered
    assert '??? note "Source (`src/cdm_data_loaders/schemas/uniprot/uniprot.xsd`)"' in rendered
    assert "[Site JSON](schemas/xsv_config/0.1.0/schema.json)" in rendered
    assert f"{SITE_URL_BASE}/schemas/xsv_config/0.1.0/schema.json" in rendered
    assert f"{GITHUB_BLOB_BASE}/src/cdm_data_loaders/schemas/uniprot/uniprot.xsd" in rendered

    plan = build_generated_docs_plan()
    stub_text = plan[Path("bundled_schemas.md")]
    assert "render_bundled_schemas()" in stub_text
    assert materialize_markdown_text(stub_text).strip() == rendered.strip()

    expected_published: dict[Path, str] = {}
    for asset in assets:
        if asset.published_rel_path is not None:
            src_file = SRC_ROOT / asset.rel_path
            expected_published[asset.published_rel_path] = src_file.read_text(encoding="utf-8")
            assert plan[asset.published_rel_path] == expected_published[asset.published_rel_path]

    planned_schemas = {rel_path for rel_path in plan if rel_path.parts[0] == SCHEMAS_SUBDIR}
    assert planned_schemas == set(expected_published)


@pytest.mark.parametrize("table_id", sorted(TABLE_TEMPLATE_REGISTRY), ids=str)
def test_render_reference_table_all_registered_tables_pass(table_id: str) -> None:
    """Verify every registered reference table in TABLE_TEMPLATE_REGISTRY renders a Markdown table."""
    rendered = render_reference_table(table_id)
    lines = rendered.splitlines()
    assert len(lines) >= MIN_MARKDOWN_TABLE_LINES
    assert lines[0].startswith("| ")
    assert lines[1].startswith("| ")


def test_render_unknown_keys_raise_key_error() -> None:
    """Verify unknown converter, pipeline, and table identifiers raise KeyError."""
    with pytest.raises(KeyError, match="Unknown converter specification"):
        render_converter_spec("nonexistent_converter")
    with pytest.raises(KeyError, match="Unknown pipeline specification"):
        render_pipeline_spec("nonexistent_pipeline")
    with pytest.raises(KeyError, match="Unknown reference table identifier"):
        render_reference_table("nonexistent_table")
    assert set(CONVERTER_SPECS) == {
        "xsd_to_pyarrow",
        "jsonschema_to_pyarrow",
        "pydantic_to_pyarrow",
        "pyarrow_to_jsonschema",
        "pyarrow_to_linkml",
    }
    assert set(PIPELINE_SPECS) == {
        "xmltopyarrow",
        "jsonlines_jsonschema",
        "xsv",
        "jsonlines_extract",
        "jsonlines_pydantic",
        "xmltodict",
    }


def test_gitignore_and_docs_directory_autogenerated_invariants() -> None:
    """Verify .gitignore covers all autogenerated docs paths and docs/ contains no stale macro stubs or schemas."""
    gitignore_lines = {
        line.strip()
        for line in (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }
    for required_pattern in (
        "/docs/cli_reference.md",
        "/docs/bundled_schemas.md",
        "/docs/*_specification.md",
        "/docs/schemas/",
    ):
        assert required_pattern in gitignore_lines

    plan = build_generated_docs_plan()
    for rel_path, expected_text in plan.items():
        is_ignored = (
            rel_path in {Path("cli_reference.md"), Path("bundled_schemas.md")}
            or (len(rel_path.parts) == 1 and rel_path.name.endswith("_specification.md"))
            or rel_path.parts[0] == SCHEMAS_SUBDIR
        )
        assert is_ignored, f"Autogenerated path docs/{rel_path.as_posix()} is not covered by .gitignore"
        disk_target = DOCS_DIR / rel_path
        if disk_target.is_file():
            assert disk_target.read_text(encoding="utf-8") == expected_text

    planned_md_names = {rel_path.name for rel_path in plan if rel_path.suffix == ".md"}
    for md_file in sorted(DOCS_DIR.glob("*.md")):
        if md_file.name in planned_md_names:
            continue
        raw = md_file.read_text(encoding="utf-8")
        assert GENERATED_MACRO_STUB_RE.search(raw) is None, f"Unexpected macro stub in docs/: {md_file.name}"

    schemas_dir = DOCS_DIR / SCHEMAS_SUBDIR
    if schemas_dir.is_dir():
        planned_schema_files = {(DOCS_DIR / rel).resolve() for rel in plan if rel.parts[0] == SCHEMAS_SUBDIR}
        on_disk_schemas = {p.resolve() for p in schemas_dir.rglob("*") if p.is_file()}
        assert on_disk_schemas <= planned_schema_files


def _stage_synthetic_docs_tree(tmp_path: Path) -> DocsGenerationPaths:
    """Create an isolated temporary repo layout with converter/pipeline specs, schemas, and hand-written docs."""
    paths = DocsGenerationPaths(
        docs_dir=tmp_path / "docs",
        converter_specs_root=tmp_path / "specs" / "schema_converters",
        pipeline_specs_root=tmp_path / "specs" / "pipelines",
        src_root=tmp_path / "src" / "cdm_data_loaders",
        repo_root=tmp_path,
    )
    paths.docs_dir.mkdir(parents=True, exist_ok=True)
    (paths.docs_dir / "index.md").write_text("# Documentation Home\n", encoding="utf-8")
    tablesort_js = paths.docs_dir / "js" / "tablesort.js"
    tablesort_js.parent.mkdir(parents=True, exist_ok=True)
    tablesort_js.write_text("// tablesort\n", encoding="utf-8")

    for conv_name in ("alpha_to_pyarrow", "beta_to_jsonschema"):
        conv_dir = paths.converter_specs_root / conv_name
        conv_dir.mkdir(parents=True, exist_ok=True)
        (conv_dir / "rules.yaml").write_text("title: Sample\nsections: []\n", encoding="utf-8")

    pipe_dir = paths.pipeline_specs_root / "sample_pipe"
    pipe_dir.mkdir(parents=True, exist_ok=True)
    (pipe_dir / "rules.yaml").write_text("title: Sample Pipe\nsections: []\n", encoding="utf-8")

    src_schema = paths.src_root / "schemas" / "xsv_config" / "0.1.0" / "schema.json"
    src_schema.parent.mkdir(parents=True, exist_ok=True)
    src_schema.write_text('{"type": "object"}\n', encoding="utf-8")
    return paths


def test_sync_generated_docs_cli_writes_and_prunes_stale_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify docs_macros CLI --check and sync create missing stubs/schemas and prune stale ones."""
    empty_paths = DocsGenerationPaths(
        docs_dir=tmp_path / "empty_docs",
        converter_specs_root=tmp_path / "missing_conv",
        pipeline_specs_root=tmp_path / "missing_pipe",
        src_root=tmp_path / "missing_src",
        repo_root=tmp_path,
    )
    assert discover_spec_names(empty_paths.converter_specs_root) == ()
    assert clean_generated_docs(empty_paths) == ()
    assert sync_generated_docs(empty_paths, include_static_pages=False).expected_paths == ()

    paths = _stage_synthetic_docs_tree(tmp_path)
    stale_conv_stub = paths.docs_dir / "legacy_to_pyarrow_specification.md"
    stale_conv_stub.write_text(render_macro_stub('render_converter_spec("legacy_to_pyarrow")'), encoding="utf-8")
    stale_pipe_stub = paths.docs_dir / "old_pipe_specification.md"
    stale_pipe_stub.write_text(render_macro_stub('render_pipeline_spec("old_pipe")'), encoding="utf-8")
    stale_schema = paths.docs_dir / "schemas" / "obsolete" / "0.1.0" / "schema.json"
    stale_schema.parent.mkdir(parents=True)
    stale_schema.write_text("{}\n", encoding="utf-8")

    assert docs_macros_cli_main(["--check"], paths=paths) == 1
    err_out = capsys.readouterr().err
    for expected_fragment in (
        "cli_reference.md",
        "alpha_to_pyarrow_specification.md",
        "legacy_to_pyarrow_specification.md",
        "old_pipe_specification.md",
        "schemas/obsolete/0.1.0/schema.json",
    ):
        assert expected_fragment in err_out

    assert docs_macros_cli_main([], paths=paths) == 0
    sync_out = capsys.readouterr().out
    assert "Wrote" in sync_out
    assert "Removed stale" in sync_out
    assert not stale_conv_stub.exists()
    assert not stale_pipe_stub.exists()
    assert not stale_schema.exists()
    assert not (paths.docs_dir / "schemas" / "obsolete").exists()
    assert (paths.docs_dir / "index.md").is_file()
    assert (paths.docs_dir / "js" / "tablesort.js").is_file()
    assert (paths.docs_dir / "cli_reference.md").read_text(encoding="utf-8") == render_macro_stub(
        "render_cli_reference()"
    )
    assert (paths.docs_dir / "alpha_to_pyarrow_specification.md").read_text(encoding="utf-8") == render_macro_stub(
        'render_converter_spec("alpha_to_pyarrow")'
    )
    assert (paths.docs_dir / "sample_pipe_specification.md").read_text(encoding="utf-8") == render_macro_stub(
        'render_pipeline_spec("sample_pipe")'
    )
    assert (paths.docs_dir / "schemas" / "xsv_config" / "0.1.0" / "schema.json").read_text(encoding="utf-8") == (
        '{"type": "object"}\n'
    )

    # idempotent second run writes nothing and passes --check
    assert docs_macros_cli_main([], paths=paths) == 0
    assert "already up to date" in capsys.readouterr().out
    assert docs_macros_cli_main(["--check"], paths=paths) == 0
    assert "Verified 6 autogenerated documentation file(s) in sync." in capsys.readouterr().out


def test_clean_generated_docs_cli_removes_all_generated_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify docs_macros CLI --clean removes all autogenerated stubs and schemas while preserving hand-written files."""
    paths = _stage_synthetic_docs_tree(tmp_path)
    assert docs_macros_cli_main([], paths=paths) == 0
    capsys.readouterr()

    # add an extra orphaned macro stub to confirm --clean removes unreferenced macro stubs as well
    orphan_stub = paths.docs_dir / "orphan_specification.md"
    orphan_stub.write_text(render_macro_stub('render_converter_spec("orphan")'), encoding="utf-8")

    assert docs_macros_cli_main(["--clean"], paths=paths) == 0
    clean_out = capsys.readouterr().out
    assert "Removed" in clean_out
    for removed_rel in (
        "cli_reference.md",
        "bundled_schemas.md",
        "alpha_to_pyarrow_specification.md",
        "beta_to_jsonschema_specification.md",
        "sample_pipe_specification.md",
        "orphan_specification.md",
        "schemas",
    ):
        assert not (paths.docs_dir / removed_rel).exists()

    assert (paths.docs_dir / "index.md").is_file()
    assert (paths.docs_dir / "js" / "tablesort.js").is_file()

    assert docs_macros_cli_main(["--clean"], paths=paths) == 0
    assert "No autogenerated documentation files present to remove." in capsys.readouterr().out


def test_discover_converter_specs_auto_discovers_and_validates(tmp_path: Path) -> None:
    """Verify discover_converter_specs infers converter config from rules.yaml + facade and validates errors."""
    specs_root = tmp_path / "specs"
    docs_dir = tmp_path / "docs"
    assert discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir) == {}

    conv_dir = specs_root / "pydantic_to_pyarrow"
    (conv_dir / "spec").mkdir(parents=True)
    (conv_dir / "rules.yaml").write_text(
        "title: Synthetic\nsections:\n  - id: s1\n    title: S1\n    generated: scalar_table\n    rules: []\n",
        encoding="utf-8",
    )
    discovered = discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)
    cfg = discovered["pydantic_to_pyarrow"]
    assert cfg.output == docs_dir / "pydantic_to_pyarrow_specification.md"
    assert cfg.suites == ("spec",)
    assert cfg.schema_lang == "python"
    assert cfg.data_lang == "json"
    assert cfg.generated_tables == {"scalar_table": "pydantic_scalar_table"}

    unknown_src_dir = specs_root / "unknown_to_pyarrow"
    unknown_src_dir.mkdir(parents=True)
    (unknown_src_dir / "rules.yaml").write_text("title: Bad Source\nsections: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Cannot infer schema_lang/data_lang"):
        discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)
    (unknown_src_dir / "rules.yaml").unlink()

    unknown_tgt_dir = specs_root / "pydantic_to_unknown"
    unknown_tgt_dir.mkdir(parents=True)
    (unknown_tgt_dir / "rules.yaml").write_text("title: Bad Target\nsections: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Cannot infer target rendering defaults"):
        discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)
    (unknown_tgt_dir / "rules.yaml").unlink()

    missing_errors_dir = specs_root / "jsonschema_to_dlt"
    missing_errors_dir.mkdir(parents=True)
    (missing_errors_dir / "rules.yaml").write_text("title: Missing Errors Tuple\nsections: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="JSONSCHEMA_TO_DLT_ERRORS"):
        discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)


@pytest.mark.parametrize(
    ("rel_to_schemas", "expected"),
    [
        pytest.param("xsv_config/0.1.0/schema.json", True, id="nested_xsv_config_json_published"),
        pytest.param("xsv_config/README.md", False, id="non_json_under_xsv_config_not_published"),
        pytest.param("gtdb/gtdb_metadata.schema.json", False, id="other_schema_group_not_published"),
        pytest.param("schema.json", False, id="root_schema_file_not_published"),
    ],
)
def test_is_published_jsonschema_cases(rel_to_schemas: str, expected: bool) -> None:
    """Verify is_published_jsonschema matches only .json files under configured published subdirectories."""
    assert is_published_jsonschema(Path(rel_to_schemas)) is expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        pytest.param("schema.json", ("JSON Schema", "json"), id="json_schema"),
        pytest.param("uniprot.xsd", ("XML Schema (XSD)", "xml"), id="xsd_schema"),
        pytest.param("model.YAML", ("YAML", "yaml"), id="uppercase_yaml"),
        pytest.param("entity.dtd", ("DTD File", "text"), id="unknown_extension_fallback"),
        pytest.param("NOTICE", ("File", "text"), id="extensionless_file_fallback"),
    ],
)
def test_resolve_schema_file_metadata_cases(filename: str, expected: tuple[str, str]) -> None:
    """Verify resolve_schema_file_metadata maps known, unknown, uppercase, and missing file extensions."""
    assert resolve_schema_file_metadata(Path(filename)) == expected


@pytest.mark.parametrize(
    ("text", "min_backticks", "expected"),
    [
        pytest.param("", 4, "````", id="empty_text_uses_minimum_fence"),
        pytest.param("```json\n{}\n```", 4, "````", id="triple_backticks_stay_at_four"),
        pytest.param("````markdown\nhi\n````", 4, "`````", id="quadruple_backticks_expand_to_five"),
        pytest.param("plain text", 3, "```", id="custom_minimum_three_backticks"),
    ],
)
def test_code_fence_for_text_cases(text: str, min_backticks: int, expected: str) -> None:
    """Verify code_fence_for_text returns a backtick delimiter longer than any backtick run in text."""
    assert code_fence_for_text(text, min_backticks=min_backticks) == expected


def test_formatting_and_schema_discovery_edge_cases(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify escape_table_cell, slugify_anchor, schema discovery filtering, and check_or_write_files."""
    assert escape_table_cell("a | b\nc") == r"a \| b c"
    assert slugify_anchor("schema", "xsv_config/0.1.0/schema.json") == "schema-xsv-config-0-1-0-schema-json"

    empty_src = tmp_path / "empty_pkg"
    empty_src.mkdir()
    assert discover_schema_assets(empty_src, repo_root=tmp_path) == []

    pkg_root = tmp_path / "src" / "cdm_data_loaders"
    schemas_dir = pkg_root / "schemas"
    (schemas_dir / "__pycache__").mkdir(parents=True)
    (schemas_dir / "__pycache__" / "cached.json").write_text("{}", encoding="utf-8")
    (schemas_dir / ".hidden").mkdir()
    (schemas_dir / ".hidden" / "secret.json").write_text("{}", encoding="utf-8")
    (schemas_dir / ".DS_Store").write_text("ignored", encoding="utf-8")
    (schemas_dir / "__init__.py").write_text("", encoding="utf-8")
    (schemas_dir / "helper.pyi").write_text("", encoding="utf-8")
    (schemas_dir / "root_notes.txt").write_text("note with ```` four backticks\n", encoding="utf-8")
    nested_dir = schemas_dir / "domain" / "0.1.0"
    nested_dir.mkdir(parents=True)
    (nested_dir / "spec.yaml").write_text("id: example\n", encoding="utf-8")
    xsv_dir = schemas_dir / "xsv_config" / "0.1.0"
    xsv_dir.mkdir(parents=True)
    (xsv_dir / "schema.json").write_text('{"type": "object"}\n', encoding="utf-8")

    discovered = discover_schema_assets(pkg_root, repo_root=tmp_path)
    assert [a.rel_path.as_posix() for a in discovered] == [
        "schemas/domain/0.1.0/spec.yaml",
        "schemas/root_notes.txt",
        "schemas/xsv_config/0.1.0/schema.json",
    ]
    assert discovered[1].code_block.fence == "`````"
    assert discovered[2].published_rel_path == Path("schemas/xsv_config/0.1.0/schema.json")

    rendered = render_bundled_schemas(discovered, src_root=pkg_root, repo_root=tmp_path)
    assert '`````text linenums="1"' in rendered
    assert f"{SITE_URL_BASE}/schemas/xsv_config/0.1.0/schema.json" in rendered

    target_file = tmp_path / "generated.py"
    assert check_or_write_files([(target_file, "x = 1\n")], check=True, repo_root=tmp_path) == 1
    assert "stale:" in capsys.readouterr().err
    assert check_or_write_files([(target_file, "x = 1\n")], check=False, repo_root=tmp_path) == 0
    assert check_or_write_files([(target_file, "x = 1\n")], check=True, repo_root=tmp_path) == 0


VALID_SYNTHETIC_INDEX_TEXT: Final[str] = (
    "## User Guides\n[Guide](guide.md)\n## Specifications\n## Developer Guides\n## API and Schema Reference\n"
)
VALID_SYNTHETIC_README_TEXT: Final[str] = (
    f"- [Home]({SITE_URL_BASE}/)\n"
    f"- [User Guides]({SITE_URL_BASE}/#user-guides)\n"
    f"- [Specifications]({SITE_URL_BASE}/#specifications)\n"
    f"- [Developer Guides]({SITE_URL_BASE}/#developer-guides)\n"
    f"- [API and Schema Reference]({SITE_URL_BASE}/#api-and-schema-reference)\n"
)


@pytest.mark.parametrize(
    ("module_or_symbol", "expected"),
    [
        pytest.param(
            "cdm_data_loaders.core.settings",
            f"{GITHUB_BLOB_BASE}/src/cdm_data_loaders/core/settings.py",
            id="dotted_module_path",
        ),
        pytest.param(
            "cdm_data_loaders.pipelines.xsv.pipeline:cli",
            f"{GITHUB_BLOB_BASE}/src/cdm_data_loaders/pipelines/xsv/pipeline.py",
            id="entry_point_module_colon_callable",
        ),
        pytest.param(
            "cdm_data_loaders.core.settings.CtsSettings",
            f"{GITHUB_BLOB_BASE}/src/cdm_data_loaders/core/settings.py",
            id="dotted_module_and_class_symbol",
        ),
        pytest.param("", None, id="empty_string_returns_none"),
        pytest.param(":cli_only", None, id="empty_module_before_colon_returns_none"),
        pytest.param("nonexistent_top_level_pkg.mod", None, id="unknown_module_returns_none"),
        pytest.param("json", None, id="stdlib_module_outside_repo_returns_none"),
        pytest.param(".relative_module", None, id="invalid_relative_module_returns_none"),
    ],
)
def test_module_blob_url_cases(module_or_symbol: str, expected: str | None) -> None:
    """Verify module_blob_url resolves repo modules, entry points, and symbols while returning None otherwise."""
    assert module_blob_url(module_or_symbol) == expected


@pytest.mark.parametrize(
    ("module_or_symbol", "expected"),
    [
        pytest.param(
            "cdm_data_loaders.core.settings",
            "api/cdm_data_loaders/core/settings.md",
            id="dotted_module_path",
        ),
        pytest.param(
            "cdm_data_loaders",
            "api/cdm_data_loaders/index.md",
            id="top_level_package_index",
        ),
        pytest.param(
            "cdm_data_loaders.pipelines.xsv",
            "api/cdm_data_loaders/pipelines/xsv/index.md",
            id="subpackage_index",
        ),
        pytest.param(
            "cdm_data_loaders.pipelines.xsv.pipeline:cli",
            "api/cdm_data_loaders/pipelines/xsv/pipeline.md",
            id="entry_point_module_colon_callable",
        ),
        pytest.param(
            "cdm_data_loaders.core.settings.CtsSettings",
            "api/cdm_data_loaders/core/settings.md",
            id="dotted_module_and_class_symbol",
        ),
        pytest.param("", None, id="empty_string_returns_none"),
        pytest.param("json", None, id="stdlib_module_outside_src_returns_none"),
        pytest.param("nonexistent_top_level_pkg.mod", None, id="unknown_module_returns_none"),
    ],
)
def test_module_api_url_cases(module_or_symbol: str, expected: str | None) -> None:
    """Verify module_api_url resolves src modules and packages to API doc paths while returning None otherwise."""
    assert module_api_url(module_or_symbol) == expected


def test_zensical_nav_sections_and_standalone_guide_links_pass() -> None:
    """Verify zensical.toml nav structure, replaced guide files, and blob/develop links in standalone docs."""
    zensical_data = tomllib.loads(ZENSICAL_PATH.read_text(encoding="utf-8"))
    nav = zensical_data["project"]["nav"]
    top_level_sections = [next(iter(entry.keys())) for entry in nav]
    assert top_level_sections == list(EXPECTED_NAV_SECTIONS)

    assert not (DOCS_DIR / "converters_ir.md").exists()
    assert (DOCS_DIR / "schema_converters.md").is_file()
    assert (DOCS_DIR / "development_and_testing.md").is_file()

    excluded = _excluded_doc_names()
    generated_md_names = {rel_path.name for rel_path in build_generated_docs_plan() if rel_path.suffix == ".md"}
    standalone_guides = sorted(
        md_path
        for md_path in DOCS_DIR.glob("*.md")
        if md_path.name not in excluded and md_path.name not in generated_md_names
    )
    assert standalone_guides
    for md_path in standalone_guides:
        stripped = FENCED_BLOCK_RE.sub("", md_path.read_text(encoding="utf-8"))
        assert GITHUB_BLOB_LINK_RE.search(stripped) is not None, (
            f"Standalone guide docs/{md_path.name} has no GitHub blob/develop codebase links"
        )


def test_about_these_docs_color_picker_assets_and_swatches_pass() -> None:
    """Verify About These Docs page, palette CSS/JS assets, zensical.toml theme defaults, and color swatches."""
    about_path = DOCS_DIR / "about_these_docs.md"
    css_path = DOCS_DIR / "css" / "palette.css"
    js_path = DOCS_DIR / "js" / "palette.js"
    assert about_path.is_file()
    assert css_path.is_file()
    assert js_path.is_file()

    zensical_data = tomllib.loads(ZENSICAL_PATH.read_text(encoding="utf-8"))
    project_data = zensical_data["project"]
    assert "css/palette.css" in project_data.get("extra_css", [])
    assert "js/palette.js" in project_data.get("extra_javascript", [])

    palette_entries = project_data["theme"]["palette"]
    assert palette_entries
    for entry in palette_entries:
        assert entry.get("primary") == "teal"
        assert entry.get("accent") == "teal"

    about_text = about_path.read_text(encoding="utf-8")
    assert "data-cdm-color-picker" in about_text
    assert 'id="cdm-palette-reset"' in about_text
    assert 'id="cdm-active-primary-value"' in about_text
    assert 'id="cdm-active-accent-value"' in about_text
    assert 'id="cdm-palette-toml"' in about_text
    assert "style=" not in about_text
    assert CSS_COLOR_LITERAL_RE.search(about_text) is None

    matches = SWATCH_BUTTON_RE.findall(about_text)
    primary_swatches = tuple(val for role, val in matches if role == "primary")
    accent_swatches = tuple(val for role, val in matches if role == "accent")
    assert primary_swatches == EXPECTED_PRIMARY_COLORS
    assert accent_swatches == EXPECTED_ACCENT_COLORS

    css_text = css_path.read_text(encoding="utf-8")
    assert ".cdm-color-swatch[data-md-color-primary]" in css_text
    assert ".cdm-color-swatch[data-md-color-accent]" in css_text
    assert "var(--md-primary-fg-color)" in css_text
    assert "var(--md-accent-fg-color)" in css_text
    assert CSS_COLOR_LITERAL_RE.search(css_text) is None

    js_text = js_path.read_text(encoding="utf-8")
    assert "button[data-md-color-primary]" in js_text
    assert "button[data-md-color-accent]" in js_text
    for color in EXPECTED_PRIMARY_COLORS:
        assert f'"{color}"' in js_text


def test_check_zensical_nav_and_index_pass() -> None:
    """Verify docs/*.md parity across zensical.toml and docs/index.md and top-level website links in README.md."""
    assert check_zensical_nav_and_index() == []


@pytest.mark.parametrize(
    ("extra_doc_name", "zensical_text", "index_text", "readme_text", "expected_errors"),
    [
        pytest.param(
            "orphan_spec.md",
            '[project]\nnav = [{ "Home" = "index.md" }, { "Guide" = "guide.md" }]\n',
            VALID_SYNTHETIC_INDEX_TEXT,
            VALID_SYNTHETIC_README_TEXT,
            [
                "docs/orphan_spec.md is missing from zensical.toml nav",
                "docs/orphan_spec.md is not linked in docs/index.md",
            ],
            id="unindexed_doc_file_caught_across_nav_and_index",
        ),
        pytest.param(
            None,
            '[project]\nnav = [{ "Home" = "index.md" }, { "Guide" = "guide.md" }, { "Ghost" = "ghost.md" }]\n',
            f"{VALID_SYNTHETIC_INDEX_TEXT}[Missing](missing_index.md)\n",
            (
                f"- [Home]({SITE_URL_BASE}/)\n"
                f"- [User Guides]({SITE_URL_BASE}/#user-guides)\n"
                f"- [Specifications]({SITE_URL_BASE}/#specifications)\n"
                f"- [Bad Section]({SITE_URL_BASE}/#nonexistent-section)\n"
                "- [Guide](docs/guide.md)\n"
            ),
            [
                "zensical.toml nav entry 'ghost.md' does not exist in docs/",
                "docs/index.md links to non-existent 'docs/missing_index.md'",
                f"README.md is missing top-level docs website link '{SITE_URL_BASE}/#developer-guides'",
                f"README.md is missing top-level docs website link '{SITE_URL_BASE}/#api-and-schema-reference'",
                "README.md docs website anchor '#nonexistent-section' does not match any H2 heading in docs/index.md",
                "README.md links to relative 'docs/guide.md' instead of the published docs website",
            ],
            id="nonexistent_targets_bad_anchor_and_relative_readme_link_caught",
        ),
        pytest.param(
            "implementation_plan.md",
            '[project]\nexclude_docs = """\nimplementation_plan.md\n"""\nnav = [{ "Home" = "index.md" }, { "Guide" = "guide.md" }]\n',
            VALID_SYNTHETIC_INDEX_TEXT,
            VALID_SYNTHETIC_README_TEXT,
            [],
            id="exclude_docs_file_ignored",
        ),
    ],
)
def test_check_zensical_nav_and_index_fail_detects_drift(
    tmp_path: Path,
    extra_doc_name: str | None,
    zensical_text: str,
    index_text: str,
    readme_text: str,
    expected_errors: list[str],
) -> None:
    """Verify check_zensical_nav_and_index detects orphaned docs, bad anchors, and relative README links."""
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    index_path = docs_dir / "index.md"
    index_path.write_text(index_text, encoding="utf-8")
    (docs_dir / "guide.md").write_text("# Guide\n", encoding="utf-8")
    if extra_doc_name is not None:
        (docs_dir / extra_doc_name).write_text("# Extra\n", encoding="utf-8")

    zensical_path = tmp_path / "zensical.toml"
    zensical_path.write_text(zensical_text, encoding="utf-8")
    readme_path = tmp_path / "README.md"
    readme_path.write_text(readme_text, encoding="utf-8")

    assert check_zensical_nav_and_index(docs_dir, zensical_path, index_path, readme_path) == expected_errors


def test_check_no_class_hierarchy_diagrams_pass() -> None:
    """Verify no Mermaid classDiagram blocks or settings/MRO inheritance chains appear across documentation."""
    assert check_no_class_hierarchy_diagrams() == []


@pytest.mark.parametrize(
    ("markdown_text", "expected_errors"),
    [
        pytest.param(
            "```mermaid\nclassDiagram\n    BaseSettings <|-- CtsSettings\n```\n",
            ["sample.md: contains forbidden Mermaid classDiagram block"],
            id="mermaid_class_diagram_detected",
        ),
        pytest.param(
            "- **Settings hierarchy**: `PubMedXmlToPyArrowSettings -> XmlToPyArrowSettings`\n",
            ["sample.md: contains forbidden class hierarchy or MRO chain listing"],
            id="settings_hierarchy_line_detected",
        ),
        pytest.param(
            "**MRO Chain:** `PubMedXmlToPyArrowSettings` -> `XmlToPyArrowSettings`\n",
            ["sample.md: contains forbidden class hierarchy or MRO chain listing"],
            id="mro_chain_line_detected",
        ),
        pytest.param(
            "```mermaid\nflowchart LR\n    A --> B\n```\n",
            [],
            id="mermaid_flowchart_allowed",
        ),
    ],
)
def test_check_no_class_hierarchy_diagrams_fail_detects_violations(
    tmp_path: Path,
    markdown_text: str,
    expected_errors: list[str],
) -> None:
    """Verify check_no_class_hierarchy_diagrams catches classDiagram blocks and MRO chains while allowing flowcharts."""
    md_file = tmp_path / "sample.md"
    md_file.write_text(markdown_text, encoding="utf-8")
    assert check_no_class_hierarchy_diagrams([md_file]) == expected_errors


def test_check_github_repo_blob_links_pass() -> None:
    """Verify every GitHub blob/develop link across hand-written and rendered docs resolves to a real file."""
    assert check_github_repo_blob_links() == []


@pytest.mark.parametrize(
    ("markdown_text", "expected_errors"),
    [
        pytest.param(
            f"See [`existing.txt`]({GITHUB_BLOB_BASE}/existing.txt#L1).\n",
            [],
            id="existing_blob_target_with_anchor_passes",
        ),
        pytest.param(
            f"See [`missing.py`]({GITHUB_BLOB_BASE}/src/missing.py).\n",
            ["sample.md: GitHub blob link target 'src/missing.py' does not exist in repository"],
            id="missing_blob_target_reported",
        ),
        pytest.param(
            "See [`existing.txt`](https://github.com/kbase/cdm-data-loaders/blob/main/existing.txt).\n",
            ["sample.md: GitHub blob link uses branch 'main' instead of 'develop' ('existing.txt')"],
            id="non_develop_branch_reported",
        ),
        pytest.param(
            f"```markdown\n[`ignored.py`]({GITHUB_BLOB_BASE}/src/ignored.py)\n```\n",
            [],
            id="fenced_code_block_links_ignored",
        ),
    ],
)
def test_check_github_repo_blob_links_detects_missing_target(
    tmp_path: Path,
    markdown_text: str,
    expected_errors: list[str],
) -> None:
    """Verify check_github_repo_blob_links flags wrong branches, missing paths, and ignores fenced code blocks."""
    (tmp_path / "existing.txt").write_text("ok\n", encoding="utf-8")
    md_file = tmp_path / "sample.md"
    md_file.write_text(markdown_text, encoding="utf-8")
    assert check_github_repo_blob_links([md_file], repo_root=tmp_path) == expected_errors


def test_check_markdown_python_blocks_pass() -> None:
    """Verify all Python code blocks in hand-written and rendered docs import valid symbols and settings fields."""
    assert check_markdown_python_blocks() == []


@pytest.mark.parametrize(
    ("markdown_text", "expected_substring"),
    [
        pytest.param(
            "```python\nfrom cdm_data_loaders.core.transforms import TableTransform\n```\n",
            "cannot import 'TableTransform' from 'cdm_data_loaders.core.transforms'",
            id="removed_symbol_in_from_import",
        ),
        pytest.param(
            "```python\nfrom cdm_data_loaders.converters.facades.jsonschema_to_pyspark.converter import JSONSchemaToPySpark\n```\n",
            "cannot import module 'cdm_data_loaders.converters.facades.jsonschema_to_pyspark.converter'",
            id="nonexistent_submodule_in_from_import",
        ),
        pytest.param(
            "```python\nimport cdm_data_loaders.nonexistent_module\n```\n",
            "cannot import module 'cdm_data_loaders.nonexistent_module'",
            id="nonexistent_module_in_direct_import",
        ),
        pytest.param(
            "```python\nfrom cdm_data_loaders.core.transforms import (\n```\n",
            "SyntaxError in Python block",
            id="syntax_error_in_block_with_cdm_import",
        ),
        pytest.param(
            (
                "```python\n"
                "from cdm_data_loaders.pipelines.jsonlines.settings import JsonlJsonschemaIngestSettings\n"
                "settings = JsonlJsonschemaIngestSettings(schema_files_module='my_mod')\n"
                "```\n"
            ),
            "unknown field 'schema_files_module' passed to JsonlJsonschemaIngestSettings(...)",
            id="stale_field_kwarg_in_settings_constructor",
        ),
    ],
)
def test_check_markdown_python_blocks_fail_detects_invalid_imports_and_kwargs(
    tmp_path: Path,
    markdown_text: str,
    expected_substring: str,
) -> None:
    """Verify check_markdown_python_blocks catches stale imports, missing modules, syntax errors, and bad kwargs."""
    md_file = tmp_path / "sample.md"
    md_file.write_text(markdown_text, encoding="utf-8")
    errors = check_markdown_python_blocks([md_file])
    assert len(errors) == 1
    assert expected_substring in errors[0]


def test_check_markdown_cli_and_env_refs_pass() -> None:
    """Verify all CLI invocations and CDL_* environment variables in hand-written and rendered docs are valid."""
    assert check_markdown_cli_and_env_refs() == []


@pytest.mark.parametrize(
    ("markdown_text", "expected_substring"),
    [
        pytest.param(
            "```sh\nuv run jsonlines_jsonschema \\\n  --schema-files-module my_schemas \\\n  --input-dir ./in\n```\n",
            "invalid flag '--schema-files-module' for CLI 'jsonlines_jsonschema'",
            id="stale_flag_in_uv_run_cli_invocation",
        ),
        pytest.param(
            "```bash\n./scripts/entrypoint.sh pipeline xsv --schema-path ./bad.json\n```\n",
            "invalid flag '--schema-path' for CLI 'xsv'",
            id="stale_flag_in_entrypoint_pipeline_invocation",
        ),
        pytest.param(
            "Configure `CDL_SCHEMA_FILES_MODULE` in the environment.\n",
            "unknown environment variable 'CDL_SCHEMA_FILES_MODULE'",
            id="stale_cdl_environment_variable_in_prose",
        ),
    ],
)
def test_check_markdown_cli_and_env_refs_fail_detects_invalid_flags_and_env_vars(
    tmp_path: Path,
    markdown_text: str,
    expected_substring: str,
) -> None:
    """Verify check_markdown_cli_and_env_refs catches invalid CLI flags and unknown CDL_* environment variables."""
    md_file = tmp_path / "sample.md"
    md_file.write_text(markdown_text, encoding="utf-8")
    errors = check_markdown_cli_and_env_refs([md_file])
    assert len(errors) == 1
    assert expected_substring in errors[0]


def test_table_transforms_docs_pass_cli_flags_match_pipeline_settings() -> None:
    """docs/table_transforms.md uses valid CLI flag names and accurate table_name requirements."""
    docs_path = DOCS_DIR / "table_transforms.md"
    docs_text = docs_path.read_text(encoding="utf-8")

    assert "--schema-path" not in docs_text
    assert "--record-tag" not in docs_text
    assert "--preset" not in docs_text
    assert 'defaults to `"data"`' not in docs_text
    assert "--schema-file" in docs_text
    assert "--xml-tag" in docs_text


def test_core_cts_execution_docs_pass_rejected_table_contract() -> None:
    """docs/core_cts_execution.md documents the unified _rejected table and REJECTED_PYARROW_SCHEMA columns."""
    docs_path = DOCS_DIR / "core_cts_execution.md"
    docs_text = docs_path.read_text(encoding="utf-8")

    assert "<table_name>_rejected" not in docs_text
    assert "line_number" not in docs_text
    assert "raw_data" not in docs_text
    assert "rejection_reason" not in docs_text
    for field_name in REJECTED_PYARROW_SCHEMA.names:
        assert field_name in docs_text


@pytest.mark.parametrize(
    ("text", "forbidden", "required"),
    [
        pytest.param(
            coerce_record_for_pyarrow.__doc__,
            "ArrowListBuffer",
            None,
            id="coerce_record_for_pyarrow_docstring",
        ),
        pytest.param(
            run_jsonlines_with_validation_pipeline.__doc__,
            ":rtyp:",
            ":rtype:",
            id="run_jsonlines_with_validation_pipeline_docstring",
        ),
        pytest.param(
            ReferencePipelineSettings.model_fields["parallel_step_2"].description,
            "ListBuffer",
            None,
            id="reference_pipeline_settings_parallel_step_2_description",
        ),
    ],
)
def test_docstrings_and_descriptions_pass_no_stale_buffer_names_or_typos(
    text: str | None,
    forbidden: str,
    required: str | None,
) -> None:
    """Docstrings and field descriptions do not reference removed *ListBuffer classes or contain :rtyp: typos."""
    assert text is not None
    assert forbidden not in text
    if required is not None:
        assert required in text


def test_griffe_pydantic_collection_pass_all_models() -> None:
    """Verify griffe and griffe-pydantic collect the entire cdm_data_loaders package without error."""
    griffe = pytest.importorskip("griffe")
    pytest.importorskip("griffe_pydantic")

    loader = griffe.GriffeLoader(
        extensions=griffe.load_extensions({"griffe_pydantic": {"schema": True}}),
        search_paths=[str(REPO_ROOT / "src")],
    )
    pkg = loader.load("cdm_data_loaders", try_relative_path=False)
    assert pkg.name == "cdm_data_loaders"


@pytest.mark.parametrize("spec_name", sorted(CONVERTER_SPECS))
def test_render_converter_spec_splits_spec_and_examples_with_schema_blocks(spec_name: str) -> None:
    """Converter specs split cases into Spec and Examples sections and embed schemas as code blocks in both."""
    cfg = CONVERTER_SPECS[spec_name]
    rendered = render_converter_spec(cfg)

    assert "## Cases" not in rendered
    assert "[Spec](#spec)" in rendered
    assert "[Examples](#examples)" in rendered
    assert "## Spec\n" in rendered
    assert "## Examples\n" in rendered

    before_spec, after_spec = rendered.split("## Spec\n", maxsplit=1)
    assert "## Fixture Index\n" in before_spec
    spec_cases_section, examples_section = after_spec.split("## Examples\n", maxsplit=1)
    assert spec_cases_section

    assert "#### Schema (`" in examples_section
    assert f"#### {cfg.expected_schema_heading} (" in examples_section
    assert "**Schema:**" not in examples_section
    assert f"**Expected {cfg.expected_schema_heading}:**" not in examples_section
    assert "```" in examples_section


@pytest.mark.parametrize(
    ("kind", "spec_name"),
    [
        *(pytest.param("converter", name, id=f"converter-{name}") for name in sorted(CONVERTER_SPECS)),
        pytest.param("pipeline", "xsv", id="pipeline-xsv"),
        pytest.param("pipeline", "xmltopyarrow", id="pipeline-xmltopyarrow"),
        pytest.param("pipeline", "jsonlines_jsonschema", id="pipeline-jsonlines_jsonschema"),
    ],
)
def test_render_error_section_rule_style_and_expandable_fixtures_pass(kind: str, spec_name: str) -> None:
    """Verify error sections render rule-style items with condition, reason, and expandable fixture codeblocks."""
    if kind == "converter":
        ctx_error = build_converter_spec_context(spec_name).error_section
        rendered = render_converter_spec(spec_name)
    else:
        ctx_error = build_pipeline_spec_context(spec_name).error_section
        assert ctx_error is not None
        rendered = render_pipeline_spec(spec_name)

    assert "| Code | Exception | Condition | Reason | Fixtures |" not in rendered
    assert "## Errors (Unsupported / Invalid Input)" in rendered

    for row in ctx_error.rows:
        assert f'<a id="{row.anchor}"></a>' in rendered
        assert f"- **{row.code}** (`{row.exception_name}`)" in rendered
        assert f"*Condition:* {row.title}" in rendered
        assert f"*Reason:* {row.reason}" in rendered
        assert "    *Fixtures:*" in rendered
        for fix in row.fixtures:
            assert f"    - [`{fix.name}`]({fix.url})" in rendered
            assert fix.description in rendered
            assert f'\n\n        ??? abstract "{fix.name}"\n\n            ' in rendered
            first_code_line = fix.code_block.content.splitlines()[0]
            assert f"            {first_code_line}" in rendered
            for param_key, param_repr in fix.param_pairs:
                assert f"`{param_key}={param_repr}`" in rendered


def test_render_error_section_boundary_cases_pass() -> None:
    """Verify partials/errors_section.md.j2 handles empty fixture lists and empty fixture descriptions."""
    template = create_jinja_env().get_template("partials/errors_section.md.j2")
    section = ErrorSectionContext(
        base_class_name="SampleConversionError",
        parent_type_name="ValueError",
        rows=(
            ErrorRowContext(
                code="E1",
                anchor="e1",
                exception_name="NoFixturesError",
                title="Missing fixture case",
                reason="Used to test empty fixture fallback.",
                fixtures=(),
            ),
            ErrorRowContext(
                code="E2",
                anchor="e2",
                exception_name="BareFixtureError",
                title="Fixture without description",
                reason="Used to test empty description and multiple param pairs.",
                fixtures=(
                    ErrorFixtureRefContext(
                        name="bad.xml",
                        url="https://example.org/bad.xml",
                        param_pairs=(("xml_tag", "'entry'"), ("table_name", "'entries'")),
                        description="",
                        code_block=CodeBlockContext(content="<root/>", lang="xml", fence="```"),
                    ),
                ),
            ),
        ),
    )
    rendered = template.render(error_section=section)
    assert "    *Fixtures:* —" in rendered
    assert (
        "    - [`bad.xml`](https://example.org/bad.xml) (`xml_tag='entry'`, `table_name='entries'`)\n\n"
        '        ??? abstract "bad.xml"\n\n'
        "            ```xml\n"
        "            <root/>\n"
        "            ```"
    ) in rendered
