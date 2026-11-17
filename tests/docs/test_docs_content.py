"""Tests for documentation content, navigation parity, links, code blocks, CLI flags, and API docstrings."""

import ast
import importlib
import re
import textwrap
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

import pytest
from pydantic import AliasChoices

from cdm_data_loaders.coercion.json_to_arrow import coerce_json_record_for_pyarrow
from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.core.settings import CdmDataLoadersBase
from cdm_data_loaders.pipelines.jsonlines.pydantic import (
    run_jsonlines_with_validation_pipeline,
)
from docs_macros.cli_context import (
    CliEntryDoc,
    discover_api_modules,
    load_cli_entries,
)
from docs_macros.constants import (
    DOCS_DIR,
    GITHUB_BLOB_BASE,
    REPO_ROOT,
    SITE_URL_BASE,
)
from docs_macros.env import build_generated_docs_plan
from tests.cdm_data_loaders.pipelines.reference.pipeline import ReferencePipelineSettings
from tests.docs.helpers import (
    INDEX_PATH,
    README_PATH,
    ZENSICAL_PATH,
    excluded_doc_names,
    resolve_markdown_sources,
)

pytestmark = pytest.mark.docs

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


def _collect_nav_md_entries(nav_node: object) -> set[str]:
    """Collect ``.md`` file references from a parsed ``zensical.toml`` ``[project].nav`` structure.

    :param nav_node: Nested ``nav`` value parsed from ``zensical.toml``.
    :type nav_node: object
    :return: Set of Markdown filenames referenced anywhere in ``nav_node``.
    :rtype: set[str]
    """
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
    for label, raw_text in resolve_markdown_sources(md_paths):
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
    """Verify every repository ``blob/develop`` link in hand-written and rendered docs resolves to a real file.

    :param md_paths: Optional explicit Markdown paths to check (defaults to the full materialized corpus).
    :type md_paths: Sequence[Path] | None
    :param repo_root: Repository root directory against which relative link paths are resolved.
    :type repo_root: Path
    :return: List of broken or wrong-branch link messages (empty when valid).
    :rtype: list[str]
    """
    errors: list[str] = []
    for label, raw_text in resolve_markdown_sources(md_paths, repo_root=repo_root):
        stripped = FENCED_BLOCK_RE.sub("", raw_text)
        for branch, rel_target in sorted(set(GITHUB_BLOB_LINK_RE.findall(stripped))):
            if branch != "develop":
                errors.append(f"{label}: GitHub blob link uses branch '{branch}' instead of 'develop' ('{rel_target}')")
            if not (repo_root / rel_target).exists():
                errors.append(f"{label}: GitHub blob link target '{rel_target}' does not exist in repository")
    return errors


def _discover_settings_classes() -> dict[str, type[CdmDataLoadersBase]]:
    """Import all package modules and return all ``CdmDataLoadersBase`` subclasses keyed by class name.

    :return: Mapping of settings class names to their ``CdmDataLoadersBase`` subclasses.
    :rtype: dict[str, type[CdmDataLoadersBase]]
    """
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
    """Return all valid keyword argument names (field names and string validation aliases) for ``settings_cls``.

    :param settings_cls: Settings class to inspect.
    :type settings_cls: type[CdmDataLoadersBase]
    :return: Set of accepted field names and string aliases.
    :rtype: set[str]
    """
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
    """Verify a single ``from cdm_data_loaders... import ...`` AST node resolves to real attributes.

    :param node: Parsed ``ast.ImportFrom`` node.
    :type node: ast.ImportFrom
    :param label: Relative Markdown file label for error reporting.
    :type label: str
    :param block_line: Starting 1-based line number of the fenced Python block.
    :type block_line: int
    :return: List of import failure messages for ``node``.
    :rtype: list[str]
    """
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
    """Verify ``cdm_data_loaders`` imports and settings kwargs inside hand-written and rendered Python blocks.

    :param md_paths: Optional explicit Markdown paths to check (defaults to the full materialized corpus).
    :type md_paths: Sequence[Path] | None
    :param settings_classes: Optional pre-discovered mapping of settings class names to classes.
    :type settings_classes: Mapping[str, type[CdmDataLoadersBase]] | None
    :return: List of syntax, import, or settings kwarg error messages.
    :rtype: list[str]
    """
    resolved_settings = settings_classes if settings_classes is not None else _discover_settings_classes()
    settings_kwargs_map = {name: _valid_settings_kwargs(cls) for name, cls in resolved_settings.items()}
    errors: list[str] = []

    for label, text in resolve_markdown_sources(md_paths):
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
    """Build per-CLI valid flag sets and the global set of valid ``CDL_*`` environment variables.

    :param cli_entries: Introspected CLI entry documentation models.
    :type cli_entries: Sequence[CliEntryDoc]
    :param settings_classes: Mapping of discovered ``CdmDataLoadersBase`` subclasses.
    :type settings_classes: Mapping[str, type[CdmDataLoadersBase]]
    :return: Pair of ``(cli_flags_by_name, valid_env_vars)``.
    :rtype: tuple[dict[str, set[str]], set[str]]
    """
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
    """Extract ``(cli_name, flags)`` if ``tokens`` invokes a registered CLI via ``uv run`` or entrypoint.

    :param tokens: Whitespace-split shell command tokens.
    :type tokens: list[str]
    :param cli_names: Set of registered project CLI entrypoint names.
    :type cli_names: set[str]
    :return: Pair of ``(cli_name, flag_tokens)`` if matched, otherwise ``None``.
    :rtype: tuple[str, list[str]] | None
    """
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
    """Verify CLI flags in shell code blocks and ``CDL_*`` environment variables across hand-written and rendered docs.

    :param md_paths: Optional explicit Markdown paths to check (defaults to the full materialized corpus).
    :type md_paths: Sequence[Path] | None
    :param cli_entries: Optional pre-loaded CLI entry documentation models.
    :type cli_entries: Sequence[CliEntryDoc] | None
    :param settings_classes: Optional pre-discovered mapping of settings class names to classes.
    :type settings_classes: Mapping[str, type[CdmDataLoadersBase]] | None
    :return: List of invalid CLI flag or unknown environment variable error messages.
    :rtype: list[str]
    """
    resolved_entries = list(cli_entries) if cli_entries is not None else load_cli_entries()
    resolved_settings = settings_classes if settings_classes is not None else _discover_settings_classes()
    cli_flags_map, valid_env_vars = _build_valid_cli_flags_and_env_vars(resolved_entries, resolved_settings)
    cli_names = set(cli_flags_map)
    errors: list[str] = []

    for label, text in resolve_markdown_sources(md_paths):
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


def test_zensical_nav_sections_and_standalone_guide_links_pass() -> None:
    """Verify zensical.toml nav structure, replaced guide files, and blob/develop links in standalone docs."""
    zensical_data = tomllib.loads(ZENSICAL_PATH.read_text(encoding="utf-8"))
    nav = zensical_data["project"]["nav"]
    top_level_sections = [next(iter(entry.keys())) for entry in nav]
    assert top_level_sections == list(EXPECTED_NAV_SECTIONS)

    assert not (DOCS_DIR / "converters_ir.md").exists()
    assert (DOCS_DIR / "schema_converters.md").is_file()
    assert (DOCS_DIR / "development_and_testing.md").is_file()

    excluded = excluded_doc_names()
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
            coerce_json_record_for_pyarrow.__doc__,
            "ArrowListBuffer",
            None,
            id="coerce_json_record_for_pyarrow_docstring",
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
