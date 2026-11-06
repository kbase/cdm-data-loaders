"""Tests for ``scripts/gen_docs.py`` and documentation drift verification across ``docs/`` and ``README.md``."""

import ast
import importlib
import re
from collections.abc import Sequence
from pathlib import Path

import pytest
import yaml
from pydantic import AliasChoices

from cdm_data_loaders.converters.jsonschema_to_pyarrow import coerce_record_for_pyarrow
from cdm_data_loaders.core.settings import CdmDataLoadersBase
from cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline import (
    run_jsonlines_with_validation_pipeline,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from scripts.gen_docs import (
    CLI_REFERENCE_PATH,
    REPO_ROOT,
    CliEntryDoc,
    discover_api_modules,
    discover_schema_assets,
    load_cli_entries,
    main,
    render_cli_reference,
    write_mkdocs_api_tree,
)
from tests.cdm_data_loaders.pipelines.reference.pipeline import ReferencePipelineSettings

pytestmark = pytest.mark.docs

MIN_EXPECTED_CLIS = 16
DOCS_DIR = REPO_ROOT / "docs"
MKDOCS_PATH = REPO_ROOT / "mkdocs.yml"
INDEX_PATH = DOCS_DIR / "index.md"
README_PATH = REPO_ROOT / "README.md"
DEFAULT_MARKDOWN_PATHS: tuple[Path, ...] = (README_PATH, *sorted(DOCS_DIR.glob("*.md")))

PYTHON_FENCE_RE = re.compile(r"```(?:python|py)\n(.*?)```", re.DOTALL)
SHELL_FENCE_RE = re.compile(r"```(?:bash|sh|shell|console)\n(.*?)```", re.DOTALL)
CDL_ENV_VAR_RE = re.compile(r"\bCDL_[A-Z0-9_]+\b")
INDEX_MD_LINK_RE = re.compile(r"\]\(([a-zA-Z0-9_-]+\.md)(?:#[^)]*)?\)")
README_DOCS_LINK_RE = re.compile(r"\]\(docs/([a-zA-Z0-9_-]+\.md)(?:#[^)]*)?\)")
SHELL_STOP_TOKENS = frozenset({"|", "||", "&&", ";", ">", ">>", "<"})


class _MkDocsYamlLoader(yaml.SafeLoader):
    """YAML SafeLoader that ignores MkDocs ``!!python/name:`` extension tags."""


_MkDocsYamlLoader.add_multi_constructor(
    "tag:yaml.org,2002:python/",
    lambda _loader, _suffix, _node: None,
)


def _collect_nav_md_entries(nav_node: object) -> set[str]:
    """Recursively collect ``.md`` file references from a parsed ``mkdocs.yml`` ``nav`` structure."""
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


def check_mkdocs_nav_and_index(
    docs_dir: Path = DOCS_DIR,
    mkdocs_path: Path = MKDOCS_PATH,
    index_path: Path = INDEX_PATH,
    readme_path: Path = README_PATH,
) -> list[str]:
    """Verify every ``docs/*.md`` file is registered in ``mkdocs.yml``, ``docs/index.md``, and ``README.md``."""
    mkdocs_data = yaml.load(mkdocs_path.read_text(encoding="utf-8"), Loader=_MkDocsYamlLoader) or {}  # noqa: S506
    raw_exclude = mkdocs_data.get("exclude_docs", "") or ""
    excluded_names = {line.strip() for line in str(raw_exclude).splitlines() if line.strip()}

    docs_files = {p.name for p in docs_dir.glob("*.md") if p.name not in excluded_names}
    nav_entries = _collect_nav_md_entries(mkdocs_data.get("nav", []))

    non_index_docs = docs_files - {"index.md"}
    index_links = set(INDEX_MD_LINK_RE.findall(index_path.read_text(encoding="utf-8")))
    readme_links = set(README_DOCS_LINK_RE.findall(readme_path.read_text(encoding="utf-8")))

    return [
        *(f"docs/{name} is missing from mkdocs.yml nav" for name in sorted(docs_files - nav_entries)),
        *(f"mkdocs.yml nav entry '{name}' does not exist in docs/" for name in sorted(nav_entries - docs_files)),
        *(f"docs/{name} is not linked in docs/index.md" for name in sorted(non_index_docs - index_links)),
        *(f"docs/index.md links to non-existent 'docs/{name}'" for name in sorted(index_links - docs_files)),
        *(f"docs/{name} is not linked in README.md" for name in sorted(non_index_docs - readme_links)),
        *(f"README.md links to non-existent 'docs/{name}'" for name in sorted(readme_links - docs_files)),
    ]


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
    md_paths: Sequence[Path] = DEFAULT_MARKDOWN_PATHS,
    settings_classes: dict[str, type[CdmDataLoadersBase]] | None = None,
) -> list[str]:
    """Verify ``cdm_data_loaders`` imports and settings class kwargs inside Markdown Python code blocks."""
    resolved_settings = settings_classes if settings_classes is not None else _discover_settings_classes()
    settings_kwargs_map = {name: _valid_settings_kwargs(cls) for name, cls in resolved_settings.items()}
    errors: list[str] = []

    for md_path in md_paths:
        text = md_path.read_text(encoding="utf-8")
        label = md_path.relative_to(REPO_ROOT).as_posix() if md_path.is_relative_to(REPO_ROOT) else md_path.name
        for match in PYTHON_FENCE_RE.finditer(text):
            block = match.group(1)
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
    settings_classes: dict[str, type[CdmDataLoadersBase]],
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
    md_paths: Sequence[Path] = DEFAULT_MARKDOWN_PATHS,
    cli_entries: Sequence[CliEntryDoc] | None = None,
    settings_classes: dict[str, type[CdmDataLoadersBase]] | None = None,
) -> list[str]:
    """Verify CLI flags in shell code blocks and ``CDL_*`` environment variables across Markdown docs."""
    resolved_entries = list(cli_entries) if cli_entries is not None else load_cli_entries()
    resolved_settings = settings_classes if settings_classes is not None else _discover_settings_classes()
    cli_flags_map, valid_env_vars = _build_valid_cli_flags_and_env_vars(resolved_entries, resolved_settings)
    cli_names = set(cli_flags_map)
    errors: list[str] = []

    for md_path in md_paths:
        text = md_path.read_text(encoding="utf-8")
        label = md_path.relative_to(REPO_ROOT).as_posix() if md_path.is_relative_to(REPO_ROOT) else md_path.name

        for match in SHELL_FENCE_RE.finditer(text):
            block = match.group(1)
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


def test_load_cli_entries_and_render_cli_reference_pass() -> None:
    """Verify all registered pipeline CLIs are introspected and docs/cli_reference.md is up to date."""
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
    assert CLI_REFERENCE_PATH.exists()
    assert CLI_REFERENCE_PATH.read_text(encoding="utf-8") == rendered
    assert main(["--check"]) == 0


def test_main_check_fail_when_output_stale(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify main --check returns 1 when the target file is missing or stale, and 0 after regeneration."""
    out_file = tmp_path / "cli_reference.md"
    assert main(["--check", "--output", str(out_file)]) == 1
    captured = capsys.readouterr()
    assert "stale:" in captured.err

    assert main(["--output", str(out_file)]) == 0
    assert out_file.exists()
    assert main(["--check", "--output", str(out_file)]) == 0


def test_discover_and_write_mkdocs_api_tree_pass(tmp_path: Path) -> None:
    """Verify module discovery, schema asset discovery, and MkDocs API tree generation."""
    modules = discover_api_modules()
    module_paths = {mod.module_path for mod in modules}
    assert "cdm_data_loaders" in module_paths
    assert "cdm_data_loaders.core.settings" in module_paths
    assert "cdm_data_loaders.pipelines.core" in module_paths

    assets = discover_schema_assets()
    asset_paths = {asset.rel_path.as_posix() for asset in assets}
    assert "schemas/uniprot/uniprot.xsd" in asset_paths
    assert "schemas/uniref/uniref.xsd" in asset_paths

    written = write_mkdocs_api_tree(output_dir=tmp_path)
    assert Path("api/SUMMARY.md") in written
    assert Path("api/index.md") in written
    assert Path("api/schemas.md") in written
    assert (tmp_path / "api" / "SUMMARY.md").exists()
    assert "::: cdm_data_loaders.core.settings" in (tmp_path / "api" / "core" / "settings.md").read_text(
        encoding="utf-8"
    )


def test_write_mkdocs_api_tree_fail_missing_writer_and_output_dir() -> None:
    """Verify write_mkdocs_api_tree raises ValueError if neither writer nor output_dir is supplied."""
    with pytest.raises(ValueError, match="Either writer or output_dir must be provided"):
        write_mkdocs_api_tree(writer=None, output_dir=None)


def test_check_mkdocs_nav_and_index_pass() -> None:
    """Verify every docs/*.md file is indexed in mkdocs.yml nav, docs/index.md, and README.md."""
    assert check_mkdocs_nav_and_index() == []


@pytest.mark.parametrize(
    ("extra_doc_name", "mkdocs_text", "index_text", "readme_text", "expected_errors"),
    [
        pytest.param(
            "orphan_spec.md",
            "nav:\n  - Home: index.md\n  - Guide: guide.md\n",
            "[Guide](guide.md)\n",
            "[Guide](docs/guide.md)\n",
            [
                "docs/orphan_spec.md is missing from mkdocs.yml nav",
                "docs/orphan_spec.md is not linked in docs/index.md",
                "docs/orphan_spec.md is not linked in README.md",
            ],
            id="unindexed_doc_file_caught_across_nav_index_and_readme",
        ),
        pytest.param(
            None,
            "nav:\n  - Home: index.md\n  - Guide: guide.md\n  - Ghost: ghost.md\n",
            "[Guide](guide.md)\n[Missing](missing_index.md)\n",
            "[Guide](docs/guide.md)\n[Missing](docs/missing_readme.md)\n",
            [
                "mkdocs.yml nav entry 'ghost.md' does not exist in docs/",
                "docs/index.md links to non-existent 'docs/missing_index.md'",
                "README.md links to non-existent 'docs/missing_readme.md'",
            ],
            id="nonexistent_targets_caught_in_nav_index_and_readme",
        ),
        pytest.param(
            "implementation_plan.md",
            "exclude_docs: |\n  implementation_plan.md\nnav:\n  - Home: index.md\n  - Guide: guide.md\n",
            "[Guide](guide.md)\n",
            "[Guide](docs/guide.md)\n",
            [],
            id="exclude_docs_file_ignored",
        ),
    ],
)
def test_check_mkdocs_nav_and_index_fail_detects_drift(
    tmp_path: Path,
    extra_doc_name: str | None,
    mkdocs_text: str,
    index_text: str,
    readme_text: str,
    expected_errors: list[str],
) -> None:
    """Verify check_mkdocs_nav_and_index detects orphaned docs and broken links while honoring exclude_docs."""
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    index_path = docs_dir / "index.md"
    index_path.write_text(index_text, encoding="utf-8")
    (docs_dir / "guide.md").write_text("# Guide\n", encoding="utf-8")
    if extra_doc_name is not None:
        (docs_dir / extra_doc_name).write_text("# Extra\n", encoding="utf-8")

    mkdocs_path = tmp_path / "mkdocs.yml"
    mkdocs_path.write_text(mkdocs_text, encoding="utf-8")
    readme_path = tmp_path / "README.md"
    readme_path.write_text(readme_text, encoding="utf-8")

    assert check_mkdocs_nav_and_index(docs_dir, mkdocs_path, index_path, readme_path) == expected_errors


def test_check_markdown_python_blocks_pass() -> None:
    """Verify all Python code blocks in docs/ and README.md import valid symbols and settings fields."""
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
            "```python\nfrom cdm_data_loaders.converters.jsonschema_to_pyspark.converter import JSONSchemaToPySpark\n```\n",
            "cannot import module 'cdm_data_loaders.converters.jsonschema_to_pyspark.converter'",
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
    """Verify all CLI invocations and CDL_* environment variables in docs/ and README.md are valid."""
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
