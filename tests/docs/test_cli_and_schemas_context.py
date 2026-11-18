"""Tests for ``docs_macros.cli_context``, ``docs_macros.schemas_context``, and ``docs_macros.formatting``."""

from pathlib import Path
from typing import Final

import markdown
import pytest

from cdm_data_loaders.cli.xsd_to_pyarrow import check_or_write_files
from docs_macros.cli_context import load_cli_entries
from docs_macros.constants import (
    DOCS_DIR,
    GITHUB_BLOB_BASE,
    REPO_ROOT,
    SCHEMAS_SUBDIR,
    SITE_URL_BASE,
    SRC_ROOT,
)
from docs_macros.formatting import (
    code_fence_for_text,
    escape_table_cell,
    module_api_url,
    module_blob_url,
    repo_blob_url,
    slugify_anchor,
)
from docs_macros.render import render_bundled_schemas, render_cli_reference
from docs_macros.schemas_context import (
    discover_schema_assets,
    is_published_jsonschema,
    resolve_schema_file_metadata,
)
from docs_macros.sync import build_generated_docs_plan
from tests.docs.helpers import materialize_markdown_text

pytestmark = pytest.mark.docs

MIN_EXPECTED_CLIS: Final[int] = 16


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
    assert "| `str | None` |" in rendered
    assert "| `'file_listing' | 'iceberg' | 'jsonl' | 'parquet'` |" in rendered
    assert "| `'iceberg' | 'jsonl' | 'parquet'` |" in rendered
    assert r"`str \| None`" not in rendered

    rendered_html = markdown.markdown(rendered, extensions=["tables"])
    assert '<td style="text-align: left;"><code>str | None</code></td>' in rendered_html
    assert r"\|" not in rendered_html

    stub_text = build_generated_docs_plan()[Path("cli_reference.md")]
    assert "render_cli_reference()" in stub_text
    assert materialize_markdown_text(stub_text).strip() == rendered.strip()


def test_discover_and_render_bundled_schemas_and_published_jsonschemas_pass() -> None:
    """Verify schema asset discovery, inline collapsible rendering, no schema_source dir, and byte-equal published JSON."""
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
    """Verify is_published_jsonschema matches .json files under configured published subdirectories."""
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


def _populate_synthetic_schemas_pkg(tmp_path: Path) -> Path:
    """Create a synthetic package tree containing valid, hidden, cache, and Python schema files.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Synthetic package root directory path.
    :rtype: Path
    """
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
    return pkg_root


def test_escape_table_cell_escapes_pipes_and_newlines() -> None:
    """Verify escape_table_cell escapes pipe characters and replaces newlines with spaces."""
    assert escape_table_cell("a | b\nc") == r"a \| b c"


def test_slugify_anchor_normalizes_prefix_and_path() -> None:
    """Verify slugify_anchor joins and normalizes prefix and path segments into a hyphenated anchor."""
    assert slugify_anchor("schema", "xsv_config/0.1.0/schema.json") == "schema-xsv-config-0-1-0-schema-json"


@pytest.mark.parametrize(
    ("populate_schemas", "expected_rel_paths"),
    [
        pytest.param(False, [], id="missing-schemas-dir"),
        pytest.param(
            True,
            [
                "schemas/domain/0.1.0/spec.yaml",
                "schemas/root_notes.txt",
                "schemas/xsv_config/0.1.0/schema.json",
            ],
            id="filters-hidden-pycache-and-py",
        ),
    ],
)
def test_discover_schema_assets_edge_cases(
    tmp_path: Path,
    populate_schemas: bool,
    expected_rel_paths: list[str],
) -> None:
    """Verify discover_schema_assets handles missing schemas directories and filters non-asset files."""
    if populate_schemas:
        src_root = _populate_synthetic_schemas_pkg(tmp_path)
    else:
        src_root = tmp_path / "empty_pkg"
        src_root.mkdir()

    discovered = discover_schema_assets(src_root, repo_root=tmp_path)
    assert [a.rel_path.as_posix() for a in discovered] == expected_rel_paths
    if populate_schemas:
        assert discovered[1].code_block.fence == "`````"
        assert discovered[2].published_rel_path == Path("schemas/xsv_config/0.1.0/schema.json")


def test_render_bundled_schemas_expands_code_fence_and_published_url(tmp_path: Path) -> None:
    """Verify render_bundled_schemas expands code fences for nested backticks and emits published schema URLs."""
    pkg_root = _populate_synthetic_schemas_pkg(tmp_path)
    discovered = discover_schema_assets(pkg_root, repo_root=tmp_path)
    rendered = render_bundled_schemas(discovered, src_root=pkg_root, repo_root=tmp_path)
    assert '`````text linenums="1"' in rendered
    assert f"{SITE_URL_BASE}/schemas/xsv_config/0.1.0/schema.json" in rendered


def test_check_or_write_files_stale_write_and_clean_sequence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify check_or_write_files reports stale files, writes updates, and passes a clean check."""
    target_file = tmp_path / "generated.py"
    assert check_or_write_files([(target_file, "x = 1\n")], check=True, repo_root=tmp_path) == 1
    assert "stale:" in capsys.readouterr().err
    assert check_or_write_files([(target_file, "x = 1\n")], check=False, repo_root=tmp_path) == 0
    assert check_or_write_files([(target_file, "x = 1\n")], check=True, repo_root=tmp_path) == 0


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
