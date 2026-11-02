"""Tests for ``scripts/gen_docs.py`` CLI reference and MkDocs API tree generation."""

from pathlib import Path

import pytest

from scripts.gen_docs import (
    CLI_REFERENCE_PATH,
    discover_api_modules,
    discover_schema_assets,
    load_cli_entries,
    main,
    render_cli_reference,
    write_mkdocs_api_tree,
)

MIN_EXPECTED_CLIS = 16


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
