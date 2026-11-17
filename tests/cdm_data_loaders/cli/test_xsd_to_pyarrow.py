"""Unit tests for the ``xsd_to_pyarrow`` CLI and bundled schema module synchronization."""

from pathlib import Path
from typing import Final

import pytest
from pydantic import ValidationError

import cdm_data_loaders.cli.xsd_to_pyarrow as xsd_to_pyarrow_cli_module
from cdm_data_loaders.cli.xsd_to_pyarrow import (
    CONFIGS as SCHEMA_CODEGEN_CONFIGS,
)
from cdm_data_loaders.cli.xsd_to_pyarrow import (
    SchemaCodegenConfig,
    render_schema_module,
    run_bundled_codegen,
)
from cdm_data_loaders.cli.xsd_to_pyarrow import (
    cli as xsd_to_pyarrow_cli,
)
from tests.helpers.paths_and_constants import BASE_DIR

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()
UNIREF_XSD: Final[Path] = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"


@pytest.mark.parametrize(
    "config",
    SCHEMA_CODEGEN_CONFIGS,
    ids=[c.output.parent.name for c in SCHEMA_CODEGEN_CONFIGS],
)
def test_precomputed_schema_modules_pass_match_generator_output(config: SchemaCodegenConfig) -> None:
    """Committed uniref, uniprot, and pubmed schema.py modules match xsd_to_pyarrow codegen output."""
    assert config.output.read_text(encoding="utf-8") == render_schema_module(config)


def test_run_bundled_codegen_pass_check_clean() -> None:
    """run_bundled_codegen returns zero when all committed schema modules are up to date."""
    assert run_bundled_codegen(check=True) == 0


@pytest.mark.parametrize(
    ("extra_args", "output_name", "expected_snippets"),
    [
        pytest.param(("--bundled", "--check"), None, (), id="bundled-check"),
        pytest.param(
            (),
            "uniref_level.py",
            ("ENTRY_STRUCT: Final[pa.StructType]",),
            id="custom-xsd-level",
        ),
        pytest.param(
            ("--xml-tag", "property"),
            "uniref_tag.py",
            ("PROPERTY_SCHEMA: Final[pa.Schema]", "SCHEMA: Final[pa.Schema] = PROPERTY_SCHEMA"),
            id="custom-xsd-xml-tag",
        ),
    ],
)
def test_xsd_to_pyarrow_cli_pass_invocations(
    tmp_path: Path,
    extra_args: tuple[str, ...],
    output_name: str | None,
    expected_snippets: tuple[str, ...],
) -> None:
    """xsd_to_pyarrow CLI supports --bundled --check, --xsd-file level mode, and --xml-tag mode."""
    if output_name is None:
        assert xsd_to_pyarrow_cli.run(cli_args=list(extra_args)) is None
        return

    output_file = tmp_path / output_name
    cli_args = ["--xsd-file", str(UNIREF_XSD), *extra_args, "--output", str(output_file)]
    xsd_to_pyarrow_cli.run(cli_args=cli_args)
    generated = output_file.read_text(encoding="utf-8")
    for snippet in expected_snippets:
        assert snippet in generated


def test_xsd_to_pyarrow_cli_fail_missing_output() -> None:
    """xsd_to_pyarrow CLI raises ValidationError when --xsd-file is given without --output."""
    with pytest.raises(ValidationError, match="--output"):
        xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(UNIREF_XSD)])


def test_xsd_to_pyarrow_cli_pass_bundled_stale_write_recheck_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """xsd_to_pyarrow CLI --bundled --check fails on stale files, writes on --bundled, and passes re-check."""
    stale_target = tmp_path / "configured_schema.py"
    temp_cfg = SchemaCodegenConfig(
        xsd_file=UNIREF_XSD,
        output=stale_target,
        docstring="Temporary configured schema.",
        xml_level=1,
    )
    monkeypatch.setattr(xsd_to_pyarrow_cli_module, "CONFIGS", (temp_cfg,))
    with pytest.raises(SystemExit) as exc_info:
        xsd_to_pyarrow_cli.run(cli_args=["--bundled", "--check"])
    assert exc_info.value.code == 1
    assert "stale:" in capsys.readouterr().err

    xsd_to_pyarrow_cli.run(cli_args=["--bundled"])
    assert stale_target.is_file()
    xsd_to_pyarrow_cli.run(cli_args=["--bundled", "--check"])
