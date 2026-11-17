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
from tests._support.paths_and_constants import BASE_DIR

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()


@pytest.mark.parametrize(
    "config",
    SCHEMA_CODEGEN_CONFIGS,
    ids=[c.output.parent.name for c in SCHEMA_CODEGEN_CONFIGS],
)
def test_precomputed_schema_modules_pass_match_generator_output(config: SchemaCodegenConfig) -> None:
    """Committed uniref, uniprot, and pubmed schema.py modules match xsd_to_pyarrow codegen output."""
    assert config.output.read_text(encoding="utf-8") == render_schema_module(config)


def test_gen_pyarrow_schema_cli_pass_and_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """xsd_to_pyarrow CLI supports custom CLI args, --bundled, --check, and default write mode."""
    assert run_bundled_codegen(check=True) == 0
    xsd_to_pyarrow_cli.run(cli_args=["--bundled", "--check"])

    uniref_xsd = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"
    level_out = tmp_path / "uniref_level.py"
    xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(uniref_xsd), "--output", str(level_out)])
    assert "ENTRY_STRUCT: Final[pa.StructType]" in level_out.read_text(encoding="utf-8")

    tag_out = tmp_path / "uniref_tag.py"
    xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(uniref_xsd), "--xml-tag", "property", "--output", str(tag_out)])
    assert "PROPERTY_SCHEMA: Final[pa.Schema]" in tag_out.read_text(encoding="utf-8")
    assert "SCHEMA: Final[pa.Schema] = PROPERTY_SCHEMA" in tag_out.read_text(encoding="utf-8")

    with pytest.raises(ValidationError):
        xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(uniref_xsd)])

    stale_target = tmp_path / "configured_schema.py"
    temp_cfg = SchemaCodegenConfig(
        xsd_file=uniref_xsd,
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
