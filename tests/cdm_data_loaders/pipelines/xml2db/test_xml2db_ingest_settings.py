"""Tests for Xml2DbSettings field validation and defaults."""

import shutil
from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.core.fields import DEFAULTS, S3
from cdm_data_loaders.pipelines.core import resolve_cts_settings
from cdm_data_loaders.pipelines.xml2db.settings import (
    DEFAULT_XML2DB_CHUNK_SIZE,
    DEFAULT_XML2DB_SHORT_NAME,
    PIPELINE_NAME,
    Xml2DbSettings,
)

MINIMAL_SETTINGS_KWARGS: dict[str, object] = {
    "buffer_size": 100,
    "log_interval": 1000,
    "dataset_name": "xml2db_test_dataset",
    "xsd_file": "uniref.xsd",
    "input_dir": "/input_dir",
    "output_dir": "/output_dir",
    "log_config_file": None,
    "dlt_dev_mode": False,
    "use_destination": "local_fs",
    "use_output_dir_for_pipeline_metadata": False,
}


def _write_xsd_fixture(input_dir: Path, test_data_dir: Path, filename: str) -> str:
    """Copy an XSD fixture from tests/data into input_dir; return its relative filename."""
    input_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(test_data_dir / "uniprot" / "uniref" / filename, input_dir / filename)
    return filename


@pytest.fixture
def settings_factory(tmp_path: Path, test_data_dir: Path) -> Callable[..., Xml2DbSettings]:
    """Return a factory that builds a valid Xml2DbSettings with fields open to override."""

    def _factory(**overrides: object) -> Xml2DbSettings:
        log_config_file = tmp_path / "logging.json"
        log_config_file.write_text('{"version": 1}', encoding="utf-8")
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)
        xsd_file = _write_xsd_fixture(input_dir, test_data_dir, "uniref.xsd")

        kwargs: dict[str, object] = {
            **MINIMAL_SETTINGS_KWARGS,
            "log_config_file": str(log_config_file),
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "xsd_file": str(input_dir / xsd_file),
        }
        kwargs.update(overrides)
        return Xml2DbSettings(**kwargs)

    return _factory


def test_xml2db_ingest_settings_pass_defaults(settings_factory: Callable[..., Xml2DbSettings]) -> None:
    """chunk_element_tag, xml2db_config_file default to None; the other defaults are set."""
    settings = settings_factory()
    assert settings.chunk_element_tag is None
    assert settings.xml2db_config_file is None
    assert settings.chunk_size == DEFAULT_XML2DB_CHUNK_SIZE
    assert settings.skip_xml_validation is True
    assert settings.short_name == DEFAULT_XML2DB_SHORT_NAME
    assert settings.file_glob == DEFAULTS["file_glob"] or settings.file_glob == "*.xml*"


def test_xml2db_ingest_settings_pass_common_cts_defaults(settings_factory: Callable[..., Xml2DbSettings]) -> None:
    """buffer_size and log_interval default to the common CTS defaults."""
    settings = settings_factory()
    assert settings.buffer_size == DEFAULTS["buffer_size"]
    assert settings.log_interval == DEFAULTS["log_interval"]


def test_xml2db_ingest_settings_pass_custom_chunk_settings(settings_factory: Callable[..., Xml2DbSettings]) -> None:
    """chunk_element_tag and chunk_size accept custom values."""
    custom_chunk_size = 5
    settings = settings_factory(chunk_element_tag="entry", chunk_size=custom_chunk_size)
    assert settings.chunk_element_tag == "entry"
    assert settings.chunk_size == custom_chunk_size


def test_xml2db_ingest_settings_pass_namespaced_chunk_element_tag(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """chunk_element_tag accepts a namespaced (Clark notation) tag."""
    settings = settings_factory(chunk_element_tag="{http://uniprot.org/uniref}entry")
    assert settings.chunk_element_tag == "{http://uniprot.org/uniref}entry"


def test_xml2db_ingest_settings_pass_empty_chunk_element_tag_rejected(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """An empty chunk_element_tag raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(chunk_element_tag="")


def test_xml2db_ingest_settings_fail_non_positive_chunk_size(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """chunk_size must be a positive integer."""
    with pytest.raises(ValidationError):
        settings_factory(chunk_size=0)


def test_xml2db_ingest_settings_fail_non_positive_buffer_size(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """buffer_size must be a positive integer."""
    with pytest.raises(ValidationError):
        settings_factory(buffer_size=0)


def test_xml2db_ingest_settings_fail_non_positive_log_interval(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """log_interval must be a positive integer."""
    with pytest.raises(ValidationError):
        settings_factory(log_interval=0)


@pytest.mark.parametrize(
    ("removed_field", "value"),
    [("loader_file_format", "parquet"), ("compact_reused_tables", True)],
    ids=["loader_file_format", "compact_reused_tables"],
)
def test_xml2db_ingest_settings_fail_removed_field_rejected(
    settings_factory: Callable[..., Xml2DbSettings], removed_field: str, value: object
) -> None:
    """Fields made obsolete by the iceberg output (always parquet, merged on write) raise ValidationError."""
    with pytest.raises(ValidationError, match=removed_field):
        settings_factory(**{removed_field: value})


def test_xml2db_ingest_settings_fail_missing_dataset_name(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """Omitting dataset_name raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(dataset_name=None)


def test_xml2db_ingest_settings_fail_missing_xsd_file() -> None:
    """Omitting xsd_file raises ValidationError."""
    with pytest.raises(ValidationError):
        Xml2DbSettings(**{**MINIMAL_SETTINGS_KWARGS, "xsd_file": None})


def test_xml2db_ingest_settings_fail_xsd_file_not_found(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """A supplied xsd_file that does not exist raises ValidationError naming the file."""
    with pytest.raises(ValidationError, match="File does not exist"):
        settings_factory(xsd_file="does_not_exist.xsd")


def test_xml2db_ingest_settings_fail_xml2db_config_file_not_found(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """A supplied xml2db_config_file that does not exist raises ValidationError naming the file."""
    with pytest.raises(ValidationError, match="File does not exist"):
        settings_factory(xml2db_config_file="does_not_exist.yaml")


def test_xml2db_ingest_settings_pass_xml2db_config_file_accepted(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """A xml2db_config_file that exists is accepted and preserved."""
    config_path = settings_factory().xsd_file
    settings = settings_factory(xml2db_config_file=config_path)
    assert settings.xml2db_config_file == config_path


def test_xml2db_ingest_settings_pass_local_destination_with_s3_output_dir_rejected(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """A local_fs destination with an s3:// output_dir raises ValueError at resolution.

    The check lives in resolve_output_dir and runs when run_cli resolves the
    settings, so constructing them still succeeds.
    """
    settings = settings_factory(output_dir="s3://some-bucket/path", use_destination="local_fs")
    with pytest.raises(ValueError, match="uses protocol 's3', but destination 'local_fs' is configured for 'file'"):
        resolve_cts_settings(settings, {"destination": {"local_fs": {"bucket_url": "/out"}}})


def test_xml2db_ingest_settings_pass_s3_destination_with_local_output_dir_rejected(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """An s3 destination with a local output_dir raises ValueError at resolution."""
    settings = settings_factory(output_dir="/local/out", use_destination=S3)
    with pytest.raises(ValueError, match="uses protocol 'file', but destination 's3' is configured for 's3'"):
        resolve_cts_settings(settings, {"destination": {"s3": {"bucket_url": "s3://bucket/prefix"}}})


def test_xml2db_ingest_settings_pass_unknown_use_destination_rejected(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """use_destination not present in the dlt config raises ValueError at resolution."""
    settings = settings_factory(use_destination="not_a_destination")
    with pytest.raises(ValueError, match="use_destination must be one of"):
        resolve_cts_settings(settings, {"destination": {"local_fs": {"bucket_url": "/out"}}})


def test_xml2db_ingest_settings_pass_pipeline_name_constant() -> None:
    """PIPELINE_NAME is the CLI program name and the dlt pipeline name."""
    assert PIPELINE_NAME == "xml2db_ingest"
    assert Xml2DbSettings.model_config.get("cli_prog_name") == PIPELINE_NAME


def test_xml2db_ingest_settings_pass_cli_shortcut_g_targets_file_glob(tmp_path: Path, test_data_dir: Path) -> None:
    """The '-g' shortcut added on top of the shared shortcuts parses as file_glob."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    xsd_file = _write_xsd_fixture(input_dir, test_data_dir, "uniref.xsd")
    log_config_file = tmp_path / "logging.json"
    log_config_file.write_text('{"version": 1}', encoding="utf-8")

    settings = CliApp.run(
        Xml2DbSettings,
        cli_args=[
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--log-config-file",
            str(log_config_file),
            "--dataset-name",
            "ds",
            "--xsd-file",
            str(input_dir / xsd_file),
            "-g",
            "*.gz",
        ],
    )
    assert settings.file_glob == "*.gz"
