"""Tests for XmlToDictIngestSettings field validation and defaults."""

from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import ValidationError

from cdm_data_loaders.core.fields import DEFAULTS
from cdm_data_loaders.pipelines.xml_to_dict_ingest import XmlToDictIngestSettings


def test_xml_to_dict_ingest_settings_pass_defaults(settings_factory: Callable[..., XmlToDictIngestSettings]) -> None:
    """file_glob defaults to '*.xml*'. buffer_size and log_interval default to the common CTS defaults."""
    settings = settings_factory()
    assert settings.file_glob == "*.xml*"
    assert settings.buffer_size == DEFAULTS["buffer_size"]
    assert settings.log_interval == DEFAULTS["log_interval"]


def test_xml_to_dict_ingest_settings_pass_custom_file_glob(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """file_glob accepts a custom glob pattern."""
    assert settings_factory(file_glob="*.rdf").file_glob == "*.rdf"


def test_xml_to_dict_ingest_settings_pass_custom_xml_tag(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """xml_tag accepts any tag name."""
    settings = settings_factory(xml_tag="entry")
    assert settings.xml_tag == "entry"


def test_xml_to_dict_ingest_settings_fail_missing_xml_tag(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """Omitting xml_tag raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(xml_tag=None)


def test_xml_to_dict_ingest_settings_fail_missing_table_name(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """Omitting table_name raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(table_name=None)


def test_xml_to_dict_ingest_settings_fail_missing_dataset_name(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """Omitting dataset_name raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(dataset_name=None)


def test_xml_to_dict_ingest_settings_fail_non_positive_buffer_size(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """buffer_size must be a positive integer."""
    with pytest.raises(ValidationError):
        settings_factory(buffer_size=0)


def test_xml_to_dict_ingest_settings_fail_non_positive_log_interval(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """log_interval must be a positive integer."""
    with pytest.raises(ValidationError):
        settings_factory(log_interval=0)


def test_xml_to_dict_ingest_settings_fail_local_destination_with_s3_output_dir(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """use_destination='local_fs' with an s3:// output_dir raises ValidationError.

    This check comes from CtsSettings. It is tested here because a
    regression would silently break where this pipeline writes its output.
    """
    with pytest.raises(ValidationError, match="Mismatch between output location and use_destination"):
        settings_factory(output_dir="s3://some-bucket/path")


def test_xml_to_dict_ingest_settings_fail_local_destination_with_s3_flag(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """use_destination='s3' with a local output_dir raises ValidationError."""
    with pytest.raises(ValidationError, match="Mismatch between output location and use_destination"):
        settings_factory(use_destination="s3")


def test_xml_to_dict_ingest_settings_pass_unknown_use_destination_rejected(
    settings_factory: Callable[..., XmlToDictIngestSettings],
) -> None:
    """use_destination not present in the dlt config raises ValidationError."""
    with pytest.raises(ValidationError, match="use_destination must be one of"):
        settings_factory(use_destination="not_a_destination")


def test_xml_to_dict_ingest_settings_pass_xml_tag_shortcut(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The file_glob field is exposed as both --file-glob and -g on the command line."""
    log_config_file = tmp_path / "logging.json"
    log_config_file.write_text('{"version": 1}')
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    argv = [
        "xml_to_dict_ingest",
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        "--log-config-file",
        str(log_config_file),
        "-g",
        "*.xml.gz",
        "--dataset-name",
        "cli_dataset",
        "--table-name",
        "entry",
        "--xml-tag",
        "entry",
    ]
    monkeypatch.setattr("sys.argv", argv)

    settings = XmlToDictIngestSettings()
    assert settings.file_glob == "*.xml.gz"
    assert settings.xml_tag == "entry"
