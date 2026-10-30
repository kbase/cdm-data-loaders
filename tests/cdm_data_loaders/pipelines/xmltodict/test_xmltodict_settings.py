"""Tests for XmlToDictSettings field validation and defaults."""

from collections.abc import Callable

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.core.fields import DEFAULTS
from cdm_data_loaders.pipelines.xmltodict.settings import PIPELINE_NAME, XmlToDictSettings
from tests.helpers import settings_to_argv


def test_xmltodict_settings_pass_defaults(settings_factory: Callable[..., XmlToDictSettings]) -> None:
    """file_glob defaults to '*.xml*'. buffer_size, log_interval, and cli_prog_name default as expected."""
    settings = settings_factory()
    assert settings.file_glob == "*.xml*"
    assert settings.buffer_size == DEFAULTS["buffer_size"]
    assert settings.log_interval == DEFAULTS["log_interval"]
    assert settings.model_config.get("cli_prog_name") == PIPELINE_NAME


def test_xmltodict_settings_pass_custom_file_glob(
    settings_factory: Callable[..., XmlToDictSettings],
) -> None:
    """file_glob accepts a custom glob pattern."""
    assert settings_factory(file_glob="*.rdf").file_glob == "*.rdf"


def test_xmltodict_settings_pass_custom_xml_tag(
    settings_factory: Callable[..., XmlToDictSettings],
) -> None:
    """xml_tag accepts any tag name."""
    settings = settings_factory(xml_tag="entry")
    assert settings.xml_tag == "entry"


@pytest.mark.parametrize("missing_field", ["xml_tag", "table_name", "dataset_name"])
def test_xmltodict_settings_fail_missing_required_fields(
    settings_factory: Callable[..., XmlToDictSettings],
    missing_field: str,
) -> None:
    """Omitting xml_tag, table_name, or dataset_name raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(**{missing_field: None})


@pytest.mark.parametrize("file_glob_arg", ["--file-glob", "-g"])
def test_xmltodict_settings_pass_xml_tag_shortcut(
    settings_factory: Callable[..., XmlToDictSettings],
    file_glob_arg: str,
) -> None:
    """The file_glob field is exposed as both --file-glob and -g on the command line."""
    base_settings = settings_factory(xml_tag="entry")
    argv = [*settings_to_argv(base_settings, exclude=["file_glob"]), file_glob_arg, "*.xml.gz"]

    settings = CliApp.run(XmlToDictSettings, cli_args=argv)
    assert settings.file_glob == "*.xml.gz"
    assert settings.xml_tag == "entry"
