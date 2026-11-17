"""Unit tests for ``cdm_data_loaders.pipelines.presets.uniprot_cdm`` (UniProt CDM and UniRef CDM)."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from frozendict import frozendict
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.registry import TransformRegistry
from cdm_data_loaders.datasets.uniprot.kb import UNIPROT_CDM_SCHEMA_MAP
from cdm_data_loaders.datasets.uniprot.uniref import UNIREF_CDM_SCHEMA_MAP
from cdm_data_loaders.pipelines.presets.uniprot_cdm import (
    PIPELINE_NAME,
    UNIPROT_LOG_INTERVAL,
    UNIREF_LOG_INTERVAL,
    UniProtSettings,
    UniRef50CdmSettings,
    UniRefCdmSettings,
    uniprot_cdm_cli,
)
from cdm_data_loaders.pipelines.presets.uniprot_xml import UNIPROT_XSD_FILE, UNIREF_XSD_FILE
from cdm_data_loaders.pipelines.xmltopyarrow import pipeline as xmltopyarrow_pipeline_module
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import ENTRY_TABLE_NAME, ENTRY_XML_TAG
from tests._support.cli_assertions import assert_cli_field_roundtrips, check_settings, settings_to_argv
from tests.cdm_data_loaders.pipelines.presets._helpers import (
    UNIREF_CDM_SETTINGS_CLASSES,
    UNIREF_CDM_VARIANTS,
)


def test_uniprot_cdm_cli_passes_settings_class_to_run_cli() -> None:
    """``uniprot_cdm_cli`` is a ``CliRunner`` wired to ``UniProtSettings`` and ``run_xmltopyarrow_pipeline``."""
    assert isinstance(uniprot_cdm_cli, CliRunner)
    assert uniprot_cdm_cli.settings_cls is UniProtSettings
    assert uniprot_cdm_cli.pipeline_fn is run_xmltopyarrow_pipeline
    assert UniProtSettings.dataset_name == PIPELINE_NAME


def test_run_uniprot_pipeline_args_set_correctly(uniprot_settings: UniProtSettings) -> None:
    """``run_xmltopyarrow_pipeline`` passes ``UniProtSettings`` and ``uniprot_cdm`` dataset kwargs to ``run_pipeline``."""
    with patch.object(xmltopyarrow_pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_xmltopyarrow_pipeline(uniprot_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": PIPELINE_NAME,
        "dataset_name": PIPELINE_NAME,
    }
    assert kwargs["pipeline_run_kwargs"] == {
        "loader_file_format": uniprot_settings.loader_file_format,
    }
    assert kwargs["settings"] == uniprot_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniprot_pipeline_sets_core_run_pipeline_args_correctly(
    uniprot_settings: UniProtSettings,
    mock_dlt: MagicMock,
) -> None:
    """``run_xmltopyarrow_pipeline`` configures ``dlt.destination`` and ``dlt.pipeline`` for UniProt CDM."""
    run_xmltopyarrow_pipeline(uniprot_settings)

    mock_dlt.destination.assert_called_once_with(uniprot_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name=PIPELINE_NAME,
        dataset_name=PIPELINE_NAME,
    )
    assert mock_dlt.pipeline.return_value.destination == mock_dlt.destination.return_value
    first_run, load_info_save_run = mock_dlt.pipeline.return_value.run.call_args_list
    assert len(first_run.args) == 1
    assert first_run.kwargs == {"loader_file_format": uniprot_settings.loader_file_format.value}
    assert load_info_save_run.args[0] is not None
    assert load_info_save_run.kwargs == {"loader_file_format": "jsonl"}


def test_uniprot_settings_all_params_set(
    test_uniprot_settings: frozendict[str, Any],
    test_uniprot_settings_reconciled: frozendict[str, Any],
) -> None:
    """``UniProtSettings`` reconciles all explicit field values."""
    settings = UniProtSettings(**test_uniprot_settings)
    check_settings(settings, test_uniprot_settings_reconciled)


def test_uniprot_cdm_defaults_and_transform(test_defaults: dict[str, Any]) -> None:
    """``UniProtSettings`` configures defaults, locked ``ClassVar`` attributes, and bound registries."""
    settings = UniProtSettings(**test_defaults)
    assert settings.dataset_name == PIPELINE_NAME
    assert settings.table_name == ENTRY_TABLE_NAME
    assert settings.xml_tag == ENTRY_XML_TAG
    assert settings.transforms_module is None
    assert settings.output_schemas_module is None
    assert settings.log_interval == UNIPROT_LOG_INTERVAL
    assert settings.xsd_file == UNIPROT_XSD_FILE

    assert isinstance(settings.table_transforms, TransformRegistry)
    transform = settings.get_table_transform(ENTRY_TABLE_NAME)
    assert callable(transform)
    assert settings.schema_map == UNIPROT_CDM_SCHEMA_MAP

    routed = transform({"_dataset": "Swiss-Prot", "accession": ["P12345"]})
    assert routed["entity"][0]["entity_id"] == "uniprot:P12345"


@pytest.mark.parametrize("field_name", list(UniProtSettings.model_fields), ids=list(UniProtSettings.model_fields))
def test_uniprot_cli_fields_parse_correctly(
    field_name: str,
    test_uniprot_settings: frozendict[str, Any],
    test_uniprot_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """All CLI flags and shortcuts on ``UniProtSettings`` round-trip cleanly."""
    assert_cli_field_roundtrips(
        UniProtSettings,
        field_name,
        test_uniprot_settings[field_name],
        test_uniprot_settings_reconciled[field_name],
        required=test_defaults,
    )


@pytest.mark.parametrize(
    ("locked_flag", "locked_value"),
    [
        pytest.param("--dataset-name", "custom_dataset", id="dataset_name"),
        pytest.param("--table-name", "custom_table", id="table_name"),
        pytest.param("--xml-tag", "custom_tag", id="xml_tag"),
        pytest.param("--transforms-module", "some.module", id="transforms_module"),
        pytest.param("--output-schemas-module", "some.module", id="output_schemas_module"),
    ],
)
def test_uniprot_locked_fields_rejected_on_cli(
    locked_flag: str,
    locked_value: str,
    test_defaults: dict[str, Any],
) -> None:
    """Locked ``ClassVar`` fields on ``UniProtSettings`` are rejected as CLI flags."""
    base = UniProtSettings(**test_defaults)
    cli_args = [*settings_to_argv(base), locked_flag, locked_value]
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {locked_flag}"):
        CliApp.run(UniProtSettings, cli_args=cli_args)


@pytest.mark.parametrize(
    ("settings_cls", "cli_obj", "cli_name", "_protocol"),
    UNIREF_CDM_VARIANTS,
    ids=[name for _, _, name, _ in UNIREF_CDM_VARIANTS],
)
def test_uniref_cli_passes_settings_class_to_run_cli(
    settings_cls: type[UniRefCdmSettings],
    cli_obj: CliRunner[Any, Any],
    cli_name: str,
    _protocol: str,
) -> None:
    """Each UniRef CDM CLI is a ``CliRunner`` wired to its settings class and ``run_xmltopyarrow_pipeline``."""
    assert isinstance(cli_obj, CliRunner)
    assert cli_obj.settings_cls is settings_cls
    assert cli_obj.pipeline_fn is run_xmltopyarrow_pipeline
    assert settings_cls.dataset_name == cli_name


def test_run_uniref_pipeline_args_set_correctly(uniref_settings: UniRefCdmSettings) -> None:
    """Each UniRef CDM variant passes its own ``pipeline_name`` and ``dataset_name`` to ``run_pipeline``."""
    with patch.object(xmltopyarrow_pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_xmltopyarrow_pipeline(uniref_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": uniref_settings.dataset_name,
        "dataset_name": uniref_settings.dataset_name,
    }
    assert kwargs["pipeline_run_kwargs"] == {
        "loader_file_format": uniref_settings.loader_file_format,
    }
    assert kwargs["settings"] == uniref_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniref_pipeline_sets_core_run_pipeline_args_correctly(
    uniref_settings: UniRefCdmSettings,
    mock_dlt: MagicMock,
) -> None:
    """``run_xmltopyarrow_pipeline`` configures ``dlt.destination`` and ``dlt.pipeline`` for UniRef CDM."""
    run_xmltopyarrow_pipeline(uniref_settings)

    mock_dlt.destination.assert_called_once_with(uniref_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name=uniref_settings.dataset_name,
        dataset_name=uniref_settings.dataset_name,
    )
    assert mock_dlt.pipeline.return_value.destination == mock_dlt.destination.return_value
    first_run, load_info_save_run = mock_dlt.pipeline.return_value.run.call_args_list
    assert len(first_run.args) == 1
    assert first_run.kwargs == {"loader_file_format": uniref_settings.loader_file_format.value}
    assert load_info_save_run.args[0] is not None
    assert load_info_save_run.kwargs == {"loader_file_format": "jsonl"}


@pytest.mark.parametrize(
    "settings_cls",
    UNIREF_CDM_SETTINGS_CLASSES,
    ids=[cls.__name__ for cls in UNIREF_CDM_SETTINGS_CLASSES],
)
def test_uniref_settings_all_params_set(
    settings_cls: type[UniRefCdmSettings],
    test_uniref_cdm_settings: frozendict[str, Any],
    test_uniref_cdm_settings_reconciled: frozendict[str, Any],
) -> None:
    """Each ``UniRefCdmSettings`` subclass reconciles all explicit field values."""
    settings = settings_cls(**test_uniref_cdm_settings)
    check_settings(settings, test_uniref_cdm_settings_reconciled)


@pytest.mark.parametrize(
    ("settings_cls", "_cli_obj", "expected_dataset_name", "expected_protocol"),
    UNIREF_CDM_VARIANTS,
    ids=[name for _, _, name, _ in UNIREF_CDM_VARIANTS],
)
def test_uniref_cdm_defaults_and_transform(
    settings_cls: type[UniRefCdmSettings],
    _cli_obj: object,
    expected_dataset_name: str,
    expected_protocol: str,
    test_defaults: dict[str, Any],
) -> None:
    """Each ``UniRefCdmSettings`` subclass configures defaults, locked ``ClassVar`` attributes, and bound registries."""
    settings = settings_cls(**test_defaults)
    assert settings.dataset_name == expected_dataset_name
    assert settings.table_name == ENTRY_TABLE_NAME
    assert settings.xml_tag == ENTRY_XML_TAG
    assert settings.transforms_module is None
    assert settings.output_schemas_module is None
    assert settings.log_interval == UNIREF_LOG_INTERVAL
    assert settings.xsd_file == UNIREF_XSD_FILE

    assert isinstance(settings.table_transforms, TransformRegistry)
    transform = settings.get_table_transform(ENTRY_TABLE_NAME)
    assert callable(transform)
    assert settings.schema_map == UNIREF_CDM_SCHEMA_MAP

    routed = transform({"_id": "UniRef_TEST", "_updated": "2025-01-01", "name": "Cluster: Test"})
    assert routed["cluster"][0]["protocol"] == expected_protocol


@pytest.mark.parametrize(
    "settings_cls",
    UNIREF_CDM_SETTINGS_CLASSES,
    ids=[cls.__name__ for cls in UNIREF_CDM_SETTINGS_CLASSES],
)
@pytest.mark.parametrize(
    "field_name",
    list(UniRef50CdmSettings.model_fields),
    ids=list(UniRef50CdmSettings.model_fields),
)
def test_uniref_cli_fields_parse_correctly(
    settings_cls: type[UniRefCdmSettings],
    field_name: str,
    test_uniref_cdm_settings: frozendict[str, Any],
    test_uniref_cdm_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """All CLI fields round-trip on every UniRef CDM settings class."""
    assert_cli_field_roundtrips(
        settings_cls,
        field_name,
        test_uniref_cdm_settings[field_name],
        test_uniref_cdm_settings_reconciled[field_name],
        test_defaults,
    )


@pytest.mark.parametrize(
    "settings_cls",
    UNIREF_CDM_SETTINGS_CLASSES,
    ids=[cls.__name__ for cls in UNIREF_CDM_SETTINGS_CLASSES],
)
@pytest.mark.parametrize(
    ("locked_flag", "locked_value"),
    [
        pytest.param("--dataset-name", "custom_dataset", id="dataset_name"),
        pytest.param("--table-name", "custom_table", id="table_name"),
        pytest.param("--xml-tag", "custom_tag", id="xml_tag"),
        pytest.param("--transforms-module", "some.module", id="transforms_module"),
        pytest.param("--output-schemas-module", "some.module", id="output_schemas_module"),
    ],
)
def test_uniref_locked_fields_rejected_on_cli(
    settings_cls: type[UniRefCdmSettings],
    locked_flag: str,
    locked_value: str,
    test_defaults: dict[str, Any],
) -> None:
    """Locked ``ClassVar`` fields on ``UniRefCdmSettings`` subclasses are rejected as CLI flags."""
    base = settings_cls(**test_defaults)
    cli_args = [*settings_to_argv(base), locked_flag, locked_value]
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {locked_flag}"):
        CliApp.run(settings_cls, cli_args=cli_args)
