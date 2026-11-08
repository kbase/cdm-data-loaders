"""Tests for the UniProt CDM DLT pipeline."""

from collections.abc import Callable
from unittest.mock import MagicMock, patch

from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.uniprot_kb_cdm import (
    PIPELINE_NAME,
    UniProtSettings,
    cli,
)
from cdm_data_loaders.pipelines.xmltopyarrow import pipeline as xmltopyarrow_pipeline_module
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli is a PipelineCli wired to UniProtSettings and run_xmltopyarrow_pipeline."""
    assert isinstance(cli, PipelineCli)
    assert cli.settings_cls is UniProtSettings
    assert cli.pipeline_fn is run_xmltopyarrow_pipeline
    assert UniProtSettings.dataset_name == PIPELINE_NAME


def test_run_uniprot_pipeline_args_set_correctly(test_settings: UniProtSettings) -> None:
    """Ensure that the pipeline arguments are set correctly for UniProt CDM."""
    with patch.object(xmltopyarrow_pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_xmltopyarrow_pipeline(test_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": PIPELINE_NAME,
        "dataset_name": PIPELINE_NAME,
    }
    assert kwargs["pipeline_run_kwargs"] == {
        "loader_file_format": test_settings.loader_file_format,
    }
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniprot_pipeline_sets_core_run_pipeline_args_correctly(
    test_settings: UniProtSettings,
    mock_dlt: MagicMock,
) -> None:
    """Ensure that run_xmltopyarrow_pipeline calls core.run_pipeline with the correct args for UniProt CDM."""
    run_xmltopyarrow_pipeline(test_settings)

    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name=PIPELINE_NAME,
        dataset_name=PIPELINE_NAME,
    )
    assert mock_dlt.pipeline.return_value.destination == mock_dlt.destination.return_value
    first_run, load_info_save_run = mock_dlt.pipeline.return_value.run.call_args_list
    assert len(first_run.args) == 1
    assert first_run.kwargs == {"loader_file_format": test_settings.loader_file_format.value}
    assert load_info_save_run.args[0] is not None
    assert load_info_save_run.kwargs == {"loader_file_format": "jsonl"}
