"""Tests for the UniRef CDM DLT pipelines."""

from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.uniref_cdm import UniRefCdmSettings
from cdm_data_loaders.pipelines.xmltopyarrow import pipeline as xmltopyarrow_pipeline_module
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from tests.cdm_data_loaders.pipelines.uniref_cdm.conftest import UNIREF_CDM_VARIANTS


@pytest.mark.parametrize(
    ("settings_cls", "cli_obj", "cli_name", "_protocol"),
    UNIREF_CDM_VARIANTS,
)
def test_cli_passes_settings_class_to_run_cli(
    settings_cls: type[UniRefCdmSettings],
    cli_obj: PipelineCli,
    cli_name: str,
    _protocol: str,
) -> None:
    """Ensure each UniRef CDM CLI is a PipelineCli wired to its settings class and run_xmltopyarrow_pipeline."""
    assert isinstance(cli_obj, PipelineCli)
    assert cli_obj.settings_cls is settings_cls
    assert cli_obj.pipeline_fn is run_xmltopyarrow_pipeline
    assert settings_cls.dataset_name == cli_name


def test_run_uniref_pipeline_args_set_correctly(test_settings: UniRefCdmSettings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each variant has its own pipeline and dataset name."""
    with patch.object(xmltopyarrow_pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_xmltopyarrow_pipeline(test_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": test_settings.dataset_name,
        "dataset_name": test_settings.dataset_name,
    }
    assert kwargs["pipeline_run_kwargs"] == {
        "loader_file_format": test_settings.loader_file_format,
    }
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniref_pipeline_sets_core_run_pipeline_args_correctly(
    test_settings: UniRefCdmSettings,
    mock_dlt: MagicMock,
) -> None:
    """Ensure that run_xmltopyarrow_pipeline calls core.run_pipeline with the correct args for UniRef CDM."""
    run_xmltopyarrow_pipeline(test_settings)

    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name=test_settings.dataset_name,
        dataset_name=test_settings.dataset_name,
    )
    assert mock_dlt.pipeline.return_value.destination == mock_dlt.destination.return_value
    first_run, load_info_save_run = mock_dlt.pipeline.return_value.run.call_args_list
    assert len(first_run.args) == 1
    assert first_run.kwargs == {"loader_file_format": test_settings.loader_file_format.value}
    assert load_info_save_run.args[0] is not None
    assert load_info_save_run.kwargs == {"loader_file_format": "jsonl"}
