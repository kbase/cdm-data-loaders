"""Tests for the UniProt DLT pipeline."""

import datetime
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from frozendict import frozendict
from pydantic_settings import CliApp

from cdm_data_loaders.parsers.uniprot.uniprot_kb import ENTRY_XML_TAG
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines import uniprot_kb as uniprot_module
from cdm_data_loaders.pipelines.cts_defaults import ARG_ALIASES
from cdm_data_loaders.pipelines.uniprot_kb import (
    UNIPROT_LOG_INTERVAL,
    UniProtSettings,
    cli,
    parse_uniprot,
    run_uniprot_pipeline,
)
from tests.pipelines.conftest import (
    TEST_BATCH_FILE_SETTINGS,
    TEST_BATCH_FILE_SETTINGS_RECONCILED,
    check_settings,
    make_batcher,
    make_settings_autofill_config,
)

# TODO: add a test to ensure that parse_uniprot_entry is called with the appropriate args. Requires mocking the file batcher and stream_xml_file_resource functions.


@pytest.fixture(autouse=True)
def patch_dlt_config(dlt_config: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch the dlt config in all tests."""
    monkeypatch.setattr(core.dlt, "config", dlt_config)


@pytest.fixture
def test_settings() -> UniProtSettings:
    """Provide a minimal valid UniProtSettings object."""
    return make_settings_autofill_config(UniProtSettings)  # type: ignore[reportReturnType]


TEST_SETTINGS = frozendict(
    **TEST_BATCH_FILE_SETTINGS,
)

TEST_SETTINGS_RECONCILED = frozendict(
    **TEST_BATCH_FILE_SETTINGS_RECONCILED,
)


def test_make_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified.

    Note that TEST_SETTINGS includes a value for pipeline_dir.
    """
    s = make_settings_autofill_config(UniProtSettings, **TEST_SETTINGS)  # type: ignore[reportReturnType]
    check_settings(s, TEST_SETTINGS_RECONCILED)


@pytest.mark.parametrize("dev_mode", ARG_ALIASES["dev_mode"])
@pytest.mark.parametrize("input_dir", ARG_ALIASES["input_dir"])
@pytest.mark.parametrize("output", ARG_ALIASES["output"])
@pytest.mark.parametrize("start_at", ARG_ALIASES["start_at"])
@pytest.mark.parametrize("use_destination", ARG_ALIASES["use_destination"])
@pytest.mark.parametrize(
    "use_output_dir_for_pipeline_metadata",
    ARG_ALIASES["use_output_dir_for_pipeline_metadata"],
)
def test_cli_app_run_alt_settings(  # noqa: PLR0913
    dev_mode: str,
    input_dir: str,
    output: str,
    start_at: str,
    use_destination: str,
    use_output_dir_for_pipeline_metadata: str,
    dlt_config: dict[str, Any],
) -> None:
    """Test all the variants of the UniProtSettings fields."""
    cli_args = [
        dev_mode,
        TEST_SETTINGS["dev_mode"],
        input_dir,
        TEST_SETTINGS["input_dir"],
        output,
        TEST_SETTINGS["output"],
        start_at,
        TEST_SETTINGS["start_at"],
        use_destination,
        TEST_SETTINGS["use_destination"],
        use_output_dir_for_pipeline_metadata,
        TEST_SETTINGS["use_output_dir_for_pipeline_metadata"],
    ]

    s = CliApp.run(
        UniProtSettings,
        dlt_config=dlt_config,
        cli_args=cli_args,
    )
    check_settings(s, TEST_SETTINGS_RECONCILED)


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with UniProtSettings as the settings class."""
    with patch.object(uniprot_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (UniProtSettings, run_uniprot_pipeline)


def test_cli_calls_run_uniprot_pipeline(monkeypatch: pytest.MonkeyPatch, dlt_config: dict[str, Any]) -> None:
    """Ensure that cli() calls run_uniprot_pipeline with the test_settings."""
    mock_settings_instance = MagicMock()
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_uniprot_pipeline = MagicMock()

    monkeypatch.setattr(uniprot_module, "UniProtSettings", mock_settings_cls)
    monkeypatch.setattr(uniprot_module, "run_uniprot_pipeline", mock_run_uniprot_pipeline)

    cli()

    mock_settings_cls.assert_called_once_with(dlt_config=dlt_config)
    mock_run_uniprot_pipeline.assert_called_once_with(mock_settings_instance)


# Tests for running the pipeline itself
def test_run_uniprot_pipeline_args_set_correctly(test_settings: UniProtSettings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniprot_module, "run_pipeline") as mock_run_pipeline:
        run_uniprot_pipeline(test_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {"pipeline_name": "uniprot_kb", "dataset_name": "uniprot_kb"}
    assert kwargs["pipeline_run_kwargs"] == {"table_format": "delta"}
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniprot_pipeline_sets_core_run_pipeline_args_correctly(
    test_settings: UniProtSettings, mock_dlt: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that run_uniprot_pipeline calls core.run_pipeline with the correct args."""
    mock_parse_uniprot = MagicMock()
    monkeypatch.setattr(uniprot_module, "parse_uniprot", mock_parse_uniprot)

    run_uniprot_pipeline(test_settings)

    # parse_uniprot was called once with the test_settings to produce the resource
    mock_parse_uniprot.assert_called_once_with(test_settings)

    # the return value of parse_uniprot(test_settings) is what gets passed to pipeline.run
    expected_resource = mock_parse_uniprot.return_value

    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        pipeline_name="uniprot_kb",
        dataset_name="uniprot_kb",
    )
    mock_dlt.pipeline.return_value.run.assert_called_once_with(
        expected_resource,
        table_format="delta",
    )


def test_parse_uniprot_resource(test_settings: UniProtSettings) -> None:
    """Ensure that parse_uniprot calls stream_xml_file_resource with the namespaced UniProt XML tag."""
    with patch.object(uniprot_module, "stream_xml_file_resource") as mock_stream:
        mock_stream.return_value = iter([])
        list(parse_uniprot(test_settings))

    assert mock_stream.call_count == 1
    kwargs = mock_stream.call_args.kwargs
    assert kwargs.keys() == {"settings", "xml_tag", "parse_fn", "log_interval"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["log_interval"] == UNIPROT_LOG_INTERVAL
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["parse_fn"], Callable)


@pytest.mark.skip("FIXME: not working -- due to parallelization?")
def test_integration_cli_uniprot_pipeline_output_validated(
    test_settings: UniProtSettings,  # the uniprot_kb UniProtSettings fixture
    fake_files: list[Path],
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that the full flow from cli() through launching the pipeline to yielding results is working correctly."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files)

    def fake_stream_xml_file(file_path: Path, tag: str) -> list[dict[str, Any]]:
        assert file_path in fake_files
        assert tag == ENTRY_XML_TAG
        return [{"stream_xml_file": file_path, "xml_tag": tag}]

    def fake_parse_uniprot_entry(
        entry: dict[str, Any],
        timestamp: datetime.datetime,
        file_path: Path,
    ) -> dict[str, list[Any]]:
        """Replacement for parse_uniprot_entry; validates there is no injected label argument."""
        assert isinstance(timestamp, datetime.datetime)
        assert entry == {"stream_xml_file": file_path, "xml_tag": ENTRY_XML_TAG}
        return {"entry": [entry], "file_path": [file_path]}

    mock_stream.side_effect = fake_stream_xml_file
    monkeypatch.setattr(uniprot_module, "parse_uniprot_entry", fake_parse_uniprot_entry)

    # make pipeline.run actually drain the generator so all assertions fire
    mock_dlt.pipeline.return_value.run.side_effect = lambda resource, **_: list(resource)

    # exercise the real cli() wiring with UniProtSettings construction mocked out
    monkeypatch.setattr(uniprot_module, "UniProtSettings", MagicMock(return_value=test_settings))
    cli()

    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name="uniprot_kb",
        destination=mock_dlt.destination.return_value,
        dataset_name="uniprot_kb",
        dev_mode=False,
    )
    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.return_value.run.assert_called_once()

    # verify with_table_name received the right rows and table names for every file, in order
    call_args_list = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    expected = []
    for f in fake_files:
        expected.extend(
            [
                [[{"stream_xml_file": f, "xml_tag": ENTRY_XML_TAG}], "entry"],
                [[f], "file_path"],
            ]
        )
    assert call_args_list == expected
