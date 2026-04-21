"""Tests for the UniRef DLT pipeline."""

import datetime
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.parsers.uniprot.uniref import ENTRY_XML_TAG, UNIREF_VARIANTS
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines import uniref as uniref_module
from cdm_data_loaders.pipelines.cts_defaults import ARG_ALIASES
from cdm_data_loaders.pipelines.uniref import (
    UNIREF_LOG_INTERVAL,
    UNIREF_VARIANT_ALIASES,
    UnirefSettings,
    cli,
    parse_uniref,
    run_uniref_pipeline,
)
from tests.pipelines.conftest import (
    TEST_BATCH_FILE_SETTINGS,
    TEST_BATCH_FILE_SETTINGS_RECONCILED,
    check_settings,
    make_batcher,
    make_settings_autofill_config,
)

# TODO: add a test to ensure that parse_uniref_entry is called with the appropriate args. Requires mocking the file batcher and stream_xml_file_resource functions.


START_AT_VALUE = 25
START_AT_STRING = "25"

TEST_DEFAULT_UNIREF_VARIANT = "50"


TEST_SETTINGS = frozendict(
    **TEST_BATCH_FILE_SETTINGS,
    uniref_variant=TEST_DEFAULT_UNIREF_VARIANT,
)

TEST_SETTINGS_RECONCILED = frozendict(
    **TEST_BATCH_FILE_SETTINGS_RECONCILED,
    uniref_variant=TEST_DEFAULT_UNIREF_VARIANT,
)


@pytest.fixture(autouse=True)
def patch_dlt_config(dlt_config: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch the dlt config in all tests."""
    monkeypatch.setattr(core.dlt, "config", dlt_config)


@pytest.fixture(params=UNIREF_VARIANTS)
def variant(request: pytest.FixtureRequest) -> str:
    """Parametrized fixture over all valid uniref variants."""
    return request.param


@pytest.fixture
def test_settings(variant: str) -> UnirefSettings:
    """A valid UnirefSettings object for each uniref variant."""
    return make_settings_autofill_config(UnirefSettings, uniref_variant=variant, input_dir="/fake/input")  # type: ignore[reportReturnType]


@pytest.mark.parametrize("variant", UNIREF_VARIANTS)
def test_settings_valid_variants_accepted(variant: str) -> None:
    """Ensure that each valid uniref_variant value is accepted without error."""
    s = make_settings_autofill_config(UnirefSettings, uniref_variant=variant)
    assert isinstance(s, UnirefSettings)
    assert s.uniref_variant == variant


@pytest.mark.parametrize("value", UNIREF_VARIANTS)
@pytest.mark.parametrize("uniref_variant", UNIREF_VARIANT_ALIASES)
def test_cli_valid_variants_accepted(uniref_variant: str, dlt_config: dict[str, Any], value: str) -> None:
    """Ensure that each valid uniref_variant value is accepted without error when passed via CLI."""
    s = CliApp.run(UnirefSettings, dlt_config=dlt_config, cli_args=[uniref_variant, value])
    assert isinstance(s, UnirefSettings)
    assert s.uniref_variant == value


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
def test_invalid_variant_raises(value: str) -> None:
    """Ensure that an unrecognised uniref_variant raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        make_settings_autofill_config(UnirefSettings, uniref_variant=value)

    exc_message = str(exc_info.value)
    assert "uniref_variant must be one of" in exc_message


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize("uniref_variant", UNIREF_VARIANT_ALIASES)
def test_cli_invalid_variant_via_cli_raises(value: str, uniref_variant: str, dlt_config: dict[str, Any]) -> None:
    """Ensure that an invalid uniref_variant passed via CLI raises an error."""
    # with pytest.raises(ValidationError, match="Value error, uniref_variant must be one of"):
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(UnirefSettings, dlt_config=dlt_config, cli_args=[uniref_variant, value])

    exc_message = str(exc_info.value)
    assert "Value error, uniref_variant must be one of" in exc_message


def test_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref_variant argument raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        make_settings_autofill_config(UnirefSettings)

    exc_message = str(exc_info.value)
    assert "Field required" in exc_message


def test_cli_missing_required_uniref_variant_raises(dlt_config: dict[str, Any]) -> None:
    """Ensure that omitting the required uniref_variant argument raises an error."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(UnirefSettings, dlt_config=dlt_config, cli_args=[])

    exc_message = str(exc_info.value)
    assert "Field required" in exc_message


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize("uniref_variant", UNIREF_VARIANT_ALIASES)
def test_cli_invalid_variant_and_destination_via_cli_raises(
    value: str, uniref_variant: str, dlt_config: dict[str, Any]
) -> None:
    """Ensure that invalid uniref_variant and use_destination passed via CLI raises an error with both errors.

    N.b. Pydantic only reports the first error!!!
    """
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(
            UnirefSettings,
            dlt_config=dlt_config,
            cli_args=[uniref_variant, value, "--use_destination", "some invalid destination"],
        )

    # Check that both errors are present in the exception message
    exc_message = str(exc_info.value)
    assert "Value error, uniref_variant must be one of" in exc_message


def test_make_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified.

    Note that TEST_SETTINGS includes a value for pipeline_dir.
    """
    s = make_settings_autofill_config(UnirefSettings, **TEST_SETTINGS)
    check_settings(s, TEST_SETTINGS_RECONCILED)


@pytest.mark.parametrize("dev_mode", ARG_ALIASES["dev_mode"])
@pytest.mark.parametrize("input_dir", ARG_ALIASES["input_dir"])
@pytest.mark.parametrize("output", ARG_ALIASES["output"])
@pytest.mark.parametrize("start_at", ARG_ALIASES["start_at"])
@pytest.mark.parametrize("uniref_variant", UNIREF_VARIANT_ALIASES)
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
    uniref_variant: str,
    use_destination: str,
    use_output_dir_for_pipeline_metadata: str,
    dlt_config,
) -> None:
    """Test all the variants of the UnirefSettings fields."""
    cli_args = [
        dev_mode,
        TEST_SETTINGS["dev_mode"],
        input_dir,
        TEST_SETTINGS["input_dir"],
        output,
        TEST_SETTINGS["output"],
        start_at,
        TEST_SETTINGS["start_at"],
        uniref_variant,
        TEST_SETTINGS["uniref_variant"],
        use_destination,
        TEST_SETTINGS["use_destination"],
        use_output_dir_for_pipeline_metadata,
        TEST_SETTINGS["use_output_dir_for_pipeline_metadata"],
    ]

    s = CliApp.run(
        UnirefSettings,
        dlt_config=dlt_config,
        cli_args=cli_args,
    )
    check_settings(s, TEST_SETTINGS_RECONCILED)


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with UnirefSettings as the settings class."""
    with patch.object(uniref_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (UnirefSettings, run_uniref_pipeline)


def test_cli_calls_run_uniref_pipeline(monkeypatch: pytest.MonkeyPatch, dlt_config: dict[str, Any]) -> None:
    """Ensure that cli() calls run_uniref_pipeline with the settings."""
    mock_settings_instance = MagicMock()
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_uniref_pipeline = MagicMock()

    monkeypatch.setattr(uniref_module, "UnirefSettings", mock_settings_cls)
    monkeypatch.setattr(uniref_module, "run_uniref_pipeline", mock_run_uniref_pipeline)

    cli()
    mock_settings_cls.assert_called_once_with(dlt_config=dlt_config)
    mock_run_uniref_pipeline.assert_called_once_with(mock_settings_instance)


# Tests for running the pipeline itself
def test_run_uniref_pipeline_args_set_correctly(test_settings: UnirefSettings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniref_module, "run_pipeline") as mock_run_pipeline:
        run_uniref_pipeline(test_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": f"uniref_{test_settings.uniref_variant}",
        "dataset_name": "uniprot_kb",
    }
    assert kwargs["pipeline_run_kwargs"] == {"table_format": "delta"}
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniref_pipeline_sets_core_run_pipeline_args_correctly(
    test_settings: UnirefSettings, mock_dlt: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that run_uniref_pipeline calls core.run_pipeline with the correct args."""
    mock_parse_uniref = MagicMock()
    monkeypatch.setattr(uniref_module, "parse_uniref", mock_parse_uniref)

    run_uniref_pipeline(test_settings)

    # parse_uniref was called once with the settings to produce the resource
    mock_parse_uniref.assert_called_once_with(test_settings)

    # the return value of parse_uniref(settings) is what gets passed to pipeline.run
    expected_resource = mock_parse_uniref.return_value

    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        pipeline_name=f"uniref_{test_settings.uniref_variant}",
        dataset_name="uniprot_kb",
    )
    mock_dlt.pipeline.return_value.run.assert_called_once_with(
        expected_resource,
        table_format="delta",
    )


def test_parse_uniref_resource(test_settings: UnirefSettings) -> None:
    """Ensure that parse_uniref calls stream_xml_file_resource with the namespaced UniRef XML tag."""
    with patch.object(uniref_module, "stream_xml_file_resource") as mock_stream:
        mock_stream.return_value = iter([])
        list(parse_uniref(test_settings))

    assert mock_stream.call_count == 1
    kwargs = mock_stream.call_args.kwargs
    assert kwargs.keys() == {"settings", "xml_tag", "parse_fn", "log_interval"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["log_interval"] == UNIREF_LOG_INTERVAL
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["parse_fn"], Callable)


@pytest.mark.skip("FIXME: not working -- due to parallelization?")
def test_integration_cli_uniref_pipeline_output_validated(
    test_settings: UnirefSettings,  # parametrized over UNIREF_VARIANTS via the existing fixture
    fake_files: list[Path],
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that the full flow from cli() through launching the pipeline to yielding results is working correctly."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files)

    def fake_stream_xml_file(file_path: Path, tag: str) -> list[dict[str, Any]]:
        """Fake streaming an XML file."""
        assert file_path in fake_files
        assert tag == ENTRY_XML_TAG
        return [{"stream_xml_file": file_path, "xml_tag": tag}]

    def fake_parse_uniref_entry(
        entry: dict[str, Any],
        timestamp: datetime.datetime,
        file_path: Path,
        uniref_variant: str,
    ) -> dict[str, list[Any]]:
        """Replacement for parse_uniref_entry; validates the injected label."""
        assert isinstance(timestamp, datetime.datetime)
        assert uniref_variant == f"UniRef {test_settings.uniref_variant}"
        assert entry == {"stream_xml_file": file_path, "xml_tag": ENTRY_XML_TAG}
        return {"entry": [entry], "file_path": [file_path]}

    def fake_pipeline_run(resource, **kwargs) -> list:  # noqa: ANN001, ANN003
        """Fake pipeline run that drains the resource generator to trigger all assertions."""
        assert isinstance(resource, Callable)  # after draining the generator, resource should be a list
        # the resource should be the output of parse_uniref
        assert resource.name == "parse_uniref"
        assert kwargs == {"table_format": "delta"}
        return list(resource)

    mock_stream.side_effect = fake_stream_xml_file
    monkeypatch.setattr(uniref_module, "parse_uniref_entry", fake_parse_uniref_entry)

    # make pipeline.run drain the generator so all assertions fire
    mock_dlt.pipeline.return_value.run.side_effect = fake_pipeline_run

    # exercise the real cli() wiring with UnirefSettings construction mocked out
    monkeypatch.setattr(uniref_module, "UnirefSettings", MagicMock(return_value=test_settings))

    # RUN!
    cli()

    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name=f"uniref_{test_settings.uniref_variant}",
        use_destination=mock_dlt.use_destination.return_value,
        dataset_name="uniprot_kb",
        dev_mode=False,
    )
    mock_dlt.use_destination.assert_called_once_with(test_settings.use_destination)
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
