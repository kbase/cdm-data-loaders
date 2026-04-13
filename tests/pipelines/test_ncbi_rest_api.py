"""Tests for the NCBI datasets API pipeline functions."""

from pathlib import Path
from typing import Any
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from dlt.extract.items import DataItemWithMeta
from pydantic import ValidationError
from pydantic_settings import CliApp
from requests import HTTPError

from cdm_data_loaders.pipelines import ncbi_rest_api as ncbi_module
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT, OUTPUT_MOUNT, VALID_DESTINATIONS
from cdm_data_loaders.pipelines.ncbi_rest_api import (
    ANNOTATION,
    DATASET,
    DATASET_NAME,
    ERROR,
    Settings,
    assemble_assembly_reports,
    assembly_list,
    cli,
    get_annotation_report,
    get_assembly_reports,
    get_dataset_reports,
    run_ncbi_pipeline,
)

CASSETTES_DIR = "tests/cassettes"

DLT_TESTING_CONFIG = {
    "destination.local_fs.bucket_url": "tests/dlt_test_output",
    "normalize.data_writer.disable_compression": True,
}


@pytest.fixture(autouse=True)
def patch_rest_client_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that the REST_CLIENT_HOOKS dict is empty for tests."""
    monkeypatch.setattr("cdm_data_loaders.pipelines.ncbi_rest_api.REST_CLIENT_HOOKS", {})


@pytest.fixture(autouse=True)
def patch_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch environment variables related to runtime configs."""
    env_vars = {"timeout": "5", "max_attempts": "1", "backoff_factor": "1", "max_retry_delay": "1"}
    for v in env_vars:
        monkeypatch.setenv(f"RUNTIME__REQUEST_{v.upper()}", "1")


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """VCR config for tests that make HTTP requests."""
    return {
        "cassette_library_dir": CASSETTES_DIR,
        "record_mode": "once",  # record on first run, replay thereafter
        "serializer": "yaml",
        "match_on": ["method", "scheme", "host", "path", "query"],
        # strip the NCBI API key from cassettes
        "filter_query_parameters": ["api_key"],
        "filter_headers": ["api_key"],
        "decode_compressed_response": True,
        "allow_playback_repeats": True,
    }


ID_WITH_2K_ANNOTS = "GCF_000003135.1"
ID_WITH_500_ANNOTS = "GCF_000007725.1"
ID_TRIGGERS_500_ERR = "GCF_500_ERROR"
VALID_IDS = [ID_WITH_500_ANNOTS, ID_WITH_2K_ANNOTS]
INVALID_ID = "invalid_id"
ALL_IDS = [*VALID_IDS, INVALID_ID]
BATCH_SIZE = 500
BATCH_SIZE_STRING = "500"


@pytest.fixture(scope="module")
def config() -> Settings:
    """Default config for testing."""
    return Settings.model_validate({"input_dir": "/fake/dir"})


@pytest.fixture(scope="module")
def valid_assembly_ids() -> list[str]:
    """A list of assembly IDs."""
    return VALID_IDS


@pytest.fixture(scope="module")
def assembly_id(valid_assembly_ids: list[str]) -> str:
    """Single valid assembly ID."""
    return valid_assembly_ids[0]


@pytest.fixture(scope="module")
def invalid_assembly_id() -> str:
    """Invalid assembly ID."""
    return INVALID_ID


@pytest.fixture(scope="module")
def assembly_ids(valid_assembly_ids: list[str], invalid_assembly_id: str) -> list[str]:
    """List of assembly IDs including both valid and invalid IDs."""
    return [*valid_assembly_ids, invalid_assembly_id]


def make_settings(**kwargs: str | int | bool) -> Settings:
    """Generate a validated Settings object."""
    return Settings.model_validate(kwargs)


def test_settings_defaults() -> None:
    """Ensure the settings defaults are set up correctly."""
    s = make_settings()
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.dev_mode is False
    assert s.input_dir == INPUT_MOUNT
    assert s.pipeline_dir is None
    # FIXME: should be dlt.config["destination.local_fs.bucket_url"]
    assert s.output == OUTPUT_MOUNT
    assert s.use_output_dir_for_pipeline_metadata is False
    assert s.batch_size == ncbi_module.MAX_RESULTS_PER_PAGE


def test_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified."""
    s = make_settings(
        destination=VALID_DESTINATIONS[0],
        dev_mode=True,
        input_dir="/dir/path",
        output="/some/dir",
        use_output_dir_for_pipeline_metadata=True,
        batch_size=BATCH_SIZE,
    )
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.dev_mode is True
    assert s.input_dir == "/dir/path"
    assert s.pipeline_dir == Path("/some") / "dir" / ".dlt_conf"
    assert s.output == "/some/dir"
    assert s.use_output_dir_for_pipeline_metadata is True
    assert s.batch_size == BATCH_SIZE


@pytest.mark.parametrize("destination", VALID_DESTINATIONS)
def test_settings_valid_variants_accepted(destination: str) -> None:
    """Ensure that each valid destination value is accepted without error."""
    s = make_settings(destination=destination)
    assert s.destination == destination


@pytest.mark.parametrize("bad", ["gcs", "filesystem", "", "LocalFs"])
def test_invalid_destination_raises(bad: str) -> None:
    """Ensure that an unrecognised destination raises a ValidationError."""
    with pytest.raises(ValidationError, match="destination must be one of"):
        make_settings(destination=bad)


@pytest.mark.parametrize("input_dir", ["-i", "--input-dir", "--input_dir"])
@pytest.mark.parametrize("destination", ["-d", "--destination"])
@pytest.mark.parametrize("use_output_dir_for_pipeline_metadata", ["-p", "--pipeline-dir", "--pipeline_dir"])
@pytest.mark.parametrize("output", ["-o", "--output"])
@pytest.mark.parametrize("dev_mode_flag", ["--dev", "--dev-mode", "--dev_mode"])
@pytest.mark.parametrize("batch_size", ["-b", "--batch-size", "--batch_size"])
def test_cli_all_variants(  # noqa: PLR0913
    input_dir: str,
    destination: str,
    use_output_dir_for_pipeline_metadata: str,
    output: str,
    dev_mode_flag: str,
    batch_size: str,
) -> None:
    """Test all the variants of the Settings fields."""
    s = CliApp.run(
        Settings,
        [
            input_dir,
            "/dir/path",
            destination,
            VALID_DESTINATIONS[0],
            output,
            "/some/dir",
            use_output_dir_for_pipeline_metadata,
            "True",
            dev_mode_flag,
            "True",
            batch_size,
            BATCH_SIZE_STRING,
        ],
    )
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.dev_mode is True
    assert s.input_dir == "/dir/path"
    assert s.pipeline_dir == Path("/some") / "dir" / ".dlt_conf"
    assert s.output == "/some/dir"
    assert s.use_output_dir_for_pipeline_metadata is True
    assert s.batch_size == BATCH_SIZE


@pytest.mark.parametrize("bad", ["gcs", "filesystem", "", "LocalFs"])
def test_cli_invalid_destination_via_cli_raises(bad: str) -> None:
    """Ensure that an invalid destination passed via CLI raises an error."""
    with pytest.raises(ValidationError, match="Value error, destination must be one of"):
        CliApp.run(Settings, cli_args=["--destination", bad])


@pytest.mark.parametrize(
    ("bad_batch_size", "message"),
    [
        ("0", "Input should be greater than or equal to 1"),
        ("-1", "Input should be greater than or equal to 1"),
        ("1001", "Input should be less than or equal to 1000"),
        ("notanint", "Input should be a valid integer"),
        ("", "Input should be a valid integer"),
    ],
)
def test_cli_invalid_batch_size_via_cli_raises(bad_batch_size: str, message: str) -> None:
    """Ensure that an invalid batch size passed via CLI raises an error."""
    with pytest.raises(ValidationError, match=message):
        CliApp.run(Settings, cli_args=["--batch-size", bad_batch_size])


@pytest.mark.parametrize(
    ("boolean", "valid", "value"),
    [
        (None, False, None),
        ("notaboolean", False, None),
        ("123", False, None),
        ("", False, None),
        ("Truee", False, None),
        ("Falsee", False, None),
        ("true", True, True),
        ("false", True, False),
        ("True", True, True),
        ("False", True, False),
        ("1", True, True),
        ("0", True, False),
    ],
)
def test_cli_invalid_boolean_via_cli_raises(boolean: str, valid: bool, value: bool) -> None:
    """Ensure that an invalid boolean passed via CLI causes a ValidationError.

    :param boolean: the value of the boolean
    :type boolean: str
    :param valid: whether or not this is a valid CLI value for a boolean field
    :type valid: bool
    :param value: the expected parsed value of the boolean
    :type value: bool
    """
    if not valid:
        with pytest.raises(ValidationError, match="Input should be a valid boolean, unable to interpret input"):
            CliApp.run(Settings, cli_args=["--dev-mode", boolean])
    else:
        s = CliApp.run(Settings, cli_args=["--dev-mode", boolean])
        assert s.dev_mode is value


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with Settings as the settings class."""
    with patch.object(ncbi_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (Settings, run_ncbi_pipeline)


def test_cli_calls_run_ncbi_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that cli() calls run_ncbi_pipeline with the config."""
    mock_settings_instance = MagicMock(spec=Settings)
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_ncbi_pipeline = MagicMock()

    monkeypatch.setattr(ncbi_module, "Settings", mock_settings_cls)
    monkeypatch.setattr(ncbi_module, "run_ncbi_pipeline", mock_run_ncbi_pipeline)

    cli()

    mock_settings_cls.assert_called_once_with()
    mock_run_ncbi_pipeline.assert_called_once_with(mock_settings_instance)


def check_dataset_report(dataset_report: dict[str, Any] | None, assembly_id: str) -> None:
    """Check the basic structure of a dataset report."""
    assert dataset_report is not None
    assert dataset_report["accession"] == assembly_id
    for key in ["current_accession", "source_database", "organism", "assembly_info", "assembly_stats"]:
        assert key in dataset_report


def check_annotation_report(annotation_report: list[dict[str, Any]] | None, assembly_id: str) -> None:
    """Check the basic structure of an annotation report."""
    assert annotation_report is not None
    for item in annotation_report:
        assert isinstance(item, dict)
        assert "row_id" in item
        assert "annotation" in item
        assert item.get("annotation", {}).get("annotations", [{}])[0].get("assembly_accession") == assembly_id
    all_row_ids = [int(item["row_id"]) for item in annotation_report]
    assert all_row_ids == list(range(1, len(all_row_ids) + 1))


def test_assembly_list_resource() -> None:
    """Test that the assembly list resource yields the expected assembly IDs."""
    config = Settings.model_validate({"input_dir": "tests/data/ncbi_rest_api/input"})

    ass_list = list(assembly_list(config))
    assert ass_list == [
        "GCF_029958545.3",
        "GCF_029958565.3",
        "GCF_029958585.3",
        "invalid_id",
        "GCF_029958605.3",
        "GCF_029958625.3",
        "GCF_029958645.3",
        "GCF_029958665.3",
    ]


@pytest.mark.parametrize("dev_mode", [False, True, None])
@pytest.mark.parametrize("use_pipeline_dir", [False, True, None])
def test_run_ncbi_pipeline_sets_core_run_pipeline_args_correctly(
    config: Settings,
    dev_mode: bool | None,
    use_pipeline_dir: bool | None,
    mock_dlt: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that run_ncbi_pipeline calls core.run_pipeline with the correct args."""
    mock_assembly_report_parser = MagicMock()
    monkeypatch.setattr(ncbi_module, "assembly_report_parser", mock_assembly_report_parser)
    mock_assembly_list = MagicMock()
    monkeypatch.setattr(ncbi_module, "assembly_list", mock_assembly_list)

    base_settings: dict[str, str | bool] = {"input_dir": "tests/data/ncbi_rest_api/input", "output": "/some/dir"}
    if dev_mode is not None:
        base_settings["dev_mode"] = dev_mode
    if use_pipeline_dir is not None:
        base_settings["use_output_dir_for_pipeline_metadata"] = use_pipeline_dir

    config = Settings.model_validate(base_settings)

    run_ncbi_pipeline(config)

    mock_dlt.destination.assert_called_once_with(config.destination, max_table_nesting=0)
    mock_dlt.pipeline.assert_called_once()
    mock_assembly_list.bind.assert_called_once_with(config)
    assert mock_dlt.pipeline.call_args.kwargs["destination"] == mock_dlt.destination.return_value
    assert mock_dlt.pipeline.call_args.kwargs["pipeline_name"] == DATASET_NAME
    assert mock_dlt.pipeline.call_args.kwargs["dataset_name"] == DATASET_NAME
    if dev_mode:  # truthy
        assert mock_dlt.pipeline.call_args.kwargs["dev_mode"] is True
    else:
        assert mock_dlt.pipeline.call_args.kwargs["dev_mode"] is False
    if use_pipeline_dir:  # truthy
        assert mock_dlt.pipeline.call_args.kwargs["pipelines_dir"] == Path(config.output) / ".dlt_conf"  # type: ignore[reportArgumentType]
    else:
        assert "pipelines_dir" not in mock_dlt.pipeline.call_args.kwargs

    mock_dlt.pipeline.return_value.run.assert_called_once_with([mock_assembly_report_parser])


@pytest.mark.default_cassette("test_get_assembly_reports.yaml")
@pytest.mark.vcr
def test_get_dataset_reports() -> None:
    """Ensure that every assembly ID appears as a key in the output dict with the appropriate dict output."""
    dataset_report = get_dataset_reports(ALL_IDS)
    assert set(dataset_report.keys()) == set(ALL_IDS)
    assert dataset_report[INVALID_ID] is None
    for assembly_id in VALID_IDS:
        check_dataset_report(dataset_report.get(assembly_id), assembly_id)


def test_get_dataset_reports_empty_id_list_yields_empty_dict() -> None:
    """An empty input list produces an empty output dict."""
    assert get_dataset_reports([]) == {}


@pytest.mark.default_cassette("test_get_assembly_reports.yaml")
@pytest.mark.vcr
def test_get_annotation_report_single_page() -> None:
    """Test the retrieval of an annotation report with a single page."""
    annotation_report = get_annotation_report(ID_WITH_500_ANNOTS)
    check_annotation_report(annotation_report, ID_WITH_500_ANNOTS)


@pytest.mark.default_cassette("test_get_assembly_reports.yaml")
@pytest.mark.vcr
def test_get_annotation_report_multi_page() -> None:
    """Test the retrieval of an annotation report with multiple pages."""
    annotation_report = get_annotation_report(ID_WITH_2K_ANNOTS)
    assert isinstance(annotation_report, list)
    check_annotation_report(annotation_report, ID_WITH_2K_ANNOTS)


@mock.patch("tenacity.nap.time.sleep", MagicMock())
@pytest.mark.vcr
def test_get_annotation_report_multi_page_err() -> None:
    """An error in the middle of a multi-page retrieval should stop the whole retrieval process."""
    with pytest.raises(HTTPError, match="500 Server Error: Internal Server Error for url"):
        get_annotation_report(ID_TRIGGERS_500_ERR)


@pytest.mark.default_cassette("test_get_assembly_reports.yaml")
@pytest.mark.vcr
def test_get_annotation_report_invalid_id() -> None:
    """Test the retrieval of an annotation report for an invalid ID."""
    assert get_annotation_report(INVALID_ID) is None


def test_get_assembly_reports_empty_id_list() -> None:
    """Ensure that getting reports for an empty list returns nothing."""
    assert get_assembly_reports([]) == {}


@pytest.mark.vcr
def test_get_assembly_reports() -> None:
    """Test the retrieval of annotation and dataset reports."""
    assembly_reports = get_assembly_reports(ALL_IDS)
    assert set(assembly_reports) == {DATASET, ANNOTATION, ERROR}
    for datatype in [DATASET, ANNOTATION]:
        assert set(assembly_reports[datatype]) == set(ALL_IDS)
        assert assembly_reports[datatype][INVALID_ID] is None
    for assembly_id in [ID_WITH_2K_ANNOTS, ID_WITH_500_ANNOTS]:
        check_annotation_report(assembly_reports[ANNOTATION][assembly_id], assembly_id)
        check_dataset_report(assembly_reports[DATASET][assembly_id], assembly_id)
    assert assembly_reports[ERROR] == []


RECORDED_ERRORS = {
    "dataset_404": {
        "assembly_id": None,
        "assembly_id_list": ALL_IDS,
        "error_class": "HTTPError",
        "error_from": "dataset_report",
        "message": '404 Client Error: Not Found for url: https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCF_000007725.1%2CGCF_000003135.1%2Cinvalid_id/dataset_report?page_size=1000\nResponse: {"error":"Not Found","code":404,"message":"Your request is invalid. (For more help, see the NCBI Datasets Documentation at https://www.ncbi.nlm.nih.gov/datasets/docs/)"}\n',
        "request_url": "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCF_000007725.1%2CGCF_000003135.1%2Cinvalid_id/dataset_report?page_size=1000",
        "status": 404,
        "reason": "Not Found",
    },
    "annotation_report_500": {
        "assembly_id": ID_WITH_2K_ANNOTS,
        "assembly_id_list": None,
        "error_class": "HTTPError",
        "error_from": "annotation_report",
        "message": '500 Server Error: Internal Server Error for url: https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCF_000003135.1/annotation_report?page_size=1000&page_token=eNrjYos2NDAwjAUABagBiw\nResponse: {"error":"Internal Server Error","code":500,"message":"Internal Server Error (For more help, see the NCBI Datasets Documentation at https://www.ncbi.nlm.nih.gov/datasets/docs/)"}\n',
        "request_url": "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCF_000003135.1/annotation_report?page_size=1000&page_token=eNrjYos2NDAwjAUABagBiw",
        "status": 500,
        "reason": "Internal Server Error",
    },
    "annotation_report_404": {
        "assembly_id": ID_WITH_500_ANNOTS,
        "assembly_id_list": None,
        "error_class": "HTTPError",
        "error_from": "annotation_report",
        "message": '404 Client Error: Not Found for url: https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCF_000007725.1/annotation_report?page_size=1000\nResponse: {"error":"Not Found","code":404,"message":"Your request is invalid. (For more help, see the NCBI Datasets Documentation at https://www.ncbi.nlm.nih.gov/datasets/docs/)"}\n',
        "request_url": "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCF_000007725.1/annotation_report?page_size=1000",
        "status": 404,
        "reason": "Not Found",
    },
    "value_error": {
        "assembly_id": INVALID_ID,
        "assembly_id_list": None,
        "error_class": "ValueError",
        "error_from": "annotation_report",
        "message": f"Some error message involving {INVALID_ID}.",
        "request_url": None,
        "status": None,
        "reason": None,
    },
}


@mock.patch("tenacity.nap.time.sleep", MagicMock())
@pytest.mark.default_cassette("test_get_assembly_reports_annotation_report_errors.yaml")
@pytest.mark.vcr
def test_get_assembly_reports_annotation_report_errors() -> None:
    """Test the retrieval of assembly data when errors occur fetching annotation reports."""
    original_get_annotation_report = get_annotation_report

    def patched_get_annotation_report(assembly_id: str) -> list[dict[str, Any]] | None:
        """Patched version of get_annotation_report that throws a value error with a certain input.

        :param assembly_id: assembly ID
        :type assembly_id: str
        :raises ValueError: if the ID is INVALID_ID
        :return: output from the real get_annotation_report
        :rtype: list[dict[str, Any]] | None
        """
        if assembly_id == INVALID_ID:
            err_msg = f"Some error message involving {INVALID_ID}."
            raise ValueError(err_msg)
        return original_get_annotation_report(assembly_id)

    with mock.patch(
        "cdm_data_loaders.pipelines.ncbi_rest_api.get_annotation_report",
        side_effect=patched_get_annotation_report,
    ):
        assembly_reports = get_assembly_reports(ALL_IDS)

    assert set(assembly_reports) == {DATASET, ANNOTATION, ERROR}
    for datatype in [DATASET, ANNOTATION]:
        assert set(assembly_reports[datatype]) == set(ALL_IDS)
        assert assembly_reports[datatype][INVALID_ID] is None
    for assembly_id in [ID_WITH_2K_ANNOTS, ID_WITH_500_ANNOTS]:
        check_dataset_report(assembly_reports[DATASET][assembly_id], assembly_id)
    # ID_WITH_500 succeeds, ID_WITH_2K does not
    check_annotation_report(assembly_reports[ANNOTATION][ID_WITH_500_ANNOTS], ID_WITH_500_ANNOTS)
    assert assembly_reports[ANNOTATION][ID_WITH_2K_ANNOTS] is None

    assert assembly_reports[ERROR] == [RECORDED_ERRORS["annotation_report_500"], RECORDED_ERRORS["value_error"]]


@mock.patch("tenacity.nap.time.sleep", MagicMock())
@pytest.mark.vcr
def test_get_assembly_reports_dataset_report_errors() -> None:
    """Test the retrieval of assembly data when an error occurs fetching dataset reports."""
    assembly_reports = get_assembly_reports(ALL_IDS)
    assert set(assembly_reports) == {DATASET, ANNOTATION, ERROR}
    for datatype in [DATASET, ANNOTATION]:
        assert set(assembly_reports[datatype]) == set(ALL_IDS)
        assert assembly_reports[datatype][INVALID_ID] is None
    for assembly_id in [ID_WITH_2K_ANNOTS, ID_WITH_500_ANNOTS]:
        check_annotation_report(assembly_reports[ANNOTATION][assembly_id], assembly_id)
        assert assembly_reports[DATASET][assembly_id] is None

    assert assembly_reports[ERROR] == [RECORDED_ERRORS["dataset_404"]]


@mock.patch("tenacity.nap.time.sleep", MagicMock())
@pytest.mark.vcr
def test_get_assembly_reports_total_wipeout() -> None:
    """Test the retrieval of assembly data when all queries fail."""
    original_get_annotation_report = get_annotation_report

    def patched_get_annotation_report(assembly_id: str) -> list[dict[str, Any]] | None:
        """Patched version of get_annotation_report that throws a value error with a certain input.

        :param assembly_id: assembly ID
        :type assembly_id: str
        :raises ValueError: if the ID is INVALID_ID
        :return: output from the real get_annotation_report
        :rtype: list[dict[str, Any]] | None
        """
        if assembly_id == INVALID_ID:
            err_msg = f"Some error message involving {INVALID_ID}."
            raise ValueError(err_msg)
        return original_get_annotation_report(assembly_id)

    with mock.patch(
        "cdm_data_loaders.pipelines.ncbi_rest_api.get_annotation_report",
        side_effect=patched_get_annotation_report,
    ):
        output = get_assembly_reports(ALL_IDS)

    assert output == {
        DATASET: dict.fromkeys(ALL_IDS),
        ANNOTATION: dict.fromkeys(ALL_IDS),
        ERROR: [
            RECORDED_ERRORS["dataset_404"],
            RECORDED_ERRORS["annotation_report_404"],
            RECORDED_ERRORS["annotation_report_500"],
            RECORDED_ERRORS["value_error"],
        ],
    }


@pytest.mark.skip("FIXME: not working, possibly due to parallelization?")
@pytest.mark.vcr
def test_get_assembly_report_parser_with_cassette(assembly_ids: list[str], tmp_path: Path) -> None:
    with patch("dlt.mark") as mock_dlt_mark:
        config = Settings.model_validate({"input_dir": "tests/data/ncbi_rest_api/input", "output": str(tmp_path)})
        run_ncbi_pipeline(config)


def collect_results(reports: dict) -> dict[str, list]:
    """Drain the generator returned by assemble_assembly_reports into a dict keyed by table name."""
    results: dict[str, list] = {}
    for item in assemble_assembly_reports(reports):
        assert isinstance(item, DataItemWithMeta), f"Expected DataItemWithMeta, got {type(item)}"
        table_name = item.meta.table_name
        results.setdefault(table_name, [])
        results[table_name].extend(item.data)
    return results


# assemble_assembly_reports tests

DATASET_REPORT_1 = {
    "accession": ID_WITH_2K_ANNOTS,
    "organism": {"tax_id": 9606, "organism_name": "Homo sapiens"},
    "assembly_stats": {"total_sequence_length": 3099734149},
}

DATASET_REPORT_2 = {
    "accession": ID_WITH_500_ANNOTS,
    "organism": {"tax_id": 10090, "organism_name": "Mus musculus"},
    "assembly_stats": {"total_sequence_length": 2728222451},
}

ANNOTATION_REPORT_1 = [
    {"release_date": "2022-01-01", "annotation_name": "Annotation A"},
    {"release_date": "2022-06-01", "annotation_name": "Annotation B"},
]

ANNOTATION_REPORT_2 = [
    {"release_date": "2023-01-01", "annotation_name": "Annotation C"},
]

ERROR_REPORT = {
    "assembly_id": INVALID_ID,
    "assembly_id_list": None,
    "error_class": "HTTPError",
    "error_from": "dataset_report",
    "message": "404 Not Found",
    "request_url": "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCA_000001635.9/dataset_report",
    "status": 404,
    "reason": "Not Found",
}


@pytest.fixture
def full_assembly_reports() -> dict[str, Any]:
    """Assembly reports fixture with valid data for two assemblies and no errors."""
    return {
        DATASET: {
            ID_WITH_2K_ANNOTS: DATASET_REPORT_1,
            ID_WITH_500_ANNOTS: DATASET_REPORT_2,
        },
        ANNOTATION: {
            ID_WITH_2K_ANNOTS: ANNOTATION_REPORT_1,
            ID_WITH_500_ANNOTS: ANNOTATION_REPORT_2,
        },
        ERROR: [],
    }


@pytest.fixture
def reports_with_errors() -> dict[str, Any]:
    """Assembly reports fixture with one None dataset report, one None annotation report, and one error entry."""
    return {
        DATASET: {
            ID_WITH_2K_ANNOTS: DATASET_REPORT_1,
            ID_WITH_500_ANNOTS: DATASET_REPORT_2,
            INVALID_ID: None,
        },
        ANNOTATION: {
            ID_WITH_2K_ANNOTS: ANNOTATION_REPORT_1,
            ID_WITH_500_ANNOTS: ANNOTATION_REPORT_2,
            INVALID_ID: None,
        },
        ERROR: [ERROR_REPORT],
    }


EXPECTED_DB_TABLES = {
    f"{DATASET}_report": [
        {"assembly_id": ID_WITH_2K_ANNOTS, **DATASET_REPORT_1},
        {
            "assembly_id": ID_WITH_500_ANNOTS,
            **DATASET_REPORT_2,
        },
    ],
    f"{ANNOTATION}_report": [
        {"assembly_id": ID_WITH_2K_ANNOTS, **ANNOTATION_REPORT_1[0]},
        {"assembly_id": ID_WITH_2K_ANNOTS, **ANNOTATION_REPORT_1[1]},
        {"assembly_id": ID_WITH_500_ANNOTS, **ANNOTATION_REPORT_2[0]},
    ],
}

EXPECTED_DB_TABLES_WITH_ERROR = {
    f"{DATASET}_report": [*EXPECTED_DB_TABLES[f"{DATASET}_report"], {"assembly_id": INVALID_ID}],
    f"{ANNOTATION}_report": [*EXPECTED_DB_TABLES[f"{ANNOTATION}_report"]],
    "ncbi_import_error": [ERROR_REPORT],
}


@pytest.mark.parametrize("reports", [{}, None])
def test_assemble_assembly_reports_empty_dict_yields_nothing(reports: None | dict[str, Any]) -> None:
    """Ensure that empty or None as input produces no output items."""
    assert list(assemble_assembly_reports(reports)) == []  # type: ignore[reportArgumentType]


def test_assemble_assembly_reports_yields_two_items_when_no_errors(full_assembly_reports: dict[str, Any]) -> None:
    """When the error list is empty, the generator should yield only two DataItemWithMeta objects."""
    assert len(list(assemble_assembly_reports(full_assembly_reports))) == 2


def test_assemble_assembly_reports_all_items_are_data_item_with_meta(full_assembly_reports: dict[str, Any]) -> None:
    """Every item yielded by the generator should be a DataItemWithMeta instance."""
    for item in assemble_assembly_reports(full_assembly_reports):
        assert isinstance(item, DataItemWithMeta)


def test_assemble_assembly_reports_table_names_when_errors_present(reports_with_errors: dict[str, Any]) -> None:
    """The output should contain dataset_report, annotation_report, and ncbi_import_error table names."""
    results = collect_results(reports_with_errors)
    assert set(results) == {"dataset_report", "annotation_report", "ncbi_import_error"}
    assert results == EXPECTED_DB_TABLES_WITH_ERROR


def test_assemble_assembly_reports_no_error_table_when_error_list_empty(full_assembly_reports: dict[str, Any]) -> None:
    """When the error list is empty, no ncbi_import_error table should be present in the output."""
    results = collect_results(full_assembly_reports)
    assert set(results) == {"dataset_report", "annotation_report"}
    assert results == EXPECTED_DB_TABLES


def test_assemble_assembly_reports_dataset_report_none_report_emits_row_with_only_assembly_id() -> None:
    """A None dataset report should still produce a row containing only the assembly_id key."""
    reports = {
        DATASET: {INVALID_ID: None},
        ANNOTATION: {INVALID_ID: None},
        ERROR: [],
    }
    results = collect_results(reports)
    # no ncbi_import_error or annotation_report
    assert results["dataset_report"] == [{"assembly_id": INVALID_ID}]


def test_assemble_assembly_reports_multiple_errors_all_yielded() -> None:
    """All entries in the error list should appear as individual rows in the ncbi_import_error table."""
    errors = [
        {"error_class": "HTTPError", "error_from": "dataset_report", "message": "err1"},
        {"error_class": "ValueError", "error_from": "annotation_report", "message": "err2"},
    ]
    reports = {
        DATASET: {},
        ANNOTATION: {},
        ERROR: errors,
    }
    results = collect_results(reports)
    assert results["ncbi_import_error"] == errors
    assert set(results) == {"ncbi_import_error"}
