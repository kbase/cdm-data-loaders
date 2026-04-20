"""Tests for the all_the_bacteria pipeline."""

import csv
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from requests.exceptions import HTTPError

from cdm_data_loaders.pipelines import all_the_bacteria, core
from cdm_data_loaders.pipelines.all_the_bacteria import (
    DATASET_NAME,
    AtbSettings,
    cli,
    download_atb_index_tsv,
    get_file_download_links,
    osf_file_downloader,
    run_atb_pipeline,
)
from cdm_data_loaders.pipelines.cts_defaults import VALID_DESTINATIONS
from cdm_data_loaders.utils.download.core import NonRetryableDownloadError
from tests.pipelines.conftest import CASSETTES_DIR


@pytest.fixture(autouse=True)
def patch_dlt_config(dlt_config: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch the dlt config in all tests."""
    monkeypatch.setattr(core.dlt, "config", dlt_config)


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """VCR config for tests that make HTTP requests."""
    return {
        "cassette_library_dir": CASSETTES_DIR,
        "record_mode": "once",  # record on first run, replay thereafter
        "serializer": "yaml",
        "match_on": ["method", "scheme", "host", "path", "query"],
        "decode_compressed_response": True,
        "allow_playback_repeats": True,
    }


@pytest.fixture
def test_settings(tmp_path: Path, dlt_config: dict[str, Any]) -> AtbSettings:
    """Generate a fake settings for testing."""
    return AtbSettings(dlt_config=dlt_config, output=str(tmp_path))


def test_cli_calls_run_ncbi_pipeline(monkeypatch: pytest.MonkeyPatch, dlt_config: dict[str, Any]) -> None:
    """Ensure that cli() calls run_ncbi_pipeline with the settings."""
    mock_settings_instance = MagicMock()
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_atb_pipeline = MagicMock()

    monkeypatch.setattr(all_the_bacteria, "AtbSettings", mock_settings_cls)
    monkeypatch.setattr(all_the_bacteria, "run_atb_pipeline", mock_run_atb_pipeline)

    cli()

    mock_settings_cls.assert_called_once_with(dlt_config=dlt_config)
    mock_run_atb_pipeline.assert_called_once_with(mock_settings_instance)


@pytest.mark.vcr
def test_download_atb_index_tsv_vcr(test_settings: AtbSettings) -> None:
    """Ensure that the download_atb_index function fetches the correct file."""
    output_file = download_atb_index_tsv(test_settings)
    raw_data_dir = Path(test_settings.raw_data_dir)
    assert raw_data_dir.is_dir()
    assert output_file.exists()
    assert output_file.parent == raw_data_dir


@pytest.mark.vcr
def test_download_atb_index_tsv_error_404(test_settings: AtbSettings) -> None:
    """Ensure that a 404 response causes an error and the function to die."""
    with pytest.raises(HTTPError, match="404 Client Error"):
        download_atb_index_tsv(test_settings)


@pytest.mark.vcr
def test_download_atv_index_tsv_error_missing_key(test_settings: AtbSettings) -> None:
    """Ensure that the lack of a download link in the response throws an error."""
    with pytest.raises(RuntimeError, match="Could not find download URL in response from "):
        download_atb_index_tsv(test_settings)


def test_download_atv_index_cannot_create_dir(dlt_config: dict[str, Any]) -> None:
    """Ensure that the output raw_data_dir directory can be saved to."""
    with pytest.raises(OSError, match=r"(Read-only file system|Permission denied)"):
        download_atb_index_tsv(AtbSettings(dlt_config=dlt_config, output="/path/to/file"))


@pytest.mark.vcr
def test_download_atv_index_tsv_error_cannot_download_tsv(test_settings: AtbSettings) -> None:
    """Ensure that the lack of a download link in the response throws an error."""
    with pytest.raises(NonRetryableDownloadError, match="Client error: 404 Not Found"):
        download_atb_index_tsv(test_settings)


def test_get_file_download_links() -> None:
    """Ensure that the appropriate files are picked out of the ATB file index TSV file."""
    file_path = Path("tests") / "data" / "atb" / "all_atb_files.tsv"
    filtered_files = list(get_file_download_links(file_path))
    # load the expected results
    expected = Path("tests") / "data" / "atb" / "filtered_files.tsv"
    with expected.open() as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        expected_files = list(reader)
    assert len(filtered_files[0]) > 1
    assert filtered_files[0] == expected_files


def test_get_file_download_links_invalid_file(caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that the correct fields are present in the ATB TSV file and throw an error if not."""
    file_path = Path("tests") / "data" / "atb" / "invalid_atb_files.tsv"
    with pytest.raises(RuntimeError, match="Missing required ATB file index TSV headers"):
        list(get_file_download_links(file_path))
    records = caplog.records
    assert records[-1].levelno == logging.ERROR
    assert records[-1].message.startswith(
        "Missing required ATB file index TSV headers: ['filename', 'md5', 'project', 'url']"
    )
    assert records[-2].levelno == logging.WARNING
    assert records[-2].message.startswith("ATB file index TSV headers have changed.")


def test_get_file_download_links_empty_file(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    """Ensure that an empty file causes a runtime error."""
    file_path = tmp_path / "fake_file.tsv"
    file_path.touch()
    with pytest.raises(RuntimeError, match=f"No valid TSV data found in {file_path!s}"):
        list(get_file_download_links(file_path))
    records = caplog.records
    assert records[-1].levelno == logging.ERROR
    assert records[-1].message == f"No valid TSV data found in {file_path!s}"


def test_get_file_download_links_no_file() -> None:
    """Ensure an error is thrown if the file cannot be found."""
    file_path = Path("/path") / "to" / "file"
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        list(get_file_download_links(file_path))


# osf_file_downloader tests
@pytest.mark.parametrize(
    ("atb_file_list", "expected_calls", "expected_paths"),
    [
        (
            [
                {"filename": "file1.txt", "url": "https://osf.io/file1", "md5": "md5sum1"},
                {"filename": "file2.txt", "url": "https://osf.io/file2", "md5": "md5sum2"},
            ],
            [
                (
                    "https://osf.io/file1",
                    "file1.txt",
                    "md5sum1",
                ),
                (
                    "https://osf.io/file2",
                    "file2.txt",
                    "md5sum2",
                ),
            ],
            ["file1.txt", "file2.txt"],
        ),
    ],
)
def test_osf_file_downloader_success(
    test_settings: AtbSettings,
    atb_file_list: list[dict[str, Any]],
    expected_calls: list[tuple[str, str, str]],
    expected_paths: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ensure that the osf_file_downloader function correctly calls the download client for each file."""
    mock_download_client = MagicMock()
    mock_logger = MagicMock()
    with (
        patch("cdm_data_loaders.pipelines.all_the_bacteria.FileDownloader", return_value=mock_download_client),
        patch("cdm_data_loaders.pipelines.all_the_bacteria.logger", mock_logger),
    ):
        output = list(osf_file_downloader(test_settings, atb_file_list))

    for item, filename in zip(atb_file_list, expected_paths, strict=True):
        assert item["path"] == str(Path(test_settings.raw_data_dir) / filename)

    assert output[0].data == [
        {**f, "path": str(Path(test_settings.raw_data_dir) / f["filename"])} for f in atb_file_list
    ]

    assert mock_download_client.download.call_count == len(expected_calls)

    for url, filename, checksum in expected_calls:
        mock_download_client.download.assert_any_call(
            url,
            Path(test_settings.raw_data_dir) / filename,
            expected_checksum=checksum,
            checksum_fn="md5",
        )

    mock_logger.assert_not_called()
    # no logs should be emitted for successful downloads
    assert caplog.records == []


@pytest.mark.parametrize(
    ("atb_file_list", "download_side_effect", "expected_exceptions", "expected_paths"),
    [
        (
            [
                {"filename": "good_file.txt", "url": "https://osf.io/good", "md5": "md5sum1"},
                {"filename": "great_file.txt", "url": "https://osf.io/great", "md5": "md5sum2"},
                {"filename": "bad_file.txt", "url": "https://osf.io/bad", "md5": "badmd5"},
            ],
            lambda url, _save_path, **_kwargs: (
                (_ for _ in ()).throw(RuntimeError("download failed")) if url == "https://osf.io/bad" else None
            ),
            ["Could not download file from https://osf.io/bad: download failed"],
            {"good_file.txt": True, "great_file.txt": True, "bad_file.txt": False},
        ),
        (
            [
                {"filename": "bad_file.txt", "url": "https://osf.io/bad", "md5": "badmd5"},
                {"filename": "even_worse.txt", "url": "https://osf.io/even_worse", "md5": "badmd5"},
            ],
            lambda _url, _save_path, **_kwargs: (_ for _ in ()).throw(Exception("Boom!")),
            [
                "Could not download file from https://osf.io/bad: Boom!",
                "Could not download file from https://osf.io/even_worse: Boom!",
            ],
            {"bad_file.txt": False, "even_worse.txt": False},
        ),
    ],
)
def test_osf_file_downloader_error_handling(
    test_settings: AtbSettings,
    atb_file_list: list[dict[str, Any]],
    download_side_effect: Callable,
    expected_exceptions: list[str],
    expected_paths: dict[str, bool],
) -> None:
    """Ensure that errors during file download are handled correctly."""
    mock_download_client = MagicMock()
    mock_download_client.download.side_effect = download_side_effect
    mock_logger = MagicMock()

    with (
        patch("cdm_data_loaders.pipelines.all_the_bacteria.FileDownloader", return_value=mock_download_client),
        patch("cdm_data_loaders.pipelines.all_the_bacteria.logger", mock_logger),
    ):
        list(osf_file_downloader(test_settings, atb_file_list))

    for item in atb_file_list:
        if item["filename"] in expected_paths and expected_paths[item["filename"]]:
            assert item["path"] == str(Path(test_settings.raw_data_dir) / item["filename"])
        else:
            assert "path" not in item

    # FIXME: why is caplog not working here? Ideally this should use caplog instead of a mock logger.
    exception_call_args = [call.args[0] for call in mock_logger.exception.call_args_list]
    assert exception_call_args == expected_exceptions


def test_run_atb_pipeline(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_atb_pipeline binds settings and delegates to run_pipeline with correct args."""
    mock_atb_file_list = MagicMock()
    mock_file_downloader = MagicMock()
    mock_run_pipeline = MagicMock()

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", mock_atb_file_list)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", mock_file_downloader)
    monkeypatch.setattr(all_the_bacteria, "run_pipeline", mock_run_pipeline)

    output = run_atb_pipeline(test_settings)
    assert output is None

    mock_atb_file_list.bind.assert_called_once_with(test_settings)
    mock_file_downloader.bind.assert_called_once_with(test_settings)
    mock_run_pipeline.assert_called_once_with(
        settings=test_settings,
        resource=mock_file_downloader,
        destination_kwargs={"max_table_nesting": 0},
        pipeline_kwargs={
            "pipeline_name": DATASET_NAME,
            "dataset_name": DATASET_NAME,
        },
        pipeline_run_kwargs=None,
    )


def test_run_atb_pipeline_bind_order(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both resources are bound before run_pipeline is called."""
    call_order: list[str] = []

    mock_atb_file_list = MagicMock()
    mock_atb_file_list.bind.side_effect = lambda *_: call_order.append("atb_file_list.bind")

    mock_file_downloader = MagicMock()
    mock_file_downloader.bind.side_effect = lambda *_: call_order.append("file_downloader.bind")

    def track_run_pipeline(**_: Any) -> None:
        call_order.append("run_pipeline")

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", mock_atb_file_list)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", mock_file_downloader)
    monkeypatch.setattr(all_the_bacteria, "run_pipeline", track_run_pipeline)

    run_atb_pipeline(test_settings)

    assert call_order.index("atb_file_list.bind") < call_order.index("run_pipeline")
    assert call_order.index("file_downloader.bind") < call_order.index("run_pipeline")


def test_run_atb_pipeline_resource_passed_to_run_pipeline_is_file_downloader(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The resource argument to run_pipeline is the (possibly mutated) file_downloader object."""
    sentinel = MagicMock(name="file_downloader_sentinel")
    mock_run_pipeline = MagicMock()

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", MagicMock())
    monkeypatch.setattr(all_the_bacteria, "file_downloader", sentinel)
    monkeypatch.setattr(all_the_bacteria, "run_pipeline", mock_run_pipeline)

    run_atb_pipeline(test_settings)

    assert mock_run_pipeline.call_args.kwargs["resource"] is sentinel


@pytest.mark.parametrize("use_output_dir_for_pipeline_metadata", [True, False])
@pytest.mark.parametrize("dev_mode", [True, False])
def test_run_atb_pipeline_pipeline_dir_present_or_absent(
    use_output_dir_for_pipeline_metadata: bool,
    dev_mode: bool,
    mock_dlt: MagicMock,
    dlt_config: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check that the appropriate args are passed as pipeline_kwargs."""
    settings = AtbSettings(
        dlt_config=dlt_config,
        output="/my/output",
        use_destination=VALID_DESTINATIONS[0],
        use_output_dir_for_pipeline_metadata=use_output_dir_for_pipeline_metadata,
        dev_mode=dev_mode,
    )
    mock_file_downloader = MagicMock(name="file_downloader")
    if use_output_dir_for_pipeline_metadata:
        assert settings.pipeline_dir == "/my/output/.dlt_conf"
    else:
        assert settings.pipeline_dir is None

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", MagicMock())
    monkeypatch.setattr(all_the_bacteria, "file_downloader", mock_file_downloader)

    run_atb_pipeline(settings)

    assert mock_dlt.destination.call_args_list == [call(VALID_DESTINATIONS[0], max_table_nesting=0)]
    mock_pipeline = mock_dlt.pipeline.return_value
    assert mock_pipeline.run.call_args_list == [call(mock_file_downloader)]

    pipeline_kwargs: dict[str, Any] = {"pipeline_name": DATASET_NAME, "dataset_name": DATASET_NAME}
    if use_output_dir_for_pipeline_metadata:
        pipeline_kwargs["pipelines_dir"] = settings.pipeline_dir
    if dev_mode:
        pipeline_kwargs["dev_mode"] = True

    assert mock_dlt.pipeline.call_args_list == [call(destination=mock_dlt.destination.return_value, **pipeline_kwargs)]
