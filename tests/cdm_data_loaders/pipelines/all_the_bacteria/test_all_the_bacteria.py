"""Tests for the all_the_bacteria pipeline."""

import copy
import csv
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import dlt
import pytest
from frozendict import frozendict
from pydantic import ValidationError
from requests.exceptions import HTTPError

from cdm_data_loaders.core.fields import PIPELINE_DIR_NAME
from cdm_data_loaders.pipelines import all_the_bacteria
from cdm_data_loaders.pipelines.all_the_bacteria import (
    ALL_ATB_FILE_NAME,
    DATASET_NAME,
    AtbSettings,
    cli,
    download_atb_index_tsv,
    get_file_download_links,
    load_patterns,
    osf_file_downloader,
    run_atb_pipeline,
)
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.utils.file_transfer.core import NonRetryableDownloadError
from tests.cdm_data_loaders.pipelines.conftest import VALID_DESTINATIONS
from tests.conftest import S3


@pytest.fixture
def pattern_file(tmp_path: Path) -> Path:
    """Pattern file for testing load_patterns."""
    p = tmp_path / "patterns.txt"
    p.write_text(
        "hello world\nfoo.bar\nstarts with*\n2+2=4\n",
        encoding="utf-8",
    )
    return p


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli is a PipelineCli wired to AtbSettings and run_atb_pipeline."""
    assert isinstance(cli, PipelineCli)
    assert cli.settings_cls is AtbSettings
    assert cli.pipeline_fn is run_atb_pipeline


@pytest.mark.parametrize(
    ("candidate", "expected_match"),
    [
        pytest.param("hello world", True, id="exact-hello"),
        pytest.param("foo.bar", True, id="exact-dot"),
        pytest.param("2+2=4", True, id="exact-plus"),
        pytest.param("hello world!!!", False, id="trailing-chars"),
        pytest.param("well, hello world", False, id="leading-chars"),
        pytest.param("fooXbar", False, id="dot-is-literal"),
        pytest.param("22=4", False, id="plus-is-literal"),
        pytest.param("starts with", True, id="wildcard-empty-suffix"),
        pytest.param("starts with anything", True, id="wildcard-alpha-suffix"),
        pytest.param("starts with 123!@#", True, id="wildcard-symbol-suffix"),
        pytest.param("ends with", False, id="wildcard-wrong-prefix"),
    ],
)
def test_load_patterns_exact_match(pattern_file: Path, candidate: str, expected_match: bool) -> None:
    """Plain text lines should match against their literal content or trailing wildcard."""
    pattern = load_patterns(pattern_file)
    assert isinstance(pattern, re.Pattern)
    assert bool(pattern.match(candidate)) is expected_match


@pytest.mark.parametrize(
    ("candidate", "expected_match"),
    [
        pytest.param("hello", True, id="hello"),
        pytest.param("world", True, id="world"),
        pytest.param("", False, id="empty-string"),
    ],
)
def test_load_patterns_blank_lines_are_ignored(tmp_path: Path, candidate: str, expected_match: bool) -> None:
    """Blank lines in the file should be skipped."""
    p = tmp_path / "patterns.txt"
    p.write_text("\nhello\n\nworld\n", encoding="utf-8")
    pattern = load_patterns(p)
    assert isinstance(pattern, re.Pattern)
    assert bool(pattern.match(candidate)) is expected_match


@pytest.mark.parametrize("candidate", ["", "anything at all"], ids=["empty", "non-empty"])
def test_load_patterns_only_wildcard_matches_anything(tmp_path: Path, candidate: str) -> None:
    """A file containing only '*' should produce a pattern that matches any string."""
    p = tmp_path / "patterns.txt"
    p.write_text("*\n", encoding="utf-8")
    pattern = load_patterns(p)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match(candidate)


@pytest.mark.parametrize(
    ("candidate", "expected_match"),
    [
        pytest.param("cat", True, id="cat"),
        pytest.param("dog", True, id="dog"),
        pytest.param("bird", True, id="bird"),
        pytest.param("fish", False, id="fish"),
    ],
)
def test_load_patterns_alternation(tmp_path: Path, candidate: str, expected_match: bool) -> None:
    """Each line in the file should become an alternative in the combined pattern."""
    p = tmp_path / "patterns.txt"
    p.write_text("cat\ndog\nbird\n", encoding="utf-8")
    pattern = load_patterns(p)
    assert isinstance(pattern, re.Pattern)
    assert bool(pattern.match(candidate)) is expected_match


def test_load_patterns_no_file_returns_none(tmp_path: Path) -> None:
    """Ensure that loading a non-existent file returns None."""
    pattern = load_patterns(tmp_path / "some" / "path")
    assert pattern is None


def test_load_patterns_touched_file_returns_none(tmp_path: Path) -> None:
    """Ensure that loading a zero-byte (touched) file returns None."""
    p = tmp_path / "patterns.txt"
    p.touch()
    pattern = load_patterns(p)
    assert pattern is None


def test_load_patterns_empty_file_returns_none(tmp_path: Path) -> None:
    """Ensure that loading an empty file returns None."""
    p = tmp_path / "patterns.txt"
    p.write_text("\n\n   \t\n  \n   \n\t\t\n", encoding="utf-8")
    pattern = load_patterns(p)
    assert pattern is None


@pytest.mark.vcr
def test_download_atb_index_tsv_vcr(test_settings: AtbSettings) -> None:
    """Ensure that the download_atb_index function fetches the correct file."""
    output_file = download_atb_index_tsv(test_settings)
    raw_data_dir = Path(test_settings.raw_data_dir)
    assert raw_data_dir.is_dir()
    assert output_file.exists()
    assert output_file.parent == raw_data_dir


@pytest.mark.vcr
@pytest.mark.default_cassette("test_download_atb_index_tsv_vcr.yaml")
def test_download_atb_index_tsv_vcr_destination_s3(test_s3_settings: AtbSettings) -> None:
    """Ensure that the download_atb_index function fetches the correct file."""
    output_file = download_atb_index_tsv(test_s3_settings)
    raw_data_dir = Path(test_s3_settings.raw_data_dir)
    assert raw_data_dir.is_dir()
    assert output_file.exists()
    assert output_file.parent == raw_data_dir


@pytest.mark.vcr
def test_download_atb_index_tsv_error_404(test_settings: AtbSettings) -> None:
    """Ensure that a 404 response causes an error and the function to die."""
    with pytest.raises(HTTPError, match="404 Client Error"):
        download_atb_index_tsv(test_settings)


@pytest.mark.vcr
@pytest.mark.default_cassette("test_download_atb_index_tsv_vcr.yaml")
def test_download_atb_index_s3_error_boom(test_s3_settings: AtbSettings) -> None:
    """Ensure that an error in the download causes things to die unpleasantly."""
    mock_download_client = MagicMock()
    mock_download_client.download.side_effect = ValueError("ZOMG!")
    with (
        patch("cdm_data_loaders.pipelines.all_the_bacteria.FileDownloader", return_value=mock_download_client),
        pytest.raises(ValueError, match="ZOMG!"),
    ):
        download_atb_index_tsv(test_s3_settings)

    download_url = "https://osf.io/download/r6gcp/"
    mock_download_client.download.assert_called_once_with(
        url=download_url,
        destination=test_s3_settings.raw_data_dir / ALL_ATB_FILE_NAME,
    )


@pytest.mark.vcr
def test_download_atv_index_tsv_error_missing_key(test_settings: AtbSettings) -> None:
    """Ensure that the lack of a download link in the response throws an error."""
    with pytest.raises(RuntimeError, match="Could not find download URL in response from "):
        download_atb_index_tsv(test_settings)


def test_download_atv_index_cannot_create_dir(tmp_path: Path) -> None:
    """Ensure that the output raw_data_dir directory can be saved to."""
    # N.b. error message is OS specific -- may not work on Windows
    with pytest.raises(ValidationError, match=r"(Read-only file system|Permission denied)"):
        download_atb_index_tsv(
            AtbSettings(input_dir=str(tmp_path), output_dir="/path/to/file", use_destination="local_fs")  # pyright: ignore[reportCallIssue]
        )


@pytest.mark.vcr
def test_download_atv_index_tsv_error_cannot_download_tsv(test_settings: AtbSettings) -> None:
    """Ensure that the lack of a download link in the response throws an error."""
    with pytest.raises(NonRetryableDownloadError, match="Client error: 404 Not Found"):
        download_atb_index_tsv(test_settings)


def test_get_file_download_links(test_settings: AtbSettings) -> None:
    """Ensure that the appropriate files are picked out of the ATB file index TSV file using the default matcher."""
    file_path = Path("tests") / "data" / "atb" / "all_atb_files.tsv"
    filtered_files = list(get_file_download_links(test_settings, file_path))
    # load the expected results
    expected = Path("tests") / "data" / "atb" / "filtered_files.tsv"
    with expected.open() as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        expected_files = list(reader)
    assert filtered_files[0] == expected_files


EXPECTED_LINES = {
    "*": "all_atb_files.tsv",
    "AllTheBacteria/Annotation/Bakta\nAllTheBacteria/Assembly\nAllTheBacteria/Metadata\n": "assembly_bakta_metadata_exact.tsv",
    "AllTheBacteria/Annotation/Bakta*\nAllTheBacteria/Assembly\nAllTheBacteria/Metadata\n": "assembly_bakta_star_metadata.tsv",
    "AllTheBacteria/Annotation/Bakta": "bakta_exact.tsv",
    "AllTheBacteria/Annotation/Bakta*": "bakta_star.tsv",
}


@pytest.mark.parametrize("pattern_lines", EXPECTED_LINES)
def test_get_file_download_links_use_pattern_file(tmp_path: Path, pattern_lines: str) -> None:
    """Generate a pattern file from EXPECTED_LINES and check that the output from get_file_download_links is correct."""
    # create the pattern file
    p = tmp_path / "patterns.txt"
    p.write_text(f"{pattern_lines}\n", encoding="utf-8")

    settings = AtbSettings(
        input_dir=str(tmp_path),
        output_dir=str(tmp_path),
        pattern_file=str(tmp_path / "patterns.txt"),
        use_destination="local_fs",
    )  # pyright: ignore[reportCallIssue]
    file_path = Path("tests") / "data" / "atb" / "all_atb_files.tsv"
    filtered_files = list(get_file_download_links(settings, file_path))
    # load the expected results
    expected = Path("tests") / "data" / "atb" / EXPECTED_LINES[pattern_lines]
    with expected.open() as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        expected_files = list(reader)
    assert len(filtered_files[0]) > 1
    assert filtered_files[0] == expected_files


def test_get_file_download_links_invalid_file(test_settings: AtbSettings, caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that the correct fields are present in the ATB TSV file and throw an error if not."""
    file_path = Path("tests") / "data" / "atb" / "invalid_atb_files.tsv"
    with pytest.raises(RuntimeError, match="Missing required ATB file index TSV headers"):
        list(get_file_download_links(test_settings, file_path))
    records = caplog.records
    assert records[-1].levelno == logging.ERROR
    assert (
        records[-1]
        .getMessage()
        .startswith("Missing required ATB file index TSV headers: ['filename', 'md5', 'project', 'url']")
    )
    assert records[-2].levelno == logging.WARNING
    assert records[-2].getMessage().startswith("ATB file index TSV headers have changed.")


def test_get_file_download_links_empty_file(
    test_settings: AtbSettings, caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    """Ensure that an empty file causes a runtime error."""
    file_path = tmp_path / "fake_file.tsv"
    file_path.touch()
    with pytest.raises(RuntimeError, match=f"No valid TSV data found in {file_path!s}"):
        list(get_file_download_links(test_settings, file_path))
    records = caplog.records
    assert records[-1].levelno == logging.ERROR
    assert records[-1].getMessage() == f"No valid TSV data found in {file_path!s}"


def test_get_file_download_links_no_file(test_settings: AtbSettings) -> None:
    """Ensure an error is thrown if the file cannot be found."""
    file_path = Path("/path") / "to" / "file"
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        list(get_file_download_links(test_settings, file_path))


FILE_DOWNLOADER_OUTPUT = [
    frozendict(
        {
            "filename": "file1.txt",
            "url": "https://osf.io/file1",
            "md5": "md5sum1",
            "project": "AllTheBacteria/Annotation/Project",
            "path": "Annotation/Project/file1.txt",
        }
    ),
    frozendict(
        {
            "filename": "file2.txt",
            "url": "https://osf.io/file2",
            "md5": "md5sum2",
            "project": "AllTheBacteria/Side/Project/",
            "path": "Side/Project/file2.txt",
        }
    ),
    frozendict(
        {
            "filename": "some/path/to/file3.txt",
            "url": "https://osf.io/file3",
            "md5": "md5sum3",
            "project": "AllTheBacteria",
            "path": "some/path/to/file3.txt",
        }
    ),
    frozendict(
        {
            "filename": "not/least/file4.txt",
            "url": "https://osf.io/file4",
            "md5": "md5sum4",
            "project": "AllTheBacteria/last/but",
            "path": "last/but/not/least/file4.txt",
        }
    ),
]


# osf_file_downloader tests
@pytest.mark.parametrize(
    "atb_input",
    [
        # each of the files singly and then the whole lot as a batch
        *[[f] for f in FILE_DOWNLOADER_OUTPUT],
        FILE_DOWNLOADER_OUTPUT,
    ],
)
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
def test_osf_file_downloader_success(
    request: pytest.FixtureRequest,
    atb_input: list[dict[str, Any]],
    use_destination: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ensure that the osf_file_downloader function correctly calls the download client for each file."""
    settings = request.getfixturevalue("test_s3_settings" if use_destination == S3 else "test_settings")

    atb_file_list = [{k: v for k, v in f.items() if k != "path"} for f in atb_input]

    # expected_output path needs the raw data dir adding to it
    expected_output = [dict(f.items()) for f in atb_input]
    for f in expected_output:
        f["path"] = f"{settings.raw_data_dir}/{f['path']}"

    mock_download_client = MagicMock()
    with patch(
        "cdm_data_loaders.pipelines.all_the_bacteria.FileDownloader",
        return_value=mock_download_client,
    ):
        # get the output from the generator
        output = list(osf_file_downloader(settings, atb_file_list))

    # should have a separate call for each file downloaded
    assert mock_download_client.download.call_count == len(atb_file_list)

    call_list = [c.kwargs for c in mock_download_client.download.call_args_list]
    expected_calls = [
        {
            "url": f["url"],
            "destination": Path(settings.raw_data_dir) / f["path"],
            "expected_checksum": f["md5"],
            "checksum_fn": "md5",
        }
        for f in atb_input
    ]
    assert call_list == expected_calls

    # output from dlt.mark.with_table_name
    assert output[0].data == expected_output
    # the input args are mutated in place
    assert atb_file_list == expected_output

    # no logs should be emitted for successful downloads
    assert caplog.records == []


@pytest.mark.parametrize(
    ("atb_file_list", "expected_exceptions", "expected_paths"),
    [
        (
            [
                {
                    "project": "AllTheBacteria/One",
                    "filename": "Two/good_file.txt",
                    "url": "https://osf.io/good",
                    "md5": "md5sum1",
                },
                {
                    "project": "AllTheBacteria/One/Two",
                    "filename": "great_file.txt",
                    "url": "https://osf.io/great",
                    "md5": "md5sum2",
                },
                {
                    "project": "AllTheBacteria/One",
                    "filename": "fail.txt",
                    "url": "https://osf.io/fail",
                    "md5": "badmd5",
                },
            ],
            ["Could not download file from https://osf.io/fail: Loser!"],
            {"Two/good_file.txt": True, "great_file.txt": True, "fail.txt": False},
        ),
        (
            [
                {
                    "project": "Dud",
                    "filename": "bad_file.txt",
                    "url": "https://osf.io/bad",
                    "md5": "badmd5",
                },
                {
                    "project": "Dud",
                    "filename": "also_very_bad.txt",
                    "url": "https://osf.io/also_very_bad",
                    "md5": "badmd5",
                },
            ],
            [
                "Could not download file from https://osf.io/bad: BOOM!",
                "Could not download file from https://osf.io/also_very_bad: BOOM!",
            ],
            {"bad_file.txt": False, "also_very_bad.txt": False},
        ),
    ],
)
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
def test_osf_file_downloader_error_handling(
    atb_file_list: list[dict[str, Any]],
    expected_exceptions: list[str],
    expected_paths: dict[str, bool],
    use_destination: str,
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> None:
    """Ensure that errors during file download are handled correctly."""
    settings = request.getfixturevalue("test_s3_settings" if use_destination == S3 else "test_settings")

    def file_downloader_boom(**args) -> None:  # noqa: ANN003
        if "url" in args:
            if "bad" in args["url"]:
                msg = "BOOM!"
                raise ValueError(msg)
            if "fail" in args["url"]:
                msg_0 = "Loser!"
                raise RuntimeError(msg_0)

    mock_download_client = MagicMock()
    mock_download_client.download.side_effect = file_downloader_boom

    with patch(
        "cdm_data_loaders.pipelines.all_the_bacteria.FileDownloader",
        return_value=mock_download_client,
    ):
        list(osf_file_downloader(settings, atb_file_list))

    assert mock_download_client.download.call_count == len(atb_file_list)

    expected_file_names = {
        "Two/good_file.txt": f"{settings.raw_data_dir}/One/Two/good_file.txt",
        "great_file.txt": f"{settings.raw_data_dir}/One/Two/great_file.txt",
    }

    for item in atb_file_list:
        if item["filename"] in expected_paths and expected_paths[item["filename"]]:
            assert item["path"] == expected_file_names[item["filename"]]
        else:
            assert "path" not in item

    log_messages = [r.getMessage() for r in caplog.records]
    assert expected_exceptions == log_messages


def test_run_atb_pipeline(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_atb_pipeline binds settings and delegates to run_pipeline with correct args."""
    mock_atb_file_list = MagicMock()
    mock_file_downloader = MagicMock()
    mock_run_pipeline = MagicMock(return_value="load info")

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", mock_atb_file_list)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", mock_file_downloader)
    monkeypatch.setattr(all_the_bacteria, "run_pipeline", mock_run_pipeline)

    output = run_atb_pipeline(test_settings)
    assert output == "load info"

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
        pipeline_run_kwargs={"loader_file_format": str(test_settings.loader_file_format)},
    )


def test_run_atb_pipeline_bind_order(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure the resources have the settings bound before run_pipeline is called."""
    call_order: list[str] = []

    mock_atb_file_list = MagicMock()
    mock_atb_file_list.bind.side_effect = lambda *_: call_order.append("atb_file_list.bind")

    mock_file_downloader = MagicMock()
    mock_file_downloader.bind.side_effect = lambda *_: call_order.append("file_downloader.bind")

    def track_run_pipeline(**_: Any) -> None:  # noqa: ANN401
        call_order.append("run_pipeline")

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", mock_atb_file_list)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", mock_file_downloader)
    monkeypatch.setattr(all_the_bacteria, "run_pipeline", track_run_pipeline)

    run_atb_pipeline(test_settings)

    assert call_order.index("atb_file_list.bind") < call_order.index("run_pipeline")
    assert call_order.index("file_downloader.bind") < call_order.index("run_pipeline")


@pytest.mark.parametrize("use_output_dir_for_pipeline_metadata", [True, False])
@pytest.mark.parametrize("dlt_dev_mode", [True, False])
def test_run_atb_pipeline_pipeline_dir_present_or_absent(
    use_output_dir_for_pipeline_metadata: bool,
    dlt_dev_mode: bool,
    mock_dlt: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Check that the appropriate args are passed as pipeline_kwargs."""
    output_dir = tmp_path / "output"
    settings = AtbSettings(
        input_dir=str(tmp_path),
        output_dir=str(output_dir),
        use_destination=VALID_DESTINATIONS[0],
        use_output_dir_for_pipeline_metadata=use_output_dir_for_pipeline_metadata,
        dlt_dev_mode=dlt_dev_mode,
    )  # pyright: ignore[reportCallIssue]
    mock_file_downloader = MagicMock(name="file_downloader")
    if use_output_dir_for_pipeline_metadata:
        assert settings.pipeline_dir == output_dir.resolve() / PIPELINE_DIR_NAME
    else:
        assert settings.pipeline_dir is None

    monkeypatch.setattr(all_the_bacteria, "atb_file_list", MagicMock())
    monkeypatch.setattr(all_the_bacteria, "file_downloader", mock_file_downloader)

    run_atb_pipeline(settings)

    assert mock_dlt.destination.call_args_list == [call(VALID_DESTINATIONS[0], max_table_nesting=0)]
    mock_pipeline = mock_dlt.pipeline.return_value
    assert mock_pipeline.destination == mock_dlt.destination.return_value
    assert mock_pipeline.run.call_args_list[0] == call(
        mock_file_downloader,
        loader_file_format=str(settings.loader_file_format),
    )

    pipeline_kwargs: dict[str, Any] = {"pipeline_name": DATASET_NAME, "dataset_name": DATASET_NAME}
    if use_output_dir_for_pipeline_metadata:
        pipeline_kwargs["pipelines_dir"] = settings.pipeline_dir
    if dlt_dev_mode:
        pipeline_kwargs["dev_mode"] = True

    assert mock_dlt.pipeline.call_args_list == [call(**pipeline_kwargs)]


def test_run_atb_pipeline_pass_repeated_invocation_does_not_mutate_singleton_resources_in_place(
    test_defaults: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Calling run_atb_pipeline more than once should not fail due to in-place .bind() on module-level singletons."""
    atb_file_list_copy, file_downloader_copy = copy.deepcopy(
        (all_the_bacteria.atb_file_list, all_the_bacteria.file_downloader)
    )
    monkeypatch.setattr(all_the_bacteria, "atb_file_list", atb_file_list_copy)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", file_downloader_copy)

    bound_settings: list[AtbSettings] = []

    def fake_run_pipeline(*, settings: AtbSettings, **_: Any) -> str:  # noqa: ANN401
        bound_settings.append(settings)
        return "ok"

    monkeypatch.setattr(all_the_bacteria, "run_pipeline", fake_run_pipeline)

    settings_1 = AtbSettings(**{**test_defaults, "output_dir": str(tmp_path / "out1"), "version": "1.0"})
    settings_2 = AtbSettings(**{**test_defaults, "output_dir": str(tmp_path / "out2"), "version": "2.0"})

    assert run_atb_pipeline(settings_1) == "ok"
    assert run_atb_pipeline(settings_2) == "ok"
    assert bound_settings == [settings_1, settings_2]


def test_get_file_download_links_pass_concurrent_downloads_across_worker_threads(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Multiple matching files in the ATB index TSV should be downloaded concurrently across worker threads."""
    atb_file_list_copy, file_downloader_copy = copy.deepcopy(
        (all_the_bacteria.atb_file_list, all_the_bacteria.file_downloader)
    )
    monkeypatch.setattr(all_the_bacteria, "atb_file_list", atb_file_list_copy)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", file_downloader_copy)

    tsv_path = Path("tests") / "data" / "atb" / "filtered_files.tsv"
    monkeypatch.setattr(all_the_bacteria, "download_atb_index_tsv", lambda _settings: tsv_path)

    in_flight = 0
    max_in_flight = 0
    download_threads: set[int] = set()
    lock = threading.Lock()

    mock_download_client = MagicMock()

    def record_concurrent_download(**_kwargs: Any) -> None:  # noqa: ANN401
        nonlocal in_flight, max_in_flight
        with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            download_threads.add(threading.get_ident())
        time.sleep(0.05)
        with lock:
            in_flight -= 1

    mock_download_client.download.side_effect = record_concurrent_download
    monkeypatch.setattr(all_the_bacteria, "FileDownloader", lambda: mock_download_client)

    atb_file_list_copy.bind(test_settings)
    file_downloader_copy.bind(test_settings)

    pipeline = dlt.pipeline(
        pipeline_name="test_atb_concurrent_downloads",
        destination=dlt.destinations.dummy(completed_prob=1.0),
        pipelines_dir=str(tmp_path / "dlt_pipelines"),
        dev_mode=True,
    )
    pipeline.extract(file_downloader_copy)

    assert mock_download_client.download.call_count > 1
    assert max_in_flight > 1
    assert len(download_threads) > 1


def test_file_downloader_pass_parallelized_transformer_runs_on_worker_threads(
    test_settings: AtbSettings,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """file_downloader (parallelized=True) should execute FileDownloader.download concurrently on worker threads."""
    atb_file_list_copy, file_downloader_copy = copy.deepcopy(
        (all_the_bacteria.atb_file_list, all_the_bacteria.file_downloader)
    )
    monkeypatch.setattr(all_the_bacteria, "atb_file_list", atb_file_list_copy)
    monkeypatch.setattr(all_the_bacteria, "file_downloader", file_downloader_copy)

    # Yield 4 separate 1-file batches so dlt fans out 4 calls to file_downloader, isolating the
    # parallelized=True generator-return threading bug from get_file_download_links' single-list yield.
    batches = [
        (
            {
                "project": "AllTheBacteria/Annotation/Bakta",
                "project_id": "p1",
                "filename": f"file_{idx}.tar.xz",
                "file_id": f"id_{idx}",
                "url": f"https://osf.io/download/file_{idx}",
                "md5": f"md5_{idx}",
                "size": "100",
            },
        )
        for idx in range(4)
    ]
    monkeypatch.setattr(all_the_bacteria, "download_atb_index_tsv", lambda _settings: tmp_path / ALL_ATB_FILE_NAME)
    monkeypatch.setattr(all_the_bacteria, "get_file_download_links", lambda _settings, _tsv: iter(batches))

    main_tid = threading.get_ident()
    download_threads: set[int] = set()
    lock = threading.Lock()

    mock_download_client = MagicMock()

    def record_download_thread(**_kwargs: Any) -> None:  # noqa: ANN401
        with lock:
            download_threads.add(threading.get_ident())
        time.sleep(0.05)

    mock_download_client.download.side_effect = record_download_thread
    monkeypatch.setattr(all_the_bacteria, "FileDownloader", lambda: mock_download_client)

    atb_file_list_copy.bind(test_settings)
    file_downloader_copy.bind(test_settings)

    pipeline = dlt.pipeline(
        pipeline_name="test_atb_parallel",
        destination=dlt.destinations.dummy(completed_prob=1.0),
        pipelines_dir=str(tmp_path / "dlt_pipelines"),
        dev_mode=True,
    )
    pipeline.extract(file_downloader_copy)

    assert mock_download_client.download.call_count == len(batches)
    assert main_tid not in download_threads
    assert len(download_threads) > 1
