"""Shared fixtures for pipelines tests."""

import logging
from itertools import batched
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import dlt
import dlt.common.configuration.accessors
import pytest
from frozendict import frozendict

from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.all_the_bacteria import AtbSettings
from cdm_data_loaders.pipelines.cts_defaults import (
    DEFAULT_CTS_SETTINGS,
    DEFAULT_START_AT,
    VALID_DESTINATIONS,
    BatchedFileInputSettings,
    CtsSettings,
)
from cdm_data_loaders.pipelines.ncbi_rest_api import NcbiSettings
from cdm_data_loaders.pipelines.uniprot_kb import UniProtSettings
from cdm_data_loaders.pipelines.uniref import UnirefSettings

CASSETTES_DIR = "tests/cassettes"

START_AT_VALUE = 50
START_AT_STRING = "50"

TEST_DLT_CONFIG = frozendict(
    {
        "destination.local_fs.bucket_url": "/output_dir",
        "destination.local_fs.destination_type": "filesystem",
        "destination.s3.bucket_url": "s3://some/s3/bucket",
        "destination.s3.destination_type": "filesystem",
        "normalize.data_writer.disable_compression": False,
    }
)


DESTINATION_OUTPUT = TEST_DLT_CONFIG[f"destination.{DEFAULT_CTS_SETTINGS['use_destination']}.bucket_url"]

DEFAULT_CTS_SETTINGS_RECONCILED = frozendict(
    {
        **DEFAULT_CTS_SETTINGS,
        "output": DESTINATION_OUTPUT,
        "raw_data_dir": f"{DESTINATION_OUTPUT}/raw_data",
        "pipeline_dir": None,
    }
)

DEFAULT_BATCH_FILE_SETTINGS_RECONCILED = frozendict({**DEFAULT_CTS_SETTINGS_RECONCILED, "start_at": DEFAULT_START_AT})

TEST_CTS_SETTINGS = frozendict(
    {
        "dev_mode": "false",
        "input_dir": "/dir/path",
        "output": "/some/dir",
        "use_destination": VALID_DESTINATIONS[1],
        "use_output_dir_for_pipeline_metadata": "true",
    }
)

TEST_CTS_SETTINGS_EXPECTED = frozendict(
    {
        **TEST_CTS_SETTINGS,
        "dev_mode": False,
        "use_output_dir_for_pipeline_metadata": True,
    }
)

TEST_CTS_SETTINGS_RECONCILED = frozendict(
    {**TEST_CTS_SETTINGS_EXPECTED, "pipeline_dir": "/some/dir/.dlt_conf", "raw_data_dir": "/some/dir/raw_data"}
)

TEST_BATCH_FILE_SETTINGS = frozendict(
    **TEST_CTS_SETTINGS,
    start_at=START_AT_STRING,
)

TEST_BATCH_FILE_SETTINGS_EXPECTED = frozendict(
    **TEST_CTS_SETTINGS_EXPECTED,
    start_at=START_AT_VALUE,
)
TEST_BATCH_FILE_SETTINGS_RECONCILED = frozendict(
    {**TEST_BATCH_FILE_SETTINGS_EXPECTED, "pipeline_dir": "/some/dir/.dlt_conf", "raw_data_dir": "/some/dir/raw_data"}
)


DEFAULT_VCR_CONFIG = frozendict(
    {
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
)


@pytest.fixture(autouse=True)
def logging_setup(caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that the dlt logger propagates logs to the root logger, is set to INFO, and that any messages are cleared."""
    logger = logging.getLogger("dlt")
    logger.propagate = True
    caplog.set_level(logging.INFO)
    caplog.clear()


def make_batcher(files: list[Path], batch_size: int = 5) -> MagicMock:
    """Return a mock BatchCursor that yields ``files`` in batches then an empty list."""
    batches = [list(b) for b in batched(files, batch_size, strict=False)]
    mock_batcher = MagicMock()
    mock_batcher.get_batch.side_effect = [*batches, []]
    return mock_batcher


def _generate_dlt_config() -> dict[str, Any]:
    """Return a fresh DLT config dict (same shape as the conftest fixture)."""
    return {
        "destination": {"local_fs": {"bucket_url": "/output_dir"}, "s3": {"bucket_url": "s3://my-bucket/output"}},
        "destination.local_fs.bucket_url": "/output_dir",
        "destination.s3.bucket_url": "s3://my-bucket/output",
        "normalize.data_writer.disable_compression": False,
    }


def make_settings(
    settings_cls: type[CtsSettings],
    dlt_config: dict[str, Any] | None = None,
    **kwargs: str | int | Path | dict[str, Any] | dlt.common.configuration.accessors._ConfigAccessor,
) -> CtsSettings | BatchedFileInputSettings | NcbiSettings | AtbSettings:
    """Generate a validated Settings object."""
    return settings_cls(**{"dlt_config": dlt_config, **kwargs})


def make_settings_autofill_config(
    settings_cls: type[CtsSettings],
    **kwargs: str | int | Path | dict[str, Any] | bool | dlt.common.configuration.accessors._ConfigAccessor | None,
) -> CtsSettings | BatchedFileInputSettings | NcbiSettings | AtbSettings | UniProtSettings | UnirefSettings:
    """Generate a validated Settings object, supplying the dlt_config if necessary."""
    if not kwargs:
        kwargs = {}
    if "dlt_config" not in kwargs:
        kwargs["dlt_config"] = _generate_dlt_config()
    return settings_cls.model_validate(kwargs)


def check_settings(
    settings_object: CtsSettings,
    expected: dict[str, Any] | frozendict,
) -> None:
    """Check that the settings object has the expected values."""
    assert settings_object.dlt_config is not None
    assert settings_object.model_dump(exclude={"dlt_config"}) == expected

    # make sure we have both raw_data_dir and pipeline_dir
    assert "raw_data_dir" in expected
    assert "pipeline_dir" in expected
    for attr, value in expected.items():
        assert getattr(settings_object, attr) == value


@pytest.fixture
def dlt_config() -> dict[str, Any]:
    """DLT config for testing purposes."""
    return _generate_dlt_config()


@pytest.fixture
def fake_files() -> list[Path]:
    """List of five files, used for testing."""
    return [Path(f"/fake/input/part_{n}.xml") for n in [1, 2, 3, 4, 5]]


@pytest.fixture
def mock_dlt(monkeypatch: pytest.MonkeyPatch, dlt_config: dict[str, Any]) -> MagicMock:
    """Patch dlt in core, wiring pipeline.return_value to a fresh MagicMock."""
    dlt_mock = MagicMock()
    # patch the slack_incoming_hook config value so that tests do not send slack notifications
    dlt_mock.pipeline.return_value.runtime_config.slack_incoming_hook = None
    # patch the config in case dlt is used when initialising a settings object
    dlt_mock.config = dlt_config
    monkeypatch.setattr(core, "dlt", dlt_mock)
    return dlt_mock


@pytest.fixture
def patched_io(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    """Patch BatchCursor and stream_xml_file inside core; return (mock_batcher_cls, mock_stream)."""
    mock_batcher_cls = MagicMock()
    mock_stream = MagicMock(return_value=[])
    monkeypatch.setattr(core, "BatchCursor", mock_batcher_cls)
    monkeypatch.setattr(core, "stream_xml_file", mock_stream)
    return mock_batcher_cls, mock_stream


@pytest.fixture
def patched_io_empty_batcher(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    """Like patched_io but BatchCursor immediately returns an empty batch."""
    mock_batcher_cls = MagicMock()
    mock_stream = MagicMock(return_value=[])
    mock_batcher_cls.return_value = make_batcher([])
    monkeypatch.setattr(core, "BatchCursor", mock_batcher_cls)
    monkeypatch.setattr(core, "stream_xml_file", mock_stream)
    return mock_batcher_cls, mock_stream
