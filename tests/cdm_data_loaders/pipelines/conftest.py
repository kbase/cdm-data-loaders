"""Shared fixtures for pipelines tests."""

import copy
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock, patch

import dlt
import pytest

from cdm_data_loaders.core.fields import LOCAL_FS, S3
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import (
    DISABLE_COMPRESSION_ENV_VAR,
)

HOOK_ENV_VAR: Final[str] = "RUNTIME__SLACK_INCOMING_HOOK"
HOOK_PART_ENV_VARS: Final[tuple[str, ...]] = ("VARIABLE_B", "VARIABLE_T", "CHAR_STR")
VALID_DESTINATIONS = [LOCAL_FS, S3]

TEST_LOG_CONFIG_FILE: Final[Path] = Path("tests") / "data" / "pipelines" / "log_config.json"


DLT_CONFIG: Final[dict[str, Any]] = {
    "destination": {
        LOCAL_FS: {"bucket_url": "/configured/out/"},
        "s3": {"bucket_url": "s3://bucket/prefix"},
    }
}


@pytest.fixture(autouse=True)
def clean_env() -> Iterator[None]:
    """Remove env vars that core reads or writes, and restore the whole environment afterwards."""
    with patch.dict(os.environ):
        for name in (*HOOK_PART_ENV_VARS, HOOK_ENV_VAR, DISABLE_COMPRESSION_ENV_VAR):
            os.environ.pop(name, None)
        for name in [key for key in os.environ if key.startswith("CDL_")]:
            del os.environ[name]
        os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
        yield


@pytest.fixture
def slack_mock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the dlt Slack sender used by send_slack_message_carefully."""
    mock = MagicMock()
    monkeypatch.setattr(core, "send_slack_message", mock)
    return mock


@pytest.fixture
def careful_slack_mock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace send_slack_message_carefully as called by run_pipeline."""
    mock = MagicMock()
    monkeypatch.setattr(core, "send_slack_message_carefully", mock)
    return mock


@pytest.fixture
def mock_init_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Stop run_cli reconfiguring logging and point dlt.config at a plain dict."""
    mock = MagicMock()
    monkeypatch.setattr(core, "init_logger", mock)
    monkeypatch.setattr(dlt, "config", copy.deepcopy(DLT_CONFIG))
    return mock


@pytest.fixture
def mock_pipeline(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace dlt.pipeline with a factory whose pipeline has no Slack hook and returns no load info."""
    pipeline = MagicMock()
    pipeline.runtime_config.slack_incoming_hook = None
    pipeline.run.return_value = None
    factory = MagicMock(return_value=pipeline)
    monkeypatch.setattr(dlt, "pipeline", factory)
    return factory


@pytest.fixture
def mock_dlt(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace dlt.destination and dlt.pipeline with a single mock, as run_ncbi_pipeline uses both."""
    mock = MagicMock()
    mock.pipeline.return_value.runtime_config.slack_incoming_hook = None
    mock.pipeline.return_value.run.return_value = MagicMock()
    monkeypatch.setattr(dlt, "destination", mock.destination)
    monkeypatch.setattr(dlt, "pipeline", mock.pipeline)
    return mock


@pytest.fixture
def mock_build_destination(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace build_destination as called by run_pipeline."""
    mock = MagicMock()
    monkeypatch.setattr(core, "build_destination", mock)
    return mock
