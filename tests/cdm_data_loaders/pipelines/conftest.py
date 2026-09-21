"""Shared fixtures for pipelines tests."""

import logging
from collections.abc import Generator
from itertools import batched
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock

import pytest

from cdm_data_loaders.core.settings import LoggerSettings
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.utils import cdm_logger

START_AT_VALUE: Final[int] = 50
START_AT_STRING: Final[str] = "50"
TEST_LOG_CONFIG_FILE: Final[Path] = Path("tests") / "data" / "pipelines" / "log_config.json"


def make_batcher(files: list[Path], batch_size: int = 5) -> MagicMock:
    """Return a mock NumericFileSequenceBatcher that yields ``files`` in batches then an empty list."""
    batches = [list(b) for b in batched(files, batch_size, strict=False)]
    mock_batcher = MagicMock()
    mock_batcher.get_batch.side_effect = [*batches, []]
    return mock_batcher


@pytest.fixture
def fake_files() -> list[Path]:
    """List of five files, used for testing."""
    return [Path(f"/fake/input/part_{n}.xml") for n in [1, 2, 3, 4, 5]]


@pytest.fixture(autouse=True)
def real_init_logger() -> Generator[None]:
    """Run the real ``core.init_logger`` against a simple test-data config file.

    Snapshot the process-wide logging state before the test and restore it
    afterwards, so ``dictConfig`` / dlt's ``config_root_logger`` cannot leak
    handlers, levels, or the ``ROOT_LOGGER_CONFIGURED`` global between tests.
    """
    root_logger = logging.getLogger()
    dlt_logger = logging.getLogger("dlt")
    saved = {
        "level": root_logger.level,
        "handlers": list(root_logger.handlers),
        "filters": list(root_logger.filters),
        "disabled": root_logger.disabled,
        "dlt_propagate": dlt_logger.propagate,
        "dlt_handlers": list(dlt_logger.handlers),
        "root_logger_configured": cdm_logger.ROOT_LOGGER_CONFIGURED,
    }

    core.init_logger(LoggerSettings(log_config_file=str(TEST_LOG_CONFIG_FILE)))
    yield

    # restore global logger state
    for handler in root_logger.handlers:
        if handler not in saved["handlers"]:
            handler.close()
    root_logger.handlers = saved["handlers"]
    root_logger.setLevel(saved["level"])
    root_logger.filters = saved["filters"]
    root_logger.disabled = saved["disabled"]
    dlt_logger.propagate = saved["dlt_propagate"]
    dlt_logger.handlers = saved["dlt_handlers"]
    cdm_logger.ROOT_LOGGER_CONFIGURED = saved["root_logger_configured"]


@pytest.fixture(autouse=True)
def mock_send_slack_message(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch send_slack_message in core to prevent undue slack notifications."""
    slack_mock = MagicMock()
    monkeypatch.setattr(core, "send_slack_message", slack_mock)
    return slack_mock


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
