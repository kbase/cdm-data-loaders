"""Shared fixtures for pipelines tests."""

from collections.abc import Generator
from itertools import batched
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cdm_data_loaders.pipelines import core


def make_batcher(files: list[Path], batch_size: int = 5) -> MagicMock:
    """Return a mock BatchCursor that yields ``files`` in batches then an empty list."""
    batches = [list(b) for b in batched(files, batch_size, strict=False)]
    mock_batcher = MagicMock()
    mock_batcher.get_batch.side_effect = [*batches, []]
    return mock_batcher


@pytest.fixture
def fake_files() -> list[Path]:
    """List of five files, used for testing."""
    return [Path(f"/fake/input/part_{n}.xml") for n in [1, 2, 3, 4, 5]]


@pytest.fixture
def mock_dlt(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch dlt in core, wiring pipeline.return_value to a fresh MagicMock."""
    dlt_mock = MagicMock()
    # patch the slack_incoming_hook config value so that tests do not send slack notifications
    mock_slack_hook = MagicMock(slack_incoming_hook=None)
    dlt_mock.pipeline.return_value = MagicMock(runtime_config=mock_slack_hook)
    monkeypatch.setattr(core, "dlt", dlt_mock)

    return dlt_mock


@pytest.fixture
def patched_io() -> Generator[tuple[MagicMock, MagicMock], Any]:
    """Patch BatchCursor and stream_xml_file in core, yielding (mock_batcher_cls, mock_stream)."""
    with (
        patch("cdm_data_loaders.pipelines.core.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.core.stream_xml_file") as mock_stream,
    ):
        mock_batcher_cls.return_value = make_batcher([])
        yield mock_batcher_cls, mock_stream


@pytest.fixture
def patched_io_empty_batcher() -> Generator[tuple[MagicMock, MagicMock], Any]:
    """Patch BatchCursor to yield no files and stream_xml_file in core, yielding (mock_batcher_cls, mock_stream)."""
    with (
        patch("cdm_data_loaders.pipelines.core.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.core.stream_xml_file") as mock_stream,
    ):
        mock_batcher_cls.return_value.get_batch.return_value = []
        yield mock_batcher_cls, mock_stream
