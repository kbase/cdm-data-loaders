"""Shared unit-test fixtures for ``tests/cdm_data_loaders/pipelines``."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import dlt
import pytest
from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines import runner


@pytest.fixture
def careful_slack_mock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace ``send_slack_message_carefully`` as called by ``run_pipeline``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Installed ``MagicMock``.
    :rtype: MagicMock
    """
    mock = MagicMock()
    monkeypatch.setattr(runner, "send_slack_message_carefully", mock)
    return mock


@pytest.fixture
def mock_dlt(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace ``dlt.destination`` and ``dlt.pipeline`` with a single composite ``MagicMock``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Installed ``MagicMock``.
    :rtype: MagicMock
    """
    mock = MagicMock()
    mock_pipeline = mock.pipeline.return_value
    mock_pipeline.pipeline_name = "mock_pipeline"
    mock_pipeline.dataset_name = "mock_dataset"
    mock_pipeline.runtime_config.slack_incoming_hook = None
    mock_pipeline.last_trace.started_at = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    mock_pipeline.last_trace.finished_at = datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC)
    mock_load_info = MagicMock(spec=LoadInfo)
    mock_load_info.__len__.return_value = 1
    mock_load_info.has_failed_jobs = False
    mock_load_info.loads_ids = ["1.0"]
    mock_load_info.load_packages = []
    mock_load_info.asdict.return_value = {"loads_ids": ["1.0"]}
    mock_pipeline.run.return_value = mock_load_info
    monkeypatch.setattr(dlt, "destination", mock.destination)
    monkeypatch.setattr(dlt, "pipeline", mock.pipeline)
    return mock
