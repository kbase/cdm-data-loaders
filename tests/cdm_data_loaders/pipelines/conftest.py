"""Shared unit-test fixtures for ``tests/cdm_data_loaders/pipelines``."""

from unittest.mock import MagicMock

import dlt
import pytest

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
    mock.pipeline.return_value.runtime_config.slack_incoming_hook = None
    mock.pipeline.return_value.run.return_value = MagicMock()
    monkeypatch.setattr(dlt, "destination", mock.destination)
    monkeypatch.setattr(dlt, "pipeline", mock.pipeline)
    return mock
