"""Pytest fixtures for ``datasets.ncbi_ftp`` unit tests."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import cdm_data_loaders.datasets.ncbi_ftp.manifest as manifest_mod
from tests.cdm_data_loaders.datasets.ncbi_ftp._helpers import MD5_CHECKSUMS_TXT


@pytest.fixture
def manifest_transfer_mocks(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Mock FTP MD5 retrieval and S3 object listing on ``datasets.ncbi_ftp.manifest``.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Namespace exposing ``retrieve`` and ``list`` ``MagicMock`` objects.
    :rtype: SimpleNamespace
    """
    mock_retrieve = MagicMock(return_value=MD5_CHECKSUMS_TXT)
    mock_list = MagicMock(return_value=["one key"])

    monkeypatch.setattr(manifest_mod, "ftp_retrieve_text", mock_retrieve)
    monkeypatch.setattr(manifest_mod, "list_objects", mock_list)

    return SimpleNamespace(retrieve=mock_retrieve, list=mock_list)
