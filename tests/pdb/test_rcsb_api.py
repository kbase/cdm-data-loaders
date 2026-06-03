"""Unit tests for cdm_data_loaders.pdb.rcsb_api."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from cdm_data_loaders.pdb.rcsb_api import (
    RCSB_ENTRY_CORE_URL,
    RCSB_PUBMED_URL,
    _to_classic_id,
    fetch_entry_core,
    fetch_entry_pubmed,
)


def _mock_response(status_code: int, json_data: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:  # noqa: PLR2004
        resp.raise_for_status.side_effect = httpx.HTTPStatusError("error", request=MagicMock(), response=resp)
    return resp


def _mock_client(response: MagicMock) -> MagicMock:
    client = MagicMock(spec=httpx.Client)
    client.get.return_value = response
    return client


# ── _to_classic_id ────────────────────────────────────────────────────────


class TestToClassicId:
    def test_extended_id_converted(self):
        assert _to_classic_id("pdb_00001abc") == "1ABC"

    def test_extended_id_leading_zeros_stripped(self):
        assert _to_classic_id("pdb_000101m1") == "101M1"

    def test_extended_id_all_zeros_uses_last_four(self):
        # edge case: "pdb_00000000" → lstrip("0") is "" → fallback to last 4
        result = _to_classic_id("pdb_00000000")
        assert result == "0000"

    def test_already_classic_is_uppercased(self):
        assert _to_classic_id("4hhb") == "4HHB"

    def test_already_uppercase_unchanged(self):
        assert _to_classic_id("4HHB") == "4HHB"


# ── fetch_entry_core ──────────────────────────────────────────────────────


class TestFetchEntryCore:
    def test_returns_json_on_success(self):
        entry_data = {"rcsb_id": "1ABC", "struct": {"title": "TEST STRUCTURE"}}
        client = _mock_client(_mock_response(200, entry_data))

        result = fetch_entry_core("pdb_00001abc", client=client)

        assert result == entry_data
        client.get.assert_called_once_with(RCSB_ENTRY_CORE_URL.format("1ABC"))

    def test_raises_on_404(self):
        client = _mock_client(_mock_response(404, {}))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_entry_core("pdb_00001abc", client=client)

    def test_raises_on_500(self):
        client = _mock_client(_mock_response(500, {}))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_entry_core("pdb_00001abc", client=client)

    def test_classic_id_used_in_url(self):
        """URL must use the classic 4-char ID, not the extended form."""
        entry_data = {"rcsb_id": "4HHB"}
        client = _mock_client(_mock_response(200, entry_data))

        fetch_entry_core("pdb_00004hhb", client=client)

        called_url = client.get.call_args[0][0]
        assert "4HHB" in called_url
        assert "pdb_00004hhb" not in called_url


# ── fetch_entry_pubmed ────────────────────────────────────────────────────


class TestFetchEntryPubmed:
    def test_returns_json_on_success(self):
        pubmed_data = {"rcsb_pubmed_abstract_text": "An abstract about proteins."}
        client = _mock_client(_mock_response(200, pubmed_data))

        result = fetch_entry_pubmed("pdb_00001abc", client=client)

        assert result == pubmed_data
        client.get.assert_called_once_with(RCSB_PUBMED_URL.format("1ABC"))

    def test_returns_none_on_404(self):
        """404 means no PubMed record — should return None, not raise."""
        client = _mock_client(_mock_response(404, {}))

        result = fetch_entry_pubmed("pdb_00001abc", client=client)

        assert result is None

    def test_raises_on_500(self):
        client = _mock_client(_mock_response(500, {}))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_entry_pubmed("pdb_00001abc", client=client)

    def test_classic_id_used_in_url(self):
        pubmed_data = {"rcsb_pubmed_abstract_text": "Abstract text."}
        client = _mock_client(_mock_response(200, pubmed_data))

        fetch_entry_pubmed("pdb_00004hhb", client=client)

        called_url = client.get.call_args[0][0]
        assert "4HHB" in called_url
