"""Tests for rcsb_metadata.fetch."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from cdm_data_loaders.rcsb_metadata.fetch import (
    DEFAULT_BATCH_SIZE,
    RCSB_ENTRY_IDS_URL,
    _batched,
    fetch_entity,
    fetch_entry_ids,
)
from cdm_data_loaders.utils.download.graphql_client import GraphQLClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _http_response(status_code: int, json_data) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:  # noqa: PLR2004
        resp.raise_for_status.side_effect = httpx.HTTPStatusError("error", request=MagicMock(), response=resp)
    return resp


def _gql_client(return_data: list[dict]) -> MagicMock:
    """Return a mock GraphQLClient whose post_query returns entries from return_data."""
    mock = MagicMock(spec=GraphQLClient)
    mock.post_query.return_value = {"entries": return_data}
    return mock


# ---------------------------------------------------------------------------
# _batched helper
# ---------------------------------------------------------------------------


class TestBatched:
    def test_single_batch(self):
        result = list(_batched(["a", "b", "c"], 10))
        assert result == [["a", "b", "c"]]

    def test_multiple_batches(self):
        ids = list(range(5))
        result = list(_batched(ids, 2))
        assert result == [[0, 1], [2, 3], [4]]

    def test_exact_multiple(self):
        ids = list(range(4))
        result = list(_batched(ids, 2))
        assert result == [[0, 1], [2, 3]]

    def test_empty(self):
        result = list(_batched([], 10))
        assert result == []


# ---------------------------------------------------------------------------
# fetch_entry_ids
# ---------------------------------------------------------------------------


class TestFetchEntryIds:
    def test_returns_list_of_ids(self):
        ids = ["4HHB", "1CBS", "2RH1"]
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = _http_response(200, ids)

        result = fetch_entry_ids(client=mock_client)

        assert result == ids
        mock_client.get.assert_called_once_with(RCSB_ENTRY_IDS_URL)

    def test_uses_custom_url(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = _http_response(200, ["4HHB"])

        fetch_entry_ids(url="https://example.com/ids", client=mock_client)

        mock_client.get.assert_called_once_with("https://example.com/ids")

    def test_raises_on_http_error(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = _http_response(500, {})

        with pytest.raises(httpx.HTTPStatusError):
            fetch_entry_ids(client=mock_client)

    def test_does_not_close_provided_client(self):
        """When caller provides the client, we should not close it."""
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.get.return_value = _http_response(200, [])

        fetch_entry_ids(client=mock_client)

        mock_client.close.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_entity
# ---------------------------------------------------------------------------


class TestFetchEntity:
    def test_yields_entry_records(self):
        records = [{"rcsb_id": "4HHB"}, {"rcsb_id": "1CBS"}]
        gql = _gql_client(records)

        result = list(fetch_entity("entries", ["4HHB", "1CBS"], gql_client=gql))

        assert result == records

    def test_batches_ids(self):
        gql = _gql_client([])
        ids = [f"ID{i:04d}" for i in range(7)]

        list(fetch_entity("entries", ids, gql_client=gql, batch_size=3))

        # Should be called 3 times: [0:3], [3:6], [6:7]
        assert gql.post_query.call_count == 3

    def test_passes_batch_to_query(self):
        gql = _gql_client([{"rcsb_id": "4HHB"}])
        ids = ["4HHB", "1CBS"]

        list(fetch_entity("entries", ids, gql_client=gql, batch_size=2))

        call_kwargs = gql.post_query.call_args[1]
        assert call_kwargs["variables"]["ids"] == ids

    def test_empty_entries_key_yields_nothing(self):
        gql = MagicMock(spec=GraphQLClient)
        gql.post_query.return_value = {}  # no "entries" key

        result = list(fetch_entity("entries", ["4HHB"], gql_client=gql))

        assert result == []

    def test_none_entries_value_yields_nothing(self):
        gql = MagicMock(spec=GraphQLClient)
        gql.post_query.return_value = {"entries": None}

        result = list(fetch_entity("entries", ["4HHB"], gql_client=gql))

        assert result == []

    def test_does_not_close_provided_gql_client(self):
        gql = _gql_client([])
        list(fetch_entity("entries", [], gql_client=gql))
        gql.close.assert_not_called()

    def test_uses_custom_graphql_url(self):
        gql = _gql_client([])
        custom_url = "https://staging.rcsb.org/graphql"

        list(fetch_entity("entries", ["4HHB"], gql_client=gql, graphql_url=custom_url))

        call_url = gql.post_query.call_args[0][0]
        assert call_url == custom_url

    def test_closes_auto_created_gql_client(self):
        """When no gql_client is provided, the auto-created one should be closed."""
        mock_gql = _gql_client([{"rcsb_id": "4HHB"}])
        with patch("cdm_data_loaders.rcsb_metadata.fetch.GraphQLClient", return_value=mock_gql):
            list(fetch_entity("entries", ["4HHB"]))
        mock_gql.close.assert_called_once()


class TestFetchEntryIdsAutoClient:
    def test_closes_auto_created_http_client(self):
        """When no client is provided, the auto-created httpx.Client should be closed."""
        mock_client = MagicMock()
        mock_client.get.return_value = _http_response(200, ["4HHB"])
        with patch("cdm_data_loaders.rcsb_metadata.fetch.httpx.Client", return_value=mock_client):
            fetch_entry_ids()
        mock_client.close.assert_called_once()
