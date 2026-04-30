"""Unit tests for utils.download.graphql_client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from cdm_data_loaders.utils.download.core import DownloadError, NonRetryableDownloadError
from cdm_data_loaders.utils.download.graphql_client import GraphQLClient

GRAPHQL_URL = "https://data.rcsb.org/graphql"
SIMPLE_QUERY = "query { entries(entry_ids: [\"4HHB\"]) { rcsb_id } }"


def _mock_response(status_code: int, json_data: dict) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = str(json_data)
    return resp


class TestGraphQLClientSuccess:
    def test_returns_data_key(self):
        payload = {"data": {"entries": [{"rcsb_id": "4HHB"}]}}
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, payload)

        gql = GraphQLClient(client=mock_client, max_attempts=1)
        result = gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert result == {"entries": [{"rcsb_id": "4HHB"}]}

    def test_posts_to_correct_url(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, {"data": {}})

        gql = GraphQLClient(client=mock_client, max_attempts=1)
        gql.post_query(GRAPHQL_URL, SIMPLE_QUERY, variables={"ids": ["4HHB"]})

        mock_client.post.assert_called_once()
        call_url = mock_client.post.call_args[0][0]
        assert call_url == GRAPHQL_URL

    def test_variables_included_in_request(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, {"data": {}})
        variables = {"ids": ["4HHB", "1CBS"]}

        gql = GraphQLClient(client=mock_client, max_attempts=1)
        gql.post_query(GRAPHQL_URL, SIMPLE_QUERY, variables=variables)

        call_json = mock_client.post.call_args[1]["json"]
        assert call_json["variables"] == variables

    def test_empty_variables_sends_empty_dict(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, {"data": {}})

        gql = GraphQLClient(client=mock_client, max_attempts=1)
        gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        call_json = mock_client.post.call_args[1]["json"]
        assert call_json["variables"] == {}

    def test_missing_data_key_returns_empty_dict(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, {})

        gql = GraphQLClient(client=mock_client, max_attempts=1)
        result = gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert result == {}


class TestGraphQLClientErrors:
    def test_4xx_raises_non_retryable(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(400, {"errors": [{"message": "bad query"}]})

        gql = GraphQLClient(client=mock_client, max_attempts=3)
        with pytest.raises(NonRetryableDownloadError):
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

    def test_4xx_not_retried(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(404, {})

        gql = GraphQLClient(client=mock_client, max_attempts=3)
        with pytest.raises(NonRetryableDownloadError):
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert mock_client.post.call_count == 1

    def test_graphql_level_errors_raises_non_retryable(self):
        payload = {"data": None, "errors": [{"message": "Field not found", "locations": []}]}
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, payload)

        gql = GraphQLClient(client=mock_client, max_attempts=1)
        with pytest.raises(NonRetryableDownloadError, match="GraphQL errors"):
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

    def test_5xx_retried_up_to_max_attempts(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(503, {})

        gql = GraphQLClient(client=mock_client, max_attempts=3, min_backoff=0, max_backoff=0)
        with pytest.raises(DownloadError):
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert mock_client.post.call_count == 3

    def test_timeout_exception_retried(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.side_effect = httpx.TimeoutException("timeout")

        gql = GraphQLClient(client=mock_client, max_attempts=2, min_backoff=0, max_backoff=0)
        with pytest.raises(httpx.TimeoutException):
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert mock_client.post.call_count == 2

    def test_transport_error_retried(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.side_effect = httpx.TransportError("connection reset")

        gql = GraphQLClient(client=mock_client, max_attempts=2, min_backoff=0, max_backoff=0)
        with pytest.raises(httpx.TransportError):
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert mock_client.post.call_count == 2

    def test_5xx_succeeds_after_retry(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.side_effect = [
            _mock_response(503, {}),
            _mock_response(200, {"data": {"entries": []}}),
        ]

        gql = GraphQLClient(client=mock_client, max_attempts=3, min_backoff=0, max_backoff=0)
        result = gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        assert result == {"entries": []}
        assert mock_client.post.call_count == 2


class TestGraphQLClientContextManager:
    def test_close_called_on_exit(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(200, {"data": {}})

        with GraphQLClient(client=mock_client) as gql:
            gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        mock_client.close.assert_called_once()

    def test_close_called_on_exception(self):
        mock_client = MagicMock(spec=httpx.Client)
        mock_client.post.return_value = _mock_response(400, {})

        with pytest.raises(NonRetryableDownloadError):
            with GraphQLClient(client=mock_client) as gql:
                gql.post_query(GRAPHQL_URL, SIMPLE_QUERY)

        mock_client.close.assert_called_once()
