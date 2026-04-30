"""Generic synchronous GraphQL client with retry support.

Wraps ``httpx.Client`` to POST GraphQL queries with exponential backoff on
transient errors (5xx responses, timeouts, transport errors) and immediate
re-raise on client errors (4xx).

Usage::

    from cdm_data_loaders.utils.download.graphql_client import GraphQLClient

    with GraphQLClient() as client:
        data = client.post_query(
            url="https://data.rcsb.org/graphql",
            query=\"\"\"query Entries($ids: [String!]!) {
                entries(entry_ids: $ids) { rcsb_id }
            }\"\"\",
            variables={"ids": ["4HHB", "1CBS"]},
        )
"""

import logging
from typing import Any

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.download.core import DownloadError, NonRetryableDownloadError

logger: logging.Logger = get_cdm_logger()

_RETRYABLE_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.TransportError,
    DownloadError,
)


def _get_default_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(60.0),
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        follow_redirects=True,
    )


class GraphQLClient:
    """Synchronous GraphQL client with retry on transient errors.

    Can be used as a context manager to automatically close the underlying
    ``httpx.Client``::

        with GraphQLClient() as gql:
            result = gql.post_query(url, query, variables)

    :param client: optional pre-configured ``httpx.Client``; a default client
        is created if not provided
    :param max_attempts: total attempts including the first (default 5)
    :param min_backoff: minimum retry wait in seconds (default 1)
    :param max_backoff: maximum retry wait in seconds (default 60)
    """

    def __init__(  # noqa: D107
        self,
        client: httpx.Client | None = None,
        max_attempts: int = 5,
        min_backoff: int = 1,
        max_backoff: int = 60,
    ) -> None:
        self._client = client or _get_default_client()
        self._retry = retry(
            retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(min=min_backoff, max=max_backoff),
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )

    def post_query(
        self,
        url: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL POST query and return the ``"data"`` payload.

        Retries on 5xx responses, timeouts, and transport errors.  Raises
        :class:`~cdm_data_loaders.utils.download.core.NonRetryableDownloadError`
        immediately on 4xx responses.

        :param url: GraphQL endpoint URL
        :param query: GraphQL query string
        :param variables: optional variable dict
        :return: value of the ``"data"`` key from the JSON response
        :raises NonRetryableDownloadError: on 4xx HTTP errors
        :raises DownloadError: if all retry attempts are exhausted on transient errors
        """

        @self._retry
        def _execute() -> dict[str, Any]:
            resp = self._client.post(url, json={"query": query, "variables": variables or {}})
            if 400 <= resp.status_code < 500:  # noqa: PLR2004
                msg = f"GraphQL request failed with status {resp.status_code}: {resp.text[:200]}"
                raise NonRetryableDownloadError(msg)
            if resp.status_code >= 500:  # noqa: PLR2004
                msg = f"GraphQL server error {resp.status_code}: {resp.text[:200]}"
                raise DownloadError(msg)
            payload = resp.json()
            if "errors" in payload:
                # GraphQL-level errors — treat as non-retryable
                msg = f"GraphQL errors: {payload['errors']}"
                raise NonRetryableDownloadError(msg)
            return payload.get("data", {})

        return _execute()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> "GraphQLClient":  # noqa: D105, PYI034
        return self

    def __exit__(self, *_: object) -> None:  # noqa: D105
        self.close()
