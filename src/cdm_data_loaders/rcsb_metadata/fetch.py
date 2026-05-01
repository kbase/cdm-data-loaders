"""Fetch PDB entry IDs and per-entity metadata from the RCSB GraphQL API.

Two public functions:

* :func:`fetch_entry_ids` — returns all released PDB entry IDs (~226 K)
* :func:`fetch_entity` — yields one dict per entry for a given entity type,
  batching the IDs to avoid oversized requests

The RCSB entry-ID endpoint returns a simple JSON array; no authentication
is needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

import httpx

from cdm_data_loaders.rcsb_metadata.queries import get_query
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.download.graphql_client import GraphQLClient

logger = get_cdm_logger()

RCSB_GRAPHQL_URL = "https://data.rcsb.org/graphql"
RCSB_ENTRY_IDS_URL = "https://data.rcsb.org/rest/v1/holdings/current/entry_ids"
DEFAULT_BATCH_SIZE = 1000


def fetch_entry_ids(
    url: str = RCSB_ENTRY_IDS_URL,
    client: httpx.Client | None = None,
) -> list[str]:
    """Return the list of all released PDB entry IDs from the RCSB holdings endpoint.

    :param url: URL of the entry-IDs JSON array
    :param client: optional ``httpx.Client``; one is created if not provided
    :return: list of PDB entry ID strings (e.g. ``["4HHB", "1CBS", …]``)
    """
    close_client = client is None
    if client is None:
        client = httpx.Client(timeout=httpx.Timeout(60.0), follow_redirects=True)
    try:
        logger.debug("Fetching released entry IDs from %s", url)
        resp = client.get(url)
        resp.raise_for_status()
        ids: list[str] = resp.json()
        logger.debug("Retrieved %d released entry IDs", len(ids))
        return ids
    finally:
        if close_client:
            client.close()


def _batched(items: list[str], size: int) -> Generator[list[str]]:
    """Yield successive *size*-length chunks of *items*."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_entity(
    entity_type: str,
    pdb_ids: list[str],
    gql_client: GraphQLClient | None = None,
    graphql_url: str = RCSB_GRAPHQL_URL,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Generator[dict[str, Any]]:
    """Yield one result dict per PDB entry for *entity_type*.

    Batches *pdb_ids* into groups of *batch_size* to avoid oversized requests.
    Each yielded dict is the raw RCSB response object for one entry.

    :param entity_type: one of the keys in :data:`~cdm_data_loaders.rcsb_metadata.queries.ENTITY_QUERIES`
    :param pdb_ids: list of PDB entry ID strings
    :param gql_client: optional :class:`~cdm_data_loaders.utils.download.graphql_client.GraphQLClient`;
        one is created (and closed) if not provided
    :param graphql_url: RCSB GraphQL endpoint URL
    :param batch_size: number of IDs per GraphQL request
    :yields: one dict per PDB entry from the ``entries`` list in the response
    """
    query = get_query(entity_type)
    close_client = gql_client is None
    if gql_client is None:
        gql_client = GraphQLClient()

    total = len(pdb_ids)
    processed = 0
    try:
        for batch in _batched(pdb_ids, batch_size):
            data = gql_client.post_query(graphql_url, query, variables={"ids": batch})
            entries: list[dict[str, Any]] = data.get("entries") or []
            yield from entries
            processed += len(batch)
            logger.debug("Fetched %s: %d/%d IDs processed", entity_type, processed, total)
    finally:
        if close_client:
            gql_client.close()
