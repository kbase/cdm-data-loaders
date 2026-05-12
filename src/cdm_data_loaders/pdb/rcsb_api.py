"""RCSB REST API client for fetching per-entry metadata.

Provides two public functions:

* :func:`fetch_entry_core` — fetches the core entry metadata for one PDB entry
* :func:`fetch_entry_pubmed` — fetches the linked PubMed record for one PDB entry
  (returns ``None`` when no PubMed record exists)

Both functions accept the extended PDB ID (e.g. ``"pdb_00001abc"``) and
internally convert it to the classic 4-char form required by the RCSB REST API.
"""

from __future__ import annotations

from typing import Any

import httpx

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger

logger = get_cdm_logger()

RCSB_ENTRY_CORE_URL = "https://data.rcsb.org/rest/v1/core/entry/{}"
RCSB_PUBMED_URL = "https://data.rcsb.org/rest/v1/core/pubmed/{}"


def _to_classic_id(pdb_id: str) -> str:
    """Convert an extended PDB ID to the classic uppercase 4-char form.

    E.g. ``"pdb_00001abc"`` → ``"1ABC"``.

    :param pdb_id: extended PDB ID (``"pdb_XXXXXXXX"``) or already-classic ID
    :return: uppercase classic PDB ID
    """
    if pdb_id.startswith("pdb_"):
        stripped = pdb_id[4:].lstrip("0")
        classic = stripped if stripped else pdb_id[-4:]
    else:
        classic = pdb_id
    return classic.upper()


def _make_client() -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(30.0), follow_redirects=True)


def fetch_entry_core(
    pdb_id: str,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Fetch the RCSB core entry record for *pdb_id*.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param client: optional ``httpx.Client``; one is created and closed if not provided
    :return: raw RCSB entry JSON dict
    :raises httpx.HTTPStatusError: on non-200 responses
    """
    classic = _to_classic_id(pdb_id)
    url = RCSB_ENTRY_CORE_URL.format(classic)
    close_client = client is None
    if client is None:
        client = _make_client()
    try:
        logger.debug("Fetching RCSB core entry for %s from %s", classic, url)
        resp = client.get(url)
        resp.raise_for_status()
        return resp.json()
    finally:
        if close_client:
            client.close()


def fetch_entry_pubmed(
    pdb_id: str,
    client: httpx.Client | None = None,
) -> dict[str, Any] | None:
    """Fetch the RCSB PubMed record for *pdb_id*, or ``None`` if not available.

    Returns ``None`` when the entry has no linked PubMed record (HTTP 404).

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param client: optional ``httpx.Client``; one is created and closed if not provided
    :return: raw RCSB PubMed JSON dict, or ``None``
    :raises httpx.HTTPStatusError: on non-404 error responses
    """
    classic = _to_classic_id(pdb_id)
    url = RCSB_PUBMED_URL.format(classic)
    close_client = client is None
    if client is None:
        client = _make_client()
    try:
        logger.debug("Fetching RCSB PubMed record for %s from %s", classic, url)
        resp = client.get(url)
        if resp.status_code == 404:  # noqa: PLR2004
            logger.debug("No PubMed record for %s", classic)
            return None
        resp.raise_for_status()
        return resp.json()
    finally:
        if close_client:
            client.close()
