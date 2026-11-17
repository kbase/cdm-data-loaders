"""Shared constants and XML helper functions for UniProt dataset tests."""

from typing import Final

from frozendict import frozendict

SAMPLE_RAW_ROWS: Final[tuple[frozendict[str, str], ...]] = (
    frozendict({"uniprot_id": "P12345", "db_reference_type": "NCBI_TaxID", "db_reference_id": "9606"}),
    frozendict({"uniprot_id": "Q8N158", "db_reference_type": "GeneID", "db_reference_id": "284697"}),
)


def wrap_entry_xml(xml: str, namespace: str) -> str:
    """Wrap XML text in an ``<entry xmlns='...'>`` element.

    :param xml: Inner XML payload.
    :type xml: str
    :param namespace: Target XML namespace URI.
    :type namespace: str
    :return: Wrapped ``<entry>`` XML document fragment.
    :rtype: str
    """
    return f"<entry xmlns='{namespace}'>{xml}</entry>"
