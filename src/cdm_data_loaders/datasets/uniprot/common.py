"""Shared mapping, date, and identifier helpers for UniProt dataset transforms."""

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final

ENSEMBL_PREFIX_TYPES: Final[dict[str, str]] = {
    "ENST": "transcript",
    "ENSP": "protein sequence",
    "ENSG": "gene sequence",
    "ENSE": "exon",
    "ENSFM": "protein family",
    "ENSGT": "gene tree",
    "ENSR": "regulatory feature",
}


def iter_mappings(val: object) -> Iterable[Mapping[str, Any]]:
    """Yield mappings from a single ``Mapping``, a ``Sequence`` of ``Mapping`` objects, or ``None``.

    :param val: candidate mapping, sequence of mappings, or ``None``
    :type val: object
    :return: iterable of ``Mapping`` instances
    :rtype: Iterable[Mapping[str, Any]]
    """
    if isinstance(val, Mapping):
        yield val
    elif isinstance(val, Sequence) and not isinstance(val, (str, bytes, bytearray)):
        for item in val:
            if isinstance(item, Mapping):
                yield item


def format_date(val: object) -> str | None:
    """Format a ``datetime.date`` or string value as an ISO 8601 string, or return ``None``.

    :param val: date, string, or ``None`` value
    :type val: object
    :return: ISO-formatted date string or ``None``
    :rtype: str | None
    """
    if isinstance(val, dt.date):
        return val.isoformat()
    if isinstance(val, str):
        return val
    return None


def ensembl_type(xref: str) -> str:
    """Infer the Ensembl entity category from an Ensembl identifier prefix.

    :param xref: Ensembl cross-reference identifier
    :type xref: str
    :return: entity category label
    :rtype: str
    """
    return ENSEMBL_PREFIX_TYPES.get(xref[0:4], ENSEMBL_PREFIX_TYPES.get(xref[0:5], "sequence"))
