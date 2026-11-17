"""Shared mapping, date, CURIE, and CDM entry-transform helpers for UniProt dataset transforms."""

import datetime as dt
from collections.abc import Callable, Iterable, Mapping, Sequence
from logging import Logger
from types import MappingProxyType
from typing import Any, Final

from cdm_data_loaders.core.buffer import make_rejected_table_result
from cdm_data_loaders.core.registry import RecordTransformFn, TransformContext

DEFAULT_ENTRY_TABLE_NAME: Final[str] = "entry"
UNIPROT_CURIE_NAMESPACE: Final[str] = "uniprot"
UNIPROT_CURIE_PREFIX: Final[str] = f"{UNIPROT_CURIE_NAMESPACE}:"
UNIREF_CURIE_NAMESPACE: Final[str] = "uniref"
UNIREF_CURIE_PREFIX: Final[str] = f"{UNIREF_CURIE_NAMESPACE}:"
NCBI_TAXON_PREFIX: Final[str] = "NCBITaxon"

PREFIX_TRANSLATION: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        "UniProtKB": UNIPROT_CURIE_NAMESPACE,
        "UniProtKB/Swiss-Prot": UNIPROT_CURIE_NAMESPACE,
        "UniProtKB/TrEMBL": UNIPROT_CURIE_NAMESPACE,
        "UniProtKB accession": UNIPROT_CURIE_NAMESPACE,
        "UniProtKB ID": "uniprot_name",
        "UniParc ID": "uniparc",
        "UniRef90 ID": UNIREF_CURIE_NAMESPACE,
        "UniRef50 ID": UNIREF_CURIE_NAMESPACE,
        "UniRef100 ID": UNIREF_CURIE_NAMESPACE,
        "NCBI Taxonomy": NCBI_TAXON_PREFIX,
        "NCBI taxonomy": NCBI_TAXON_PREFIX,
        "GeneID": "NCBIGene",
        "pubmed": "PMID",
    }
)

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
    return ENSEMBL_PREFIX_TYPES.get(xref[0:5], ENSEMBL_PREFIX_TYPES.get(xref[0:4], "sequence"))


def format_curie(db: str, accession: str, *, fallback_upper: bool = False) -> str:
    """Format ``db`` and ``accession`` into a BioRegistry CURIE string.

    :param db: source database name or reference type
    :type db: str
    :param accession: identifier within ``db``
    :type accession: str
    :param fallback_upper: when ``True``, uppercase ``db`` if it is absent from ``PREFIX_TRANSLATION``
    :type fallback_upper: bool
    :return: CURIE string ``<prefix>:<accession>``
    :rtype: str
    """
    fallback = db.upper() if fallback_upper else db
    return f"{PREFIX_TRANSLATION.get(db, fallback)}:{accession}"


def resolve_timestamp(timestamp: dt.datetime | None) -> dt.datetime:
    """Return ``timestamp`` when provided, else ``datetime.now(UTC)``.

    :param timestamp: explicit UTC timestamp or ``None``
    :type timestamp: dt.datetime | None
    :return: resolved UTC timestamp
    :rtype: dt.datetime
    """
    return timestamp if timestamp is not None else dt.datetime.now(dt.UTC)


def reject_cdm_entry(
    entry: Mapping[str, Any],
    error_detail: str,
    ctx: TransformContext | None = None,
    *,
    default_table: str = DEFAULT_ENTRY_TABLE_NAME,
) -> dict[str, list[dict[str, Any]]]:
    """Build a ``{_rejected: [row]}`` mapping preserving ``ctx.table_name`` and ``ctx.source_file``.

    :param entry: input XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :param error_detail: human-readable rejection message
    :type error_detail: str
    :param ctx: optional transform context carrying table name and source file
    :type ctx: TransformContext | None
    :param default_table: fallback table name when ``ctx`` is ``None``
    :type default_table: str
    :return: single-row ``_rejected`` table result mapping
    :rtype: dict[str, list[dict[str, Any]]]
    """
    table_name = ctx.table_name if ctx is not None else default_table
    source_file = ctx.source_file if ctx is not None else None
    return make_rejected_table_result(
        table_name,
        entry,
        error_detail,
        source_file=source_file,
    )


def run_cdm_entry_parser(
    entry: Mapping[str, Any],
    parse_fn: Callable[[], dict[str, list[dict[str, Any]]]],
    *,
    ctx: TransformContext | None = None,
    logger: Logger,
    error_message: str,
) -> dict[str, list[dict[str, Any]]]:
    """Execute ``parse_fn`` and route unexpected exceptions to ``_rejected`` with ``ctx`` provenance.

    :param entry: input XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :param parse_fn: zero-argument callable producing routed CDM table rows
    :type parse_fn: Callable[[], dict[str, list[dict[str, Any]]]]
    :param ctx: optional transform context carrying table name and source file
    :type ctx: TransformContext | None
    :param logger: module logger used to log unexpected exceptions
    :type logger: Logger
    :param error_message: log message emitted on unexpected parse errors
    :type error_message: str
    :return: routed CDM table rows or a single-row ``_rejected`` mapping
    :rtype: dict[str, list[dict[str, Any]]]
    """
    try:
        return parse_fn()
    except Exception as exc:
        logger.exception(error_message)
        return reject_cdm_entry(entry, str(exc), ctx)


def make_cdm_entry_transform(
    entry_fn: Callable[[Mapping[str, Any], TransformContext | None], dict[str, list[dict[str, Any]]]],
) -> RecordTransformFn:
    """Wrap ``entry_fn(entry, ctx)`` into a context-aware ``RecordTransformFn``.

    :param entry_fn: callable accepting ``(entry, ctx)`` and returning routed CDM table rows
    :type entry_fn: Callable[[Mapping[str, Any], TransformContext | None], dict[str, list[dict[str, Any]]]]
    :return: ``RecordTransformFn`` accepting ``(entry, ctx=None)``
    :rtype: RecordTransformFn
    """

    def _transform(
        entry: dict[str, Any],
        ctx: TransformContext | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        return entry_fn(entry, ctx)

    return _transform
