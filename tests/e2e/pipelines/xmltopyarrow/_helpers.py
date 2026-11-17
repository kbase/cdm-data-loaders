"""Row normalization helpers for ``tests/e2e/pipelines/xmltopyarrow``."""

from typing import Any

from tests._support.cli_assertions import strip_none_values
from tests._support.table_readers import denormalize_identifier, strip_metadata_columns


def strip_xsd_arrow_row(row: dict[str, Any]) -> dict[str, Any]:
    """Strip dlt metadata columns, unmangle top-level column names, and drop ``None`` struct fields.

    :param row: Raw Arrow-converted dictionary row.
    :type row: dict[str, Any]
    :return: Cleaned row dictionary.
    :rtype: dict[str, Any]
    """
    unmangled = {denormalize_identifier(k): v for k, v in strip_metadata_columns(row).items()}
    return strip_none_values(unmangled)


def _normalize_xsd_reference_member(member: dict[str, Any]) -> dict[str, Any]:
    """Normalize an ``xmltodict`` member dict to match XSD-typed cardinality and scalar types.

    :param member: Raw ``xmltodict`` member dictionary.
    :type member: dict[str, Any]
    :return: Normalized member dictionary.
    :rtype: dict[str, Any]
    """
    normalized = dict(member)
    db_ref = dict(normalized["dbReference"])
    if "property" in db_ref and isinstance(db_ref["property"], dict):
        db_ref["property"] = [db_ref["property"]]
    normalized["dbReference"] = db_ref
    if "sequence" in normalized and isinstance(normalized["sequence"], dict):
        seq = dict(normalized["sequence"])
        if "_length" in seq:
            seq["_length"] = int(seq["_length"])
        normalized["sequence"] = seq
    return normalized


def normalize_uniref_entry_for_xsd(raw_entry: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw ``xmltodict`` UniRef entry dict to match XSD-backed Arrow output.

    :param raw_entry: Raw ``xmltodict`` UniRef entry dictionary.
    :type raw_entry: dict[str, Any]
    :return: Normalized UniRef entry dictionary.
    :rtype: dict[str, Any]
    """
    entry = dict(raw_entry.get("entry", raw_entry))
    if "property" in entry and isinstance(entry["property"], dict):
        entry["property"] = [entry["property"]]
    entry["representativeMember"] = _normalize_xsd_reference_member(entry["representativeMember"])
    if "member" in entry:
        members = entry["member"] if isinstance(entry["member"], list) else [entry["member"]]
        entry["member"] = [_normalize_xsd_reference_member(m) for m in members]
    return entry


def reconstruct_xsd_entries(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Extract normalized XSD entry dicts from the single Arrow-backed ``entry`` table.

    :param tables: Mapping of table names to loaded row lists.
    :type tables: dict[str, list[dict[str, Any]]]
    :return: Normalized entry dictionaries.
    :rtype: list[dict[str, Any]]
    """
    return [strip_xsd_arrow_row(row) for row in tables.get("entry", [])]
