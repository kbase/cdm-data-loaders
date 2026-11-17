"""UniRef XML entry transforms and CDM PyArrow output schemas."""

import datetime as dt
import functools
from collections.abc import Mapping
from logging import Logger, getLogger
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.buffer import make_rejected_table_result
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, RecordTransformFn
from cdm_data_loaders.datasets.uniprot.common import format_date, iter_mappings

UNIREF_URL: Final[str] = "http://uniprot.org/uniref"
ENTRY_XML_TAG: Final[str] = f"{{{UNIREF_URL}}}entry"
UNIREF: Final[str] = "UniRef"
ENTITY_ID: Final[str] = "entity_id"

PREFIX_TRANSLATION: Final[dict[str, str]] = {
    "UniProtKB ID": "uniprot_name",
    "UniProtKB accession": "uniprot",
    "UniParc ID": "uniparc",
    "UniRef90 ID": "uniref",
    "UniRef50 ID": "uniref",
    "UniRef100 ID": "uniref",
    "NCBI taxonomy": "NCBITaxon",
}

CLUSTER_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("cluster_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("cluster_type", pa.string()),
        pa.field("protocol", pa.string()),
        pa.field("description", pa.string()),
    ]
)

ENTITY_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entity_id", pa.string()),
        pa.field("entity_type", pa.string()),
        pa.field("data_source", pa.string()),
        pa.field("data_source_entity_id", pa.string()),
        pa.field("data_source_updated", pa.string()),
        pa.field("updated", pa.timestamp("us", tz="UTC")),
    ]
)

CLUSTERMEMBER_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entity_id", pa.string()),
        pa.field("cluster_id", pa.string()),
        pa.field("is_representative", pa.bool_()),
        pa.field("is_seed", pa.bool_()),
    ]
)

UNIREF_CDM_SCHEMA_MAP: Final[ArrowSchemaRegistry] = ArrowSchemaRegistry(
    registry={
        "cluster": CLUSTER_SCHEMA,
        "entity": ENTITY_SCHEMA,
        "clustermember": CLUSTERMEMBER_SCHEMA,
    }
)

logger: Logger = getLogger(__name__)


def generate_dbxref(db: str, acc: str) -> str:
    """Format a database cross-reference using BioRegistry CURIE prefixes.

    :param db: UniRef database type string
    :type db: str
    :param acc: database identifier
    :type acc: str
    :return: CURIE-formatted identifier
    :rtype: str
    """
    return f"{PREFIX_TRANSLATION.get(db, db)}:{acc}"


def extract_cluster(entry: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract UniRef cluster and entity metadata from an XSD-converted entry dictionary.

    :param entry: XSD-converted UniRef entry dictionary
    :type entry: Mapping[str, Any]
    :return: tuple of ``(cluster_row, entity_row)`` dictionaries
    :rtype: tuple[dict[str, Any], dict[str, Any]]
    """
    entry_id = entry.get("_id")
    if not entry_id:
        return ({}, {})
    cluster_id = f"uniref:{entry_id}"
    updated_str = format_date(entry.get("_updated"))
    cluster: dict[str, Any] = {
        "cluster_id": cluster_id,
        "name": entry.get("name"),
        "cluster_type": "Protein",
        "description": None,
    }
    entity: dict[str, Any] = {
        "entity_id": cluster_id,
        "entity_type": "Cluster",
        "data_source": UNIREF,
        "data_source_entity_id": entry_id,
        "data_source_updated": updated_str,
    }

    return (cluster, entity)


def extract_cross_refs(
    dbref: Mapping[str, Any],
    cluster_id: str,
    *,
    is_representative: bool,
) -> dict[str, Any]:
    """Extract a ``clustermember`` row from a single ``dbReference`` dictionary.

    :param dbref: XSD-converted ``dbReference`` mapping
    :type dbref: Mapping[str, Any]
    :param cluster_id: parent cluster CURIE identifier
    :type cluster_id: str
    :param is_representative: whether this member is the cluster representative
    :type is_representative: bool
    :return: ``clustermember`` row dictionary, or empty dict if ``dbref`` lacks type or ID
    :rtype: dict[str, Any]
    """
    entity_db = dbref.get("_type")
    entity_xref = dbref.get("_id")
    if not entity_xref or not entity_db:
        logger.warning("Incomplete dbReference in %s", cluster_id)
        return {}

    entity_id = generate_dbxref(entity_db, entity_xref)
    properties = list(iter_mappings(dbref.get("property")))

    if entity_db == "UniProtKB ID":
        uniprot_id = next(
            (prop.get("_value") for prop in properties if prop.get("_type") == "UniProtKB accession"),
            None,
        )
        if uniprot_id:
            entity_id = f"uniprot:{uniprot_id}"
        else:
            logger.warning("Could not find UniProt accession for UniProtKB ID %s", entity_xref)

    is_seed_val = next(
        (prop.get("_value") for prop in properties if prop.get("_type") == "isSeed"),
        None,
    )
    is_seed = is_seed_val == "true"

    return {
        "entity_id": entity_id,
        "cluster_id": cluster_id,
        "is_representative": is_representative,
        "is_seed": is_seed,
    }


def parse_uniref_entry(
    entry: Mapping[str, Any],
    timestamp: dt.datetime,
    uniref_variant: str,
) -> dict[str, list[dict[str, Any]]]:
    """Parse a single XSD-converted UniRef ``entry`` dictionary into CDM table rows.

    :param entry: XSD-converted UniRef entry dictionary
    :type entry: Mapping[str, Any]
    :param timestamp: UTC timestamp assigned to ``entity.updated``
    :type timestamp: dt.datetime
    :param uniref_variant: protocol label for the UniRef variant (for example, ``"UniRef 50"``)
    :type uniref_variant: str
    :return: dictionary of parsed table rows keyed by destination table name
    :rtype: dict[str, list[dict[str, Any]]]
    """
    cluster, entity = extract_cluster(entry)
    cluster_id = cluster.get("cluster_id")
    if not cluster_id:
        logger.debug("No cluster ID found in entry")
        return make_rejected_table_result("entry", entry, "No cluster ID found in entry")

    entity["updated"] = timestamp
    cluster["protocol"] = uniref_variant

    cluster_members = [
        *(
            extract_cross_refs(repr_db, cluster_id, is_representative=True)
            for repr_member in iter_mappings(entry.get("representativeMember"))
            for repr_db in iter_mappings(repr_member.get("dbReference"))
        ),
        *(
            extract_cross_refs(mem_db, cluster_id, is_representative=False)
            for member in iter_mappings(entry.get("member"))
            for mem_db in iter_mappings(member.get("dbReference"))
        ),
    ]

    return {
        "entity": [entity],
        "cluster": [cluster],
        "clustermember": [c for c in cluster_members if c],
    }


def make_uniref_cdm_transform(
    uniref_variant: str,
    timestamp: dt.datetime | None = None,
) -> RecordTransformFn:
    """Create a ``RecordTransformFn`` that transforms XSD-parsed UniRef ``entry`` dicts into CDM tables.

    :param uniref_variant: protocol label for the UniRef variant (for example, ``"UniRef 50"``)
    :type uniref_variant: str
    :param timestamp: UTC timestamp assigned to ``entity.updated`` (defaults to ``datetime.now(UTC)``)
    :type timestamp: dt.datetime | None
    :return: ``RecordTransformFn`` transforming ``entry`` records into CDM table rows
    :rtype: RecordTransformFn
    """
    resolved_ts = timestamp if timestamp is not None else dt.datetime.now(dt.UTC)
    return functools.partial(
        parse_uniref_entry,
        timestamp=resolved_ts,
        uniref_variant=uniref_variant,
    )
