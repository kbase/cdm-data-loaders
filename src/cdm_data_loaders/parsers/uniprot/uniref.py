"""UniRef XML File Parser.

Core parser for UniRef XML entries.

Captures the following data about each UniRef cluster:
- ID and name
- members, including representative member and seed
- sequence of the representative member
- whether this is UniRef50, 90, or 100.

Data goes into the Cluster, ClusterMember and Entity tables.

"""

import datetime as dt
import json
from collections.abc import Mapping
from logging import Logger, getLogger
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, RecordTransformFn

UNIREF_URL = "http://uniprot.org/uniref"
ENTRY_XML_TAG = f"{{{UNIREF_URL}}}entry"
UNIREF = "UniRef"
ENTITY_ID = "entity_id"

PREFIX_TRANSLATION = {
    "UniProtKB ID": "uniprot_name",  # this is actually the uniprot name
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
    """Generate a database reference that uses BioRegistry prefixes."""
    return f"{PREFIX_TRANSLATION.get(db, db)}:{acc}"


def extract_cluster(entry: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract the UniRef cluster ID and name from an XSD-converted entry dictionary."""
    entry_id = entry.get("_id")
    if not entry_id:
        return ({}, {})
    cluster_id = f"uniref:{entry_id}"
    raw_updated = entry.get("_updated")
    updated_str = raw_updated.isoformat() if isinstance(raw_updated, dt.date) else raw_updated
    cluster = {
        "cluster_id": cluster_id,
        "name": entry.get("name"),
        "cluster_type": "Protein",
        "description": None,
        # fill in protocol afterwards
    }
    entity = {
        "entity_id": cluster_id,
        "entity_type": "Cluster",
        "data_source": UNIREF,
        "data_source_entity_id": entry_id,
        "data_source_updated": updated_str,
        # fill in other columns later
    }

    return (cluster, entity)


def extract_cross_refs(
    dbref: Mapping[str, Any],
    cluster_id: str,
    is_representative: bool,  # noqa: FBT001
) -> dict[str, Any]:
    """Extract UniRef90/50/UniParc cross references from a single ``dbReference`` dictionary."""
    entity_db = dbref.get("_type")
    entity_xref = dbref.get("_id")
    if not entity_xref or not entity_db:
        # should never happen!
        logger.warning("Incomplete dbReference in %s", cluster_id)
        return {}

    entity_id = generate_dbxref(entity_db, entity_xref)
    properties = dbref.get("property") or []

    # if this is a UniProtKB ID, check whether there is a UniProtKB accession
    if entity_db == "UniProtKB ID":
        uniprot_id = next(
            (
                prop.get("_value")
                for prop in properties
                if isinstance(prop, Mapping) and prop.get("_type") == "UniProtKB accession"
            ),
            None,
        )
        if uniprot_id:
            entity_id = f"uniprot:{uniprot_id}"
        else:
            logger.warning("Could not find UniProt accession for UniProtKB ID %s", entity_xref)

    # is this a seed member?
    is_seed_val = next(
        (prop.get("_value") for prop in properties if isinstance(prop, Mapping) and prop.get("_type") == "isSeed"),
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
    """Parse a single XSD-converted UniRef ``entry`` dictionary into CDM-friendly rows."""
    # Cluster basic info
    (cluster, entity) = extract_cluster(entry)
    cluster_id = cluster.get("cluster_id")
    if not cluster_id:
        logger.debug("No cluster ID found in entry")
        return {
            REJECTED_TABLE_NAME: [
                {
                    "table_name": "entry",
                    # FIXME: add source_file from the XML parser
                    "source_file": None,
                    "line_no": None,
                    "raw_record": json.dumps(dict(entry), default=str),
                    "error_detail": "No cluster ID found in entry",
                }
            ]
        }

    # add the current timestamp
    entity["updated"] = timestamp
    # add in the protocol (which version of UniRef this is)
    cluster["protocol"] = uniref_variant

    cluster_members: list[dict[str, Any]] = []
    # Cross references from representative and members
    repr_member = entry.get("representativeMember")
    if isinstance(repr_member, Mapping):
        repr_db = repr_member.get("dbReference")
        if isinstance(repr_db, Mapping):
            cluster_members.append(extract_cross_refs(repr_db, cluster_id, is_representative=True))

    for member in entry.get("member") or []:
        if isinstance(member, Mapping):
            mem_db = member.get("dbReference")
            if isinstance(mem_db, Mapping):
                cluster_members.append(extract_cross_refs(mem_db, cluster_id, is_representative=False))

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

    :param uniref_variant: protocol label for the UniRef variant (e.g. ``"UniRef 50"``)
    :type uniref_variant: str
    :param timestamp: UTC timestamp assigned to ``entity.updated`` (defaults to ``datetime.now(UTC)``)
    :type timestamp: dt.datetime | None
    :return: ``RecordTransformFn`` transforming ``entry`` records into CDM table rows
    """
    resolved_ts = timestamp if timestamp is not None else dt.datetime.now(dt.UTC)
    return lambda entry: parse_uniref_entry(
        entry=entry,
        timestamp=resolved_ts,
        uniref_variant=uniref_variant,
    )
