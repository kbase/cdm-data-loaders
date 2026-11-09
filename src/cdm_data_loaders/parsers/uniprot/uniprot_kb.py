"""UniProt XML Parser Core.

Contains the core logic for parsing XSD-converted UniProt XML entry dictionaries into CDM tables.
"""

import datetime as dt
import functools
import json
from collections.abc import Iterable, Mapping, Sequence
from logging import Logger, getLogger
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, RecordTransformFn
from cdm_data_loaders.utils.helpers import _ensembl_type

DB: Final[str] = "db"
DESCRIPTION: Final[str] = "description"
ENTITY_ID: Final[str] = "entity_id"
NAME: Final[str] = "name"
NCBI_TAXON: Final[str] = "NCBITaxon"
XREF: Final[str] = "xref"

# XML namespace mapping for UniProt entries
HTTPS_NS: Final[str] = "https://uniprot.org/uniprot"
NS: Final[dict[str, str]] = {"ns": HTTPS_NS, "": HTTPS_NS}
ENTRY_XML_TAG: Final[str] = f"{{{HTTPS_NS}}}entry"

# CURIE prefixes
PREFIX_TRANSLATION: Final[dict[str, str]] = {
    "UniProtKB": "UniProt",
    "UniProtKB/Swiss-Prot": "UniProt",
    "UniProtKB/TrEMBL": "UniProt",
    "NCBI Taxonomy": NCBI_TAXON,
    "GeneID": "NCBIGene",
    "pubmed": "PMID",
}

ENTITY_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entity_id", pa.string()),
        pa.field("entity_type", pa.string()),
        pa.field("data_source_entity_id", pa.string()),
        pa.field("data_source_created", pa.string()),
        pa.field("data_source_modified", pa.string()),
        pa.field("data_source_entity_version", pa.string()),
        pa.field("data_source_id", pa.string()),
        pa.field("created", pa.string()),
        pa.field("data_source", pa.string()),
        pa.field("updated", pa.timestamp("us", tz="UTC")),
    ]
)

IDENTIFIER_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entity_id", pa.string()),
        pa.field("db", pa.string()),
        pa.field("xref", pa.string()),
        pa.field("description", pa.string()),
        pa.field("relationship", pa.string()),
    ]
)

NAME_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entity_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("description", pa.string()),
    ]
)

PROTEIN_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("protein_id", pa.string()),
        pa.field("evidence_for_existence", pa.string()),
        pa.field("length", pa.int64()),
        pa.field("hash", pa.string()),
        pa.field("sequence", pa.string()),
    ]
)

ENTITY_X_PUBLICATION_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entity_id", pa.string()),
        pa.field("publication_id", pa.string()),
    ]
)

UNIPROT_CDM_SCHEMA_MAP: Final[ArrowSchemaRegistry] = ArrowSchemaRegistry(
    registry={
        "entity": ENTITY_SCHEMA,
        "identifier": IDENTIFIER_SCHEMA,
        "name": NAME_SCHEMA,
        "protein": PROTEIN_SCHEMA,
        "entity_x_publication": ENTITY_X_PUBLICATION_SCHEMA,
    }
)

logger: Logger = getLogger(__name__)


def _iter_mappings(val: object) -> Iterable[Mapping[str, Any]]:
    """Yield mappings from a single Mapping, a Sequence of Mappings, or None.

    :param val: a single Mapping, a Sequence of Mappings, or None
    :type val: object
    :return: iterable of Mapping objects
    :rtype: Iterable[Mapping[str, Any]]
    """
    if isinstance(val, Mapping):
        yield val
    elif isinstance(val, Sequence) and not isinstance(val, (str, bytes, bytearray)):
        for item in val:
            if isinstance(item, Mapping):
                yield item


def _format_date(val: object) -> str | None:
    """Format a date or string value as an ISO 8601 string, or return None.

    :param val: date, string, or None value
    :type val: object
    :return: ISO-formatted date string or None
    :rtype: str | None
    """
    if isinstance(val, dt.date):
        return val.isoformat()
    if isinstance(val, str):
        return val
    return None


def parse_identifiers(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse the UniProt accessions from an XSD-converted entry dictionary.

    :param entry: XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :return: list of identifiers in "db" / "xref" format
    :rtype: list[dict[str, Any]]
    """
    return [
        {DB: "UniProt", XREF: acc.strip(), DESCRIPTION: "UniProt accession"}
        for acc in entry.get("accession") or []
        if isinstance(acc, str) and acc.strip()
    ]


def parse_names(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse the names from an XSD-converted entry dictionary.

    :param entry: XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :return: list of names with a description of the name type
    :rtype: list[dict[str, Any]]
    """
    name_list: list[dict[str, Any]] = [
        {NAME: n.strip(), DESCRIPTION: "UniProt entry name"}
        for n in entry.get("name") or []
        if isinstance(n, str) and n.strip()
    ]
    protein = entry.get("protein")
    if isinstance(protein, Mapping):
        for name_type in ("recommended", "alternative", "submitted"):
            name_groups = list(_iter_mappings(protein.get(f"{name_type}Name")))
            for name_length in ("full", "short"):
                for group in name_groups:
                    for name_obj in _iter_mappings(group.get(f"{name_length}Name")):
                        text = name_obj.get("#text")
                        if isinstance(text, str) and text.strip():
                            name_list.append(
                                {
                                    NAME: text.strip(),
                                    DESCRIPTION: f"UniProt {name_type} {name_length} name",
                                }
                            )

    # gene names
    name_list.extend(
        {
            NAME: gene_name.get("#text"),
            DESCRIPTION: f"UniProt gene name, {gene_name.get('_type')}",
        }
        for gene in _iter_mappings(entry.get("gene"))
        for gene_name in _iter_mappings(gene.get("name"))
    )

    return name_list


def parse_cross_references(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse the dbReferences from an XSD-converted entry dictionary.

    :param entry: XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :return: list of cross references in "db" / "xref" / "description" / "relationship" format
    :rtype: list[dict[str, Any]]
    """
    refs: list[dict[str, Any]] = []
    entry_acc: str | None = None
    for dbxref in _iter_mappings(entry.get("dbReference")):
        db: str | None = dbxref.get("_type")
        xref: str | None = dbxref.get("_id")
        if not db or not xref:
            continue
        molecules = [mol_id for mol in _iter_mappings(dbxref.get("molecule")) if (mol_id := mol.get("_id"))]
        if len(molecules) > 1:
            if not entry_acc:
                accessions = [
                    acc.strip() for acc in entry.get("accession") or [] if isinstance(acc, str) and acc.strip()
                ]
                entry_acc = accessions[0] if accessions else None
            logger.warning("Found many molecules for entry %s", entry_acc)
        description_suffix = f" for UniProt:{molecules[0]}" if molecules else ""

        prop_list = [
            {"type": p.get("_type"), "value": p.get("_value")}
            for p in _iter_mappings(dbxref.get("property"))
            if p.get("_type") is not None and p.get("_value") is not None
        ]
        prop_list_dispatch_hash = {
            "Ensembl": parse_ensembl_dbxref,
            # embl/genbank
            "EMBL": parse_embl_dbxref,
            "RefSeq": parse_refseq_dbxref,
            "MANE-Select": parse_mane_dbxref,
        }

        if prop_list and db in prop_list_dispatch_hash:
            refs.extend(prop_list_dispatch_hash[db](xref, prop_list, description_suffix))
            continue

        if db == "GO":
            refs.extend([{DB: "GO", XREF: xref.removeprefix("GO:")}])
            continue

        if description_suffix:
            refs.extend([{DB: db, XREF: xref, DESCRIPTION: f"{db} ID{description_suffix}"}])
            continue

        # default parse
        refs.extend([{DB: db, XREF: xref}])
    return refs


def _parse_typed_dbxref(  # noqa: PLR0913, PLR0917
    db: str,
    label: str,
    id_type: str,
    xref: str,
    prop_list: list[dict[str, str]],
    description_suffix: str,
) -> list[dict[str, str]]:
    """Parse a dbxref with a primary typed ID and property cross-references.

    :param db: target database identifier
    :type db: str
    :param label: human-readable database label
    :type label: str
    :param id_type: identifier type label
    :type id_type: str
    :param xref: primary cross-reference value
    :type xref: str
    :param prop_list: list of property type/value mappings
    :type prop_list: list[dict[str, str]]
    :param description_suffix: optional molecule suffix
    :type description_suffix: str
    :return: list of cross-reference records
    :rtype: list[dict[str, str]]
    """
    return [
        {DB: db, XREF: xref, DESCRIPTION: f"{label} {id_type} ID{description_suffix}"},
        *[{DB: db, XREF: p["value"], DESCRIPTION: f"{label} {p['type']}{description_suffix}"} for p in prop_list],
    ]


def parse_ensembl_dbxref(xref: str, prop_list: list[dict[str, str]], description_suffix: str) -> list[dict[str, str]]:
    """Parse an Ensembl dbxref.

    :param xref: primary Ensembl cross-reference
    :type xref: str
    :param prop_list: property dicts attached to the dbReference
    :type prop_list: list[dict[str, str]]
    :param description_suffix: optional molecule suffix
    :type description_suffix: str
    :return: list of parsed Ensembl cross-reference dicts
    :rtype: list[dict[str, str]]
    """
    return _parse_typed_dbxref("ensembl", "Ensembl", _ensembl_type(xref), xref, prop_list, description_suffix)


def parse_refseq_dbxref(xref: str, prop_list: list[dict[str, str]], description_suffix: str) -> list[dict[str, str]]:
    """Parse a RefSeq dbxref.

    :param xref: primary RefSeq cross-reference
    :type xref: str
    :param prop_list: property dicts attached to the dbReference
    :type prop_list: list[dict[str, str]]
    :param description_suffix: optional molecule suffix
    :type description_suffix: str
    :return: list of parsed RefSeq cross-reference dicts
    :rtype: list[dict[str, str]]
    """
    return _parse_typed_dbxref("refseq", "RefSeq", "protein sequence", xref, prop_list, description_suffix)


def parse_embl_dbxref(xref: str, prop_list: list[dict[str, str]], description_suffix: str) -> list[dict[str, str]]:
    """Parse an EMBL / GenBank dbxref.

    :param xref: primary EMBL cross-reference
    :type xref: str
    :param prop_list: property dicts attached to the dbReference
    :type prop_list: list[dict[str, str]]
    :param description_suffix: optional molecule suffix
    :type description_suffix: str
    :return: list of parsed EMBL/GenBank cross-reference dicts
    :rtype: list[dict[str, str]]
    """
    mol_types = [p["value"] for p in prop_list if p["type"] == "molecule type"]
    description = f"EMBL/GenBank {mol_types[0]} ID" if mol_types and len(mol_types) == 1 else "EMBL/GenBank ID"
    return [
        {DB: "genbank", XREF: xref, DESCRIPTION: f"{description}{description_suffix}"},
        *[
            {DB: "genbank", XREF: p["value"], DESCRIPTION: f"EMBL/GenBank {p['type']}{description_suffix}"}
            for p in prop_list
            if p["type"] not in ("molecule type", "status")
        ],
    ]


def parse_mane_dbxref(xref: str, prop_list: list[dict[str, str]], description_suffix: str) -> list[dict[str, str]]:
    """Parse a MANE-Select dbxref.

    :param xref: primary MANE-Select cross-reference
    :type xref: str
    :param prop_list: property dicts attached to the dbReference
    :type prop_list: list[dict[str, str]]
    :param description_suffix: optional molecule suffix
    :type description_suffix: str
    :return: list of parsed MANE-Select cross-reference dicts
    :rtype: list[dict[str, str]]
    """
    likely_type = _ensembl_type(xref)
    refs = [
        {
            DB: "ensembl",
            XREF: xref,
            DESCRIPTION: f"Ensembl {likely_type} ID{description_suffix}, via MANE-Select",
        }
    ]
    for p in prop_list:
        if p["type"].startswith("RefSeq "):
            refs.append(
                {
                    DB: "refseq",
                    XREF: p["value"],
                    DESCRIPTION: f"{p['type']}{description_suffix}, via MANE-Select",
                }
            )
        elif p["value"].startswith("ENS"):
            refs.append(
                {
                    DB: "ensembl",
                    XREF: p["value"],
                    DESCRIPTION: f"Ensembl {p['type']}{description_suffix}, via MANE-Select",
                }
            )
    return refs


def parse_protein_info(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Extract information for the protein table from an XSD-converted entry dictionary.

    :param entry: XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :return: dictionary of protein properties
    :rtype: dict[str, Any]
    """
    protein: dict[str, Any] = {}
    protein_existence = entry.get("proteinExistence")
    if isinstance(protein_existence, Mapping) and protein_existence.get("_type") is not None:
        protein["evidence_for_existence"] = protein_existence.get("_type")

    seq_elem = entry.get("sequence")
    if isinstance(seq_elem, Mapping):
        protein.update(
            {
                "length": int(seq_elem.get("_length") or 0),
                "hash": seq_elem.get("_checksum"),
            }
        )
        sequence = seq_elem.get("#text")
        if isinstance(sequence, str) and sequence:
            protein["sequence"] = sequence
    return protein


def parse_organism(entry: Mapping[str, Any]) -> list[dict[str, str]]:
    """Parse the organism taxon designation from an XSD-converted entry dictionary.

    :param entry: XSD-converted entry dictionary
    :type entry: Mapping[str, Any]
    :return: list of organism taxon cross-reference dicts
    :rtype: list[dict[str, str]]
    """
    organism = entry.get("organism")
    if isinstance(organism, Mapping):
        for taxon_ref in _iter_mappings(organism.get("dbReference")):
            if taxon_ref.get("_type") == "NCBI Taxonomy" and (taxon_id := taxon_ref.get("_id")):
                return [
                    {
                        DB: NCBI_TAXON,
                        XREF: taxon_id,
                        DESCRIPTION: "UniProt taxon designation",
                        "relationship": "RO:0002162: in taxon",
                    }
                ]
    return []


def parse_references(entry: Mapping[str, Any]) -> list[dict[str, str]]:
    """Parse the publications for an entry and extract any that have prioritized database identifiers.

    :param entry: the XSD-converted UniProt entry being parsed
    :type entry: Mapping[str, Any]
    :return: list of publications associated with the entry
    :rtype: list[dict[str, str]]
    """
    publications: set[str] = set()
    for reference in _iter_mappings(entry.get("reference")):
        citation = reference.get("citation")
        if not isinstance(citation, Mapping):
            continue
        citation_refs = [
            {DB: str(ref_type).lower(), XREF: str(ref_id)}
            for ref in _iter_mappings(citation.get("dbReference"))
            if (ref_type := ref.get("_type")) and (ref_id := ref.get("_id"))
        ]
        if not citation_refs:
            continue

        all_ref_types = {c[DB] for c in citation_refs}
        ref_db_priority_order = ["doi", "pmcid", "pmid", "pubmed", "agricola"]
        unexpected_types = all_ref_types - set(ref_db_priority_order)
        if unexpected_types:
            # oh no!!
            logger.warning("Unexpected dbxref types in publications: %s", ", ".join(sorted(unexpected_types)))

        ref_found = False
        for ref_type in ref_db_priority_order:
            refs = [ref[XREF] for ref in citation_refs if ref[DB] == ref_type]
            if refs:
                publications.add(f"{PREFIX_TRANSLATION.get(ref_type, ref_type.upper())}:{refs[0]}")
                ref_found = True
                break

        # oh no!!
        if not ref_found:
            logger.warning("Could not find priority ref type for reference: %s", reference)

    return [{"publication_id": pub} for pub in publications]


def parse_uniprot_entry(
    entry: Mapping[str, Any],
    timestamp: dt.datetime,
) -> dict[str, list[dict[str, Any]]]:
    """Parse an XSD-converted UniProt entry dictionary into CDM schema tables.

    :param entry: an XSD-converted entry dictionary from a UniProt XML file
    :type entry: Mapping[str, Any]
    :param timestamp: current UTC timestamp
    :type timestamp: dt.datetime
    :return: dictionary of parsed data, indexed by table name
    :rtype: dict[str, list[dict[str, Any]]]
    """
    try:
        accessions = [acc.strip() for acc in entry.get("accession") or [] if isinstance(acc, str) and acc.strip()]
        if not accessions:
            logger.error("No accession found for entry")
            return {
                REJECTED_TABLE_NAME: [
                    {
                        "table_name": "entry",
                        "source_file": None,
                        "line_no": None,
                        "raw_record": json.dumps(dict(entry), default=str),
                        "error_detail": "No accession found",
                    }
                ]
            }

        first_acc = accessions[0]
        uniprot_id = f"uniprot:{first_acc}"
        raw_version = entry.get("_version")
        entity = {
            ENTITY_ID: uniprot_id,
            "entity_type": "protein",
            "data_source_entity_id": first_acc,
            "data_source_created": _format_date(entry.get("_created")),
            "data_source_modified": _format_date(entry.get("_modified")),
            "data_source_entity_version": str(raw_version) if raw_version is not None else None,
            "data_source_id": None,  # SwissProt / TrEMBL
            "created": None,
            "data_source": f"UniProt/{entry.get('_dataset')}",
            "updated": timestamp,
        }

        return {
            "entity": [entity],
            "identifier": [
                {ENTITY_ID: uniprot_id, **e}
                for e in [*parse_identifiers(entry), *parse_cross_references(entry), *parse_organism(entry)]
            ],
            "name": [{ENTITY_ID: uniprot_id, **e} for e in parse_names(entry)],
            "protein": [{"protein_id": uniprot_id, **parse_protein_info(entry)}],
            "entity_x_publication": [{ENTITY_ID: uniprot_id, **e} for e in parse_references(entry)],
        }
    except Exception as e:
        logger.exception("Parse error while processing entry")
        return {
            REJECTED_TABLE_NAME: [
                {
                    "table_name": "entry",
                    "source_file": None,
                    "line_no": None,
                    "raw_record": json.dumps(dict(entry), default=str),
                    "error_detail": str(e),
                }
            ]
        }


def make_uniprot_cdm_transform(
    timestamp: dt.datetime | None = None,
) -> RecordTransformFn:
    """Create a ``RecordTransformFn`` that transforms XSD-parsed UniProt ``entry`` dicts into CDM tables.

    :param timestamp: UTC timestamp assigned to ``entity.updated`` (defaults to ``datetime.now(UTC)``)
    :type timestamp: dt.datetime | None
    :return: ``RecordTransformFn`` transforming ``entry`` records into CDM table rows
    :rtype: RecordTransformFn
    """
    resolved_ts = timestamp if timestamp is not None else dt.datetime.now(dt.UTC)
    return functools.partial(parse_uniprot_entry, timestamp=resolved_ts)
