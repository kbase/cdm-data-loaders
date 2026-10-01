"""Reconstruction helpers for the xml2db-based UniRef reference tests.

xml2db (unlike xmltodict) produces a normalized relational model: one row per entry/member/
property plus many-to-many junction tables, with primary/foreign keys namespaced per source file
by `process_xml_file_with_xml2db`. dlt further normalizes table and column names to snake_case
on load. These helpers join everything back into one nested dict per entry, comparable across
pipeline runs and against the plain xmltodict-based reference.
"""

import gzip
from collections import Counter, defaultdict
from collections.abc import Collection
from functools import partial
from io import BytesIO
from json import dumps
from pathlib import Path
from typing import Any, Final

import duckdb
from dlt.common.normalizers.naming.snake_case import NamingConvention
from pyiceberg.table import StaticTable
from xml2db import DataModel, Document

from cdm_data_loaders.readers.xml2db_doc import flatten_xml2db_document
from tests.integration.pipelines.pipeline_helpers import strip_metadata_columns

ICEBERG_METADATA_DIR: Final[str] = "metadata"
ICEBERG_METADATA_GLOB: Final[str] = "*.metadata.json"
DLT_TABLE_PREFIX: Final[str] = "_dlt"
RECORD_HASH_COLUMN: Final[str] = "xml2db_record_hash"
PK_PREFIX: Final[str] = "pk_"
FK_PREFIX: Final[str] = "fk_"

ENTRY_TABLE = "entry"
PROPERTY_TABLE = "property"
MEMBER_TABLE = "representative_member"
ENTRY_PROPERTY_JUNCTION = "entry_property"
ENTRY_MEMBER_JUNCTION = "entry_member_representative_member"
MEMBER_PROPERTY_JUNCTION = "representative_member_db_reference_property"
EXPECTED_ENTRY_COUNT = 100


def read_iceberg_tables(dataset_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Read every iceberg table in a pipeline output dataset directory into lists of row dicts.

    Reads each table's latest metadata file directly with pyiceberg, so no catalog or DuckDB
    extension is needed. dlt's own bookkeeping tables are skipped.

    :param dataset_dir: the pipeline's output dataset directory (`output_dir / dataset_name`).
    :type dataset_dir: Path
    :return: table name -> list of row dicts, including dlt metadata columns; empty if nothing was loaded.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    tables: dict[str, list[dict[str, Any]]] = {}
    if not dataset_dir.is_dir():
        return tables
    for table_dir in sorted(dataset_dir.iterdir()):
        metadata_dir = table_dir / ICEBERG_METADATA_DIR
        if table_dir.name.startswith(DLT_TABLE_PREFIX) or not metadata_dir.is_dir():
            continue
        latest_metadata = max(metadata_dir.glob(ICEBERG_METADATA_GLOB))
        tables[table_dir.name] = StaticTable.from_metadata(str(latest_metadata)).scan().to_arrow().to_pylist()
    return tables


def count_rows_without_run_wide_dedup(model: DataModel, xml_files: list[Path]) -> dict[str, int]:
    """Count each table's rows when every file is parsed on its own, with no deduplication across files.

    This is what the pipeline would load if it did nothing about content repeated between files, so it
    shows how much duplication the run-wide deduplication has to remove.

    :param model: the xml2db `DataModel` to parse with.
    :type model: DataModel
    :param xml_files: gzipped XML files to parse.
    :type xml_files: list[Path]
    :return: table name (as xml2db names it) -> total rows across all files.
    :rtype: dict[str, int]
    """
    counts: Counter[str] = Counter()
    for xml_file in xml_files:
        document = Document(model)
        with gzip.open(xml_file, "rb") as fh:
            document.parse_xml(BytesIO(fh.read()), skip_validation=True)
        for table_name, rows in flatten_xml2db_document(model, document, file_key=xml_file.name).items():
            counts[table_name] += len(rows)
    return dict(counts)


def load_with_xml2db_into_duckdb(xsd_file: Path, short_name: str, xml_files: list[Path], db_path: Path) -> None:
    """Load gzipped XML files into a DuckDB database with xml2db's own database loader.

    Each file is parsed into its own `Document` and merged into one shared set of tables, in order,
    exactly as xml2db's documentation describes, so the database enforces uniqueness across files.

    :param xsd_file: the XSD describing the XML files.
    :type xsd_file: Path
    :param short_name: the xml2db data model short name.
    :type short_name: str
    :param xml_files: gzipped XML files to load, in order.
    :type xml_files: list[Path]
    :param db_path: where to create the DuckDB database file.
    :type db_path: Path
    """
    model = DataModel(
        xsd_file=str(xsd_file), short_name=short_name, connection_string=f"duckdb:///{db_path.as_posix()}"
    )
    try:
        for xml_file in xml_files:
            document = Document(model)
            with gzip.open(xml_file, "rb") as fh:
                document.parse_xml(BytesIO(fh.read()), skip_validation=True)
            document.insert_into_target_tables()
    finally:
        model.engine.dispose()


def read_duckdb_tables(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    """Read every table in a DuckDB database into lists of row dicts, under the table names xml2db gave them.

    :param db_path: the DuckDB database file.
    :type db_path: Path
    :return: table name -> list of row dicts; tables with no rows map to an empty list.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        table_names = [
            name
            for (name,) in connection.execute(
                "select table_name from information_schema.tables where table_schema = 'main'"
            ).fetchall()
        ]
        tables: dict[str, list[dict[str, Any]]] = {}
        for table_name in table_names:
            cursor = connection.execute(f'select * from "{table_name}"')
            columns = [description[0] for description in cursor.description]
            tables[table_name] = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    finally:
        connection.close()
    return tables


def normalise_names_like_dlt(tables: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """Rename tables and columns with dlt's snake_case convention, as the pipeline's loaded tables are.

    :param tables: table name -> rows, with xml2db's own (camelCase) names.
    :type tables: dict[str, list[dict[str, Any]]]
    :return: the same data under dlt's names.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    naming = NamingConvention()
    return {
        naming.normalize_table_identifier(table_name): [
            {naming.normalize_identifier(column): value for column, value in row.items()} for row in rows
        ]
        for table_name, rows in tables.items()
    }


def _hex_if_bytes(value: Any) -> Any:
    """Express a hash value as hex text, whether it is raw bytes or already hex."""
    return value.hex() if isinstance(value, bytes) else value


def all_null_columns(rows: list[dict[str, Any]]) -> set[str]:
    """The columns that are null in every row, ignoring dlt metadata columns.

    dlt does not create a column that never receives a value, whereas a database load creates every
    column of the schema, so the two differ by exactly these columns.

    :param rows: a table's rows.
    :type rows: list[dict[str, Any]]
    :return: names of the columns with no non-null value; empty if there are no rows.
    :rtype: set[str]
    """
    if not rows:
        return set()
    return {column for column in strip_metadata_columns(rows[0]) if all(row.get(column) is None for row in rows)}


def content_view(tables: dict[str, list[dict[str, Any]]], exclude: Collection[str] = ()) -> dict[str, list[str]]:
    """Describe each table's rows without depending on how rows are keyed.

    The pipeline and a database load key their rows differently (strings versus database integers).
    Each row's own key is dropped, and every foreign key is replaced by the content hash of the row it
    points to, so two loads with the same content have the same view whatever keys they used. Columns
    that are null in every row of a table are left out (see `all_null_columns`).

    :param tables: table name -> rows, under dlt's names; dlt metadata columns are ignored.
    :type tables: dict[str, list[dict[str, Any]]]
    :param exclude: names of tables to leave out.
    :type exclude: Collection[str]
    :return: table name -> sorted JSON text of each row, for every non-empty table not excluded.
    :rtype: dict[str, list[str]]
    :raises ValueError: if a foreign key points at a table that has no record hash to resolve it with.
    """
    kept = {
        name: [strip_metadata_columns(row) for row in rows]
        for name, rows in tables.items()
        if rows and name not in exclude
    }
    hash_by_key = {
        name: {row[f"{PK_PREFIX}{name}"]: _hex_if_bytes(row[RECORD_HASH_COLUMN]) for row in rows}
        for name, rows in kept.items()
        if RECORD_HASH_COLUMN in rows[0]
    }
    view: dict[str, list[str]] = {}
    for name, rows in kept.items():
        described = []
        skipped_columns = all_null_columns(rows)
        for row in rows:
            described_row: dict[str, Any] = {}
            for column, value in row.items():
                if column == f"{PK_PREFIX}{name}" or column in skipped_columns:
                    continue
                if column.startswith(FK_PREFIX):
                    target = column.removeprefix(FK_PREFIX)
                    if target not in hash_by_key:
                        err_msg = f"{name}.{column} points at {target}, which has no record hashes"
                        raise ValueError(err_msg)
                    described_row[column] = None if value is None else hash_by_key[target][value]
                else:
                    described_row[column] = _hex_if_bytes(value)
            described.append(dumps(described_row, sort_keys=True, default=str))
        view[name] = sorted(described)
    return view


def _index_by(rows: list[dict[str, Any]], key: str) -> dict[Any, dict[str, Any]]:
    """Index a list of rows by a unique column value."""
    return {row[key]: row for row in rows}


def _group_by(rows: list[dict[str, Any]], key: str) -> dict[Any, list[dict[str, Any]]]:
    """Group a list of rows by a (non-unique) column value."""
    grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return grouped


def _property_pairs(
    member_or_entry_pk: Any,
    junction_rows: dict[Any, list[dict[str, Any]]],
    properties_by_pk: dict[Any, dict[str, Any]],
) -> list[list[str]]:
    """Resolve a set of junction-table links into sorted [type, value] property pairs."""
    pairs = [
        [properties_by_pk[link["fk_property"]]["type"], properties_by_pk[link["fk_property"]]["value"]]
        for link in junction_rows.get(member_or_entry_pk, [])
    ]
    return sorted(pairs)


def _member_summary(
    member_row: dict[str, Any],
    member_property_junction: dict[Any, list[dict[str, Any]]],
    properties_by_pk: dict[Any, dict[str, Any]],
) -> dict[str, Any]:
    """Build a comparable summary of a (representative or plain) member row."""
    return {
        "dbReference_type": member_row.get("db_reference_type"),
        "dbReference_id": member_row.get("db_reference_id"),
        "sequence_length": member_row.get("sequence_length"),
        "sequence_checksum": member_row.get("sequence_checksum"),
        "sequence_value": member_row.get("sequence_value"),
        "properties": _property_pairs(
            member_row["pk_representative_member"], member_property_junction, properties_by_pk
        ),
    }


def reconstruct_xml2db_entries(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Rebuild one nested dict per UniRef entry from the xml2db pipeline's relational tables.

    :param tables: table name -> list of row dicts, as read from a pipeline's output dataset.
    :type tables: dict[str, list[dict[str, Any]]]
    :return: one dict per entry with id/name/properties/representative_member/members, comparable
        across pipeline runs and (after normalizing key ordering) against an xmltodict reference.
    :rtype: list[dict[str, Any]]
    """
    entry_rows = [strip_metadata_columns(r) for r in tables.get(ENTRY_TABLE, [])]
    property_rows = [strip_metadata_columns(r) for r in tables.get(PROPERTY_TABLE, [])]
    member_rows = [strip_metadata_columns(r) for r in tables.get(MEMBER_TABLE, [])]
    entry_property_rows = [strip_metadata_columns(r) for r in tables.get(ENTRY_PROPERTY_JUNCTION, [])]
    entry_member_rows = [strip_metadata_columns(r) for r in tables.get(ENTRY_MEMBER_JUNCTION, [])]
    member_property_rows = [strip_metadata_columns(r) for r in tables.get(MEMBER_PROPERTY_JUNCTION, [])]

    properties_by_pk = _index_by(property_rows, "pk_property")
    members_by_pk = _index_by(member_rows, "pk_representative_member")
    entry_property_junction = _group_by(entry_property_rows, "fk_entry")
    entry_member_junction = _group_by(entry_member_rows, "fk_entry")
    member_property_junction = _group_by(member_property_rows, "fk_representative_member")

    entries = []
    for entry in entry_rows:
        representative_member = members_by_pk[entry["fk_representative_member"]]
        members = [
            _member_summary(members_by_pk[link["fk_representative_member"]], member_property_junction, properties_by_pk)
            for link in entry_member_junction.get(entry["pk_entry"], [])
        ]
        entries.append(
            {
                "id": entry["id"],
                "updated": entry["updated"],
                "name": entry["name"],
                "properties": _property_pairs(entry["pk_entry"], entry_property_junction, properties_by_pk),
                "representative_member": _member_summary(
                    representative_member, member_property_junction, properties_by_pk
                ),
                "members": sorted(members, key=partial(dumps, sort_keys=True, default=str)),
            }
        )
    return entries


def assert_uniref_referential_integrity(tables: dict[str, list[dict[str, Any]]]) -> None:
    """Every foreign key in a merged xml2db dataset resolves to a row in its target table."""
    entry_pks = {row["pk_entry"] for row in tables["entry"]}
    property_pks = {row["pk_property"] for row in tables["property"]}
    member_pks = {row["pk_representative_member"] for row in tables["representative_member"]}

    assert len(entry_pks) == len(tables["entry"]) == EXPECTED_ENTRY_COUNT
    assert {row["fk_representative_member"] for row in tables["entry"]} <= member_pks

    for row in tables["entry_property"]:
        assert row["fk_entry"] in entry_pks
        assert row["fk_property"] in property_pks

    for row in tables.get("entry_member_representative_member", []):
        assert row["fk_entry"] in entry_pks
        assert row["fk_representative_member"] in member_pks

    for row in tables["representative_member_db_reference_property"]:
        assert row["fk_representative_member"] in member_pks
        assert row["fk_property"] in property_pks
