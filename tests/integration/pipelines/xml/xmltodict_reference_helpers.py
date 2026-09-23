"""Nested-entry reconstruction and reference-dataset helpers for uniref tests."""

import contextlib
import gzip
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Final

import duckdb
import pyarrow.parquet as pq

from cdm_data_loaders.converters.core.paths import set_nested
from cdm_data_loaders.core.fields import PARQUET

DLT_METADATA_PREFIX: Final[str] = "_dlt"

XMLNS_COLUMNS: Final[frozenset[str]] = frozenset({"entry__axmlns", "entry__axmlns_xsi"})

DLT_TO_XML_NAMES: Final[dict[str, str]] = {
    "aid": "@id",
    "aupdated": "@updated",
    "atype": "@type",
    "avalue": "@value",
    "alength": "@length",
    "achecksum": "@checksum",
    "_text": "#text",
    "representative_member": "representativeMember",
    "db_reference": "dbReference",
}


def read_parquet_tables(output_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Read all parquet files under a pipeline output dataset dir, grouped by table name."""
    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for parquet_file in sorted(output_dir.rglob("*.parquet")):
        tables[parquet_file.parent.name].extend(pq.read_table(parquet_file).to_pylist())
    return dict(tables)


def read_pipeline_tables(output_dir: Path, output_format: str | None = None) -> dict[str, list[dict[str, Any]]]:
    """Read all json files under a pipeline output dataset dir, grouped by table name.

    Defaults to reading jsonlines files; supply "parquet" as file format to read parquet files.
    """
    if output_format and output_format == PARQUET:
        return read_parquet_tables(output_dir)

    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for json_file in sorted(output_dir.rglob("*.json*")):
        if not json_file.is_file():
            continue
        # ignore metadata dirs
        if json_file.parent.name.startswith("_dlt"):
            continue
        # Detect gzip by magic bytes rather than relying on the extension,
        # in case a .json file is actually gzipped or vice versa.
        with json_file.open("rb") as f:
            is_gzipped = f.read(2) == b"\x1f\x8b"

        opener = gzip.open if is_gzipped else open
        with opener(json_file, "rt", encoding="utf-8") as f:
            for raw_line in f:
                with contextlib.suppress(BaseException):
                    tables[json_file.parent.name].append(json.loads(raw_line.strip()))

    return dict(tables)


def strip_metadata_columns(row: dict[str, Any]) -> dict[str, Any]:
    """Drop dlt metadata columns from a row dict."""
    return {key: value for key, value in row.items() if not key.startswith(DLT_METADATA_PREFIX)}


def denormalize_identifier(name: str) -> str:
    """Restore an xmltodict key from a dlt-normalized fragment."""
    return DLT_TO_XML_NAMES.get(name, name)


def denormalize_path(path: str) -> list[str]:
    """Split a dlt name into xml key fragments, unmangling each one."""
    return [denormalize_identifier(fragment) for fragment in path.split("__")]


def rows_as_multiset(tables: dict[str, list[dict[str, Any]]], table_name: str) -> list[str]:
    """Rows of a table as sorted JSON strings for order-insensitive comparison."""
    return sorted(
        json.dumps(strip_metadata_columns(row), sort_keys=True, default=str) for row in tables.get(table_name, [])
    )


def parse_json_string(value: Any) -> Any:
    """Parse a string holding a JSON object or array; pass any other value through.

    Parquet output with max_table_nesting=0 stores nested content as JSON strings,
    while jsonl output keeps it as parsed structures.
    """
    if isinstance(value, str) and value[:1] in ("{", "["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def reconstruct_entries(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Rebuild nested dicts from the pipeline's flattened output tables.

    The main table holds scalar columns; child tables hold list contents,
    linked by _dlt_parent_id and ordered by _dlt_list_idx. A child table's parent
    may be a main entry row or another nested row, so the walk follows parentage
    rather than table-name prefixes.
    """
    top_tables = [t for t in tables if "__" not in t]
    if not top_tables or len(top_tables) > 1:
        err_msg = "Could not find an appropriate top-level table for dataset"
        raise ValueError(err_msg)
    top_table = top_tables[0]

    main_rows = tables.get(top_table, [])
    if not main_rows:
        return []

    parent_index: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for table_name, rows in tables.items():
        if table_name == top_table or table_name.startswith("_dlt"):
            continue
        for row in rows:
            parent_index[row["_dlt_parent_id"]].append((table_name, row))

    entries: list[dict[str, Any]] = []
    for row in main_rows:
        entry: dict[str, Any] = {}
        for column, value in strip_metadata_columns(row).items():
            if column in XMLNS_COLUMNS or value is None:
                continue
            set_nested(entry, denormalize_path(column.removeprefix(f"{top_table}__")), parse_json_string(value))
        _attach_nested_rows(entry, row["_dlt_id"], parent_index, [], depth_limit=100, top_table=top_table)
        entries.append(entry)
    return entries


def _attach_nested_rows(
    target: dict[str, Any],
    parent_id: str,
    parent_index: dict[str, list[tuple[str, dict[str, Any]]]],
    parent_root_path: list[str],
    depth_limit: int,
    top_table: str,
) -> None:
    """Attach child-table rows (and their own children, recursively) to a target dict.

    parent_root_path is the target's own path from the root entry; child paths are
    made relative to it, since a child table name encodes the path from the root.
    """
    if depth_limit <= 0:
        return
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table_name, row in parent_index.get(parent_id, []):
        grouped[table_name].append(row)

    for table_name, rows in grouped.items():
        rows.sort(key=lambda r: r["_dlt_list_idx"])
        full_path = _child_table_path(table_name, top_table)
        relative_path = full_path[len(parent_root_path) :] if parent_root_path else full_path
        children = []
        for row in rows:
            child: dict[str, Any] = {}
            for column, raw in strip_metadata_columns(row).items():
                if raw is None:
                    continue
                set_nested(child, denormalize_path(column), parse_json_string(raw))
            _attach_nested_rows(child, row["_dlt_id"], parent_index, full_path, depth_limit - 1, top_table)
            children.append(child)
        set_nested(target, relative_path, children)


def _child_table_path(table_name: str, top_table: str) -> list[str]:
    """Derive the xml key path for a child table from its dlt name.

    Table names are prefixed with the parent chain (entry__entry__property); the
    leading fragments repeat the root table name and are dropped.
    """
    fragments = denormalize_path(table_name)
    while len(fragments) > 1 and fragments[0] == top_table:
        fragments = fragments[1:]
    return fragments


def reference_entries_from_duckdb(connection: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Extract reference entries from DuckDB as canonical nested Python dicts."""
    rows = connection.execute("SELECT entry FROM reference ORDER BY entry['_id']").fetchall()
    return [_normalize_reference_entry(entry) for (entry,) in rows]


def _normalize_reference_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Normalize a DuckDB STRUCT entry dict to the shape of reconstructed pipeline entries."""
    normalized: dict[str, Any] = {}
    for key, raw_value in entry.items():
        if raw_value is None:
            continue
        value = raw_value
        if isinstance(value, str) and value.startswith(("{", "[")):
            value = json.loads(value)
        normalized[key] = _normalize_reference_value(value)
    return normalized


def _normalize_reference_value(value: Any) -> Any:
    """Recursively normalize DuckDB STRUCT/LIST/JSON values to plain Python structures."""
    if isinstance(value, list):
        return [_normalize_reference_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_reference_value(item) for key, item in value.items() if item is not None}
    return value
