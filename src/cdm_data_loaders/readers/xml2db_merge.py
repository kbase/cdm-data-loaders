"""Prepare xml2db rows for merging into iceberg tables with dlt.

`process_xml_file_with_xml2db` parses each source file, and each chunk within a file, into its own
xml2db `Document`. xml2db deduplicates "reused" tables only within one `Document`, so a run that
parses several `Document`s yields the same content-addressed row many times (see
`cdm_data_loaders.readers.xml2db_doc.is_xml2db_content_addressed_table`).

dlt merges into iceberg tables with pyiceberg's `Table.upsert`. That has two limitations this
module works around:

- It raises on a batch containing repeated key values, and does not deduplicate its input. Repeated
  rows are therefore dropped from the stream before dlt sees them.
- With a key of more than one column it builds a deeply nested match filter that crashes the
  process in native code on batches of a few thousand rows. Junction tables, which have no primary
  key of their own, are therefore given a single-column key derived from the pair of keys they link.
"""

from collections.abc import Mapping
from hashlib import sha1
from threading import Lock
from typing import Any

from frozendict import frozendict
from xml2db import DataModel

from cdm_data_loaders.readers.xml2db_doc import is_xml2db_content_addressed_table

MERGE_WRITE_DISPOSITION: frozendict[str, str] = frozendict({"disposition": "merge", "strategy": "insert-only"})

type TableHints = dict[str, dict[str, Any]]
type Rows = list[dict[str, Any]]


def _junction_key(parent_key: Any, child_key: Any) -> str:
    """Derive a single-column key for a junction row from the keys it links."""
    return sha1(f"{parent_key}\x00{child_key}".encode(), usedforsecurity=False).hexdigest()


class MergePreparer:
    """Prepare the rows of a run for an iceberg merge, and describe how to merge them.

    Safe to share between the threads of a parallelised dlt transformer. Memory use grows with the
    number of distinct content-addressed rows in the run.

    Rows of a content-addressed table are kept only the first time their primary key is seen.
    A junction row is kept only when its parent row was kept in the same call, mirroring xml2db's
    own database merge, which writes the links of a parent only when it inserts the parent.
    Repeated (parent, child) pairs within one parent are collapsed to one row. Each kept junction
    row gains a `pk_<junction table>` column holding its single-column merge key.
    """

    def __init__(self, model: DataModel) -> None:
        """Index the model's tables and junction tables.

        :param model: the xml2db `DataModel` the rows are parsed with.
        :type model: DataModel
        """
        self._lock = Lock()
        self._seen: dict[str, set[Any]] = {}
        self._content_tables: set[str] = {
            table.name for table in model.tables.values() if is_xml2db_content_addressed_table(model, table)
        }
        self._junctions: dict[str, tuple[str, str]] = {
            rel.rel_table_name: (table.name, rel.other_table.name)
            for table in model.tables.values()
            for rel in table.relations_n.values()
            if rel.other_table.is_reused
        }
        self.table_hints: TableHints = {
            **{
                table.name: {"primary_key": f"pk_{table.name}", "write_disposition": dict(MERGE_WRITE_DISPOSITION)}
                for table in model.tables.values()
            },
            **{
                name: {"primary_key": f"pk_{name}", "write_disposition": dict(MERGE_WRITE_DISPOSITION)}
                for name in self._junctions
            },
        }
        """dlt hints for each table's pages: merge on the table's primary key, skipping existing rows."""

    def prepare(self, tables: Mapping[str, Rows]) -> dict[str, Rows]:
        """Drop rows already seen in this run from one flattened `Document`, and key its junction rows.

        :param tables: table name -> rows, as returned by `flatten_xml2db_document`.
        :type tables: Mapping[str, Rows]
        :return: table name -> the rows to merge.
        :rtype: dict[str, Rows]
        """
        prepared: dict[str, Rows] = {}
        new_keys: dict[str, set[Any]] = {}
        with self._lock:
            for table_name, rows in tables.items():
                if table_name in self._junctions:
                    continue
                if table_name not in self._content_tables:
                    prepared[table_name] = list(rows)
                    continue
                seen = self._seen.setdefault(table_name, set())
                pk_column = f"pk_{table_name}"
                fresh: set[Any] = set()
                prepared[table_name] = []
                for row in rows:
                    key = row[pk_column]
                    if key in seen:
                        continue
                    seen.add(key)
                    fresh.add(key)
                    prepared[table_name].append(row)
                new_keys[table_name] = fresh

        for table_name, rows in tables.items():
            if table_name in self._junctions:
                prepared[table_name] = self._prepare_junction(table_name, rows, new_keys)
        return prepared

    def _prepare_junction(self, table_name: str, rows: Rows, new_keys: Mapping[str, set[Any]]) -> Rows:
        """Keep the junction rows of newly seen parents, once per pair, each with its merge key."""
        parent, child = self._junctions[table_name]
        parent_column = f"fk_{parent}"
        child_column = f"fk_{child}"
        fresh_parents = new_keys.get(parent)
        seen_pairs: set[tuple[Any, Any]] = set()
        prepared: Rows = []
        for row in rows:
            if fresh_parents is not None and row[parent_column] not in fresh_parents:
                continue
            pair = (row[parent_column], row[child_column])
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            prepared.append({f"pk_{table_name}": _junction_key(*pair), **row})
        return prepared
