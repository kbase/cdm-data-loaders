"""
Usage:
    python inspect_pyiceberg.py oss
    python inspect_pyiceberg.py dlt_iceberg

The catalog config and table identifier here are best-effort guesses
based on the local catalog setup in the two pipeline scripts. You will
likely need to adjust `warehouse` / `uri` / the table identifier string
to match what dlt actually created -- check the `_storage/<target>/`
directory for the generated SQLite catalog file and warehouse path
before running.
"""

import sys
from pathlib import Path

from pyiceberg.catalog import load_catalog

BASE_DIR = Path(__file__).parent
TABLE_IDENTIFIER = "schema_evo_test.support_tickets"

CATALOG_CONFIGS = {
    "oss": {
        "type": "sql",
        "uri": f"sqlite:///{BASE_DIR / '_storage' / 'oss_core' / 'catalog.db'}",
        "warehouse": f"file://{BASE_DIR / '_storage' / 'oss_core'}",
    },
    "dlt_iceberg": {
        "type": "sql",
        "uri": f"sqlite:///{BASE_DIR / '_storage' / 'dlt_iceberg' / 'catalog.db'}",
        "warehouse": f"file://{BASE_DIR / '_storage' / 'dlt_iceberg' / 'warehouse'}",
    },
}


def inspect(target: str):
    cfg = CATALOG_CONFIGS[target]
    catalog = load_catalog(target, **cfg)

    print(f"=== Namespaces in catalog ({target}) ===")
    print(catalog.list_namespaces())

    print("\n=== Tables in namespace 'schema_evo_test' ===")
    print(catalog.list_tables("schema_evo_test"))

    table = catalog.load_table(TABLE_IDENTIFIER)

    print("\n=== Iceberg schema (field id, name, type, doc) ===")
    for field in table.schema().fields:
        print(
            f"  id={field.field_id:<3} name={field.name:<20} "
            f"type={str(field.field_type):<20} required={field.required} doc={field.doc}"
        )

    print("\n=== Snapshot history ===")
    for snap in table.history():
        print(f"  snapshot_id={snap.snapshot_id} timestamp_ms={snap.timestamp_ms}")

    arrow_tbl = table.scan().to_arrow()
    print("\n=== Current row count ===")
    print(arrow_tbl.num_rows)

    print("\n=== Current data ===")
    print(arrow_tbl.to_pandas().sort_values("ticket_id").to_string())


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in CATALOG_CONFIGS:
        print(__doc__)
        sys.exit(1)
    inspect(sys.argv[1])
