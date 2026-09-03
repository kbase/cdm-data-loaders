"""
Usage:
    python pipeline_dlt_iceberg.py load1                       # initial load, V1 schema
    python pipeline_dlt_iceberg.py load2 [--strategy STRATEGY] # second load, V2 schema
    python pipeline_dlt_iceberg.py inspect                     # print columns + data

STRATEGY defaults to "upsert". Pass "delete-insert" to test whether that
strategy (claimed by dlt-iceberg but not documented for OSS-core dlt's
Iceberg support) handles schema evolution differently.
"""

import json
import sys
from pathlib import Path

import dlt
from dlt_iceberg import iceberg

from cdm_data_loaders.iceberg_expt.models import SupportTicketV1, SupportTicketV2

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STORAGE_DIR = BASE_DIR / "_storage" / "dlt_iceberg"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

PIPELINE_NAME = "schema_evo_dlt_iceberg"
DATASET_NAME = "schema_evo_test"
TABLE_NAME = "support_tickets"

# Local, offline SQLite-backed catalog. Adjust catalog_uri/warehouse to
# match whatever your installed dlt-iceberg version expects if this
# constructor signature has changed since this was written.
destination = iceberg(
    catalog_uri=f"sqlite:///{STORAGE_DIR}/catalog.db",
    catalog_name="local",
    warehouse=str(STORAGE_DIR / "warehouse"),
    namespace=DATASET_NAME,
)


def load_jsonl(path: Path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def make_resource(model, batch_file: str, strategy: str = "upsert"):
    @dlt.resource(
        name=TABLE_NAME,
        columns=model,
        write_disposition={"disposition": "merge", "strategy": strategy},
        primary_key="ticket_id",
    )
    def support_tickets():
        yield from load_jsonl(DATA_DIR / batch_file)

    return support_tickets


def get_pipeline():
    return dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=destination,
        dataset_name=DATASET_NAME,
    )


def load1():
    pipeline = get_pipeline()
    info = pipeline.run(make_resource(SupportTicketV1, "batch1_v1.jsonl"))
    print(info)


def load2(strategy: str = "upsert"):
    pipeline = get_pipeline()
    info = pipeline.run(make_resource(SupportTicketV2, "batch2_v2.jsonl", strategy=strategy))
    print(info)


def inspect():
    pipeline = get_pipeline()
    ds = pipeline.dataset()
    df = ds[TABLE_NAME].df()
    print("=== Columns ===")
    print(list(df.columns))
    print("\n=== Row count ===")
    print(len(df))
    print("\n=== Data (sorted by ticket_id) ===")
    print(df.sort_values("ticket_id").to_string())


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] not in ("load1", "load2", "inspect"):
        print(__doc__)
        sys.exit(1)

    cmd = args[0]
    strategy = "upsert"
    if "--strategy" in args:
        strategy = args[args.index("--strategy") + 1]

    if cmd == "load1":
        load1()
    elif cmd == "load2":
        load2(strategy=strategy)
    else:
        inspect()
