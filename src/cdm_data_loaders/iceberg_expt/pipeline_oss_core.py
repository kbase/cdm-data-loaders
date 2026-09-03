"""
Usage:
    python pipeline_oss_core.py load1      # initial load, SupportTicketV1 schema
    python pipeline_oss_core.py load2      # second load, SupportTicketV2 schema (new columns)
    python pipeline_oss_core.py inspect    # print resulting columns + data via dataset()

Run load1, then load2, then inspect. Check whether `priority` and the
`assignee` object appear, and whether ticket_id 1/2 show updated values
(upsert) while 3-5 persist untouched and 6-7 are newly inserted.
"""

import json
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STORAGE_DIR = BASE_DIR / "_storage" / "oss_core"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# Local, offline Iceberg catalog: `iceberg_catalog_type = "sql"` uses a
# SQLite-backed catalog, intended by dlt for local dev/testing.
os.environ.setdefault("DESTINATION__FILESYSTEM__BUCKET_URL", f"file://{STORAGE_DIR}")
os.environ.setdefault("ICEBERG_CATALOG__ICEBERG_CATALOG_NAME", "default")
os.environ.setdefault("ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE", "sql")

import dlt  # noqa: E402  (import after env vars are set)

from cdm_data_loaders.iceberg_expt.models import SupportTicketV1, SupportTicketV2  # noqa: E402

PIPELINE_NAME = "schema_evo_oss_core"
DATASET_NAME = "schema_evo_test"
TABLE_NAME = "support_tickets"


def load_jsonl(path: Path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def make_resource(model, batch_file: str):
    @dlt.resource(
        name=TABLE_NAME,
        columns=model,
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="ticket_id",
        table_format="iceberg",
    )
    def support_tickets():
        yield from load_jsonl(DATA_DIR / batch_file)

    return support_tickets


def get_pipeline():
    return dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",
        dataset_name=DATASET_NAME,
    )


def load1():
    pipeline = get_pipeline()
    info = pipeline.run(make_resource(SupportTicketV1, "batch1_v1.jsonl"))
    print(info)


def load2():
    pipeline = get_pipeline()
    info = pipeline.run(make_resource(SupportTicketV2, "batch2_v2.jsonl"))
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


COMMANDS = {"load1": load1, "load2": load2, "inspect": inspect}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else None
    if cmd not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    COMMANDS[cmd]()
