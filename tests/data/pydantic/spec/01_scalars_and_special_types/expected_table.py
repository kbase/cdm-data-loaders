"""Expected PyArrow table for 01_scalars_and_special_types."""

# ruff: noqa: INP001

import datetime as dt
from decimal import Decimal

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "rec-001",
            "active": True,
            "count": 42,
            "ratio": 0.875,
            "price": Decimal("199.95"),
            "unconstrained_dec": 3.14159,
            "release_date": dt.date(2026, 4, 10),
            "created_at": dt.datetime(2026, 4, 10, 12, 30, 0, tzinfo=dt.UTC),
            "local_ts": dt.datetime(2026, 4, 10, 12, 30, 0, tzinfo=dt.UTC).replace(tzinfo=None),
            "alarm_time": dt.time(8, 15, 30),
            "elapsed": "PT1H30M",
            "item_uuid": "12345678-1234-5678-1234-567812345678",
            "config_path": "/etc/cdm/loader.yaml",
            "host_ip": "192.168.1.10",
            "homepage": "https://kbase.us/data",
            "contact_email": "info@kbase.us",
            "api_token": "top-secret-token",
            "raw_bytes": b"hello",
            "secret_blob": b"key",
            "encoded_payload": b"python",
            "encoded_text": "text",
            "optional_note": "Full scalar record",
        },
        {
            "id": "rec-002",
            "active": False,
            "count": 1,
            "ratio": 0.0,
            "price": Decimal("0.50"),
            "unconstrained_dec": 2.5,
            "release_date": dt.date(2025, 12, 31),
            "created_at": dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.UTC),
            "local_ts": dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.UTC).replace(tzinfo=None),
            "alarm_time": dt.time(0, 0, 0),
            "elapsed": "PT45S",
            "item_uuid": "87654321-4321-8765-4321-876543218765",
            "config_path": "/var/data/input.json",
            "host_ip": "10.0.0.1",
            "homepage": "https://example.org/",
            "contact_email": "admin@example.org",
            "api_token": "secondary-token",
            "raw_bytes": b"world",
            "secret_blob": b"sec",
            "encoded_payload": b"arrow",
            "encoded_text": "cdm",
            "optional_note": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
