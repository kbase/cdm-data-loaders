"""Expected PyArrow table for 01_single_tag_and_default_namespace."""

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_id": "itm-1",
            "_active": True,
            "name": "Spectrometer",
            "quantity": 4,
            "price": 1250.5,
            "released": dt.date(2025, 3, 15),
            "notes": "Calibrated",
            "tag": ["optics", "lab"],
        },
        {
            "_id": "itm-2",
            "_active": False,
            "name": "Centrifuge",
            "quantity": 1,
            "price": 890.0,
            "released": None,
            "notes": None,
            "tag": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
