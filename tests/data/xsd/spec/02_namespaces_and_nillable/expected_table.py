"""Expected PyArrow table for 02_namespaces_and_nillable (xml_tag='entry')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_id": "e1",
            "label": {"_lang": "en"},
            "comment": [{"_type": "function"}, {"_type": "subunit", "text": "Homodimer"}],
        },
        {"_id": "e2", "title": "Normal Title"},
    ],
    schema=EXPECTED_SCHEMA,
)
