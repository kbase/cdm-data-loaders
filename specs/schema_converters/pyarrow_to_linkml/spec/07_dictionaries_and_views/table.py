"""Sample PyArrow table for 07_dictionaries_and_views."""

from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_arrays(
    [
        pa.array(["VIEW-01", "VIEW-02"], type=pa.string_view()),
        pa.array([b"\xca\xfe\xba\xbe", b"plain-bytes"], type=pa.binary_view()),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int16()),
            pa.array(["metagenome", "isolate"], type=pa.string()),
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, None], type=pa.int8()),
            pa.array([1024], type=pa.uint16()),
            ordered=True,
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int8()),
            pa.array([Decimal("15.25"), Decimal("1.00")], type=pa.decimal128(6, 2)),
        ),
        pa.array([["marine", "pelagic"], []], type=pa.list_view(pa.field("item", pa.string(), nullable=False))),
        pa.array([[10, 200], None], type=pa.large_list_view(pa.field("item", pa.uint8(), nullable=False))),
    ],
    schema=SCHEMA,
)
