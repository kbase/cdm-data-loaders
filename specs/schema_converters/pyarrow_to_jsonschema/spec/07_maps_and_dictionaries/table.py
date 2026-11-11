"""Sample PyArrow table for 07_maps_and_dictionaries."""

from decimal import Decimal

import pyarrow as pa
from schema import ANNOTATION_VALUE_STRUCT, SCHEMA

TABLE = pa.Table.from_arrays(
    [
        pa.array(["PROT-001", "PROT-002"], type=pa.string()),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int16()),
            pa.array(["protein_coding", "rRNA"], type=pa.string()),
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, None], type=pa.int8()),
            pa.array([1], type=pa.int64()),
            ordered=True,
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int8()),
            pa.array([Decimal("12.50"), Decimal("0.75")], type=pa.decimal128(8, 2)),
        ),
        pa.array(
            [
                [("PF00001", {"db": "Pfam", "evalue": 1.25e-12}), ("IPR000276", None)],
                [],
            ],
            type=pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={"comment": "External database key."}),
                pa.field("value", ANNOTATION_VALUE_STRUCT, nullable=True),
            ),
        ),
        pa.array(
            [[(1, 45.5), (2, 88.25)], None],
            type=pa.map_(pa.int32(), pa.field("value", pa.float64(), nullable=False), keys_sorted=True),
        ),
    ],
    schema=SCHEMA,
)
