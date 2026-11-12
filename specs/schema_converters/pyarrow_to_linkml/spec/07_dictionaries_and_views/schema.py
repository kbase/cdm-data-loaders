"""PyArrow schema for 07_dictionaries_and_views."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string_view(), nullable=False),
        pa.field("payload_view", pa.binary_view(), nullable=False),
        pa.field("category", pa.dictionary(pa.int16(), pa.string()), nullable=False),
        pa.field("rank_code", pa.dictionary(pa.int8(), pa.uint16(), ordered=True), nullable=True),
        pa.field("weight_tier", pa.dictionary(pa.int8(), pa.decimal128(6, 2)), nullable=False),
        pa.field("tags_view", pa.list_view(pa.field("item", pa.string(), nullable=False)), nullable=False),
        pa.field("scores_large_view", pa.large_list_view(pa.field("item", pa.uint8(), nullable=False)), nullable=True),
    ],
    metadata={"title": "CatalogViewRecord"},
)
