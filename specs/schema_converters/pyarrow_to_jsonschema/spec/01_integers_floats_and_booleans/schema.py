"""PyArrow schema for 01_integers_floats_and_booleans."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("flag", pa.bool_(), nullable=False),
        pa.field("i8", pa.int8(), nullable=False),
        pa.field("i16", pa.int16(), nullable=False),
        pa.field("i32", pa.int32(), nullable=False),
        pa.field("i64", pa.int64(), nullable=False),
        pa.field("u8", pa.uint8(), nullable=False),
        pa.field("u16", pa.uint16(), nullable=False),
        pa.field("u32", pa.uint32(), nullable=False),
        pa.field("u64", pa.uint64(), nullable=False),
        pa.field("f16", pa.float16(), nullable=False),
        pa.field("f32", pa.float32(), nullable=False),
        pa.field("f64", pa.float64(), nullable=False),
    ]
)
