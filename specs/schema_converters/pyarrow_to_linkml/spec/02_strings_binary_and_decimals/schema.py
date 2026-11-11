"""PyArrow schema for 02_strings_binary_and_decimals."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("utf8_str", pa.string(), nullable=False),
        pa.field("large_str", pa.large_string(), nullable=False),
        pa.field("raw_bin", pa.binary(), nullable=False),
        pa.field("large_bin", pa.large_binary(), nullable=False),
        pa.field("fixed_bin_4", pa.binary(4), nullable=False),
        pa.field("dec_standard", pa.decimal128(10, 2), nullable=False),
        pa.field("dec_integer_only", pa.decimal128(8, 0), nullable=False),
        pa.field("dec256_high_prec", pa.decimal256(50, 10), nullable=True),
    ]
)
