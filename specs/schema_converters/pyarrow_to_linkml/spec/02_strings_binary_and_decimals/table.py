"""Sample PyArrow table for 02_strings_binary_and_decimals."""

from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "utf8_str": "contig_0001",
            "large_str": "ATGCNATGCNATGCN",
            "raw_bin": b"\x00\x01\x02\xff",
            "large_bin": b"kbase-large-payload",
            "fixed_bin_4": b"\xde\xad\xbe\xef",
            "dec_standard": Decimal("-12345678.95"),
            "dec_integer_only": Decimal(98765432),
            "dec256_high_prec": Decimal("0.0000000001"),
        },
        {
            "utf8_str": "",
            "large_str": "second-large-string",
            "raw_bin": b"",
            "large_bin": b"\x89PNG",
            "fixed_bin_4": b"\x00\x00\x00\x01",
            "dec_standard": Decimal("0.00"),
            "dec_integer_only": Decimal(-42),
            "dec256_high_prec": None,
        },
    ],
    schema=SCHEMA,
)
