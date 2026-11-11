"""Sample PyArrow table for 01_integers_floats_and_booleans."""

import numpy as np
import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "flag": True,
            "i8": -128,
            "i16": -32768,
            "i32": -2147483648,
            "i64": -9223372036854775808,
            "u8": 0,
            "u16": 0,
            "u32": 0,
            "u64": 0,
            "f16": np.float16(-1.5),
            "f32": -0.25,
            "f64": -123456.7890125,
        },
        {
            "flag": False,
            "i8": 127,
            "i16": 32767,
            "i32": 2147483647,
            "i64": 9223372036854775807,
            "u8": 255,
            "u16": 65535,
            "u32": 4294967295,
            "u64": 18446744073709551615,
            "f16": np.float16(3.5),
            "f32": 2.5,
            "f64": 0.0,
        },
    ],
    schema=SCHEMA,
)
