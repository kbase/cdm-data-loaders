"""Shared numeric width, precision, and temporal digit constants for schema converters."""

from typing import Final

INT8_WIDTH: Final[int] = 8
INT16_WIDTH: Final[int] = 16
INT32_WIDTH: Final[int] = 32
INT32_MIN: Final[int] = -2_147_483_648
INT32_MAX: Final[int] = 2_147_483_647

DECIMAL128_BITS: Final[int] = 128
DECIMAL256_BITS: Final[int] = 256
DECIMAL128_MAX_PRECISION: Final[int] = 38
DECIMAL256_MAX_PRECISION: Final[int] = 76
WEI_PRECISION: Final[int] = DECIMAL256_MAX_PRECISION

MAX_MILLIS_DIGITS: Final[int] = 3
MAX_MICROS_DIGITS: Final[int] = 6
