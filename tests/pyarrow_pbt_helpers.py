"""Shared Hypothesis strategies and PyArrow/Parquet round-trip assertions for converter property tests."""

import base64
import datetime as dt
import io
import json
from decimal import Decimal
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

INT8_MIN: Final[int] = -128
INT8_MAX: Final[int] = 127
INT16_MIN: Final[int] = -32768
INT16_MAX: Final[int] = 32767
INT32_MIN: Final[int] = -(2**31)
INT32_MAX: Final[int] = 2**31 - 1
INT64_MIN: Final[int] = -(2**63)
INT64_MAX: Final[int] = 2**63 - 1

UINT8_MAX: Final[int] = 255
UINT16_MAX: Final[int] = 65535
UINT32_MAX: Final[int] = 2**32 - 1
UINT64_MAX: Final[int] = 2**64 - 1

MIN_SAFE_DATE: Final[dt.date] = dt.date(1900, 1, 1)
MAX_SAFE_DATE: Final[dt.date] = dt.date(2200, 12, 31)
MIN_SAFE_NAIVE_DT: Final[dt.datetime] = dt.datetime(1900, 1, 1, 0, 0, 0, tzinfo=dt.UTC).replace(tzinfo=None)
MAX_SAFE_NAIVE_DT: Final[dt.datetime] = dt.datetime(2200, 12, 31, 23, 59, 59, tzinfo=dt.UTC).replace(tzinfo=None)


def encode_base64_ascii(raw: bytes) -> str:
    """Encode raw bytes as an ASCII Base64 string.

    :param raw: Input byte string.
    :type raw: bytes
    :return: Base64-encoded ASCII string.
    :rtype: str
    """
    return base64.b64encode(raw).decode("ascii")


def safe_text_strategy(min_size: int = 0, max_size: int = 24) -> SearchStrategy[str]:
    """Build a strategy for printable Unicode text without control or surrogate characters.

    :param min_size: Minimum character length.
    :type min_size: int
    :param max_size: Maximum character length.
    :type max_size: int
    :return: Hypothesis strategy yielding strings.
    :rtype: SearchStrategy[str]
    """
    return st.text(
        alphabet=st.characters(blacklist_categories=("Cs", "Cc")),
        min_size=min_size,
        max_size=max_size,
    )


def stripped_text_strategy(min_size: int = 1, max_size: int = 24) -> SearchStrategy[str]:
    """Build a strategy for non-empty printable strings without leading or trailing whitespace.

    :param min_size: Minimum character length before stripping.
    :type min_size: int
    :param max_size: Maximum character length.
    :type max_size: int
    :return: Hypothesis strategy yielding non-empty stripped strings.
    :rtype: SearchStrategy[str]
    """
    eff_min = max(1, min_size)
    eff_max = max(eff_min, max_size)
    alphabet = st.characters(blacklist_categories=("Cs", "Cc", "Zs", "Zl", "Zp"))
    return st.text(alphabet=alphabet, min_size=eff_min, max_size=eff_max)


def iso_date_str_strategy() -> SearchStrategy[str]:
    """Build a strategy for ISO-8601 calendar date strings in ``[1900-01-01, 2200-12-31]``.

    :return: Hypothesis strategy yielding ``YYYY-MM-DD`` strings.
    :rtype: SearchStrategy[str]
    """
    return st.dates(min_value=MIN_SAFE_DATE, max_value=MAX_SAFE_DATE).map(dt.date.isoformat)


def iso_datetime_str_strategy() -> SearchStrategy[str]:
    """Build a strategy for timezone-aware ISO-8601 timestamp strings in UTC.

    :return: Hypothesis strategy yielding ISO-8601 datetime strings.
    :rtype: SearchStrategy[str]
    """
    return st.datetimes(
        min_value=MIN_SAFE_NAIVE_DT,
        max_value=MAX_SAFE_NAIVE_DT,
        timezones=st.just(dt.UTC),
    ).map(dt.datetime.isoformat)


def iso_time_str_strategy() -> SearchStrategy[str]:
    """Build a strategy for naive ISO-8601 time strings.

    :return: Hypothesis strategy yielding ``HH:MM:SS[.ffffff]`` strings.
    :rtype: SearchStrategy[str]
    """
    return st.times().map(dt.time.isoformat)


def base64_str_strategy(max_size: int = 32) -> SearchStrategy[str]:
    """Build a strategy for valid Base64-encoded ASCII strings.

    :param max_size: Maximum unencoded byte length.
    :type max_size: int
    :return: Hypothesis strategy yielding Base64 strings.
    :rtype: SearchStrategy[str]
    """
    return st.binary(max_size=max_size).map(encode_base64_ascii)


def hex_str_strategy(max_size: int = 32) -> SearchStrategy[str]:
    """Build a strategy for valid hexadecimal (Base16) strings.

    :param max_size: Maximum unencoded byte length.
    :type max_size: int
    :return: Hypothesis strategy yielding hex strings.
    :rtype: SearchStrategy[str]
    """
    return st.binary(max_size=max_size).map(bytes.hex)


def _pbt_json_default(obj: object) -> str:
    """Serialize Python date, time, Decimal, and arbitrary bytes values for JSON round-trip checks.

    :param obj: Non-JSON-native Python object.
    :type obj: object
    :return: String representation suitable for ``json.dumps``.
    :rtype: str
    :raises TypeError: If ``obj`` is not a supported Arrow scalar Python object.
    """
    if isinstance(obj, dt.date | dt.time):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, bytes):
        return encode_base64_ascii(obj)
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def assert_coerced_rows_roundtrip(rows: list[dict[str, Any]], pa_schema: pa.Schema) -> pa.Table:
    """Verify coerced rows conform to ``pa_schema``, build a valid Arrow Table, and round-trip via Parquet.

    :param rows: Coerced row dictionaries produced by a record/XML converter.
    :type rows: list[dict[str, Any]]
    :param pa_schema: Target PyArrow table schema.
    :type pa_schema: pa.Schema
    :return: Validated PyArrow Table constructed from ``rows``.
    :rtype: pa.Table
    """
    schema_names = set(pa_schema.names)
    for row in rows:
        assert set(row.keys()) <= schema_names

    serialized = json.loads(json.dumps(rows, default=_pbt_json_default))
    assert len(serialized) == len(rows)

    table = pa.Table.from_pylist(rows, schema=pa_schema)
    table.validate(full=True)
    assert table.schema.equals(pa_schema, check_metadata=True)
    assert table.num_rows == len(rows)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    roundtrip_table = pq.read_table(buf)
    assert roundtrip_table.num_rows == table.num_rows
    for col_name in pa_schema.names:
        assert (col_name, roundtrip_table.column(col_name).to_pylist()) == (
            col_name,
            table.column(col_name).to_pylist(),
        )
    return table
