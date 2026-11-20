"""Unit tests for shared PyArrow coercion primitives in ``cdm_data_loaders.coercion._arrow_core``."""

from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Final

import pyarrow as pa
import pytest

from cdm_data_loaders.coercion._arrow_core import (
    ArrowDecimalType,
    DefaultStrEncoding,
    coerce_binary,
    coerce_date,
    coerce_decimal,
    coerce_numeric,
    coerce_scalar_for_pyarrow,
    coerce_string,
    coerce_struct_from_mapping_or_tuple,
    coerce_time,
    coerce_timestamp,
    comparable_sort_key,
)

PLUS_TWO_TZ = timezone(timedelta(hours=2))
NAIVE_REF_DT = datetime(2024, 2, 29, 12, 30, tzinfo=UTC).replace(tzinfo=None)
UTC_REF_DT = datetime(2024, 2, 29, 12, 30, tzinfo=UTC)


class _ColorEnum(StrEnum):
    RED = "red"


def test_comparable_sort_key_orders_mixed_scalar_types() -> None:
    """Sort heterogeneous scalar values in fixed order with None last."""
    values: list[object] = [None, "b", b"raw", True, False, 2.5, 1, Decimal("9.9")]
    assert sorted(values, key=comparable_sort_key) == [False, True, 1, 2.5, b"raw", "b", Decimal("9.9"), None]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("2024-02-29", date(2024, 2, 29), id="iso-string"),
        pytest.param(UTC_REF_DT, date(2024, 2, 29), id="datetime-instance"),
        pytest.param(date(2024, 2, 29), date(2024, 2, 29), id="date-instance"),
        pytest.param(123, 123, id="non-date-passthrough"),
    ],
)
def test_coerce_date_converts_supported_inputs(raw: object, expected: object) -> None:
    """Convert ISO strings and datetimes to date while passing unsupported objects through."""
    assert coerce_date(raw) == expected


@pytest.mark.parametrize(
    ("raw", "target_type", "expected"),
    [
        pytest.param("2024-02-29T12:30:00", pa.timestamp("us", tz="UTC"), UTC_REF_DT, id="naive-str-to-utc"),
        pytest.param(NAIVE_REF_DT, pa.timestamp("us", tz="UTC"), UTC_REF_DT, id="naive-dt-to-utc"),
        pytest.param("2024-02-29T14:30:00+02:00", pa.timestamp("us", tz="UTC"), UTC_REF_DT, id="offset-str-to-utc"),
        pytest.param(
            datetime(2024, 2, 29, 14, 30, tzinfo=PLUS_TWO_TZ),
            pa.timestamp("us", tz="UTC"),
            UTC_REF_DT,
            id="offset-dt-to-utc",
        ),
        pytest.param("2024-02-29T12:30:00Z", pa.timestamp("us"), NAIVE_REF_DT, id="z-str-to-naive"),
        pytest.param("2024-02-29T14:30:00+02:00", pa.timestamp("us"), NAIVE_REF_DT, id="offset-str-to-naive"),
        pytest.param(UTC_REF_DT, pa.timestamp("us"), NAIVE_REF_DT, id="utc-dt-to-naive"),
        pytest.param("2024-02-29T12:30:00", pa.timestamp("us"), NAIVE_REF_DT, id="naive-str-to-naive"),
        pytest.param(42, pa.timestamp("us", tz="UTC"), 42, id="non-datetime-passthrough"),
    ],
)
def test_coerce_timestamp_normalizes_timezone_awareness(
    raw: object,
    target_type: pa.TimestampType,
    expected: object,
) -> None:
    """Normalize aware and naive datetimes or ISO strings to match target TimestampType tz."""
    assert coerce_timestamp(raw, target_type) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("08:15:30+02:00", time(8, 15, 30), id="aware-iso-string"),
        pytest.param(time(8, 15, 30, tzinfo=UTC), time(8, 15, 30), id="aware-time-object"),
        pytest.param("08:15:30", time(8, 15, 30), id="naive-iso-string"),
        pytest.param(99, 99, id="non-time-passthrough"),
    ],
)
def test_coerce_time_strips_timezone_offset(raw: object, expected: object) -> None:
    """Strip tzinfo from aware time objects and ISO strings for PyArrow time64 compatibility."""
    result = coerce_time(raw)
    assert result == expected
    if isinstance(result, time):
        assert result.tzinfo is None


@pytest.mark.parametrize(
    ("raw", "target_type", "expected"),
    [
        pytest.param("19.956", pa.decimal128(10, 2), Decimal("19.96"), id="str-quantized-up"),
        pytest.param(0, pa.decimal128(10, 2), Decimal("0.00"), id="zero-int"),
        pytest.param(-2.5, pa.decimal128(10, 3), Decimal("-2.500"), id="negative-float"),
        pytest.param(
            Decimal("1" * 35 + ".126"),
            pa.decimal128(38, 2),
            Decimal("1" * 35 + ".13"),
            id="high-precision-decimal128",
        ),
        pytest.param(
            Decimal("9" * 70 + ".12346"),
            pa.decimal256(76, 4),
            Decimal("9" * 70 + ".1235"),
            id="high-precision-decimal256",
        ),
        pytest.param(True, pa.decimal128(10, 2), True, id="bool-passthrough"),
        pytest.param(["bad"], pa.decimal128(10, 2), ["bad"], id="list-passthrough"),
    ],
)
def test_coerce_decimal_quantizes_precision_and_scale(
    raw: object,
    target_type: ArrowDecimalType,
    expected: object,
) -> None:
    """Quantize numeric and string scalars to target scale across decimal128 and decimal256."""
    coerced = coerce_decimal(raw, target_type)
    assert coerced == expected
    if isinstance(coerced, Decimal):
        assert coerced.as_tuple().exponent == -target_type.scale
        table = pa.Table.from_pylist([{"d": coerced}], schema=pa.schema([pa.field("d", target_type)]))
        assert table.num_rows == 1


@pytest.mark.parametrize(
    ("raw", "metadata", "default_encoding", "expected"),
    [
        pytest.param(b"raw", None, "base64", b"raw", id="bytes-direct"),
        pytest.param(bytearray(b"raw"), None, "base64", b"raw", id="bytearray"),
        pytest.param(memoryview(b"raw"), None, "base64", b"raw", id="memoryview"),
        pytest.param(
            "deadbeef",
            {b"jsonschema_type": b"string (contentEncoding: base16)"},
            "base64",
            b"\xde\xad\xbe\xef",
            id="jsonschema-base16",
        ),
        pytest.param(
            "deadbeef",
            {b"xsd_type": b"xs:hexBinary"},
            "utf-8",
            b"\xde\xad\xbe\xef",
            id="xsd-hexbinary",
        ),
        pytest.param(
            "YWJj",
            {b"pydantic_type": b"binary (base64)"},
            "utf-8",
            b"abc",
            id="pydantic-base64",
        ),
        pytest.param(
            "YWJj",
            {b"xsd_type": b"xs:base64Binary"},
            "utf-8",
            b"abc",
            id="xsd-base64binary",
        ),
        pytest.param(
            "plain",
            {b"pydantic_type": b"binary"},
            "base64",
            b"plain",
            id="pydantic-plain-utf8-binary",
        ),
        pytest.param("YWJj", None, "base64", b"abc", id="unannotated-default-base64"),
        pytest.param("plain", None, "utf-8", b"plain", id="unannotated-default-utf8"),
        pytest.param(123, None, "base64", 123, id="int-passthrough"),
    ],
)
def test_coerce_binary_decodes_metadata_and_fallback_encodings(
    raw: object,
    metadata: dict[bytes, bytes] | None,
    default_encoding: DefaultStrEncoding,
    expected: object,
) -> None:
    """Decode binary inputs using jsonschema, pydantic, or xsd metadata or fallback encoding."""
    assert coerce_binary(raw, metadata, default_str_encoding=default_encoding) == expected


@pytest.mark.parametrize(
    ("raw", "target_type", "expected"),
    [
        pytest.param(7, pa.float64(), 7.0, id="int-to-float"),
        pytest.param(Decimal("2.5"), pa.float64(), 2.5, id="decimal-to-float"),
        pytest.param("-3.25", pa.float64(), -3.25, id="str-to-float"),
        pytest.param(7.0, pa.int64(), 7, id="integral-float-to-int"),
        pytest.param(Decimal("10.0"), pa.int64(), 10, id="integral-decimal-to-int"),
        pytest.param("-42", pa.int64(), -42, id="str-to-int"),
        pytest.param(7.5, pa.int64(), 7.5, id="fractional-float-passthrough"),
        pytest.param(Decimal("7.5"), pa.int64(), Decimal("7.5"), id="fractional-decimal-passthrough"),
        pytest.param(True, pa.int64(), True, id="bool-int-passthrough"),
        pytest.param(False, pa.float64(), False, id="bool-float-passthrough"),
    ],
)
def test_coerce_numeric_converts_and_preserves_guards(
    raw: object,
    target_type: pa.DataType,
    expected: object,
) -> None:
    """Convert valid numeric inputs to int or float without coercing booleans or truncating fractions."""
    assert coerce_numeric(raw, target_type) == expected


def test_coerce_scalar_for_pyarrow_unwraps_enum() -> None:
    """Unwrap StrEnum values for PyArrow string columns."""
    assert coerce_scalar_for_pyarrow(_ColorEnum.RED, pa.string()) == "red"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(UTC_REF_DT, "2024-02-29T12:30:00+00:00", id="utc-datetime"),
        pytest.param(b"text", "text", id="bytes"),
        pytest.param((1, date(2024, 1, 1)), '[1, "2024-01-01"]', id="tuple-with-date"),
    ],
)
def test_coerce_string_formats_values(raw: object, expected: str) -> None:
    """Format temporal, binary, and container values for string columns."""
    assert coerce_string(raw) == expected


TUPLE_STRUCT: Final[pa.StructType] = pa.struct([pa.field("item_0", pa.int64()), pa.field("item_1", pa.string())])
UNION_STRUCT: Final[pa.StructType] = pa.struct([pa.field("label", pa.string()), pa.field("_value", pa.int64())])
PLAIN_STRUCT: Final[pa.StructType] = pa.struct([pa.field("label", pa.string())])


@pytest.mark.parametrize(
    ("raw", "struct_type", "allow_tuple", "expected"),
    [
        pytest.param((1, "a"), TUPLE_STRUCT, False, (1, "a"), id="tuple-disallowed-passthrough"),
        pytest.param((1, None), TUPLE_STRUCT, True, {"item_0": 1}, id="tuple-allowed-omits-none"),
        pytest.param("42", UNION_STRUCT, False, {"_value": 42}, id="scalar-union-value-field"),
        pytest.param("orphan", PLAIN_STRUCT, False, "orphan", id="non-mapping-passthrough"),
    ],
)
def test_coerce_struct_from_mapping_or_tuple_handles_tuple_union_and_passthrough(
    raw: object,
    struct_type: pa.StructType,
    allow_tuple: bool,
    expected: object,
) -> None:
    """Coerce positional tuples, _value scalar unions, and non-matching values on StructType."""
    assert (
        coerce_struct_from_mapping_or_tuple(
            raw,
            struct_type,
            coerce_scalar_for_pyarrow,
            allow_tuple=allow_tuple,
        )
        == expected
    )
