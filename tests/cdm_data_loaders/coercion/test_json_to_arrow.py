"""Unit tests for JSON-to-PyArrow record coercion."""

import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Final

import pyarrow as pa
import pytest

from cdm_data_loaders.coercion.json_to_arrow import coerce_json_record_for_pyarrow

NAIVE_LEAP_DATETIME: Final[datetime] = datetime(2024, 2, 29, 12, 30, tzinfo=UTC).replace(tzinfo=None)

BOUNDARY_PA_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("day", pa.date32()),
        pa.field("ts_naive_target", pa.timestamp("us")),
        pa.field("tod", pa.time64("us")),
        pa.field("zero_dec", pa.decimal128(38, 2)),
        pa.field("neg_dec", pa.decimal128(38, 2)),
        pa.field("neg_float", pa.float64()),
        pa.field("neg_int", pa.int64()),
        pa.field("large_text", pa.large_string()),
        pa.field("collapsed_list", pa.large_string()),
        pa.field("nested", pa.struct([pa.field("when", pa.date32())])),
        pa.field("large_items", pa.large_list(pa.decimal128(38, 1))),
        pa.field("attrs", pa.map_(pa.string(), pa.date32())),
    ]
)


def test_coerce_json_record_for_pyarrow_pass_temporal_decimal_and_nested_types() -> None:
    """JSON-parsed values coerce to match temporal, decimal, numeric, struct, list, map, and string fields."""
    pa_schema = pa.schema(
        [
            pa.field("day", pa.date32()),
            pa.field("ts_z", pa.timestamp("us", tz="UTC")),
            pa.field("ts_naive", pa.timestamp("us", tz="UTC")),
            pa.field("tod", pa.time64("us")),
            pa.field("price", pa.decimal128(38, 2)),
            pa.field("ratio", pa.float64()),
            pa.field("whole", pa.int64()),
            pa.field("collapsed_obj", pa.string()),
            pa.field("collapsed_num", pa.string()),
            pa.field("nested", pa.struct([pa.field("when", pa.date32())])),
            pa.field("items", pa.list_(pa.decimal128(38, 1))),
            pa.field("attrs", pa.map_(pa.string(), pa.date32())),
        ]
    )
    raw_record: dict[str, Any] = {
        "day": "2024-02-29",
        "ts_z": "2024-02-29T12:30:00Z",
        "ts_naive": "2024-02-29T12:30:00",
        "tod": "08:15:30",
        "price": 19.95,
        "ratio": 2,
        "whole": 7.0,
        "collapsed_obj": {"a": 1},
        "collapsed_num": 42,
        "nested": {"when": "2024-03-01", "extra": "dropped"},
        "items": [1.5, 2],
        "attrs": {"start": "2024-01-01"},
        "undeclared_top": "dropped",
    }

    coerced = coerce_json_record_for_pyarrow(raw_record, pa_schema)

    assert coerced == {
        "day": date(2024, 2, 29),
        "ts_z": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "ts_naive": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "tod": time(8, 15, 30),
        "price": Decimal("19.95"),
        "ratio": 2.0,
        "whole": 7,
        "collapsed_obj": json.dumps({"a": 1}),
        "collapsed_num": "42",
        "nested": {"when": date(2024, 3, 1)},
        "items": [Decimal("1.5"), Decimal(2)],
        "attrs": {"start": date(2024, 1, 1)},
    }
    table = pa.Table.from_pylist([coerced], schema=pa_schema)
    assert table.num_rows == 1
    assert table.schema == pa_schema


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param({}, {}, id="empty-dict"),
        pytest.param(
            dict.fromkeys(BOUNDARY_PA_SCHEMA.names),
            dict.fromkeys(BOUNDARY_PA_SCHEMA.names),
            id="all-nulls",
        ),
        pytest.param(
            {
                "day": date(2024, 1, 1),
                "ts_naive_target": "2024-02-29T12:30:00",
                "tod": time(0, 0, 0),
                "zero_dec": 0,
                "neg_dec": "-0.05",
                "neg_float": -3,
                "neg_int": -7.0,
                "large_text": "already-string",
                "collapsed_list": [1, "two"],
                "nested": {"when": None},
                "large_items": [None, Decimal("-1.5")],
                "attrs": {"none_val": None},
            },
            {
                "day": date(2024, 1, 1),
                "ts_naive_target": NAIVE_LEAP_DATETIME,
                "tod": time(0, 0, 0),
                "zero_dec": Decimal(0),
                "neg_dec": Decimal("-0.05"),
                "neg_float": -3.0,
                "neg_int": -7,
                "large_text": "already-string",
                "collapsed_list": json.dumps([1, "two"]),
                "nested": {"when": None},
                "large_items": [None, Decimal("-1.5")],
                "attrs": {"none_val": None},
            },
            id="boundary-values",
        ),
        pytest.param(
            {"nested": {}, "large_items": [], "attrs": {}},
            {"nested": {}, "large_items": [], "attrs": {}},
            id="empty-containers",
        ),
    ],
)
def test_coerce_json_record_for_pyarrow_pass_nulls_empty_and_boundary_values(
    record: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    """Nulls, empty containers, zero/negative numbers, tz-naive timestamps, and large types coerce."""
    coerced = coerce_json_record_for_pyarrow(record, BOUNDARY_PA_SCHEMA)
    assert coerced == expected
    table = pa.Table.from_pylist([coerced], schema=BOUNDARY_PA_SCHEMA)
    assert table.num_rows == 1


def test_coerce_json_record_for_pyarrow_pass_preserves_boolean_and_non_integral_float_guards() -> None:
    """Booleans are not coerced to numeric/decimal types, and non-integral floats are not truncated to int."""
    pa_schema = pa.schema(
        [
            pa.field("dec", pa.decimal128(38, 2)),
            pa.field("flt", pa.float64()),
            pa.field("integer_from_bool", pa.int64()),
            pa.field("integer_from_frac", pa.int64()),
        ]
    )
    fractional_value = 7.5
    raw = {
        "dec": True,
        "flt": False,
        "integer_from_bool": True,
        "integer_from_frac": fractional_value,
    }
    coerced = coerce_json_record_for_pyarrow(raw, pa_schema)
    assert coerced == raw
    assert isinstance(coerced["dec"], bool)
    assert isinstance(coerced["flt"], bool)
    assert isinstance(coerced["integer_from_bool"], bool)
    assert coerced["integer_from_frac"] == fractional_value


def test_coerce_json_record_for_pyarrow_pass_aware_temporal_quantized_decimal_and_extended_arrow_types() -> None:
    """Aware ISO timestamps/times on naive columns, high-scale decimals, numeric strings, and view/fixed types coerce."""
    pa_schema = pa.schema(
        [
            pa.field("ts_naive_from_z", pa.timestamp("us")),
            pa.field("ts_naive_from_offset", pa.timestamp("us")),
            pa.field("ts_utc_from_offset", pa.timestamp("us", tz="UTC")),
            pa.field("tod_from_offset", pa.time64("us")),
            pa.field("quantized_dec", pa.decimal128(10, 2)),
            pa.field("int_from_str", pa.int64()),
            pa.field("float_from_str", pa.float64()),
            pa.field("hex_xsd", pa.large_binary(), metadata={"xsd_type": "xs:hexBinary"}),
            pa.field("b64_pyd", pa.binary(3), metadata={"pydantic_type": "binary (base64)"}),
            pa.field("view_str", pa.string_view()),
            pa.field("fixed_pair", pa.list_(pa.date32(), 2)),
        ]
    )
    raw: dict[str, Any] = {
        "ts_naive_from_z": "2024-02-29T12:30:00Z",
        "ts_naive_from_offset": "2024-02-29T14:30:00+02:00",
        "ts_utc_from_offset": "2024-02-29T14:30:00+02:00",
        "tod_from_offset": "08:15:30+02:00",
        "quantized_dec": "19.956",
        "int_from_str": "42",
        "float_from_str": "3.25",
        "hex_xsd": "deadbeef",
        "b64_pyd": "YWJj",
        "view_str": {"nested": True},
        "fixed_pair": ["2024-01-01", "2024-01-02"],
    }

    coerced = coerce_json_record_for_pyarrow(raw, pa_schema)

    assert coerced == {
        "ts_naive_from_z": NAIVE_LEAP_DATETIME,
        "ts_naive_from_offset": NAIVE_LEAP_DATETIME,
        "ts_utc_from_offset": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "tod_from_offset": time(8, 15, 30),
        "quantized_dec": Decimal("19.96"),
        "int_from_str": 42,
        "float_from_str": 3.25,
        "hex_xsd": b"\xde\xad\xbe\xef",
        "b64_pyd": b"abc",
        "view_str": json.dumps({"nested": True}),
        "fixed_pair": [date(2024, 1, 1), date(2024, 1, 2)],
    }
    table = pa.Table.from_pylist([coerced], schema=pa_schema)
    assert table.num_rows == 1
    assert table.schema.equals(pa_schema, check_metadata=True)
