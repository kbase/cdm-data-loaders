"""Unit tests for Pydantic-to-PyArrow record coercion."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address
from pathlib import PurePosixPath
from typing import Annotated, Any
from uuid import UUID

import pyarrow as pa
import pytest
from pydantic import AnyUrl, BaseModel, Field, RootModel, SecretBytes, SecretStr, computed_field

from cdm_data_loaders.coercion.json_to_arrow import coerce_json_record_for_pyarrow
from cdm_data_loaders.coercion.pydantic_to_arrow import (
    coerce_pydantic_record_for_pyarrow,
    to_jsonable_recursive,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import generate_pyarrow_schema


class _SampleRecord(BaseModel):
    record_id: str = Field(serialization_alias="recordId")
    secret_token: SecretStr
    raw_key: SecretBytes
    tags: frozenset[str]
    amount: Annotated[Decimal, Field(max_digits=8, decimal_places=2)]
    duration: timedelta
    created_at: datetime


@pytest.mark.parametrize(
    "from_model",
    [
        pytest.param(True, id="validated-model"),
        pytest.param(False, id="raw-mapping"),
    ],
)
def test_coerce_pydantic_record_for_pyarrow_pass_model_and_mapping_parity(from_model: bool) -> None:
    """coerce_pydantic_record_for_pyarrow produces identical rows from validated models and raw dicts."""
    pa_schema = generate_pyarrow_schema(_SampleRecord, by_alias=True)
    raw: dict[str, Any] = {
        "record_id": "rec-1",
        "secret_token": "top-secret",
        "raw_key": b"\x01\x02",
        "tags": ["beta", "alpha"],
        "amount": "12.34",
        "duration": "PT90S",
        "created_at": "2024-05-01T10:00:00Z",
    }
    record_input: _SampleRecord | dict[str, Any] = _SampleRecord.model_validate(raw) if from_model else raw
    model_arg = None if from_model else _SampleRecord

    coerced = coerce_pydantic_record_for_pyarrow(record_input, pa_schema, model=model_arg, by_alias=True)
    assert coerced == {
        "recordId": "rec-1",
        "secret_token": "top-secret",
        "raw_key": b"\x01\x02",
        "tags": ["alpha", "beta"],
        "amount": Decimal("12.34"),
        "duration": "PT1M30S",
        "created_at": datetime(2024, 5, 1, 10, 0, tzinfo=UTC),
    }
    table = pa.Table.from_pylist([coerced], schema=pa_schema)
    assert table.num_rows == 1


def test_to_jsonable_recursive_pass_unwraps_secrets_and_sorts_sets() -> None:
    """to_jsonable_recursive unwraps secrets, formats durations, and sorts set values."""
    payload = {
        "token": SecretStr("hidden"),
        "blob": SecretBytes(b"abc"),
        "tags": {"z", "a"},
        "wait": timedelta(hours=1, minutes=5),
    }
    assert to_jsonable_recursive(payload) == {
        "token": "hidden",
        "blob": "abc",
        "tags": ["a", "z"],
        "wait": "PT1H5M",
    }


@pytest.mark.parametrize(
    ("raw_record", "schema"),
    [
        pytest.param(
            {
                "ts_utc": "2024-02-29T14:30:00+02:00",
                "ts_naive": "2024-02-29T14:30:00+02:00",
                "tod": "08:15:30+02:00",
                "day": "2024-02-29",
                "dec128": "19.956",
                "dec256": "1234567890123456789012345678901234567890.12346",
                "num_int": "42",
                "num_float": "3.25",
                "hex_bin": "deadbeef",
                "b64_bin": "YWJj",
                "view_text": {"k": [1, True]},
                "fixed_dates": ["2024-01-01", "2024-01-02"],
                "nested": {"code": 7},
                "lookup": {"d": "2024-03-01"},
            },
            pa.schema(
                [
                    pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
                    pa.field("ts_naive", pa.timestamp("us")),
                    pa.field("tod", pa.time64("us")),
                    pa.field("day", pa.date32()),
                    pa.field("dec128", pa.decimal128(10, 2)),
                    pa.field("dec256", pa.decimal256(76, 4)),
                    pa.field("num_int", pa.int64()),
                    pa.field("num_float", pa.float64()),
                    pa.field(
                        "hex_bin",
                        pa.large_binary(),
                        metadata={"jsonschema_type": "string (contentEncoding: base16)"},
                    ),
                    pa.field("b64_bin", pa.binary(3), metadata={"pydantic_type": "binary (base64)"}),
                    pa.field("view_text", pa.string_view()),
                    pa.field("fixed_dates", pa.list_(pa.date32(), 2)),
                    pa.field("nested", pa.struct([pa.field("code", pa.float64())])),
                    pa.field("lookup", pa.map_(pa.string(), pa.date32())),
                ]
            ),
            id="temporal-decimal-binary-and-containers",
        ),
        pytest.param(
            {"empty_struct": {}, "empty_list": [], "empty_map": {}, "null_col": None},
            pa.schema(
                [
                    pa.field("empty_struct", pa.struct([pa.field("a", pa.int64())])),
                    pa.field("empty_list", pa.large_list(pa.string())),
                    pa.field("empty_map", pa.map_(pa.string(), pa.int64())),
                    pa.field("null_col", pa.decimal128(10, 2)),
                ]
            ),
            id="nulls-and-empty-containers",
        ),
    ],
)
def test_coerce_json_and_pydantic_parity_on_plain_mapping(
    raw_record: dict[str, Any],
    schema: pa.Schema,
) -> None:
    """Plain JSON-compatible mappings coerce to identical Arrow-valid rows across both coercers."""
    from_json = coerce_json_record_for_pyarrow(raw_record, schema)
    from_pydantic = coerce_pydantic_record_for_pyarrow(raw_record, schema)
    assert from_json == from_pydantic
    table = pa.Table.from_pylist([from_json], schema=schema)
    assert table.num_rows == 1
    assert table.schema.equals(schema, check_metadata=True)


class _Color(Enum):
    RED = "red"


@dataclass
class _Coord:
    x: int
    y: int | None = None


class _ModelWithExcludedAndComputed(BaseModel):
    visible: int
    hidden: str = Field(default="skip", exclude=True)
    coord: _Coord

    @computed_field
    @property
    def doubled(self) -> int:
        """Return twice the visible field value.

        :return: Doubled integer value.
        :rtype: int
        """
        return self.visible * 2


class _NestedRootModel(RootModel[_ModelWithExcludedAndComputed]):
    pass


class _MappingRootModel(RootModel[dict[str, int]]):
    pass


class _ScalarRootModel(RootModel[int]):
    pass


@pytest.mark.parametrize(
    ("instance", "pa_schema", "expected"),
    [
        pytest.param(
            _NestedRootModel(_ModelWithExcludedAndComputed(visible=5, coord=_Coord(x=3))),
            pa.schema(
                [
                    pa.field("visible", pa.int64()),
                    pa.field("hidden", pa.string()),
                    pa.field("doubled", pa.int64()),
                    pa.field("coord", pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])),
                ]
            ),
            {
                "visible": 5,
                "doubled": 10,
                "coord": {"x": 3},
            },
            id="nested-root-model",
        ),
        pytest.param(
            _MappingRootModel({"k": 9}),
            pa.schema([pa.field("k", pa.int64())]),
            {"k": 9},
            id="mapping-root-model",
        ),
        pytest.param(
            _ScalarRootModel(42),
            pa.schema([pa.field("_value", pa.int64())]),
            {"_value": 42},
            id="scalar-root-model",
        ),
    ],
)
def test_coerce_pydantic_record_for_pyarrow_pass_root_models_excluded_fields_and_dataclass(
    instance: BaseModel,
    pa_schema: pa.Schema,
    expected: dict[str, Any],
) -> None:
    """RootModel variants, excluded fields, computed fields, and dataclass structs coerce as expected."""
    assert coerce_pydantic_record_for_pyarrow(instance, pa_schema) == expected


def test_to_jsonable_recursive_pass_root_model_enum_network_and_scalars() -> None:
    """to_jsonable_recursive converts nested RootModel, Enum, dataclass, network, and stdlib scalars."""
    payload = {
        "root_num": _ScalarRootModel(7),
        "color": _Color.RED,
        "addr": IPv4Address("192.0.2.1"),
        "site": AnyUrl("https://example.org/data"),
        "uid": UUID("12345678-1234-5678-1234-567812345678"),
        "path": PurePosixPath("/data/in.json"),
        "day": date(2024, 6, 1),
        "price": Decimal("3.50"),
        "coord": _Coord(x=1),
        "model": _ModelWithExcludedAndComputed(visible=4, coord=_Coord(x=2)),
    }
    assert to_jsonable_recursive(payload) == {
        "root_num": 7,
        "color": "red",
        "addr": "192.0.2.1",
        "site": "https://example.org/data",
        "uid": "12345678-1234-5678-1234-567812345678",
        "path": "/data/in.json",
        "day": "2024-06-01",
        "price": "3.50",
        "coord": {"x": 1},
        "model": {"visible": 4, "doubled": 8, "coord": {"x": 2}},
    }
