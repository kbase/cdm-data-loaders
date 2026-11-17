"""Unit tests for Pydantic-to-PyArrow record coercion."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated

import pyarrow as pa
from pydantic import BaseModel, Field, SecretBytes, SecretStr

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


def test_coerce_pydantic_record_for_pyarrow_pass_model_and_mapping_parity() -> None:
    """coerce_pydantic_record_for_pyarrow produces identical rows from validated models and raw dicts."""
    pa_schema = generate_pyarrow_schema(_SampleRecord, by_alias=True)
    raw = {
        "record_id": "rec-1",
        "secret_token": "top-secret",
        "raw_key": b"\x01\x02",
        "tags": ["beta", "alpha"],
        "amount": "12.34",
        "duration": "PT90S",
        "created_at": "2024-05-01T10:00:00Z",
    }
    instance = _SampleRecord.model_validate(raw)

    from_model = coerce_pydantic_record_for_pyarrow(instance, pa_schema, by_alias=True)
    from_dict = coerce_pydantic_record_for_pyarrow(raw, pa_schema, model=_SampleRecord, by_alias=True)

    assert from_model == from_dict
    assert from_model == {
        "recordId": "rec-1",
        "secret_token": "top-secret",
        "raw_key": b"\x01\x02",
        "tags": ["alpha", "beta"],
        "amount": Decimal("12.34"),
        "duration": "PT1M30S",
        "created_at": datetime(2024, 5, 1, 10, 0, tzinfo=UTC),
    }
    table = pa.Table.from_pylist([from_model], schema=pa_schema)
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
