"""Unit tests for PyArrow-to-JSON-Schema and PyArrow-to-LinkML record serialization."""

from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from cdm_data_loaders.coercion.arrow_to_records import (
    serialize_record_for_jsonschema,
    serialize_record_for_linkml,
    serialize_table_for_jsonschema,
    serialize_table_for_linkml,
)


def test_serialize_table_and_record_for_jsonschema_pass_scalars_and_maps() -> None:
    """serialize_table_for_jsonschema formats decimals, binary values, timestamps, and maps."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("price", pa.decimal128(10, 2)),
            pa.field("raw", pa.binary()),
            pa.field("day", pa.date32()),
            pa.field("seen", pa.timestamp("us", tz="UTC")),
            pa.field("labels", pa.map_(pa.string(), pa.int32())),
        ]
    )
    row = {
        "id": 1,
        "price": Decimal("9.50"),
        "raw": b"hi",
        "day": date(2024, 6, 1),
        "seen": datetime(2024, 6, 1, 8, 30, tzinfo=UTC),
        "labels": [("k", 7)],
    }
    table = pa.Table.from_pylist([row], schema=schema)
    serialized_rows = serialize_table_for_jsonschema(table)
    assert len(serialized_rows) == 1
    assert serialized_rows[0] == serialize_record_for_jsonschema(row, schema)
    assert serialized_rows[0]["price"] == "9.50"
    assert serialized_rows[0]["raw"] == "aGk="
    assert serialized_rows[0]["day"] == "2024-06-01"
    assert serialized_rows[0]["seen"] == "2024-06-01T08:30:00+00:00"
    assert serialized_rows[0]["labels"] == [{"key": "k", "value": 7}]


def test_serialize_table_and_record_for_linkml_pass_decimals_and_nulls() -> None:
    """serialize_table_for_linkml formats decimals as strings, preserves nulls, and enforces list rules."""
    schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("score", pa.decimal128(8, 2)),
            pa.field("note", pa.string(), nullable=True),
        ]
    )
    row = {"id": "item-1", "score": Decimal("3.25"), "note": None}
    table = pa.Table.from_pylist([row], schema=schema)
    expected = {"id": "item-1", "score": "3.25", "note": None}
    assert serialize_table_for_linkml(table) == [expected]
    assert serialize_record_for_linkml(row, schema) == expected


@pytest.mark.parametrize(
    ("table", "match"),
    [
        pytest.param(
            pa.Table.from_pylist(
                [{"tags": ["ok", None]}],
                schema=pa.schema([pa.field("tags", pa.list_(pa.string()))]),
            ),
            "null list elements",
            id="null-list-item",
        ),
        pytest.param(
            pa.Table.from_pylist(
                [{"matrix": [[1, 2]]}],
                schema=pa.schema([pa.field("matrix", pa.list_(pa.list_(pa.int32())))]),
            ),
            "nested list or map items",
            id="nested-list-item",
        ),
    ],
)
def test_serialize_table_for_linkml_fail_invalid_multivalued_lists(table: pa.Table, match: str) -> None:
    """serialize_table_for_linkml rejects null list items and nested lists."""
    with pytest.raises(ValueError, match=match):
        serialize_table_for_linkml(table)


@pytest.mark.parametrize(
    "serializer",
    [serialize_table_for_jsonschema, serialize_table_for_linkml],
    ids=["jsonschema", "linkml"],
)
@pytest.mark.parametrize(
    "bad_table",
    [
        pytest.param([{"id": 1}], id="list-not-table"),
        pytest.param({"id": 1}, id="dict-not-table"),
    ],
)
def test_serialize_table_fail_non_pyarrow_table(
    bad_table: object,
    serializer: Callable[[pa.Table], list[dict[str, Any]]],
) -> None:
    """serialize_table_for_jsonschema and serialize_table_for_linkml reject non-Table inputs."""
    with pytest.raises(TypeError, match=r"Expected a pyarrow\.Table"):
        serializer(bad_table)  # type: ignore[arg-type]
