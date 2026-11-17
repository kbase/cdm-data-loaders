"""Unit tests for ``cdm_data_loaders.utils.arrow_schema``."""

from collections.abc import Sequence
from typing import Final

import pyarrow as pa
import pytest

from cdm_data_loaders.utils.arrow_schema import (
    assemble_pyarrow_schema,
    decode_field_metadata,
    is_any_binary_type,
    is_any_list_type,
    is_any_string_type,
    is_non_json_arrow_type,
)

SAMPLE_FIELDS: Final[tuple[pa.Field, ...]] = (
    pa.field("id", pa.string(), nullable=False),
    pa.field("count", pa.int64(), nullable=True),
)
EXPECTED_ASSEMBLED_SCHEMA: Final[pa.Schema] = pa.schema(SAMPLE_FIELDS, metadata={"source": "unit"})


@pytest.mark.parametrize(
    "component",
    [
        pytest.param(pa.struct(SAMPLE_FIELDS), id="struct-type"),
        pytest.param(list(SAMPLE_FIELDS), id="field-sequence"),
    ],
)
def test_assemble_pyarrow_schema_pass_from_struct_and_sequence(
    component: pa.StructType | Sequence[pa.Field],
) -> None:
    """Assemble the expected schema from a ``pa.StructType`` or a sequence of ``pa.Field`` instances."""
    assembled = assemble_pyarrow_schema(component, metadata={"source": "unit"})
    assert assembled.equals(EXPECTED_ASSEMBLED_SCHEMA, check_metadata=True)
    assert assembled.metadata == {b"source": b"unit"}


def test_assemble_pyarrow_schema_rejects_empty_component() -> None:
    """Raise ``ValueError`` when assembling a schema with zero fields."""
    with pytest.raises(ValueError, match="Cannot assemble a PyArrow schema from an empty component"):
        assemble_pyarrow_schema([])


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(None, {}, id="none"),
        pytest.param({}, {}, id="empty"),
        pytest.param(
            {b"definition": b"Accession ID", b"title": b"ID"},
            {"definition": "Accession ID", "title": "ID"},
            id="utf8-pairs",
        ),
    ],
)
def test_decode_field_metadata(raw: dict[bytes, bytes] | None, expected: dict[str, str]) -> None:
    """Decode UTF-8 byte metadata dictionaries or return an empty dict for missing metadata."""
    assert decode_field_metadata(raw) == expected


@pytest.mark.parametrize(
    ("dtype", "expected_binary", "expected_string", "expected_list"),
    [
        pytest.param(pa.binary(), True, False, False, id="binary"),
        pytest.param(pa.large_binary(), True, False, False, id="large-binary"),
        pytest.param(pa.binary_view(), True, False, False, id="binary-view"),
        pytest.param(pa.binary(4), True, False, False, id="fixed-size-binary"),
        pytest.param(pa.string(), False, True, False, id="string"),
        pytest.param(pa.large_string(), False, True, False, id="large-string"),
        pytest.param(pa.string_view(), False, True, False, id="string-view"),
        pytest.param(pa.list_(pa.int64()), False, False, True, id="list"),
        pytest.param(pa.large_list(pa.int64()), False, False, True, id="large-list"),
        pytest.param(pa.list_(pa.int64(), 2), False, False, True, id="fixed-size-list"),
        pytest.param(pa.list_view(pa.int64()), False, False, True, id="list-view"),
        pytest.param(pa.large_list_view(pa.int64()), False, False, True, id="large-list-view"),
        pytest.param(pa.int64(), False, False, False, id="int64"),
        pytest.param(pa.map_(pa.string(), pa.int64()), False, False, False, id="map"),
    ],
)
def test_arrow_type_predicates_classify_binary_string_and_list_variants(
    dtype: pa.DataType,
    expected_binary: bool,
    expected_string: bool,
    expected_list: bool,
) -> None:
    """Classify all PyArrow binary, string, and list type variants without false positives."""
    assert is_any_binary_type(dtype) is expected_binary
    assert is_any_string_type(dtype) is expected_string
    assert is_any_list_type(dtype) is expected_list


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        pytest.param(pa.date32(), True, id="date32"),
        pytest.param(pa.date64(), True, id="date64"),
        pytest.param(pa.timestamp("us", tz="UTC"), True, id="timestamp-utc"),
        pytest.param(pa.time64("us"), True, id="time64"),
        pytest.param(pa.duration("us"), True, id="duration"),
        pytest.param(pa.decimal128(10, 2), True, id="decimal128"),
        pytest.param(pa.decimal256(40, 4), True, id="decimal256"),
        pytest.param(pa.binary(), True, id="binary"),
        pytest.param(pa.large_binary(), True, id="large-binary"),
        pytest.param(pa.binary_view(), True, id="binary-view"),
        pytest.param(pa.binary(8), True, id="fixed-binary"),
        pytest.param(pa.string(), False, id="string"),
        pytest.param(pa.large_string(), False, id="large-string"),
        pytest.param(pa.string_view(), False, id="string-view"),
        pytest.param(pa.int64(), False, id="int64"),
        pytest.param(pa.float64(), False, id="float64"),
        pytest.param(pa.bool_(), False, id="bool"),
        pytest.param(pa.list_(pa.string()), False, id="list-string"),
    ],
)
def test_is_non_json_arrow_type(dtype: pa.DataType, expected: bool) -> None:
    """Detect temporal, decimal, and binary Arrow types that cannot serialize with json.dumps."""
    assert is_non_json_arrow_type(dtype) is expected
