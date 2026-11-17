"""Unit tests for ``cdm_data_loaders.utils.arrow_schema``."""

import pyarrow as pa
import pytest

from cdm_data_loaders.utils.arrow_schema import assemble_pyarrow_schema, decode_field_metadata


def test_assemble_pyarrow_schema_from_struct_and_sequence() -> None:
    """Assemble identical schemas from a ``pa.StructType`` or a sequence of ``pa.Field`` instances."""
    fields = [
        pa.field("id", pa.string(), nullable=False),
        pa.field("count", pa.int64(), nullable=True),
    ]
    struct_type = pa.struct(fields)
    from_struct = assemble_pyarrow_schema(struct_type, metadata={"source": "unit"})
    from_fields = assemble_pyarrow_schema(fields, metadata={"source": "unit"})
    assert from_struct.equals(from_fields, check_metadata=True)
    assert from_struct.metadata == {b"source": b"unit"}


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
