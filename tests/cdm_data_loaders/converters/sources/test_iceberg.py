"""Iceberg source types, scopes and loaded table envelopes."""

import functools
from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID

import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UnknownType,
    UUIDType,
)

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import TypedNode
from cdm_data_loaders.converters.sources.iceberg import IcebergReader, iceberg_value
from tests.helpers.iceberg_catalog import make_iceberg_table as make_table


@pytest.mark.parametrize(
    ("source", "kind", "logical", "width", "timezone"),
    [
        (BooleanType(), "boolean", "boolean", None, None),
        (IntegerType(), "integer", "int", 32, None),
        (LongType(), "integer", "long", 64, None),
        (FloatType(), "number", "float", 32, None),
        (DoubleType(), "number", "double", 64, None),
        (StringType(), "string", "string", None, None),
        (DateType(), "string", "date", None, None),
        (TimeType(), "string", "time", None, None),
        (UUIDType(), "string", "uuid", None, None),
        (BinaryType(), "string", "binary", None, None),
        (TimestampType(), "string", "timestamp-without-tz", None, False),
        (TimestamptzType(), "string", "timestamp-with-tz", None, True),
    ],
    ids=["boolean", "integer", "long", "float", "double", "string", "date", "time", "uuid", "binary", "naive", "aware"],
)
def test_read_pass_primitive_facts(
    source: IcebergType,
    kind: str,
    logical: str,
    width: int | None,
    timezone: bool | None,
) -> None:
    """Physical widths and timestamp timezone distinctions remain available to emitters."""
    node = IcebergReader().read(Schema(NestedField(1, "data", source, required=True))).root.properties[0].node
    assert (node.type, node.hints.logical_type, node.hints.bit_width, node.hints.timezone) == (
        kind,
        logical,
        width,
        timezone,
    )
    assert node.nullable is False


def test_read_pass_nested_types_and_metadata_scopes() -> None:
    """Maps, list elements, structs and defaults carry separate type and field facts."""
    schema = Schema(
        NestedField(1, "amount", DecimalType(20, 4), initial_default=Decimal("1.2300"), write_default=None, doc=None),
        NestedField(2, "fixed", FixedType(12)),
        NestedField(
            3, "entries", ListType(4, StructType(NestedField(5, "name", StringType())), element_required=False)
        ),
        NestedField(6, "map", MapType(7, IntegerType(), 8, StringType(), value_required=False)),
    )
    document = IcebergReader().read(schema)
    amount, fixed, entries, mapping = document.root.properties
    assert amount.node.type == "number"
    assert (amount.node.hints.precision, amount.node.hints.scale) == (20, 4)
    assert amount.node.constraints == {}
    assert amount.extensions["x-iceberg"] == {
        "field_id": 1,
        "required": False,
        "initial_default": Decimal("1.2300"),
        "write_default": None,
    }
    assert amount.annotations == {"description": None}
    assert not amount.node.extensions
    assert (fixed.node.hints.length,) == (12,)
    assert fixed.extensions["x-iceberg"]["initial_default"] is None
    assert entries.node.items.type == "object"
    assert entries.node.items.nullable is True
    assert entries.node.items.properties[0].name == "name"
    assert entries.node.extensions["x-iceberg"] == {"element_id": 4, "element_required": False}
    assert mapping.node.type == "map"
    assert mapping.node.key_type.type == "integer"
    assert mapping.node.key_type.nullable is False
    assert mapping.node.value_type.nullable is True
    assert mapping.node.extensions["x-iceberg"] == {"key_id": 7, "value_id": 8, "value_required": False}


def test_read_table_pass_envelope_without_io() -> None:
    """Real in-memory table metadata includes partition, property and snapshot facts."""
    schema = Schema(NestedField(1, "id", LongType(), required=True), identifier_field_ids=[1])
    table = make_table(
        ("ns", "table"),
        schema,
        PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "id")),
        {"comment": "records"},
    )
    document = IcebergReader().read_table(table, ("ns", "table"))
    assert document.identifier == "urn:iceberg:ns.table"
    assert document.name == "table"
    assert document.annotations == {"description": "records"}
    assert document.extensions["x-iceberg"] == {
        "identifier": ("ns", "table"),
        "schema_id": table.schema().schema_id,
        "format_version": table.metadata.format_version,
        "current_snapshot_id": None,
        "location": table.metadata.location,
        "properties": {"comment": "records"},
        "partition_spec": ({"source_id": 1, "name": "id", "transform": "identity"},),
        "identifier_field_ids": (1,),
    }


def test_read_pass_empty_schema() -> None:
    """Empty Iceberg schemas produce an object root with no properties."""
    assert IcebergReader().read(Schema()).root.properties == ()


@pytest.mark.parametrize(
    ("call", "match"),
    [
        pytest.param(
            functools.partial(IcebergReader().read, {}),  # type: ignore[arg-type]
            None,
            id="invalid-source-mapping",
        ),
        pytest.param(
            functools.partial(IcebergReader().read_table, make_table(("ns", "table"), Schema()), ()),
            "identifier",
            id="empty-table-identifier",
        ),
        pytest.param(
            functools.partial(IcebergReader().read_table, make_table(("ns", "table"), Schema()), ("", "t")),
            "nonempty strings",
            id="empty-identifier-part",
        ),
        pytest.param(
            functools.partial(
                IcebergReader().read_table,
                make_table(("ns", "table"), Schema()),
                (123, "t"),  # type: ignore[arg-type]
            ),
            "nonempty strings",
            id="non-string-identifier-part",
        ),
    ],
)
def test_read_fail_invalid_source_and_identifier(
    call: Callable[[], object],
    match: str | None,
) -> None:
    """Invalid source objects and empty identifiers fail at the public boundary with ConversionError."""
    with pytest.raises(ConversionError, match=match):
        call()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1.2300"), Decimal("1.2300")),
        (b"\x00\xff", "AP8="),
        (UUID(int=0), "00000000-0000-0000-0000-000000000000"),
        (datetime(2026, 1, 1, tzinfo=UTC), "2026-01-01T00:00:00+00:00"),
        (date(2026, 1, 1), "2026-01-01"),
        (time(1, 2, tzinfo=UTC), "01:02:00+00:00"),
        ({"nested": [None, Decimal("0.0001")]}, {"nested": (None, Decimal("0.0001"))}),
    ],
    ids=["decimal", "binary", "uuid", "timestamp", "date", "time", "nested"],
)
def test_iceberg_value_pass_precise_serialization(value: object, expected: object) -> None:
    """Typed metadata serializes explicitly without a Decimal-to-float conversion."""
    assert iceberg_value(value) == expected


@pytest.mark.parametrize(
    ("call", "error_type", "match"),
    [
        pytest.param(
            functools.partial(IcebergReader().read, Schema(NestedField(1, "unknown", UnknownType()))),
            ConversionError,
            "No IR mapping",
            id="unmapped-unknown-type",
        ),
        pytest.param(
            functools.partial(iceberg_value, object()),
            TypeError,
            "Unsupported",
            id="unsupported-python-default",
        ),
    ],
)
def test_read_fail_unsupported_type_and_metadata(
    call: Callable[[], object],
    error_type: type[Exception],
    match: str,
) -> None:
    """Unmapped Iceberg types and arbitrary Python defaults are not guessed."""
    with pytest.raises(error_type, match=match):
        call()


def test_read_pass_provenance_and_extensions() -> None:
    """IcebergReader.read preserves schema provenance paths, x-iceberg metadata, and closed root additional_properties."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=True, doc="primary id"),
        NestedField(2, "nested", StructType(NestedField(3, "inner", StringType()))),
        schema_id=7,
        identifier_field_ids=[1],
    )
    doc = IcebergReader().read(schema)
    expected_schema_id = 7
    assert doc.extensions["x-iceberg"] == {"schema_id": expected_schema_id, "identifier_field_ids": (1,)}
    assert doc.provenance is not None
    assert doc.provenance.source == "iceberg"
    assert doc.provenance.metadata["schema-id"] == expected_schema_id
    assert doc.root.additional_properties == TypedNode(type="never", nullable=False)
    id_field, nested_field = doc.root.properties
    assert id_field.annotations == {"description": "primary id"}
    assert id_field.provenance is not None
    assert id_field.provenance.path == ("id",)
    assert nested_field.node.properties[0].provenance is not None
    assert nested_field.node.properties[0].provenance.path == ("nested", "inner")


def test_read_node_pass_standalone_list_provenance() -> None:
    """read_node attaches empty root provenance and element path for standalone lists."""
    standalone_node = IcebergReader().read_node(ListType(10, StringType(), element_required=False))
    assert standalone_node.provenance is not None
    assert standalone_node.provenance.path == ()
    assert standalone_node.items is not None
    assert standalone_node.items.provenance is not None
    assert standalone_node.items.provenance.path == ("element",)


def test_read_field_pass_standalone_map_provenance() -> None:
    """read_field attaches field, key, and value provenance paths for standalone maps."""
    standalone_field = IcebergReader().read_field(NestedField(11, "col", MapType(12, StringType(), 13, IntegerType())))
    assert standalone_field.provenance is not None
    assert standalone_field.provenance.path == ("col",)
    assert standalone_field.annotations == {"description": None}
    assert standalone_field.node.key_type is not None
    assert standalone_field.node.key_type.provenance is not None
    assert standalone_field.node.key_type.provenance.path == ("col", "key")
    assert standalone_field.node.value_type is not None
    assert standalone_field.node.value_type.provenance is not None
    assert standalone_field.node.value_type.provenance.path == ("col", "value")


def test_read_table_pass_without_comment() -> None:
    """read_table leaves annotations empty and records identifier and format version when no comment is set."""
    table_no_comment = make_table(("ns", "plain"), Schema(NestedField(1, "id", LongType(), required=True)))
    table_doc = IcebergReader().read_table(table_no_comment, ("ns", "plain"))
    assert table_doc.annotations == {}
    assert table_doc.provenance is not None
    assert table_doc.provenance.path == ("ns", "plain")
    assert "format-version" in table_doc.provenance.metadata
