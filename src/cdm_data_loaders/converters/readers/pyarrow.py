"""Read PyArrow schemas, fields, and data types into the converter IR."""

from dataclasses import dataclass, replace
from typing import Final

import pyarrow as pa
from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import (
    SPEC_BINARY,
    SPEC_BOOL,
    SPEC_DATE,
    SPEC_FLOAT64,
    SPEC_INT8,
    SPEC_INT16,
    SPEC_INT32,
    SPEC_INT64,
    SPEC_STRING,
    SPEC_TIME,
    SPEC_UINT8,
    SPEC_UINT16,
    SPEC_UINT32,
    SPEC_UINT64,
    NodeHints,
    NodeType,
    Provenance,
    SchemaDocument,
    TypedNode,
)
from cdm_data_loaders.converters.core.ir import (
    Field as SchemaField,
)
from cdm_data_loaders.converters.core.metadata import decode_field_metadata

FIXED_SCALAR_SPEC: Final[frozendict[pa.DataType, tuple[NodeType, NodeHints]]] = frozendict(
    {
        pa.bool_(): SPEC_BOOL,
        pa.int8(): SPEC_INT8,
        pa.int16(): SPEC_INT16,
        pa.int32(): SPEC_INT32,
        pa.int64(): SPEC_INT64,
        pa.uint8(): SPEC_UINT8,
        pa.uint16(): SPEC_UINT16,
        pa.uint32(): SPEC_UINT32,
        pa.uint64(): SPEC_UINT64,
        pa.float64(): SPEC_FLOAT64,
        pa.date32(): SPEC_DATE,
        pa.time64("us"): SPEC_TIME,
        pa.binary(): SPEC_BINARY,
        pa.string(): SPEC_STRING,
    }
)


class PyArrowReaderError(ConversionError):
    """Raised when a PyArrow type cannot be converted into the converter IR."""


@dataclass(frozen=True, slots=True)
class PyArrowReader:
    """Read PyArrow ``pa.Schema``, ``pa.Field``, and ``pa.DataType`` objects into the converter IR."""

    source_name: str = "pyarrow"

    def read(self, source: pa.Schema) -> SchemaDocument:
        """Read a PyArrow ``pa.Schema`` into a ``SchemaDocument``.

        :param source: Input PyArrow schema.
        :type source: pa.Schema
        :return: Structural IR document.
        :rtype: SchemaDocument
        """
        fields = tuple(self._field(pa_field, (pa_field.name,)) for pa_field in source)
        required_names = tuple(f.name for f in fields if f.required)
        meta = decode_field_metadata(source.metadata)
        annotations = {"description": meta["comment"]} if "comment" in meta else {}
        root = TypedNode(
            type="object",
            nullable=False,
            properties=fields,
            required_names=required_names,
            additional_properties=TypedNode(type="never", nullable=False),
            annotations=annotations,
            provenance=Provenance(self.source_name, ()),
        )
        return SchemaDocument(
            root=root,
            annotations=annotations,
            provenance=Provenance(self.source_name, ()),
        )

    def read_field(self, source: pa.Field) -> SchemaField:
        """Read a PyArrow field into an IR ``Field``.

        :param source: PyArrow field.
        :type source: pa.Field
        :return: Structural IR field.
        :rtype: SchemaField
        """
        return self._field(source, (source.name,))

    def read_node(
        self,
        dtype: pa.DataType,
        *,
        nullable: bool = False,
        metadata: dict[bytes, bytes] | None = None,
        path: tuple[str | int, ...] = (),
    ) -> TypedNode:
        """Read a PyArrow data type into an IR ``TypedNode``.

        :param dtype: PyArrow data type to convert.
        :type dtype: pa.DataType
        :param nullable: Whether the node allows ``None`` values.
        :type nullable: bool
        :param metadata: Optional raw Arrow field metadata bytes.
        :type metadata: dict[bytes, bytes] | None
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Structural IR node.
        :rtype: TypedNode
        """
        return self._type(dtype, nullable=nullable, metadata=metadata, path=path)

    def _field(self, pa_field: pa.Field, path: tuple[str | int, ...]) -> SchemaField:
        """Convert a PyArrow field into an IR ``Field`` with separated presence and description.

        :param pa_field: Input PyArrow field.
        :type pa_field: pa.Field
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Structural IR field.
        :rtype: SchemaField
        """
        meta = decode_field_metadata(pa_field.metadata)
        annotations = {"description": meta["comment"]} if "comment" in meta else {}
        prov_meta = {k: meta[k] for k in ("pydantic_type", "xsd_type", "jsonschema_type") if k in meta}
        node = self._type(
            pa_field.type,
            nullable=pa_field.nullable,
            metadata=pa_field.metadata,
            path=path,
        )
        return SchemaField(
            name=pa_field.name,
            node=node,
            required=not pa_field.nullable,
            annotations=annotations,
            provenance=Provenance(self.source_name, path, prov_meta),
        )

    def _type(
        self,
        dtype: pa.DataType,
        *,
        nullable: bool,
        metadata: dict[bytes, bytes] | None,
        path: tuple[str | int, ...],
    ) -> TypedNode:
        """Convert a PyArrow ``DataType`` into a structural IR ``TypedNode``.

        :param dtype: Input PyArrow data type.
        :type dtype: pa.DataType
        :param nullable: Whether the node is nullable.
        :type nullable: bool
        :param metadata: Optional raw Arrow field metadata bytes.
        :type metadata: dict[bytes, bytes] | None
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Structural IR node.
        :rtype: TypedNode
        """
        meta = decode_field_metadata(metadata)
        prov_meta = {k: meta[k] for k in ("pydantic_type", "xsd_type", "jsonschema_type") if k in meta}
        provenance = Provenance(self.source_name, path, prov_meta)

        if isinstance(dtype, pa.StructType):
            fields = tuple(self._field(child, (*path, child.name)) for child in dtype)
            required_names = tuple(f.name for f in fields if f.required)
            return TypedNode(
                type="object",
                nullable=nullable,
                properties=fields,
                required_names=required_names,
                additional_properties=TypedNode(type="never", nullable=False),
                provenance=provenance,
            )

        if isinstance(dtype, pa.ListType):
            val_field = dtype.value_field
            item_node = self._type(
                val_field.type,
                nullable=val_field.nullable,
                metadata=val_field.metadata,
                path=(*path, "element"),
            )
            return TypedNode(
                type="array",
                nullable=nullable,
                items=item_node,
                provenance=provenance,
            )

        if isinstance(dtype, pa.MapType):
            key_field = dtype.key_field
            item_field = dtype.item_field
            key_node = self._type(
                key_field.type,
                nullable=False,
                metadata=key_field.metadata,
                path=(*path, "key"),
            )
            val_node = self._type(
                item_field.type,
                nullable=item_field.nullable,
                metadata=item_field.metadata,
                path=(*path, "value"),
            )
            return TypedNode(
                type="map",
                nullable=nullable,
                key_type=key_node,
                value_type=val_node,
                provenance=provenance,
            )

        return replace(self._scalar(dtype, provenance), nullable=nullable)

    @staticmethod
    def _scalar(dtype: pa.DataType, provenance: Provenance) -> TypedNode:
        """Convert a scalar PyArrow type into a ``TypedNode`` with ``NodeHints``.

        :param dtype: Scalar PyArrow data type.
        :type dtype: pa.DataType
        :param provenance: Provenance record for the node.
        :type provenance: Provenance
        :return: Scalar IR ``TypedNode``.
        :rtype: TypedNode
        :raises PyArrowReaderError: If ``dtype`` is not a supported scalar Arrow type.
        """
        if dtype in FIXED_SCALAR_SPEC:
            node_type, hints = FIXED_SCALAR_SPEC[dtype]
            return TypedNode(type=node_type, nullable=False, hints=hints, provenance=provenance)
        if isinstance(dtype, pa.Decimal128Type):
            return TypedNode(
                type="number",
                nullable=False,
                hints=NodeHints(logical_type="decimal", precision=dtype.precision, scale=dtype.scale),
                provenance=provenance,
            )
        if isinstance(dtype, pa.TimestampType):
            has_tz = dtype.tz is not None
            return TypedNode(
                type="string",
                nullable=False,
                hints=NodeHints(
                    logical_type="timestamp-with-tz" if has_tz else "timestamp-without-tz",
                    timezone=has_tz,
                ),
                provenance=provenance,
            )
        msg = f"Unsupported PyArrow scalar type in PyArrowReader: {dtype}"
        raise PyArrowReaderError(msg)
