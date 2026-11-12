"""Read PyArrow schemas, fields, and data types into the converter IR."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Final, Never

import pyarrow as pa
from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry, Extensions
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
    SPEC_UINT8,
    SPEC_UINT16,
    SPEC_UINT32,
    SPEC_UINT64,
    NodeHints,
    NodeType,
    Provenance,
    SchemaDocument,
    Source,
    TypedNode,
)
from cdm_data_loaders.converters.core.ir import (
    Field as SchemaField,
)
from cdm_data_loaders.converters.core.ir_values import Value, mutable_value
from cdm_data_loaders.converters.core.metadata import decode_field_metadata

SPEC_FLOAT16: Final[tuple[NodeType, NodeHints]] = ("number", NodeHints(logical_type="float16", bit_width=16))
SPEC_FLOAT32: Final[tuple[NodeType, NodeHints]] = ("number", NodeHints(logical_type="float", bit_width=32))
SPEC_FLOAT64_HINTED: Final[tuple[NodeType, NodeHints]] = ("number", NodeHints(logical_type="double", bit_width=64))
DECIMAL128_BITS: Final[int] = 128
DECIMAL256_BITS: Final[int] = 256

PROVENANCE_METADATA_KEYS: Final[tuple[str, ...]] = ("pydantic_type", "xsd_type", "jsonschema_type")
RESERVED_SCHEMA_METADATA_KEYS: Final[frozenset[str]] = frozenset({"comment", "title", "$id"})

ERR_INVALID_INPUT: Final[str] = "E1"
ERR_EMPTY_CONTAINER: Final[str] = "E2"
ERR_DUPLICATE_FIELD: Final[str] = "E3"
ERR_NULL_TYPE: Final[str] = "E4"
ERR_INVALID_DECIMAL: Final[str] = "E5"
ERR_DURATION_OR_INTERVAL: Final[str] = "E6"
ERR_UNION_TYPE: Final[str] = "E7"
ERR_UNSUPPORTED_TYPE: Final[str] = "E8"

FIXED_SCALAR_SPEC: Final[frozendict[pa.DataType, tuple[NodeType, NodeHints, str]]] = frozendict(
    {
        pa.bool_(): (*SPEC_BOOL, "bool"),
        pa.int8(): (*SPEC_INT8, "int8"),
        pa.int16(): (*SPEC_INT16, "int16"),
        pa.int32(): (*SPEC_INT32, "int32"),
        pa.int64(): (*SPEC_INT64, "int64"),
        pa.uint8(): (*SPEC_UINT8, "uint8"),
        pa.uint16(): (*SPEC_UINT16, "uint16"),
        pa.uint32(): (*SPEC_UINT32, "uint32"),
        pa.uint64(): (*SPEC_UINT64, "uint64"),
        pa.float16(): (*SPEC_FLOAT16, "float16"),
        pa.float32(): (*SPEC_FLOAT32, "float32"),
        pa.float64(): (*SPEC_FLOAT64_HINTED, "float64"),
        pa.date32(): (*SPEC_DATE, "date32"),
        pa.date64(): (*SPEC_DATE, "date64"),
        pa.binary(): (*SPEC_BINARY, "binary"),
        pa.large_binary(): (*SPEC_BINARY, "large_binary"),
        pa.binary_view(): (*SPEC_BINARY, "binary_view"),
        pa.string(): (*SPEC_STRING, "string"),
        pa.large_string(): (*SPEC_STRING, "large_string"),
        pa.string_view(): (*SPEC_STRING, "string_view"),
    }
)

LEGACY_FIXED_SCALAR_SPEC: Final[frozendict[pa.DataType, tuple[NodeType, NodeHints]]] = frozendict(
    {
        **{dtype: (nt, nh) for dtype, (nt, nh, _) in FIXED_SCALAR_SPEC.items()},
        pa.float64(): SPEC_FLOAT64,
    }
)


def _format_path(path: tuple[str | int, ...]) -> str:
    """Render a structural path tuple as a dotted diagnostic string.

    :param path: Path segments from the root schema.
    :type path: tuple[str | int, ...]
    :return: Human-readable dotted path string.
    :rtype: str
    """
    return ".".join(str(part) for part in path) if path else "<root>"


class PyArrowReaderError(ConversionError):
    """Raised when a PyArrow schema, field, or data type cannot be converted into the converter IR."""


@dataclass(frozen=True, slots=True)
class PyArrowReader:
    """Read PyArrow ``pa.Schema``, ``pa.Field``, and ``pa.DataType`` objects into the converter IR."""

    source_name: Source = "pyarrow"
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
    include_pyarrow_metadata: bool = True
    error_type: type[ConversionError] = PyArrowReaderError
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def _raise_error(self, code: str, msg: str) -> Never:
        """Raise the configured exception class for ``code``.

        :param code: Specification error code (``E1``..``E8``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :raises ConversionError: Always raises the mapped ``ConversionError`` subclass.
        """
        exc_cls = self.error_by_code.get(code, self.error_type)
        raise exc_cls(msg)

    def _x_pyarrow(self, payload: Mapping[str, Any]) -> Mapping[str, Value]:
        """Build validated ``x-pyarrow`` extensions when metadata emission is enabled.

        :param payload: Candidate ``x-pyarrow`` payload dictionary.
        :type payload: Mapping[str, Any]
        :return: Validated ``Extensions`` mapping or empty ``frozendict``.
        :rtype: Mapping[str, Value]
        """
        if not self.include_pyarrow_metadata or not payload:
            return frozendict()
        return Extensions({"x-pyarrow": dict(payload)}, self.extension_registry)

    def _validate_field_list(
        self,
        fields: Sequence[pa.Field],
        container_label: str,
        path: tuple[str | int, ...],
    ) -> None:
        """Reject empty field lists and duplicate sibling field names.

        :param fields: Sequence of Arrow fields to validate.
        :type fields: Sequence[pa.Field]
        :param container_label: Diagnostic container label (``"schema"`` or ``"struct"``).
        :type container_label: str
        :param path: Structural path tuple for diagnostics.
        :type path: tuple[str | int, ...]
        :raises ConversionError: On empty ``fields`` (``E2``) or duplicate field names (``E3``).
        """
        loc = f" at '{_format_path(path)}'" if path else ""
        if not fields:
            self._raise_error(
                ERR_EMPTY_CONTAINER,
                f"Empty PyArrow {container_label} with 0 fields is not supported{loc}.",
            )
        seen: set[str] = set()
        for pa_field in fields:
            if pa_field.name in seen:
                self._raise_error(
                    ERR_DUPLICATE_FIELD,
                    f"Duplicate field name {pa_field.name!r} in PyArrow {container_label}{loc}.",
                )
            seen.add(pa_field.name)

    def read(
        self,
        source: pa.Schema,
        *,
        schema_name: str | None = None,
        schema_id: str | None = None,
    ) -> SchemaDocument:
        """Read a PyArrow ``pa.Schema`` into a ``SchemaDocument``.

        :param source: Input PyArrow schema.
        :type source: pa.Schema
        :param schema_name: Optional explicit document / root class name overriding ``metadata["title"]``.
        :type schema_name: str | None
        :param schema_id: Optional explicit document identifier URI overriding ``metadata["$id"]``.
        :type schema_id: str | None
        :return: Structural IR document.
        :rtype: SchemaDocument
        :raises ConversionError: If ``source`` is not a valid non-empty ``pa.Schema``.
        """
        if not isinstance(source, pa.Schema):
            self._raise_error(ERR_INVALID_INPUT, f"Expected a pyarrow.Schema instance, got {type(source).__name__}.")
        raw_fields = list(source)
        self._validate_field_list(raw_fields, "schema", ())

        fields = tuple(self._field(pa_field, (pa_field.name,)) for pa_field in raw_fields)
        required_names = tuple(f.name for f in fields if f.required)
        meta = decode_field_metadata(source.metadata)
        annotations: dict[str, Value] = {"description": meta["comment"]} if meta.get("comment") else {}
        resolved_name = schema_name if schema_name is not None else (meta["title"] if meta.get("title") else None)
        resolved_id = schema_id if schema_id is not None else (meta["$id"] if meta.get("$id") else None)
        extra_meta = {k: v for k, v in meta.items() if k not in RESERVED_SCHEMA_METADATA_KEYS}
        doc_extensions = self._x_pyarrow({"metadata": extra_meta} if extra_meta else {})

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
            name=resolved_name,
            identifier=resolved_id,
            annotations=annotations,
            extensions=doc_extensions,
            provenance=Provenance(self.source_name, ()),
        )

    def read_field(self, source: pa.Field) -> SchemaField:
        """Read a PyArrow field into an IR ``Field``.

        :param source: PyArrow field.
        :type source: pa.Field
        :return: Structural IR field.
        :rtype: SchemaField
        :raises ConversionError: If ``source`` is not a ``pa.Field`` or contains an unsupported type.
        """
        if not isinstance(source, pa.Field):
            self._raise_error(ERR_INVALID_INPUT, f"Expected a pyarrow.Field instance, got {type(source).__name__}.")
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
        :raises ConversionError: If ``dtype`` is not a supported ``pa.DataType``.
        """
        if not isinstance(dtype, pa.DataType):
            self._raise_error(ERR_INVALID_INPUT, f"Expected a pyarrow.DataType instance, got {type(dtype).__name__}.")
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
        annotations: dict[str, Value] = {"description": meta["comment"]} if meta.get("comment") else {}
        prov_meta = {k: meta[k] for k in PROVENANCE_METADATA_KEYS if k in meta}
        extra_meta = {k: v for k, v in meta.items() if k != "comment"}
        field_extensions = self._x_pyarrow({"metadata": extra_meta} if extra_meta else {})
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
            extensions=field_extensions,
            provenance=Provenance(self.source_name, path, prov_meta),
        )

    def _item_node(
        self,
        child_field: pa.Field,
        *,
        nullable: bool,
        path: tuple[str | int, ...],
    ) -> TypedNode:
        """Convert a list element or map key/value ``pa.Field`` into an annotated ``TypedNode``.

        :param child_field: Inner Arrow element, key, or value field.
        :type child_field: pa.Field
        :param nullable: Effective nullability of the element node.
        :type nullable: bool
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Converted IR ``TypedNode`` carrying any element-level description and metadata.
        :rtype: TypedNode
        """
        node = self._type(
            child_field.type,
            nullable=nullable,
            metadata=child_field.metadata,
            path=path,
        )
        meta = decode_field_metadata(child_field.metadata)
        if meta.get("comment"):
            merged_ann = {**mutable_value(node.annotations), "description": meta["comment"]}
            node = replace(node, annotations=merged_ann)
        extra_meta = {k: v for k, v in meta.items() if k != "comment"}
        if self.include_pyarrow_metadata and extra_meta:
            existing_x = dict(mutable_value(node.extensions.get("x-pyarrow", frozendict())))
            existing_x["metadata"] = extra_meta
            node = replace(node, extensions=self._x_pyarrow(existing_x))
        return node

    def _list_node(
        self,
        dtype: pa.DataType,
        *,
        nullable: bool,
        provenance: Provenance,
        path: tuple[str | int, ...],
    ) -> TypedNode:
        """Convert any supported Arrow list variant into an ``array`` ``TypedNode``.

        :param dtype: Arrow list, large_list, list_view, large_list_view, or fixed_size_list type.
        :type dtype: pa.DataType
        :param nullable: Whether the outer list column is nullable.
        :type nullable: bool
        :param provenance: Node provenance record.
        :type provenance: Provenance
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Array IR ``TypedNode``.
        :rtype: TypedNode
        """
        val_field: pa.Field = dtype.value_field
        item_node = self._item_node(
            val_field,
            nullable=val_field.nullable,
            path=(*path, "element"),
        )
        if isinstance(dtype, pa.FixedSizeListType):
            hints = NodeHints(logical_type="fixed-size-list", length=dtype.list_size)
            x_pa: dict[str, Any] = {"arrow_type": "fixed_size_list", "list_size": dtype.list_size}
        elif isinstance(dtype, pa.LargeListType):
            hints = NodeHints()
            x_pa = {"arrow_type": "large_list"}
        elif isinstance(dtype, pa.ListViewType):
            hints = NodeHints()
            x_pa = {"arrow_type": "list_view"}
        elif isinstance(dtype, pa.LargeListViewType):
            hints = NodeHints()
            x_pa = {"arrow_type": "large_list_view"}
        else:
            hints = NodeHints()
            x_pa = {"arrow_type": "list"}
        return TypedNode(
            type="array",
            nullable=nullable,
            hints=hints,
            items=item_node,
            extensions=self._x_pyarrow(x_pa),
            provenance=provenance,
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
        :raises ConversionError: If ``dtype`` uses an unsupported or invalid Arrow construct.
        """
        meta = decode_field_metadata(metadata)
        prov_meta = {k: meta[k] for k in PROVENANCE_METADATA_KEYS if k in meta}
        provenance = Provenance(self.source_name, path, prov_meta)
        loc = _format_path(path)

        if pa.types.is_null(dtype):
            self._raise_error(ERR_NULL_TYPE, f"Standalone PyArrow null type (pa.null()) is not supported at '{loc}'.")
        if isinstance(dtype, pa.BaseExtensionType):
            self._raise_error(ERR_UNSUPPORTED_TYPE, f"Unsupported PyArrow extension type {dtype!r} at '{loc}'.")
        if pa.types.is_duration(dtype) or pa.types.is_interval(dtype):
            self._raise_error(
                ERR_DURATION_OR_INTERVAL,
                f"Unsupported PyArrow scalar type (duration/interval) {dtype} at '{loc}'.",
            )
        if isinstance(dtype, pa.UnionType):
            self._raise_error(
                ERR_UNION_TYPE,
                f"Unsupported PyArrow union type ({dtype.mode} union) {dtype} at '{loc}'.",
            )
        if pa.types.is_run_end_encoded(dtype):
            self._raise_error(
                ERR_UNSUPPORTED_TYPE,
                f"Unsupported PyArrow physical encoding (run_end_encoded) {dtype} at '{loc}'.",
            )

        if isinstance(dtype, pa.DictionaryType):
            val_node = self._type(dtype.value_type, nullable=nullable, metadata=metadata, path=path)
            if not self.include_pyarrow_metadata:
                return val_node
            existing_x = dict(mutable_value(val_node.extensions.get("x-pyarrow", frozendict())))
            inner_arrow = str(existing_x.get("arrow_type", str(dtype.value_type)))
            merged_x = {
                **existing_x,
                "arrow_type": "dictionary",
                "value_type": inner_arrow,
                "index_type": str(dtype.index_type),
                "ordered": bool(dtype.ordered),
            }
            return replace(val_node, extensions=self._x_pyarrow(merged_x))

        if isinstance(dtype, pa.StructType):
            struct_fields = list(dtype)
            self._validate_field_list(struct_fields, "struct", path)
            fields = tuple(self._field(child, (*path, child.name)) for child in struct_fields)
            required_names = tuple(f.name for f in fields if f.required)
            return TypedNode(
                type="object",
                nullable=nullable,
                properties=fields,
                required_names=required_names,
                additional_properties=TypedNode(type="never", nullable=False),
                extensions=self._x_pyarrow({"arrow_type": "struct"}),
                provenance=provenance,
            )

        if isinstance(
            dtype,
            pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType,
        ):
            return self._list_node(dtype, nullable=nullable, provenance=provenance, path=path)

        if isinstance(dtype, pa.MapType):
            key_node = self._item_node(dtype.key_field, nullable=False, path=(*path, "key"))
            val_node = self._item_node(dtype.item_field, nullable=dtype.item_field.nullable, path=(*path, "value"))
            return TypedNode(
                type="map",
                nullable=nullable,
                key_type=key_node,
                value_type=val_node,
                extensions=self._x_pyarrow({"arrow_type": "map", "keys_sorted": bool(dtype.keys_sorted)}),
                provenance=provenance,
            )

        return replace(self._scalar(dtype, provenance, path), nullable=nullable)

    def _scalar(
        self,
        dtype: pa.DataType,
        provenance: Provenance,
        path: tuple[str | int, ...],
    ) -> TypedNode:
        """Convert a scalar PyArrow type into a ``TypedNode`` with ``NodeHints`` and ``x-pyarrow``.

        :param dtype: Scalar PyArrow data type.
        :type dtype: pa.DataType
        :param provenance: Provenance record for the node.
        :type provenance: Provenance
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Scalar IR ``TypedNode``.
        :rtype: TypedNode
        :raises ConversionError: If ``dtype`` is an invalid decimal (``E5``) or unsupported scalar (``E8``).
        """
        if dtype in FIXED_SCALAR_SPEC:
            node_type, hints, arrow_name = FIXED_SCALAR_SPEC[dtype]
            return TypedNode(
                type=node_type,
                nullable=False,
                hints=hints,
                extensions=self._x_pyarrow({"arrow_type": arrow_name}),
                provenance=provenance,
            )
        if isinstance(dtype, pa.Decimal128Type | pa.Decimal256Type):
            if dtype.scale < 0 or dtype.scale > dtype.precision:
                self._raise_error(
                    ERR_INVALID_DECIMAL,
                    f"Invalid PyArrow decimal scale/precision in {dtype} at '{_format_path(path)}': "
                    f"require 0 <= scale <= precision, got precision={dtype.precision}, scale={dtype.scale}.",
                )
            is_128 = isinstance(dtype, pa.Decimal128Type)
            arrow_name = "decimal128" if is_128 else "decimal256"
            bits = DECIMAL128_BITS if is_128 else DECIMAL256_BITS
            return TypedNode(
                type="number",
                nullable=False,
                hints=NodeHints(logical_type="decimal", precision=dtype.precision, scale=dtype.scale, bit_width=bits),
                extensions=self._x_pyarrow(
                    {
                        "arrow_type": arrow_name,
                        "logical_type": "decimal",
                        "precision": dtype.precision,
                        "scale": dtype.scale,
                    }
                ),
                provenance=provenance,
            )
        if isinstance(dtype, pa.FixedSizeBinaryType):
            return TypedNode(
                type="string",
                nullable=False,
                hints=NodeHints(logical_type="fixed", length=dtype.byte_width),
                extensions=self._x_pyarrow({"arrow_type": "fixed_size_binary", "byte_width": dtype.byte_width}),
                provenance=provenance,
            )
        if isinstance(dtype, pa.Time32Type | pa.Time64Type):
            arrow_name = "time32" if isinstance(dtype, pa.Time32Type) else "time64"
            return TypedNode(
                type="string",
                nullable=False,
                hints=NodeHints(logical_type="time"),
                extensions=self._x_pyarrow({"arrow_type": arrow_name, "unit": dtype.unit}),
                provenance=provenance,
            )
        if isinstance(dtype, pa.TimestampType):
            has_tz = bool(dtype.tz)
            logical = "timestamp-with-tz" if has_tz else "timestamp-without-tz"
            x_pa: dict[str, Any] = {"arrow_type": "timestamp", "logical_type": logical, "unit": dtype.unit}
            if has_tz and dtype.tz:
                x_pa["tz"] = dtype.tz
            return TypedNode(
                type="string",
                nullable=False,
                hints=NodeHints(logical_type=logical, timezone=has_tz),
                extensions=self._x_pyarrow(x_pa),
                provenance=provenance,
            )
        exc_cls = self.error_by_code.get(ERR_UNSUPPORTED_TYPE, self.error_type)
        msg = f"Unsupported PyArrow scalar type in PyArrowReader at '{_format_path(path)}': {dtype}"
        raise exc_cls(msg)
