"""Read Pydantic v2 BaseModel definitions into the converter IR."""

import inspect
from dataclasses import dataclass, replace
from typing import Any, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import BaseModel

from cdm_data_loaders.converters.core.ir import Field, NodeHints, NodeType, Provenance, SchemaDocument, TypedNode
from cdm_data_loaders.converters.pydantic_to_pyarrow import (
    DEFAULT_MAX_RECURSION_DEPTH,
    PydanticToPyArrowError,
    generate_pyarrow_schema,
)

FIXED_SCALAR_SPEC: Final[frozendict[pa.DataType, tuple[NodeType, NodeHints]]] = frozendict(
    {
        pa.bool_(): ("boolean", NodeHints(logical_type="boolean")),
        pa.int64(): ("integer", NodeHints(logical_type="long", bit_width=64)),
        pa.float64(): ("number", NodeHints(logical_type="double")),
        pa.date32(): ("string", NodeHints(logical_type="date")),
        pa.time64("us"): ("string", NodeHints(logical_type="time")),
        pa.binary(): ("string", NodeHints(logical_type="binary")),
        pa.string(): ("string", NodeHints(logical_type="string")),
    }
)


def _decode_metadata(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    """Decode Arrow byte key/value metadata into string key/value pairs."""
    if not metadata:
        return {}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in metadata.items()}


def _clean_docstring(cls: type[Any]) -> str | None:
    """Return a cleaned class docstring if explicitly defined on ``cls``."""
    raw = cls.__dict__.get("__doc__")
    if not raw or not isinstance(raw, str):
        return None
    cleaned = inspect.cleandoc(raw).strip()
    return cleaned or None


@dataclass(frozen=True, slots=True)
class PydanticReader:
    """Build typed converter IR documents from Pydantic v2 BaseModel classes.

    :param max_recursion_depth: Maximum times a recursive ``BaseModel`` may be expanded along an ancestor path.
    :type max_recursion_depth: int
    :param by_alias: When ``True``, use field aliases rather than Python attribute names.
    :type by_alias: bool
    """

    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    by_alias: bool = False

    def __post_init__(self) -> None:
        """Validate reader configuration."""
        if isinstance(self.max_recursion_depth, bool) or not isinstance(self.max_recursion_depth, int):
            msg = "max_recursion_depth must be an integer >= 1"
            raise TypeError(msg)
        if self.max_recursion_depth < 1:
            msg = "max_recursion_depth must be >= 1"
            raise ValueError(msg)
        if type(self.by_alias) is not bool:
            msg = "by_alias must be a boolean"
            raise TypeError(msg)

    def read(self, source: type[BaseModel]) -> SchemaDocument:
        """Read a Pydantic ``BaseModel`` subclass into a ``SchemaDocument``.

        :param source: Pydantic v2 ``BaseModel`` subclass.
        :type source: type[BaseModel]
        :return: Structural IR document with ``source="pydantic"`` provenance.
        :rtype: SchemaDocument
        :raises PydanticToPyArrowError: If the model or any of its fields violates ``E1``-``E12``.
        """
        pa_schema = generate_pyarrow_schema(
            source,
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
        )
        doc = _clean_docstring(source)
        fields = tuple(self._field(pa_field, (pa_field.name,)) for pa_field in pa_schema)
        required_names = tuple(f.name for f in fields if f.required)
        annotations = {"description": doc} if doc is not None else {}
        root = TypedNode(
            type="object",
            nullable=False,
            properties=fields,
            required_names=required_names,
            additional_properties=TypedNode(type="never", nullable=False),
            annotations=annotations,
            provenance=Provenance("pydantic", ()),
        )
        qualname = source.__qualname__.replace(".<locals>.", ".")
        return SchemaDocument(
            root=root,
            name=source.__name__,
            identifier=f"urn:pydantic:{source.__module__}.{qualname}",
            annotations=annotations,
            provenance=Provenance("pydantic", (source.__name__,)),
        )

    def read_field(self, source: pa.Field) -> Field:
        """Read a PyArrow field produced by ``generate_pyarrow_schema`` into an IR ``Field``.

        :param source: PyArrow field produced by ``generate_pyarrow_schema``.
        :type source: pa.Field
        :return: Structural IR field with ``source="pydantic"`` provenance.
        :rtype: Field
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
        """Read a PyArrow data type produced by ``generate_pyarrow_schema`` into an IR ``TypedNode``.

        :param dtype: PyArrow data type to convert.
        :type dtype: pa.DataType
        :param nullable: Whether the node allows ``None`` values.
        :type nullable: bool
        :param metadata: Optional raw Arrow field metadata bytes.
        :type metadata: dict[bytes, bytes] | None
        :param path: Structural provenance path tuple.
        :type path: tuple[str | int, ...]
        :return: Structural IR node with ``source="pydantic"`` provenance.
        :rtype: TypedNode
        """
        return self._type(dtype, nullable=nullable, metadata=metadata, path=path)

    def _field(self, pa_field: pa.Field, path: tuple[str | int, ...]) -> Field:
        """Convert a PyArrow field into an IR ``Field`` with separated presence and description."""
        meta = _decode_metadata(pa_field.metadata)
        annotations = {"description": meta["comment"]} if "comment" in meta else {}
        prov_meta = {"pydantic_type": meta["pydantic_type"]} if "pydantic_type" in meta else {}
        node = self._type(
            pa_field.type,
            nullable=pa_field.nullable,
            metadata=pa_field.metadata,
            path=path,
        )
        return Field(
            name=pa_field.name,
            node=node,
            required=not pa_field.nullable,
            annotations=annotations,
            provenance=Provenance("pydantic", path, prov_meta),
        )

    def _type(
        self,
        dtype: pa.DataType,
        *,
        nullable: bool,
        metadata: dict[bytes, bytes] | None,
        path: tuple[str | int, ...],
    ) -> TypedNode:
        """Convert a PyArrow ``DataType`` into a structural IR ``TypedNode``."""
        meta = _decode_metadata(metadata)
        prov_meta = {"pydantic_type": meta["pydantic_type"]} if "pydantic_type" in meta else {}
        provenance = Provenance("pydantic", path, prov_meta)

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
        """Convert a scalar PyArrow type into a ``TypedNode`` with ``NodeHints``."""
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
        msg = f"Unsupported PyArrow scalar type in PydanticReader: {dtype}"
        raise PydanticToPyArrowError(msg)
