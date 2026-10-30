"""Emit PyArrow schema objects from immutable typed schema nodes."""

import json
import logging
from decimal import Decimal
from typing import ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from jsonschema import Draft202012Validator
from pydantic import Field, field_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.inference import decimal_places, json_type_from_enum
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import NodeHints, SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.metadata import ConversionContext
from cdm_data_loaders.converters.emitters._tabular_base import (
    DECIMAL128_MAX_PRECISION,
    DEFAULT_STRING_FORMATS,
    INT32_MAX,
    INT32_MIN,
    INT32_WIDTH,
    _TabularEmitterBase,
    fits_int32_bounds,
    merge_format_map_with_defaults,
)

logger = logging.getLogger(__name__)

INT8_WIDTH: Final = 8
INT16_WIDTH: Final = 16
DECIMAL256_MAX_PRECISION: Final = 76
WEI_PRECISION: Final = DECIMAL256_MAX_PRECISION
MAX_MILLIS_DIGITS: Final = 3
MAX_MICROS_DIGITS: Final = 6
DEFAULT_FORMAT_MAP: Final[frozendict[str, pa.DataType]] = frozendict(
    {
        "date": pa.date32(),
        "date-time": pa.timestamp("us", tz="UTC"),
        **dict.fromkeys(DEFAULT_STRING_FORMATS, pa.string()),
    }
)


def merge_format_map(value: object) -> object:
    """Validate format overrides and merge them into the built-in mapping.

    :param value: a mapping from JSON Schema format names to PyArrow types
    :type value: object
    :return: the merged mapping, or ``value`` unchanged when it is not a mapping
    :rtype: object
    :raises ValueError: if a key is not a string or a value is not a PyArrow ``DataType``
    """
    return merge_format_map_with_defaults(value, DEFAULT_FORMAT_MAP, pa.DataType, "PyArrow")


class PyArrowEmitterError(ConversionError):
    """A typed schema cannot be represented by the configured Arrow policy."""


class PyArrowEmitter(_TabularEmitterBase[pa.DataType]):
    """Convert typed schema nodes into PyArrow schemas and empty tables."""

    _error_cls: ClassVar[type[ConversionError]] = PyArrowEmitterError
    _target_name: ClassVar[str] = "Arrow"
    _string_type_label: ClassVar[str] = "string"
    _union_approx_reason: ClassVar[str] = "Arrow has no union type compatible with table storage."
    _array_label: ClassVar[str] = "Arrow lists"
    _slot_label: ClassVar[str] = "list's"

    format_map: frozendict[str, pa.DataType] = Field(default_factory=lambda: DEFAULT_FORMAT_MAP)

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: object) -> object:
        """Merge nonempty format overrides into the built-in mapping."""
        return merge_format_map(value) if value else value

    def emit(self, document: SchemaDocument) -> pa.Schema:
        """Emit a table schema, requiring the root to be a struct.

        :param document: the schema document to convert
        :type document: SchemaDocument
        :return: the Arrow schema
        :rtype: pa.Schema
        :raises PyArrowEmitterError: if the root does not resolve to a struct
        """
        validator_cls = document.jsonschema_validator_cls or Draft202012Validator
        result = self.emit_node(document.root, self.build_context(validator_cls))
        if pa.types.is_struct(result):
            return pa.schema([result.field(index) for index in range(result.num_fields)])
        msg = (
            "Root JSON Schema did not resolve to a struct.\nThis can happen if it has no "
            "'properties' but does have 'patternProperties' or 'additionalProperties', which map to "
            "a map type instead. An Arrow table requires a struct at the top level of its schema."
        )
        raise PyArrowEmitterError(msg)

    def emit_table(self, document: SchemaDocument) -> pa.Table:
        """Emit an empty table with the document's schema.

        :param document: the schema document to convert
        :type document: SchemaDocument
        :return: a zero-row table
        :rtype: pa.Table
        :raises PyArrowEmitterError: if the root does not resolve to a struct
        """
        return self.emit(document).empty_table()

    def build_metadata(
        self, node: TypedNode, ctx: ConversionContext, field: SchemaField | None = None
    ) -> dict[str, str]:
        """Select field metadata and encode it as Arrow's string key/value pairs.

        :param node: the node whose constraints, annotations and extensions are searched
        :type node: TypedNode
        :param ctx: draft-specific metadata context
        :type ctx: ConversionContext
        :param field: the owning field, whose annotations and extensions take precedence
        :type field: SchemaField | None
        :return: ``jsonschema`` (JSON object), ``comment`` and ``original_union`` (JSON array) entries
        :rtype: dict[str, str]
        """
        values, jsonschema, original_union = self._select_metadata_parts(node, ctx, field)
        metadata: dict[str, str] = {}
        if jsonschema:
            metadata["jsonschema"] = _dump_json(jsonschema)
        description = values.get("description")
        if isinstance(description, str):
            metadata["comment"] = description
        if original_union is not None:
            metadata["original_union"] = _dump_json(original_union)
        return metadata

    def _string_type(self) -> pa.DataType:
        return pa.string()

    def _scalar_types(self) -> dict[str, pa.DataType]:
        return {"boolean": pa.bool_(), "null": pa.null(), "collapsed-union": pa.string()}

    def _infer_enum_type(self, values: list[object]) -> pa.DataType:
        return infer_type_from_enum(values)

    def _emit_object(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> pa.DataType:
        """Emit fixed fields, a dynamic map, or an empty struct."""
        if depth >= self.max_nesting:
            logger.warning("Max nesting limit (%d) reached; collapsing object to string.", self.max_nesting)
            return pa.string()

        additional = node.additional_properties
        schema_additional = additional is not None and additional.type not in {"any", "never"}
        if node.properties:
            if node.pattern_properties or schema_additional:
                logger.warning(
                    "Object schema declares both fixed 'properties' and dynamic 'patternProperties' / "
                    "'additionalProperties'; an Arrow struct requires fixed field names, so the "
                    "dynamic keys are ignored."
                )
            return pa.struct(
                [
                    pa.field(
                        prop.name,
                        self.emit_node(prop.node, ctx, depth + 1),
                        nullable=not prop.required,
                        metadata=self.build_metadata(prop.node, ctx, prop) or None,
                    )
                    for prop in node.properties
                ]
            )
        value_node = additional if schema_additional else None
        if node.pattern_properties:
            if len(node.pattern_properties) > 1:
                logger.warning(
                    "Multiple 'patternProperties' patterns found; only the first pattern's schema is "
                    "used as the map value type, since an Arrow map has a single value type."
                )
            value_node = next(iter(node.pattern_properties.values()))
        if value_node is not None:
            return pa.map_(pa.string(), pa.field("value", self.emit_node(value_node, ctx, depth + 1), nullable=True))
        logger.warning(
            "Object schema has no 'properties', 'patternProperties', or schema-valued "
            "'additionalProperties'; converting to an empty struct."
        )
        return pa.struct([])

    def _emit_map(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> pa.MapType:
        """Emit a map with independently typed keys and values."""
        if node.key_type is None or node.value_type is None:
            msg = "Map nodes require both key_type and value_type"
            raise PyArrowEmitterError(msg)
        value = pa.field(
            "value", self.emit_node(node.value_type, ctx, depth + 1), nullable=_element_nullable(node.value_type)
        )
        return pa.map_(self.emit_node(node.key_type, ctx, depth + 1), value)

    def _emit_array(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> pa.ListType:
        """Emit homogeneous arrays, approximating tuple schemas by their first slot."""
        element = self._resolve_array_element(node)
        if element is None:
            return pa.list_(pa.field("item", pa.string(), nullable=True))
        return pa.list_(pa.field("item", self.emit_node(element, ctx, depth + 1), nullable=_element_nullable(element)))

    def _emit_string(self, node: TypedNode) -> pa.DataType:
        """Map recognized formats to Arrow types."""
        value = node.constraints.get("format")
        return self.format_map.get(value, pa.string()) if isinstance(value, str) else pa.string()

    @staticmethod
    def _emit_integer(node: TypedNode) -> pa.DataType:
        """Use int32 only when both bounds fit."""
        return pa.int32() if fits_int32_bounds(node) else pa.int64()

    @staticmethod
    def _emit_number(node: TypedNode) -> pa.DataType:
        """Derive decimal scale from numeric JSON multipleOf values."""
        multiple = node.constraints.get("multipleOf")
        if isinstance(multiple, (int, float, Decimal)):
            scale = decimal_places(multiple)
            if scale > DECIMAL128_MAX_PRECISION:
                msg = f"Unsupported Arrow decimal precision/scale: ({DECIMAL128_MAX_PRECISION}, {scale})"
                raise PyArrowEmitterError(msg)
            return pa.decimal128(DECIMAL128_MAX_PRECISION, scale)
        return pa.float64()

    @staticmethod
    def _emit_logical(node: TypedNode) -> pa.DataType | None:
        """Preserve physical widths and logical source types without a JSON encoding."""
        if node.provenance is not None and node.provenance.source == "json-schema":
            return None
        hints = node.hints
        logical = hints.logical_type
        if logical in {"decimal", "wei"}:
            return _decimal_type(hints, wei=logical == "wei")
        if logical in {"timestamp", "timestamp-with-tz", "timestamp-without-tz"}:
            unit = _timestamp_unit(hints.precision)
            naive = hints.timezone is False or logical == "timestamp-without-tz"
            return pa.timestamp(unit) if naive else pa.timestamp(unit, tz="UTC")
        if node.type == "integer" and hints.bit_width is not None:
            return _integer_type(hints.bit_width)
        if node.type == "number" and (logical == "float" or hints.bit_width == INT32_WIDTH):
            return pa.float32()
        fixed = pa.binary(hints.length) if hints.length is not None else pa.binary()
        return {"date": pa.date32(), "time": pa.time64("us"), "binary": pa.binary(), "fixed": fixed}.get(logical)


def infer_type_from_enum(values: list[object]) -> pa.DataType:
    """Map homogeneous JSON enum values to Arrow scalar types.

    :param values: the enum members
    :type values: list[object]
    :return: ``bool_``, ``int64``, ``float64`` or ``string``
    :rtype: pa.DataType
    """
    return {
        "boolean": pa.bool_(),
        "integer": pa.int64(),
        "number": pa.float64(),
        "string": pa.string(),
    }[json_type_from_enum(values)]


def _element_nullable(node: TypedNode) -> bool:
    """Honor non-null element facts from typed sources; JSON item schemas stay nullable."""
    from_json = node.provenance is None or node.provenance.source == "json-schema"
    return from_json or node.nullable is not False


def _decimal_type(hints: NodeHints, *, wei: bool) -> pa.DataType:
    """Select decimal128 or decimal256 from precision and scale hints."""
    default_precision = WEI_PRECISION if wei else DECIMAL128_MAX_PRECISION
    precision = hints.precision if hints.precision is not None else default_precision
    scale = hints.scale if hints.scale is not None else 0
    if not 1 <= precision <= DECIMAL256_MAX_PRECISION or scale > precision:
        msg = f"Unsupported Arrow decimal precision/scale: ({precision}, {scale})"
        raise PyArrowEmitterError(msg)
    return pa.decimal128(precision, scale) if precision <= DECIMAL128_MAX_PRECISION else pa.decimal256(precision, scale)


def _timestamp_unit(precision: int | None) -> str:
    """Convert a count of fractional second digits to an Arrow time unit."""
    if precision is None:
        return "us"
    if precision == 0:
        return "s"
    if precision <= MAX_MILLIS_DIGITS:
        return "ms"
    return "us" if precision <= MAX_MICROS_DIGITS else "ns"


def _integer_type(bit_width: int) -> pa.DataType:
    """Select the narrowest signed Arrow integer holding the given bit width."""
    if bit_width <= INT8_WIDTH:
        return pa.int8()
    if bit_width <= INT16_WIDTH:
        return pa.int16()
    return pa.int32() if bit_width <= INT32_WIDTH else pa.int64()


def _json_default(value: object) -> str:
    """Encode exact decimals as strings; reject all other unsupported objects."""
    if isinstance(value, Decimal):
        return str(value)
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


def _dump_json(value: object) -> str:
    """Serialize metadata without converting decimals to binary floats."""
    return json.dumps(value, default=_json_default)

__all__ = [
    "DECIMAL128_MAX_PRECISION",
    "DECIMAL256_MAX_PRECISION",
    "DEFAULT_FORMAT_MAP",
    "INT8_WIDTH",
    "INT16_WIDTH",
    "INT32_MAX",
    "INT32_MIN",
    "INT32_WIDTH",
    "MAX_MICROS_DIGITS",
    "MAX_MILLIS_DIGITS",
    "WEI_PRECISION",
    "ConversionContext",
    "PyArrowEmitter",
    "PyArrowEmitterError",
    "infer_type_from_enum",
    "merge_format_map",
]
