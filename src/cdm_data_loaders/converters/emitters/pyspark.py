"""Emit PySpark schema objects from immutable typed schema nodes."""

import logging
from decimal import Decimal
from typing import ClassVar, Final

from frozendict import frozendict
from jsonschema import Draft202012Validator
from pydantic import Field, field_validator
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.inference import decimal_places, json_type_from_enum
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.core.metadata import ConversionContext
from cdm_data_loaders.converters.emitters._tabular_base import (
    DECIMAL128_MAX_PRECISION,
    DEFAULT_STRING_FORMATS,
    INT32_WIDTH,
    _TabularEmitterBase,
    fits_int32_bounds,
    merge_format_map_with_defaults,
)

logger = logging.getLogger(__name__)

DEFAULT_FORMAT_MAP: Final[frozendict[str, DataType]] = frozendict(
    {
        "date": DateType(),
        "date-time": TimestampType(),
        **dict.fromkeys(DEFAULT_STRING_FORMATS, StringType()),
    }
)


def merge_format_map(value: object) -> object:
    """Validate format overrides and merge them into the built-in mapping."""
    return merge_format_map_with_defaults(value, DEFAULT_FORMAT_MAP, DataType, "PySpark")


class PySparkEmitterError(ConversionError):
    """A typed schema cannot be represented by the configured Spark policy."""


class PySparkEmitter(_TabularEmitterBase[DataType]):
    """Convert typed schema nodes into pure Python Spark schema objects."""

    _error_cls: ClassVar[type[ConversionError]] = PySparkEmitterError
    _target_name: ClassVar[str] = "PySpark"
    _string_type_label: ClassVar[str] = "StringType"
    _union_approx_reason: ClassVar[str] = "PySpark has no union type."
    _array_label: ClassVar[str] = "PySpark arrays"
    _slot_label: ClassVar[str] = "array's"

    format_map: frozendict[str, DataType] = Field(default_factory=lambda: DEFAULT_FORMAT_MAP)

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: object) -> object:
        """Merge nonempty format overrides into the built-in mapping."""
        return merge_format_map(value) if value else value

    def emit(self, document: SchemaDocument) -> StructType:
        """Emit a table schema, requiring the result to be a struct."""
        validator_cls = document.jsonschema_validator_cls or Draft202012Validator
        result = self.emit_node(document.root, self.build_context(validator_cls))
        if isinstance(result, StructType):
            return result
        msg = (
            "Root JSON Schema did not resolve to a StructType.\nThis can happen if it has no "
            "'properties' but does have 'patternProperties' or 'additionalProperties', which map to "
            "MapType instead. PySpark requires a StructType at the top level of the table schema."
        )
        raise PySparkEmitterError(msg)

    def build_metadata(
        self, node: TypedNode, ctx: ConversionContext, field: SchemaField | None = None
    ) -> dict[str, object]:
        """Copy selected field metadata into independent mutable containers."""
        values, jsonschema, original_union = self._select_metadata_parts(node, ctx, field)
        metadata: dict[str, object] = {}
        if jsonschema:
            metadata["jsonschema"] = jsonschema
        if "description" in values:
            metadata["comment"] = mutable_value(values["description"])
        if original_union is not None:
            metadata["original_union"] = original_union
        return metadata

    def _string_type(self) -> DataType:
        return StringType()

    def _scalar_types(self) -> dict[str, DataType]:
        return {"boolean": BooleanType(), "null": NullType(), "collapsed-union": StringType()}

    def _infer_enum_type(self, values: list[object]) -> DataType:
        return infer_type_from_enum(values)

    def _emit_object(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> DataType:
        """Emit fixed fields, a dynamic map, or an empty struct."""
        if depth >= self.max_nesting:
            logger.warning("Max nesting limit (%d) reached; collapsing object to StringType.", self.max_nesting)
            return StringType()

        additional = node.additional_properties
        schema_additional = additional is not None and additional.type not in {"any", "never"}
        if node.properties:
            if node.pattern_properties or schema_additional:
                logger.warning(
                    "Object schema declares both fixed 'properties' and dynamic 'patternProperties' / "
                    "'additionalProperties'; PySpark's StructType requires fixed field names, so the "
                    "dynamic keys are ignored."
                )
            return StructType(
                [
                    StructField(
                        prop.name,
                        self.emit_node(prop.node, ctx, depth + 1),
                        nullable=not prop.required,
                        metadata=self.build_metadata(prop.node, ctx, prop),
                    )
                    for prop in node.properties
                ]
            )
        value_node = additional if schema_additional else None
        if node.pattern_properties:
            if len(node.pattern_properties) > 1:
                logger.warning(
                    "Multiple 'patternProperties' patterns found; only the first pattern's schema is "
                    "used as the MapType value type, since PySpark's MapType has a single value type."
                )
            value_node = next(iter(node.pattern_properties.values()))
        if value_node is not None:
            return MapType(StringType(), self.emit_node(value_node, ctx, depth + 1), valueContainsNull=True)
        logger.warning(
            "Object schema has no 'properties', 'patternProperties', or schema-valued "
            "'additionalProperties'; converting to an empty StructType()."
        )
        return StructType([])

    def _emit_map(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> MapType:
        """Emit a map with independently typed keys and values."""
        if node.key_type is None or node.value_type is None:
            msg = "Map nodes require both key_type and value_type"
            raise PySparkEmitterError(msg)
        return MapType(
            self.emit_node(node.key_type, ctx, depth + 1),
            self.emit_node(node.value_type, ctx, depth + 1),
            valueContainsNull=True,
        )

    def _emit_array(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> ArrayType:
        """Emit homogeneous arrays, approximating tuple schemas by their first slot."""
        element = self._resolve_array_element(node)
        element_type = StringType() if element is None else self.emit_node(element, ctx, depth + 1)
        return ArrayType(element_type, containsNull=True)

    def _emit_string(self, node: TypedNode) -> DataType:
        """Map recognized formats to Spark types."""
        value = node.constraints.get("format")
        return self.format_map.get(value, StringType()) if isinstance(value, str) else StringType()

    @staticmethod
    def _emit_integer(node: TypedNode) -> DataType:
        """Use int32 only when both bounds fit."""
        return IntegerType() if fits_int32_bounds(node) else LongType()

    @staticmethod
    def _emit_number(node: TypedNode) -> DataType:
        """Derive decimal scale from numeric JSON multipleOf values."""
        multiple = node.constraints.get("multipleOf")
        if isinstance(multiple, (int, float, Decimal)):
            scale = decimal_places(multiple)
            if scale > DECIMAL128_MAX_PRECISION:
                msg = f"Unsupported Spark decimal precision/scale: ({DECIMAL128_MAX_PRECISION}, {scale})"
                raise PySparkEmitterError(msg)
            return DecimalType(DECIMAL128_MAX_PRECISION, scale)
        return DoubleType()

    @staticmethod
    def _emit_logical(node: TypedNode) -> DataType | None:
        """Preserve physical widths and logical source types without a JSON encoding."""
        if node.provenance is not None and node.provenance.source == "json-schema":
            return None
        hints = node.hints
        logical = hints.logical_type
        if logical in {"decimal", "wei"}:
            precision = hints.precision if hints.precision is not None else DECIMAL128_MAX_PRECISION
            scale = hints.scale if hints.scale is not None else 0
            if not 1 <= precision <= DECIMAL128_MAX_PRECISION or scale > precision:
                msg = f"Unsupported Spark decimal precision/scale: ({precision}, {scale})"
                raise PySparkEmitterError(msg)
            return DecimalType(precision, scale)
        if logical in {"timestamp", "timestamp-with-tz", "timestamp-without-tz"}:
            return (
                TimestampNTZType() if hints.timezone is False or logical == "timestamp-without-tz" else TimestampType()
            )
        if node.type == "integer" and hints.bit_width is not None:
            return IntegerType() if hints.bit_width <= INT32_WIDTH else LongType()
        if node.type == "number" and (logical == "float" or hints.bit_width == INT32_WIDTH):
            return FloatType()
        return {"date": DateType(), "binary": BinaryType(), "fixed": BinaryType()}.get(logical)


def infer_type_from_enum(values: list[object]) -> DataType:
    """Map homogeneous JSON enum values to Spark scalar types."""
    return {
        "boolean": BooleanType(),
        "integer": LongType(),
        "number": DoubleType(),
        "string": StringType(),
    }[json_type_from_enum(values)]
