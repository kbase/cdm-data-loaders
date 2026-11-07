"""Emit PySpark schema objects from immutable typed schema nodes."""

from collections.abc import Mapping
from typing import ClassVar, Final

from frozendict import frozendict
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
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.core.metadata import ConversionContext
from cdm_data_loaders.converters.emitters._tabular_base import (
    DEFAULT_STRING_FORMATS,
    INT32_WIDTH,
    _TabularEmitterBase,
    infer_enum_type,
    merge_format_map_with_defaults,
    resolve_logical_decimal,
)

DEFAULT_FORMAT_MAP: Final[frozendict[str, DataType]] = frozendict(
    {
        "date": DateType(),
        "date-time": TimestampType(),
        **dict.fromkeys(DEFAULT_STRING_FORMATS, StringType()),
    }
)
SCALAR_TYPE_MAP: Final[frozendict[str, DataType]] = frozendict(
    {
        "boolean": BooleanType(),
        "null": NullType(),
        "collapsed-union": StringType(),
    }
)
ENUM_TYPE_MAP: Final[frozendict[str, DataType]] = frozendict(
    {
        "boolean": BooleanType(),
        "integer": LongType(),
        "number": DoubleType(),
        "string": StringType(),
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
    _decimal_target_label: ClassVar[str] = "Spark"
    _string_type_label: ClassVar[str] = "StringType"
    _struct_type_label: ClassVar[str] = "PySpark's StructType"
    _empty_struct_label: ClassVar[str] = "empty StructType()"
    _map_type_label: ClassVar[str] = "PySpark's MapType"
    _map_value_label: ClassVar[str] = "MapType value type"
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
        result = self._emit_root_node(document)
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

    def _int32_type(self) -> DataType:
        return IntegerType()

    def _int64_type(self) -> DataType:
        return LongType()

    def _float64_type(self) -> DataType:
        return DoubleType()

    def _decimal128_type(self, precision: int, scale: int) -> DataType:
        return DecimalType(precision, scale)

    def _scalar_types(self) -> Mapping[str, DataType]:
        return SCALAR_TYPE_MAP

    def _infer_enum_type(self, values: list[object]) -> DataType:
        return infer_type_from_enum(values)

    def _struct_type(self, properties: tuple[SchemaField, ...], ctx: ConversionContext, depth: int) -> StructType:
        return StructType(
            [
                StructField(
                    prop.name,
                    self.emit_node(prop.node, ctx, depth + 1),
                    nullable=not prop.required,
                    metadata=self.build_metadata(prop.node, ctx, prop),
                )
                for prop in properties
            ]
        )

    def _map_type(
        self,
        key_type: DataType,
        value_type: DataType,
        *,
        value_nullable: bool,  # noqa: ARG002
    ) -> MapType:
        return MapType(key_type, value_type, valueContainsNull=True)

    def _array_type(
        self,
        element_type: DataType,
        *,
        element_nullable: bool,  # noqa: ARG002
    ) -> ArrayType:
        return ArrayType(element_type, containsNull=True)

    @staticmethod
    def _emit_logical(node: TypedNode) -> DataType | None:
        """Preserve physical widths and logical source types without a JSON encoding."""
        if node.provenance is not None and node.provenance.source == "json-schema":
            return None
        hints = node.hints
        logical = hints.logical_type
        if logical in {"decimal", "wei"}:
            precision, scale = resolve_logical_decimal(
                hints,
                target_label="Spark",
                error_cls=PySparkEmitterError,
            )
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
    return infer_enum_type(values, ENUM_TYPE_MAP)
