"""Shared base class and helpers for tabular schema emitters (PySpark and PyArrow)."""

import logging
from collections.abc import Mapping
from decimal import Decimal
from typing import ClassVar, Final

from frozendict import frozendict
from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, Field

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.inference import is_unconstrained_node
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import TypedNode
from cdm_data_loaders.converters.core.ir_values import Value, mutable_value
from cdm_data_loaders.converters.core.metadata import DEFAULT_METADATA_KEYWORDS, ConversionContext

INT32_MIN: Final = -2_147_483_648
INT32_MAX: Final = 2_147_483_647
INT32_WIDTH: Final = 32
DECIMAL128_MAX_PRECISION: Final = 38

DEFAULT_STRING_FORMATS: Final[tuple[str, ...]] = (
    "time",
    "duration",
    "email",
    "hostname",
    "idn-email",
    "idn-hostname",
    "ipv4",
    "ipv6",
    "iri-reference",
    "iri",
    "uri-reference",
    "uri",
    "uuid",
)


def merge_format_map_with_defaults[DataTypeT](
    value: object,
    default_map: Mapping[str, DataTypeT],
    data_type_cls: type[DataTypeT],
    target_label: str,
) -> object:
    """Validate format overrides and merge them into the built-in mapping."""
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) or not isinstance(item, data_type_cls) for key, item in value.items()):
            msg = f"format_map must map strings to {target_label} DataType instances"
            raise ValueError(msg)
        return frozendict({**default_map, **value})
    return value


def fits_int32_bounds(node: TypedNode) -> bool:
    """Return True when both integer bounds are present and fit inside signed int32."""
    minimum = node.constraints.get("minimum", node.constraints.get("exclusiveMinimum"))
    maximum = node.constraints.get("maximum", node.constraints.get("exclusiveMaximum"))
    return (
        isinstance(minimum, (int, float, Decimal))
        and isinstance(maximum, (int, float, Decimal))
        and minimum >= INT32_MIN
        and maximum <= INT32_MAX
    )


class _TabularEmitterBase[DataTypeT](BaseModel):
    """Shared node-dispatch and metadata-selection logic for tabular emitters."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    _error_cls: ClassVar[type[ConversionError]] = ConversionError
    _target_name: ClassVar[str] = ""
    _string_type_label: ClassVar[str] = "string"
    _union_approx_reason: ClassVar[str] = ""
    _array_label: ClassVar[str] = "arrays"
    _slot_label: ClassVar[str] = "array's"

    treat_unknown_as_string: bool = True
    emit_unions_as_json: bool = False
    extra_metadata_keywords: frozenset[str] = Field(default_factory=frozenset)
    max_nesting: int = Field(default=10, ge=1)

    def _logger(self) -> logging.Logger:
        """Return the concrete emitter module logger."""
        return logging.getLogger(type(self).__module__)

    def build_context(self, validator_cls: type) -> ConversionContext:
        """Build draft-specific metadata policy and report invalid requested keywords."""
        context = ConversionContext(validator_cls, self.extra_metadata_keywords)
        if context.invalid_extra_metadata_keywords:
            self._logger().warning(
                "Ignoring invalid extra_metadata_keywords %s: these are neither recognised JSON Schema "
                "keywords for the detected draft (%s) nor vendor extensions prefixed with 'x-'.",
                sorted(context.invalid_extra_metadata_keywords),
                validator_cls.__name__,
            )
        return context

    def emit_node(self, node: TypedNode, ctx: ConversionContext | None = None, depth: int = 0) -> DataTypeT:
        """Emit a scalar or container node without a table-root restriction."""
        if ctx is None:
            ctx = self.build_context(Draft202012Validator)
        logical = self._emit_logical(node)
        if logical is not None:
            return logical
        kind = self._type_name(node)
        if kind in {"object", "array", "map"}:
            return {"object": self._emit_object, "array": self._emit_array, "map": self._emit_map}[kind](
                node, ctx, depth
            )
        scalars = self._scalar_types()
        if kind in scalars:
            return scalars[kind]
        if kind == "enum":
            values = node.constraints["enum"]
            if isinstance(values, tuple):
                return self._infer_enum_type(list(values))
        converters = {"string": self._emit_string, "integer": self._emit_integer, "number": self._emit_number}
        return converters[kind](node) if kind in converters else self._emit_unknown(node, ctx)

    def _select_metadata_parts(
        self, node: TypedNode, ctx: ConversionContext, field: SchemaField | None = None
    ) -> tuple[Mapping[str, Value], dict[str, object], list[str] | None]:
        """Extract raw metadata values, selected JSON Schema keywords, and optional union types."""
        values = node.aggregate_metadata(field)
        selected = DEFAULT_METADATA_KEYWORDS | ctx.allowed_extra_metadata_keywords
        jsonschema = {key: mutable_value(values[key]) for key in selected if key in values}
        original_union: list[str] | None = None
        if self.emit_unions_as_json:
            declaration = node.declared_type
            if isinstance(declaration, tuple):
                non_null = [kind for kind in declaration if kind != "null"]
                if len(non_null) > 1:
                    original_union = list(non_null)
        return values, jsonschema, original_union

    def _type_name(self, node: TypedNode) -> str:
        """Select declared, enum or inferred types without flattening the IR."""
        declaration = node.declared_type
        if isinstance(declaration, tuple):
            non_null = [kind for kind in declaration if kind != "null"]
            if not non_null:
                return "null"
            if len(non_null) == 1:
                return non_null[0]
            if not self.treat_unknown_as_string:
                msg = f"Unsupported multi-type union: {list(declaration)!r}"
                raise self._error_cls(msg)
            if self.emit_unions_as_json:
                self._logger().info("Emitting multi-type union %r as JSON string.", list(declaration))
            else:
                self._logger().warning(
                    "Collapsing multi-type union %r to %s.", list(declaration), self._string_type_label
                )
            return "collapsed-union"
        if declaration is not None:
            return declaration
        if "enum" in node.constraints:
            return "enum"
        return node.inferred_type or node.type

    def _emit_unknown(self, node: TypedNode, ctx: ConversionContext) -> DataTypeT:
        """Approximate combiners or apply strict unsupported-type handling."""
        if node.type in {"any", "never"} and (node.provenance is None or node.provenance.source == "json-schema"):
            message = (
                f"Boolean JSON Schema `{node.type == 'any'}` has no {self._target_name} equivalent "
                "(unconstrained 'any' type or unsatisfiable 'never' type)."
            )
        else:
            for keyword, branches in (("oneOf", node.one_of), ("anyOf", node.any_of)):
                if branches:
                    self._logger().warning(
                        "Approximating '%s' by using only its first branch for type inference; %s",
                        keyword,
                        self._union_approx_reason,
                    )
                    return self.emit_node(branches[0], ctx)
            for keyword in ("not", "if", "then", "else"):
                if keyword in node.schema_keywords:
                    self._logger().warning(
                        "Ignoring unsupported conditional keyword '%s'; it has no effect on the resulting %s type.",
                        keyword,
                        self._target_name,
                    )
            message = f"Unsupported/unknown schema type: {node.declared_type or node.type!r}"
        if not self.treat_unknown_as_string:
            raise self._error_cls(message)
        self._logger().warning("%s Falling back to %s.", message, self._string_type_label)
        return self._string_type()

    def _resolve_array_element(self, node: TypedNode) -> TypedNode | None:
        """Resolve homogeneous array element node, warning on tuple schemas."""
        element = node.items
        if node.prefix_items is not None:
            self._logger().warning(
                "Array schema uses 'prefixItems' (tuple validation); %s are homogeneous, so "
                "only the first tuple slot's type is used as the %s element type.",
                self._array_label,
                self._slot_label,
            )
            element = node.prefix_items[0] if node.prefix_items else None
        elif isinstance(element, tuple):
            self._logger().warning(
                "Array schema uses tuple-style 'items' (list of schemas); %s are "
                "homogeneous, so only the first item's type is used as the %s element type.",
                self._array_label,
                self._slot_label,
            )
            element = element[0] if element else None
        if element is None or is_unconstrained_node(element):
            return None
        return element

    def _string_type(self) -> DataTypeT:
        raise NotImplementedError

    def _scalar_types(self) -> dict[str, DataTypeT]:
        raise NotImplementedError

    def _infer_enum_type(self, values: list[object]) -> DataTypeT:
        raise NotImplementedError

    def _emit_object(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> DataTypeT:
        raise NotImplementedError

    def _emit_array(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> DataTypeT:
        raise NotImplementedError

    def _emit_map(self, node: TypedNode, ctx: ConversionContext, depth: int = 0) -> DataTypeT:
        raise NotImplementedError

    def _emit_string(self, node: TypedNode) -> DataTypeT:
        raise NotImplementedError

    @staticmethod
    def _emit_integer(node: TypedNode) -> DataTypeT:
        raise NotImplementedError

    @staticmethod
    def _emit_number(node: TypedNode) -> DataTypeT:
        raise NotImplementedError

    @staticmethod
    def _emit_logical(node: TypedNode) -> DataTypeT | None:
        raise NotImplementedError
