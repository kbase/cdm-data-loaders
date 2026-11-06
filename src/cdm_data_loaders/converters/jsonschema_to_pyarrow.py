"""Convert JSON Schema through the shared reader and PyArrow emitter."""

import base64
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import Field, PositiveInt, field_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.io import JsonSchemaConverterBase, coerce_extra_metadata_keywords
from cdm_data_loaders.converters.core.ir import SchemaDocument
from cdm_data_loaders.converters.emitters.pyarrow import DEFAULT_FORMAT_MAP, PyArrowEmitter, merge_format_map
from cdm_data_loaders.converters.readers.json_schema import (
    DEFAULT_MAX_RECURSION_DEPTH,
    JsonSchemaReader,
)


class JSONSchemaToPyArrowError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted.

    Subclasses raised by :func:`generate_pyarrow_schema` carry a stable specification ``code`` and a short
    ``title``; their docstrings state why the construct is rejected. The converter specification document is
    generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class InvalidJSONSchemaError(JSONSchemaToPyArrowError):
    """Raised when the input document is not a valid JSON Schema."""


class UnsupportedDialectError(InvalidJSONSchemaError):
    """Only JSON Schema Draft 2020-12 and the KBase Draft 2020-12 XSV metaschema are supported."""

    code = "E1"
    title = "Missing or unsupported `$schema` dialect"


class NonStructRootError(JSONSchemaToPyArrowError):
    """A `pa.Schema` needs top-level columns, so the root must resolve to a fixed-property object."""

    code = "E2"
    title = "Non-struct top-level schema"


class EmptyObjectError(JSONSchemaToPyArrowError):
    """Parquet cannot store a `pa.struct([])` with no child fields."""

    code = "E3"
    title = "Empty object or tuple (zero fields)"


class UnconstrainedSchemaError(JSONSchemaToPyArrowError):
    """An untyped JSON value cannot be assigned a concrete Arrow `DataType`."""

    code = "E4"
    title = "Unconstrained or boolean schema"


class OpenTupleError(JSONSchemaToPyArrowError):
    """A field cannot be both a positional `pa.struct` and an unbounded `pa.list_`."""

    code = "E5"
    title = "Open tuple (`prefixItems` with schema-valued `items`)"


class MixedPropertiesAndMapError(JSONSchemaToPyArrowError):
    """An object cannot be both a fixed-field `pa.struct` and a dynamic `pa.map_`."""

    code = "E6"
    title = "Fixed `properties` mixed with a dynamic map"


class NegationError(JSONSchemaToPyArrowError):
    """A complement type does not define a positive storage `DataType`."""

    code = "E7"
    title = "Negation (`not`)"


class IncompatibleBranchShapesError(JSONSchemaToPyArrowError):
    """Branches with different structural shapes (object vs array, array vs scalar, object vs map) cannot share one column."""

    code = "E8"
    title = "Incompatible structural union branches"


class UnsupportedRefError(JSONSchemaToPyArrowError):
    """Every `$ref` must resolve to a local `#/$defs/...` definition; `$dynamicRef` and `$recursiveRef` are not supported."""

    code = "E9"
    title = "Unresolvable or unsupported reference"


class RecursiveNonJsonTypeError(JSONSchemaToPyArrowError):
    """Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`."""

    code = "E10"
    title = "Non-JSON-serializable type in a recursive `$ref` cycle"


class NullOnlySchemaError(JSONSchemaToPyArrowError):
    """Parquet has no physical storage type for a `pa.null()` column."""

    code = "E11"
    title = "Null-only schema"


class ValueFieldCollisionError(JSONSchemaToPyArrowError):
    """The synthesized `_value` field of a scalar + object union would collide with a declared `_value` property."""

    code = "E12"
    title = "`_value` collision in a scalar + object union"


JSONSCHEMA_TO_PYARROW_ERRORS: Final[tuple[type[JSONSchemaToPyArrowError], ...]] = (
    UnsupportedDialectError,
    NonStructRootError,
    EmptyObjectError,
    UnconstrainedSchemaError,
    OpenTupleError,
    MixedPropertiesAndMapError,
    NegationError,
    IncompatibleBranchShapesError,
    UnsupportedRefError,
    RecursiveNonJsonTypeError,
    NullOnlySchemaError,
    ValueFieldCollisionError,
)

_ERROR_BY_CODE: Final[frozendict[str, type[JSONSchemaToPyArrowError]]] = frozendict(
    {cls.code: cls for cls in JSONSCHEMA_TO_PYARROW_ERRORS}
)


class JSONSchemaToPyArrow(JsonSchemaConverterBase[pa.Schema]):
    """Read Draft 2020-12 JSON Schema through the shared IR reader and emit PyArrow schemas and empty tables."""

    _error_cls: ClassVar[type[ConversionError]] = JSONSchemaToPyArrowError
    _invalid_schema_error_cls: ClassVar[type[ConversionError]] = UnsupportedDialectError
    _converter_name: ClassVar[str] = "JSONSchemaToPyArrow"
    _root_target_label: ClassVar[str] = "a struct schema"

    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    format_map: frozendict[str, pa.DataType] = Field(default_factory=lambda: DEFAULT_FORMAT_MAP)
    treat_unknown_as_string: bool = True
    emit_unions_as_json: bool = False
    extra_metadata_keywords: frozenset[str] = Field(default_factory=frozenset)
    max_nesting: PositiveInt = Field(default=100)

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: object) -> object:
        """Delegate nonempty format overrides to the emitter."""
        return merge_format_map(value) if value else value

    @field_validator("extra_metadata_keywords", mode="before")
    @classmethod
    def _coerce_extra_metadata_keywords(cls, value: object) -> object:
        """Normalize supported metadata keyword collections."""
        return coerce_extra_metadata_keywords(value)

    def _reader(self) -> JsonSchemaReader:
        """Configure strict Draft 2020-12 IR reader with specification error codes."""
        return JsonSchemaReader(
            self.extension_registry,
            error_type=self._error_cls,
            converter_name=self._converter_name,
            dereference_function=self._dereference_function,
            strict=True,
            max_recursion_depth=self.max_recursion_depth,
            error_by_code=_ERROR_BY_CODE,
        )

    def _read(self, schema: dict[str, Any]) -> SchemaDocument:
        """Read schema into a ``SchemaDocument``, preserving specification error identity."""
        try:
            return self._reader().read(schema)
        except ConversionError as error:
            if isinstance(error, self._error_cls):
                raise
            raise self._error_cls(str(error)) from error

    def _emitter(self) -> PyArrowEmitter:
        """Pass target policy to the emitter."""
        return PyArrowEmitter(
            format_map=self.format_map,
            treat_unknown_as_string=self.treat_unknown_as_string,
            emit_unions_as_json=self.emit_unions_as_json,
            extra_metadata_keywords=self.extra_metadata_keywords,
            max_nesting=self.max_nesting,
        )

    def convert_to_table(self, schema: dict[str, Any]) -> pa.Table:
        """Convert a Draft 2020-12 JSON Schema object document to an empty Arrow table.

        :param schema: the JSON Schema, which must declare ``$schema`` and have an object root
        :type schema: dict[str, Any]
        :return: a zero-row table with the converted schema
        :rtype: pa.Table
        :raises InvalidJSONSchemaError: if ``$schema`` is missing or unsupported
        :raises JSONSchemaToPyArrowError: if the root is not an object or a construct cannot be converted
        """
        return self.convert(schema).empty_table()

    def convert_to_python(self, schema: dict[str, Any], *, docstring: str | None = None) -> str:
        """Convert a Draft 2020-12 JSON Schema object document to a static Python schema module.

        :param schema: the JSON Schema, which must declare ``$schema`` and have an object root
        :type schema: dict[str, Any]
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        :raises InvalidJSONSchemaError: if ``$schema`` is missing or unsupported
        :raises JSONSchemaToPyArrowError: if the root is not an object or a construct cannot be converted
        """
        doc = self._read(schema)
        return self._emitter().emit_python(doc, docstring=docstring)

    def convert_to_python_file(
        self,
        schema: dict[str, Any],
        output: str | Path,
        *,
        docstring: str | None = None,
    ) -> Path:
        """Convert a Draft 2020-12 JSON Schema object document and write a static Python schema module to ``output``.

        :param schema: the JSON Schema, which must declare ``$schema`` and have an object root
        :type schema: dict[str, Any]
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        :raises InvalidJSONSchemaError: if ``$schema`` is missing or unsupported
        :raises JSONSchemaToPyArrowError: if the root is not an object or a construct cannot be converted
        """
        doc = self._read(schema)
        return self._emitter().emit_python_file(doc, output, docstring=docstring)


def resolve_scalar_type(node: Mapping[str, Any]) -> tuple[pa.DataType, str]:
    """Map a scalar JSON Schema node to its PyArrow type and ``jsonschema_type`` label.

    :param node: scalar schema node (``type``, ``format``, ``contentEncoding``, ``enum`` or ``const``)
    :type node: Mapping[str, Any]
    :return: ``(pa.DataType, jsonschema_type)``
    :rtype: tuple[pa.DataType, str]
    :raises JSONSchemaToPyArrowError: if ``node`` is not a convertible scalar schema
    """
    reader = JsonSchemaReader(
        error_type=JSONSchemaToPyArrowError,
        converter_name="JSONSchemaToPyArrow",
        strict=True,
        error_by_code=_ERROR_BY_CODE,
    )
    ir_node = reader.read_node(node)
    label = ir_node.provenance.metadata.get("jsonschema_type") if ir_node.provenance else None
    if not isinstance(label, str):
        msg = f"Cannot resolve scalar type for schema node: {node!r}"
        raise JSONSchemaToPyArrowError(msg)
    return PyArrowEmitter().emit_node(ir_node), label


def generate_pyarrow_schema(
    schema: dict[str, Any],
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> pa.Schema:
    """Generate a deterministic PyArrow Schema from a Draft 2020-12 JSON Schema.

    :param schema: JSON Schema Draft 2020-12 dictionary
    :type schema: dict[str, Any]
    :param max_recursion_depth: maximum expansion depth for recursive ``$defs`` references
    :type max_recursion_depth: int
    :return: PyArrow schema with top-level fields corresponding to root object properties
    :rtype: pa.Schema
    :raises JSONSchemaToPyArrowError: if ``schema`` uses an unsupported dialect or construct
    """
    return JSONSchemaToPyArrow(max_recursion_depth=max_recursion_depth).convert(schema)


def _parse_iso_timestamp(value: str, data_type: pa.TimestampType) -> datetime:
    """Parse an ISO-8601 timestamp string, attaching UTC when target type is tz-aware."""
    parsed_dt = datetime.fromisoformat(value)
    if parsed_dt.tzinfo is None and data_type.tz is not None:
        return parsed_dt.replace(tzinfo=UTC)
    return parsed_dt


def _decode_binary_scalar(value: str, metadata: dict[bytes, bytes] | None) -> bytes:
    """Decode a Base64 or Base16 (hex) string into raw bytes based on field metadata."""
    if metadata and b"jsonschema_type" in metadata and b"base16" in metadata[b"jsonschema_type"]:
        return bytes.fromhex(value)
    return base64.b64decode(value)


def _coerce_string_input_scalar(
    value: str,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None,
) -> Any:  # noqa: ANN401
    """Coerce a string value when the target PyArrow type is binary, temporal, or decimal."""
    if pa.types.is_binary(data_type):
        return _decode_binary_scalar(value, metadata)
    if pa.types.is_date(data_type):
        return date.fromisoformat(value)
    if pa.types.is_timestamp(data_type):
        return _parse_iso_timestamp(value, data_type)
    if pa.types.is_time(data_type):
        return time.fromisoformat(value)
    if pa.types.is_decimal(data_type):
        return Decimal(value)
    return value


def _coerce_scalar_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
) -> Any:  # noqa: ANN401
    """Coerce a JSON-parsed value to match a PyArrow scalar type."""
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, str):
        return _coerce_string_input_scalar(value, data_type, metadata)
    if pa.types.is_decimal(data_type) and isinstance(value, int | float) and not isinstance(value, bool):
        return Decimal(str(value))
    if pa.types.is_floating(data_type) and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if pa.types.is_integer(data_type) and isinstance(value, float) and value.is_integer():
        return int(value)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return json.dumps(value) if isinstance(value, Mapping | list | bool) else str(value)
    return value


def _coerce_struct_for_pyarrow(value: Any, data_type: pa.StructType) -> Any:  # noqa: ANN401
    """Coerce a mapping, closed tuple list, or scalar union value to match a PyArrow StructType."""
    if isinstance(value, Mapping):
        return {
            field.name: _coerce_value_for_pyarrow(value[field.name], field.type, field.metadata)
            for field in data_type
            if field.name in value
        }
    if isinstance(value, list) and len(data_type) > 0 and data_type[0].name == "item_0":
        return {
            field.name: _coerce_value_for_pyarrow(value[idx], field.type, field.metadata)
            for idx, field in enumerate(data_type)
            if idx < len(value) and value[idx] is not None
        }
    value_field = next((field for field in data_type if field.name == "_value"), None)
    if value_field is not None:
        return {"_value": _coerce_value_for_pyarrow(value, value_field.type, value_field.metadata)}
    return value


def _coerce_value_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
) -> Any:  # noqa: ANN401
    """Recursively coerce a JSON-parsed value to match the target PyArrow DataType."""
    if value is None:
        return None
    if pa.types.is_struct(data_type):
        return _coerce_struct_for_pyarrow(value, data_type)
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)) and isinstance(value, list):
        item_field = data_type.value_field
        return [_coerce_value_for_pyarrow(item, item_field.type, item_field.metadata) for item in value]
    if pa.types.is_map(data_type) and isinstance(value, Mapping):
        key_field = data_type.key_field
        item_field = data_type.item_field
        return {
            _coerce_value_for_pyarrow(k, key_field.type, key_field.metadata): _coerce_value_for_pyarrow(
                v, item_field.type, item_field.metadata
            )
            for k, v in value.items()
        }
    return _coerce_scalar_for_pyarrow(value, data_type, metadata)


def coerce_record_for_pyarrow(
    record: dict[str, Any],
    schema: pa.Schema,
) -> dict[str, Any]:
    """Coerce a JSON-validated record dictionary to match a PyArrow schema.

    Only fields declared in ``schema`` are retained and coerced recursively so
    ``pyarrow.Table.from_pylist`` can construct a table matching ``schema``.

    :param record: JSON-parsed and schema-validated record dictionary
    :type record: dict[str, Any]
    :param schema: target PyArrow schema for the record's table
    :type schema: pa.Schema
    :return: coerced record dictionary ready for ``DictBuffer`` or ``pa.Table.from_pylist``
    :rtype: dict[str, Any]
    """
    return {
        field.name: _coerce_value_for_pyarrow(record[field.name], field.type, field.metadata)
        for field in schema
        if field.name in record
    }


convert_json_record = coerce_record_for_pyarrow
