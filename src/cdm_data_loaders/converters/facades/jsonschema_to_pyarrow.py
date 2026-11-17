"""Convert JSON Schema through the shared reader and PyArrow emitter."""

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import Field, PositiveInt, field_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.io import ExtraMetadataKeywords
from cdm_data_loaders.converters.emitters.pyarrow import (
    STRICT_DEFAULT_FORMAT_MAP,
    PyArrowEmitter,
    merge_strict_format_map,
)
from cdm_data_loaders.converters.emitters.pyarrow_codegen.renderer import (
    render_python_schema_module,
    write_python_schema_file,
)
from cdm_data_loaders.converters.exceptions.facades.jsonschema_to_pyarrow import (
    ERROR_BY_CODE,
    JSONSchemaToPyArrowError,
    UnsupportedDialectError,
)
from cdm_data_loaders.converters.facades._base import JsonSchemaConverterBase
from cdm_data_loaders.converters.sources.json_schema.strict_reader import StrictJsonSchemaReader
from cdm_data_loaders.utils.arrow_schema import is_non_json_arrow_type
from cdm_data_loaders.utils.recursion import DEFAULT_MAX_RECURSION_DEPTH, StrictPositiveRecursionDepth
from cdm_data_loaders.validation.jsonschema.dereference import dereference_schema
from cdm_data_loaders.validation.jsonschema.models import ValidatedSchema

if TYPE_CHECKING:
    from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema

STRICT_DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(format_map=STRICT_DEFAULT_FORMAT_MAP)


class JSONSchemaToPyArrow(JsonSchemaConverterBase[pa.Schema]):
    """Read Draft 2020-12 JSON Schema through the shared IR reader and emit PyArrow schemas and empty tables."""

    _run_root_guards: ClassVar[bool] = False
    _error_cls: ClassVar[type[ConversionError]] = JSONSchemaToPyArrowError
    _invalid_schema_error_cls: ClassVar[type[ConversionError]] = UnsupportedDialectError
    _converter_name: ClassVar[str] = "JSONSchemaToPyArrow"
    _root_target_label: ClassVar[str] = "a struct schema"

    max_recursion_depth: StrictPositiveRecursionDepth = DEFAULT_MAX_RECURSION_DEPTH
    format_map: frozendict[str, pa.DataType] = Field(default=STRICT_DEFAULT_FORMAT_MAP)
    treat_unknown_as_string: bool = True
    emit_unions_as_json: bool = False
    extra_metadata_keywords: ExtraMetadataKeywords = Field(default_factory=frozenset)
    max_nesting: PositiveInt = Field(default=100)

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: object) -> object:
        """Delegate nonempty format overrides to the strict emitter defaults.

        :param value: Input format map.
        :type value: object
        :return: Merged format map.
        :rtype: object
        """
        return merge_strict_format_map(value) if value else value

    def _reader(self) -> StrictJsonSchemaReader:
        """Configure strict Draft 2020-12 IR reader with specification error codes.

        :return: Configured ``StrictJsonSchemaReader``.
        :rtype: StrictJsonSchemaReader
        """
        forbidden_formats = frozenset(fmt for fmt, dtype in self.format_map.items() if is_non_json_arrow_type(dtype))
        forbidden_formats = frozenset(
            fmt for fmt, dtype in self.format_map.items() if is_non_json_arrow_type(dtype)
        )
        return StrictJsonSchemaReader(
            self.extension_registry,
            error_type=self._error_cls,
            converter_name=self._converter_name,
            max_recursion_depth=self.max_recursion_depth,
            error_by_code=ERROR_BY_CODE,
            forbidden_recursive_formats=forbidden_formats,
        )

    def _emitter(self) -> PyArrowEmitter:
        """Pass target policy to the emitter.

        :return: Configured ``PyArrowEmitter``.
        :rtype: PyArrowEmitter
        """
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
        return render_python_schema_module(self._emitter(), doc, docstring=docstring)

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
        return write_python_schema_file(self._emitter(), doc, output, docstring=docstring)


def resolve_scalar_type(node: Mapping[str, Any]) -> tuple[pa.DataType, str]:
    """Map a scalar JSON Schema node to its PyArrow type and ``jsonschema_type`` label.

    :param node: scalar schema node (``type``, ``format``, ``contentEncoding``, ``enum`` or ``const``)
    :type node: Mapping[str, Any]
    :return: ``(pa.DataType, jsonschema_type)``
    :rtype: tuple[pa.DataType, str]
    :raises JSONSchemaToPyArrowError: if ``node`` is not a convertible scalar schema
    """
    reader = StrictJsonSchemaReader(
        error_type=JSONSchemaToPyArrowError,
        converter_name="JSONSchemaToPyArrow",
        error_by_code=ERROR_BY_CODE,
    )
    ir_node = reader.read_node(node)
    label = ir_node.provenance.metadata.get("jsonschema_type") if ir_node.provenance else None
    if not isinstance(label, str):
        msg = f"Cannot resolve scalar type for schema node: {node!r}"
        raise JSONSchemaToPyArrowError(msg)
    return STRICT_DEFAULT_EMITTER.emit_node(ir_node), label


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


def _dereference_validated_schema(schema: ValidatedSchema) -> dict[str, Any]:
    """Dereference a validated JSON Schema while preserving its top-level ``$schema`` URI.

    :param schema: Validated JSON Schema wrapper.
    :type schema: ValidatedSchema
    :return: Dereferenced JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    raw_schema = schema.jsonschema
    dereferenced = dereference_schema(raw_schema)
    if "$schema" not in dereferenced and "$schema" in raw_schema:
        dereferenced = {"$schema": raw_schema["$schema"], **dereferenced}
    return dereferenced


def convert_schema_to_pyarrow(schema: ValidatedSchema) -> pa.Schema:
    """Dereference a validated JSON Schema and convert it to a PyArrow schema.

    :param schema: validated JSON Schema
    :type schema: ValidatedSchema
    :return: target PyArrow schema for the entity table
    :rtype: pa.Schema
    :raises DereferencingError: if ``$ref`` or ``allOf`` cannot be resolved
    :raises JSONSchemaToPyArrowError: if the dereferenced schema cannot be converted to a PyArrow schema
    """
    return JSONSchemaToPyArrow().convert(_dereference_validated_schema(schema))


def convert_xsv_schema_to_pyarrow(validated_schema: "ValidatedXsvSchema") -> pa.Schema:
    """Convert a validated XSV JSON Schema to a PyArrow schema.

    In XSV schemas, top-level ``required`` declares the ordered CSV columns,
    unlisted columns default to nullable strings, and any column whose
    schema permits ``null`` is nullable in PyArrow.

    :param validated_schema: validated XSV JSON Schema
    :type validated_schema: ValidatedXsvSchema
    :return: PyArrow schema for the XSV table
    :rtype: pa.Schema
    """
    dereferenced = _dereference_validated_schema(validated_schema)

    raw_properties = dereferenced.get("properties", {})
    properties: dict[str, Any] = {
        col: raw_properties.get(col, {"type": ["string", "null"]}) for col in validated_schema.required_cols
    }
    for col, prop_spec in raw_properties.items():
        if col not in properties:
            properties[col] = prop_spec

    normalized_schema = {
        **dereferenced,
        "type": dereferenced.get("type", "object"),
        "properties": properties,
        "required": list(validated_schema.required_cols),
    }
    return JSONSchemaToPyArrow().convert(normalized_schema)
