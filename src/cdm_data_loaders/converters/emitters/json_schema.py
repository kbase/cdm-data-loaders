"""Emit JSON Schema from typed schema facts and scoped metadata."""

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.inference import decimal_pattern
from cdm_data_loaders.converters.core.ir import Field, SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"
DLT_TYPES: Final[frozendict[str, dict[str, Any]]] = frozendict(
    {
        "text": {"type": "string"},
        "bigint": {"type": "integer"},
        "double": {"type": "number"},
        "decimal": {"type": "string", "pattern": r"^-?\d+(\.\d+)?$", "x-dlt": {"data_type": "decimal"}},
        "bool": {"type": "boolean"},
        "timestamp": {"type": "string", "format": "date-time", "x-dlt": {"data_type": "timestamp"}},
        "date": {"type": "string", "format": "date"},
        "time": {"type": "string", "format": "time"},
        "binary": {"type": "string", "contentEncoding": "base64"},
        "json": {},
        "wei": {"type": "integer", "x-dlt": {"data_type": "wei"}},
    }
)
DLT_KNOWN: Final[frozenset[str]] = frozenset({"name", "data_type", "nullable", "description", "precision", "scale"})
ICEBERG_LOGICAL_TYPES: Final[frozenset[str]] = frozenset(
    {
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "string",
        "date",
        "time",
        "uuid",
        "timestamp-without-tz",
        "timestamp-with-tz",
        "decimal",
        "fixed",
    }
)
ICEBERG_ELEMENT_KEYS: Final[frozenset[str]] = frozenset(
    {"element_id", "element_required", "key_id", "value_id", "value_required"}
)
PYARROW_SIGNED_INT_TYPES: Final[frozenset[str]] = frozenset({"int8", "int16", "int32", "long"})
PYARROW_UNSIGNED_INT_TYPES: Final[frozenset[str]] = frozenset({"uint8", "uint16", "uint32", "uint64"})
PYARROW_LOGICAL_TYPES: Final[frozenset[str]] = frozenset(
    {
        "boolean",
        "int8",
        "int16",
        "int32",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float",
        "double",
        "string",
        "binary",
        "fixed",
        "date",
        "time",
        "timestamp-without-tz",
        "timestamp-with-tz",
        "decimal",
    }
)


def _nullable(schema: dict[str, Any], *, nullable: bool) -> dict[str, Any]:
    """Wrap a value schema without moving its metadata.

    :param schema: Base JSON Schema object.
    :type schema: dict[str, Any]
    :param nullable: Whether to wrap ``schema`` in a null union.
    :type nullable: bool
    :return: Resulting JSON Schema object.
    :rtype: dict[str, Any]
    """
    return {"anyOf": [schema, {"type": "null"}]} if nullable else schema


def _schema_object(*, schema: dict[str, Any] | bool) -> dict[str, Any]:
    """Represent a boolean schema as an object when metadata must be attached.

    :param schema: JSON Schema dict or boolean schema.
    :type schema: dict[str, Any] | bool
    :return: Equivalent JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    if isinstance(schema, bool):
        return {} if schema else {"not": {}}
    return schema


def _closed_object_schema(properties: dict[str, Any], node: TypedNode) -> dict[str, Any]:
    """Build a closed JSON Schema object from rendered properties and node required facts.

    :param properties: Rendered property subschemas.
    :type properties: dict[str, Any]
    :param node: Source object IR node.
    :type node: TypedNode
    :return: Closed JSON Schema object definition.
    :rtype: dict[str, Any]
    """
    result: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    required = node.required_names
    if required is None:
        required = tuple(field.name for field in node.properties if field.required)
    if required:
        result["required"] = list(required)
    return result


def _kv_array_schema(
    *,
    key_schema: dict[str, Any] | bool,
    value_schema: dict[str, Any] | bool,
    description: str | None = None,
) -> dict[str, Any]:
    """Render a map node as an array of closed key/value object items.

    :param key_schema: Rendered key JSON Schema.
    :type key_schema: dict[str, Any] | bool
    :param value_schema: Rendered value JSON Schema.
    :type value_schema: dict[str, Any] | bool
    :param description: Optional description string.
    :type description: str | None
    :return: Array-of-entries JSON Schema object.
    :rtype: dict[str, Any]
    """
    result: dict[str, Any] = {"type": "array"}
    if description is not None:
        result["description"] = description
    result["items"] = {
        "type": "object",
        "properties": {"key": key_schema, "value": value_schema},
        "required": ["key", "value"],
        "additionalProperties": False,
    }
    return result


@dataclass(frozen=True, slots=True)
class JsonSchemaEmitter:
    """Render typed trees using the source's JSON Schema compatibility policy."""

    preserve_unknown_hints: bool = True

    def __post_init__(self) -> None:
        """Reject nonboolean hint policies."""
        if type(self.preserve_unknown_hints) is not bool:
            msg = "preserve_unknown_hints must be a boolean"
            raise TypeError(msg)

    def emit(self, document: SchemaDocument) -> dict[str, Any]:
        """Return an independently owned JSON Schema document.

        :param document: IR schema document to emit.
        :type document: SchemaDocument
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        provenance = document.provenance or document.root.provenance
        source = provenance.source if provenance is not None else None
        if source == "dlt":
            result = self._dlt_node(document.root)
        elif source == "iceberg":
            result = self._iceberg_node(document.root)
        elif source == "pyarrow":
            return self._pyarrow_document(document)
        else:
            return self._json_document(document)
        annotations = mutable_value(document.annotations)
        if source == "iceberg" and not annotations.get("description"):
            annotations.pop("description", None)
        result.update(annotations)
        result.update(mutable_value(document.extensions))
        result["$schema"] = document.dialect or JSON_SCHEMA_DIALECT
        if document.identifier is not None:
            result["$id"] = document.identifier
        elif source == "dlt":
            result["$id"] = f"urn:dlt:{document.name}"
        if document.name is not None:
            result["title"] = document.name
        return result

    def emit_into(self, document: SchemaDocument, schema: dict[str, Any]) -> dict[str, Any]:
        """Add a document's schema to a namespace document's ``$defs`` without overwriting existing entries.

        :param document: Named, table-rooted document to convert.
        :type document: SchemaDocument
        :param schema: Destination namespace document, updated only after successful conversion.
        :type schema: dict[str, Any]
        :return: The destination namespace document.
        :rtype: dict[str, Any]
        :raises ConversionError: If ``document.name`` is ``None``.
        """
        if document.name is None:
            msg = "A namespaced JSON Schema entry requires a named document"
            raise ConversionError(msg)
        table_schema = self.emit(document)
        table_schema.pop("$schema", None)
        schema.setdefault("$defs", {})[document.name] = table_schema
        return schema

    def emit_node(self, node: TypedNode) -> dict[str, Any] | bool:
        """Emit a standalone node using its source's rendering policy.

        :param node: IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary or boolean schema.
        :rtype: dict[str, Any] | bool
        """
        source = node.provenance.source if node.provenance is not None else None
        if source == "iceberg":
            return self._iceberg_node(node)
        if source == "pyarrow":
            return self._pyarrow_node(node)
        if source == "dlt":
            return self._dlt_node(node)
        return self._json_node(node)

    def emit_field(self, field: Field) -> dict[str, Any] | bool:
        """Emit a field, retaining source-specific metadata placement.

        :param field: IR field to emit.
        :type field: Field
        :return: Rendered JSON Schema dictionary or boolean schema.
        :rtype: dict[str, Any] | bool
        """
        source = field.node.provenance.source if field.node.provenance is not None else None
        if source == "iceberg":
            return self._iceberg_field(field)
        if source == "pyarrow":
            return self._pyarrow_field(field)
        if source == "dlt":
            return self._dlt_node(field.node)
        return self._json_field(field)

    def _json_document(self, document: SchemaDocument) -> dict[str, Any]:
        """Emit structural JSON facts without changing the source dialect.

        :param document: IR schema document to emit.
        :type document: SchemaDocument
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        result = _schema_object(schema=self._json_node(document.root))
        result.update(mutable_value(document.annotations))
        result.update(mutable_value(document.extensions))
        if document.dialect is not None:
            result["$schema"] = document.dialect
        if document.identifier is not None:
            result["$id"] = document.identifier
        if document.name is not None:
            result.setdefault("title", document.name)
        return result

    def _json_node(self, node: TypedNode) -> dict[str, Any] | bool:
        """Preserve declarations separately from inferred types and literal values.

        :param node: IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary or boolean schema.
        :rtype: dict[str, Any] | bool
        :raises ConversionError: If ``node.type`` is ``unknown`` without a declaration.
        """
        from_json = node.provenance is not None and node.provenance.source == "json-schema"
        result = mutable_value(node.constraints)
        result.update(mutable_value(node.annotations))
        result.update(mutable_value(node.extensions))
        if node.declared_type is not None:
            result["type"] = list(node.declared_type) if isinstance(node.declared_type, tuple) else node.declared_type
        elif not from_json and node.type not in {"any", "never", "unknown", "map"}:
            result["type"] = node.type
        elif not from_json and node.type == "unknown":
            msg = "Cannot emit an unknown type without a JSON Schema declaration"
            raise ConversionError(msg)
        self._json_children(node, result)
        if node.type == "never":
            result["not"] = {}
        if from_json:
            if not node.source_keywords and (
                (node.type == "any" and not result) or (node.type == "never" and result == {"not": {}})
            ):
                return node.type == "any"
            result = {**{key: result[key] for key in node.source_keywords if key in result}, **result}
        else:
            result = _nullable(result, nullable=node.nullable is True and node.type not in {"any", "never", "null"})
        return result

    def _json_children(self, node: TypedNode, result: dict[str, Any]) -> None:
        """Render schema positions without interpreting literal keyword payloads.

        :param node: IR node whose child schemas are being rendered.
        :type node: TypedNode
        :param result: Target JSON Schema dictionary to populate in place.
        :type result: dict[str, Any]
        """
        if node.properties or "properties" in node.source_keywords:
            result["properties"] = {field.name: self._json_field(field) for field in node.properties}
        if node.required_names is not None:
            result["required"] = list(node.required_names)
        elif any(field.required for field in node.properties):
            result["required"] = [field.name for field in node.properties if field.required]
        if node.pattern_properties or "patternProperties" in node.source_keywords:
            result["patternProperties"] = {
                key: self._json_node(child) for key, child in node.pattern_properties.items()
            }
        if node.items is not None:
            result["items"] = (
                [self._json_node(child) for child in node.items]
                if isinstance(node.items, tuple)
                else self._json_node(node.items)
            )
        if node.additional_properties is not None:
            result["additionalProperties"] = self._json_node(node.additional_properties)
        for keyword, children in (("prefixItems", node.prefix_items), ("anyOf", node.any_of), ("oneOf", node.one_of)):
            if children is not None:
                result[keyword] = [self._json_node(child) for child in children]
        result.update({key: self._json_node(child) for key, child in node.schema_keywords.items()})
        for keyword, children in node.schema_maps.items():
            result.setdefault(keyword, {}).update({key: self._json_node(child) for key, child in children.items()})
        if node.type == "map":
            result.update(self._json_map(node))

    def _json_field(self, field: Field) -> dict[str, Any] | bool:
        """Attach field metadata without changing an unannotated boolean schema.

        :param field: IR field to emit.
        :type field: Field
        :return: Rendered JSON Schema dictionary or boolean schema.
        :rtype: dict[str, Any] | bool
        """
        schema = self._json_node(field.node)
        if not field.annotations and not field.extensions:
            return schema
        result = _schema_object(schema=schema)
        result.update(mutable_value(field.annotations))
        result.update(mutable_value(field.extensions))
        return result

    def _json_map(self, node: TypedNode) -> dict[str, Any]:
        """Represent arbitrary typed keys as key/value pairs.

        :param node: Map IR node.
        :type node: TypedNode
        :return: Array-of-entries JSON Schema dictionary.
        :rtype: dict[str, Any]
        :raises ConversionError: If ``node.key_type`` or ``node.value_type`` is ``None``.
        """
        if node.key_type is None or node.value_type is None:
            msg = "Map nodes require key and value types"
            raise ConversionError(msg)
        return _kv_array_schema(
            key_schema=self._json_node(node.key_type),
            value_schema=self._json_node(node.value_type),
        )

    def _dlt_node(self, node: TypedNode) -> dict[str, Any]:
        """Render table structure separately from nullable columns.

        :param node: dlt IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        if node.type == "object":
            result = _closed_object_schema(
                {field.name: self._dlt_node(field.node) for field in node.properties},
                node,
            )
        elif node.type == "array" and isinstance(node.items, TypedNode):
            result = {"type": "array", "items": self._dlt_node(node.items)}
        else:
            return self._dlt_scalar(node)
        result.update(mutable_value(node.annotations))
        result.update(mutable_value(node.extensions))
        return result

    def _dlt_scalar(self, node: TypedNode) -> dict[str, Any]:
        """Select logical templates and keep column metadata inside null unions.

        :param node: Scalar dlt IR node.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        :raises ConversionError: If the node has an unknown dlt ``data_type``.
        """
        logical = node.hints.logical_type
        if node.type == "unknown" or (logical is not None and logical not in DLT_TYPES):
            path = node.provenance.path if node.provenance is not None else ()
            name = path[-1] if path else "<unknown>"
            msg = f"Column {name!r} has unknown dlt data_type {logical!r}."
            raise ConversionError(msg)
        result = deepcopy(DLT_TYPES[logical]) if logical is not None else {}
        if node.hints.precision is not None or node.hints.scale is not None:
            hints = result.setdefault("x-dlt", {})
            hints["precision"] = node.hints.precision
            if node.hints.scale is not None:
                hints["scale"] = node.hints.scale
        result.update(mutable_value(node.constraints))
        result.update(mutable_value(node.annotations))
        metadata = mutable_value(node.extensions)
        source_hints = metadata.pop("x-dlt", {})
        if self.preserve_unknown_hints:
            hints = {key: value for key, value in source_hints.items() if key not in DLT_KNOWN and value is not None}
            if hints:
                result.setdefault("x-dlt", {}).update(hints)
        result.update(metadata)
        return _nullable(result, nullable=bool(node.nullable and result))

    def _iceberg_node(self, node: TypedNode) -> dict[str, Any]:
        """Wrap nullable types and elements before applying field metadata.

        :param node: Iceberg IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        result = self._iceberg_type(node)
        result.update(mutable_value(node.constraints))
        result.update(mutable_value(node.annotations))
        extensions = mutable_value(node.extensions)
        metadata = extensions.pop("x-iceberg", {})
        metadata = {key: value for key, value in metadata.items() if key not in ICEBERG_ELEMENT_KEYS}
        if metadata:
            result.setdefault("x-iceberg", {}).update(metadata)
        result.update(extensions)
        return _nullable(result, nullable=node.nullable is True)

    def _iceberg_type(self, node: TypedNode) -> dict[str, Any]:
        """Render structural Iceberg nodes directly from their typed children.

        :param node: Iceberg IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        if node.type == "object":
            result = _closed_object_schema(
                {field.name: self._iceberg_field(field) for field in node.properties},
                node,
            )
        elif node.type == "array" and isinstance(node.items, TypedNode):
            result = {"type": "array", "items": self._iceberg_node(node.items)}
        elif node.type == "map" and node.key_type is not None and node.value_type is not None:
            result = _kv_array_schema(
                key_schema=self._iceberg_node(node.key_type),
                value_schema=self._iceberg_node(node.value_type),
                description="Iceberg map rendered as key/value pairs (JSON object keys must be strings).",
            )
        else:
            result = self._iceberg_scalar(node)
        return result

    def _iceberg_scalar(self, node: TypedNode) -> dict[str, Any]:
        """Apply physical bounds and source logical encodings to scalar nodes.

        :param node: Scalar Iceberg IR node.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        :raises ConversionError: If the logical type is unsupported or missing required parameters.
        """
        hints = node.hints
        logical = hints.logical_type
        if logical not in ICEBERG_LOGICAL_TYPES:
            msg = f"No JSON Schema mapping for Iceberg type: {logical or node.type}"
            raise ConversionError(msg)
        result: dict[str, Any] = {"type": node.type}
        if node.type == "integer" and hints.bit_width:
            bound = 2 ** (hints.bit_width - 1)
            result.update(minimum=-bound, maximum=bound - 1)
        if logical in {"date", "time", "uuid", "timestamp-with-tz"}:
            result["format"] = "date-time" if logical == "timestamp-with-tz" else logical
        elif logical == "timestamp-without-tz":
            result.update(
                description="ISO 8601 timestamp with no timezone attached.",
                **{"x-iceberg": {"logical_type": logical}},
            )
        elif logical == "decimal":
            if hints.precision is None or hints.scale is None:
                msg = "Iceberg decimals require precision and scale"
                raise ConversionError(msg)
            result = {
                "type": "string",
                "pattern": decimal_pattern(hints.precision, hints.scale),
                "x-iceberg": {"logical_type": logical, "precision": hints.precision, "scale": hints.scale},
            }
        elif logical == "fixed":
            if hints.length is None:
                msg = "Iceberg fixed types require length"
                raise ConversionError(msg)
            result = {
                "type": "string",
                "contentEncoding": "base64",
                "x-iceberg": {"logical_type": logical, "length": hints.length},
            }
        return result

    def _iceberg_field(self, field: Field) -> dict[str, Any]:
        """Place descriptions, IDs and non-null defaults outside optional types.

        :param field: Iceberg IR field to emit.
        :type field: Field
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        result = self._iceberg_node(field.node)
        annotations = mutable_value(field.annotations)
        if not annotations.get("description"):
            annotations.pop("description", None)
        result.update(annotations)
        extensions = mutable_value(field.extensions)
        metadata = extensions.pop("x-iceberg", {})
        metadata = {
            key: value
            for key, value in metadata.items()
            if key not in {"initial_default", "write_default"} or value is not None
        }
        if metadata:
            result.setdefault("x-iceberg", {}).update(metadata)
        result.update(extensions)
        return result

    def _pyarrow_document(self, document: SchemaDocument) -> dict[str, Any]:
        """Emit a Draft 2020-12 JSON Schema document from a PyArrow IR document.

        :param document: PyArrow-sourced IR schema document.
        :type document: SchemaDocument
        :return: Rendered JSON Schema dictionary with canonical top-level keyword ordering.
        :rtype: dict[str, Any]
        """
        body = self._pyarrow_node(document.root)
        annotations = mutable_value(document.annotations)
        if not annotations.get("description"):
            annotations.pop("description", None)
        envelope: dict[str, Any] = {"$schema": document.dialect or JSON_SCHEMA_DIALECT}
        if document.identifier is not None:
            envelope["$id"] = document.identifier
        if document.name is not None:
            envelope["title"] = document.name
        if "description" in annotations:
            envelope["description"] = annotations["description"]
        envelope.update({key: val for key, val in body.items() if key != "description"})
        envelope.update({key: val for key, val in annotations.items() if key != "description"})
        extensions = mutable_value(document.extensions)
        pa_meta = extensions.pop("x-pyarrow", {})
        if pa_meta:
            envelope.setdefault("x-pyarrow", {}).update(pa_meta)
        envelope.update(extensions)
        return envelope

    def _pyarrow_node(self, node: TypedNode) -> dict[str, Any]:
        """Wrap nullable PyArrow types and elements before applying outer field metadata.

        :param node: PyArrow IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        result = self._pyarrow_type(node)
        result.update(mutable_value(node.constraints))
        annotations = mutable_value(node.annotations)
        if not annotations.get("description"):
            annotations.pop("description", None)
        result.update(annotations)
        extensions = mutable_value(node.extensions)
        pa_meta = extensions.pop("x-pyarrow", {})
        if pa_meta:
            result.setdefault("x-pyarrow", {}).update(pa_meta)
        result.update(extensions)
        return _nullable(result, nullable=node.nullable is True)

    def _pyarrow_type(self, node: TypedNode) -> dict[str, Any]:
        """Render structural PyArrow nodes directly from their typed children.

        :param node: PyArrow IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        if node.type == "object":
            return _closed_object_schema(
                {field.name: self._pyarrow_field(field) for field in node.properties},
                node,
            )
        if node.type == "array" and isinstance(node.items, TypedNode):
            result: dict[str, Any] = {"type": "array", "items": self._pyarrow_node(node.items)}
            if node.hints.logical_type == "fixed-size-list" and node.hints.length is not None:
                result["minItems"] = node.hints.length
                result["maxItems"] = node.hints.length
            return result
        if node.type == "map" and node.key_type is not None and node.value_type is not None:
            return _kv_array_schema(
                key_schema=self._pyarrow_node(node.key_type),
                value_schema=self._pyarrow_node(node.value_type),
                description="PyArrow map rendered as key/value pairs (JSON object keys must be strings).",
            )
        return self._pyarrow_scalar(node)

    def _pyarrow_scalar(self, node: TypedNode) -> dict[str, Any]:
        """Apply physical bounds and logical encodings to PyArrow scalar nodes.

        :param node: Scalar PyArrow IR node.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        :raises ConversionError: If the logical type is unsupported or missing required parameters.
        """
        hints = node.hints
        logical = hints.logical_type
        if logical not in PYARROW_LOGICAL_TYPES:
            msg = f"No JSON Schema mapping for PyArrow type: {logical or node.type}"
            raise ConversionError(msg)
        result: dict[str, Any] = {"type": node.type}
        if logical in PYARROW_SIGNED_INT_TYPES and hints.bit_width:
            bound = 2 ** (hints.bit_width - 1)
            result.update(minimum=-bound, maximum=bound - 1)
        elif logical in PYARROW_UNSIGNED_INT_TYPES and hints.bit_width:
            result.update(minimum=0, maximum=(2**hints.bit_width) - 1)
        if logical in {"date", "time", "timestamp-with-tz"}:
            result["format"] = "date-time" if logical == "timestamp-with-tz" else logical
        elif logical == "timestamp-without-tz":
            result["description"] = "ISO 8601 timestamp with no timezone attached."
        elif logical in {"binary", "fixed"}:
            result["contentEncoding"] = "base64"
        elif logical == "decimal":
            if hints.precision is None or hints.scale is None:
                msg = "PyArrow decimals require precision and scale"
                raise ConversionError(msg)
            result = {
                "type": "string",
                "pattern": decimal_pattern(hints.precision, hints.scale),
            }
        return result

    def _pyarrow_field(self, field: Field) -> dict[str, Any]:
        """Place field descriptions and custom Arrow metadata outside nullable wrappers.

        :param field: PyArrow IR field to emit.
        :type field: Field
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        result = self._pyarrow_node(field.node)
        annotations = mutable_value(field.annotations)
        if not annotations.get("description"):
            annotations.pop("description", None)
        result.update(annotations)
        extensions = mutable_value(field.extensions)
        pa_meta = extensions.pop("x-pyarrow", {})
        if pa_meta:
            result.setdefault("x-pyarrow", {}).update(pa_meta)
        result.update(extensions)
        return result
