"""Emit LinkML YAML-compatible schema mappings from typed schema documents."""

import re
from dataclasses import dataclass, field
from typing import Any, Final

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.ir import Field as SchemaField
from cdm_data_loaders.converters.ir import SchemaDocument, TypedNode
from cdm_data_loaders.converters.ir_values import mutable_value

_LINKML_PREFIX: Final = "https://w3id.org/linkml/"
_IDENTIFIER_PATTERN: Final = re.compile(r"[^A-Za-z0-9_]")
_SCALAR_RANGES: Final = {
    "string": "string",
    "integer": "integer",
    "number": "float",
    "boolean": "boolean",
}
_LOGICAL_RANGES: Final = {
    "date": "date",
    "time": "time",
    "timestamp": "datetime",
    "timestamp-with-tz": "datetime",
    "timestamp-without-tz": "datetime",
    "decimal": "decimal",
}


class LinkMLEmitterError(ConversionError):
    """Raised when a typed node has no faithful LinkML representation."""


@dataclass(slots=True)
class LinkMLEmitter:
    """Render a document as a LinkML SchemaDefinition YAML mapping."""

    schema_name: str | None = None
    _classes: dict[str, dict[str, Any]] = field(init=False, default_factory=dict)
    _enums: dict[str, dict[str, Any]] = field(init=False, default_factory=dict)
    _used_names: set[str] = field(init=False, default_factory=set)

    def emit(self, document: SchemaDocument) -> dict[str, Any]:
        """Emit a standalone LinkML SchemaDefinition mapping."""
        if document.root.type != "object":
            msg = "A LinkML schema requires an object root"
            raise LinkMLEmitterError(msg)
        self._classes.clear()
        self._enums.clear()
        self._used_names.clear()
        title = document.annotations.get("title")
        document_name = document.name or title if isinstance(title, str) else document.name
        schema_name = self._identifier(self.schema_name or document_name or "schema")
        root_name = self._unique_name(document_name or "Root")
        self._emit_class(root_name, document.root)
        identifier = document.identifier or f"urn:linkml:{schema_name}"
        result: dict[str, Any] = {
            "id": identifier,
            "name": schema_name,
            "prefixes": {schema_name: f"{identifier}#", "linkml": _LINKML_PREFIX},
            "default_prefix": schema_name,
            "imports": ["linkml:types"],
            "classes": self._classes,
        }
        if document.annotations.get("description") is not None:
            result["description"] = mutable_value(document.annotations["description"])
        if self._enums:
            result["enums"] = self._enums
        return result

    def _emit_class(self, class_name: str, node: TypedNode) -> None:
        """Emit an object node as a class with local attribute definitions."""
        attributes = {field.name: self._emit_attribute(field, class_name) for field in node.properties}
        definition: dict[str, Any] = {"attributes": attributes}
        if node.annotations.get("description") is not None:
            definition["description"] = mutable_value(node.annotations["description"])
        self._classes[class_name] = definition

    def _emit_attribute(self, field: SchemaField, owner_name: str) -> dict[str, Any]:
        """Emit field cardinality, scalar constraints, and the target range."""
        node = field.node
        result = self._node_expression(node, owner_name, field.name)
        if field.required:
            result["required"] = True
        if field.annotations.get("description") is not None:
            result["description"] = mutable_value(field.annotations["description"])
        elif node.annotations.get("description") is not None:
            result["description"] = mutable_value(node.annotations["description"])
        return result

    def _node_expression(self, node: TypedNode, owner_name: str, field_name: str) -> dict[str, Any]:
        """Emit one LinkML slot expression from a typed value node."""
        if node.any_of is not None or node.one_of is not None or isinstance(node.declared_type, tuple):
            msg = f"LinkML emission does not support union node {field_name!r}"
            raise LinkMLEmitterError(msg)
        result: dict[str, Any] = {}
        if node.type == "array":
            if not isinstance(node.items, TypedNode):
                msg = f"LinkML emission requires one item schema for array field {field_name!r}"
                raise LinkMLEmitterError(msg)
            result = self._node_expression(node.items, owner_name, field_name)
            result["multivalued"] = True
            return result
        if node.type == "object":
            class_name = self._unique_name(f"{owner_name}_{field_name}")
            self._emit_class(class_name, node)
            result["range"] = class_name
        elif "enum" in node.constraints:
            result["range"] = self._emit_enum(node, owner_name, field_name)
        else:
            result["range"] = self._scalar_range(node, field_name)
        self._emit_constraints(node, result)
        return result

    def _emit_enum(self, node: TypedNode, owner_name: str, field_name: str) -> str:
        """Emit a named LinkML enum for string-only JSON enum values."""
        values = node.constraints["enum"]
        if not isinstance(values, tuple) or any(not isinstance(value, str) for value in values):
            msg = f"LinkML enums require string values for field {field_name!r}"
            raise LinkMLEmitterError(msg)
        enum_name = self._unique_name(f"{owner_name}_{field_name}_enum")
        self._enums[enum_name] = {"permissible_values": dict.fromkeys(values)}
        return enum_name

    def _scalar_range(self, node: TypedNode, field_name: str) -> str:
        """Map scalar facts to LinkML's built-in type ranges."""
        logical = node.hints.logical_type
        if logical in _LOGICAL_RANGES:
            return _LOGICAL_RANGES[logical]
        if node.type in _SCALAR_RANGES:
            return _SCALAR_RANGES[node.type]
        msg = f"LinkML emission does not support {node.type!r} field {field_name!r}"
        raise LinkMLEmitterError(msg)

    @staticmethod
    def _emit_constraints(node: TypedNode, result: dict[str, Any]) -> None:
        """Map shared scalar constraints to LinkML slot-expression fields."""
        constraints = node.constraints
        unsupported = set(constraints) - {"enum", "pattern", "minimum", "maximum"}
        if unsupported:
            msg = f"LinkML emission does not support JSON Schema constraints: {sorted(unsupported)!r}"
            raise LinkMLEmitterError(msg)
        mappings = {
            "pattern": "pattern",
            "minimum": "minimum_value",
            "maximum": "maximum_value",
        }
        for source, target in mappings.items():
            if source in constraints:
                result[target] = mutable_value(constraints[source])

    def _unique_name(self, value: str) -> str:
        """Create a unique LinkML-compatible definition name."""
        base = self._identifier(value)
        candidate = base
        index = 2
        while candidate in self._used_names:
            candidate = f"{base}_{index}"
            index += 1
        self._used_names.add(candidate)
        return candidate

    @staticmethod
    def _identifier(value: str) -> str:
        """Create a non-empty LinkML-compatible identifier."""
        identifier = _IDENTIFIER_PATTERN.sub("_", value).strip("_")
        if not identifier:
            identifier = "schema"
        if identifier[0].isdigit():
            identifier = f"_{identifier}"
        return identifier
