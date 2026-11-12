"""Emit official LinkML schema models from typed schema documents."""

import re
from dataclasses import dataclass, field
from typing import Final, cast

from linkml_runtime.linkml_model.meta import (
    ClassDefinition,
    EnumDefinition,
    PermissibleValue,
    Prefix,
    SchemaDefinition,
    SlotDefinition,
)

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value

LINKML_PREFIX: Final = "https://w3id.org/linkml/"
IDENTIFIER_PATTERN: Final = re.compile(r"[^A-Za-z0-9_]")
SCALAR_RANGES: Final = {
    "string": "string",
    "integer": "integer",
    "number": "float",
    "boolean": "boolean",
}
LOGICAL_RANGES: Final = {
    "date": "date",
    "time": "time",
    "timestamp": "datetime",
    "timestamp-with-tz": "datetime",
    "timestamp-without-tz": "datetime",
    "decimal": "decimal",
}
UNSIGNED_LOGICAL_INTS: Final[frozenset[str]] = frozenset({"uint8", "uint16", "uint32", "uint64"})


class LinkMLEmitterError(ConversionError):
    """Raised when a typed node has no faithful LinkML representation."""


class EmptyLinkMLSlotNameError(LinkMLEmitterError):
    """Raised when a field has an empty or whitespace-only name that cannot form a LinkML slot."""


class NestedLinkMLMultivaluedError(LinkMLEmitterError):
    """Raised when an array contains nested array or map items that cannot be represented as a flat LinkML slot."""


@dataclass(slots=True)
class LinkMLEmitter:
    """Render a document as a LinkML SchemaDefinition."""

    schema_name: str | None = None
    _classes: dict[str, ClassDefinition] = field(init=False, default_factory=dict)
    _enums: dict[str, EnumDefinition] = field(init=False, default_factory=dict)
    _used_names: set[str] = field(init=False, default_factory=set)

    def emit(self, document: SchemaDocument) -> SchemaDefinition:
        """Emit a standalone LinkML schema model.

        :param document: Typed schema document to convert.
        :type document: SchemaDocument
        :returns: Schema model suitable for LinkML's YAML dumper.
        """
        if document.root.type != "object":
            msg = "A LinkML schema requires an object root"
            raise LinkMLEmitterError(msg)
        self._classes = {}
        self._enums = {}
        self._used_names.clear()
        title = document.annotations.get("title")
        document_name = document.name or title if isinstance(title, str) else document.name
        schema_name = self._identifier(self.schema_name or document_name or "schema")
        root_name = self._unique_name(document_name or "Root")
        self._emit_class(root_name, document.root, include_description=False)
        identifier = document.identifier or f"urn:linkml:{schema_name}"
        description = document.annotations.get("description")
        return SchemaDefinition(
            id=identifier,
            name=schema_name,
            prefixes={
                schema_name: Prefix(prefix_prefix=schema_name, prefix_reference=f"{identifier}#"),
                "linkml": Prefix(prefix_prefix="linkml", prefix_reference=LINKML_PREFIX),
            },
            default_prefix=schema_name,
            imports=["linkml:types"],
            classes=list(self._classes.values()),
            enums=list(self._enums.values()),
            description=str(description) if description is not None else None,
        )

    def emit_node(self, node: TypedNode) -> SlotDefinition:
        """Emit a single node as a slot definition.

        :param node: Typed value node to convert.
        :type node: TypedNode
        :returns: LinkML slot definition named ``node``.
        """
        return self._node_expression(node, "Node", "node")

    def emit_into(self, document: SchemaDocument, schema: SchemaDefinition) -> SchemaDefinition:
        """Add a document's classes to a schema without overwriting existing definitions.

        :param document: Object-rooted document to convert.
        :type document: SchemaDocument
        :param schema: Destination schema, updated only after successful conversion.
        :type schema: SchemaDefinition
        :returns: The destination schema.
        """
        if document.root.type != "object":
            msg = "A LinkML schema requires an object root"
            raise LinkMLEmitterError(msg)
        self._classes = {}
        self._enums = {}
        classes = cast("dict[str, ClassDefinition]", schema.classes)
        enums = cast("dict[str, EnumDefinition]", schema.enums)
        types = cast("dict[str, object]", schema.types)
        self._used_names = set(classes) | set(enums) | set(types)
        root_name = self._unique_name(document.name or "Root")
        self._emit_class(root_name, document.root)
        description = document.annotations.get("description")
        if description is not None:
            self._classes[root_name].description = str(description)
        classes.update(self._classes)
        enums.update(self._enums)
        return schema

    def emit_field(self, field: SchemaField) -> SlotDefinition:
        """Emit a single field attribute.

        :param field: Typed field to convert.
        :type field: SchemaField
        :returns: Named LinkML slot definition.
        """
        return self._emit_attribute(field, "Field")

    def _emit_class(self, class_name: str, node: TypedNode, *, include_description: bool = True) -> None:
        """Emit an object node as a class with local attribute definitions."""
        description = node.annotations.get("description") if include_description else None
        self._classes[class_name] = ClassDefinition(
            name=class_name,
            attributes=[self._emit_attribute(field, class_name) for field in node.properties],
            description=str(description) if description is not None else None,
        )

    def _emit_attribute(self, field: SchemaField, owner_name: str) -> SlotDefinition:
        """Emit field cardinality, scalar constraints, and the target range.

        :param field: IR field to convert.
        :type field: SchemaField
        :param owner_name: Owning LinkML class name.
        :type owner_name: str
        :return: Emitted LinkML ``SlotDefinition``.
        :rtype: SlotDefinition
        :raises EmptyLinkMLSlotNameError: If ``field.name`` is empty or whitespace-only.
        """
        if not field.name or not field.name.strip():
            msg = f"LinkML slot names must be non-empty, got {field.name!r}."
            raise EmptyLinkMLSlotNameError(msg)
        node = field.node
        result = self._node_expression(node, owner_name, field.name)
        if field.required:
            result.required = True
        if field.annotations.get("description") is not None:
            result.description = str(field.annotations["description"])
        elif node.annotations.get("description") is not None:
            result.description = str(node.annotations["description"])
        return result

    def _node_expression(self, node: TypedNode, owner_name: str, field_name: str) -> SlotDefinition:
        """Emit one LinkML slot expression from a typed value node.

        :param node: IR value node to convert.
        :type node: TypedNode
        :param owner_name: Owning LinkML class name.
        :type owner_name: str
        :param field_name: Slot attribute name.
        :type field_name: str
        :return: Emitted LinkML ``SlotDefinition``.
        :rtype: SlotDefinition
        :raises LinkMLEmitterError: If ``node`` cannot be represented in LinkML.
        """
        if node.any_of is not None or node.one_of is not None or isinstance(node.declared_type, tuple):
            msg = f"LinkML emission does not support union node {field_name!r}"
            raise LinkMLEmitterError(msg)
        result = SlotDefinition(name=field_name)
        if node.type == "array":
            if not isinstance(node.items, TypedNode):
                msg = f"LinkML emission requires one item schema for array field {field_name!r}"
                raise LinkMLEmitterError(msg)
            if node.items.type in {"array", "map"}:
                msg = f"LinkML emission does not support nested {node.items.type!r} items inside array field {field_name!r}"
                raise NestedLinkMLMultivaluedError(msg)
            result = self._node_expression(node.items, owner_name, field_name)
            result.multivalued = True
            if node.provenance is not None and node.provenance.source == "pyarrow":
                result.minimum_value = None
                result.maximum_value = None
            if node.hints.logical_type == "fixed-size-list" and node.hints.length is not None:
                result.minimum_cardinality = node.hints.length
                result.maximum_cardinality = node.hints.length
            return result

        if node.type == "map":
            if not isinstance(node.key_type, TypedNode) or not isinstance(node.value_type, TypedNode):
                msg = f"LinkML emission requires key and value schemas for map field {field_name!r}"
                raise LinkMLEmitterError(msg)
            map_class_name = self._unique_name(f"{owner_name}_{field_name}_map")
            value_required = (
                node.value_type.nullable is False
                if node.provenance is not None and node.provenance.source == "pyarrow"
                else False
            )
            map_node = TypedNode(
                type="object",
                nullable=False,
                properties=(
                    SchemaField("key", node.key_type, required=True),
                    SchemaField("value", node.value_type, required=value_required),
                ),
                provenance=node.provenance,
            )
            self._emit_class(map_class_name, map_node)
            return SlotDefinition(name=field_name, range=map_class_name, multivalued=True)

        if node.type == "object":
            class_name = self._unique_name(f"{owner_name}_{field_name}")
            self._emit_class(class_name, node)
            result.range = class_name
        elif "enum" in node.constraints:
            result.range = self._emit_enum(node, owner_name, field_name)
        else:
            result.range = self._scalar_range(node, field_name)
            self._emit_pyarrow_bounds(node, result)
        self._emit_constraints(node, result)
        return result

    @staticmethod
    def _emit_pyarrow_bounds(node: TypedNode, result: SlotDefinition) -> None:
        """Attach signed or unsigned integer bit-width bounds for PyArrow integer nodes.

        :param node: Scalar IR node.
        :type node: TypedNode
        :param result: Target LinkML ``SlotDefinition`` to populate.
        :type result: SlotDefinition
        """
        if (
            node.provenance is not None
            and node.provenance.source == "pyarrow"
            and node.type == "integer"
            and node.hints.bit_width is not None
        ):
            if node.hints.logical_type in UNSIGNED_LOGICAL_INTS:
                result.minimum_value = 0
                result.maximum_value = (2**node.hints.bit_width) - 1
            else:
                bound = 2 ** (node.hints.bit_width - 1)
                result.minimum_value = -bound
                result.maximum_value = bound - 1

    def _emit_enum(self, node: TypedNode, owner_name: str, field_name: str) -> str:
        """Emit a named LinkML enum for string-only JSON enum values."""
        values = node.constraints["enum"]
        if not isinstance(values, tuple) or any(not isinstance(value, str) for value in values):
            msg = f"LinkML enums require string values for field {field_name!r}"
            raise LinkMLEmitterError(msg)
        enum_name = self._unique_name(f"{owner_name}_{field_name}_enum")
        self._enums[enum_name] = EnumDefinition(
            name=enum_name,
            permissible_values=[PermissibleValue(text=value) for value in values if isinstance(value, str)],
        )
        return enum_name

    def _scalar_range(self, node: TypedNode, field_name: str) -> str:
        """Map scalar facts to LinkML's built-in type ranges."""
        logical = node.hints.logical_type
        if logical in LOGICAL_RANGES:
            return LOGICAL_RANGES[logical]
        if node.type in SCALAR_RANGES:
            return SCALAR_RANGES[node.type]
        msg = f"LinkML emission does not support {node.type!r} field {field_name!r}"
        raise LinkMLEmitterError(msg)

    @staticmethod
    def _emit_constraints(node: TypedNode, result: SlotDefinition) -> None:
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
                setattr(result, target, mutable_value(constraints[source]))

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
        identifier = IDENTIFIER_PATTERN.sub("_", value).strip("_")
        if not identifier:
            identifier = "schema"
        if identifier[0].isdigit():
            identifier = f"_{identifier}"
        return identifier
