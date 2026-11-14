"""Immutable, target-independent schema facts and composition contracts."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final, Literal, Protocol

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import validated_extensions
from cdm_data_loaders.converters.core.ir_values import Value, freeze_mapping

type NodeType = Literal[
    "object", "map", "array", "string", "integer", "number", "boolean", "null", "any", "never", "unknown"
]
type Source = Literal["json-schema", "dlt", "iceberg", "pydantic", "xsd", "pyarrow"]


@dataclass(frozen=True, slots=True)
class Provenance:
    """Source location and owned metadata, not an alternate conversion input."""

    source: Source
    path: tuple[str | int, ...] = ()
    metadata: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]

    def __post_init__(self) -> None:
        """Isolate source paths and metadata."""
        if self.source not in {"json-schema", "dlt", "iceberg", "pydantic", "xsd", "pyarrow"}:
            msg = f"Unknown schema source: {self.source}"
            raise ValueError(msg)
        if any(not isinstance(part, (str, int)) or isinstance(part, bool) for part in self.path):
            msg = "Source paths require string or integer segments"
            raise TypeError(msg)
        object.__setattr__(self, "path", tuple(self.path))
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class NodeHints:
    """Source logical facts; target encodings and fallbacks belong to emitters."""

    logical_type: str | None = None
    precision: int | None = None
    scale: int | None = None
    bit_width: int | None = None
    timezone: bool | None = None
    length: int | None = None

    def __post_init__(self) -> None:
        """Reject malformed logical type facts without coercion."""
        if self.logical_type is not None and not isinstance(self.logical_type, str):
            msg = "logical_type must be a string"
            raise TypeError(msg)
        for name in ("precision", "scale", "bit_width", "length"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                msg = f"{name} must be a nonnegative integer"
                raise ValueError(msg)
        if self.timezone is not None and type(self.timezone) is not bool:
            msg = "timezone must be a boolean or None"
            raise TypeError(msg)


SPEC_BOOL: Final[tuple[NodeType, NodeHints]] = ("boolean", NodeHints(logical_type="boolean"))
SPEC_INT8: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="int8", bit_width=8))
SPEC_INT16: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="int16", bit_width=16))
SPEC_INT32: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="int32", bit_width=32))
SPEC_INT64: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="long", bit_width=64))
SPEC_UINT8: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="uint8", bit_width=8))
SPEC_UINT16: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="uint16", bit_width=16))
SPEC_UINT32: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="uint32", bit_width=32))
SPEC_UINT64: Final[tuple[NodeType, NodeHints]] = ("integer", NodeHints(logical_type="uint64", bit_width=64))
SPEC_FLOAT64: Final[tuple[NodeType, NodeHints]] = ("number", NodeHints(logical_type="double"))
SPEC_DATE: Final[tuple[NodeType, NodeHints]] = ("string", NodeHints(logical_type="date"))
SPEC_TIME: Final[tuple[NodeType, NodeHints]] = ("string", NodeHints(logical_type="time"))
SPEC_TS_NAIVE: Final[tuple[NodeType, NodeHints]] = (
    "string",
    NodeHints(logical_type="timestamp-without-tz", timezone=False),
)
SPEC_TS_UTC: Final[tuple[NodeType, NodeHints]] = (
    "string",
    NodeHints(logical_type="timestamp-with-tz", timezone=True),
)
SPEC_BINARY: Final[tuple[NodeType, NodeHints]] = ("string", NodeHints(logical_type="binary"))
SPEC_STRING: Final[tuple[NodeType, NodeHints]] = ("string", NodeHints(logical_type="string"))


@dataclass(frozen=True, slots=True)
class TypedNode:
    """A value schema, independent of a property's presence requirement.

    ``declared_type`` preserves absent, scalar and union-list declarations.
    ``nullable=None`` means unresolved, not false. Schema-valued keywords are
    stored as nodes, never mixed into literal enum/default/example data.
    """

    type: NodeType
    nullable: bool | None = None
    declared_type: str | tuple[str, ...] | None = None
    inferred_type: str | None = None
    hints: NodeHints = field(default_factory=NodeHints)
    properties: tuple["Field", ...] = ()
    required_names: tuple[str, ...] | None = None
    items: "TypedNode | tuple[TypedNode, ...] | None" = None
    prefix_items: tuple["TypedNode", ...] | None = None
    pattern_properties: Mapping[str, "TypedNode"] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType] # pyright: ignore[reportAssignmentType]
    additional_properties: "TypedNode | None" = None
    any_of: tuple["TypedNode", ...] | None = None
    one_of: tuple["TypedNode", ...] | None = None
    schema_keywords: Mapping[str, "TypedNode"] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    schema_maps: Mapping[str, Mapping[str, "TypedNode"]] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    key_type: "TypedNode | None" = None
    value_type: "TypedNode | None" = None
    constraints: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    annotations: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    extensions: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    provenance: Provenance | None = None
    source_keywords: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Own mappings and sequences and validate child structure."""
        self._validate_types()
        object.__setattr__(self, "source_keywords", _string_tuple(self.source_keywords))
        if self.declared_type is not None and not isinstance(self.declared_type, str):
            object.__setattr__(self, "declared_type", _string_tuple(self.declared_type))
        object.__setattr__(self, "properties", tuple(self.properties))
        if any(not isinstance(prop, Field) for prop in self.properties):
            msg = "properties must contain Field instances"
            raise TypeError(msg)
        if len({prop.name for prop in self.properties}) != len(self.properties):
            msg = "Duplicate property names"
            raise ValueError(msg)
        if self.required_names is not None:
            object.__setattr__(self, "required_names", _string_tuple(self.required_names))
        self._own_children()
        object.__setattr__(self, "constraints", _schema_metadata(self.constraints))
        _init_common_metadata(self)

    def _validate_types(self) -> None:
        """Reject invalid scalar facts and mutable stand-ins for typed models."""
        if self.type not in {
            "object",
            "map",
            "array",
            "string",
            "integer",
            "number",
            "boolean",
            "null",
            "any",
            "never",
            "unknown",
        }:
            msg = f"Unknown node type: {self.type}"
            raise ValueError(msg)
        if self.nullable is not None and type(self.nullable) is not bool:
            msg = "nullable must be a boolean or None"
            raise TypeError(msg)
        if self.inferred_type is not None and not isinstance(self.inferred_type, str):
            msg = "inferred_type must be a string or None"
            raise TypeError(msg)
        if not isinstance(self.hints, NodeHints):
            msg = "hints must be NodeHints"
            raise TypeError(msg)

    def _own_children(self) -> None:
        """Copy each schema-valued child collection into immutable containers."""
        for name in ("prefix_items", "any_of", "one_of"):
            sequence = getattr(self, name)
            if sequence is not None:
                object.__setattr__(self, name, _node_tuple(sequence))
        if isinstance(self.items, (list, tuple)):
            object.__setattr__(self, "items", _node_tuple(self.items))
        elif self.items is not None and not isinstance(self.items, TypedNode):
            msg = "items must be a node or tuple of nodes"
            raise TypeError(msg)
        for name in ("additional_properties", "key_type", "value_type"):
            child = getattr(self, name)
            if child is not None and not isinstance(child, TypedNode):
                msg = f"{name} must be a node"
                raise TypeError(msg)
        for name in ("pattern_properties", "schema_keywords"):
            object.__setattr__(self, name, _node_mapping(getattr(self, name)))
        _string_tuple(tuple(self.schema_maps))
        object.__setattr__(
            self,
            "schema_maps",
            frozendict({name: _node_mapping(children) for name, children in self.schema_maps.items()}),
        )

    def aggregate_metadata(self, field: "Field | None" = None) -> Mapping[str, Value]:
        """Merge constraints, annotations, and extensions into a single mapping."""
        all_meta = {**self.constraints, **self.annotations, **self.extensions}
        if field is not None:
            all_meta.update(field.annotations)
            all_meta.update(field.extensions)
        return all_meta


def _schema_metadata(values: Mapping[str, Value]) -> Mapping[str, Value]:
    """Own schema metadata without accepting alternate extension declarations."""
    owned = freeze_mapping(values)
    for name in owned:
        if name.startswith("x-"):
            msg = f"Extension namespace {name} must use extensions"
            raise ConversionError(msg)
    return owned


def _string_tuple(values: tuple[str, ...]) -> tuple[str, ...]:
    """Own ordered string declarations without accepting a scalar string."""
    if not isinstance(values, (list, tuple)) or any(not isinstance(value, str) for value in values):
        msg = "Expected a list or tuple of strings"
        raise TypeError(msg)
    return tuple(values)


def _node_tuple(values: tuple[TypedNode, ...]) -> tuple[TypedNode, ...]:
    """Own a sequence of immutable schema nodes."""
    if any(not isinstance(value, TypedNode) for value in values):
        msg = "Expected TypedNode values"
        raise TypeError(msg)
    return tuple(values)


def _validate_provenance(value: Provenance | None) -> None:
    """Reject mutable or untyped provenance substitutes."""
    if value is not None and not isinstance(value, Provenance):
        msg = "provenance must be Provenance or None"
        raise TypeError(msg)


def _init_common_metadata(obj: "TypedNode | Field | SchemaDocument") -> None:
    """Validate provenance and freeze annotations and extensions on an IR model.

    :param obj: IR node, field, or document instance being initialized.
    :type obj: TypedNode | Field | SchemaDocument
    """
    _validate_provenance(obj.provenance)
    object.__setattr__(obj, "annotations", _schema_metadata(obj.annotations))
    object.__setattr__(obj, "extensions", validated_extensions(obj.extensions))


def _node_mapping(nodes: Mapping[str, TypedNode]) -> Mapping[str, TypedNode]:
    """Own a named collection of immutable nodes."""
    if any(not isinstance(key, str) or not isinstance(value, TypedNode) for key, value in nodes.items()):
        msg = "Expected string keys and TypedNode values"
        raise TypeError(msg)
    return frozendict(nodes)


@dataclass(frozen=True, slots=True)
class Field:
    """Property presence and metadata outside any nullable value wrapper."""

    name: str
    node: TypedNode
    required: bool = False
    annotations: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    extensions: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    provenance: Provenance | None = None

    def __post_init__(self) -> None:
        """Validate field identity and own its metadata."""
        if not isinstance(self.name, str) or not isinstance(self.node, TypedNode) or type(self.required) is not bool:
            msg = "Fields require a string name, node and boolean required flag"
            raise TypeError(msg)
        _init_common_metadata(self)


@dataclass(frozen=True, slots=True)
class SchemaDocument:
    """A named schema root with document metadata separate from value metadata."""

    root: TypedNode
    name: str | None = None
    dialect: str | None = None
    identifier: str | None = None
    annotations: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    extensions: Mapping[str, Value] = field(default_factory=frozendict)  # pyright: ignore[reportAssignmentType]
    provenance: Provenance | None = None
    jsonschema_validator_cls: type | None = None

    def __post_init__(self) -> None:
        """Validate the root and isolate document metadata."""
        if not isinstance(self.root, TypedNode):
            msg = "A schema document requires a TypedNode root"
            raise TypeError(msg)
        for name in ("name", "dialect", "identifier"):
            if getattr(self, name) is not None and not isinstance(getattr(self, name), str):
                msg = f"{name} must be a string or None"
                raise TypeError(msg)
        _init_common_metadata(self)


class Emitter[Output](Protocol):
    """Consume structural IR directly, applying target policy at emission."""

    def emit(self, document: SchemaDocument) -> Output:
        """Produce a target schema from the document tree."""
        ...
