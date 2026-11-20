"""Immutable, target-independent schema facts and composition contracts."""

from collections.abc import Callable, Mapping
from dataclasses import field
from typing import Annotated, Final, Literal, Protocol

from frozendict import frozendict
from pydantic import (
    AfterValidator,
    ConfigDict,
    GetPydanticSchema,
    InstanceOf,
    StrictBool,
    StrictInt,
    StrictStr,
)
from pydantic import Field as PydanticField
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic_core import core_schema

from cdm_data_loaders.converters.core.errors import ConversionError, unwrap_conversion_errors
from cdm_data_loaders.converters.core.extensions import Extensions, validated_extensions
from cdm_data_loaders.converters.core.ir_values import Value, freeze_dict, freeze_mapping

IR_DATACLASS_CONFIG: Final[ConfigDict] = ConfigDict(extra="forbid")

type NodeType = Literal[
    "object", "map", "array", "string", "integer", "number", "boolean", "null", "any", "never", "unknown"
]
type Source = Literal["json-schema", "dlt", "iceberg", "pydantic", "xsd", "pyarrow"]
type NonNegativeStrictInt = Annotated[StrictInt, PydanticField(ge=0)]


def _validate_frozen_mapping(values: Mapping[str, Value]) -> Mapping[str, Value]:
    """Validate and deep-freeze a JSON-shaped metadata mapping.

    :param values: Candidate metadata mapping.
    :type values: Mapping[str, Value]
    :return: Deep-frozen schema mapping.
    :rtype: Mapping[str, Value]
    :raises ValueError: If ``values`` contains non-string keys or unsupported values.
    """
    try:
        return freeze_mapping(values)
    except TypeError as error:
        raise ValueError(str(error)) from error


def _schema_metadata(values: Mapping[str, Value]) -> Mapping[str, Value]:
    """Own schema metadata without accepting alternate extension declarations.

    :param values: Candidate schema metadata mapping.
    :type values: Mapping[str, Value]
    :return: Deep-frozen schema metadata mapping.
    :rtype: Mapping[str, Value]
    :raises ConversionError: If a top-level key starts with ``x-``.
    """
    owned = _validate_frozen_mapping(values)
    for name in owned:
        if name.startswith("x-"):
            msg = f"Extension namespace {name} must use extensions"
            raise ConversionError(msg)
    return owned


def _validate_schema_metadata(values: Mapping[str, Value]) -> Mapping[str, Value]:
    """Validate and freeze schema constraints or annotations via ``_schema_metadata``.

    :param values: Candidate schema metadata mapping.
    :type values: Mapping[str, Value]
    :return: Deep-frozen schema metadata mapping.
    :rtype: Mapping[str, Value]
    :raises ValueError: If ``values`` contains invalid keys or values.
    :raises ConversionError: If a top-level key starts with ``x-``.
    """
    return _schema_metadata(values)


def _validate_extensions_map(values: Mapping[str, Value]) -> Extensions:
    """Validate an extension mapping or preserve an already-validated ``Extensions`` object.

    :param values: Candidate extension mapping or ``Extensions`` instance.
    :type values: Mapping[str, Value]
    :return: Validated immutable ``Extensions`` mapping.
    :rtype: Extensions
    :raises ValueError: If ``values`` contains non-string keys or unsupported values.
    :raises ExtensionError: If any namespace or payload violates its contract.
    """
    try:
        return validated_extensions(values)
    except TypeError as error:
        raise ValueError(str(error)) from error


def _mapping_validator_schema(
    validator: Callable[[Mapping[str, Value]], Mapping[str, Value]],
) -> GetPydanticSchema:
    """Build a Pydantic schema checking ``Mapping`` first and then applying ``validator``.

    :param validator: Mapping validator returning an immutable mapping.
    :type validator: Callable[[Mapping[str, Value]], Mapping[str, Value]]
    :return: ``GetPydanticSchema`` annotation metadata.
    :rtype: GetPydanticSchema
    """
    return GetPydanticSchema(
        lambda _tp, _handler: core_schema.chain_schema(
            [
                core_schema.is_instance_schema(Mapping),
                core_schema.no_info_plain_validator_function(validator),
            ]
        )
    )


def _unique_fields(fields: tuple["Field", ...]) -> tuple["Field", ...]:
    """Reject duplicate property names in an object node.

    :param fields: Validated property ``Field`` tuple.
    :type fields: tuple[Field, ...]
    :return: Validated property ``Field`` tuple.
    :rtype: tuple[Field, ...]
    :raises ValueError: If two ``Field`` entries share the same ``name``.
    """
    if len({prop.name for prop in fields}) != len(fields):
        msg = "Duplicate property names"
        raise ValueError(msg)
    return fields


type FrozenValueMap = Annotated[Mapping[str, Value], _mapping_validator_schema(_validate_frozen_mapping)]
type SchemaMetadataMap = Annotated[Mapping[str, Value], _mapping_validator_schema(_validate_schema_metadata)]
type ExtensionsMap = Annotated[Mapping[str, Value], _mapping_validator_schema(_validate_extensions_map)]
type NodeMap = Annotated[Mapping[StrictStr, InstanceOf["TypedNode"]], AfterValidator(freeze_dict)]
type NestedNodeMap = Annotated[Mapping[StrictStr, NodeMap], AfterValidator(freeze_dict)]


@pydantic_dataclass(frozen=True, slots=True, config=IR_DATACLASS_CONFIG)
class Provenance:
    """Source location and owned metadata, not an alternate conversion input."""

    source: Source
    path: tuple[StrictStr | StrictInt, ...] = ()
    metadata: FrozenValueMap = field(default_factory=frozendict)


@pydantic_dataclass(frozen=True, slots=True, config=IR_DATACLASS_CONFIG)
class NodeHints:
    """Source logical facts; target encodings and fallbacks belong to emitters."""

    logical_type: StrictStr | None = None
    precision: NonNegativeStrictInt | None = None
    scale: NonNegativeStrictInt | None = None
    bit_width: NonNegativeStrictInt | None = None
    timezone: StrictBool | None = None
    length: NonNegativeStrictInt | None = None


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


@pydantic_dataclass(frozen=True, slots=True, config=IR_DATACLASS_CONFIG)
class TypedNode:
    """A value schema, independent of a property's presence requirement.

    ``declared_type`` preserves absent, scalar and union-list declarations.
    ``nullable=None`` means unresolved, not false. Schema-valued keywords are
    stored as nodes, never mixed into literal enum/default/example data.
    """

    type: NodeType
    nullable: StrictBool | None = None
    declared_type: StrictStr | tuple[StrictStr, ...] | None = None
    inferred_type: StrictStr | None = None
    hints: InstanceOf[NodeHints] = field(default_factory=NodeHints)
    properties: Annotated[tuple[InstanceOf["Field"], ...], AfterValidator(_unique_fields)] = ()
    required_names: tuple[StrictStr, ...] | None = None
    items: InstanceOf["TypedNode"] | tuple[InstanceOf["TypedNode"], ...] | None = None
    prefix_items: tuple[InstanceOf["TypedNode"], ...] | None = None
    pattern_properties: NodeMap = field(default_factory=frozendict)
    additional_properties: InstanceOf["TypedNode"] | None = None
    any_of: tuple[InstanceOf["TypedNode"], ...] | None = None
    one_of: tuple[InstanceOf["TypedNode"], ...] | None = None
    schema_keywords: NodeMap = field(default_factory=frozendict)
    schema_maps: NestedNodeMap = field(default_factory=frozendict)
    key_type: InstanceOf["TypedNode"] | None = None
    value_type: InstanceOf["TypedNode"] | None = None
    constraints: SchemaMetadataMap = field(default_factory=frozendict)
    annotations: SchemaMetadataMap = field(default_factory=frozendict)
    extensions: ExtensionsMap = field(default_factory=Extensions)
    provenance: InstanceOf[Provenance] | None = None
    source_keywords: tuple[StrictStr, ...] = ()

    def aggregate_metadata(self, field: "Field | None" = None) -> Mapping[str, Value]:
        """Merge constraints, annotations, and extensions into a single mapping.

        :param field: Optional enclosing property ``Field`` whose metadata overrides node metadata.
        :type field: Field | None
        :return: Merged metadata mapping.
        :rtype: Mapping[str, Value]
        """
        all_meta = {**self.constraints, **self.annotations, **self.extensions}
        if field is not None:
            all_meta.update(field.annotations)
            all_meta.update(field.extensions)
        return all_meta


@pydantic_dataclass(frozen=True, slots=True, config=IR_DATACLASS_CONFIG)
class Field:
    """Property presence and metadata outside any nullable value wrapper."""

    name: StrictStr
    node: InstanceOf[TypedNode]
    required: StrictBool = False
    annotations: SchemaMetadataMap = field(default_factory=frozendict)
    extensions: ExtensionsMap = field(default_factory=Extensions)
    provenance: InstanceOf[Provenance] | None = None


@pydantic_dataclass(frozen=True, slots=True, config=IR_DATACLASS_CONFIG)
class SchemaDocument:
    """A named schema root with document metadata separate from value metadata."""

    root: InstanceOf[TypedNode]
    name: StrictStr | None = None
    dialect: StrictStr | None = None
    identifier: StrictStr | None = None
    annotations: SchemaMetadataMap = field(default_factory=frozendict)
    extensions: ExtensionsMap = field(default_factory=Extensions)
    provenance: InstanceOf[Provenance] | None = None
    jsonschema_validator_cls: InstanceOf[type] | None = None


class Emitter[Output](Protocol):
    """Consume structural IR directly, applying target policy at emission."""

    def emit(self, document: SchemaDocument) -> Output:
        """Produce a target schema from the document tree."""
        ...


for _ir_cls in (TypedNode, Field, SchemaDocument):
    unwrap_conversion_errors(_ir_cls)
