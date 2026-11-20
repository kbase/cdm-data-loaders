"""Reference resolution, node validation, and cycle checks for strict JSON Schema."""

from collections.abc import Mapping
from dataclasses import field, replace
from typing import Annotated, Any, Final

from frozendict import frozendict
from pydantic import ConfigDict, GetPydanticSchema, InstanceOf, StrictBool, StrictInt, StrictStr
from pydantic.dataclasses import dataclass
from pydantic_core import core_schema

from cdm_data_loaders.converters.core.errors import ConversionError, ErrorCodeMap, raise_spec_error
from cdm_data_loaders.converters.core.extensions import ExtensionRegistry, default_extension_registry
from cdm_data_loaders.converters.sources.json_schema.common import extract_extensions
from cdm_data_loaders.utils.recursion import StrictPositiveRecursionDepth
from cdm_data_loaders.validation.jsonschema.keywords import iter_child_subschemas

CONSTRAINING_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "type",
        "properties",
        "items",
        "prefixItems",
        "patternProperties",
        "enum",
        "const",
        "$ref",
        "allOf",
        "anyOf",
        "oneOf",
        "dependentSchemas",
        "then",
        "else",
    }
)

DEFAULT_FORBIDDEN_RECURSIVE_FORMATS: Final[frozenset[str]] = frozenset({"date"})


type _MappingInstance[K, V] = Annotated[
    Mapping[K, V],
    GetPydanticSchema(lambda _tp, _handler: core_schema.is_instance_schema(Mapping)),
]


@dataclass(frozen=True, slots=True, config=ConfigDict(arbitrary_types_allowed=True, extra="forbid"))
class StrictBuildContext:
    """Immutable traversal context for strict JSON Schema IR lowering."""

    defs: _MappingInstance[str, Any]
    ref_counts: Mapping[StrictStr, StrictInt]
    max_recursion_depth: StrictPositiveRecursionDepth
    extension_registry: InstanceOf[ExtensionRegistry] = field(default_factory=default_extension_registry)
    error_type: type[ConversionError] = field(default_factory=lambda: ConversionError)
    error_by_code: ErrorCodeMap = field(default_factory=frozendict)
    forbidden_recursive_formats: frozenset[StrictStr] = field(
        default_factory=lambda: DEFAULT_FORBIDDEN_RECURSIVE_FORMATS
    )

    def with_ref(self, ref_name: str) -> "StrictBuildContext":
        """Return a new context with the recursion counter for ``ref_name`` incremented by 1.

        :param ref_name: ``$defs`` definition name being entered.
        :type ref_name: str
        :return: Updated traversal context.
        :rtype: StrictBuildContext
        """
        return replace(
            self,
            ref_counts={**self.ref_counts, ref_name: self.ref_counts.get(ref_name, 0) + 1},
        )


@dataclass(frozen=True, slots=True, config=ConfigDict(extra="forbid"))
class StrictNodeMeta:
    """Node-level path, nullability, and facet payloads for strict IR lowering."""

    path: tuple[StrictStr | StrictInt, ...] = ()
    nullable: StrictBool = False
    annotations: _MappingInstance[str, Any] = field(default_factory=frozendict)
    extensions: _MappingInstance[str, Any] = field(default_factory=frozendict)
    constraints: _MappingInstance[str, Any] = field(default_factory=frozendict)


def is_null_branch(node: Mapping[str, Any]) -> bool:
    """Return ``True`` if ``node`` represents a null-only schema.

    :param node: JSON Schema node mapping.
    :type node: Mapping[str, Any]
    :return: ``True`` if ``node`` matches ``null`` alone.
    :rtype: bool
    """
    node_type = node.get("type")
    if node_type in ("null", ["null"]):
        return True
    if "const" in node and node["const"] is None and "type" not in node:
        return True
    return bool(
        "enum" in node
        and isinstance(node["enum"], list)
        and all(v is None for v in node["enum"])
        and "type" not in node
    )


def resolve_ref(
    ref: object,
    defs: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
) -> tuple[str, dict[str, Any]]:
    """Resolve a local ``#/$defs/<name>`` reference pointer against ``defs``.

    :param ref: Reference URI string.
    :type ref: object
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :return: Tuple of ``(definition_name, target_schema_dict)``.
    :rtype: tuple[str, dict[str, Any]]
    """
    prefix = "#/$defs/"
    if not isinstance(ref, str) or not ref.startswith(prefix):
        msg = f"Unresolved or unsupported non-local $ref: {ref!r}"
        return raise_spec_error("E9", msg, error_type=error_type, error_by_code=error_by_code)
    name = ref[len(prefix) :]
    if name not in defs or not isinstance(defs[name], dict):
        msg = f"Unresolvable or unresolved $ref {ref!r}: definition {name!r} does not exist in $defs"
        return raise_spec_error("E9", msg, error_type=error_type, error_by_code=error_by_code)
    return name, defs[name]


def validate_node(
    node: object,
    *,
    extension_registry: ExtensionRegistry,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
) -> dict[str, Any]:
    """Validate that ``node`` is a constrained JSON Schema object without unsupported keywords.

    :param node: Raw JSON Schema node.
    :type node: object
    :param extension_registry: Extension registry used for ``x-*`` validation.
    :type extension_registry: ExtensionRegistry
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :return: Validated schema dictionary.
    :rtype: dict[str, Any]
    """
    if isinstance(node, bool) or not isinstance(node, dict):
        msg = f"Unconstrained or boolean schema node is not supported: {node!r}"
        return raise_spec_error("E4", msg, error_type=error_type, error_by_code=error_by_code)
    extract_extensions(node, extension_registry)
    if isinstance(node.get("if"), Mapping):
        extract_extensions(node["if"], extension_registry)
    if "not" in node:
        msg = "Unsupported JSON Schema keyword 'not': negation schemas cannot be converted to PyArrow"
        return raise_spec_error("E7", msg, error_type=error_type, error_by_code=error_by_code)
    if "$dynamicRef" in node or "$recursiveRef" in node:
        msg = "Unresolved or unsupported dynamic reference keyword '$dynamicRef' in JSON Schema"
        return raise_spec_error("E9", msg, error_type=error_type, error_by_code=error_by_code)

    has_constraint = any(k in node for k in CONSTRAINING_KEYWORDS) or isinstance(node.get("additionalProperties"), dict)
    if not has_constraint:
        msg = f"Unconstrained schema without type or structural keywords is not supported: {node!r}"
        return raise_spec_error("E4", msg, error_type=error_type, error_by_code=error_by_code)

    has_fixed_props = bool(node.get("properties")) or any(
        k in node for k in ("allOf", "anyOf", "oneOf", "dependentSchemas", "then", "else")
    )
    has_map_schema = isinstance(node.get("additionalProperties"), dict) or bool(node.get("patternProperties"))
    if has_fixed_props and has_map_schema:
        msg = "Mixed fixed properties and dynamic map (additionalProperties/patternProperties) is not supported"
        return raise_spec_error("E6", msg, error_type=error_type, error_by_code=error_by_code)
    return node


def check_no_forbidden_types_in_recursive_cycle(
    ref_name: str,
    target: Mapping[str, Any],
    ctx: StrictBuildContext,
) -> None:
    """Raise ``E10`` if a recursive ``$defs`` cycle contains non-JSON-serializable types.

    :param ref_name: Name of the recursive ``$defs`` entry.
    :type ref_name: str
    :param target: Target schema mapping for ``ref_name``.
    :type target: Mapping[str, Any]
    :param ctx: Active strict build context carrying ``defs``, ``forbidden_recursive_formats``, and error classes.
    :type ctx: StrictBuildContext
    """
    visited_refs: set[str] = {ref_name}
    stack: list[object] = [target]
    while stack:
        current = stack.pop()
        if not isinstance(current, Mapping):
            continue
        fmt = current.get("format")
        encoding = current.get("contentEncoding")
        if (isinstance(fmt, str) and fmt in ctx.forbidden_recursive_formats) or encoding in {"base64", "base16"}:
            msg = (
                f"Non-JSON-serializable type (format={fmt!r}, "
                f"contentEncoding={encoding!r}) is not supported "
                f"inside recursive $defs cycle {ref_name!r}"
            )
            raise_spec_error("E10", msg, error_type=ctx.error_type, error_by_code=ctx.error_by_code)
        if "$ref" in current and isinstance(current["$ref"], str):
            child_ref_name, child_target = resolve_ref(
                current["$ref"], ctx.defs, error_type=ctx.error_type, error_by_code=ctx.error_by_code
            )
            if child_ref_name not in visited_refs:
                visited_refs.add(child_ref_name)
                stack.append(child_target)
        stack.extend(iter_child_subschemas(current))


def has_null_possibility(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
    _visited: frozenset[str] = frozenset(),
    _alias_visited: frozenset[str] = frozenset(),
) -> bool:
    """Return ``True`` if ``node`` or any of its referenced or combinator branches allows ``null``.

    :param node: JSON Schema node mapping.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :param _visited: Visited ``$defs`` names along the active ``$ref`` and combinator chain.
    :type _visited: frozenset[str]
    :param _alias_visited: Visited ``$defs`` names along an unbroken ``$ref`` alias chain.
    :type _alias_visited: frozenset[str]
    :return: ``True`` if ``null`` is permitted.
    :rtype: bool
    """
    if "$ref" in node:
        ref_name, target = resolve_ref(node["$ref"], defs, error_type=error_type, error_by_code=error_by_code)
        if ref_name in _alias_visited:
            msg = f"Circular $ref alias cycle detected at {node['$ref']!r}"
            return raise_spec_error("E9", msg, error_type=error_type, error_by_code=error_by_code)
        if ref_name not in _visited and has_null_possibility(
            target,
            defs,
            error_type=error_type,
            error_by_code=error_by_code,
            _visited=_visited | {ref_name},
            _alias_visited=_alias_visited | {ref_name},
        ):
            return True
    node_type = node.get("type")
    if node_type == "null" or (isinstance(node_type, list) and "null" in node_type):
        return True
    if ("const" in node and node["const"] is None) or (
        "enum" in node and isinstance(node["enum"], list) and None in node["enum"]
    ):
        return True
    for kw in ("anyOf", "oneOf", "allOf"):
        if kw in node and isinstance(node[kw], list):
            for branch in node[kw]:
                if isinstance(branch, Mapping) and (
                    is_null_branch(branch)
                    or has_null_possibility(
                        branch,
                        defs,
                        error_type=error_type,
                        error_by_code=error_by_code,
                        _visited=_visited,
                    )
                ):
                    return True
    return False


def get_inherited_description(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
    _visited: frozenset[str] = frozenset(),
    _alias_visited: frozenset[str] = frozenset(),
) -> str | None:
    """Extract the local or ``$ref``-inherited ``description`` string for a schema node.

    :param node: JSON Schema node mapping.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :param _visited: Visited ``$defs`` names along the active ``$ref`` and combinator chain.
    :type _visited: frozenset[str]
    :param _alias_visited: Visited ``$defs`` names along an unbroken ``$ref`` alias chain.
    :type _alias_visited: frozenset[str]
    :return: Description string or ``None``.
    :rtype: str | None
    """
    if isinstance(node.get("description"), str) and node["description"]:
        return node["description"]
    if "$ref" in node:
        ref_name, target = resolve_ref(node["$ref"], defs, error_type=error_type, error_by_code=error_by_code)
        if ref_name in _alias_visited:
            msg = f"Circular $ref alias cycle detected at {node['$ref']!r}"
            return raise_spec_error("E9", msg, error_type=error_type, error_by_code=error_by_code)
        if ref_name in _visited:
            return None
        return get_inherited_description(
            target,
            defs,
            error_type=error_type,
            error_by_code=error_by_code,
            _visited=_visited | {ref_name},
            _alias_visited=_alias_visited | {ref_name},
        )
    for kw in ("anyOf", "oneOf"):
        if kw in node and isinstance(node[kw], list):
            non_null = [b for b in node[kw] if isinstance(b, Mapping) and not is_null_branch(b)]
            if len(non_null) == 1 and "$ref" in non_null[0]:
                return get_inherited_description(
                    non_null[0],
                    defs,
                    error_type=error_type,
                    error_by_code=error_by_code,
                    _visited=_visited,
                )
    return None
