"""Reference resolution, node validation, and cycle checks for strict JSON Schema."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, raise_spec_error
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry
from cdm_data_loaders.converters.sources.json_schema.common import extract_extensions
from cdm_data_loaders.validation.jsonschema.metaschema import CUSTOM_META_SCHEMA_URI

SUPPORTED_SCHEMA_DIALECTS: Final[frozenset[str]] = frozenset(
    {
        "https://json-schema.org/draft/2020-12/schema",
        CUSTOM_META_SCHEMA_URI,
    }
)

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


@dataclass(frozen=True, slots=True)
class StrictBuildContext:
    """Immutable traversal context for strict JSON Schema IR lowering."""

    defs: Mapping[str, Any]
    ref_counts: Mapping[str, int]
    max_recursion_depth: int
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
    error_type: type[ConversionError] = ConversionError
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def with_ref(self, ref_name: str) -> "StrictBuildContext":
        """Return a new context with the recursion counter for ``ref_name`` incremented by 1.

        :param ref_name: ``$defs`` definition name being entered.
        :type ref_name: str
        :return: Updated traversal context.
        :rtype: StrictBuildContext
        """
        return StrictBuildContext(
            defs=self.defs,
            ref_counts={**self.ref_counts, ref_name: self.ref_counts.get(ref_name, 0) + 1},
            max_recursion_depth=self.max_recursion_depth,
            extension_registry=self.extension_registry,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )


@dataclass(frozen=True, slots=True)
class StrictNodeMeta:
    """Node-level path, nullability, and facet payloads for strict IR lowering."""

    path: tuple[str | int, ...] = ()
    nullable: bool = False
    annotations: Mapping[str, Any] = field(default_factory=frozendict)
    extensions: Mapping[str, Any] = field(default_factory=frozendict)
    constraints: Mapping[str, Any] = field(default_factory=frozendict)


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
    defs: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
) -> None:
    """Raise ``E10`` if a recursive ``$defs`` cycle contains non-JSON-serializable types.

    :param ref_name: Name of the recursive ``$defs`` entry.
    :type ref_name: str
    :param target: Target schema mapping for ``ref_name``.
    :type target: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    """
    visited_refs: set[str] = {ref_name}
    stack: list[object] = [target]
    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            if current.get("format") == "date" or current.get("contentEncoding") in {"base64", "base16"}:
                msg = (
                    f"Non-JSON-serializable type (format={current.get('format')!r}, "
                    f"contentEncoding={current.get('contentEncoding')!r}) is not supported "
                    f"inside recursive $defs cycle {ref_name!r}"
                )
                raise_spec_error("E10", msg, error_type=error_type, error_by_code=error_by_code)
            if "$ref" in current and isinstance(current["$ref"], str):
                child_ref_name, child_target = resolve_ref(
                    current["$ref"], defs, error_type=error_type, error_by_code=error_by_code
                )
                if child_ref_name not in visited_refs:
                    visited_refs.add(child_ref_name)
                    stack.append(child_target)
            stack.extend(v for k, v in current.items() if k != "$defs")
        elif isinstance(current, list):
            stack.extend(current)


def has_null_possibility(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
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
    :return: ``True`` if ``null`` is permitted.
    :rtype: bool
    """
    if "$ref" in node:
        _, target = resolve_ref(node["$ref"], defs, error_type=error_type, error_by_code=error_by_code)
        if has_null_possibility(target, defs, error_type=error_type, error_by_code=error_by_code):
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
                    or has_null_possibility(branch, defs, error_type=error_type, error_by_code=error_by_code)
                ):
                    return True
    return False


def get_inherited_description(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
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
    :return: Description string or ``None``.
    :rtype: str | None
    """
    if isinstance(node.get("description"), str) and node["description"]:
        return node["description"]
    if "$ref" in node:
        _, target = resolve_ref(node["$ref"], defs, error_type=error_type, error_by_code=error_by_code)
        return get_inherited_description(target, defs, error_type=error_type, error_by_code=error_by_code)
    for kw in ("anyOf", "oneOf"):
        if kw in node and isinstance(node[kw], list):
            non_null = [b for b in node[kw] if isinstance(b, Mapping) and not is_null_branch(b)]
            if len(non_null) == 1 and "$ref" in non_null[0]:
                return get_inherited_description(non_null[0], defs, error_type=error_type, error_by_code=error_by_code)
    return None
