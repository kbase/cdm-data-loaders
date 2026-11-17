"""Node-kind classification and combinator merging for strict JSON Schema."""

import copy
import json
from collections.abc import Mapping
from typing import Any

from cdm_data_loaders.converters.core.errors import ConversionError, raise_spec_error
from cdm_data_loaders.converters.core.extensions import ExtensionRegistry
from cdm_data_loaders.converters.sources.json_schema.strict_refs import (
    StrictBuildContext,
    check_no_forbidden_types_in_recursive_cycle,
    is_null_branch,
    resolve_ref,
    validate_node,
)
from cdm_data_loaders.validation.jsonschema.keywords import (
    DICT_OF_SCHEMAS_KEYWORDS,
    strip_ref_target_identity,
)


def merge_property_definitions(prop_defs: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple schema definitions for the same property name.

    :param prop_defs: List of schema dictionaries for one property.
    :type prop_defs: list[dict[str, Any]]
    :return: Merged property schema dictionary.
    :rtype: dict[str, Any]
    """
    if len(prop_defs) == 1:
        return prop_defs[0]
    first_json = json.dumps(prop_defs[0], sort_keys=True)
    if all(json.dumps(p, sort_keys=True) == first_json for p in prop_defs[1:]):
        return prop_defs[0]
    merged: dict[str, Any] = {"anyOf": prop_defs}
    for prop_schema in prop_defs:
        if "description" in prop_schema:
            merged["description"] = prop_schema["description"]
            break
    return merged


def merge_ref_target_and_siblings(
    target: Mapping[str, Any],
    node: Mapping[str, Any],
    *,
    exclude_keys: frozenset[str] = frozenset({"$ref"}),
) -> dict[str, Any]:
    """Strip resource identity keywords from ``target`` and merge sibling keywords from ``node``.

    :param target: Resolved ``$defs`` target schema mapping.
    :type target: Mapping[str, Any]
    :param node: Referencing JSON Schema node mapping.
    :type node: Mapping[str, Any]
    :param exclude_keys: Keys on ``node`` to omit when merging siblings.
    :type exclude_keys: frozenset[str]
    :return: Merged schema dictionary.
    :rtype: dict[str, Any]
    """
    merged: dict[str, Any] = strip_ref_target_identity(copy.deepcopy(dict(target)))
    for key, value in node.items():
        if key in exclude_keys:
            continue
        if key in DICT_OF_SCHEMAS_KEYWORDS and isinstance(merged.get(key), Mapping) and isinstance(value, Mapping):
            sub_map: dict[str, Any] = dict(merged[key])
            for sub_key, sub_val in value.items():
                existing = sub_map.get(sub_key)
                if isinstance(existing, dict) and isinstance(sub_val, dict):
                    sub_map[sub_key] = merge_property_definitions([existing, copy.deepcopy(sub_val)])
                else:
                    sub_map[sub_key] = copy.deepcopy(sub_val)
            merged[key] = sub_map
        elif key == "required" and isinstance(value, list):
            prior_req = merged["required"] if isinstance(merged.get("required"), list) else []
            merged["required"] = list(dict.fromkeys([*prior_req, *value]))
        elif key == "allOf" and isinstance(merged.get("allOf"), list) and isinstance(value, list):
            merged["allOf"] = [*merged["allOf"], *copy.deepcopy(value)]
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def classify_resolved_node_kind(
    node: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
) -> str:
    """Classify a validated, non-``$ref`` schema node into its structural shape.

    :param node: Validated non-``$ref`` JSON Schema node.
    :type node: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :return: Structural shape classification string.
    :rtype: str
    """
    if "properties" not in node and "type" not in node:
        if any(kw in node for kw in ("anyOf", "oneOf")):
            return "union"
        if "allOf" in node:
            return "object"
    node_type = node.get("type")
    if isinstance(node_type, list):
        non_null = [t for t in node_type if t != "null"]
        if len(non_null) == 1:
            node_type = non_null[0]
        elif any(t in {"object", "array"} for t in non_null):
            msg = f"Incompatible branch types in multi-type union: {node_type!r}"
            raise_spec_error("E8", msg, error_type=error_type, error_by_code=error_by_code)
        else:
            return "scalar"
    if node_type == "object" or any(k in node for k in ("properties", "additionalProperties", "patternProperties")):
        has_props = bool(node.get("properties")) or any(
            k in node for k in ("allOf", "anyOf", "oneOf", "dependentSchemas", "then", "else")
        )
        has_map = isinstance(node.get("additionalProperties"), dict) or bool(node.get("patternProperties"))
        return "map" if (has_map and not has_props) else "object"
    if node_type == "array" or "items" in node or "prefixItems" in node:
        return "array"
    return "scalar"


def classify_node_kind(
    node: object,
    defs: Mapping[str, Any],
    *,
    extension_registry: ExtensionRegistry,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
    _visited: frozenset[str] = frozenset(),
) -> str:
    """Validate ``node`` and return ``'null'``, ``'object'``, ``'array'``, ``'map'``, ``'scalar'``, or ``'union'``.

    :param node: Raw JSON Schema node.
    :type node: object
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param extension_registry: Extension registry used for ``x-*`` validation.
    :type extension_registry: ExtensionRegistry
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :param _visited: Visited ``$defs`` names along the active ``$ref`` alias chain.
    :type _visited: frozenset[str]
    :return: Structural shape classification string.
    :rtype: str
    """
    validated = validate_node(
        node,
        extension_registry=extension_registry,
        error_type=error_type,
        error_by_code=error_by_code,
    )
    if is_null_branch(validated):
        return "null"
    if "$ref" in validated:
        ref_name, target = resolve_ref(validated["$ref"], defs, error_type=error_type, error_by_code=error_by_code)
        if ref_name in _visited:
            msg = f"Circular $ref alias cycle detected at {validated['$ref']!r}"
            return raise_spec_error("E9", msg, error_type=error_type, error_by_code=error_by_code)
        merged = merge_ref_target_and_siblings(target, validated)
        return classify_node_kind(
            merged,
            defs,
            extension_registry=extension_registry,
            error_type=error_type,
            error_by_code=error_by_code,
            _visited=_visited | {ref_name},
        )
    return classify_resolved_node_kind(validated, error_type=error_type, error_by_code=error_by_code)


def extract_choice_branches_into_object(
    branches: list[Any],
    kw: str,
    ctx: StrictBuildContext,
    raw_props: dict[str, list[tuple[dict[str, Any], StrictBuildContext]]],
    scalar_branches: list[dict[str, Any]],
) -> set[str]:
    """Extract properties, common required fields, and scalar branches from an ``anyOf`` or ``oneOf`` list.

    :param branches: List of combinator branch schemas.
    :type branches: list[Any]
    :param kw: Combinator keyword (``'anyOf'`` or ``'oneOf'``).
    :type kw: str
    :param ctx: Active strict build context.
    :type ctx: StrictBuildContext
    :param raw_props: Accumulator mapping property names to ``(schema, ctx)`` pairs.
    :type raw_props: dict[str, list[tuple[dict[str, Any], StrictBuildContext]]]
    :param scalar_branches: Accumulator list of scalar branch schemas.
    :type scalar_branches: list[dict[str, Any]]
    :return: Set of property names required across all object branches.
    :rtype: set[str]
    """
    non_null = [b for b in branches if not (isinstance(b, Mapping) and is_null_branch(b))]
    if not non_null:
        msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow type"
        return raise_spec_error("E11", msg, error_type=ctx.error_type, error_by_code=ctx.error_by_code)
    branch_req_sets: list[set[str]] = []
    has_scalar = False
    for branch in non_null:
        branch_kind = classify_node_kind(
            branch,
            ctx.defs,
            extension_registry=ctx.extension_registry,
            error_type=ctx.error_type,
            error_by_code=ctx.error_by_code,
        )
        if branch_kind == "scalar":
            has_scalar = True
            scalar_branches.append(branch)
        elif branch_kind == "object":
            b_props, b_req, b_scalars = extract_object_parts_with_refs(branch, ctx)
            for prop_name, (prop_schema, prop_ctx) in b_props.items():
                raw_props.setdefault(prop_name, []).append((prop_schema, prop_ctx))
            branch_req_sets.append(b_req)
            scalar_branches.extend(b_scalars)
        else:
            msg = f"Incompatible branch shape {branch_kind!r} in {kw} combinator"
            return raise_spec_error("E8", msg, error_type=ctx.error_type, error_by_code=ctx.error_by_code)
    if branch_req_sets and not has_scalar:
        return set.intersection(*branch_req_sets)
    return set()


def _finalize_merged_object_props(
    raw_props: Mapping[str, list[tuple[dict[str, Any], StrictBuildContext]]],
) -> dict[str, tuple[dict[str, Any], StrictBuildContext]]:
    """Collapse accumulated per-property schema definitions into single merged ``(schema, ctx)`` entries.

    :param raw_props: Mapping from property name to ordered ``(schema, ctx)`` pairs.
    :type raw_props: Mapping[str, list[tuple[dict[str, Any], StrictBuildContext]]]
    :return: Mapping from property name to ``(merged_schema, first_ctx)``.
    :rtype: dict[str, tuple[dict[str, Any], StrictBuildContext]]
    """
    return {
        prop_name: (merge_property_definitions([item[0] for item in items]), items[0][1])
        for prop_name, items in raw_props.items()
    }


def _extract_ref_object_parts(
    validated: dict[str, Any],
    ctx: StrictBuildContext,
) -> tuple[dict[str, tuple[dict[str, Any], StrictBuildContext]], set[str], list[dict[str, Any]]]:
    """Extract object properties, required set, and scalar union branches for a ``$ref`` object node.

    :param validated: Validated object node containing ``$ref``.
    :type validated: dict[str, Any]
    :param ctx: Active strict build context.
    :type ctx: StrictBuildContext
    :return: Tuple of ``(merged_props, required_set, scalar_branches)``.
    :rtype: tuple[dict[str, tuple[dict[str, Any], StrictBuildContext]], set[str], list[dict[str, Any]]]
    """
    ref_name, target = resolve_ref(
        validated["$ref"], ctx.defs, error_type=ctx.error_type, error_by_code=ctx.error_by_code
    )
    if ctx.ref_counts.get(ref_name, 0) >= 1:
        check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx)
    next_ctx = ctx.with_ref(ref_name)
    target_props, target_req, target_scalars = extract_object_parts_with_refs(
        strip_ref_target_identity(copy.deepcopy(target)),
        next_ctx,
    )
    sibling_keys = {k: v for k, v in validated.items() if k not in {"$ref", "description"}}
    if not sibling_keys:
        return target_props, target_req, target_scalars

    sibling_node: dict[str, Any] = {"type": "object", **copy.deepcopy(sibling_keys)}
    sib_props, sib_req, sib_scalars = extract_object_parts_with_refs(sibling_node, ctx)

    raw_props: dict[str, list[tuple[dict[str, Any], StrictBuildContext]]] = {
        prop_name: [item] for prop_name, item in target_props.items()
    }
    for prop_name, item in sib_props.items():
        raw_props.setdefault(prop_name, []).append(item)
    return _finalize_merged_object_props(raw_props), target_req | sib_req, [*target_scalars, *sib_scalars]


def extract_object_parts_with_refs(
    node: dict[str, Any],
    ctx: StrictBuildContext,
) -> tuple[dict[str, tuple[dict[str, Any], StrictBuildContext]], set[str], list[dict[str, Any]]]:
    """Extract ordered merged property definitions, required set, and scalar union branches for an object node.

    :param node: Object JSON Schema node dictionary.
    :type node: dict[str, Any]
    :param ctx: Active strict build context.
    :type ctx: StrictBuildContext
    :return: Tuple of ``(merged_props, required_set, scalar_branches)``.
    :rtype: tuple[dict[str, tuple[dict[str, Any], StrictBuildContext]], set[str], list[dict[str, Any]]]
    """
    validated = validate_node(
        node,
        extension_registry=ctx.extension_registry,
        error_type=ctx.error_type,
        error_by_code=ctx.error_by_code,
    )
    if "$ref" in validated:
        return _extract_ref_object_parts(validated, ctx)

    raw_props: dict[str, list[tuple[dict[str, Any], StrictBuildContext]]] = {}
    required: set[str] = set(validated.get("required", []))
    scalar_branches: list[dict[str, Any]] = []

    for sub in validated.get("allOf", []):
        sub_kind = classify_node_kind(
            sub,
            ctx.defs,
            extension_registry=ctx.extension_registry,
            error_type=ctx.error_type,
            error_by_code=ctx.error_by_code,
        )
        if sub_kind != "object":
            msg = f"Incompatible non-object branch shape {sub_kind!r} in unmerged 'allOf' combinator"
            return raise_spec_error("E8", msg, error_type=ctx.error_type, error_by_code=ctx.error_by_code)
        sub_props, sub_req, sub_scalars = extract_object_parts_with_refs(sub, ctx)
        for prop_name, item in sub_props.items():
            raw_props.setdefault(prop_name, []).append(item)
        required |= sub_req
        scalar_branches.extend(sub_scalars)

    for prop_name, prop_schema in validated.get("properties", {}).items():
        validated_prop = validate_node(
            prop_schema,
            extension_registry=ctx.extension_registry,
            error_type=ctx.error_type,
            error_by_code=ctx.error_by_code,
        )
        raw_props.setdefault(prop_name, []).append((validated_prop, ctx))

    for kw in ("anyOf", "oneOf"):
        if kw in validated:
            required |= extract_choice_branches_into_object(
                validated[kw],
                kw,
                ctx,
                raw_props,
                scalar_branches,
            )

    conditional_subschemas = [
        *validated.get("dependentSchemas", {}).values(),
        *(validated[kw] for kw in ("then", "else") if kw in validated),
    ]
    for cond_schema in conditional_subschemas:
        c_props, _, _ = extract_object_parts_with_refs(cond_schema, ctx)
        for prop_name, item in c_props.items():
            raw_props.setdefault(prop_name, []).append(item)

    return _finalize_merged_object_props(raw_props), required, scalar_branches
