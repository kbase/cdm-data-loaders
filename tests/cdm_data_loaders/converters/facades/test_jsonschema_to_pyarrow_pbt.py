"""Property-based tests for JSON Schema-to-PyArrow schema generation and record coercion."""

import copy
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from _pytest.mark.structures import ParameterSet
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy
from jsonschema import Draft202012Validator

from cdm_data_loaders.coercion.pyarrow import coerce_record_for_pyarrow
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrow,
    RecursiveNonJsonTypeError,
    generate_pyarrow_schema,
)
from tests.conftest import BASE_DIR
from tests.converter_spec import Case, Run, load_cases
from tests.pyarrow_pbt_helpers import (
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    assert_coerced_rows_roundtrip,
    base64_str_strategy,
    hex_str_strategy,
    iso_date_str_strategy,
    iso_datetime_str_strategy,
    iso_time_str_strategy,
    safe_text_strategy,
)

JSONSCHEMA_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "jsonschema_to_pyarrow").resolve()
CASES: Final[tuple[Case, ...]] = tuple(
    load_cases(JSONSCHEMA_DATA_DIR / "spec") + load_cases(JSONSCHEMA_DATA_DIR / "examples")
)

CASE_RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, case.runs[0], id=f"{case.suite}-{case.name}") for case in CASES
)


def _load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk.

    :param path: Path to the JSON file.
    :type path: Path
    :return: Parsed JSON dictionary.
    :rtype: dict[str, Any]
    """
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = f"Expected a JSON object in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _strip_crlf(text: str) -> str:
    """Strip trailing carriage returns and newlines produced by Python regex ``$`` matching.

    :param text: Generated regex string.
    :type text: str
    :return: String without trailing CR/LF characters.
    :rtype: str
    """
    return text.rstrip("\r\n")


def _string_strategy(node: Mapping[str, Any]) -> SearchStrategy[str]:
    """Build a Hypothesis strategy for a JSON Schema ``string`` node.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :return: Hypothesis strategy yielding conforming strings.
    :rtype: SearchStrategy[str]
    """
    encoding = node.get("contentEncoding")
    if encoding in {"base64", "base16"}:
        return base64_str_strategy() if encoding == "base64" else hex_str_strategy()

    format_strategies: dict[str, SearchStrategy[str]] = {
        "date": iso_date_str_strategy(),
        "date-time": iso_datetime_str_strategy(),
        "time": iso_time_str_strategy(),
        "duration": st.sampled_from(["PT1M", "PT1H30M", "P1D", "PT45S"]),
        "uuid": st.uuids().map(str),
        "uri": st.from_regex(r"https://example\.org/[a-z0-9_]{1,12}", fullmatch=True).map(_strip_crlf),
        "uint64": st.integers(min_value=0, max_value=INT64_MAX).map(str),
        "int64": st.integers(min_value=INT64_MIN, max_value=INT64_MAX).map(str),
    }
    fmt = node.get("format")
    if isinstance(fmt, str) and fmt in format_strategies:
        return format_strategies[fmt]

    min_len = int(node.get("minLength", 0))
    max_len = min(int(node.get("maxLength", 24)), max(min_len, 24))
    pattern = node.get("pattern")
    if isinstance(pattern, str):
        return (
            st.from_regex(pattern, fullmatch=True, alphabet=st.characters(min_codepoint=32, max_codepoint=126))
            .map(_strip_crlf)
            .filter(lambda s: min_len <= len(s) <= int(node.get("maxLength", 1024)))
        )
    return safe_text_strategy(min_size=min_len, max_size=max_len)


def _integer_strategy(node: Mapping[str, Any]) -> SearchStrategy[int]:
    """Build a Hypothesis strategy for a JSON Schema ``integer`` node within Arrow int32/int64 bounds.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :return: Hypothesis strategy yielding bounded integers.
    :rtype: SearchStrategy[int]
    """
    is_int32 = node.get("format") == "int32"
    low = INT32_MIN if is_int32 else INT64_MIN
    high = INT32_MAX if is_int32 else INT64_MAX
    if "minimum" in node:
        low = max(low, int(node["minimum"]))
    if "exclusiveMinimum" in node:
        low = max(low, int(node["exclusiveMinimum"]) + 1)
    if "maximum" in node:
        high = min(high, int(node["maximum"]))
    if "exclusiveMaximum" in node:
        high = min(high, int(node["exclusiveMaximum"]) - 1)
    return st.integers(min_value=low, max_value=high)


def _number_strategy(node: Mapping[str, Any]) -> SearchStrategy[float | int]:
    """Build a Hypothesis strategy for a JSON Schema ``number`` node.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :return: Hypothesis strategy yielding finite floats or exact multiples.
    :rtype: SearchStrategy[float | int]
    """
    mult = node.get("multipleOf")
    if isinstance(mult, int | float) and mult > 0:
        float_mult = float(mult)
        return (
            st.integers(min_value=-1000, max_value=1000)
            .map(lambda k: round(k * float_mult, 8))
            .filter(lambda x: (x / float_mult).is_integer())
        )
    return st.floats(allow_nan=False, allow_infinity=False, min_value=-1e12, max_value=1e12)


def _resolve_ref_target(ref: str, defs: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    """Resolve a local ``#/$defs/<name>`` reference against ``defs``.

    :param ref: JSON Reference URI string.
    :type ref: str
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :return: Tuple of ``(def_name, target_schema_dict)``.
    :rtype: tuple[str, Mapping[str, Any]]
    """
    prefix = "#/$defs/"
    if not ref.startswith(prefix):
        msg = f"Unsupported $ref in property test strategy: {ref!r}"
        raise ValueError(msg)
    def_name = ref.removeprefix(prefix)
    return def_name, defs[def_name]


def _hits_recursion_limit(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    ref_counts: Mapping[str, int],
    max_depth: int,
) -> bool:
    """Return True if ``node`` directly references a ``$defs`` entry that has reached ``max_depth``.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param ref_counts: Active reference expansion counts per ``$defs`` key.
    :type ref_counts: Mapping[str, int]
    :param max_depth: Maximum allowed reference depth.
    :type max_depth: int
    :return: True when expanding ``node`` would exceed ``max_depth``.
    :rtype: bool
    """
    ref = node.get("$ref")
    if isinstance(ref, str):
        def_name, _ = _resolve_ref_target(ref, defs)
        if ref_counts.get(def_name, 0) >= max_depth:
            return True
    return False


def _merge_object_branches(base: Mapping[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    """Merge ``properties`` and ``required`` from ``extra`` into ``base``.

    :param base: Base object schema dictionary.
    :type base: Mapping[str, Any]
    :param extra: Additional object schema constraints to merge.
    :type extra: Mapping[str, Any]
    :return: Merged object schema dictionary.
    :rtype: dict[str, Any]
    """
    merged = dict(base)
    props = {**base.get("properties", {}), **extra.get("properties", {})}
    if props:
        merged["properties"] = props
    req = list(dict.fromkeys([*base.get("required", []), *extra.get("required", [])]))
    if req:
        merged["required"] = req
    return merged


def _expand_all_of(node: Mapping[str, Any], defs: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten ``allOf`` branches into a single merged object schema node.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :return: Schema dictionary with ``allOf`` merged into ``properties`` and ``required``.
    :rtype: dict[str, Any]
    """
    if "allOf" not in node:
        return dict(node)
    merged = {k: v for k, v in node.items() if k != "allOf"}
    for branch in node["allOf"]:
        resolved = _resolve_ref_target(branch["$ref"], defs)[1] if "$ref" in branch else branch
        merged = _merge_object_branches(merged, _expand_all_of(resolved, defs))
    return merged


def _conditional_branches(node: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    """Split an ``if``/``then``/``else`` object schema into two concrete branch schemas.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :return: Two branch dictionaries if ``if`` is present, otherwise ``None``.
    :rtype: list[dict[str, Any]] | None
    """
    if "if" not in node:
        return None
    base = {k: v for k, v in node.items() if k not in {"if", "then", "else"}}
    if_node = node["if"]
    then_branch = _merge_object_branches(_merge_object_branches(base, if_node), node.get("then", {}))
    else_branch = _merge_object_branches(base, node.get("else", {}))
    for prop_name, cond_schema in if_node.get("properties", {}).items():
        if "const" in cond_schema and prop_name in else_branch.get("properties", {}):
            forbidden = cond_schema["const"]
            orig_prop = dict(else_branch["properties"][prop_name])
            if "enum" in orig_prop:
                orig_prop["enum"] = [v for v in orig_prop["enum"] if v != forbidden]
                else_branch["properties"] = {**else_branch["properties"], prop_name: orig_prop}
    return [then_branch, else_branch]


def _array_strategy(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    ref_counts: Mapping[str, int],
    max_depth: int,
) -> SearchStrategy[list[Any]]:
    """Build a Hypothesis strategy for a JSON Schema ``array`` node (list or closed ``prefixItems`` tuple).

    :param node: JSON Schema array node.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param ref_counts: Active reference expansion counts per ``$defs`` key.
    :type ref_counts: Mapping[str, int]
    :param max_depth: Maximum allowed reference depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding lists.
    :rtype: SearchStrategy[list[Any]]
    """
    prefix_items = node.get("prefixItems")
    if isinstance(prefix_items, list):
        min_items = int(node.get("minItems", len(prefix_items)))
        item_strats = [_node_strategy(sub, defs, ref_counts, max_depth) for sub in prefix_items]

        @st.composite
        def _build_tuple(draw: st.DrawFn) -> list[Any]:
            length = draw(st.integers(min_value=min_items, max_value=len(item_strats)))
            return [draw(item_strats[idx]) for idx in range(length)]

        return _build_tuple()

    items_schema = node.get("items")
    if not isinstance(items_schema, Mapping) or _hits_recursion_limit(items_schema, defs, ref_counts, max_depth):
        return st.just([])

    min_size = int(node.get("minItems", 0))
    max_size = min(int(node.get("maxItems", 3)), max(min_size, 3))
    elem_strat = _node_strategy(items_schema, defs, ref_counts, max_depth)
    if node.get("uniqueItems"):
        return st.lists(
            elem_strat, min_size=min_size, max_size=max_size, unique_by=lambda x: json.dumps(x, sort_keys=True)
        )
    return st.lists(elem_strat, min_size=min_size, max_size=max_size)


def _map_object_strategy(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    ref_counts: Mapping[str, int],
    max_depth: int,
) -> SearchStrategy[dict[str, Any]]:
    """Build a Hypothesis strategy for a dynamic map ``object`` (``additionalProperties`` or ``patternProperties``).

    :param node: JSON Schema map object node.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param ref_counts: Active reference expansion counts per ``$defs`` key.
    :type ref_counts: Mapping[str, int]
    :param max_depth: Maximum allowed reference depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding string-keyed map dictionaries.
    :rtype: SearchStrategy[dict[str, Any]]
    """
    pattern_props = node.get("patternProperties")
    if isinstance(pattern_props, Mapping) and pattern_props:
        pattern, val_schema = next(iter(pattern_props.items()))
        key_strat = st.from_regex(
            pattern,
            fullmatch=True,
            alphabet=st.characters(min_codepoint=48, max_codepoint=122),
        ).map(_strip_crlf)
        if isinstance(node.get("propertyNames"), Mapping):
            min_k = int(node["propertyNames"].get("minLength", 1))
            max_k = int(node["propertyNames"].get("maxLength", 64))
            key_strat = key_strat.filter(lambda s: min_k <= len(s) <= max_k)
        val_strat = _node_strategy(val_schema, defs, ref_counts, max_depth)
        return st.dictionaries(key_strat, val_strat, max_size=3)

    add_props = node.get("additionalProperties")
    if isinstance(add_props, Mapping):
        key_strat = safe_text_strategy(min_size=1, max_size=12)
        val_strat = _node_strategy(add_props, defs, ref_counts, max_depth)
        return st.dictionaries(key_strat, val_strat, max_size=3)

    return st.just({})


def _object_strategy(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    ref_counts: Mapping[str, int],
    max_depth: int,
) -> SearchStrategy[dict[str, Any]]:
    """Build a Hypothesis strategy for a fixed-property or map JSON Schema ``object`` node.

    :param node: JSON Schema object node.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param ref_counts: Active reference expansion counts per ``$defs`` key.
    :type ref_counts: Mapping[str, int]
    :param max_depth: Maximum allowed reference depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding conforming object dictionaries.
    :rtype: SearchStrategy[dict[str, Any]]
    """
    expanded = _expand_all_of(node, defs)
    cond_branches = _conditional_branches(expanded)
    if cond_branches is not None:
        return st.one_of([_object_strategy(b, defs, ref_counts, max_depth) for b in cond_branches])

    properties = dict(expanded.get("properties", {}))
    dep_schemas = expanded.get("dependentSchemas", {})
    if isinstance(dep_schemas, Mapping):
        for dep_sub in dep_schemas.values():
            if isinstance(dep_sub, Mapping):
                properties.update(dep_sub.get("properties", {}))

    if not properties:
        return _map_object_strategy(expanded, defs, ref_counts, max_depth)

    required_keys = set(expanded.get("required", []))
    dep_required: Mapping[str, list[str]] = expanded.get("dependentRequired", {})

    req_strats = {
        k: _node_strategy(prop_schema, defs, ref_counts, max_depth)
        for k, prop_schema in properties.items()
        if k in required_keys
    }
    opt_candidates = {
        k: prop_schema
        for k, prop_schema in properties.items()
        if k not in required_keys and not _hits_recursion_limit(prop_schema, defs, ref_counts, max_depth)
    }

    @st.composite
    def _build_obj(draw: st.DrawFn) -> dict[str, Any]:
        result: dict[str, Any] = {k: draw(strat) for k, strat in req_strats.items()}
        opt_keys = list(opt_candidates.keys())
        if opt_keys:
            max_pick = min(len(opt_keys), 4)
            chosen_keys = draw(st.lists(st.sampled_from(opt_keys), max_size=max_pick, unique=True))
            for k in chosen_keys:
                result[k] = draw(_node_strategy(opt_candidates[k], defs, ref_counts, max_depth))
        for trigger, needed_list in dep_required.items():
            if trigger in result:
                for needed in needed_list:
                    if needed not in result and needed in properties:
                        result[needed] = draw(_node_strategy(properties[needed], defs, ref_counts, max_depth))
        return result

    return _build_obj()


def _combinator_strategy(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    ref_counts: Mapping[str, int],
    max_depth: int,
) -> SearchStrategy[Any] | None:
    """Build a strategy when ``node`` contains ``anyOf`` or ``oneOf`` branches.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param ref_counts: Active reference expansion counts per ``$defs`` key.
    :type ref_counts: Mapping[str, int]
    :param max_depth: Maximum allowed reference depth.
    :type max_depth: int
    :return: Combined branch strategy or ``None`` if no combinator keyword is present.
    :rtype: SearchStrategy[Any] | None
    """
    branches = node.get("anyOf") or node.get("oneOf")
    if not isinstance(branches, list):
        return None
    has_parent_props = "properties" in node or "required" in node
    base_parent = {k: v for k, v in node.items() if k not in {"anyOf", "oneOf"}}
    branch_strats: list[SearchStrategy[Any]] = []
    for raw_branch in branches:
        if _hits_recursion_limit(raw_branch, defs, ref_counts, max_depth):
            continue
        branch = _resolve_ref_target(raw_branch["$ref"], defs)[1] if "$ref" in raw_branch else raw_branch
        if has_parent_props and (branch.get("type") == "object" or "properties" in branch):
            merged_branch = _merge_object_branches(base_parent, branch)
            branch_strats.append(_node_strategy(merged_branch, defs, ref_counts, max_depth))
        else:
            branch_strats.append(_node_strategy(raw_branch, defs, ref_counts, max_depth))
    return st.one_of(branch_strats)


def _node_strategy(
    node: Mapping[str, Any],
    defs: Mapping[str, Any],
    ref_counts: Mapping[str, int],
    max_depth: int,
) -> SearchStrategy[Any]:
    """Build a Hypothesis strategy for any supported Draft 2020-12 JSON Schema node.

    :param node: JSON Schema node dictionary.
    :type node: Mapping[str, Any]
    :param defs: Root ``$defs`` mapping.
    :type defs: Mapping[str, Any]
    :param ref_counts: Active reference expansion counts per ``$defs`` key.
    :type ref_counts: Mapping[str, int]
    :param max_depth: Maximum allowed reference depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding values valid against ``node``.
    :rtype: SearchStrategy[Any]
    """
    if "$ref" in node:
        def_name, target = _resolve_ref_target(node["$ref"], defs)
        next_counts = {**ref_counts, def_name: ref_counts.get(def_name, 0) + 1}
        return _node_strategy(target, defs, next_counts, max_depth)

    if "const" in node or "enum" in node:
        return st.just(node["const"]) if "const" in node else st.sampled_from(node["enum"])

    comb = _combinator_strategy(node, defs, ref_counts, max_depth)
    if comb is not None:
        return comb

    node_type = node.get("type")
    if isinstance(node_type, list):
        return st.one_of([_node_strategy({**node, "type": t}, defs, ref_counts, max_depth) for t in node_type])

    scalar_dispatch: dict[str, SearchStrategy[Any]] = {
        "null": st.none(),
        "boolean": st.booleans(),
        "integer": _integer_strategy(node),
        "number": _number_strategy(node),
        "string": _string_strategy(node),
        "array": _array_strategy(node, defs, ref_counts, max_depth),
    }
    if isinstance(node_type, str) and node_type in scalar_dispatch:
        return scalar_dispatch[node_type]
    if node_type == "object" or "properties" in node or "allOf" in node:
        return _object_strategy(node, defs, ref_counts, max_depth)

    msg = f"Unsupported JSON Schema node in strategy builder: {node!r}"
    raise ValueError(msg)


def json_records_for_schema(schema: Mapping[str, Any], *, max_depth: int = 3) -> SearchStrategy[dict[str, Any]]:
    """Build a Hypothesis strategy generating valid top-level record dicts for a Draft 2020-12 JSON Schema.

    :param schema: Root Draft 2020-12 JSON Schema dictionary.
    :type schema: Mapping[str, Any]
    :param max_depth: Maximum recursion depth for ``$defs`` cycles.
    :type max_depth: int
    :return: Hypothesis strategy yielding top-level record dictionaries.
    :rtype: SearchStrategy[dict[str, Any]]
    """
    defs = schema.get("$defs", {})
    return _node_strategy(schema, defs, {}, max_depth)


@pytest.mark.pbt
@pytest.mark.parametrize(("case", "run"), CASE_RUN_PARAMS)
@given(data=st.data())
def test_coerce_record_for_pyarrow_pbt_spec_cases(case: Case, run: Run, data: st.DataObject) -> None:
    """Generated records for every spec/example JSON Schema validate, coerce, and round-trip through Parquet."""
    raw_schema = _load_json_object(case.schema)
    depth = int(run.params.get("max_recursion_depth", 2))
    pa_schema = generate_pyarrow_schema(raw_schema, max_recursion_depth=depth)
    validator = Draft202012Validator(raw_schema)

    record = data.draw(json_records_for_schema(raw_schema, max_depth=depth + 1))
    validator.validate(record)

    coerced = coerce_record_for_pyarrow(record, pa_schema)
    assert_coerced_rows_roundtrip([coerced], pa_schema)

    record_with_extra = {**record, "__undeclared_extra_field__": "ignored"}
    coerced_with_extra = coerce_record_for_pyarrow(record_with_extra, pa_schema)
    assert "__undeclared_extra_field__" not in coerced_with_extra
    assert coerced_with_extra == coerced


@pytest.mark.pbt
@given(
    depth=st.integers(min_value=1, max_value=3),
    leaf_label=safe_text_strategy(min_size=1, max_size=16),
    leaf_weight=st.integers(min_value=-1000, max_value=1000),
)
def test_recursion_cutoff_pbt_json_valid(depth: int, leaf_label: str, leaf_weight: int) -> None:
    """Recursive JSON Schema trees exceeding max_recursion_depth serialize truncated subtrees as valid JSON."""
    schema_path = JSONSCHEMA_DATA_DIR / "spec" / "06_recursion_depth" / "schema.json"
    raw_schema = _load_json_object(schema_path)
    pa_schema = generate_pyarrow_schema(raw_schema, max_recursion_depth=depth)

    node: dict[str, Any] = {"label": leaf_label, "weight": leaf_weight}
    for level in range(depth + 1):
        node = {"label": f"level_{level}", "weight": level, "branch": [node]}

    record = {"id": "rec-1", "tree": node}
    Draft202012Validator(raw_schema).validate(record)

    coerced = coerce_record_for_pyarrow(record, pa_schema)
    assert_coerced_rows_roundtrip([coerced], pa_schema)

    cursor: Any = coerced["tree"]
    for _ in range(depth - 1):
        cursor = cursor["branch"][0]
    cutoff_json = cursor["branch"][0]
    assert isinstance(cutoff_json, str)
    parsed_cutoff = json.loads(cutoff_json)
    assert isinstance(parsed_cutoff, dict)
    assert "label" in parsed_cutoff


@pytest.mark.pbt
@given(
    prop_names=st.lists(
        st.from_regex(r"[a-z][a-z0-9_]{1,12}", fullmatch=True).map(_strip_crlf),
        min_size=1,
        max_size=6,
        unique=True,
    ),
    scalar_types=st.lists(
        st.sampled_from(
            [
                {"type": "string"},
                {"type": "integer"},
                {"type": "number"},
                {"type": "boolean"},
                {"type": "string", "format": "date"},
                {"type": "string", "contentEncoding": "base64"},
            ]
        ),
        min_size=1,
        max_size=6,
    ),
    req_flags=st.lists(st.booleans(), min_size=1, max_size=6),
)
def test_generate_pyarrow_schema_and_codegen_pbt_roundtrip(
    prop_names: list[str],
    scalar_types: list[dict[str, str]],
    req_flags: list[bool],
) -> None:
    """Generated Draft 2020-12 schemas convert deterministically and round-trip through Python codegen."""
    properties = {name: copy.deepcopy(scalar_types[idx % len(scalar_types)]) for idx, name in enumerate(prop_names)}
    required = [name for idx, name in enumerate(prop_names) if req_flags[idx % len(req_flags)]]
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
    }
    if required:
        schema["required"] = required

    converter = JSONSchemaToPyArrow()
    pa_schema_1 = converter.convert(schema)
    pa_schema_2 = converter.convert(copy.deepcopy(schema))
    assert pa_schema_1.equals(pa_schema_2, check_metadata=True)

    py_source = converter.convert_to_python(schema)
    ns: dict[str, object] = {}
    exec(py_source, ns)  # noqa: S102
    assert isinstance(ns["SCHEMA"], pa.Schema)
    assert ns["SCHEMA"].equals(pa_schema_1, check_metadata=True)


@pytest.mark.pbt
@given(
    forbidden_leaf=st.sampled_from(
        [
            {"type": "string", "format": "date"},
            {"type": "string", "contentEncoding": "base64"},
            {"type": "string", "contentEncoding": "base16"},
        ]
    ),
    depth=st.integers(min_value=1, max_value=4),
)
def test_recursive_non_json_type_pbt_rejected(forbidden_leaf: dict[str, str], depth: int) -> None:
    """Any recursive $defs cycle containing a date or binary leaf raises RecursiveNonJsonTypeError at any depth."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"root": {"$ref": "#/$defs/Node"}},
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {"bad": forbidden_leaf, "next": {"$ref": "#/$defs/Node"}},
            }
        },
    }
    with pytest.raises(RecursiveNonJsonTypeError, match="recursive"):
        generate_pyarrow_schema(schema, max_recursion_depth=depth)
