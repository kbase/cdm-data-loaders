"""Property-based tests for Pydantic-to-PyArrow schema generation and model/record coercion."""

import base64
import datetime as dt
import json
import types
from collections import deque
from collections.abc import Mapping
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address
from pathlib import Path
from typing import Annotated, Any, Final, Literal, TypeAliasType, Union, get_args, get_origin, is_typeddict
from uuid import UUID

import pyarrow as pa
import pytest
from _pytest.mark.structures import ParameterSet
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import (
    AnyUrl,
    AwareDatetime,
    Base64Bytes,
    Base64Str,
    BaseModel,
    EmailStr,
    NaiveDatetime,
    RootModel,
    SecretBytes,
    SecretStr,
)

from cdm_data_loaders.coercion.pydantic import (
    _comparable_sort_key,
    _to_jsonable_recursive,
    coerce_record_for_pyarrow,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import (
    PydanticToPyArrow,
    generate_pyarrow_schema,
)
from cdm_data_loaders.converters.readers.pydantic import (
    BASE64_BYTES_MARKERS,
    NAIVE_DATETIME_MARKERS,
    RecursiveNonJsonTypeError,
    _collect_meta_items,
    _ensure_model_complete,
)
from tests.conftest import BASE_DIR
from tests.converter_spec import Case, Run, load_cases
from tests.pyarrow_pbt_helpers import (
    INT64_MAX,
    INT64_MIN,
    MAX_SAFE_DATE,
    MAX_SAFE_NAIVE_DT,
    MIN_SAFE_DATE,
    MIN_SAFE_NAIVE_DT,
    assert_coerced_rows_roundtrip,
    base64_str_strategy,
    safe_text_strategy,
    stripped_text_strategy,
)
from tests.pyarrow_spec_helpers import load_fixture_attr

PYDANTIC_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "pydantic_to_pyarrow").resolve()
CASES: Final[tuple[Case, ...]] = tuple(
    load_cases(PYDANTIC_DATA_DIR / "spec") + load_cases(PYDANTIC_DATA_DIR / "examples")
)

RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
)


def _load_model_class(path: Path) -> type[BaseModel]:
    """Load the ``MODEL`` class attribute from a ``schema.py`` fixture.

    :param path: Path to ``schema.py``.
    :type path: Path
    :return: Pydantic ``BaseModel`` subclass.
    :rtype: type[BaseModel]
    """
    return load_fixture_attr(path, "MODEL", type)


def _encode_utf8_base64(text: str) -> str:
    """Encode a UTF-8 string into an ASCII Base64 string for ``Base64Str`` validation.

    :param text: Input UTF-8 string.
    :type text: str
    :return: Base64-encoded ASCII string.
    :rtype: str
    """
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _extract_constraints(metadata: list[Any]) -> dict[str, Any]:
    """Extract numeric, length, and decimal constraints from Pydantic/annotated-types metadata.

    :param metadata: Sequence of annotation metadata objects.
    :type metadata: list[Any]
    :return: Dictionary of extracted constraint bounds.
    :rtype: dict[str, Any]
    """
    constraints: dict[str, Any] = {}
    for item in metadata:
        for attr in ("gt", "ge", "lt", "le", "min_length", "max_length", "max_digits", "decimal_places"):
            val = getattr(item, attr, None)
            if val is not None:
                constraints[attr] = val
    return constraints


BASE64_STR_MARKERS: Final[tuple[Any, ...]] = get_args(Base64Str)[1:] if get_origin(Base64Str) is Annotated else ()


def _unwrap_annotation(annotation: Any, extra_meta: list[Any] | None = None) -> tuple[Any, dict[str, Any]]:  # noqa: ANN401
    """Unwrap PEP 695 ``TypeAliasType`` and ``Annotated`` wrappers, collecting constraint metadata.

    :param annotation: Type annotation to unwrap.
    :type annotation: Any
    :param extra_meta: Outer field metadata list.
    :type extra_meta: list[Any] | None
    :return: Tuple of ``(base_annotation, constraints_dict)``.
    :rtype: tuple[Any, dict[str, Any]]
    """
    collected: list[Any] = []
    cur = annotation
    while True:
        if cur in {Base64Bytes, Base64Str, NaiveDatetime, AwareDatetime, EmailStr, AnyUrl}:
            break
        if isinstance(cur, TypeAliasType):
            cur = cur.__value__
            continue
        if get_origin(cur) is Annotated:
            args = get_args(cur)
            cur = args[0]
            collected.extend(_collect_meta_items(args[1:]))
            continue
        break
    if extra_meta:
        collected.extend(_collect_meta_items(extra_meta))

    if cur is str and any(any(m is item or m == item for m in BASE64_STR_MARKERS) for item in collected):
        cur = Base64Str
    elif cur is bytes and any(any(m is item or m == item for m in BASE64_BYTES_MARKERS) for item in collected):
        cur = Base64Bytes
    elif cur is dt.datetime and any(any(m is item or m == item for m in NAIVE_DATETIME_MARKERS) for item in collected):
        cur = NaiveDatetime

    return cur, _extract_constraints(collected)


def _decimal_strategy(constraints: Mapping[str, Any]) -> SearchStrategy[Decimal]:
    """Build a Hypothesis strategy for ``Decimal`` values honoring ``max_digits`` and ``decimal_places``.

    :param constraints: Extracted constraint dictionary.
    :type constraints: Mapping[str, Any]
    :return: Hypothesis strategy yielding ``Decimal`` values.
    :rtype: SearchStrategy[Decimal]
    """
    max_digits = constraints.get("max_digits")
    decimal_places = constraints.get("decimal_places")
    if isinstance(max_digits, int) and isinstance(decimal_places, int):
        max_unscaled = (10**max_digits) - 1
        return st.integers(min_value=-max_unscaled, max_value=max_unscaled).map(
            lambda k: Decimal(k).scaleb(-decimal_places)
        )
    return st.integers(min_value=-1_000_000, max_value=1_000_000).map(lambda k: Decimal(k).scaleb(-2))


def _int_strategy(constraints: Mapping[str, Any]) -> SearchStrategy[int]:
    """Build a Hypothesis strategy for 64-bit signed integers honoring ``gt``/``ge``/``lt``/``le``.

    :param constraints: Extracted constraint dictionary.
    :type constraints: Mapping[str, Any]
    :return: Hypothesis strategy yielding bounded integers.
    :rtype: SearchStrategy[int]
    """
    low = INT64_MIN
    high = INT64_MAX
    if "ge" in constraints:
        low = max(low, int(constraints["ge"]))
    if "gt" in constraints:
        low = max(low, int(constraints["gt"]) + 1)
    if "le" in constraints:
        high = min(high, int(constraints["le"]))
    if "lt" in constraints:
        high = min(high, int(constraints["lt"]) - 1)
    return st.integers(min_value=low, max_value=high)


VARIADIC_TUPLE_ARITY: Final[int] = 2

FIXED_SCALAR_STRATEGIES: Final[dict[Any, SearchStrategy[Any]]] = {
    None: st.none(),
    type(None): st.none(),
    bool: st.booleans(),
    float: st.floats(allow_nan=False, allow_infinity=False, min_value=-1e9, max_value=1e9),
    Base64Bytes: base64_str_strategy(),
    Base64Str: safe_text_strategy(min_size=0, max_size=16).map(_encode_utf8_base64),
    EmailStr: st.from_regex(r"[a-z]{3,8}@[a-z]{3,8}\.org", fullmatch=True).map(str.strip),
    AnyUrl: st.from_regex(r"https://example\.org/[a-z0-9_]{1,10}", fullmatch=True).map(str.strip),
    SecretStr: safe_text_strategy(min_size=1, max_size=16),
    SecretBytes: st.binary(max_size=16),
    bytes: st.binary(max_size=16),
    dt.date: st.dates(min_value=MIN_SAFE_DATE, max_value=MAX_SAFE_DATE),
    NaiveDatetime: st.datetimes(min_value=MIN_SAFE_NAIVE_DT, max_value=MAX_SAFE_NAIVE_DT),
    AwareDatetime: st.datetimes(min_value=MIN_SAFE_NAIVE_DT, max_value=MAX_SAFE_NAIVE_DT, timezones=st.just(dt.UTC)),
    dt.datetime: st.datetimes(min_value=MIN_SAFE_NAIVE_DT, max_value=MAX_SAFE_NAIVE_DT, timezones=st.just(dt.UTC)),
    dt.time: st.times(),
    dt.timedelta: st.timedeltas(min_value=dt.timedelta(0), max_value=dt.timedelta(days=30)),
    UUID: st.uuids(),
    Path: st.from_regex(r"/[a-z]{2,6}/[a-z0-9_]{2,8}\.[a-z]{2,4}", fullmatch=True).map(lambda s: Path(s.strip())),
    IPv4Address: st.ip_addresses(v=4),
}


def _scalar_strategy(base_tp: Any, constraints: Mapping[str, Any]) -> SearchStrategy[Any]:  # noqa: ANN401
    """Build a Hypothesis strategy for scalar Python and Pydantic types.

    :param base_tp: Unwrapped scalar type.
    :type base_tp: Any
    :param constraints: Extracted constraint dictionary.
    :type constraints: Mapping[str, Any]
    :return: Hypothesis strategy yielding scalar values.
    :rtype: SearchStrategy[Any]
    """
    if base_tp in FIXED_SCALAR_STRATEGIES:
        return FIXED_SCALAR_STRATEGIES[base_tp]
    if base_tp is int:
        return _int_strategy(constraints)
    if base_tp is Decimal:
        return _decimal_strategy(constraints)
    if base_tp is str:
        min_len = int(constraints.get("min_length", 0))
        max_len = min(int(constraints.get("max_length", 20)), max(min_len, 20))
        return (
            stripped_text_strategy(min_size=min_len, max_size=max_len)
            if min_len > 0
            else safe_text_strategy(min_size=0, max_size=max_len)
        )

    msg = f"Unsupported scalar type in Pydantic PBT strategy: {base_tp!r}"
    raise ValueError(msg)


def _model_hits_limit(
    tp: Any,  # noqa: ANN401
    model_counts: Mapping[type[Any], int],
    max_depth: int,
) -> bool:
    """Return True if ``tp`` references a ``BaseModel`` whose expansion count has reached ``max_depth``.

    :param tp: Type annotation to inspect.
    :type tp: Any
    :param model_counts: Active model expansion counts.
    :type model_counts: Mapping[type[Any], int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: True when expanding ``tp`` would exceed ``max_depth``.
    :rtype: bool
    """
    base, _ = _unwrap_annotation(tp)
    if isinstance(base, type) and issubclass(base, BaseModel):
        return model_counts.get(base, 0) >= max_depth
    origin = get_origin(base)
    if origin in {Union, types.UnionType, list, set, frozenset, tuple, deque}:
        return any(
            isinstance(arg, type) and issubclass(arg, BaseModel) and model_counts.get(arg, 0) >= max_depth
            for arg in get_args(base)
        )
    return False


def _sequence_or_set_strategy(
    origin: Any,  # noqa: ANN401
    elem_tp: Any,  # noqa: ANN401
    model_counts: Mapping[type[Any], int],
    max_depth: int,
) -> SearchStrategy[Any]:
    """Build a Hypothesis strategy for ``list``, ``set``, ``frozenset``, or ``deque`` annotations.

    :param origin: Collection origin type.
    :type origin: Any
    :param elem_tp: Element type annotation.
    :type elem_tp: Any
    :param model_counts: Active model expansion counts.
    :type model_counts: Mapping[type[Any], int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding collection instances.
    :rtype: SearchStrategy[Any]
    """
    if _model_hits_limit(elem_tp, model_counts, max_depth):
        return st.just(origin())
    elem_strat = _annotation_strategy(elem_tp, None, model_counts, max_depth)
    builders = {
        list: st.lists(elem_strat, max_size=3),
        set: st.sets(elem_strat, max_size=3),
        frozenset: st.frozensets(elem_strat, max_size=3),
        deque: st.lists(elem_strat, max_size=3).map(deque),
    }
    return builders[origin]


def _structured_type_strategy(
    base_tp: Any,  # noqa: ANN401
    model_counts: Mapping[type[Any], int],
    max_depth: int,
) -> SearchStrategy[Any] | None:
    """Build a Hypothesis strategy for ``Enum``, ``TypedDict``, or stdlib ``dataclass`` types.

    :param base_tp: Unwrapped type annotation.
    :type base_tp: Any
    :param model_counts: Active model expansion counts.
    :type model_counts: Mapping[type[Any], int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: Hypothesis strategy or ``None``.
    :rtype: SearchStrategy[Any] | None
    """
    if isinstance(base_tp, type) and issubclass(base_tp, Enum):
        return st.sampled_from(list(base_tp))

    if is_typeddict(base_tp):
        annotations = base_tp.__annotations__
        req_keys = getattr(base_tp, "__required_keys__", frozenset(annotations.keys()))
        opt_keys = getattr(base_tp, "__optional_keys__", frozenset())
        return st.fixed_dictionaries(
            {k: _annotation_strategy(annotations[k], None, model_counts, max_depth) for k in req_keys},
            optional={k: _annotation_strategy(annotations[k], None, model_counts, max_depth) for k in opt_keys},
        )

    if is_dataclass(base_tp) and isinstance(base_tp, type):
        field_strats = {f.name: _annotation_strategy(f.type, None, model_counts, max_depth) for f in dc_fields(base_tp)}
        return st.fixed_dictionaries(field_strats)

    return None


def _container_or_composite_strategy(
    base_tp: Any,  # noqa: ANN401
    model_counts: Mapping[type[Any], int],
    max_depth: int,
) -> SearchStrategy[Any] | None:
    """Build a Hypothesis strategy for unions, literals, enums, containers, dataclasses, or TypedDicts.

    :param base_tp: Unwrapped type annotation.
    :type base_tp: Any
    :param model_counts: Active model expansion counts.
    :type model_counts: Mapping[type[Any], int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: Hypothesis strategy if ``base_tp`` is a composite/container type, otherwise ``None``.
    :rtype: SearchStrategy[Any] | None
    """
    origin = get_origin(base_tp)
    args = get_args(base_tp)

    if origin is Literal:
        return st.sampled_from(list(args))

    if origin in {Union, types.UnionType}:
        allowed_args = [
            a
            for a in args
            if not (isinstance(a, type) and issubclass(a, BaseModel) and model_counts.get(a, 0) >= max_depth)
        ] or [type(None)]
        return st.one_of([_annotation_strategy(a, None, model_counts, max_depth) for a in allowed_args])

    if origin in {list, set, frozenset, deque}:
        return _sequence_or_set_strategy(origin, args[0], model_counts, max_depth)

    if origin is tuple:
        is_variadic = len(args) == VARIADIC_TUPLE_ARITY and args[1] is Ellipsis
        return (
            st.lists(_annotation_strategy(args[0], None, model_counts, max_depth), max_size=3).map(tuple)
            if is_variadic
            else st.tuples(*(_annotation_strategy(a, None, model_counts, max_depth) for a in args))
        )

    if origin in {dict, Mapping}:
        return st.dictionaries(
            _annotation_strategy(args[0], None, model_counts, max_depth),
            _annotation_strategy(args[1], None, model_counts, max_depth),
            max_size=3,
        )

    return _structured_type_strategy(base_tp, model_counts, max_depth)


def _base_model_dict_strategy(
    model_cls: type[BaseModel],
    model_counts: Mapping[type[Any], int],
    max_depth: int,
) -> SearchStrategy[Any]:
    """Build a Hypothesis strategy yielding raw input dicts valid for ``model_cls.model_validate``.

    :param model_cls: Pydantic ``BaseModel`` or ``RootModel`` subclass.
    :type model_cls: type[BaseModel]
    :param model_counts: Active model expansion counts.
    :type model_counts: Mapping[type[Any], int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding raw dicts (or unwrapped root values).
    :rtype: SearchStrategy[Any]
    """
    _ensure_model_complete(model_cls)
    if issubclass(model_cls, RootModel):
        root_annotation = model_cls.model_fields["root"].annotation
        return _annotation_strategy(root_annotation, None, model_counts, max_depth)

    next_counts = {**model_counts, model_cls: model_counts.get(model_cls, 0) + 1}
    req_fields: dict[str, SearchStrategy[Any]] = {}
    opt_fields: dict[str, SearchStrategy[Any]] = {}

    for fname, finfo in model_cls.model_fields.items():
        if finfo.exclude is True:
            continue
        if not finfo.is_required() and _model_hits_limit(finfo.annotation, next_counts, max_depth):
            continue
        f_strat = _annotation_strategy(finfo.annotation, finfo.metadata, next_counts, max_depth)
        key_name = finfo.alias if (finfo.alias and not model_cls.model_config.get("populate_by_name")) else fname
        if finfo.is_required():
            req_fields[key_name] = f_strat
        else:
            opt_fields[key_name] = f_strat

    return st.fixed_dictionaries(req_fields, optional=opt_fields)


def _annotation_strategy(
    annotation: Any,  # noqa: ANN401
    field_meta: list[Any] | None,
    model_counts: Mapping[type[Any], int],
    max_depth: int,
) -> SearchStrategy[Any]:
    """Build a Hypothesis strategy for any supported Pydantic field type annotation.

    :param annotation: Field type annotation.
    :type annotation: Any
    :param field_meta: Optional ``FieldInfo.metadata`` list.
    :type field_meta: list[Any] | None
    :param model_counts: Active model expansion counts.
    :type model_counts: Mapping[type[Any], int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: Hypothesis strategy yielding values matching ``annotation``.
    :rtype: SearchStrategy[Any]
    """
    base_tp, constraints = _unwrap_annotation(annotation, field_meta)
    if isinstance(base_tp, type) and issubclass(base_tp, BaseModel):
        return _base_model_dict_strategy(base_tp, model_counts, max_depth)

    composite = _container_or_composite_strategy(base_tp, model_counts, max_depth)
    if composite is not None:
        return composite

    return _scalar_strategy(base_tp, constraints)


def pydantic_raw_inputs_for_model(model_cls: type[BaseModel], *, max_depth: int = 3) -> SearchStrategy[dict[str, Any]]:
    """Build a Hypothesis strategy yielding raw dicts valid for ``model_cls.model_validate``.

    :param model_cls: Target Pydantic ``BaseModel`` subclass.
    :type model_cls: type[BaseModel]
    :param max_depth: Maximum recursion depth for self/mutually recursive models.
    :type max_depth: int
    :return: Hypothesis strategy yielding raw record dictionaries.
    :rtype: SearchStrategy[dict[str, Any]]
    """
    return _base_model_dict_strategy(model_cls, {}, max_depth)


@pytest.mark.pbt
@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
@given(data=st.data())
def test_coerce_record_for_pyarrow_pbt_spec_cases(case: Case, run: Run, data: st.DataObject) -> None:
    """Generated inputs for every Pydantic spec/example model validate, coerce, and round-trip through Parquet."""
    model_cls = _load_model_class(case.schema)
    depth = int(run.params.get("max_recursion_depth", 2))
    by_alias = bool(run.params.get("by_alias", False))
    pa_schema = generate_pyarrow_schema(model_cls, max_recursion_depth=depth, by_alias=by_alias)

    raw_input = data.draw(pydantic_raw_inputs_for_model(model_cls, max_depth=depth + 1))
    validated_instance = model_cls.model_validate(raw_input)

    row_from_dict = coerce_record_for_pyarrow(raw_input, pa_schema, model=model_cls, by_alias=by_alias)
    row_from_model = coerce_record_for_pyarrow(validated_instance, pa_schema, by_alias=by_alias)

    assert row_from_dict == row_from_model
    assert_coerced_rows_roundtrip([row_from_model], pa_schema)


@pytest.mark.pbt
@given(
    items=st.lists(
        st.one_of(st.integers(-1000, 1000), safe_text_strategy(min_size=1, max_size=10), st.none()),
        min_size=0,
        max_size=15,
    )
)
def test_set_coercion_sort_key_pbt_deterministic(items: list[int | str | None]) -> None:
    """_comparable_sort_key sorts heterogeneous and nullable set elements deterministically without TypeError."""
    unique_items = list(dict.fromkeys(items))
    reversed_items = unique_items[::-1]
    sorted_forward = sorted(unique_items, key=_comparable_sort_key)
    sorted_reverse = sorted(reversed_items, key=_comparable_sort_key)
    assert sorted_forward == sorted_reverse
    if None in unique_items:
        assert sorted_forward[-1] is None


@pytest.mark.pbt
@given(data=st.data(), depth=st.integers(min_value=1, max_value=3))
def test_to_jsonable_recursive_pbt_always_serializable(data: st.DataObject, depth: int) -> None:
    """_to_jsonable_recursive converts arbitrary RecursiveCladeRecord instances into valid JSON structures."""
    schema_path = PYDANTIC_DATA_DIR / "spec" / "10_bounded_recursion" / "schema.py"
    model_cls = _load_model_class(schema_path)
    raw_input = data.draw(pydantic_raw_inputs_for_model(model_cls, max_depth=depth + 2))
    instance = model_cls.model_validate(raw_input)

    jsonable = _to_jsonable_recursive(instance)
    encoded = json.dumps(jsonable)
    assert isinstance(json.loads(encoded), dict)

    pa_schema = generate_pyarrow_schema(model_cls, max_recursion_depth=depth)
    row = coerce_record_for_pyarrow(instance, pa_schema)
    assert_coerced_rows_roundtrip([row], pa_schema)


@pytest.mark.pbt
@given(
    forbidden_type=st.sampled_from([dt.date, bytes, SecretBytes, Base64Bytes]),
    depth=st.integers(min_value=1, max_value=4),
)
def test_recursive_non_json_pydantic_type_pbt_rejected(
    forbidden_type: Any,  # noqa: ANN401
    depth: int,
) -> None:
    """Recursive BaseModels containing date or binary fields raise RecursiveNonJsonTypeError at any depth."""

    class BadRecursiveNode(BaseModel):
        payload: forbidden_type  # type: ignore[valid-type]
        child: "BadRecursiveNode | None" = None

    BadRecursiveNode.model_rebuild()

    with pytest.raises(RecursiveNonJsonTypeError, match="recursive"):
        generate_pyarrow_schema(BadRecursiveNode, max_recursion_depth=depth)


@pytest.mark.pbt
@pytest.mark.parametrize(("case", "run"), RUN_PARAMS[:10])
def test_pydantic_converter_determinism_and_codegen_pbt(case: Case, run: Run) -> None:
    """PydanticToPyArrow.convert and convert_to_python produce identical schemas across all spec cases."""
    model_cls = _load_model_class(case.schema)
    depth = int(run.params.get("max_recursion_depth", 2))
    by_alias = bool(run.params.get("by_alias", False))
    converter = PydanticToPyArrow(max_recursion_depth=depth, by_alias=by_alias)

    schema_a = converter.convert(model_cls)
    schema_b = converter.convert(model_cls)
    assert schema_a.equals(schema_b, check_metadata=True)

    py_source = converter.convert_to_python(model_cls)
    ns: dict[str, object] = {}
    exec(py_source, ns)  # noqa: S102
    assert isinstance(ns["SCHEMA"], pa.Schema)
    assert ns["SCHEMA"].equals(schema_a, check_metadata=True)
