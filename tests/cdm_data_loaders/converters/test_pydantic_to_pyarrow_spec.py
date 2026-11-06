"""Specification tests for Pydantic-to-PyArrow schema generation and model/record coercion.

Cases, rules and error fixtures are declared in manifests under ``tests/data/pydantic`` (``rules.yaml``,
``*/case.yaml`` and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/pydantic_to_pyarrow_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import datetime as dt
import json
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Final, Literal, NewType, NotRequired, Required, TypedDict

import pyarrow as pa
import pytest
from _pytest.mark.structures import ParameterSet
from frozendict import frozendict
from pydantic import AwareDatetime, BaseModel, Field, NaiveDatetime, RootModel, computed_field

from cdm_data_loaders.converters.pydantic_to_pyarrow import (
    PYDANTIC_TO_PYARROW_ERRORS,
    EmptyModelError,
    InvalidDecimalConstraintsError,
    InvalidPydanticModelError,
    NonStringMapKeyError,
    NonStructRootError,
    PydanticToPyArrow,
    PydanticToPyArrowError,
    RecursiveNonJsonTypeError,
    UnconstrainedFieldError,
    UnsupportedFieldTypeError,
    coerce_record_for_pyarrow,
    generate_pyarrow_schema,
    resolve_scalar_type,
)
from scripts.gen_converter_spec import PYDANTIC_TO_PYARROW, render
from tests.converter_spec import Case, ErrorCase, RuleCatalogue, Run, load_cases, load_error_cases, load_rules
from tests.pyarrow_spec_helpers import (
    assert_expected_table_valid,
    assert_rows_and_table_match_expected,
    assert_schema_matches_expected,
    load_fixture_attr,
)

PYDANTIC_DATA_DIR: Final[Path] = Path(__file__).resolve().parents[2] / "data" / "pydantic"
RULES: Final[RuleCatalogue] = load_rules(PYDANTIC_DATA_DIR / "rules.yaml")
CASES: Final[tuple[Case, ...]] = tuple(
    load_cases(PYDANTIC_DATA_DIR / "spec") + load_cases(PYDANTIC_DATA_DIR / "examples")
)
ERROR_CASES: Final[tuple[ErrorCase, ...]] = tuple(
    load_error_cases(PYDANTIC_DATA_DIR / "spec" / "errors" / "manifest.yaml")
)
ERRORS_BY_CODE: Final[frozendict[str, type[PydanticToPyArrowError]]] = frozendict(
    {cls.code: cls for cls in PYDANTIC_TO_PYARROW_ERRORS}
)

RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
)
ERROR_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES
)


def _load_model_class(path: Path) -> Any:  # noqa: ANN401
    """Load the ``MODEL`` class attribute from a ``schema.py`` fixture."""
    return load_fixture_attr(path, "MODEL", object)


def _load_json_records(path: Path) -> list[dict[str, Any]]:
    """Load a list of JSON record objects from disk."""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        msg = f"Expected a list of JSON records in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _max_recursion_depth(run: Run) -> int:
    """Return the configured ``max_recursion_depth`` parameter for ``run``."""
    return int(run.params.get("max_recursion_depth", 2))


def _by_alias(run: Run) -> bool:
    """Return the configured ``by_alias`` parameter for ``run``."""
    return bool(run.params.get("by_alias", False))


def test_manifests_pass_consistency_and_spec_up_to_date() -> None:
    """Verify rule coverage, fixture file existence, error code coverage, and specification document freshness."""
    unknown = {f"{case.suite}/{case.name}": sorted(set(case.rules) - RULES.rules.keys()) for case in CASES}
    assert {name: ids for name, ids in unknown.items() if ids} == {}

    used_rules = {rule_id for case in CASES for rule_id in case.rules}
    assert sorted(RULES.rules.keys() - used_rules) == []

    paths = [p for case in CASES for p in (case.schema, case.expected_schema)]
    paths += [p for case in CASES for run in case.runs for p in (run.data, run.expected_json, run.expected_table)]
    paths += [err.file for err in ERROR_CASES]
    assert [str(p) for p in paths if not p.is_file()] == []

    listed_errors = {err.file for err in ERROR_CASES}
    on_disk_errors = set((PYDANTIC_DATA_DIR / "spec" / "errors").glob("*.py"))
    assert sorted(str(p) for p in on_disk_errors - listed_errors) == []

    assert len(ERRORS_BY_CODE) == len(PYDANTIC_TO_PYARROW_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in PYDANTIC_TO_PYARROW_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []

    assert PYDANTIC_TO_PYARROW.output.read_text(encoding="utf-8") == render(PYDANTIC_TO_PYARROW), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_fixtures_pass_records_and_expected_table_validate(case: Case, run: Run) -> None:
    """Verify input records validate against the Pydantic model and expected table matches expected JSON and schema."""
    model_cls = _load_model_class(case.schema)
    records = _load_json_records(run.data)
    for record in records:
        model_cls.model_validate(record)
    assert_expected_table_valid(
        records,
        run.expected_json,
        run.expected_table,
        expected_count=run.expected_count,
        schema_attr=run.schema_attr,
        table_attr=run.table_attr,
    )


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_generate_pyarrow_schema_pass_cases(case: Case, run: Run) -> None:
    """Verify generate_pyarrow_schema and PydanticToPyArrow produce the exact expected PyArrow schema and table."""
    model_cls = _load_model_class(case.schema)
    depth = _max_recursion_depth(run)
    by_alias = _by_alias(run)

    pa_schema = generate_pyarrow_schema(
        model_cls,
        max_recursion_depth=depth,
        by_alias=by_alias,
    )
    assert_schema_matches_expected(pa_schema, case.expected_schema, run.schema_attr)

    converter = PydanticToPyArrow(max_recursion_depth=depth, by_alias=by_alias)
    assert converter.convert(model_cls).equals(pa_schema, check_metadata=True)
    empty_table = converter.convert_to_table(model_cls)
    assert empty_table.num_rows == 0
    assert empty_table.schema.equals(pa_schema, check_metadata=True)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_coerce_record_for_pyarrow_pass_cases(case: Case, run: Run) -> None:
    """Verify coerce_record_for_pyarrow produces identical expected tables from dicts and validated BaseModels."""
    model_cls = _load_model_class(case.schema)
    depth = _max_recursion_depth(run)
    by_alias = _by_alias(run)
    pa_schema = generate_pyarrow_schema(model_cls, max_recursion_depth=depth, by_alias=by_alias)
    raw_records = _load_json_records(run.data)

    rows_from_dict = [
        coerce_record_for_pyarrow(record, pa_schema, model=model_cls, by_alias=by_alias) for record in raw_records
    ]
    assert_rows_and_table_match_expected(
        rows_from_dict, pa_schema, run.expected_json, run.expected_table, table_attr=run.table_attr
    )

    rows_from_model = [
        coerce_record_for_pyarrow(model_cls.model_validate(record), pa_schema, by_alias=by_alias)
        for record in raw_records
    ]
    assert_rows_and_table_match_expected(
        rows_from_model, pa_schema, run.expected_json, run.expected_table, table_attr=run.table_attr
    )


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_pyarrow_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Verify each error fixture raises the expected PydanticToPyArrowError subclass."""
    model_cls = _load_model_class(err.file)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_pyarrow_schema(model_cls)


@pytest.mark.parametrize(
    ("annotation", "expected_type", "expected_label"),
    [
        pytest.param(NewType("UserId", int), pa.int64(), "integer", id="newtype-int"),
        pytest.param(Literal[b"raw"], pa.binary(), "binary", id="literal-bytes"),
        pytest.param(Literal["a", "b"], pa.string(), "string", id="string-literal"),
        pytest.param(
            Annotated[Decimal, Field(max_digits=6, decimal_places=1)]
            | Annotated[Decimal, Field(max_digits=6, decimal_places=4)],
            pa.decimal128(9, 4),
            "decimal",
            id="decimal-union-compatible",
        ),
        pytest.param(
            Annotated[Decimal, Field(max_digits=38, decimal_places=0)]
            | Annotated[Decimal, Field(max_digits=38, decimal_places=10)],
            pa.float64(),
            "decimal",
            id="decimal-union-overflow-to-float64",
        ),
        pytest.param(
            AwareDatetime | NaiveDatetime,
            pa.timestamp("us", tz="UTC"),
            "date-time | date-time-naive",
            id="timestamp-union-widens-to-utc",
        ),
    ],
)
def test_resolve_scalar_type_pass_special_cases(
    annotation: Any,  # noqa: ANN401
    expected_type: pa.DataType,
    expected_label: str,
) -> None:
    """Verify resolve_scalar_type handles NewType, Literals, Decimal union widening, and timestamp widening."""
    dtype, label = resolve_scalar_type(annotation)
    assert dtype == expected_type
    assert label == expected_label


@pytest.mark.parametrize(
    ("annotation", "expected_exc", "match"),
    [
        pytest.param(None, UnsupportedFieldTypeError, "NoneType", id="standalone-none"),
        pytest.param(type(None), UnsupportedFieldTypeError, "NoneType", id="standalone-nonetype"),
        pytest.param(Any, UnconstrainedFieldError, "Unconstrained", id="standalone-any"),
        pytest.param(object, UnconstrainedFieldError, "Unconstrained", id="standalone-object"),
        pytest.param(
            Annotated[Decimal, Field(max_digits=10, decimal_places=-1)],
            InvalidDecimalConstraintsError,
            "decimal128 scale",
            id="negative-decimal-places",
        ),
        pytest.param(
            Annotated[Decimal, Field(max_digits=4, decimal_places=6)],
            InvalidDecimalConstraintsError,
            "decimal128 scale",
            id="decimal-places-exceeds-max-digits",
        ),
    ],
)
def test_resolve_scalar_type_fail_invalid_annotations(
    annotation: Any,  # noqa: ANN401
    expected_exc: type[PydanticToPyArrowError],
    match: str,
) -> None:
    """Verify resolve_scalar_type raises appropriate errors for invalid scalar annotations."""
    with pytest.raises(expected_exc, match=match):
        resolve_scalar_type(annotation)


def test_resolve_scalar_type_pass_and_fail_enum_variants() -> None:
    """Verify resolve_scalar_type handles float/bool/bytes enums and rejects empty or unsupported enums."""

    class FloatEnum(float, Enum):
        HALF = 0.5

    class BoolBytesEnum(Enum):
        FLAG = True
        RAW = b"x"

    class EmptyEnum(Enum):
        pass

    class BadObjEnum(Enum):
        OBJ = object()

    assert resolve_scalar_type(FloatEnum) == (pa.float64(), "number")
    assert resolve_scalar_type(BoolBytesEnum) == (pa.string(), "boolean | binary")
    assert resolve_scalar_type(Literal[FloatEnum.HALF]) == (pa.float64(), "number")

    with pytest.raises(UnsupportedFieldTypeError, match="Empty Enum"):
        resolve_scalar_type(EmptyEnum)
    with pytest.raises(UnsupportedFieldTypeError, match="Unsupported Enum member"):
        resolve_scalar_type(BadObjEnum)


def test_generate_pyarrow_schema_pass_edge_cases() -> None:
    """Verify schema generation for parameterized type aliases, TypedDict Required/NotRequired, and tuple unpack."""
    type BoxedList[T] = list[T]
    type PairTuple = tuple[int, str]
    type KeyA = Literal["a"]
    type KeyB = Literal["b"]

    class DetailedDict(TypedDict, total=False):
        req_field: Required[str]
        opt_field: NotRequired[int]

    @dataclass
    class DefaultedDataclass:
        count: int = 5
        items: list[str] = field(default_factory=list)

    class ChildA(BaseModel):
        shared: int

    class ChildB(BaseModel):
        shared: int

    class RootUnionModel(RootModel[ChildA | ChildB]):
        pass

    class EdgeCaseModel(BaseModel):
        boxed: BoxedList[int]
        aliased_tuple: PairTuple
        td: DetailedDict
        dc: DefaultedDataclass
        same_list_union: list[int] | set[int]
        literal_key_map: dict[Literal["a", "b"], int]
        union_key_map: dict[KeyA | KeyB, int]
        excluded_field: str = Field(default="skip", exclude=True)

        @computed_field
        @property
        def computed_val(self) -> int:
            return 1

    pa_schema = generate_pyarrow_schema(EdgeCaseModel)
    assert pa_schema.field("boxed").type == pa.list_(pa.field("item", pa.int64(), nullable=False))
    assert pa_schema.field("aliased_tuple").type == pa.struct(
        [
            pa.field("item_0", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
            pa.field("item_1", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        ]
    )
    td_struct = pa_schema.field("td").type
    assert td_struct.field("req_field").nullable is False
    assert td_struct.field("opt_field").nullable is True
    assert "excluded_field" not in pa_schema.names
    assert "computed_val" in pa_schema.names

    root_union_schema = generate_pyarrow_schema(RootUnionModel)
    assert root_union_schema.field("shared").type == pa.int64()


def test_generate_pyarrow_schema_fail_additional_error_branches() -> None:
    """Verify schema generation error branches for invalid models, tuples, maps, and recursive types."""
    type CyclicAlias = str | list[CyclicAlias]
    type GenericCyclic[T] = T | list[GenericCyclic[T]]

    class GenericModel[T](BaseModel):
        item: T

    class GenericRoot[T](RootModel[T]):
        pass

    class BareBaseModelField(BaseModel):
        sub: BaseModel

    class UnparameterizedGenericField(BaseModel):
        sub: GenericModel

    class UnparameterizedGenericRootField(BaseModel):
        sub: GenericRoot

    class EmptyTupleField(BaseModel):
        empty: tuple[()]

    class IntKeyedMapModel(BaseModel):
        counts: dict[int, str]

    class AnyKeyedMapModel(BaseModel):
        counts: dict[Any, str]

    class NullOnlyModel(BaseModel):
        empty_val: None = None

    class CyclicAliasModel(BaseModel):
        tree: CyclicAlias

    class GenericCyclicAliasModel(BaseModel):
        tree: GenericCyclic[int]

    class RootUnionWithScalar(RootModel[BareBaseModelField | int]):
        pass

    for model_cls, exc_cls in (
        (GenericModel, UnconstrainedFieldError),
        (BareBaseModelField, InvalidPydanticModelError),
        (UnparameterizedGenericField, UnconstrainedFieldError),
        (UnparameterizedGenericRootField, UnconstrainedFieldError),
        (EmptyTupleField, EmptyModelError),
        (IntKeyedMapModel, NonStringMapKeyError),
        (AnyKeyedMapModel, UnconstrainedFieldError),
        (NullOnlyModel, UnsupportedFieldTypeError),
        (CyclicAliasModel, RecursiveNonJsonTypeError),
        (GenericCyclicAliasModel, RecursiveNonJsonTypeError),
        (RootUnionWithScalar, NonStructRootError),
    ):
        with pytest.raises(exc_cls):
            generate_pyarrow_schema(model_cls)


def test_coerce_record_for_pyarrow_pass_unvalidated_and_boundary_coercions() -> None:
    """Verify coerce_record_for_pyarrow handles unvalidated dicts, RootModels, dataclasses, and temporal/binary inputs."""

    @dataclass
    class InnerDc:
        label: str
        opt: str | None = None

    class RootScalarWrapper(RootModel[int]):
        pass

    class BoundaryModel(BaseModel):
        id: str
        count: int
        ratio: float
        flag_str: str
        raw_bin: bytes
        event_date: dt.date
        event_time: dt.time
        aware_dt: AwareDatetime
        naive_dt: NaiveDatetime
        items_deque: deque[int]
        dc: InnerDc

    pa_schema = generate_pyarrow_schema(BoundaryModel)
    aware_ts = dt.datetime(2026, 4, 10, 12, 0, 0, tzinfo=dt.UTC)
    naive_ts = aware_ts.replace(tzinfo=None)
    aware_time = dt.time(8, 30, 0, tzinfo=dt.UTC)

    raw_input: dict[str, Any] = {
        "id": "b1",
        "count": 10.0,
        "ratio": Decimal("2.5"),
        "flag_str": b"bytes-to-str",
        "raw_bin": bytearray(b"abc"),
        "event_date": aware_ts,
        "event_time": aware_time,
        "aware_dt": naive_ts,
        "naive_dt": aware_ts,
        "items_deque": deque([RootScalarWrapper(1), RootScalarWrapper(2)]),
        "dc": InnerDc(label="d1"),
        "undeclared_extra": "ignored",
    }

    expected_count = 10
    expected_ratio = 2.5
    coerced = coerce_record_for_pyarrow(raw_input, pa_schema)
    assert "undeclared_extra" not in coerced
    assert coerced["count"] == expected_count
    assert coerced["ratio"] == expected_ratio
    assert coerced["flag_str"] == "bytes-to-str"
    assert coerced["raw_bin"] == b"abc"
    assert coerced["event_date"] == dt.date(2026, 4, 10)
    assert coerced["event_time"] == dt.time(8, 30, 0)
    assert coerced["aware_dt"] == aware_ts
    assert coerced["naive_dt"] == naive_ts
    assert coerced["items_deque"] == [1, 2]
    assert coerced["dc"] == {"label": "d1"}

    table = pa.Table.from_pylist([coerced], schema=pa_schema)
    assert table.num_rows == 1
