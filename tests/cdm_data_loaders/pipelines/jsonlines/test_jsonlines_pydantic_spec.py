"""Specification and property-based tests for jsonlines_pydantic reusing pydantic_to_pyarrow converter specs."""

import datetime as dt
import io
import json
import sys
import tempfile
import types
from collections import deque
from collections.abc import Mapping
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address
from pathlib import Path
from typing import Any, Final
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _pytest.mark.structures import ParameterSet
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, Field, SecretStr, TypeAdapter, ValidationError

from cdm_data_loaders.coercion.pydantic_to_arrow import coerce_pydantic_record_for_pyarrow
from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.pipelines.jsonlines.pydantic import (
    build_entity_resource,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings
from tests.cdm_data_loaders.converters.facades.test_pydantic_to_pyarrow_pbt import (
    pydantic_raw_inputs_for_model,
)
from tests.conftest import LOCAL_FS, SCHEMA_CONVERTER_SPECS_DIR
from tests.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases
from tests.helpers import make_cts_settings, register_temp_module
from tests.pyarrow_pbt_helpers import assert_coerced_rows_roundtrip
from tests.pyarrow_spec_helpers import (
    assert_schema_matches_expected,
    load_expected_table,
    load_fixture_attr,
)

PYDANTIC_CONVERTER_SPEC_DIR: Final[Path] = (SCHEMA_CONVERTER_SPECS_DIR / "pydantic_to_pyarrow").resolve()
CASES: Final[tuple[Case, ...]] = tuple(
    load_cases(PYDANTIC_CONVERTER_SPEC_DIR / "spec") + load_cases(PYDANTIC_CONVERTER_SPEC_DIR / "examples")
)
ERROR_CASES: Final[tuple[ErrorCase, ...]] = tuple(
    load_error_cases(PYDANTIC_CONVERTER_SPEC_DIR / "spec" / "errors" / "manifest.yaml")
)

RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
)
ERROR_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES
)

ENTITY_TABLE_NAME: Final[str] = "items"
TIMEDELTA_JSON_ADAPTER: Final[TypeAdapter[dt.timedelta]] = TypeAdapter(dt.timedelta)


def _load_model_class(path: Path) -> object:
    """Load the ``MODEL`` class attribute from a ``schema.py`` or error fixture.

    :param path: Path to the Python fixture file defining ``MODEL``.
    :type path: Path
    :return: Loaded ``MODEL`` object.
    :rtype: object
    """
    return load_fixture_attr(path, "MODEL", object)


def _load_json_records(path: Path) -> list[dict[str, Any]]:
    """Load a JSON list of record dictionaries from ``path``.

    :param path: Path to the JSON array file.
    :type path: Path
    :return: List of JSON-parsed record dictionaries.
    :rtype: list[dict[str, Any]]
    """
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        msg = f"Expected a list of JSON records in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _run_settings_overrides(run: Run) -> dict[str, Any]:
    """Extract ``JsonlPydanticIngestSettings`` keyword overrides from ``run.params``.

    :param run: Converter specification run entry.
    :type run: Run
    :return: Dictionary of settings overrides for ``max_recursion_depth`` and ``by_alias``.
    :rtype: dict[str, Any]
    """
    overrides: dict[str, Any] = {}
    if "max_recursion_depth" in run.params:
        overrides["max_recursion_depth"] = int(run.params["max_recursion_depth"])
    if "by_alias" in run.params:
        overrides["by_alias"] = bool(run.params["by_alias"])
    return overrides


def stage_jsonl_run(
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
    case: Case,
    run: Run,
) -> tuple[type[BaseModel], JsonlPydanticIngestSettings]:
    """Write ``run.data`` as JSONL lines and construct ``JsonlPydanticIngestSettings`` for ``case`` and ``run``.

    :param tmp_path: Temporary test directory.
    :type tmp_path: Path
    :param dlt_destination_config: Configured dlt destination name.
    :type dlt_destination_config: str
    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param case: Converter specification case.
    :type case: Case
    :param run: Converter specification run.
    :type run: Run
    :return: Tuple of ``(model_cls, settings)``.
    :rtype: tuple[type[BaseModel], JsonlPydanticIngestSettings]
    """
    model_cls: type[BaseModel] = load_fixture_attr(case.schema, "MODEL", type)
    input_dir = tmp_path / "input"
    entity_dir = input_dir / ENTITY_TABLE_NAME
    entity_dir.mkdir(parents=True, exist_ok=True)

    records = _load_json_records(run.data)
    jsonl_text = "".join(f"{json.dumps(rec)}\n" for rec in records)
    (entity_dir / "data.jsonl").write_text(jsonl_text, encoding="utf-8")

    mod_name = register_temp_module(
        monkeypatch,
        f"_pydantic_spec_models_{case.suite}_{case.name}_{run.id}".replace("-", "_"),
        ENTITY_MODELS={ENTITY_TABLE_NAME: model_cls},
    )
    settings = make_cts_settings(
        JsonlPydanticIngestSettings,
        tmp_path,
        dataset_name="jsonl_pydantic_spec_ds",
        input_dir=str(input_dir),
        entity_models=mod_name,
        loader_file_format="parquet",
        use_destination=dlt_destination_config,
        **_run_settings_overrides(run),
    )
    return model_cls, settings


def _to_jsonl_compatible(value: object) -> object:
    """Convert raw Hypothesis-drawn Python values into JSON-serializable JSONL inputs for Pydantic validation.

    :param value: Drawn raw input value.
    :type value: object
    :return: JSON-serializable value accepted by ``json.dumps`` and ``model.model_validate``.
    :rtype: object
    """
    result: object = value
    if isinstance(value, Enum):
        result = _to_jsonl_compatible(value.value)
    elif is_dataclass(value) and not isinstance(value, type):
        result = {f.name: _to_jsonl_compatible(getattr(value, f.name)) for f in dc_fields(value)}
    elif isinstance(value, Mapping):
        result = {str(_to_jsonl_compatible(k)): _to_jsonl_compatible(v) for k, v in value.items()}
    elif isinstance(value, list | tuple | set | frozenset | deque):
        result = [_to_jsonl_compatible(v) for v in value]
    elif isinstance(value, bytes | bytearray):
        result = bytes(value).decode("utf-8", errors="ignore")
    elif isinstance(value, dt.date | dt.datetime | dt.time):
        result = value.isoformat()
    elif isinstance(value, dt.timedelta):
        result = TIMEDELTA_JSON_ADAPTER.dump_python(value, mode="json")
    elif isinstance(value, Decimal | UUID | Path | IPv4Address):
        result = str(value)
    return result


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_jsonl_pydantic_settings_pass_converter_spec_schemas(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JsonlPydanticIngestSettings resolves the exact expected PyArrow schema for every converter spec and example case."""
    _, settings = stage_jsonl_run(tmp_path, dlt_destination_config, monkeypatch, case, run)
    assert settings.schema_map is not None
    actual_schema = settings.schema_map.registry.get(ENTITY_TABLE_NAME)
    assert actual_schema is not None
    assert_schema_matches_expected(actual_schema, case.expected_schema, run.schema_attr)


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_jsonl_pydantic_settings_fail_converter_error_manifest(
    err: ErrorCase,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JsonlPydanticIngestSettings rejects every invalid model fixture in the pydantic_to_pyarrow error manifest."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    model_obj = _load_model_class(err.file)
    mod_name = register_temp_module(
        monkeypatch,
        f"_pydantic_err_{err.code}_{err.file.stem}",
        ENTITY_MODELS={ENTITY_TABLE_NAME: model_obj},
    )
    expected_exc: type[Exception] = ValidationError if err.code == "E1" else RuntimeError
    with pytest.raises(expected_exc, match=err.match):
        make_cts_settings(
            JsonlPydanticIngestSettings,
            tmp_path,
            dataset_name="jsonl_pydantic_error_ds",
            entity_models=mod_name,
            input_dir=str(input_dir),
            use_destination=dlt_destination_config,
        )


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_build_entity_resource_pass_converter_spec_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_entity_resource coerces JSONL records to the exact expected PyArrow table and Parquet output across all converter specs."""
    model_cls, settings = stage_jsonl_run(tmp_path, dlt_destination_config, monkeypatch, case, run)
    pages = list(build_entity_resource(ENTITY_TABLE_NAME, model_cls, settings))

    assert pages
    assert all(isinstance(page, pa.Table) for page in pages)
    assert not any(page.schema.equals(REJECTED_PYARROW_SCHEMA) for page in pages)

    expected_table = load_expected_table(run.expected_table, run.table_attr)
    expected_rows = _load_json_records(run.expected_json)

    actual_table = pa.concat_tables(pages)
    actual_table.validate(full=True)
    assert actual_table.num_rows == len(expected_rows)
    assert actual_table.schema.equals(expected_table.schema, check_metadata=True)
    assert actual_table.equals(expected_table, check_metadata=True)
    assert actual_table.to_pylist() == expected_table.to_pylist()

    buf = io.BytesIO()
    pq.write_table(actual_table, buf)
    buf.seek(0)
    roundtrip = pq.read_table(buf)
    assert roundtrip.num_rows == expected_table.num_rows
    assert roundtrip.to_pylist() == expected_table.to_pylist()


def test_build_entity_resource_pass_coerces_pydantic_types_after_table_transform(
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_entity_resource coerces SecretStr, frozenset, Decimal, and aliased fields via coercion.pydantic_to_arrow after table_transforms."""

    class SecretMetric(BaseModel):
        record_id: str = Field(serialization_alias="recordId")
        api_key: SecretStr
        tags: frozenset[str]
        ratio: Decimal

    def pass_through(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        return {ENTITY_TABLE_NAME: [row]}

    output_schema = pa.schema(
        [
            pa.field("recordId", pa.string(), nullable=False),
            pa.field("api_key", pa.string(), nullable=False),
            pa.field("tags", pa.list_(pa.field("item", pa.string(), nullable=False)), nullable=False),
            pa.field("ratio", pa.float64(), nullable=False),
        ]
    )

    input_dir = tmp_path / "input"
    entity_dir = input_dir / ENTITY_TABLE_NAME
    entity_dir.mkdir(parents=True)
    (entity_dir / "data.jsonl").write_text(
        '{"record_id": "r-1", "api_key": "top-secret", "tags": ["z", "a"], "ratio": "1.25"}\n',
        encoding="utf-8",
    )

    mod_name = register_temp_module(
        monkeypatch,
        "_pydantic_transform_coercion_mod",
        ENTITY_MODELS={ENTITY_TABLE_NAME: SecretMetric},
        TABLE_TRANSFORMS={ENTITY_TABLE_NAME: pass_through},
        OUTPUT_SCHEMAS={ENTITY_TABLE_NAME: output_schema},
    )
    settings = make_cts_settings(
        JsonlPydanticIngestSettings,
        tmp_path,
        dataset_name="jsonl_pydantic_transform_coercion_ds",
        input_dir=str(input_dir),
        entity_models=mod_name,
        transforms_module=mod_name,
        output_schemas_module=mod_name,
        by_alias=True,
        loader_file_format="parquet",
        use_destination=dlt_destination_config,
    )

    pages = list(build_entity_resource(ENTITY_TABLE_NAME, SecretMetric, settings))
    assert len(pages) == 1
    table = pages[0]
    assert isinstance(table, pa.Table)
    assert table.schema.equals(output_schema)
    assert table.to_pylist() == [
        {
            "recordId": "r-1",
            "api_key": "top-secret",
            "tags": ["a", "z"],
            "ratio": 1.25,
        }
    ]


@pytest.mark.parametrize(
    ("by_alias", "expected_key"),
    [
        pytest.param(False, "record_id", id="field-names"),
        pytest.param(True, "recordId", id="by-alias"),
    ],
)
def test_build_entity_resource_pass_jsonl_loader_format_respects_by_alias(
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
    by_alias: bool,
    expected_key: str,
) -> None:
    """When loader_file_format is jsonl, build_entity_resource yields dumped dicts honoring settings.by_alias."""

    class AliasedItem(BaseModel):
        record_id: str = Field(serialization_alias="recordId")

    input_dir = tmp_path / "input"
    entity_dir = input_dir / ENTITY_TABLE_NAME
    entity_dir.mkdir(parents=True)
    (entity_dir / "data.jsonl").write_text('{"record_id": "r-1"}\n', encoding="utf-8")

    mod_name = register_temp_module(
        monkeypatch,
        f"_pydantic_jsonl_alias_{by_alias}",
        ENTITY_MODELS={ENTITY_TABLE_NAME: AliasedItem},
    )
    settings = make_cts_settings(
        JsonlPydanticIngestSettings,
        tmp_path,
        dataset_name="jsonl_pydantic_jsonl_alias_ds",
        input_dir=str(input_dir),
        entity_models=mod_name,
        by_alias=by_alias,
        loader_file_format="jsonl",
        use_destination=dlt_destination_config,
    )

    items = list(build_entity_resource(ENTITY_TABLE_NAME, AliasedItem, settings))
    assert items == [{expected_key: "r-1"}]


@pytest.mark.pbt
@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
@given(data=st.data())
def test_build_entity_resource_pass_pbt_converter_spec_cases(
    case: Case,
    run: Run,
    data: st.DataObject,
) -> None:
    """Hypothesis-generated JSONL inputs for every spec/example model match coerce_pydantic_record_for_pyarrow and round-trip Parquet."""
    model_cls: type[BaseModel] = load_fixture_attr(case.schema, "MODEL", type)
    depth = int(run.params.get("max_recursion_depth", 2))
    by_alias = bool(run.params.get("by_alias", False))

    raw_drawn = data.draw(pydantic_raw_inputs_for_model(model_cls, max_depth=depth + 1))
    json_record = _to_jsonl_compatible(raw_drawn)
    encoded_line = json.dumps(json_record)
    parsed_record = json.loads(encoded_line)
    validated_instance = model_cls.model_validate(parsed_record)

    mod_name = f"_pydantic_pbt_models_{case.suite}_{case.name}_{run.id}".replace("-", "_")
    mod = types.ModuleType(mod_name)
    mod.ENTITY_MODELS = {ENTITY_TABLE_NAME: model_cls}  # type: ignore[attr-defined]

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_dir = Path(tmp_str)
        input_dir = tmp_dir / "input"
        entity_dir = input_dir / ENTITY_TABLE_NAME
        entity_dir.mkdir(parents=True, exist_ok=True)
        (entity_dir / "pbt.jsonl").write_text(f"{encoded_line}\n", encoding="utf-8")

        sys.modules[mod_name] = mod
        try:
            settings = make_cts_settings(
                JsonlPydanticIngestSettings,
                tmp_dir,
                dataset_name="jsonl_pydantic_pbt_ds",
                input_dir=str(input_dir),
                entity_models=mod_name,
                loader_file_format="parquet",
                use_destination=LOCAL_FS,
                **_run_settings_overrides(run),
            )
            assert settings.schema_map is not None
            pa_schema = settings.schema_map.registry[ENTITY_TABLE_NAME]
            pages = list(build_entity_resource(ENTITY_TABLE_NAME, model_cls, settings))
        finally:
            sys.modules.pop(mod_name, None)

    assert len(pages) == 1
    actual_table = pages[0]
    assert isinstance(actual_table, pa.Table)
    expected_row = coerce_pydantic_record_for_pyarrow(validated_instance, pa_schema, by_alias=by_alias)
    expected_table = pa.Table.from_pylist([expected_row], schema=pa_schema)
    assert actual_table.equals(expected_table, check_metadata=True)
    assert_coerced_rows_roundtrip([expected_row], pa_schema)
