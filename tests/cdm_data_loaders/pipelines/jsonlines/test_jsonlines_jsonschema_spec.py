"""Specification tests for the JSONLines (JSON Schema) reader, settings, and ingestion pipeline.

Cases, rules, and error fixtures are declared in manifests under ``specs/pipelines/jsonlines_jsonschema``
(``rules.yaml``, ``spec/*/case.yaml``, and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/jsonlines_jsonschema_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import json
import shutil
import sys
import types
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _pytest.mark.structures import ParameterSet
from dlt.sources.filesystem import filesystem

import cdm_data_loaders.pipelines.jsonlines.base as jsonl_base_module
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline import (
    _make_resolve_error,
    run_jsonlines_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import (
    JSONLINES_JSONSCHEMA_ERRORS,
    JSONSCHEMA_PIPELINE_NAME,
    JsonlJsonschemaError,
    JsonlJsonschemaIngestSettings,
)
from cdm_data_loaders.readers.jsonlines import make_page_router, stream_jsonl_lines
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from scripts.gen_converter_spec import JSONLINES_JSONSCHEMA_PIPELINE, render
from tests.cdm_data_loaders.pipelines.helpers import fetch_all_duckdb, run_with_duckdb_pipeline
from tests.conftest import PIPELINE_SPECS_DIR
from tests.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases, load_rules
from tests.helpers import make_cts_settings
from tests.pipeline_spec import load_pipeline_cases
from tests.pyarrow_spec_helpers import (
    assert_schema_matches_expected,
    json_default,
    load_expected_table,
    load_fixture_attr,
    load_module,
)

JSONL_SPEC_DIR = (PIPELINE_SPECS_DIR / "jsonlines_jsonschema").resolve()
RULES = load_rules(JSONL_SPEC_DIR / "rules.yaml")
CASES: list[Case] = load_cases(JSONL_SPEC_DIR / "spec")
ERROR_CASES: list[ErrorCase] = load_error_cases(JSONL_SPEC_DIR / "spec" / "errors" / "manifest.yaml")
ERRORS_BY_CODE: dict[str, type[JsonlJsonschemaError]] = {cls.code: cls for cls in JSONLINES_JSONSCHEMA_ERRORS}

RUN_PARAMS: list[ParameterSet] = [
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
]
ERROR_PARAMS: list[ParameterSet] = [pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES]


def _load_json_records(path: Path) -> list[dict[str, Any]]:
    """Load a list of JSON record objects from disk."""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        msg = f"Expected a list of JSON records in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _load_module_attr(py_file: Path, attr: str) -> dict[str, Any]:
    """Import ``py_file`` (rebinding ``expected_schema`` when needed) and return ``attr``."""
    previous_schema_mod = sys.modules.get("expected_schema")
    schema_sibling = py_file.parent / "expected_schema.py"
    if py_file.stem != "expected_schema" and schema_sibling.is_file():
        sys.modules["expected_schema"] = load_module(schema_sibling)
    try:
        return load_fixture_attr(py_file, attr, dict)
    finally:
        if previous_schema_mod is None:
            sys.modules.pop("expected_schema", None)
        else:
            sys.modules["expected_schema"] = previous_schema_mod


def _register_entity_schemas_module(
    monkeypatch: pytest.MonkeyPatch,
    mod_name: str,
    entity_schemas: dict[str, Any],
) -> str:
    """Create an in-memory module with ``ENTITY_SCHEMAS`` and register it in ``sys.modules``."""
    mod = types.ModuleType(mod_name)
    mod.ENTITY_SCHEMAS = entity_schemas  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, mod_name, mod)
    return mod_name


def _stage_run_settings(
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
    case: Case,
    run: Run,
) -> JsonlJsonschemaIngestSettings:
    """Copy a spec run's JSONL data file(s) into ``input_dir`` and build ``JsonlJsonschemaIngestSettings``."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    run_params = dict(run.params)
    entity_type: str = run_params.pop("entity_type")
    extra_entities: dict[str, str] = run_params.pop("extra_entities", {})

    if run.data.is_dir():
        shutil.copytree(run.data, input_dir, dirs_exist_ok=True)
    else:
        entity_dir = input_dir / entity_type
        entity_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(run.data, entity_dir / run.data.name)

    if "entity_schemas" not in run_params:
        schemas_dict: dict[str, Any] = {
            entity_type: json.loads(case.schema.read_text(encoding="utf-8")),
        }
        for extra_table, extra_schema_rel in extra_entities.items():
            extra_schema_path = (case.directory / extra_schema_rel).resolve()
            schemas_dict[extra_table] = json.loads(extra_schema_path.read_text(encoding="utf-8"))
        mod_name = f"_jsonl_spec_schemas_{case.name}_{run.id}"
        run_params["entity_schemas"] = _register_entity_schemas_module(monkeypatch, mod_name, schemas_dict)

    defaults: dict[str, Any] = {
        "dataset_name": "jsonl_spec_dataset",
        "input_dir": str(input_dir),
        "loader_file_format": "parquet",
        "use_destination": dlt_destination_config,
        **run_params,
    }
    return make_cts_settings(JsonlJsonschemaIngestSettings, tmp_path, **defaults)


# Manifest consistency


def test_manifests_pass_every_case_rule_exists() -> None:
    """Every rule referenced by a case manifest is defined in rules.yaml."""
    all_cases = load_pipeline_cases(JSONL_SPEC_DIR)
    unknown = {f"{case.suite}/{case.name}": sorted(set(case.rules) - RULES.rules.keys()) for case in all_cases}
    assert {name: ids for name, ids in unknown.items() if ids} == {}


def test_manifests_pass_every_rule_is_exercised() -> None:
    """Every rule in rules.yaml is exercised by at least one case."""
    all_cases = load_pipeline_cases(JSONL_SPEC_DIR)
    used = {rule_id for case in all_cases for rule_id in case.rules}
    assert sorted(RULES.rules.keys() - used) == []


def test_manifests_pass_every_case_file_exists() -> None:
    """Every file or directory referenced by a case or error manifest exists."""
    files = [p for case in CASES for p in (case.schema, case.expected_schema)]
    files += [p for case in CASES for run in case.runs for p in (run.expected_json, run.expected_table)]
    files += [run.expected_rejected for case in CASES for run in case.runs if run.expected_rejected is not None]
    files += [extra_path for case in CASES for run in case.runs for extra_path in run.extra_expected_json.values()]
    files += [err.file for err in ERROR_CASES]
    assert [str(p) for p in files if not p.is_file()] == []

    data_paths = [run.data for case in CASES for run in case.runs]
    assert [str(p) for p in data_paths if not p.exists()] == []


def test_manifests_pass_every_error_fixture_listed() -> None:
    """Every JSON file in spec/errors is listed in the error manifest."""
    listed = {err.file for err in ERROR_CASES}
    on_disk = set((JSONL_SPEC_DIR / "spec" / "errors").glob("*.json"))
    assert sorted(str(p) for p in on_disk - listed) == []


def test_manifests_pass_error_codes_unique_and_covered() -> None:
    """Error codes are unique, every manifest entry uses a known code, and every code has a fixture."""
    assert len(ERRORS_BY_CODE) == len(JSONLINES_JSONSCHEMA_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in JSONLINES_JSONSCHEMA_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


@pytest.mark.docs
def test_specification_document_pass_up_to_date() -> None:
    """The committed specification document matches the output of scripts/gen_converter_spec.py."""
    assert JSONLINES_JSONSCHEMA_PIPELINE.output.read_text(encoding="utf-8") == render(JSONLINES_JSONSCHEMA_PIPELINE), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )


# Schema resolution


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_jsonl_jsonschema_settings_schema_resolution_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JsonlJsonschemaIngestSettings resolves the expected PyArrow schema(s) in schema_map."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, monkeypatch, case, run)
    assert settings.schema_map is not None

    if run.extra_expected_json:
        expected_schemas: dict[str, pa.Schema] = _load_module_attr(case.expected_schema, "EXPECTED_SCHEMAS")
        for table_name, expected_pa_schema in expected_schemas.items():
            actual_pa_schema = settings.schema_map.registry.get(table_name)
            assert actual_pa_schema is not None
            assert actual_pa_schema.equals(expected_pa_schema, check_metadata=True)
    else:
        entity_type = run.params["entity_type"]
        actual_pa_schema = settings.schema_map.registry.get(entity_type)
        assert actual_pa_schema is not None
        assert_schema_matches_expected(actual_pa_schema, case.expected_schema, run.schema_attr)


# Per-entity streaming, JSON Schema validation, pre-processing, and PyArrow coercion


def _collect_routed_jsonl_batches(settings: JsonlJsonschemaIngestSettings) -> dict[str, list[pa.Table]]:
    """Run file pre-processors, JSONL line streaming, and page routing across all configured entity tables."""
    batches_by_table: dict[str, list[pa.Table]] = {}
    for table_name in sorted(settings.schema_registry.registry):
        validated_schema = settings.schema_registry.registry[table_name]
        router = make_page_router(
            table_name,
            settings.buffer_size,
            _make_resolve_error(validated_schema),
            table_transform=settings.get_table_transform(table_name),
            schema_map=settings.schema_map,
        )
        file_items = sorted(
            filesystem(bucket_url=str(settings.input_dir), file_glob=f"{table_name}/{settings.file_glob}"),
            key=lambda item: item["relative_path"],
        )
        preprocessors = settings.get_file_preprocessors(table_name)
        valid_items = []
        for item in file_items:
            curr_item = item
            failed = False
            for preprocess_fn in preprocessors:
                try:
                    curr_item = preprocess_fn(curr_item)
                except Exception as exc:  # noqa: BLE001
                    rejected_row = {
                        "table_name": table_name,
                        "source_file": item["relative_path"],
                        "line_no": None,
                        "raw_record": None,
                        "error_detail": str(exc),
                    }
                    rejected_pa = pa.Table.from_pylist([rejected_row], schema=REJECTED_PYARROW_SCHEMA)
                    for routed in router(rejected_pa):
                        batches_by_table.setdefault(routed.meta.table_name, []).append(routed.data)
                    failed = True
                    break
            if not failed and curr_item is not None:
                valid_items.append(curr_item)

        for page in stream_jsonl_lines(iter(valid_items), settings.buffer_size):
            for routed in router(page):
                assert isinstance(routed.data, pa.Table)
                batches_by_table.setdefault(routed.meta.table_name, []).append(routed.data)
    return batches_by_table


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_stream_and_route_jsonl_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Streaming, pre-processing, and routing JSONL records yields PyArrow tables matching expected_table and expected JSON."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, monkeypatch, case, run)
    batches_by_table = _collect_routed_jsonl_batches(settings)

    entity_type = run.params["entity_type"]
    expected_table: pa.Table = load_expected_table(run.expected_table, run.table_attr)
    expected_rows = _load_json_records(run.expected_json)
    target_batches = batches_by_table.get(entity_type, [])
    actual_table = (
        pa.concat_tables(target_batches) if target_batches else pa.Table.from_pylist([], schema=expected_table.schema)
    )
    assert actual_table.schema.equals(expected_table.schema, check_metadata=True)
    if expected_rows:
        assert actual_table.equals(expected_table, check_metadata=True)
    assert json.loads(json.dumps(actual_table.to_pylist(), default=json_default)) == expected_rows

    sink = pa.BufferOutputStream()
    pq.write_table(actual_table, sink)
    if expected_rows:
        assert pq.read_table(pa.BufferReader(sink.getvalue())).equals(expected_table)

    if run.extra_expected_json:
        expected_pa_tables: dict[str, pa.Table] = _load_module_attr(run.expected_table, "EXPECTED_TABLES")
        for extra_table_name, extra_json_path in run.extra_expected_json.items():
            expected_pa = expected_pa_tables[extra_table_name]
            extra_batches = batches_by_table.get(extra_table_name, [])
            actual_pa = (
                pa.concat_tables(extra_batches)
                if extra_batches
                else pa.Table.from_pylist([], schema=expected_pa.schema)
            )
            assert actual_pa.schema.equals(expected_pa.schema, check_metadata=True)
            assert actual_pa.equals(expected_pa, check_metadata=True)
            assert json.loads(json.dumps(actual_pa.to_pylist(), default=json_default)) == _load_json_records(
                extra_json_path
            )

            extra_sink = pa.BufferOutputStream()
            pq.write_table(actual_pa, extra_sink)
            assert pq.read_table(pa.BufferReader(extra_sink.getvalue())).equals(expected_pa)

    rejected_batches = batches_by_table.get(REJECTED_TABLE_NAME, [])
    if run.expected_rejected is None:
        assert rejected_batches == []
    else:
        assert rejected_batches
        rejected_table = pa.concat_tables(rejected_batches)
        assert rejected_table.schema.equals(REJECTED_PYARROW_SCHEMA)
        assert rejected_table.to_pylist() == _load_json_records(run.expected_rejected)


# End-to-end pipeline execution


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_run_jsonlines_pipeline_end_to_end_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_jsonlines_pipeline loads all expected entity, transformed, and _rejected rows into the destination."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, monkeypatch, case, run)

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        jsonl_base_module,
        lambda: run_jsonlines_pipeline(settings),
        expected_pipeline_name=JSONSCHEMA_PIPELINE_NAME,
        expected_dataset_name=settings.dataset_name,
        pipeline_name_prefix=f"jsonl_spec_{case.name}_{run.id}",
    )

    entity_type = run.params["entity_type"]
    expected_rows = _load_json_records(run.expected_json)
    if expected_rows:
        ((loaded_count,),) = fetch_all_duckdb(
            load_info.pipeline,
            f"SELECT COUNT(*) FROM {entity_type}",  # noqa: S608
        )
        assert loaded_count == len(expected_rows)
    else:
        assert entity_type not in load_info.pipeline.default_schema.tables

    for extra_table_name, extra_json_path in run.extra_expected_json.items():
        expected_extra = _load_json_records(extra_json_path)
        ((extra_count,),) = fetch_all_duckdb(
            load_info.pipeline,
            f"SELECT COUNT(*) FROM {extra_table_name}",  # noqa: S608
        )
        assert extra_count == len(expected_extra)

    if "missing_entity" in settings.schema_registry.registry:
        assert "missing_entity" not in load_info.pipeline.default_schema.tables

    if run.expected_rejected is None:
        assert REJECTED_TABLE_NAME not in load_info.pipeline.default_schema.tables
    else:
        expected_rejected = _load_json_records(run.expected_rejected)
        rejected_rows = fetch_all_duckdb(
            load_info.pipeline,
            "SELECT table_name, source_file, line_no, raw_record, error_detail FROM _rejected ORDER BY source_file, line_no, error_detail",
        )
        actual_rejected = [
            {
                "table_name": r[0],
                "source_file": r[1],
                "line_no": r[2],
                "raw_record": r[3],
                "error_detail": r[4],
            }
            for r in rejected_rows
        ]
        assert actual_rejected == sorted(
            expected_rejected,
            key=lambda item: (item["source_file"], item["line_no"] or 0, item["error_detail"]),
        )


# Errors


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_jsonl_jsonschema_errors_fail_error_manifest(
    err: ErrorCase,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each error fixture raises the expected JsonlJsonschemaError subclass when initializing JsonlJsonschemaIngestSettings."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    loaded_json = json.loads(err.file.read_text(encoding="utf-8"))
    if err.params.get("raw_registry"):
        entity_schemas = loaded_json
    else:
        entity_key = err.params.get("entity_key", "items")
        entity_schemas = {entity_key: loaded_json}

    mod_name = _register_entity_schemas_module(
        monkeypatch,
        f"_jsonl_err_{err.code}_{err.file.stem}",
        entity_schemas,
    )
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        make_cts_settings(
            JsonlJsonschemaIngestSettings,
            tmp_path,
            dataset_name="jsonl_error_dataset",
            entity_schemas=mod_name,
            input_dir=str(input_dir),
            use_destination=dlt_destination_config,
        )
