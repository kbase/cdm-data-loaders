"""Specification tests for the XSV schema converter, reader, and ingestion pipeline.

Cases, rules, and error fixtures are declared in manifests under ``specs/pipelines/xsv`` (``rules.yaml``,
``spec/*/case.yaml``, and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/xsv_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import datetime as dt
import json
import shutil
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _pytest.mark.structures import ParameterSet
from dlt.sources.filesystem import filesystem

import cdm_data_loaders.pipelines.xsv.pipeline as xsv_pipeline_module
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import convert_xsv_schema_to_pyarrow
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.pipelines.xsv.pipeline import XsvWorkPaths, _process_xsv_file, run_xsv_pipeline
from cdm_data_loaders.pipelines.xsv.settings import (
    PIPELINE_NAME,
    XSV_SCHEMA_ERRORS,
    XsvIngestSettings,
    XsvSchemaError,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import validate_jsonschema
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from scripts.gen_converter_spec import XSV_TO_PYARROW, render
from tests.cdm_data_loaders.pipelines.helpers import fetch_all_duckdb, run_with_duckdb_pipeline
from tests.conftest import PIPELINE_SPECS_DIR
from tests.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases, load_rules
from tests.helpers import make_cts_settings
from tests.pyarrow_spec_helpers import assert_schema_matches_expected, load_expected_table

XSV_DATA_DIR = (PIPELINE_SPECS_DIR / "xsv").resolve()
RULES = load_rules(XSV_DATA_DIR / "rules.yaml")
CASES: list[Case] = load_cases(XSV_DATA_DIR / "spec")
ERROR_CASES: list[ErrorCase] = load_error_cases(XSV_DATA_DIR / "spec" / "errors" / "manifest.yaml")
ERRORS_BY_CODE: dict[str, type[XsvSchemaError]] = {cls.code: cls for cls in XSV_SCHEMA_ERRORS}

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


def _normalize_pylist_for_json(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert Arrow Python values (date, datetime, Decimal) to their JSON representation."""
    normalized: list[dict[str, Any]] = []
    for row in rows:
        out_row: dict[str, Any] = {}
        for key, val in row.items():
            if isinstance(val, (dt.date, dt.datetime)):
                out_row[key] = val.isoformat()
            elif isinstance(val, Decimal):
                out_row[key] = str(val)
            else:
                out_row[key] = val
        normalized.append(out_row)
    return normalized


def _stage_run_settings(
    tmp_path: Path,
    dlt_destination_config: str,
    case: Case,
    run: Run,
) -> XsvIngestSettings:
    """Copy a spec run's data file(s) into a clean ``input_dir`` and build ``XsvIngestSettings``."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    if run.data.is_dir():
        shutil.copytree(run.data, input_dir, dirs_exist_ok=True)
        default_glob = "*"
    else:
        shutil.copy2(run.data, input_dir / run.data.name)
        default_glob = run.data.name

    defaults: dict[str, Any] = {
        "dataset_name": "xsv_spec_dataset",
        "file_glob": default_glob,
        "input_dir": str(input_dir),
        "loader_file_format": "parquet",
        "schema_file": str(case.schema),
        "table_name": "spec_table",
        "use_destination": dlt_destination_config,
        **run.params,
    }
    return make_cts_settings(XsvIngestSettings, tmp_path, **defaults)


# Manifest consistency


def test_manifests_pass_every_case_rule_exists() -> None:
    """Every rule referenced by a case manifest is defined in rules.yaml."""
    unknown = {f"{case.suite}/{case.name}": sorted(set(case.rules) - RULES.rules.keys()) for case in CASES}
    assert {name: ids for name, ids in unknown.items() if ids} == {}


def test_manifests_pass_every_rule_is_exercised() -> None:
    """Every rule in rules.yaml is exercised by at least one case."""
    used = {rule_id for case in CASES for rule_id in case.rules}
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
    on_disk = set((XSV_DATA_DIR / "spec" / "errors").glob("*.json"))
    assert sorted(str(p) for p in on_disk - listed) == []


def test_manifests_pass_error_codes_unique_and_covered() -> None:
    """Error codes are unique, every manifest entry uses a known code, and every code has a fixture."""
    assert len(ERRORS_BY_CODE) == len(XSV_SCHEMA_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in XSV_SCHEMA_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


@pytest.mark.docs
def test_specification_document_pass_up_to_date() -> None:
    """The committed specification document matches the output of scripts/gen_converter_spec.py."""
    assert XSV_TO_PYARROW.output.read_text(encoding="utf-8") == render(XSV_TO_PYARROW), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )


# Schema generation


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_convert_xsv_schema_to_pyarrow_pass_cases(case: Case, run: Run) -> None:
    """convert_xsv_schema_to_pyarrow produces the exact expected PyArrow schema."""
    validated = validate_jsonschema(case.schema)
    pa_schema = convert_xsv_schema_to_pyarrow(validated)
    assert_schema_matches_expected(pa_schema, case.expected_schema, run.schema_attr)


# Per-file cleaning, validation, reading, and PyArrow coercion


@pytest.mark.usefixtures("qsv_cmd")
@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_process_xsv_file_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
) -> None:
    """_process_xsv_file cleans, validates, and yields PyArrow tables matching expected_table and expected.json."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, case, run)
    work_paths = work_paths_factory(settings)

    file_items = sorted(
        filesystem(bucket_url=str(settings.input_dir), file_glob=settings.file_glob),
        key=lambda item: item["relative_path"],
    )
    batches_by_table: dict[str, list[pa.Table]] = {}
    for item in file_items:
        for yielded in _process_xsv_file(item, settings, work_paths):
            assert isinstance(yielded.data, pa.Table)
            batches_by_table.setdefault(yielded.meta.table_name, []).append(yielded.data)

    expected_table: pa.Table = load_expected_table(run.expected_table, run.table_attr)
    target_batches = batches_by_table.get(settings.table_name, [])
    actual_table = (
        pa.concat_tables(target_batches) if target_batches else pa.Table.from_pylist([], schema=expected_table.schema)
    )
    assert actual_table.schema.equals(expected_table.schema, check_metadata=True)
    assert actual_table.equals(expected_table, check_metadata=True)
    assert _normalize_pylist_for_json(actual_table.to_pylist()) == _load_json_records(run.expected_json)

    sink = pa.BufferOutputStream()
    pq.write_table(actual_table, sink)
    assert pq.read_table(pa.BufferReader(sink.getvalue())).equals(expected_table)

    for extra_table_name, extra_json_path in run.extra_expected_json.items():
        extra_batches = batches_by_table.get(extra_table_name, [])
        assert extra_batches, f"Expected batches for extra table {extra_table_name!r}"
        extra_table = pa.concat_tables(extra_batches)
        assert _normalize_pylist_for_json(extra_table.to_pylist()) == _load_json_records(extra_json_path)

    rejected_batches = batches_by_table.get(REJECTED_TABLE_NAME, [])
    if run.expected_rejected is None:
        assert rejected_batches == []
    else:
        assert rejected_batches
        rejected_table = pa.concat_tables(rejected_batches)
        assert rejected_table.schema.equals(REJECTED_PYARROW_SCHEMA)
        assert rejected_table.to_pylist() == _load_json_records(run.expected_rejected)


# End-to-end pipeline execution


@pytest.mark.usefixtures("qsv_cmd")
@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_run_xsv_pipeline_end_to_end_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """run_xsv_pipeline loads all expected target, extra, and _rejected rows into the destination."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, case, run)
    expected_rows = _load_json_records(run.expected_json)

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        lambda: run_xsv_pipeline(settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name=settings.dataset_name,
        pipeline_name_prefix=f"xsv_spec_{case.name}_{run.id}",
    )

    if expected_rows:
        ((loaded_count,),) = fetch_all_duckdb(
            load_info.pipeline,
            f"SELECT COUNT(*) FROM {settings.table_name}",  # noqa: S608
        )
        assert loaded_count == len(expected_rows)
    else:
        assert settings.table_name not in load_info.pipeline.default_schema.tables

    for extra_table_name, extra_json_path in run.extra_expected_json.items():
        expected_extra = _load_json_records(extra_json_path)
        ((extra_count,),) = fetch_all_duckdb(
            load_info.pipeline,
            f"SELECT COUNT(*) FROM {extra_table_name}",  # noqa: S608
        )
        assert extra_count == len(expected_extra)

    if run.expected_rejected is None:
        assert REJECTED_TABLE_NAME not in load_info.pipeline.default_schema.tables
    else:
        expected_rejected = _load_json_records(run.expected_rejected)
        rejected_rows = fetch_all_duckdb(
            load_info.pipeline,
            "SELECT table_name, source_file, line_no, raw_record, error_detail FROM _rejected ORDER BY source_file, error_detail",
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
            key=lambda item: (item["source_file"], item["error_detail"]),
        )


# Errors


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_xsv_schema_errors_fail_error_manifest(
    err: ErrorCase,
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """Each error fixture raises the XsvSchemaError subclass for its code when initializing XsvIngestSettings."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        make_cts_settings(
            XsvIngestSettings,
            tmp_path,
            dataset_name="xsv_error_dataset",
            file_glob="*.csv",
            input_dir=str(input_dir),
            schema_file=str(err.file),
            table_name="my_table",
            use_destination=dlt_destination_config,
        )
