"""Specification tests for the XML-to-PyArrow reader, settings, and ingestion pipeline.

Cases, rules, and error fixtures are declared in manifests under ``specs/pipelines/xmltopyarrow``
(``rules.yaml``, ``spec/*/case.yaml``, and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/xmltopyarrow_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _pytest.mark.structures import ParameterSet
from dlt.sources.filesystem import filesystem
from pydantic import ValidationError

import cdm_data_loaders.pipelines.xmltopyarrow.pipeline as xml_pipeline_module
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    PIPELINE_NAME,
    PUBMED_SCHEMAS,
    UNIPROT_SCHEMA,
    XMLTOPYARROW_CONFIG_ERRORS,
    PubMedXmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    XmlToPyArrowConfigError,
    XmlToPyArrowSettings,
)
from cdm_data_loaders.readers.xml import process_xml_file_to_pyarrow
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from scripts.gen_converter_spec import XMLTOPYARROW_PIPELINE, render
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

XML_SPEC_DIR = (PIPELINE_SPECS_DIR / "xmltopyarrow").resolve()
RULES = load_rules(XML_SPEC_DIR / "rules.yaml")
CASES: list[Case] = load_cases(XML_SPEC_DIR / "spec")
ERROR_CASES: list[ErrorCase] = load_error_cases(XML_SPEC_DIR / "spec" / "errors" / "manifest.yaml")
ERRORS_BY_CODE: dict[str, type[XmlToPyArrowConfigError]] = {cls.code: cls for cls in XMLTOPYARROW_CONFIG_ERRORS}

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


def _stage_run_settings(
    tmp_path: Path,
    dlt_destination_config: str,
    case: Case,
    run: Run,
) -> XmlToPyArrowSettings:
    """Copy a spec run's XML data file(s) into a clean ``input_dir`` and build ``XmlToPyArrowSettings``."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    if run.data.is_dir():
        shutil.copytree(run.data, input_dir, dirs_exist_ok=True)
        default_glob = "*.xml*"
    else:
        shutil.copy2(run.data, input_dir / run.data.name)
        default_glob = run.data.name

    run_params = dict(run.params)
    if "xml_level" not in run_params and "xml_tag" not in run_params and run.xml_tag is not None:
        run_params["xml_tag"] = run.xml_tag

    defaults: dict[str, Any] = {
        "dataset_name": "xml_spec_dataset",
        "file_glob": default_glob,
        "input_dir": str(input_dir),
        "loader_file_format": "parquet",
        "use_destination": dlt_destination_config,
        "xsd_file": str(case.schema),
        **run_params,
    }
    return make_cts_settings(XmlToPyArrowSettings, tmp_path, **defaults)


# Manifest consistency


def test_manifests_pass_every_case_rule_exists() -> None:
    """Every rule referenced by a case manifest is defined in rules.yaml."""
    all_cases = load_pipeline_cases(XML_SPEC_DIR)
    unknown = {f"{case.suite}/{case.name}": sorted(set(case.rules) - RULES.rules.keys()) for case in all_cases}
    assert {name: ids for name, ids in unknown.items() if ids} == {}


def test_manifests_pass_every_rule_is_exercised() -> None:
    """Every rule in rules.yaml is exercised by at least one case."""
    all_cases = load_pipeline_cases(XML_SPEC_DIR)
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
    """Every XSD file in spec/errors is listed in the error manifest."""
    listed = {err.file for err in ERROR_CASES}
    on_disk = set((XML_SPEC_DIR / "spec" / "errors").glob("*.xsd"))
    assert sorted(str(p) for p in on_disk - listed) == []


def test_manifests_pass_error_codes_unique_and_covered() -> None:
    """Error codes are unique, every manifest entry uses a known code, and every code has a fixture."""
    assert len(ERRORS_BY_CODE) == len(XMLTOPYARROW_CONFIG_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in XMLTOPYARROW_CONFIG_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


@pytest.mark.docs
def test_specification_document_pass_up_to_date() -> None:
    """The committed specification document matches the output of scripts/gen_converter_spec.py."""
    assert XMLTOPYARROW_PIPELINE.output.read_text(encoding="utf-8") == render(XMLTOPYARROW_PIPELINE), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )


# Schema resolution and presets


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_xmltopyarrow_settings_schema_resolution_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """XmlToPyArrowSettings resolves the expected PyArrow schema(s) in schema_map."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, case, run)
    assert settings.schema_map is not None

    if run.extra_expected_json:
        expected_schemas: dict[str, pa.Schema] = _load_module_attr(case.expected_schema, "EXPECTED_SCHEMAS")
        for table_name, expected_pa_schema in expected_schemas.items():
            actual_pa_schema = settings.schema_map.registry.get(table_name)
            assert actual_pa_schema is not None
            assert actual_pa_schema.equals(expected_pa_schema, check_metadata=True)
    else:
        assert settings.table_name is not None
        actual_pa_schema = settings.schema_map.registry.get(settings.table_name)
        assert actual_pa_schema is not None
        assert_schema_matches_expected(actual_pa_schema, case.expected_schema, run.schema_attr)


def test_preset_xmltopyarrow_settings_pass_precomputed_schemas(
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """Preset settings classes bind precomputed PyArrow schemas and default pre-processors."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    uniprot = make_cts_settings(
        UniProtXmlToPyArrowSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=dlt_destination_config,
    )
    assert uniprot.xml_tag == "entry"
    assert uniprot.table_name == "entry"
    assert uniprot.pyarrow_schema == UNIPROT_SCHEMA

    pubmed = make_cts_settings(
        PubMedXmlToPyArrowSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=dlt_destination_config,
    )
    assert pubmed.xml_level == 1
    assert pubmed.root_tag is None
    assert pubmed.pyarrow_schemas == dict(PUBMED_SCHEMAS)
    assert len(pubmed.get_file_preprocessors("PubmedArticle")) == 1


# Per-file pre-processing, XML streaming, and PyArrow coercion


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_process_xml_file_to_pyarrow_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """Pre-processors + process_xml_file_to_pyarrow yield PyArrow tables matching expected_table and expected JSON."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, case, run)

    file_items = sorted(
        filesystem(bucket_url=str(settings.input_dir), file_glob=settings.file_glob),
        key=lambda item: item["relative_path"],
    )
    preprocessors = settings.get_file_preprocessors(settings.table_name)
    batches_by_table: dict[str, list[pa.Table]] = {}
    for item in file_items:
        curr_item = item
        failed = False
        for preprocess_fn in preprocessors:
            try:
                curr_item = preprocess_fn(curr_item)
            except Exception as exc:  # noqa: BLE001
                rejected_row = {
                    "table_name": settings.table_name,
                    "source_file": item["relative_path"],
                    "line_no": None,
                    "raw_record": None,
                    "error_detail": str(exc),
                }
                batches_by_table.setdefault(REJECTED_TABLE_NAME, []).append(
                    pa.Table.from_pylist([rejected_row], schema=REJECTED_PYARROW_SCHEMA)
                )
                failed = True
                break
        if not failed and curr_item is not None:
            for yielded in process_xml_file_to_pyarrow(curr_item, settings):
                assert isinstance(yielded.data, pa.Table)
                batches_by_table.setdefault(yielded.meta.table_name, []).append(yielded.data)

    primary_table_name = settings.table_name or run.xml_tag
    assert primary_table_name is not None
    expected_table: pa.Table = load_expected_table(run.expected_table, run.table_attr)
    expected_rows = _load_json_records(run.expected_json)
    target_batches = batches_by_table.get(primary_table_name, [])
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
def test_run_xmltopyarrow_pipeline_end_to_end_pass_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """run_xmltopyarrow_pipeline loads all expected target, level, and _rejected rows into the destination."""
    settings = _stage_run_settings(tmp_path, dlt_destination_config, case, run)

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xml_pipeline_module,
        lambda: run_xmltopyarrow_pipeline(settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name=settings.dataset_name,
        pipeline_name_prefix=f"xml_spec_{case.name}_{run.id}",
    )

    primary_table_name = settings.table_name or run.xml_tag
    assert primary_table_name is not None
    expected_rows = _load_json_records(run.expected_json)
    if expected_rows:
        ((loaded_count,),) = fetch_all_duckdb(
            load_info.pipeline,
            f"SELECT COUNT(*) FROM {primary_table_name}",  # noqa: S608
        )
        assert loaded_count == len(expected_rows)
    else:
        assert primary_table_name not in load_info.pipeline.default_schema.tables

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
def test_xmltopyarrow_config_errors_fail_error_manifest(
    err: ErrorCase,
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """Each error fixture raises a ValidationError wrapping the expected XmlToPyArrowConfigError subclass."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValidationError) as exc_info:
        make_cts_settings(
            XmlToPyArrowSettings,
            tmp_path,
            dataset_name="xml_error_dataset",
            input_dir=str(input_dir),
            use_destination=dlt_destination_config,
            xsd_file=str(err.file),
            **err.params,
        )
    causes = [
        e["ctx"]["error"]
        for e in exc_info.value.errors()
        if "ctx" in e and isinstance(e["ctx"].get("error"), XmlToPyArrowConfigError)
    ]
    assert len(causes) == 1, f"Expected an XmlToPyArrowConfigError cause, got {exc_info.value.errors()}"
    cause = causes[0]
    assert isinstance(cause, ERRORS_BY_CODE[err.code])
    assert re.search(err.match, str(cause))
