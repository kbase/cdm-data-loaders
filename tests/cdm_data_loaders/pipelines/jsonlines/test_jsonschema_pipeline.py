"""Tests for the JSON Schema validation JSONL pipeline."""

import json
import shutil
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline import (
    _make_resolve_error,
    build_entity_resource,
    ncbi_datasets_cli,
    run_jsonlines_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import (
    JsonlJsonschemaIngestSettings,
    NcbiDatasetsSettings,
    convert_schema_to_pyarrow,
)
from cdm_data_loaders.readers.jsonlines import make_page_router
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedSchema
from cdm_data_loaders.schemas.ncbi_datasets.ncbi_openapi import (
    ANNOTATION_REPORT_TABLE,
    DATASET_REPORT_TABLE,
    filter_annotation_report,
)
from cdm_data_loaders.schemas.ncbi_datasets.ncbi_openapi import (
    ENTITY_SCHEMAS as NCBI_DATASETS_ENTITY_SCHEMAS,
)
from cdm_data_loaders.schemas.ncbi_datasets.schema import SCHEMAS as NCBI_DATASETS_SCHEMAS
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.helpers import assert_run_ok, read_pipeline_tables
from tests.cdm_data_loaders.pipelines.jsonlines.conftest import make_item
from tests.conftest import TEST_DATA_DIR
from tests.helpers import make_cts_settings, register_temp_module, settings_to_argv


def make_router(
    schema: dict[str, Any], buffer_size: int = 2, *, pyarrow_schema: pa.Schema | None = None
) -> Callable[[list[dict[str, Any]]], Any]:
    """Build the router the jsonschema pipeline uses for the widget table."""
    validated_schema = ValidatedSchema(jsonschema=schema)
    resolved_pa_schema = pyarrow_schema if pyarrow_schema is not None else convert_schema_to_pyarrow(validated_schema)
    return make_page_router(
        "widget",
        buffer_size,
        _make_resolve_error(validated_schema),
        schema_map=ArrowSchemaRegistry(registry={"widget": resolved_pa_schema}),
    )


def test_make_page_router_pass_parse_error_preserves_raw_record(widget_schema: dict[str, Any]) -> None:
    """Malformed JSON bypasses validation and preserves its source text."""
    validate = make_router(widget_schema)
    item = {
        "record": None,
        "raw_record": "{bad json",
        "parse_error": "bad json",
        "source_file": "data.jsonl",
        "line_no": 3,
    }

    results = list(validate([item]))

    assert len(results) == 1
    assert results[0].meta.table_name == "_rejected"
    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.schema == REJECTED_PYARROW_SCHEMA
    assert results[0].data.to_pylist() == [
        {
            "table_name": "widget",
            "source_file": "data.jsonl",
            "line_no": 3,
            "raw_record": "{bad json",
            "error_detail": "bad json",
        }
    ]


def test_make_page_router_fail_uses_declared_draft() -> None:
    """Draft 4 boolean exclusiveMinimum rejects the boundary value."""
    schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {"count": {"type": "number", "minimum": 5, "exclusiveMinimum": True}},
    }
    validate = make_router(schema, pyarrow_schema=pa.schema([pa.field("count", pa.float64())]))
    item = make_item({"count": 5})

    results = list(validate([item]))

    assert [result.meta.table_name for result in results] == ["_rejected"]


def test_make_page_router_pass_preserves_records_without_inserting_defaults(widget_schema: dict[str, Any]) -> None:
    """Valid data is coerced into a PyArrow Table matching the schema without inserted defaults."""
    record = {
        "widget_id": "a",
        "count": 0,
        "collected": "2024-02-29",
        "nested": {"tags": ["one", "two"]},
        "undeclared": "ignored",
    }
    validate = make_router(widget_schema)
    results = list(validate([make_item(record)]))
    assert len(results) == 1
    assert results[0].meta.table_name == "widget"
    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.schema == convert_schema_to_pyarrow(ValidatedSchema(jsonschema=widget_schema))
    assert results[0].data.to_pylist() == [
        {
            "widget_id": "a",
            "count": 0,
            "collected": date(2024, 2, 29),
            "label": None,
            "nested": {"tags": ["one", "two"]},
        }
    ]
    assert "label" not in record


def test_make_page_router_pass_empty_page(widget_schema: dict[str, Any]) -> None:
    """An empty page yields no routed output items."""
    validate = make_router(widget_schema)
    assert list(validate([])) == []


@pytest.mark.parametrize(
    ("record", "messages"),
    [
        pytest.param({"count": 1}, ["'widget_id' is a required property"], id="missing-required"),
        pytest.param({"widget_id": "a", "count": "1"}, ["'1' is not of type 'integer'"], id="no-coercion"),
        pytest.param({"widget_id": "a", "count": True}, ["True is not of type 'integer'"], id="boolean-not-integer"),
        pytest.param({"widget_id": "a", "count": -1}, ["-1 is less than the minimum of 0"], id="minimum"),
        pytest.param(
            {"widget_id": "a", "count": 1, "collected": "2023-02-29"},
            ["'2023-02-29' is not a 'date'"],
            id="invalid-format",
        ),
        pytest.param({}, ["'count' is a required property", "'widget_id' is a required property"], id="all-errors"),
        pytest.param(None, ["None is not of type 'object'"], id="null"),
        pytest.param([], ["[] is not of type 'object'"], id="array"),
        pytest.param(42, ["42 is not of type 'object'"], id="scalar"),
    ],
)
def test_make_page_router_fail_reports_exact_errors(
    widget_schema: dict[str, Any], record: object, messages: list[str]
) -> None:
    """Invalid records retain their raw text and report every validation error."""
    item = make_item(record, line_no=7)
    results = list(make_router(widget_schema)([item]))
    assert len(results) == 1
    assert results[0].meta.table_name == "_rejected"
    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.schema == REJECTED_PYARROW_SCHEMA
    assert results[0].data.to_pylist() == [
        {
            "table_name": "widget",
            "source_file": "file.jsonl",
            "line_no": 7,
            "raw_record": item["raw_record"],
            "error_detail": json.dumps(messages),
        }
    ]


def test_make_page_router_pass_flushes_independent_buffers(widget_schema: dict[str, Any]) -> None:
    """Full and partial valid/rejected pages flush without leaking between calls."""
    valid = {"widget_id": "a", "count": 0}
    items = [make_item(record, index) for index, record in enumerate([valid, {}, valid, {}, valid, {}], start=1)]
    validate = make_router(widget_schema)
    for _iteration in range(2):
        pages = list(validate(items))
        assert [(page.meta.table_name, len(page.data)) for page in pages] == [
            ("widget", 2),
            ("_rejected", 2),
            ("widget", 1),
            ("_rejected", 1),
        ]
        assert all(isinstance(page.data, pa.Table) for page in pages)
        assert [
            row["line_no"] for page in pages if page.meta.table_name == "_rejected" for row in page.data.to_pylist()
        ] == [
            2,
            4,
            6,
        ]


@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "gzip"])
def test_build_entity_resource_pass_reads_selected_files(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    widget_schema: dict[str, Any],
    write_gzip_file: Callable[[Path, str, str], Path],
    compressed: bool,
) -> None:
    """The real resource reads plain/gzip files, skips blanks, and honors the glob."""
    settings = schema_settings_factory()
    entity_dir = Path(settings.input_dir) / "widget"
    entity_dir.mkdir()
    record = {"widget_id": "a", "count": 0}
    contents = f"\n{json.dumps(record)}\n\n{{broken\n"
    filename = "data.jsonl.gz" if compressed else "data.jsonl"
    if compressed:
        write_gzip_file(entity_dir, filename, contents)
    else:
        (entity_dir / filename).write_text(contents, encoding="utf-8")
    (entity_dir / "ignored.txt").write_text("not JSON", encoding="utf-8")

    resource = build_entity_resource("widget", ValidatedSchema(jsonschema=widget_schema), settings)
    rows = list(resource)

    expected_page_count = 2
    assert len(rows) == expected_page_count
    assert all(isinstance(item, pa.Table) for item in rows)
    valid_table = next(item for item in rows if "widget_id" in item.schema.names)
    rejected_table = next(item for item in rows if item.schema == REJECTED_PYARROW_SCHEMA)
    assert valid_table.to_pylist() == [{"widget_id": "a", "count": 0, "collected": None, "label": None, "nested": None}]
    expected_error = "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
    assert rejected_table.to_pylist() == [
        {
            "table_name": "widget",
            "source_file": f"widget/{filename}",
            "line_no": 4,
            "raw_record": "{broken",
            "error_detail": expected_error,
        }
    ]


@pytest.mark.parametrize("create_directory", [False, True], ids=["missing", "empty"])
def test_build_entity_resource_pass_no_files(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    widget_schema: dict[str, Any],
    create_directory: bool,
) -> None:
    """Missing and empty entity directories yield no records."""
    settings = schema_settings_factory()
    if create_directory:
        (Path(settings.input_dir) / "widget").mkdir()
    assert list(build_entity_resource("widget", ValidatedSchema(jsonschema=widget_schema), settings)) == []


def test_run_jsonlines_pipeline_pass_loads_valid_and_rejected_rows(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
) -> None:
    """The pipeline converts schemas to PyArrow and writes both valid Arrow pages and rejected records."""
    settings = schema_settings_factory()
    entity_dir = Path(settings.input_dir) / "widget"
    entity_dir.mkdir()
    valid_record = {"widget_id": "w1", "count": 3, "collected": "2024-02-29"}
    invalid_record = {"widget_id": "w2", "count": -1}
    (entity_dir / "data.jsonl").write_text(
        f"{json.dumps(valid_record)}\n{json.dumps(invalid_record)}\n",
        encoding="utf-8",
    )

    assert_run_ok(run_jsonlines_pipeline(settings))
    dataset_dir = Path(settings.output_dir) / str(settings.dataset_name)
    assert (dataset_dir / "widget").is_dir()
    assert (dataset_dir / "_rejected").is_dir()


def test_run_jsonlines_pipeline_pass_filters_and_splits_records_with_transforms_module(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_jsonlines_pipeline filters rows and splits records across tables using TABLE_TRANSFORMS and OUTPUT_SCHEMAS."""

    def transform(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        return (
            {}
            if row["count"] == 0
            else {
                "widget": [{"widget_id": row["widget_id"], "count": row["count"]}],
                "widget_tag": [
                    {"widget_id": row["widget_id"], "tag": tag} for tag in (row.get("nested") or {}).get("tags", [])
                ],
            }
        )

    output_schemas = {
        "widget": pa.schema(
            [pa.field("widget_id", pa.string(), nullable=False), pa.field("count", pa.int64(), nullable=False)]
        ),
        "widget_tag": pa.schema(
            [pa.field("widget_id", pa.string(), nullable=False), pa.field("tag", pa.string(), nullable=False)]
        ),
    }
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_jsonschema_transforms",
        TABLE_TRANSFORMS={"widget": transform},
        OUTPUT_SCHEMAS=output_schemas,
    )
    settings = schema_settings_factory(
        transforms_module=transforms_mod,
        output_schemas_module=transforms_mod,
    )
    entity_dir = Path(settings.input_dir) / "widget"
    entity_dir.mkdir()
    filtered_record = {"widget_id": "w0", "count": 0, "nested": {"tags": ["ignored"]}}
    valid_record = {"widget_id": "w1", "count": 3, "nested": {"tags": ["alpha", "beta"]}}
    invalid_record = {"widget_id": "w2", "count": -1}
    (entity_dir / "data.jsonl").write_text(
        f"{json.dumps(filtered_record)}\n{json.dumps(valid_record)}\n{json.dumps(invalid_record)}\n",
        encoding="utf-8",
    )

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))
    dataset = load_info.pipeline.dataset()
    assert dataset.widget.df().to_dict(orient="records") == [{"widget_id": "w1", "count": 3}]
    assert sorted(dataset.widget_tag.df()["tag"].tolist()) == ["alpha", "beta"]
    assert len(dataset.table("_rejected").df()) == 1
    assert dataset.table("_rejected").df().iloc[0]["table_name"] == "widget"


@pytest.mark.parametrize(
    ("record", "expected_kept"),
    [
        pytest.param({"total_count": 0}, False, id="missing-reports"),
        pytest.param({"messages": [{"warning": {"message": "no annotations"}}]}, False, id="messages-only"),
        pytest.param({"reports": [], "total_count": 0}, False, id="empty-reports"),
        pytest.param(
            {"reports": [{"row_id": "1", "annotation": {"name": "int"}}], "total_count": 1},
            True,
            id="non-empty-reports",
        ),
    ],
)
def test_filter_annotation_report_pass_discards_missing_or_empty_reports(
    record: dict[str, Any],
    expected_kept: bool,
) -> None:
    """filter_annotation_report drops records without a non-empty top-level reports list."""
    result = filter_annotation_report(record)
    if expected_kept:
        assert result == {ANNOTATION_REPORT_TABLE: [record]}
    else:
        assert result == {}


@pytest.mark.parametrize("table_name", [ANNOTATION_REPORT_TABLE, DATASET_REPORT_TABLE], ids=str)
def test_ncbi_datasets_precomputed_schemas_match_converter(table_name: str) -> None:
    """Precomputed NCBI Datasets PyArrow schemas match convert_schema_to_pyarrow output."""
    expected = convert_schema_to_pyarrow(ValidatedSchema(jsonschema=NCBI_DATASETS_ENTITY_SCHEMAS[table_name]))
    assert NCBI_DATASETS_SCHEMAS[table_name] == expected


@pytest.mark.parametrize(
    "annotation_fixture_name",
    ["annotations.jsonl", "synthesized_annotations.jsonl"],
    ids=["three-example-responses", "synthesized-twenty-responses"],
)
def test_ncbi_datasets_pipeline_pass_loads_realistic_data_and_discards_empty_annotations(
    tmp_path: Path,
    dlt_destination_config: str,
    pipeline_runner: PipelineTestRunner,
    annotation_fixture_name: str,
) -> None:
    """ncbi_datasets loads dataset_report and annotation_report records while discarding responses without 'reports'."""
    ncbi_data_dir = TEST_DATA_DIR / "ncbi_datasets"
    source_ann_file = ncbi_data_dir / "annotation_report" / annotation_fixture_name
    source_ds_file = ncbi_data_dir / "dataset_report" / "chunk_4" / "dataset" / "dataset_reports_0000.jsonl"

    ann_records = [
        json.loads(line) for line in source_ann_file.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    expected_kept_ann = [rec for rec in ann_records if rec.get("reports")]
    expected_dropped_count = len(ann_records) - len(expected_kept_ann)
    min_empty_response_ratio = 0.25
    assert expected_dropped_count >= 1
    assert expected_dropped_count / len(ann_records) >= min_empty_response_ratio

    ds_records = [json.loads(line) for line in source_ds_file.read_text(encoding="utf-8").splitlines() if line.strip()]

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "bucket"
    ann_input_dir = input_dir / ANNOTATION_REPORT_TABLE
    ds_input_dir = input_dir / DATASET_REPORT_TABLE
    ann_input_dir.mkdir(parents=True)
    ds_input_dir.mkdir(parents=True)

    shutil.copyfile(source_ann_file, ann_input_dir / annotation_fixture_name)
    shutil.copyfile(source_ds_file, ds_input_dir / source_ds_file.name)

    settings = make_cts_settings(
        NcbiDatasetsSettings,
        tmp_path,
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )
    load_info = assert_run_ok(pipeline_runner.run(ncbi_datasets_cli, cli_args=settings_to_argv(settings)))
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, str(settings.loader_file_format))

    assert "_rejected" not in tables
    assert len(tables[ANNOTATION_REPORT_TABLE]) == len(expected_kept_ann)
    assert [row["total_count"] for row in tables[ANNOTATION_REPORT_TABLE]] == [
        rec["total_count"] for rec in expected_kept_ann
    ]
    assert [len(row["reports"]) for row in tables[ANNOTATION_REPORT_TABLE]] == [
        len(rec["reports"]) for rec in expected_kept_ann
    ]
    assert len(tables[DATASET_REPORT_TABLE]) == len(ds_records)
    assert [row["accession"] for row in tables[DATASET_REPORT_TABLE]] == [rec["accession"] for rec in ds_records]


def test_ncbi_datasets_pipeline_pass_routes_invalid_records_to_rejected(
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """Invalid JSON or schema-invalid records in ncbi_datasets route to _rejected while no-reports rows are dropped."""
    ncbi_data_dir = TEST_DATA_DIR / "ncbi_datasets"
    ann_lines = (ncbi_data_dir / "annotation_report" / "annotations.jsonl").read_text(encoding="utf-8").splitlines()

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "bucket"
    ann_input_dir = input_dir / ANNOTATION_REPORT_TABLE
    ds_input_dir = input_dir / DATASET_REPORT_TABLE
    ann_input_dir.mkdir(parents=True)
    ds_input_dir.mkdir(parents=True)

    # Line 1: {"total_count": 0} (dropped), Line 2: 1-report valid, Line 3: invalid type for total_count
    (ann_input_dir / "batch.jsonl").write_text(
        f"{ann_lines[0]}\n{ann_lines[1]}\n{json.dumps({'total_count': 'not-an-int'})}\n",
        encoding="utf-8",
    )
    (ds_input_dir / "batch.jsonl").write_text(
        f"{json.dumps({'accession': 'GCF_000005845.2'})}\n{{malformed json\n",
        encoding="utf-8",
    )

    settings = make_cts_settings(
        NcbiDatasetsSettings,
        tmp_path,
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )
    load_info = assert_run_ok(run_jsonlines_pipeline(settings))
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, str(settings.loader_file_format))

    assert len(tables[ANNOTATION_REPORT_TABLE]) == 1
    assert tables[ANNOTATION_REPORT_TABLE][0]["total_count"] == 1
    assert len(tables[DATASET_REPORT_TABLE]) == 1
    assert tables[DATASET_REPORT_TABLE][0]["accession"] == "GCF_000005845.2"
    rejected = sorted(tables["_rejected"], key=lambda row: row["table_name"])
    assert [(row["table_name"], row["line_no"]) for row in rejected] == [
        (ANNOTATION_REPORT_TABLE, 3),
        (DATASET_REPORT_TABLE, 2),
    ]
