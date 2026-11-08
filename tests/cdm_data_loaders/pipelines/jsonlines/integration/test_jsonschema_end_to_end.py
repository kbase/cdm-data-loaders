"""End-to-end JSON Schema ingestion tests using real filesystem destinations."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from cdm_data_loaders.converters.facades.pyiceberg_to_jsonschema import table_to_json_schema
from cdm_data_loaders.converters.facades.pyiceberg_to_linkml import table_to_linkml
from cdm_data_loaders.core.fields import ICEBERG, PARQUET
from cdm_data_loaders.pipelines.jsonlines.settings import JSONSCHEMA_PIPELINE_NAME, JsonlJsonschemaIngestSettings
from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.helpers import (
    LOADER_FILE_FORMATS,
    assert_iceberg_table_metadata,
    assert_pipeline_report_written,
    assert_run_ok,
    parse_json_string,
    read_data_rows,
)
from tests.helpers import settings_to_argv

pytestmark = pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)

WIDGET_RECORD = {"widget_id": "a", "count": 0, "nested": {"tags": ["one", "two"]}}
GADGET_RECORD = {"widget_id": "g", "count": 3}
EXPECTED_WIDGET_ROW = {
    "widget_id": "a",
    "count": 0,
    "collected": None,
    "label": None,
    "nested": {"tags": ["one", "two"]},
}
EXPECTED_GADGET_ROW = {
    "widget_id": "g",
    "count": 3,
    "collected": None,
    "label": None,
    "nested": None,
}
TYPE_ERROR_RAW = '{"widget_id":"bad", "count":"2"}'
FORMAT_ERROR_RAW = '{"widget_id":"bad","count":1,"collected":"2023-02-29"}'
MALFORMED_RAW = "{broken"
PARSE_ERROR = "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"


@pytest.fixture
def schema_input_dir(tmp_path: Path, write_gzip_file: Callable[[Path, str, str], Path]) -> Path:
    """Create two entities containing plain, compressed, valid, and invalid inputs."""
    input_dir = tmp_path / "input"
    widget_dir = input_dir / "widget"
    widget_dir.mkdir(parents=True)
    widget_line = json.dumps(WIDGET_RECORD)
    (widget_dir / "plain.jsonl").write_text(
        f"\n{widget_line}\n{MALFORMED_RAW}\n{TYPE_ERROR_RAW}\n{widget_line}", encoding="utf-8"
    )
    write_gzip_file(widget_dir, "compressed.jsonl.gz", f"{widget_line}\n{FORMAT_ERROR_RAW}\nnull")
    (widget_dir / "ignored.txt").write_text(MALFORMED_RAW, encoding="utf-8")
    gadget_dir = input_dir / "gadget"
    gadget_dir.mkdir()
    (gadget_dir / "records.jsonl").write_text(json.dumps(GADGET_RECORD), encoding="utf-8")
    return input_dir


def read_data_tables(settings: JsonlJsonschemaIngestSettings, dataset_name: str) -> dict[str, list[dict[str, Any]]]:
    """Read persisted rows without dlt metadata, preserving rejection text."""
    return read_data_rows(Path(settings.output_dir) / dataset_name, str(settings.loader_file_format))


@pytest.mark.parametrize("buffer_size", [1, 2, 10], ids=["single-row", "partial-pages", "single-page"])
def test_run_jsonlines_pipeline_pass_persists_valid_and_rejected_rows(
    schema_input_dir: Path,
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    loader_file_format: str,
    buffer_size: int,
    pipeline_runner: PipelineTestRunner,
) -> None:
    """Mixed files load every row exactly once with intact nested data and rejection provenance."""
    settings = schema_settings_factory(
        input_dir=str(schema_input_dir), loader_file_format=loader_file_format, buffer_size=buffer_size
    )

    load_info = assert_run_ok(pipeline_runner.run(JSONSCHEMA_PIPELINE_NAME, settings=settings))
    assert load_info.pipeline.pipeline_name == JSONSCHEMA_PIPELINE_NAME
    assert load_info.dataset_name == "schema_test"
    assert_pipeline_report_written(
        Path(settings.output_dir),
        load_info,
        metadata_moved=True,
        ingested_files={"widget/plain.jsonl", "widget/compressed.jsonl.gz", "gadget/records.jsonl"},
    )
    tables = read_data_tables(settings, load_info.dataset_name)
    assert set(tables) == {"widget", "_rejected", "gadget"}
    expected_widget = EXPECTED_WIDGET_ROW if settings.use_pyarrow else WIDGET_RECORD
    expected_gadget = EXPECTED_GADGET_ROW if settings.use_pyarrow else GADGET_RECORD
    assert [{key: parse_json_string(value) for key, value in row.items()} for row in tables["widget"]] == [
        expected_widget
    ] * 3
    assert tables["gadget"] == [expected_gadget]
    dataset_dir = Path(settings.output_dir) / load_info.dataset_name
    if loader_file_format == ICEBERG:
        for table_name in ("widget", "gadget", "_rejected"):
            assert_iceberg_table_metadata(dataset_dir, table_name)
        with SqlCatalog("test", uri="sqlite:///:memory:") as catalog:
            catalog.create_namespace(load_info.dataset_name)
            metadata_file = max((dataset_dir / "widget" / "metadata").glob("*.metadata.json"))
            iceberg_table = catalog.register_table((load_info.dataset_name, "widget"), metadata_file.as_uri())
            json_schema_doc = table_to_json_schema(iceberg_table, (load_info.dataset_name, "widget"))
            linkml_doc = table_to_linkml(iceberg_table, (load_info.dataset_name, "widget"))
        assert "widget_id" in json_schema_doc["properties"]
        assert {"widget", "widget_nested"} <= set(linkml_doc.classes)
    if loader_file_format in {PARQUET, ICEBERG}:
        widget_parquet = next((dataset_dir / "widget").rglob("*.parquet"))
        parquet_schema = pq.read_schema(widget_parquet)
        assert parquet_schema.field("widget_id").type == pa.string()
        assert parquet_schema.field("count").type == pa.int64()
        assert parquet_schema.field("collected").type == pa.date32()
        assert parquet_schema.field("label").type == pa.string()
        assert parquet_schema.field("nested").type == pa.struct(
            [pa.field("tags", pa.list_(pa.field("element", pa.string(), nullable=False)))]
        )
    assert sorted(tables["_rejected"], key=lambda row: (row["source_file"], row["line_no"])) == [
        {
            "table_name": "widget",
            "source_file": "widget/compressed.jsonl.gz",
            "line_no": 2,
            "raw_record": FORMAT_ERROR_RAW,
            "error_detail": json.dumps(["'2023-02-29' is not a 'date'"]),
        },
        {
            "table_name": "widget",
            "source_file": "widget/compressed.jsonl.gz",
            "line_no": 3,
            "raw_record": "null",
            "error_detail": json.dumps(["None is not of type 'object'"]),
        },
        {
            "table_name": "widget",
            "source_file": "widget/plain.jsonl",
            "line_no": 3,
            "raw_record": MALFORMED_RAW,
            "error_detail": PARSE_ERROR,
        },
        {
            "table_name": "widget",
            "source_file": "widget/plain.jsonl",
            "line_no": 4,
            "raw_record": TYPE_ERROR_RAW,
            "error_detail": json.dumps(["'2' is not of type 'integer'"]),
        },
    ]


def test_run_jsonlines_pipeline_pass_all_invalid(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    loader_file_format: str,
    pipeline_runner: PipelineTestRunner,
) -> None:
    """An entirely invalid entity still produces its complete rejection table."""
    settings = schema_settings_factory(loader_file_format=loader_file_format, buffer_size=1)
    table_dir = Path(settings.input_dir) / "widget"
    table_dir.mkdir()
    (table_dir / "invalid.jsonl").write_text(f"{MALFORMED_RAW}\n{TYPE_ERROR_RAW}", encoding="utf-8")

    load_info = assert_run_ok(pipeline_runner.run(JSONSCHEMA_PIPELINE_NAME, settings=settings))
    assert_pipeline_report_written(
        Path(settings.output_dir), load_info, metadata_moved=True, ingested_files=["widget/invalid.jsonl"]
    )
    reject_row_data = read_data_tables(settings, load_info.dataset_name)
    reject_row_data["_rejected"] = sorted(
        reject_row_data["_rejected"], key=lambda row: (row["source_file"], row["line_no"])
    )
    assert reject_row_data == {
        "_rejected": [
            {
                "table_name": "widget",
                "source_file": "widget/invalid.jsonl",
                "line_no": 1,
                "raw_record": MALFORMED_RAW,
                "error_detail": PARSE_ERROR,
            },
            {
                "table_name": "widget",
                "source_file": "widget/invalid.jsonl",
                "line_no": 2,
                "raw_record": TYPE_ERROR_RAW,
                "error_detail": json.dumps(["'2' is not of type 'integer'"]),
            },
        ]
    }


def test_cli_pass_loads_selected_entity(
    schema_input_dir: Path,
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    entity_schemas_factory: Callable[[object], str],
    widget_schema: dict[str, Any],
    loader_file_format: str,
    pipeline_runner: PipelineTestRunner,
) -> None:
    """The real CTS CLI accepts a registry shortcut and the requested file format."""
    settings = schema_settings_factory(
        input_dir=str(schema_input_dir),
        loader_file_format=loader_file_format,
        dataset_name="schema_cli",
        entity_schemas=entity_schemas_factory({"gadget": widget_schema}),
    )

    load_info = assert_run_ok(
        pipeline_runner.run(
            JSONSCHEMA_PIPELINE_NAME,
            cli_args=[
                *settings_to_argv(settings, exclude=["entity_schemas"]),
                "-m",
                settings.entity_schemas,
            ],
        )
    )
    assert load_info.dataset_name == "schema_cli"
    assert_pipeline_report_written(
        Path(settings.output_dir), load_info, metadata_moved=True, ingested_files=["gadget/records.jsonl"]
    )
    expected_gadget = EXPECTED_GADGET_ROW if settings.use_pyarrow else GADGET_RECORD
    assert read_data_tables(settings, load_info.dataset_name) == {"gadget": [expected_gadget]}
