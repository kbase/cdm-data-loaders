"""Tests for JSON Schema validation, PyArrow conversion, and rejected-record routing."""

import gzip
import json
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest
from pydantic import ValidationError

from cdm_data_loaders.converters.jsonschema_to_pyarrow import JSONSchemaToPyArrowError
from cdm_data_loaders.pipelines.jsonlines.base import make_page_router
from cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline import (
    _make_resolve_error,
    build_entity_resource,
    build_pyarrow_schemas,
    coerce_record_for_pyarrow,
    convert_schema_to_pyarrow,
    load_entity_schemas,
    run_jsonlines_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlJsonschemaIngestSettings
from cdm_data_loaders.utils.jsonschema.dereferencer import DereferencingError
from tests.cdm_data_loaders.pipelines.jsonlines.conftest import make_item
from tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_schemas import (
    ENTITY_SCHEMAS as DATASET_ENTITY_SCHEMAS,
)


def make_router(schema: dict[str, Any], buffer_size: int = 2) -> Callable[[list[dict[str, Any]]], Any]:
    """Build the router the jsonschema pipeline uses for the widget table."""
    pyarrow_schema = convert_schema_to_pyarrow(schema)
    return make_page_router(
        "widget",
        buffer_size,
        _make_resolve_error(schema),
        transform_valid=lambda record: coerce_record_for_pyarrow(record, pyarrow_schema),
        pyarrow_schema=pyarrow_schema,
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

    assert [(result.meta.table_name, result.data) for result in results] == [
        (
            "widget_rejected",
            [{"source_file": "data.jsonl", "line_no": 3, "raw_record": "{bad json", "error_detail": "bad json"}],
        )
    ]


def test_make_page_router_fail_uses_declared_draft() -> None:
    """Draft 4 boolean exclusiveMinimum rejects the boundary value."""
    schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {"count": {"type": "number", "minimum": 5, "exclusiveMinimum": True}},
    }
    validate = make_router(schema)
    item = {
        "record": {"count": 5},
        "raw_record": '{"count": 5}',
        "parse_error": None,
        "source_file": "data.jsonl",
        "line_no": 1,
    }

    results = list(validate([item]))

    assert [result.meta.table_name for result in results] == ["widget_rejected"]


def test_load_entity_schemas_pass_returns_registry(
    schema_module_factory: Callable[[object], str], widget_schema: dict[str, Any]
) -> None:
    """Valid schemas from multiple supported drafts are returned unchanged."""
    registry = {
        "widget": widget_schema,
        "older": {"$schema": "http://json-schema.org/draft-04/schema#", "type": "object"},
    }
    assert load_entity_schemas(schema_module_factory(registry)) is registry


@pytest.mark.parametrize(
    ("registry", "message"),
    [
        pytest.param(None, "non-empty ENTITY_SCHEMAS", id="none"),
        pytest.param({}, "non-empty ENTITY_SCHEMAS", id="empty"),
        pytest.param([], "non-empty ENTITY_SCHEMAS", id="list"),
        pytest.param({"": {}}, "non-empty table names", id="empty-table"),
        pytest.param({1: {}}, "non-empty table names", id="non-string-table"),
        pytest.param({"widget": {}}, r"missing the \$schema keyword", id="missing-draft"),
        pytest.param({"widget": {"$schema": "https://example.invalid/schema"}}, "Unsupported", id="unknown-draft"),
    ],
)
def test_load_entity_schemas_fail_malformed_registry(
    schema_module_factory: Callable[[object], str], registry: object, message: str
) -> None:
    """Malformed registries and unsupported drafts fail before extraction."""
    with pytest.raises(ValueError, match=message):
        load_entity_schemas(schema_module_factory(registry))


@pytest.mark.parametrize(
    "schema",
    [pytest.param(True, id="boolean-schema"), pytest.param({"$schema": []}, id="non-string-draft")],
)
def test_load_entity_schemas_fail_invalid_types(schema_module_factory: Callable[[object], str], schema: object) -> None:
    """Schema dictionaries and draft declarations must have the documented types."""
    with pytest.raises(TypeError, match="JSON Schema for widget must"):
        load_entity_schemas(schema_module_factory({"widget": schema}))


def test_load_entity_schemas_fail_missing_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A module must explicitly provide ENTITY_SCHEMAS."""
    module_name = f"_test_missing_registry_{uuid4().hex}"
    monkeypatch.setitem(sys.modules, module_name, ModuleType(module_name))
    with pytest.raises(ValueError, match="non-empty ENTITY_SCHEMAS"):
        load_entity_schemas(module_name)


def test_load_entity_schemas_fail_missing_module() -> None:
    """Import failures retain their original exception."""
    with pytest.raises(ModuleNotFoundError):
        load_entity_schemas(f"_missing_schema_module_{uuid4().hex}")


def test_load_entity_schemas_fail_reports_all_invalid_schemas(
    schema_module_factory: Callable[[object], str], widget_schema: dict[str, Any]
) -> None:
    """Schema errors identify every failing table, not just the first."""
    module_name = schema_module_factory(
        {"first": {**widget_schema, "type": "invalid"}, "second": {**widget_schema, "required": 7}}
    )
    with pytest.raises(RuntimeError, match="errors were found") as error:
        load_entity_schemas(module_name)
    assert all(text in str(error.value) for text in [module_name, "first:", "second:"])


def test_convert_schema_to_pyarrow_pass_flat_and_nested_schema(widget_schema: dict[str, Any]) -> None:
    """Validated JSON Schemas convert to PyArrow schemas with expected types and nullability."""
    pa_schema = convert_schema_to_pyarrow(widget_schema)

    assert pa_schema == pa.schema(
        [
            pa.field("widget_id", pa.string(), nullable=False),
            pa.field("count", pa.int64(), nullable=False),
            pa.field("collected", pa.date32(), nullable=True),
            pa.field("label", pa.string(), nullable=True),
            pa.field(
                "nested",
                pa.struct([pa.field("tags", pa.list_(pa.string()), nullable=True)]),
                nullable=True,
            ),
        ]
    )


def test_convert_schema_to_pyarrow_pass_dereferences_root_and_nested_refs() -> None:
    """Schemas using root $ref, nested $defs, and allOf dereference before PyArrow conversion."""
    draft7_root_ref_schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$ref": "#/definitions/RootItem",
        "definitions": {
            "Base": {
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "string"}},
            },
            "RootItem": {
                "allOf": [
                    {"$ref": "#/definitions/Base"},
                    {
                        "type": "object",
                        "properties": {"score": {"type": "number", "multipleOf": 0.01}},
                    },
                ]
            },
        },
    }

    pa_schema = convert_schema_to_pyarrow(draft7_root_ref_schema)
    assert pa_schema == pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("score", pa.decimal128(38, 2), nullable=True),
        ]
    )

    dataset_pa_schema = convert_schema_to_pyarrow(DATASET_ENTITY_SCHEMAS["dataset"])
    assert "accession" in dataset_pa_schema.names
    assert dataset_pa_schema.field("accession").type == pa.string()


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(
            {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {}},
            id="empty-properties",
        ),
        pytest.param(
            {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"},
            id="missing-properties",
        ),
    ],
)
def test_convert_schema_to_pyarrow_pass_empty_object_schema(schema: dict[str, Any]) -> None:
    """Object schemas with empty or omitted properties convert to an empty PyArrow schema."""
    assert convert_schema_to_pyarrow(schema) == pa.schema([])


@pytest.mark.parametrize(
    ("schema", "exc_type"),
    [
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"item": {"$ref": "#/$defs/Missing"}},
            },
            DereferencingError,
            id="unresolvable-ref",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$ref": "#/$defs/Loop",
                "$defs": {"Loop": {"$ref": "#/$defs/Loop"}},
            },
            DereferencingError,
            id="circular-ref",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "string",
            },
            JSONSchemaToPyArrowError,
            id="non-object-root",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": ["object", "null"],
                "properties": {"id": {"type": "string"}},
            },
            JSONSchemaToPyArrowError,
            id="nullable-object-root",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            JSONSchemaToPyArrowError,
            id="root-map-without-properties",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"val": {"type": "number", "multipleOf": Decimal("1e-39")}},
            },
            JSONSchemaToPyArrowError,
            id="decimal-scale-exceeds-precision",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {"id": {"type": "string"}},
            },
            JSONSchemaToPyArrowError,
            id="missing-schema-draft",
        ),
    ],
)
def test_convert_schema_to_pyarrow_fail_invalid_schemas(schema: dict[str, Any], exc_type: type[Exception]) -> None:
    """Dereferencing and PyArrow conversion failures raise their documented exception types."""
    with pytest.raises(exc_type):
        convert_schema_to_pyarrow(schema)


def test_build_pyarrow_schemas_pass_multiple_and_empty_registries(widget_schema: dict[str, Any]) -> None:
    """Valid registries convert every table to a PyArrow schema, and an empty mapping returns empty."""
    assert build_pyarrow_schemas({}) == {}
    schemas = build_pyarrow_schemas({"widget": widget_schema, "dataset": DATASET_ENTITY_SCHEMAS["dataset"]})
    assert set(schemas) == {"widget", "dataset"}
    assert schemas["widget"] == convert_schema_to_pyarrow(widget_schema)
    assert "accession" in schemas["dataset"].names


def test_build_pyarrow_schemas_fail_reports_all_unconvertible_schemas() -> None:
    """Conversion errors across multiple tables are aggregated into a single RuntimeError."""
    table_to_schema: dict[str, dict[str, Any]] = {
        "broken_ref": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"item": {"$ref": "#/$defs/Missing"}},
        },
        "non_struct_root": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "string",
        },
    }

    with pytest.raises(RuntimeError, match="converting schemas to PyArrow") as error:
        build_pyarrow_schemas(table_to_schema)
    assert all(name in str(error.value) for name in ["broken_ref:", "non_struct_root:"])


def test_coerce_record_for_pyarrow_pass_temporal_decimal_and_nested_types() -> None:
    """JSON-parsed values are coerced to match temporal, decimal, numeric, struct, list, map, and string fields."""
    pa_schema = pa.schema(
        [
            pa.field("day", pa.date32()),
            pa.field("ts_z", pa.timestamp("us", tz="UTC")),
            pa.field("ts_naive", pa.timestamp("us", tz="UTC")),
            pa.field("tod", pa.time64("us")),
            pa.field("price", pa.decimal128(38, 2)),
            pa.field("ratio", pa.float64()),
            pa.field("whole", pa.int64()),
            pa.field("collapsed_obj", pa.string()),
            pa.field("collapsed_num", pa.string()),
            pa.field("nested", pa.struct([pa.field("when", pa.date32())])),
            pa.field("items", pa.list_(pa.decimal128(38, 1))),
            pa.field("attrs", pa.map_(pa.string(), pa.date32())),
        ]
    )
    raw_record: dict[str, Any] = {
        "day": "2024-02-29",
        "ts_z": "2024-02-29T12:30:00Z",
        "ts_naive": "2024-02-29T12:30:00",
        "tod": "08:15:30",
        "price": 19.95,
        "ratio": 2,
        "whole": 7.0,
        "collapsed_obj": {"a": 1},
        "collapsed_num": 42,
        "nested": {"when": "2024-03-01", "extra": "dropped"},
        "items": [1.5, 2],
        "attrs": {"start": "2024-01-01"},
        "undeclared_top": "dropped",
    }

    coerced = coerce_record_for_pyarrow(raw_record, pa_schema)

    assert coerced == {
        "day": date(2024, 2, 29),
        "ts_z": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "ts_naive": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "tod": time(8, 15, 30),
        "price": Decimal("19.95"),
        "ratio": 2.0,
        "whole": 7,
        "collapsed_obj": json.dumps({"a": 1}),
        "collapsed_num": "42",
        "nested": {"when": date(2024, 3, 1)},
        "items": [Decimal("1.5"), Decimal(2)],
        "attrs": {"start": date(2024, 1, 1)},
    }
    table = pa.Table.from_pylist([coerced], schema=pa_schema)
    assert table.num_rows == 1
    assert table.schema == pa_schema


def test_coerce_record_for_pyarrow_pass_nulls_empty_and_boundary_values() -> None:
    """Nulls, empty containers, zero/negative numbers, tz-naive timestamps, large types, and pre-coerced values pass."""
    pa_schema = pa.schema(
        [
            pa.field("day", pa.date32()),
            pa.field("ts_naive_target", pa.timestamp("us")),
            pa.field("tod", pa.time64("us")),
            pa.field("zero_dec", pa.decimal128(38, 2)),
            pa.field("neg_dec", pa.decimal128(38, 2)),
            pa.field("neg_float", pa.float64()),
            pa.field("neg_int", pa.int64()),
            pa.field("large_text", pa.large_string()),
            pa.field("collapsed_list", pa.large_string()),
            pa.field("nested", pa.struct([pa.field("when", pa.date32())])),
            pa.field("large_items", pa.large_list(pa.decimal128(38, 1))),
            pa.field("attrs", pa.map_(pa.string(), pa.date32())),
        ]
    )

    assert coerce_record_for_pyarrow({}, pa_schema) == {}

    all_nulls = dict.fromkeys(pa_schema.names)
    assert coerce_record_for_pyarrow(all_nulls, pa_schema) == all_nulls

    boundary_record: dict[str, Any] = {
        "day": date(2024, 1, 1),
        "ts_naive_target": "2024-02-29T12:30:00",
        "tod": time(0, 0, 0),
        "zero_dec": 0,
        "neg_dec": "-0.05",
        "neg_float": -3,
        "neg_int": -7.0,
        "large_text": "already-string",
        "collapsed_list": [1, "two"],
        "nested": {"when": None},
        "large_items": [None, Decimal("-1.5")],
        "attrs": {"none_val": None},
    }
    coerced = coerce_record_for_pyarrow(boundary_record, pa_schema)
    assert coerced == {
        "day": date(2024, 1, 1),
        "ts_naive_target": datetime(2024, 2, 29, 12, 30),  # noqa: DTZ001
        "tod": time(0, 0, 0),
        "zero_dec": Decimal(0),
        "neg_dec": Decimal("-0.05"),
        "neg_float": -3.0,
        "neg_int": -7,
        "large_text": "already-string",
        "collapsed_list": json.dumps([1, "two"]),
        "nested": {"when": None},
        "large_items": [None, Decimal("-1.5")],
        "attrs": {"none_val": None},
    }
    rows_to_write = [coerced, all_nulls]
    table = pa.Table.from_pylist(rows_to_write, schema=pa_schema)
    assert table.num_rows == len(rows_to_write)

    empty_containers = {"nested": {}, "large_items": [], "attrs": {}}
    assert coerce_record_for_pyarrow(empty_containers, pa_schema) == empty_containers


def test_coerce_record_for_pyarrow_pass_preserves_boolean_and_non_integral_float_guards() -> None:
    """Booleans are not coerced to numeric/decimal types, and non-integral floats are not truncated to int."""
    pa_schema = pa.schema(
        [
            pa.field("dec", pa.decimal128(38, 2)),
            pa.field("flt", pa.float64()),
            pa.field("integer_from_bool", pa.int64()),
            pa.field("integer_from_frac", pa.int64()),
        ]
    )
    fractional_value = 7.5
    raw = {
        "dec": True,
        "flt": False,
        "integer_from_bool": True,
        "integer_from_frac": fractional_value,
    }
    coerced = coerce_record_for_pyarrow(raw, pa_schema)
    assert coerced == raw
    assert isinstance(coerced["dec"], bool)
    assert isinstance(coerced["flt"], bool)
    assert isinstance(coerced["integer_from_bool"], bool)
    assert coerced["integer_from_frac"] == fractional_value


def test_make_page_router_pass_preserves_records_and_empty_pages(widget_schema: dict[str, Any]) -> None:
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
    assert results[0].data.schema == convert_schema_to_pyarrow(widget_schema)
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
    assert [(result.meta.table_name, result.data) for result in results] == [
        (
            "widget_rejected",
            [
                {
                    "source_file": "file.jsonl",
                    "line_no": 7,
                    "raw_record": item["raw_record"],
                    "error_detail": json.dumps(messages),
                }
            ],
        )
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
            ("widget_rejected", 2),
            ("widget", 1),
            ("widget_rejected", 1),
        ]
        assert all(isinstance(page.data, pa.Table) for page in pages if page.meta.table_name == "widget")
        assert [row["line_no"] for page in pages if page.meta.table_name == "widget_rejected" for row in page.data] == [
            2,
            4,
            6,
        ]


@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "gzip"])
def test_build_entity_resource_pass_reads_selected_files(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    widget_schema: dict[str, Any],
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
        with gzip.open(entity_dir / filename, "wt", encoding="utf-8") as stream:
            stream.write(contents)
    else:
        (entity_dir / filename).write_text(contents, encoding="utf-8")
    (entity_dir / "ignored.txt").write_text("not JSON", encoding="utf-8")

    resource = build_entity_resource("widget", widget_schema, settings)
    rows = list(resource)

    valid_tables = [item for item in rows if isinstance(item, pa.Table)]
    assert len(valid_tables) == 1
    assert valid_tables[0].to_pylist() == [
        {"widget_id": "a", "count": 0, "collected": None, "label": None, "nested": None}
    ]
    rejected = next(row for row in rows if isinstance(row, dict) and "error_detail" in row)
    expected_error = "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
    assert rejected == {"source_file": filename, "line_no": 4, "raw_record": "{broken", "error_detail": expected_error}
    assert len(rows) == len(valid_tables) + 1


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
    assert list(build_entity_resource("widget", widget_schema, settings)) == []


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

    load_info = run_jsonlines_pipeline(settings)
    assert load_info is not None
    assert not load_info.has_failed_jobs
    dataset_dir = Path(settings.output_dir) / str(settings.dataset_name)
    assert (dataset_dir / "widget").is_dir()
    assert (dataset_dir / "widget_rejected").is_dir()


def test_run_jsonlines_pipeline_fail_unknown_tables(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
) -> None:
    """Unknown tables fail before the shared runner creates pipeline state."""
    settings = schema_settings_factory(table_names=["unknown"])
    with pytest.raises(ValueError, match=r"Unknown table name\(s\) requested: \['unknown'\]"):
        run_jsonlines_pipeline(settings)
    assert list(Path(settings.output_dir).iterdir()) == []


def test_run_jsonlines_pipeline_fail_unconvertible_pyarrow_schema(
    schema_module_factory: Callable[[object], str],
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
) -> None:
    """Valid JSON Schemas that cannot convert to PyArrow fail before creating pipeline output."""
    module_name = schema_module_factory(
        {
            "map_root": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": {"type": "string"},
            }
        }
    )
    settings = schema_settings_factory(schema_files_module=module_name)
    with pytest.raises(RuntimeError, match="converting schemas to PyArrow"):
        run_jsonlines_pipeline(settings)
    assert list(Path(settings.output_dir).iterdir()) == []


@pytest.mark.parametrize(
    ("table_names", "expected"),
    [
        pytest.param(None, None, id="all-tables"),
        pytest.param("widget, gadget", ["widget", "gadget"], id="comma-separated"),
        pytest.param(["widget"], ["widget"], id="list"),
        pytest.param(" , ", None, id="blank"),
    ],
)
def test_settings_pass_table_selection(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    table_names: str | list[str] | None,
    expected: list[str] | None,
) -> None:
    """JSON Schema settings retain the shared table selection contract."""
    settings = schema_settings_factory(table_names=table_names)
    assert settings.table_names == expected
    assert settings.model_config["cli_prog_name"] == "jsonlines_jsonschema"


@pytest.mark.parametrize("buffer_size", [0, -1], ids=["zero", "negative"])
def test_settings_fail_invalid_buffer_size(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings], buffer_size: int
) -> None:
    """Settings reject nonpositive buffers before resource construction."""
    with pytest.raises(ValidationError, match="buffer_size"):
        schema_settings_factory(buffer_size=buffer_size)
