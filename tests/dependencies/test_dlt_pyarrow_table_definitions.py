"""Characterisation tests for PyArrow table and column definitions through dlt pipelines.

Checks what table metadata, column descriptions, types, nullability, field names, and row values
dlt keeps or drops across:
- Input mode: ``pa.Table`` passed straight to ``pipeline.run`` vs pages yielded from ``DictBuffer``.
- Destination format: ``loader_file_format="parquet"`` vs ``loader_file_format="iceberg"``
  (``loader_file_format="parquet", table_format="iceberg"``).
- Schema shape: flat scalar table vs four-level nested ``struct`` / ``list`` / ``map`` table,
  plus non-``snake_case`` column and nested field names.
"""

from collections.abc import Generator
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.libs.pyarrow import deserialize_type
from dlt.common.libs.pyiceberg import get_iceberg_tables
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    IcebergType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from cdm_data_loaders.core.buffer import DictBuffer
from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.utils.iceberg.cleanup import install_iceberg_catalog_cleanup
from specs._loader.dlt_contracts import (
    CAMEL_FLAT_ARROW_SCHEMA,
    CAMEL_FLAT_ROWS,
    CAMEL_NESTED_ARROW_SCHEMA,
    CAMEL_NESTED_ROWS,
    EXPECTED_CAMEL_FLAT_PARQUET_SCHEMA,
    EXPECTED_CAMEL_FLAT_ROWS,
    EXPECTED_CAMEL_NESTED_PARQUET_SCHEMA,
    EXPECTED_CAMEL_NESTED_ROWS,
    PYARROW_PROBE_DATASET_NAME,
    PYARROW_PROBE_TABLE_NAME,
    SHAPE_EXPECTATIONS,
    TABLE_METADATA_KEYS,
    TABLE_SHAPES,
    TableShape,
    decode_arrow_metadata,
    extract_arrow_field_metadata,
    with_parquet_compliant_list_elements,
)
from tests.helpers.table_readers import is_metadata_table


class InputMode(StrEnum):
    """How PyArrow data enters ``pipeline.run``."""

    DIRECT_TABLE = "direct_table"
    DICT_BUFFER = "dict_buffer"


INPUT_MODES: Final[tuple[InputMode, ...]] = (InputMode.DIRECT_TABLE, InputMode.DICT_BUFFER)
LOADER_FORMATS: Final[tuple[LoaderFileFormatEnum, ...]] = (
    LoaderFileFormatEnum.PARQUET,
    LoaderFileFormatEnum.ICEBERG,
)


def assert_arrow_schemas_equal_with_metadata(actual: pa.Schema, expected: pa.Schema) -> None:
    """Assert two PyArrow schemas match on names, types, nullability, and metadata at every depth.

    PyArrow's Parquet C++ reader names ``MapType``'s internal wrapper struct after the column rather
    than ``pa.map_``'s ``"entries"``, making ``Schema.equals(..., check_metadata=True)`` return
    ``False`` on schemas with ``pa.map_`` even when ``key_field``, ``item_field``, and all metadata match.

    :param actual: Actual ``pa.Schema``.
    :type actual: pa.Schema
    :param expected: Expected ``pa.Schema``.
    :type expected: pa.Schema
    """
    assert actual == expected
    assert decode_arrow_metadata(actual.metadata) == decode_arrow_metadata(expected.metadata)
    assert actual.names == expected.names

    def _assert_field_equal(actual_field: pa.Field, expected_field: pa.Field) -> None:
        assert actual_field.name == expected_field.name
        assert actual_field.nullable == expected_field.nullable
        assert decode_arrow_metadata(actual_field.metadata) == decode_arrow_metadata(expected_field.metadata)
        actual_type = actual_field.type
        expected_type = expected_field.type
        if pa.types.is_struct(actual_type):
            assert pa.types.is_struct(expected_type)
            assert len(actual_type) == len(expected_type)
            for actual_child, expected_child in zip(actual_type, expected_type, strict=True):
                _assert_field_equal(actual_child, expected_child)
        elif pa.types.is_list(actual_type) or pa.types.is_large_list(actual_type):
            assert type(actual_type) is type(expected_type)
            _assert_field_equal(actual_type.value_field, expected_type.value_field)
        elif pa.types.is_map(actual_type):
            assert pa.types.is_map(expected_type)
            assert actual_type.keys_sorted == expected_type.keys_sorted
            _assert_field_equal(actual_type.key_field, expected_type.key_field)
            _assert_field_equal(actual_type.item_field, expected_type.item_field)
        else:
            assert actual_field.equals(expected_field, check_metadata=True)

    for actual_top, expected_top in zip(actual, expected, strict=True):
        _assert_field_equal(actual_top, expected_top)


def extract_iceberg_field_docs(schema: IcebergSchema) -> dict[str, str | None]:
    """Collect ``NestedField.doc`` across root and nested struct fields in ``schema``.

    :param schema: Iceberg table schema.
    :type schema: IcebergSchema
    :return: Mapping from dotted field path to ``NestedField.doc``.
    :rtype: dict[str, str | None]
    """
    docs: dict[str, str | None] = {}

    def _visit_type(iceberg_type: IcebergType, prefix: str) -> None:
        if isinstance(iceberg_type, StructType):
            for child in iceberg_type.fields:
                child_path = f"{prefix}.{child.name}" if prefix else child.name
                docs[child_path] = child.doc
                _visit_type(child.field_type, child_path)
        elif isinstance(iceberg_type, ListType):
            _visit_type(iceberg_type.element_type, f"{prefix}.element")
        elif isinstance(iceberg_type, MapType):
            _visit_type(iceberg_type.key_type, f"{prefix}.key")
            _visit_type(iceberg_type.value_type, f"{prefix}.value")

    _visit_type(schema.as_struct(), "")
    return docs


def build_dict_buffer_resource(
    rows: tuple[dict[str, Any], ...],
    schema: pa.Schema,
    table_name: str,
) -> DltResource:
    """Build a minimal ``@dlt.resource`` that yields ``pa.Table`` pages via ``DictBuffer``.

    :param rows: Input row dicts matching ``schema``.
    :type rows: tuple[dict[str, Any], ...]
    :param schema: Target ``pa.Schema`` registered in ``ArrowSchemaRegistry``.
    :type schema: pa.Schema
    :param table_name: Target table name for ``DictBuffer`` routing.
    :type table_name: str
    :return: Configured ``DltResource``.
    :rtype: DltResource
    """

    @dlt.resource(name=f"{table_name}_buffered")
    def _buffered_rows() -> Generator[DataItemWithMeta]:
        # max_items=1 exercises both per-item page emission and final flush
        buffer = DictBuffer(
            max_items=1,
            schema_map=ArrowSchemaRegistry(registry={table_name: schema}),
        )
        for row in rows[:-1]:
            yield from buffer.add_item(table_name, dict(row))
        if rows:
            buffer.add_items({table_name: []})
            yield from buffer.add_item(table_name, dict(rows[-1]))
        yield from buffer.flush()

    return _buffered_rows


def run_simple_pyarrow_pipeline(
    tmp_path: Path,
    *,
    rows: tuple[dict[str, Any], ...],
    schema: pa.Schema,
    input_mode: InputMode,
    loader_file_format: LoaderFileFormatEnum,
    table_name: str = PYARROW_PROBE_TABLE_NAME,
) -> tuple[dlt.Pipeline, Path]:
    """Run a minimal ``dlt`` filesystem pipeline for a PyArrow schema and format.

    :param tmp_path: Temporary directory for pipeline state and destination bucket.
    :type tmp_path: Path
    :param rows: Input rows matching ``schema``.
    :type rows: tuple[dict[str, Any], ...]
    :param schema: PyArrow schema with table and field metadata.
    :type schema: pa.Schema
    :param input_mode: ``DIRECT_TABLE`` or ``DICT_BUFFER``.
    :type input_mode: InputMode
    :param loader_file_format: ``PARQUET`` or ``ICEBERG``.
    :type loader_file_format: LoaderFileFormatEnum
    :param table_name: Destination table name.
    :type table_name: str
    :return: Tuple of ``(pipeline, bucket_dir)``.
    :rtype: tuple[dlt.Pipeline, Path]
    """
    pipelines_dir = tmp_path / "pipelines"
    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name=f"defs_{table_name}_{input_mode}_{loader_file_format}",
        destination=dlt.destinations.filesystem(bucket_url=str(bucket_dir)),
        dataset_name=PYARROW_PROBE_DATASET_NAME,
        pipelines_dir=str(pipelines_dir),
    )

    run_kwargs: dict[str, Any] = (
        {"loader_file_format": str(LoaderFileFormatEnum.PARQUET), "table_format": str(LoaderFileFormatEnum.ICEBERG)}
        if loader_file_format == LoaderFileFormatEnum.ICEBERG
        else {"loader_file_format": str(loader_file_format)}
    )
    if loader_file_format == LoaderFileFormatEnum.ICEBERG:
        install_iceberg_catalog_cleanup()

    if input_mode == InputMode.DIRECT_TABLE:
        arrow_table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
        load_info = pipeline.run(arrow_table, table_name=table_name, **run_kwargs)
    else:
        resource = build_dict_buffer_resource(rows, schema, table_name)
        load_info = pipeline.run(resource, **run_kwargs)

    assert not load_info.has_failed_jobs
    assert len(load_info.loads_ids) == 1
    return pipeline, bucket_dir


def assert_dlt_schema_definitions(
    pipeline: dlt.Pipeline,
    *,
    input_schema: pa.Schema,
    expected_columns: dict[str, dict[str, Any]],
    table_name: str = PYARROW_PROBE_TABLE_NAME,
) -> None:
    """Assert what ``pipeline.default_schema`` keeps and drops from ``input_schema``.

    :param pipeline: Executed ``dlt.Pipeline``.
    :type pipeline: dlt.Pipeline
    :param input_schema: Source ``pa.Schema`` (after any expected top-level name normalization).
    :type input_schema: pa.Schema
    :param expected_columns: Expected ``dlt`` column dicts excluding ``x-nested-type``.
    :type expected_columns: dict[str, dict[str, Any]]
    :param table_name: Expected ``dlt`` table name.
    :type table_name: str
    """
    data_tables = [name for name in pipeline.default_schema.tables if not is_metadata_table(name)]
    assert data_tables == [table_name]

    table_def = pipeline.default_schema.tables[table_name]
    # dlt ignores pa.Schema.metadata; neither table description nor Iceberg table properties are set
    assert table_def.get("description") is None
    assert "x-iceberg-table-properties" not in table_def

    columns = table_def["columns"]
    assert list(columns) == list(expected_columns)

    for col_name, col_def in columns.items():
        # dlt ignores top-level pa.Field.metadata when building TTableSchemaColumns
        assert col_def.get("description") is None
        comparable = {k: v for k, v in col_def.items() if k != "x-nested-type"}
        assert comparable == expected_columns[col_name]

        input_field = input_schema.field(col_name)
        if pa.types.is_nested(input_field.type):
            nested_hint = col_def["x-nested-type"]
            assert isinstance(nested_hint, str)
            assert nested_hint.startswith("arrow-ipc:")
            # nested Arrow IPC type hint keeps inner field names, types, nullability, and inner pa.Field.metadata
            roundtripped_type = deserialize_type(nested_hint)
            assert roundtripped_type.equals(input_field.type, check_metadata=True)
        else:
            assert "x-nested-type" not in col_def


def assert_parquet_definitions(
    bucket_dir: Path,
    *,
    expected_schema: pa.Schema,
    expected_table_metadata: dict[str, str],
    expected_rows: list[dict[str, Any]],
    table_name: str = PYARROW_PROBE_TABLE_NAME,
) -> None:
    """Assert Parquet output preserves schema metadata, recursive field metadata, types, and rows.

    :param bucket_dir: Filesystem destination root directory.
    :type bucket_dir: Path
    :param expected_schema: Expected ``pa.Schema`` including table and field metadata.
    :type expected_schema: pa.Schema
    :param expected_table_metadata: Expected decoded ``pa.Schema.metadata``.
    :type expected_table_metadata: dict[str, str]
    :param expected_rows: Expected table rows.
    :type expected_rows: list[dict[str, Any]]
    :param table_name: Target table directory name.
    :type table_name: str
    """
    dataset_dir = bucket_dir / PYARROW_PROBE_DATASET_NAME
    table_dirs = [p.name for p in sorted(dataset_dir.iterdir()) if p.is_dir() and not is_metadata_table(p.name)]
    assert table_dirs == [table_name]

    parquet_files = sorted((dataset_dir / table_name).glob("*.parquet"))
    assert len(parquet_files) == 1

    expected_parquet_schema = with_parquet_compliant_list_elements(expected_schema)
    pq_schema = pq.read_schema(parquet_files[0])
    assert decode_arrow_metadata(pq_schema.metadata) == expected_table_metadata
    assert extract_arrow_field_metadata(pq_schema) == extract_arrow_field_metadata(expected_parquet_schema)
    assert_arrow_schemas_equal_with_metadata(pq_schema, expected_parquet_schema)

    loaded_rows = pq.read_table(parquet_files[0]).to_pylist()
    assert loaded_rows == expected_rows


def assert_iceberg_definitions(
    pipeline: dlt.Pipeline,
    bucket_dir: Path,
    *,
    expected_iceberg_schema: IcebergSchema,
    expected_iceberg_docs: dict[str, str | None],
    expected_scan_arrow_schema: pa.Schema,
    expected_rows: list[dict[str, Any]],
    table_name: str = PYARROW_PROBE_TABLE_NAME,
) -> None:
    """Assert what Iceberg output keeps and drops across catalog metadata, Arrow scan, and data files.

    :param pipeline: Executed ``dlt.Pipeline``.
    :type pipeline: dlt.Pipeline
    :param bucket_dir: Filesystem destination root directory.
    :type bucket_dir: Path
    :param expected_iceberg_schema: Expected ``IcebergSchema`` including ``NestedField.doc``.
    :type expected_iceberg_schema: IcebergSchema
    :param expected_iceberg_docs: Expected ``{dotted_path: doc}`` mapping across root and struct fields.
    :type expected_iceberg_docs: dict[str, str | None]
    :param expected_scan_arrow_schema: Expected ``table.scan().to_arrow().schema`` with metadata.
    :type expected_scan_arrow_schema: pa.Schema
    :param expected_rows: Expected table rows.
    :type expected_rows: list[dict[str, Any]]
    :param table_name: Target Iceberg table name.
    :type table_name: str
    """
    iceberg_tables = get_iceberg_tables(pipeline)
    try:
        assert set(iceberg_tables) == {table_name}
        table = iceberg_tables[table_name]

        # pa.Schema.metadata is not forwarded to Iceberg table properties
        assert TABLE_METADATA_KEYS.isdisjoint(table.properties)

        iceberg_schema = table.schema()
        assert extract_iceberg_field_docs(iceberg_schema) == expected_iceberg_docs
        assert iceberg_schema == expected_iceberg_schema

        arrow_table = table.scan().to_arrow()
        scan_schema = arrow_table.schema
        assert decode_arrow_metadata(scan_schema.metadata) == {}
        assert extract_arrow_field_metadata(scan_schema) == extract_arrow_field_metadata(expected_scan_arrow_schema)
        assert_arrow_schemas_equal_with_metadata(scan_schema, expected_scan_arrow_schema)
        assert scan_schema.equals(expected_scan_arrow_schema, check_metadata=True)
        assert arrow_table.to_pylist() == expected_rows
    finally:
        for table in iceberg_tables.values():
            table.catalog.close()

    # parquet data files rewritten by PyIceberg drop pa.Schema.metadata and tag fields with PARQUET:field_id
    data_files = sorted((bucket_dir / PYARROW_PROBE_DATASET_NAME / table_name / "data").glob("*.parquet"))
    assert len(data_files) == 1
    data_file_schema = pq.read_schema(data_files[0])
    assert decode_arrow_metadata(data_file_schema.metadata) == {}
    assert "PARQUET:field_id" in decode_arrow_metadata(data_file_schema.field(0).metadata)


@pytest.mark.parametrize("loader_file_format", LOADER_FORMATS, ids=[fmt.value for fmt in LOADER_FORMATS])
@pytest.mark.parametrize("table_shape", TABLE_SHAPES, ids=[shape.value for shape in TABLE_SHAPES])
@pytest.mark.parametrize("input_mode", INPUT_MODES, ids=[mode.value for mode in INPUT_MODES])
def test_dlt_pyarrow_table_and_column_definitions(
    tmp_path: Path,
    input_mode: InputMode,
    table_shape: TableShape,
    loader_file_format: LoaderFileFormatEnum,
) -> None:
    """Check what table and column definitions survive across direct vs DictBuffer, flat vs deep nested, and Parquet vs Iceberg."""
    spec = SHAPE_EXPECTATIONS[table_shape]
    # every field at every depth in the input schema carries non-empty metadata
    assert all(extract_arrow_field_metadata(spec.input_arrow_schema).values())
    pipeline, bucket_dir = run_simple_pyarrow_pipeline(
        tmp_path,
        rows=spec.rows,
        schema=spec.input_arrow_schema,
        input_mode=input_mode,
        loader_file_format=loader_file_format,
    )

    assert_dlt_schema_definitions(
        pipeline,
        input_schema=spec.input_arrow_schema,
        expected_columns=spec.expected_dlt_columns,
    )

    if loader_file_format == LoaderFileFormatEnum.PARQUET:
        assert_parquet_definitions(
            bucket_dir,
            expected_schema=spec.input_arrow_schema,
            expected_table_metadata=spec.table_metadata,
            expected_rows=list(spec.rows),
        )
    else:
        assert_iceberg_definitions(
            pipeline,
            bucket_dir,
            expected_iceberg_schema=spec.expected_iceberg_schema,
            expected_iceberg_docs=spec.expected_iceberg_docs,
            expected_scan_arrow_schema=spec.expected_iceberg_scan_arrow_schema,
            expected_rows=list(spec.rows),
        )


@pytest.mark.parametrize("loader_file_format", LOADER_FORMATS, ids=[fmt.value for fmt in LOADER_FORMATS])
@pytest.mark.parametrize("table_shape", TABLE_SHAPES, ids=[shape.value for shape in TABLE_SHAPES])
@pytest.mark.parametrize("input_mode", INPUT_MODES, ids=[mode.value for mode in INPUT_MODES])
def test_dlt_pyarrow_identifier_normalization_definitions(
    tmp_path: Path,
    input_mode: InputMode,
    table_shape: TableShape,
    loader_file_format: LoaderFileFormatEnum,
) -> None:
    """Check dlt snake_case normalization renames root Arrow columns while keeping nested struct field names and metadata rules."""
    is_flat = table_shape == TableShape.FLAT
    input_schema = CAMEL_FLAT_ARROW_SCHEMA if is_flat else CAMEL_NESTED_ARROW_SCHEMA
    input_rows = CAMEL_FLAT_ROWS if is_flat else CAMEL_NESTED_ROWS
    expected_parquet_schema = EXPECTED_CAMEL_FLAT_PARQUET_SCHEMA if is_flat else EXPECTED_CAMEL_NESTED_PARQUET_SCHEMA
    expected_rows = EXPECTED_CAMEL_FLAT_ROWS if is_flat else EXPECTED_CAMEL_NESTED_ROWS

    pipeline, bucket_dir = run_simple_pyarrow_pipeline(
        tmp_path,
        rows=input_rows,
        schema=input_schema,
        input_mode=input_mode,
        loader_file_format=loader_file_format,
    )

    expected_dlt_columns: dict[str, dict[str, Any]] = (
        {
            "entry_id": {"name": "entry_id", "data_type": "bigint", "nullable": False},
            "_raw_code": {"name": "_raw_code", "data_type": "text", "nullable": True},
        }
        if is_flat
        else {
            "entry_id": {"name": "entry_id", "data_type": "bigint", "nullable": False},
            "outer_block": {"name": "outer_block", "data_type": "json", "nullable": False},
        }
    )
    assert_dlt_schema_definitions(
        pipeline,
        input_schema=expected_parquet_schema,
        expected_columns=expected_dlt_columns,
    )

    if loader_file_format == LoaderFileFormatEnum.PARQUET:
        assert_parquet_definitions(
            bucket_dir,
            expected_schema=expected_parquet_schema,
            expected_table_metadata=decode_arrow_metadata(input_schema.metadata),
            expected_rows=expected_rows,
        )
    elif is_flat:
        assert_iceberg_definitions(
            pipeline,
            bucket_dir,
            expected_iceberg_schema=IcebergSchema(
                NestedField(field_id=1, name="entry_id", field_type=LongType(), required=True, doc="Entry ID doc"),
                NestedField(field_id=2, name="_raw_code", field_type=StringType(), required=False, doc=None),
            ),
            expected_iceberg_docs={"entry_id": "Entry ID doc", "_raw_code": None},
            expected_scan_arrow_schema=pa.schema(
                [
                    pa.field("entry_id", pa.int64(), nullable=False, metadata={"doc": "Entry ID doc"}),
                    pa.field("_raw_code", pa.string(), nullable=True),
                ]
            ),
            expected_rows=expected_rows,
        )
    else:
        assert_iceberg_definitions(
            pipeline,
            bucket_dir,
            expected_iceberg_schema=IcebergSchema(
                NestedField(field_id=1, name="entry_id", field_type=LongType(), required=True, doc="Entry ID doc"),
                NestedField(
                    field_id=2,
                    name="outer_block",
                    field_type=StructType(
                        NestedField(
                            field_id=3,
                            name="camelChild",
                            field_type=StringType(),
                            required=True,
                            doc="Child doc",
                        ),
                        NestedField(
                            field_id=4,
                            name="deepList",
                            field_type=ListType(
                                element_id=5,
                                element_type=StructType(
                                    NestedField(
                                        field_id=6,
                                        name="_innerCode",
                                        field_type=LongType(),
                                        required=True,
                                        doc="Inner doc",
                                    ),
                                ),
                                element_required=True,
                            ),
                            required=True,
                            doc=None,
                        ),
                    ),
                    required=True,
                    doc="Outer block doc",
                ),
            ),
            expected_iceberg_docs={
                "entry_id": "Entry ID doc",
                "outer_block": "Outer block doc",
                "outer_block.camelChild": "Child doc",
                "outer_block.deepList": None,
                "outer_block.deepList.element._innerCode": "Inner doc",
            },
            expected_scan_arrow_schema=pa.schema(
                [
                    pa.field("entry_id", pa.int64(), nullable=False, metadata={"doc": "Entry ID doc"}),
                    pa.field(
                        "outer_block",
                        pa.struct(
                            [
                                pa.field("camelChild", pa.string(), nullable=False, metadata={"doc": "Child doc"}),
                                pa.field(
                                    "deepList",
                                    pa.list_(
                                        pa.field(
                                            "element",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "_innerCode",
                                                        pa.int64(),
                                                        nullable=False,
                                                        metadata={"doc": "Inner doc"},
                                                    )
                                                ]
                                            ),
                                            nullable=False,
                                        )
                                    ),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                        metadata={"doc": "Outer block doc"},
                    ),
                ]
            ),
            expected_rows=expected_rows,
        )
