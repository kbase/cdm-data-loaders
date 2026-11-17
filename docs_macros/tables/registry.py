"""Canonical registry mapping reference table identifiers to headers and row builders."""

from collections.abc import Callable
from types import MappingProxyType
from typing import Final

from docs_macros.tables.converter_scalars import (
    build_jsonschema_scalar_rows,
    build_pyarrow_to_jsonschema_rows,
    build_pyarrow_to_linkml_rows,
    build_pydantic_scalar_rows,
    build_xsd_scalar_rows,
)
from docs_macros.tables.dlt_nested import (
    build_dlt_nested_placement_rows,
    build_dlt_nested_single_table_column_rows,
    build_dlt_unnested_table_rows,
)
from docs_macros.tables.dlt_pyarrow import (
    build_dlt_pyarrow_flat_column_rows,
    build_dlt_pyarrow_identifier_rows,
    build_dlt_pyarrow_nested_dlt_column_rows,
    build_dlt_pyarrow_nested_path_rows,
)
from docs_macros.tables.models import TableRenderContext
from docs_macros.tables.pipeline_tables import (
    build_jsonlines_jsonschema_draft_rows,
    build_rejected_schema_rows,
    build_xmltopyarrow_preset_rows,
    build_xsv_column_type_rows,
    build_xsv_config_rows,
)

TABLE_TEMPLATE_REGISTRY: Final[MappingProxyType[str, tuple[tuple[str, ...], Callable[[], tuple[object, ...]]]]] = (
    MappingProxyType(
        {
            "xsd_scalar_table": (
                ("XSD built-in type(s)", "Python type (coerced)", "PyArrow `DataType`"),
                build_xsd_scalar_rows,
            ),
            "jsonschema_scalar_table": (
                ("JSON Schema", "Python type (coerced)", "PyArrow `DataType`", '`metadata["jsonschema_type"]`'),
                build_jsonschema_scalar_rows,
            ),
            "pydantic_scalar_table": (
                (
                    "Pydantic type / annotation",
                    "Python type (coerced)",
                    "PyArrow `DataType`",
                    '`metadata["pydantic_type"]`',
                ),
                build_pydantic_scalar_rows,
            ),
            "pyarrow_to_jsonschema_scalar_table": (
                ("PyArrow `DataType`", "Serialized JSON value", "JSON Schema (core)", "`x-pyarrow`"),
                build_pyarrow_to_jsonschema_rows,
            ),
            "pyarrow_to_linkml_scalar_table": (
                (
                    "PyArrow `DataType`",
                    "Serialized JSON value",
                    "LinkML `range`",
                    "Slot bounds (`minimum_value` .. `maximum_value`)",
                ),
                build_pyarrow_to_linkml_rows,
            ),
            "xsv_config_table": (
                ("`x-xsv-config` key", "JSON Schema constraint", "`CleanerValidatorArgs` field", "Default"),
                build_xsv_config_rows,
            ),
            "column_type_table": (
                (
                    "Column JSON Schema",
                    "DuckDB CSV `dtype`",
                    "`force_not_null`",
                    "PyArrow `DataType`",
                    "`Field.nullable`",
                ),
                build_xsv_column_type_rows,
            ),
            "presets_table": (
                (
                    "CLI Command",
                    "Settings Class",
                    "`dataset_name`",
                    "Target Selection",
                    "`file_glob`",
                    "Default Pre-processors",
                ),
                build_xmltopyarrow_preset_rows,
            ),
            "supported_drafts_table": (
                ("Declared `$schema` URI", "Validator Class", "Format Checking"),
                build_jsonlines_jsonschema_draft_rows,
            ),
            "rejected_schema_table": (
                ("Column", "PyArrow `DataType`", "`Field.nullable`", "Description"),
                build_rejected_schema_rows,
            ),
            "dlt_pyarrow_flat_columns_table": (
                (
                    "Column",
                    "Input PyArrow Field",
                    "Input Metadata Keys",
                    "`dlt` Column",
                    "Parquet Field",
                    "Iceberg `NestedField`",
                    "Iceberg Scan (`to_arrow()`)",
                ),
                build_dlt_pyarrow_flat_column_rows,
            ),
            "dlt_pyarrow_nested_dlt_columns_table": (
                (
                    "Column",
                    "Input PyArrow Field",
                    "`dlt` Column",
                    "Arrow IPC Hint (`deserialize_type`)",
                ),
                build_dlt_pyarrow_nested_dlt_column_rows,
            ),
            "dlt_pyarrow_nested_paths_table": (
                (
                    "Input Path",
                    "Slot Kind",
                    "Input Type and Metadata",
                    "Parquet Type and Metadata",
                    "Iceberg Slot and `doc`",
                    "Iceberg Scan (`to_arrow()`)",
                ),
                build_dlt_pyarrow_nested_path_rows,
            ),
            "dlt_pyarrow_identifier_normalization_table": (
                (
                    "Fixture",
                    "Input Path",
                    "Slot Kind",
                    "`dlt` Path (`default_schema` / `x-nested-type`)",
                    "Parquet Path",
                    "Iceberg Path and `doc`",
                ),
                build_dlt_pyarrow_identifier_rows,
            ),
            "dlt_nested_placement_matrix_table": (
                (
                    "Placement",
                    "Where `max_table_nesting=0` Is Set",
                    "Python `dict` Outcome",
                    "`pyarrow.Table` Outcome",
                ),
                build_dlt_nested_placement_rows,
            ),
            "dlt_nested_single_table_columns_table": (
                (
                    "Column",
                    "`dict` (`nesting=0`): `dlt` / Parquet / Iceberg",
                    "`dict` (`nesting=0`): JSONL",
                    "`pyarrow.Table`: `dlt` / Parquet / Iceberg",
                    "`pyarrow.Table`: JSONL (`ArrowToJsonlWriter`)",
                ),
                build_dlt_nested_single_table_column_rows,
            ),
            "dlt_nested_unnested_tables_table": (
                (
                    "Generated Table",
                    "Source Path and Role",
                    "`dlt` Columns",
                    "Parquet Schema",
                    "Iceberg Schema",
                ),
                build_dlt_unnested_table_rows,
            ),
        }
    )
)


def build_table_context(table_id: str) -> TableRenderContext:
    """Resolve ``table_id`` in ``TABLE_TEMPLATE_REGISTRY`` into a ``TableRenderContext``.

    :param table_id: Canonical reference table identifier.
    :type table_id: str
    :return: Bound ``TableRenderContext`` with headers and extracted rows.
    :rtype: TableRenderContext
    """
    if table_id not in TABLE_TEMPLATE_REGISTRY:
        msg = f"Unknown reference table identifier: {table_id!r}"
        raise KeyError(msg)
    headers, builder = TABLE_TEMPLATE_REGISTRY[table_id]
    return TableRenderContext(table_id=table_id, headers=headers, rows=builder())
