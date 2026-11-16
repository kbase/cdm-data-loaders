"""Structured row models and live schema-introspection builders for dlt nested output tables."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import IcebergType, ListType, MapType, StructType

from docs_macros.formatting import FrozenDocModel, escape_table_cell, pa_ctor_expr
from tests.dependencies.test_dlt_nested_output_behaviour import (
    ALL_NESTING_PLACEMENTS,
    ARROW_SCHEMA,
    COMPLEX_COLUMNS,
    EXPECTED_ARROW_SCHEMA_DICT_NESTING_0,
    EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED,
    EXPECTED_DLT_COLUMNS_DICT_NESTING_0,
    EXPECTED_DLT_COLUMNS_DICT_UNNESTED,
    EXPECTED_DLT_COLUMNS_PYARROW,
    EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0,
    EXPECTED_ICEBERG_SCHEMA_PYARROW,
    EXPECTED_ICEBERG_SCHEMAS_DICT_UNNESTED,
    EXPECTED_JSON_STRING_ROWS,
    EXPECTED_PARSED_JSON_ROWS,
    NESTING_ZERO_PLACEMENTS,
    TABLE_NAME,
    NestingPlacement,
)

PLACEMENT_CONFIG_LABELS: Final[Mapping[NestingPlacement, str]] = MappingProxyType(
    {
        NestingPlacement.RESOURCE_AND_TRANSFORMER: (
            "`@dlt.resource(max_table_nesting=0)` and `@dlt.transformer(max_table_nesting=0)`"
        ),
        NestingPlacement.TRANSFORMER_ONLY: "`@dlt.transformer(max_table_nesting=0)`",
        NestingPlacement.DESTINATION: "`dlt.destinations.filesystem(..., max_table_nesting=0)`",
        NestingPlacement.RESOURCE_ONLY: "`@dlt.resource(max_table_nesting=0)` (omitted on `@dlt.transformer`)",
        NestingPlacement.UNSET: "Omitted on resource, transformer, and destination",
    }
)

UNNESTED_TABLE_ROLES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "widget": "Root table; flattens `address.city` and `address.zip` into `__`-joined columns",
        "widget__address__street": "Scalar `list` items from `address.street`",
        "widget__children": "Struct `list` items from `children` (`k`, `v` columns)",
        "widget__metrics": "Outer `list` link table for `metrics` `(key, value)` 2-tuples",
        "widget__metrics__list": "Inner tuple elements from `metrics`, coerced to `text` (`'score'`, `'98.5'`)",
        "widget__tags": "Scalar `list` items from `tags`",
    }
)


class DltNestedPlacementRow(FrozenDocModel):
    """Row model for the ``max_table_nesting`` placement behaviour matrix."""

    placement: str
    configured_on: str
    dict_outcome: str
    pyarrow_outcome: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            f"`{escape_table_cell(self.placement)}`",
            self.configured_on,
            self.dict_outcome,
            self.pyarrow_outcome,
        )


class DltNestedSingleTableColumnRow(FrozenDocModel):
    """Row model comparing single-table columns between ``dict`` (nesting=0) and ``pyarrow.Table``."""

    column_name: str
    dict_parquet_iceberg: str
    dict_jsonl: str
    pyarrow_parquet_iceberg: str
    pyarrow_jsonl: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            f"`{escape_table_cell(self.column_name)}`",
            self.dict_parquet_iceberg,
            self.dict_jsonl,
            self.pyarrow_parquet_iceberg,
            self.pyarrow_jsonl,
        )


class DltUnnestedTableRow(FrozenDocModel):
    """Row model summarizing one unnested table generated for ``dict`` items."""

    table_name: str
    role_summary: str
    dlt_columns_summary: str
    parquet_schema_summary: str
    iceberg_schema_summary: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            f"`{escape_table_cell(self.table_name)}`",
            self.role_summary,
            self.dlt_columns_summary,
            self.parquet_schema_summary,
            self.iceberg_schema_summary,
        )


def format_compact_arrow_type(dtype: pa.DataType) -> str:
    """Format a scalar or nested PyArrow ``DataType`` with concise child field summaries.

    :param dtype: PyArrow data type to summarize.
    :type dtype: pa.DataType
    :return: Concise ``pa.*`` expression string.
    :rtype: str
    """
    if pa.types.is_struct(dtype):
        names = ", ".join(child.name for child in dtype)
        return f"pa.struct([{names}])"
    if pa.types.is_list(dtype):
        return f"pa.list_({format_compact_arrow_type(dtype.value_type)})"
    if pa.types.is_map(dtype):
        key_expr = format_compact_arrow_type(dtype.key_type)
        item_expr = format_compact_arrow_type(dtype.item_type)
        return f"pa.map_({key_expr}, {item_expr})"
    return pa_ctor_expr(dtype)


def format_compact_iceberg_type(iceberg_type: IcebergType) -> str:
    """Format a scalar or nested PyIceberg ``IcebergType`` with concise child summaries.

    :param iceberg_type: Iceberg type to summarize.
    :type iceberg_type: IcebergType
    :return: Concise Iceberg type label.
    :rtype: str
    """
    if isinstance(iceberg_type, StructType):
        names = ", ".join(field.name for field in iceberg_type.fields)
        return f"StructType({names})"
    if isinstance(iceberg_type, ListType):
        return f"ListType({format_compact_iceberg_type(iceberg_type.element_type)})"
    if isinstance(iceberg_type, MapType):
        key_label = format_compact_iceberg_type(iceberg_type.key_type)
        val_label = format_compact_iceberg_type(iceberg_type.value_type)
        return f"MapType({key_label}, {val_label})"
    return f"{type(iceberg_type).__name__}()"


def summarize_json_value_sample(col_name: str, parsed_row: Mapping[str, Any], *, is_dlt_meta: bool) -> str:
    """Format the JSONL value representation for ``col_name`` from ``parsed_row``.

    :param col_name: Column name in the output record.
    :type col_name: str
    :param parsed_row: Sample parsed JSONL row dictionary.
    :type parsed_row: Mapping[str, Any]
    :param is_dlt_meta: Whether ``col_name`` is a ``_dlt_*`` system column.
    :type is_dlt_meta: bool
    :return: Concise Markdown description of the JSONL value.
    :rtype: str
    """
    if is_dlt_meta:
        return "Non-empty `str` (`_dlt` metadata)"
    sample = parsed_row[col_name]
    if isinstance(sample, dict):
        return "Native JSON `object`"
    if isinstance(sample, list):
        if col_name == "metrics":
            return f"JSON `array` of `[key, value]` pairs (`{escape_table_cell(repr(sample))}`)"
        return "Native JSON `array`"
    return f"JSON `{type(sample).__name__}` (`{escape_table_cell(repr(sample))}`)"


def format_arrow_schema_fields(schema: pa.Schema) -> str:
    """Format all fields of ``schema`` as comma-separated ``name: type`` code spans.

    :param schema: PyArrow table schema.
    :type schema: pa.Schema
    :return: Markdown summary of field names, types, and non-null flags.
    :rtype: str
    """
    parts: list[str] = []
    for field in schema:
        type_str = format_compact_arrow_type(field.type)
        suffix = " (not null)" if not field.nullable else ""
        parts.append(f"`{escape_table_cell(f'{field.name}: {type_str}')}`{suffix}")
    return ", ".join(parts)


def format_iceberg_schema_fields(schema: IcebergSchema) -> str:
    """Format all root fields of ``schema`` as comma-separated ``name: Type`` code spans.

    :param schema: PyIceberg table schema.
    :type schema: IcebergSchema
    :return: Markdown summary of field names, types, and required flags.
    :rtype: str
    """
    parts: list[str] = []
    for field in schema.fields:
        type_str = format_compact_iceberg_type(field.field_type)
        suffix = " (required)" if field.required else ""
        parts.append(f"`{escape_table_cell(f'{field.name}: {type_str}')}`{suffix}")
    return ", ".join(parts)


def build_dlt_nested_placement_rows() -> tuple[DltNestedPlacementRow, ...]:
    """Build placement matrix rows across ``ALL_NESTING_PLACEMENTS``.

    :return: Tuple of ``DltNestedPlacementRow`` records.
    :rtype: tuple[DltNestedPlacementRow, ...]
    """
    zero_set = frozenset(NESTING_ZERO_PLACEMENTS)
    unnested_count = len(EXPECTED_DLT_COLUMNS_DICT_UNNESTED)
    single_dict_count = len(EXPECTED_DLT_COLUMNS_DICT_NESTING_0)
    pyarrow_count = len(EXPECTED_DLT_COLUMNS_PYARROW)
    pyarrow_summary = (
        f"`{pyarrow_count}` table (`{TABLE_NAME}`); native Arrow nested types kept (`max_table_nesting` ignored)"
    )

    rows: list[DltNestedPlacementRow] = []
    for placement in ALL_NESTING_PLACEMENTS:
        if placement in zero_set:
            dict_summary = (
                f"`{single_dict_count}` table (`{TABLE_NAME}`); complex fields typed as `json` "
                "(JSON strings in Parquet/Iceberg, native JSON in JSONL)"
            )
        elif placement == NestingPlacement.RESOURCE_ONLY:
            dict_summary = (
                f"`{unnested_count}` tables (`{TABLE_NAME}` + `{unnested_count - 1}` child tables); "
                "resource hint does not propagate to `@dlt.transformer`"
            )
        else:
            dict_summary = (
                f"`{unnested_count}` tables (`{TABLE_NAME}` + `{unnested_count - 1}` child tables); "
                "flattens `struct` scalars and splits `list` fields"
            )
        rows.append(
            DltNestedPlacementRow(
                placement=placement.value,
                configured_on=PLACEMENT_CONFIG_LABELS[placement],
                dict_outcome=dict_summary,
                pyarrow_outcome=pyarrow_summary,
            )
        )
    return tuple(rows)


def build_dlt_nested_single_table_column_rows() -> tuple[DltNestedSingleTableColumnRow, ...]:
    """Compare root ``widget`` columns between ``dict`` (``max_table_nesting=0``) and ``pyarrow.Table``.

    :return: Tuple of ``DltNestedSingleTableColumnRow`` records.
    :rtype: tuple[DltNestedSingleTableColumnRow, ...]
    """
    dict_dlt_cols = EXPECTED_DLT_COLUMNS_DICT_NESTING_0[TABLE_NAME]
    pa_dlt_cols = EXPECTED_DLT_COLUMNS_PYARROW[TABLE_NAME]
    complex_set = frozenset(COMPLEX_COLUMNS)
    sample_json_str_row = EXPECTED_JSON_STRING_ROWS[0]
    sample_parsed_row = EXPECTED_PARSED_JSON_ROWS[0]
    pyarrow_names = frozenset(ARROW_SCHEMA.names)

    rows: list[DltNestedSingleTableColumnRow] = []
    for dict_field in EXPECTED_ARROW_SCHEMA_DICT_NESTING_0:
        col_name = dict_field.name
        is_meta = col_name not in pyarrow_names
        dict_ice_field = EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0.find_field(col_name)
        dict_dlt_type = dict_dlt_cols[col_name]
        dict_arrow_expr = format_compact_arrow_type(dict_field.type)
        dict_ice_expr = format_compact_iceberg_type(dict_ice_field.field_type)

        if col_name in complex_set:
            sample_str = escape_table_cell(repr(sample_json_str_row[col_name]))
            dict_pq_ice = (
                f"`{dict_dlt_type}` / `{dict_arrow_expr}` / `{dict_ice_expr}` (JSON string, e.g. `{sample_str}`)"
            )
        elif is_meta:
            dict_pq_ice = (
                f"`{dict_dlt_type}` / `{dict_arrow_expr}` (`nullable=False`) / `{dict_ice_expr}` (`required=True`)"
            )
        else:
            dict_pq_ice = f"`{dict_dlt_type}` / `{dict_arrow_expr}` / `{dict_ice_expr}`"

        dict_jsonl = summarize_json_value_sample(col_name, sample_parsed_row, is_dlt_meta=is_meta)

        if is_meta:
            pa_pq_ice = "*(not added)*"
            pa_jsonl = "*(not added)*"
        else:
            pa_field = ARROW_SCHEMA.field(col_name)
            pa_ice_field = EXPECTED_ICEBERG_SCHEMA_PYARROW.find_field(col_name)
            pa_dlt_type = pa_dlt_cols[col_name]
            pa_arrow_expr = escape_table_cell(format_compact_arrow_type(pa_field.type))
            pa_ice_expr = escape_table_cell(format_compact_iceberg_type(pa_ice_field.field_type))
            dlt_label = f"`{pa_dlt_type}` (`x-nested-type`)" if col_name in complex_set else f"`{pa_dlt_type}`"
            pa_pq_ice = f"{dlt_label} / `{pa_arrow_expr}` / `{pa_ice_expr}`"
            pa_jsonl = summarize_json_value_sample(col_name, sample_parsed_row, is_dlt_meta=False)

        rows.append(
            DltNestedSingleTableColumnRow(
                column_name=col_name,
                dict_parquet_iceberg=dict_pq_ice,
                dict_jsonl=dict_jsonl,
                pyarrow_parquet_iceberg=pa_pq_ice,
                pyarrow_jsonl=pa_jsonl,
            )
        )
    return tuple(rows)


def build_dlt_unnested_table_rows() -> tuple[DltUnnestedTableRow, ...]:
    """Extract schemas across all unnested tables generated for ``dict`` items.

    :return: Tuple of ``DltUnnestedTableRow`` records.
    :rtype: tuple[DltUnnestedTableRow, ...]
    """
    rows: list[DltUnnestedTableRow] = []
    for table_name, dlt_cols in EXPECTED_DLT_COLUMNS_DICT_UNNESTED.items():
        dlt_summary = ", ".join(f"`{escape_table_cell(f'{col}: {dtype}')}`" for col, dtype in dlt_cols.items())
        pq_schema = EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED[table_name]
        ice_schema = EXPECTED_ICEBERG_SCHEMAS_DICT_UNNESTED[table_name]
        rows.append(
            DltUnnestedTableRow(
                table_name=table_name,
                role_summary=UNNESTED_TABLE_ROLES.get(table_name, "Generated child table"),
                dlt_columns_summary=dlt_summary,
                parquet_schema_summary=format_arrow_schema_fields(pq_schema),
                iceberg_schema_summary=format_iceberg_schema_fields(ice_schema),
            )
        )
    return tuple(rows)
