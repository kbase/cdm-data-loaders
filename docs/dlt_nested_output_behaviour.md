---
render_macros: true
---

# `dlt` Nested Output Behaviour Across Formats and Placements

This guide summarises how `dlt` normalizes and writes nested records across Python `dict` items and `pyarrow.Table` pages, five `max_table_nesting` placements, and three `filesystem` destination formats (`parquet`, `iceberg`, and `jsonl`). Every reference table on this page renders from the schemas and expectations in [`tests/dependencies/test_dlt_nested_output_behaviour.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/dependencies/test_dlt_nested_output_behaviour.py).

- **Characterisation test suite**: [`tests/dependencies/test_dlt_nested_output_behaviour.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/dependencies/test_dlt_nested_output_behaviour.py)
- **JSONL resource and transformer setup**: [`src/cdm_data_loaders/pipelines/jsonlines/base.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/base.py)
- **Destination-level `max_table_nesting` example**: [`src/cdm_data_loaders/pipelines/all_the_bacteria.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/all_the_bacteria.py)
- **Shared pipeline runner (`destination_kwargs`)**: [`src/cdm_data_loaders/pipelines/core.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/core.py)
- **Companion PyArrow schema and metadata guide**: [`dlt` PyArrow Table and Column Definitions](dlt_pyarrow_definitions.md)

---

## Characterisation Matrix

The test suite runs 30 parametrized pipeline executions across three axes, routing every input through a `@dlt.resource` into a downstream `@dlt.transformer` via `dlt.mark.with_table_name(page, "widget")`:

- **Input item kind**:
  - `dict`: plain Python `dict` rows containing a nested `dict` with a `list` field (`address`), a scalar `list` (`tags`), a `list` of `dict` records (`children`, including `[]`), and a `list` of `(str, float)` 2-tuples (`metrics`).
  - `pyarrow`: `pa.Table.from_pylist(ROWS, schema=ARROW_SCHEMA)` typing those four columns as `pa.struct`, `pa.list_(pa.string())`, `pa.list_(pa.struct(...))`, and `pa.map_(pa.string(), pa.float64())`.
- **`max_table_nesting` placement**:
  - `resource_and_transformer`, `transformer_only`, `destination`, `resource_only`, and `unset`.
- **Filesystem destination format**:
  - `parquet` (`loader_file_format="parquet"`), `iceberg` (`loader_file_format="parquet", table_format="iceberg"`), and `jsonl` (`loader_file_format="jsonl"`).

---

## Behaviour Summary by Input Mode

| Aspect | `dict` (Unnested: `unset` or `resource_only`) | `dict` (Effective `max_table_nesting=0`) | `pyarrow.Table` (Any Placement) |
| :--- | :--- | :--- | :--- |
| **Output table count** | `6` tables (`widget` + `5` child tables). | `1` table (`widget`). | `1` table (`widget`). |
| **`dlt` complex column types** | Flattened `address__*` scalars on root; child tables for lists. | `data_type="json"` on `address`, `tags`, `children`, `metrics`. | `data_type="json"` with `x-nested-type="arrow-ipc:..."`. |
| **Parquet representation** | Separate `.parquet` files per root and child table; scalar types. | Single `.parquet` file; complex columns stored as `pa.string()` JSON text. | Single `.parquet` file; native `pa.struct`, `pa.list_`, and `pa.map_`. |
| **Iceberg representation** | `6` Iceberg tables; scalar `LongType()` and `StringType()` fields. | `1` Iceberg table; complex columns stored as `StringType()` JSON text. | `1` Iceberg table; native `StructType`, `ListType`, and `MapType`. |
| **JSONL representation** | Separate `.jsonl` files per root and child table. | Single `.jsonl` file; complex columns written as native JSON objects and arrays. | Single `.jsonl` file via `ArrowToJsonlWriter`; native JSON objects and arrays. |
| **`_dlt_*` metadata columns** | Adds `_dlt_load_id` and `_dlt_id` on root; `_dlt_parent_id`, `_dlt_list_idx`, and `_dlt_id` on children. | Adds non-null `_dlt_load_id` and `_dlt_id`. | Omitted (`_dlt_load_id` and `_dlt_id` never added). |

---

## `max_table_nesting` Placement and Transformer Propagation

{{ render_reference_table("dlt_nested_placement_matrix_table") }}

> [!WARNING]
> Setting `max_table_nesting=0` on `@dlt.resource` without setting it on a downstream `@dlt.transformer` (`resource_only`) does **not** prevent table splitting. `dlt` evaluates table nesting hints on the resource that emits the final table items (the transformer) or on the destination. Under `resource_only`, Python `dict` records split into all six unnested tables, matching `unset`.

Key placement rules:

- **Transformer-emitted `dict` pages**: set `max_table_nesting=0` on `@dlt.transformer` (`transformer_only` or `resource_and_transformer`), as done in [`build_entity_resource`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/base.py), or pass `destination_kwargs={"max_table_nesting": 0}` to `run_pipeline`, as done in [`run_atb_pipeline`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/all_the_bacteria.py).
- **`pyarrow.Table` pages**: `dlt` bypasses its relational normalizer for Arrow tables. Every placement produces one `widget` table with native nested types and no `_dlt_*` columns.

---

## Single-Table Mode: `dict` (`max_table_nesting=0`) vs `pyarrow.Table`

When `dict` items run with an effective `max_table_nesting=0` (`resource_and_transformer`, `transformer_only`, or `destination`), both `dict` and `pyarrow.Table` inputs emit one `widget` table, yet their column types and serialized values diverge in Parquet and Iceberg:

{{ render_reference_table("dlt_nested_single_table_columns_table") }}

Key differences in single-table mode:

- **JSON text vs native nested columns in Parquet and Iceberg**: for `dict` items with `max_table_nesting=0`, `dlt` serializes every `dict`, `list`, and tuple column to a compact JSON `str` (`pa.string()` in Parquet, `StringType()` in Iceberg). Downstream SQL or Spark queries must parse JSON text instead of reading native struct, list, or map fields.
- **No double-encoding in JSONL**: for `loader_file_format="jsonl"`, `dlt` writes `data_type="json"` columns as native JSON objects and arrays (`{"street": ...}`, `["a", "b", "c"]`), not escaped JSON strings.
- **`ArrowToJsonlWriter` path for `pyarrow.Table` + JSONL**: `dlt` streams `pyarrow.Table` pages through `ArrowToJsonlWriter`, serializing `pa.map_(pa.string(), pa.float64())` values as `[key, value]` pair arrays (`[["score", 98.5]]`) and omitting `_dlt_*` metadata columns.
- **`_dlt_*` system columns**: `dict` normalization injects non-null `_dlt_load_id` and `_dlt_id` columns (`nullable=False` in Parquet, `required=True` in Iceberg). `pyarrow.Table` ingestion keeps the exact root column set from `ARROW_SCHEMA`.

---

## Unnested Relational Mode (`dict` Without Effective `max_table_nesting=0`)

When `dict` items run under `unset` or `resource_only`, `dlt`'s relational normalizer flattens nested `dict` scalars into the root table and splits every `list` into a child table across Parquet, Iceberg, and JSONL:

{{ render_reference_table("dlt_nested_unnested_tables_table") }}

Key unnesting behaviours verified by the suite:

- **Nested `dict` flattening vs `list` splitting**: scalar keys inside `address` become `address__city` and `address__zip` on `widget`, while `address.street` splits into `widget__address__street` linked by `_dlt_parent_id == widget._dlt_id` and ordered by `0`-based `_dlt_list_idx`.
- **Empty lists emit zero child rows**: row 2 (`id=2`) has `children: []`, so `widget__children` contains two rows from `id=1` and zero rows linked to `id=2`'s `_dlt_id`.
- **2-tuple lists (`metrics`) create a two-level child hierarchy and coerce mixed scalars to `text`**: Python tuples `[("score", 98.5)]` normalize as nested lists. `widget__metrics` holds parent link keys without a value column, and `widget__metrics__list` stores `"score"` (`_dlt_list_idx=0`) and `"98.5"` (`_dlt_list_idx=1`) coerced to `str` (`text`).

---

## Implications for Pipeline Authors

- **Prefer `pyarrow.Table` emission (`use_pyarrow=True` or `DictBuffer`) for structured Parquet and Iceberg tables**: Arrow pages preserve `pa.struct`, `pa.list_`, and `pa.map_` in Parquet and Iceberg without JSON stringification or `_dlt_*` columns. See [JSONLines Ingestion Pipelines](jsonlines_ingest.md) and [`dlt` PyArrow Table and Column Definitions](dlt_pyarrow_definitions.md).
- **Set `max_table_nesting=0` on `@dlt.transformer` or `destination_kwargs` when yielding `dict` records without child tables**: setting `max_table_nesting=0` on `@dlt.resource` alone leaves transformer outputs unnested.
- **Avoid raw Python tuple lists in unnested `dict` pipelines**: mixed `(key, value)` tuples split into parent-link intermediate tables and coerce numeric values to `text`. Emit `list[dict[str, Any]]` for `dict` pipelines or `pa.map_` inside a `pyarrow.Table`.

```python
import dlt
import pyarrow as pa


@dlt.resource(name="raw_batches")
def raw_batches() -> list[dict[str, object]]:
    return [{"id": 1, "tags": ["a", "b"], "metrics": [("score", 98.5)]}]


# Dict single-table mode: set max_table_nesting=0 on @dlt.transformer (not @dlt.resource alone)
@dlt.transformer(data_from=raw_batches, max_table_nesting=0)
def dict_widgets(page: list[dict[str, object]]):
    yield dlt.mark.with_table_name(page, "widget")


# Native nested Parquet/Iceberg mode: emit a typed pyarrow.Table
WIDGET_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("metrics", pa.map_(pa.string(), pa.float64())),
    ]
)


@dlt.transformer(data_from=raw_batches)
def arrow_widgets(page: list[dict[str, object]]):
    yield dlt.mark.with_table_name(pa.Table.from_pylist(page, schema=WIDGET_SCHEMA), "widget")
```

For pipeline execution lifecycle, `_load_info` and `_rejected` system tables, and CTS exit-code rules, see the [Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md).

---

## Running the Characterisation Suite

Execute the characterisation tests with `uv run pytest`:

```sh
uv run pytest tests/dependencies/test_dlt_nested_output_behaviour.py
```
