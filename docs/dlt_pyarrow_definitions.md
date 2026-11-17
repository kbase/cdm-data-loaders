---
render_macros: true
---

# `dlt` PyArrow Table and Column Definitions

This guide summarises what table metadata, column descriptions, types, nullability, field names, and row values survive when PyArrow schemas pass through `dlt` pipelines. Every table on this page renders from the schema fixtures and expectations in [`tests/dependencies/test_dlt_pyarrow_table_definitions.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/dependencies/test_dlt_pyarrow_table_definitions.py).

- **Characterisation test suite**: [`tests/dependencies/test_dlt_pyarrow_table_definitions.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/dependencies/test_dlt_pyarrow_table_definitions.py)
- **Buffered Arrow page emitter (`DictBuffer`)**: [`src/cdm_data_loaders/utils/buffer.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/buffer.py)
- **Schema registry (`ArrowSchemaRegistry`)**: [`src/cdm_data_loaders/core/registry.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/registry.py)
- **Resource hint registrar (`OutputTableHints`)**: [`src/cdm_data_loaders/core/transforms.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/transforms.py)
- **Polaris Iceberg definition sync CLI**: [`scripts/apply_polaris_schema_definitions.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/apply_polaris_schema_definitions.py)
- **PyIceberg SQLite connection cleanup**: [`src/cdm_data_loaders/utils/iceberg/cleanup.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/iceberg/cleanup.py)
- **Companion nested output and `max_table_nesting` guide**: [`dlt` Nested Output Behaviour](dlt_nested_output_behaviour.md)

---

## Characterisation Matrix

The test suite runs 16 parametrized pipeline executions across three axes:

- **Input mode**:
  - `direct_table`: passes a `pa.Table` straight to `pipeline.run(...)`.
  - `dict_buffer`: yields `DataItemWithMeta` Arrow pages from `DictBuffer` backed by `ArrowSchemaRegistry` (`max_items=1`, exercising per-row emission and `flush()`).
- **Schema shape**:
  - `flat`: 10 scalar columns covering integers, floats, decimals, booleans, dates, UTC timestamps, and strings.
  - `deep_nested`: four-level nested `pa.struct`, `pa.list_`, and `pa.map_` columns, plus `camelCase` and `_leadingCamel` identifier fixtures.
- **Destination format**:
  - `parquet`: `loader_file_format="parquet"` on the `dlt` filesystem destination.
  - `iceberg`: `loader_file_format="parquet", table_format="iceberg"` via PyIceberg.

Both `direct_table` and `dict_buffer` produce identical schemas, metadata, and row values across all output layers.

---

## Behaviour Summary by Output Layer

| Definition Aspect | `dlt` Schema (`pipeline.default_schema`) | Parquet (`loader_file_format="parquet"`) | Iceberg (`table_format="iceberg"`) |
| :--- | :--- | :--- | :--- |
| **Table metadata** (`pa.Schema.metadata`) | Dropped (`description=None`, no `x-iceberg-table-properties`). | Kept (`comment`, `description`, `doc`, `title`). | Dropped from `table.properties`, `table.scan().to_arrow().schema.metadata`, and rewritten `data/*.parquet` files. |
| **Root field metadata** (`pa.Field.metadata`) | Dropped (`col_def["description"]` is `None`). | Kept for all keys (`comment`, `description`, `doc`, `xsd_type`). | Copies `metadata["doc"]` into `NestedField.doc`; ignores `comment`, `description`, and custom keys. |
| **Nested field metadata** | Kept in `x-nested-type="arrow-ipc:..."` (`deserialize_type`). | Kept at every `struct`, `list` element, and `map` key or value depth. | Copies `metadata["doc"]` onto nested `StructType` fields; drops metadata on `ListType` elements and `MapType` keys and values. |
| **Scalar types and nullability** | Maps `int16`/`int32`/`int64` to `bigint` (with `precision` 16/32) and `float32`/`float64` to `double`; omits default `precision=6` on `timestamp("us", tz="UTC")`; keeps `nullable`. | Keeps exact Arrow types (`int16`, `float32`, `decimal128`, `date32`, `timestamp`) and `nullable`. | Widens `pa.int16()` to `IntegerType()` (`pa.int32()` on Arrow scan); keeps all other tested scalar types and `required = not nullable`. |
| **Nested types** (`struct`, `list`, `map`) | Stored as `data_type="json"` with an Arrow IPC type hint in `x-nested-type`. | Native Parquet nested types; rewrites `pa.list_` child field names to `"element"`. | Native Iceberg `StructType`, `ListType`, and `MapType`; Arrow scans emit `"element"` for list children. |
| **Identifier case** | Normalizes root columns to `snake_case`; keeps nested field names unchanged. | Normalizes root columns to `snake_case`; keeps nested `struct` field names unchanged. | Normalizes root columns to `snake_case`; keeps nested `StructType` field names unchanged. |
| **Row values** | Tracked via load package state. | Roundtrips all scalar, null, list, struct, and map entries without loss. | Roundtrips all scalar, null, list, struct, and map entries without loss; tags `data/*.parquet` fields with `PARQUET:field_id`. |

---

## Flat Scalar Columns

`FLAT_ARROW_SCHEMA` attaches table-level metadata (`comment`, `description`, `doc`, `title`) and non-empty field metadata to 10 scalar columns:

{{ render_reference_table("dlt_pyarrow_flat_columns_table") }}

Key takeaways for flat schemas:

- **Iceberg reads `metadata["doc"]` alone**: columns carrying `comment`, `description`, or `xsd_type` without a `"doc"` key (`count_32`, `ratio_32`, `is_active`, `updated_at`) receive `NestedField.doc=None` in Iceberg.
- **16-bit integer promotion in Iceberg**: Iceberg has no 16-bit integer primitive, so `small_code` (`pa.int16()`) becomes `IntegerType()` in Iceberg and reads back as `pa.int32()` from `table.scan().to_arrow()`.
- **Parquet preserves everything**: `pq.read_schema(...)` retains `pa.Schema.metadata`, custom `pa.Field.metadata` keys such as `xsd_type`, and narrow widths such as `pa.int16()` and `pa.float32()`.

---

## Deep Nested Schemas (`struct`, `list`, and `map`)

### Root `dlt` Column Representation

In `pipeline.default_schema`, `dlt` marks nested PyArrow columns as `data_type="json"` and serializes the nested `pa.DataType` into `x-nested-type`:

{{ render_reference_table("dlt_pyarrow_nested_dlt_columns_table") }}

Calling `dlt.common.libs.pyarrow.deserialize_type` on `col_def["x-nested-type"]` reconstructs the exact input `pa.DataType`, including custom list element names (`synonym_item`, `feature_item`), nullability, and inner `pa.Field.metadata`.

### Nested Field Paths Across Parquet and Iceberg

`DEEP_NESTED_ARROW_SCHEMA` places non-empty `pa.Field.metadata` on every node across four nesting levels (`struct` → `struct` → `struct` → `list`, `list` → `struct` → `list` → `struct` → `map`, and `map` → `struct` → `list` → `list`):

{{ render_reference_table("dlt_pyarrow_nested_paths_table") }}

Key takeaways for nested schemas:

- **Parquet list element renaming**: `dlt` writes Parquet with `use_compliant_nested_type=True`, renaming `pa.list_` child fields (`synonym_item`, `feature_item`, `evidence_item`, `group_item`, `tag_item`) to `"element"` while keeping their nullability and `pa.Field.metadata`.
- **Parquet `MapType` wrapper struct name**: PyArrow's C++ Parquet reader names `MapType`'s internal entry struct after the parent column instead of `"entries"`, so `Schema.equals(..., check_metadata=True)` returns `False` on schemas containing `pa.map_` even when `key_field`, `item_field`, and all metadata match.
- **Iceberg `ListType` and `MapType` slot limits**: PyIceberg `ListType` and `MapType` carry field IDs and nullability flags (`element_required`, `value_required`) without `doc` attributes on element, key, or value slots. Struct fields nested *inside* list elements or map values (`features.element.kind`, `annotations.value.note`) are `NestedField` instances and keep their `metadata["doc"]` strings.

---

## Identifier Normalization (`snake_case`)

`CAMEL_FLAT_ARROW_SCHEMA` and `CAMEL_NESTED_ARROW_SCHEMA` test mixed-case and underscore-prefixed identifiers (`entryId`, `_rawCode`, `outerBlock`, `camelChild`, `deepList`, `listElem`, `_innerCode`):

{{ render_reference_table("dlt_pyarrow_identifier_normalization_table") }}

`dlt`'s default `snake_case` naming convention renames root columns (`entryId` → `entry_id`, `_rawCode` → `_raw_code`, `outerBlock` → `outer_block`) across `pipeline.default_schema`, Parquet files, and Iceberg tables. Field names inside nested `pa.struct` columns (`camelChild`, `deepList`, `_innerCode`) bypass `dlt` column normalization and keep their original casing in Parquet and Iceberg.

---

## Implications for Pipeline and Catalog Authors

- **Populate `metadata={"doc": ...}` for Iceberg field descriptions**: converters and `OUTPUT_SCHEMAS` registries (see [Table and Column Transformations](table_transforms.md)) should write field descriptions under the `"doc"` metadata key so PyIceberg populates `NestedField.doc` during table creation.
- **Register table hints and Iceberg partitions via `OutputTableHints`**: `dlt` drops `pa.Schema.metadata` before creating Iceberg tables, and PyIceberg rewrites `data/*.parquet` without schema-level metadata. Pipeline settings subclasses can return `OutputTableHints` from `get_output_table_hints()` ([`src/cdm_data_loaders/core/transforms.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/transforms.py)) to register table descriptions (`x-iceberg-table-properties={"comment": ...}`), top-level column descriptions, and `x-iceberg-partition` specs on `DltResource` variants at ingest time.
- **Sync table comments and field definitions to Polaris Iceberg tables**: for existing tables in a Polaris REST catalog, run [`scripts/apply_polaris_schema_definitions.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/apply_polaris_schema_definitions.py) (`uv run python -m scripts.apply_polaris_schema_definitions --catalog polaris --table <ns.table> --schema-source <module:ATTR_or_jsonschema>`) to apply table `comment` properties and root or nested `StructType` `NestedField.doc` strings (`doc`, `comment`, and `description` metadata keys), normalizing root PyArrow column names with `dlt`'s `snake_case` convention while matching nested `StructType` field names as written.
- **Expect `pa.int32()` when scanning `pa.int16()` columns from Iceberg**: downstream Arrow consumers reading Iceberg tables via `table.scan().to_arrow()` receive `pa.int32()` for columns ingested as `pa.int16()`.

```python
import pyarrow as pa

from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.utils.buffer import DictBuffer

ASSAY_SCHEMA = pa.schema(
    [
        pa.field("entry_id", pa.int64(), nullable=False, metadata={"doc": "Primary assay key"}),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field("taxonId", pa.int32(), nullable=False, metadata={"doc": "NCBI taxonomy ID"}),
                ]
            ),
            nullable=False,
            metadata={"doc": "Source organism record"},
        ),
    ],
    metadata={"doc": "Assay records (kept in Parquet; not forwarded to Iceberg table.properties)"},
)

buffer = DictBuffer(
    max_items=1000,
    schema_map=ArrowSchemaRegistry(registry={"assay_record": ASSAY_SCHEMA}),
)
```

For pipeline execution lifecycle, `_load_info` and `_rejected` system tables, and CTS exit-code rules, see the [Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md).

---

## Running the Characterisation Suite

Execute the characterisation tests with `uv run pytest`:

```sh
uv run pytest tests/dependencies/test_dlt_pyarrow_table_definitions.py
```
