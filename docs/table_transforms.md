# Record Filtering, Transformation, and Multi-Table Splitting

All six file-format ingestion pipelines support pluggable record-level filtering, single-table row transformation, multi-table splitting, and `pyarrow.Schema` overrides through a shared `--transforms-module` (`CDL_TRANSFORMS_MODULE`) setting:

| Pipeline | CLI Entry Point | Default Buffer Mode | Source of Input `table_name` |
| --- | --- | --- | --- |
| [extract_pipeline.py](../src/cdm_data_loaders/pipelines/jsonlines/extract_pipeline.py) | `jsonlines_extract` | `DictBuffer` (`ArrowDictBuffer` if schema map set) | `--table-name` (`settings.table_name`) |
| [extract_jsonschema_validate_pipeline.py](../src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | `jsonlines_jsonschema` | `ArrowDictBuffer` | Keys of `ENTITY_SCHEMAS` |
| [extract_pydantic_validate_pipeline.py](../src/cdm_data_loaders/pipelines/jsonlines/extract_pydantic_validate_pipeline.py) | `jsonlines_pydantic` | `DictBuffer` (`ArrowDictBuffer` if schema map set) | Keys of `ENTITY_MODELS` |
| [xsv/pipeline.py](../src/cdm_data_loaders/pipelines/xsv/pipeline.py) | `xsv` | `DictBuffer` (`ArrowDictBuffer` if schema map set) | `--table-name` (defaults to `"data"`) |
| [xmltodict/pipeline.py](../src/cdm_data_loaders/pipelines/xmltodict/pipeline.py) | `xmltodict` | `DictBuffer` (`ArrowDictBuffer` if schema map set) | `--table-name` (`settings.table_name`) |
| [xmltopyarrow/pipeline.py](../src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py) | `xmltopyarrow` | `ArrowDictBuffer` | `--table-name` or `--preset` |

---

## Processing Lifecycle

Transforms run **after** a record is parsed and validated against its input schema/model, and **before** rows are buffered into pages for `dlt`:

1. **Parse & Validate**:
   - Malformed lines or records that fail input validation (`JSON Schema`, `Pydantic`, or `qsv`) are routed directly to `_rejected` (with `table_name` set to the input table name) without calling the transform function.
2. **Transform & Route (`transform_fn`)**:
   - Each valid record (`dict[str, Any]`) is passed to the `TableTransform.transform_fn` registered for the input `table_name`.
   - `transform_fn` returns a mapping of `{destination_table_name: [row_dict, ...]}`.
   - Returning `{}` (or empty row lists) drops the record.
3. **Coerce & Buffer (`DictBuffer` / `ArrowDictBuffer`)**:
   - If a PyArrow schema map is active (either derived from the pipeline's input schema or supplied via `TableTransform.table_to_schema_map`), each emitted row is coerced against its destination table's `pyarrow.Schema` and accumulated in an `ArrowDictBuffer`.
   - Otherwise, emitted rows are accumulated in a `DictBuffer`.

---

## Defining a Transforms Module

Create an importable Python module exporting `TABLE_TRANSFORMS: dict[str, TableTransform]` (from `cdm_data_loaders.core.transforms`).

```python
from collections.abc import Mapping, Sequence
from typing import Any
import pyarrow as pa

from cdm_data_loaders.core.transforms import TableTransform

# Signature of transform_fn:
# (record: dict[str, Any]) -> Mapping[str, Sequence[dict[str, Any]]]
```

### `TableTransform` Fields

- **`transform_fn`**: `Callable[[dict[str, Any]], Mapping[str, Sequence[dict[str, Any]]]] | None = None`
  - Receives a single parsed/validated record `dict[str, Any]`.
  - Returns a dictionary mapping one or more destination table names to sequences of row dictionaries:
    - `{}` (or `{"table": []}`) -> **Filter out (drop)** the record.
    - `{"table": [new_row]}` -> **Transform** a row 1-to-1 in place.
    - `{"table": [row_1, row_2]}` -> **Unnest / fan out** 1-to-N rows into the same table.
    - `{"parent_table": [parent_row], "child_table": [child_1, child_2]}` -> **Split** a record across multiple destination tables.
  - When `transform_fn` is `None`, records are routed unchanged to `{table_name: [record]}`.
- **`table_to_schema_map`**: `Mapping[str, pa.Schema] | None = None`
  - Optional mapping of `{destination_table_name: pyarrow.Schema}`.
  - When provided:
    - In `jsonlines_jsonschema` and `xmltopyarrow`, it overrides the default single-table `pyarrow.Schema` derived from the JSON Schema or XSD so that altered columns or split destination tables have explicit Arrow schemas.
    - In `jsonlines_extract`, `jsonlines_pydantic`, `xsv`, and `xmltodict`, it upgrades buffering from `DictBuffer` to `ArrowDictBuffer`, coercing rows and yielding `pyarrow.Table` pages to `dlt`.

### Specifying the Module on the CLI or via Environment Variable

Pass `--transforms-module` (or set `CDL_TRANSFORMS_MODULE`):

- Default attribute (`TABLE_TRANSFORMS`):
  ```sh
  --transforms-module my_package.transforms
  ```
- Custom attribute name (`module:ATTR` syntax):
  ```sh
  --transforms-module my_package.transforms:CUSTOM_TRANSFORMS
  ```

---

## Example 1: Filtering Out Data

### Use Case: Dropping Empty NCBI Annotation Reports (`total_count == 0`)

NCBI Datasets API annotation report JSONL files contain summary lines where no annotations were found:

```json
{"reports": [{"annotation": {"gene_id": 1001, "name": "dnaA", "symbol": "dnaA"}}], "total_count": 1}
{"reports": [], "total_count": 0}
```

Both lines are valid against `AnnotationReportResponse.schema.json`, so `jsonlines_jsonschema` accepts both during validation. To drop records with `total_count == 0` instead of writing empty rows to the `annotation_report` table:

```python
# my_package/ncbi_transforms.py
import json
from importlib.resources import files
from typing import Any

from cdm_data_loaders.core.transforms import TableTransform

ANNOTATION_REPORT_SCHEMA = json.loads(
    files("cdm_data_loaders.parsers.ncbi.datasets_api")
    .joinpath("AnnotationReportResponse.schema.json")
    .read_text(encoding="utf-8")
)

# ENTITY_SCHEMAS and TABLE_TRANSFORMS can live in the same module or separate modules.
ENTITY_SCHEMAS: dict[str, dict[str, Any]] = {
    "annotation_report": ANNOTATION_REPORT_SCHEMA,
}


def filter_nonzero_annotation_reports(
    record: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Drop annotation report responses with total_count == 0."""
    if record.get("total_count", 0) == 0:
        return {}
    return {"annotation_report": [record]}


TABLE_TRANSFORMS: dict[str, TableTransform] = {
    "annotation_report": TableTransform(
        transform_fn=filter_nonzero_annotation_reports,
    ),
}
```

 Because `filter_nonzero_annotation_reports` does not change the structure of kept `annotation_report` rows, `table_to_schema_map` can be omitted — `jsonlines_jsonschema` automatically uses the PyArrow schema converted from `ANNOTATION_REPORT_SCHEMA`.

#### CLI Invocation

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_jsonschema \
  --schema-files-module my_package.ncbi_transforms \
  --transforms-module my_package.ncbi_transforms \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name ncbi_annotations \
  --use-destination local_fs \
  --use-output-dir-for-pipeline-metadata true \
  --loader-file-format parquet
```

---

## Example 2: Data Transformation Within a Single Table Row

### 2a. Transforming Fields in a Single Table (`xsv` or `jsonlines_extract`)

Suppose an XSV or JSONL pipeline ingests genome quality rows into table `genome_metrics` and you want to:
1. Filter out low-completeness rows (`completeness < 50.0`),
2. Normalize the `accession` prefix, and
3. Compute a derived `quality_score = completeness - (5.0 * contamination)` field within the row.

```python
# my_package/quality_transforms.py
from typing import Any

from cdm_data_loaders.core.transforms import TableTransform


def transform_genome_metrics(
    record: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Filter low-completeness rows and add a derived quality_score column."""
    completeness = float(record["completeness"])
    contamination = float(record["contamination"])
    if completeness < 50.0:
        return {}

    transformed = {
        **record,
        "accession": record["accession"].strip().upper(),
        "quality_score": round(completeness - (5.0 * contamination), 2),
    }
    return {"genome_metrics": [transformed]}


TABLE_TRANSFORMS: dict[str, TableTransform] = {
    "genome_metrics": TableTransform(
        transform_fn=transform_genome_metrics,
    ),
}
```

#### CLI Invocation (`xsv`)

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run xsv \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name checkm2 \
  --table-name genome_metrics \
  --schema-path ./schemas/genome_metrics.schema.json \
  --transforms-module my_package.quality_transforms \
  --use-destination local_fs \
  --loader-file-format parquet
```

### 2b. Single-Table Row Transformation with a Custom PyArrow Schema (`jsonlines_jsonschema` / `xmltopyarrow`)

When a single-table transformation in `jsonlines_jsonschema` or `xmltopyarrow` adds, removes, or retypes columns relative to the input schema, supply `table_to_schema_map` so `ArrowDictBuffer` serializes the transformed structure:

```python
# my_package/widget_transforms.py
from typing import Any
import pyarrow as pa

from cdm_data_loaders.core.transforms import TableTransform

WIDGET_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("widget_id", pa.string()),
        pa.field("count", pa.int64()),
        pa.field("is_bulk", pa.bool_()),
    ]
)


def enrich_widget(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Project fields and add a boolean is_bulk flag."""
    count = int(record["count"])
    return {
        "widget": [
            {
                "widget_id": record["widget_id"],
                "count": count,
                "is_bulk": count >= 100,
            }
        ]
    }


TABLE_TRANSFORMS: dict[str, TableTransform] = {
    "widget": TableTransform(
        transform_fn=enrich_widget,
        table_to_schema_map={"widget": WIDGET_OUTPUT_SCHEMA},
    ),
}
```

---

## Example 3: Splitting a Single Record Across Multiple Tables

A single `transform_fn` can decompose one input record into rows for multiple destination tables in one pass.

### 3a. Multi-Table Split with `DictBuffer` (`jsonlines_extract`, `jsonlines_pydantic`, `xsv`, `xmltodict`)

Suppose each input record in `assembly_report` contains assembly-level metadata plus a nested `contigs` list:

```json
{
  "assembly_id": "GCF_000005845.2",
  "organism_name": "Escherichia coli str. K-12 substr. MG1655",
  "total_count": 2,
  "contigs": [
    {"contig_id": "NC_000913.3", "length": 4641652, "gc_percent": 50.8},
    {"contig_id": "NZ_CP009273.1", "length": 92721, "gc_percent": 48.2}
  ]
}
```

To split each input record into an `assemblies` parent table and an `assembly_contigs` child table linked by `assembly_id`:

```python
# my_package/assembly_split_transforms.py
from typing import Any

from cdm_data_loaders.core.transforms import TableTransform


def split_assembly_and_contigs(
    record: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Filter empty reports and split assembly metadata from per-contig rows."""
    if record.get("total_count", 0) == 0:
        return {}

    assembly_id = record["assembly_id"]
    assembly_row = {
        "assembly_id": assembly_id,
        "organism_name": record.get("organism_name"),
        "contig_count": len(record.get("contigs", [])),
    }
    contig_rows = [
        {
            "assembly_id": assembly_id,
            "contig_id": contig["contig_id"],
            "length": contig["length"],
            "gc_percent": contig.get("gc_percent"),
        }
        for contig in record.get("contigs", [])
    ]

    return {
        "assemblies": [assembly_row],
        "assembly_contigs": contig_rows,
    }


TABLE_TRANSFORMS: dict[str, TableTransform] = {
    # Keyed by the input table_name ("assembly_report")
    "assembly_report": TableTransform(
        transform_fn=split_assembly_and_contigs,
    ),
}
```

Notice how:
- The key in `TABLE_TRANSFORMS` (`"assembly_report"`) matches the pipeline's input `table_name` (`--table-name assembly_report` or `ENTITY_MODELS["assembly_report"]`).
- Any malformed or invalid input records are still written to `_rejected` with `table_name = "assembly_report"`.
- Valid records are split and written to the `assemblies` and `assembly_contigs` destination tables.
- If a record has `contigs: []`, `"assembly_contigs": []` is automatically ignored so only `"assemblies"` receives a row.

#### CLI Invocation (`jsonlines_extract` or `xmltodict`)

```sh
# With jsonlines_extract:
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_extract \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name assemblies_ds \
  --table-name assembly_report \
  --transforms-module my_package.assembly_split_transforms \
  --use-destination local_fs \
  --loader-file-format parquet

# With xmltodict:
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run xmltodict \
  --input-dir ./input_xml \
  --output-dir ./output \
  --dataset-name assemblies_ds \
  --table-name assembly_report \
  --record-tag Assembly \
  --transforms-module my_package.assembly_split_transforms \
  --use-destination local_fs \
  --loader-file-format parquet
```

### 3b. Multi-Table Split with PyArrow Schemas (`jsonlines_jsonschema`, `xmltopyarrow`, or Any Pipeline Using `ArrowDictBuffer`)

When splitting records in an Arrow-backed pipeline (`jsonlines_jsonschema` or `xmltopyarrow`) — or when you want explicit Arrow schemas for any other pipeline — provide a `pyarrow.Schema` for every output table in `table_to_schema_map`:

```python
# my_package/assembly_arrow_transforms.py
from typing import Any
import pyarrow as pa

from cdm_data_loaders.core.transforms import TableTransform
from my_package.assembly_split_transforms import split_assembly_and_contigs

ASSEMBLIES_SCHEMA = pa.schema(
    [
        pa.field("assembly_id", pa.string()),
        pa.field("organism_name", pa.string()),
        pa.field("contig_count", pa.int64()),
    ]
)

ASSEMBLY_CONTIGS_SCHEMA = pa.schema(
    [
        pa.field("assembly_id", pa.string()),
        pa.field("contig_id", pa.string()),
        pa.field("length", pa.int64()),
        pa.field("gc_percent", pa.float64()),
    ]
)

TABLE_TRANSFORMS: dict[str, TableTransform] = {
    "assembly_report": TableTransform(
        transform_fn=split_assembly_and_contigs,
        table_to_schema_map={
            "assemblies": ASSEMBLIES_SCHEMA,
            "assembly_contigs": ASSEMBLY_CONTIGS_SCHEMA,
        },
    ),
}
```

When `table_to_schema_map` is provided:
- Each row returned by `split_assembly_and_contigs` is coerced to its target table's `pyarrow.Schema` via `coerce_record_for_pyarrow`.
- `ArrowDictBuffer` accumulates rows per output table (`assemblies` and `assembly_contigs`) and flushes `pyarrow.Table` pages independently when a table reaches `--buffer-size`.
- Returning an output table name that is not present in `table_to_schema_map` raises a `ValueError` immediately.

---

## Programmatic Usage in Python

You can also pass `transforms_module` directly when constructing any pipeline settings class in Python:

```python
from pathlib import Path
from cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline import (
    run_jsonlines_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlJsonschemaIngestSettings

settings = JsonlJsonschemaIngestSettings(
    input_dir=Path("./input"),
    output_dir=Path("./output"),
    dataset_name="ncbi_annotations",
    schema_files_module="my_package.ncbi_transforms",
    transforms_module="my_package.ncbi_transforms",
    use_destination="local_fs",
    use_output_dir_for_pipeline_metadata=True,
    loader_file_format="parquet",
)

load_info = run_jsonlines_pipeline(settings)
```

