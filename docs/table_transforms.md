# Record Filtering, Transformation, and Multi-Table Splitting

All six file-format ingestion pipelines support pluggable file pre-processing, record-level filtering, single-table row transformation, multi-table splitting, and `pyarrow.Schema` overrides through `--preprocessors-module` (`CDL_PREPROCESSORS_MODULE`), `--transforms-module` (`CDL_TRANSFORMS_MODULE`), and `--output-schemas-module` (`CDL_OUTPUT_SCHEMAS_MODULE`):

| Pipeline                                                                                                                                                                           | CLI Entry Point        | Default Output Format                            | Source of Input `table_name`                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| [extract_pipeline.py](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_pipeline.py)                                         | `jsonlines_extract`    | `list[dict]` (`pyarrow.Table` if schema map set) | `--table-name` (`settings.table_name`)                                                          |
| [extract_jsonschema_validate_pipeline.py](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | `jsonlines_jsonschema` | `pyarrow.Table`                                  | Keys of `ENTITY_SCHEMAS` (or preset entrypoint `ncbi_datasets`)                                 |
| [extract_pydantic_validate_pipeline.py](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_pydantic_validate_pipeline.py)     | `jsonlines_pydantic`   | `pyarrow.Table`                                  | Keys of `ENTITY_MODELS`                                                                         |
| [xsv/pipeline.py](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xsv/pipeline.py)                                                           | `xsv`                  | `pyarrow.Table`                                  | `--table-name` (`settings.table_name`)                                                          |
| [xmltodict/pipeline.py](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltodict/pipeline.py)                                               | `xmltodict`            | `list[dict]` (`pyarrow.Table` if schema map set) | `--table-name` (`settings.table_name`)                                                          |
| [xmltopyarrow/pipeline.py](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                         | `xmltopyarrow`         | `pyarrow.Table`                                  | `--table-name` (or preset entrypoints `pubmed`, `uniprot`, `uniref50`, `uniref90`, `uniref100`) |

---

## Processing Lifecycle

File pre-processors run **before** a file is opened and parsed. Record transforms run **after** a record is parsed and validated against its input schema/model, and **before** rows are buffered into pages for `dlt`:

0. **Pre-process File (`FilePreprocessFn`)**:
   - Each `FileItemDict` yielded by `dlt.sources.filesystem` passes through any `FilePreprocessFn` functions registered in `FILE_PREPROCESSORS`.
   - Returning `None` skips the file without error.
   - Raising `FilePreprocessError` routes a diagnostic row to `_rejected` (with `source_file` and `error_detail`), logs an error, skips the file, and records the failure in `report.txt` (`load_errors`).
1. **Parse and Validate**:
   - Malformed lines or records that fail input validation (`JSON Schema`, `Pydantic`, or `qsv`) are routed directly to `_rejected` (with `table_name` set to the input table name) without calling the transform function.
2. **Transform and Route (`RecordTransformFn`)**:
   - Each valid record (`dict[str, Any]`) is passed to the `RecordTransformFn` registered in `TABLE_TRANSFORMS` for the input `table_name`.
   - The transform function returns a mapping of `{destination_table_name: [row_dict, ...]}`.
   - Returning `{}` (or empty row lists) drops the record.
3. **Coerce and Buffer (`DictBuffer`)**:
   - All pipelines buffer rows using `DictBuffer`.
   - When `loader_file_format` is `parquet` or `iceberg` (`settings.use_pyarrow` is `True`), `_rejected` uses `REJECTED_PYARROW_SCHEMA`, and any destination table with an entry in `settings.schema_map` (either derived from the pipeline's input schema or supplied via `OUTPUT_SCHEMAS`) has its rows coerced against that `pyarrow.Schema` and emitted as a `pyarrow.Table` page.
   - When `loader_file_format` is `jsonl` (`settings.use_pyarrow` is `False`), `settings.schema_map` is `None` (no PyArrow schemas are generated from input schemas) and all tables (including `_rejected`) are emitted as `list[dict]` pages.

---

## Defining Transforms and Output Schemas Modules

Create an importable Python module exporting `TABLE_TRANSFORMS: dict[str, RecordTransformFn]` and/or `OUTPUT_SCHEMAS: dict[str, pa.Schema]`.

```python
from collections.abc import Mapping, Sequence
from typing import Any
import pyarrow as pa

from cdm_data_loaders.core.registry import RecordTransformFn

# Signature of RecordTransformFn:
# (record: dict[str, Any]) -> Mapping[str, Sequence[dict[str, Any]]]
```

### Registry Exports

- **`FILE_PREPROCESSORS`** (`--preprocessors-module` / `CDL_PREPROCESSORS_MODULE`):
  - Accepts either a pipeline-wide sequence `Sequence[FilePreprocessFn]` or a per-table mapping `dict[str, Sequence[FilePreprocessFn] | FilePreprocessFn]`.
  - Each `FilePreprocessFn` has signature `(file_item: FileItemDict) -> FileItemDict | None` and may return the `FileItemDict` to continue, return `None` to skip the file, or raise `FilePreprocessError` to reject the file and record the failure in `_rejected` and `report.txt`.
  - Built-in pre-processor: `cdm_data_loaders.core.preprocessors.validate_md5_sidecar` (enabled by default on the `pubmed` pipeline, where `file_glob` defaults to `"*.xml.gz"`).
- **`TABLE_TRANSFORMS`** (`--transforms-module` / `CDL_TRANSFORMS_MODULE`): `dict[str, RecordTransformFn]`
  - Each value is a callable `Callable[[dict[str, Any]], Mapping[str, Sequence[dict[str, Any]]]]`:
    - `{}` (or `{"table": []}`) -> **Filter out (drop)** the record.
    - `{"table": [new_row]}` -> **Transform** a row 1-to-1 in place.
    - `{"table": [row_1, row_2]}` -> **Unnest / fan out** 1-to-N rows into the same table.
    - `{"parent_table": [parent_row], "child_table": [child_1, child_2]}` -> **Split** a record across multiple destination tables.
    - `{"_rejected": [rejected_row]}` -> **Reject** a record into the shared `_rejected` table (`REJECTED_TABLE_NAME`).
  - When no transform is configured for a table, records are routed unchanged to `{table_name: [record]}`.
- **`OUTPUT_SCHEMAS`** (`--output-schemas-module` / `CDL_OUTPUT_SCHEMAS_MODULE`): `dict[str, pa.Schema]`
  - Optional mapping of `{destination_table_name: pyarrow.Schema}`.
  - When provided:
    - Overrides or supplies explicit `pyarrow.Schema` definitions for transformed or split destination tables.
    - Coerces emitted rows against each destination table's `pyarrow.Schema` and yields `pyarrow.Table` pages to `dlt`.

### Specifying Modules on the CLI or via Environment Variable

Pass `--transforms-module` (`CDL_TRANSFORMS_MODULE`) and/or `--output-schemas-module` (`CDL_OUTPUT_SCHEMAS_MODULE`):

```sh
--transforms-module my_package.transforms
--output-schemas-module my_package.transforms
```

---

## Example 1: Filtering Out Data

### Use Case: Dropping Empty NCBI Annotation Reports (`total_count == 0`)

NCBI Datasets API annotation report JSONL files contain summary lines where no annotations were found:

```json
{"reports": [{"annotation": {"gene_id": 1001, "name": "dnaA", "symbol": "dnaA"}}], "total_count": 1}
{"reports": [], "total_count": 0}
```

Both lines are valid against `annotationReport.schema.json`, so `jsonlines_jsonschema` accepts both during validation. To drop records with `total_count == 0` instead of writing empty rows to the `annotation_report` table:

```python
# my_package/ncbi_transforms.py
import json
from importlib.resources import files
from typing import Any

from cdm_data_loaders.core.registry import RecordTransformFn

ANNOTATION_REPORT_SCHEMA = json.loads(
    files("cdm_data_loaders.schemas.ncbi_datasets").joinpath("annotationReport.schema.json").read_text(encoding="utf-8")
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


TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "annotation_report": filter_nonzero_annotation_reports,
}
```

#### CLI Invocation

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_jsonschema \
  --entity-schemas my_package.ncbi_transforms \
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

from cdm_data_loaders.core.registry import RecordTransformFn


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


TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "genome_metrics": transform_genome_metrics,
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
  --schema-file ./schemas/genome_metrics.schema.json \
  --transforms-module my_package.quality_transforms \
  --use-destination local_fs \
  --loader-file-format parquet
```

### 2b. Single-Table Row Transformation with a Custom PyArrow Schema (`OUTPUT_SCHEMAS`)

When a single-table transformation adds, removes, or retypes columns relative to the input schema, supply `OUTPUT_SCHEMAS` via `--output-schemas-module` so `DictBuffer` serializes the transformed structure with the updated `pyarrow.Schema`:

```python
# my_package/widget_transforms.py
from typing import Any
import pyarrow as pa

from cdm_data_loaders.core.registry import RecordTransformFn

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


TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "widget": enrich_widget,
}

OUTPUT_SCHEMAS: dict[str, pa.Schema] = {
    "widget": WIDGET_OUTPUT_SCHEMA,
}
```

---

## Example 3: Splitting a Single Record Across Multiple Tables

A single `RecordTransformFn` can decompose one input record into rows for multiple destination tables in one pass.

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

from cdm_data_loaders.core.registry import RecordTransformFn


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


TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    # Keyed by the input table_name ("assembly_report")
    "assembly_report": split_assembly_and_contigs,
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
  --xml-tag Assembly \
  --transforms-module my_package.assembly_split_transforms \
  --use-destination local_fs \
  --loader-file-format parquet
```

### 3b. Multi-Table Split with PyArrow Schemas (`OUTPUT_SCHEMAS`)

When splitting records and supplying explicit Arrow schemas for output tables, provide a `pyarrow.Schema` for every output table in `OUTPUT_SCHEMAS`:

```python
# my_package/assembly_arrow_transforms.py
import pyarrow as pa

from cdm_data_loaders.core.registry import RecordTransformFn
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

TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "assembly_report": split_assembly_and_contigs,
}

OUTPUT_SCHEMAS: dict[str, pa.Schema] = {
    "assemblies": ASSEMBLIES_SCHEMA,
    "assembly_contigs": ASSEMBLY_CONTIGS_SCHEMA,
}
```

When `OUTPUT_SCHEMAS` is provided:
- Each row returned by `split_assembly_and_contigs` for a table present in `OUTPUT_SCHEMAS` is coerced to its target table's `pyarrow.Schema` via `coerce_record_for_pyarrow`.
- `DictBuffer` accumulates rows per output table (`assemblies` and `assembly_contigs`) and flushes `pyarrow.Table` pages independently when a table reaches `--buffer-size`.
- Any output table not present in the resolved schema map is left as uncoerced `dict` rows and emitted by `DictBuffer` as `list[dict]` pages.

---

## Programmatic Usage in Python

You can also pass `transforms_module` and `output_schemas_module` directly when constructing any pipeline settings class in Python:

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
    entity_schemas="my_package.ncbi_transforms",
    transforms_module="my_package.ncbi_transforms",
    use_destination="local_fs",
    use_output_dir_for_pipeline_metadata=True,
    loader_file_format="parquet",
)

load_info = run_jsonlines_pipeline(settings)
```
