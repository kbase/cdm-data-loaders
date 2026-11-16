# Record Filtering, Transformation, and Multi-Table Splitting

All six file-format ingestion pipelines support pluggable file pre-processing, record-level filtering, single-table row transformation, multi-table splitting, and `pyarrow.Schema` overrides through `--preprocessors-module` (`CDL_PREPROCESSORS_MODULE`), `--transforms-module` (`CDL_TRANSFORMS_MODULE`), and `--output-schemas-module` (`CDL_OUTPUT_SCHEMAS_MODULE`) defined in [`src/cdm_data_loaders/core/transforms.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/transforms.py) and [`src/cdm_data_loaders/core/registry.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/registry.py):

| Pipeline                                                                                                                                                                             | CLI Entry Point        | Default Output Format                            | Source of Input `table_name`                                                                    |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| [`extract_pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_pipeline.py)                                         | `jsonlines_extract`    | `list[dict]` (`pyarrow.Table` if schema map set) | `--table-name` (`settings.table_name`)                                                          |
| [`extract_jsonschema_validate_pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | `jsonlines_jsonschema` | `pyarrow.Table`                                  | Keys of `ENTITY_SCHEMAS` (or preset entrypoint `ncbi_datasets`)                                 |
| [`extract_pydantic_validate_pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_pydantic_validate_pipeline.py)     | `jsonlines_pydantic`   | `pyarrow.Table`                                  | Keys of `ENTITY_MODELS`                                                                         |
| [`xsv/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xsv/pipeline.py)                                                           | `xsv`                  | `pyarrow.Table`                                  | `--table-name` (`settings.table_name`)                                                          |
| [`xmltodict/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltodict/pipeline.py)                                               | `xmltodict`            | `list[dict]` (`pyarrow.Table` if schema map set) | `--table-name` (`settings.table_name`)                                                          |
| [`xmltopyarrow/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                         | `xmltopyarrow`         | `pyarrow.Table`                                  | `--table-name` (or preset entrypoints `pubmed`, `uniprot`, `uniref50`, `uniref90`, `uniref100`) |

---

## Processing Lifecycle

File pre-processors run **before** a file is opened and parsed. Record transforms run **after** a record is parsed and validated against its input schema or model, and **before** rows are buffered into pages by `DictBuffer` ([`src/cdm_data_loaders/utils/buffer.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/buffer.py)):

**Pre-process File (`FilePreprocessFn`)**:

  - Each `FileItemDict` yielded by `dlt.sources.filesystem` passes through any `FilePreprocessFn` functions registered in `FILE_PREPROCESSORS`.
  - Returning `None` skips the file without error.
  - Raising `FilePreprocessError` routes a diagnostic row to `_rejected` (with `source_file` and `error_detail`), logs an error, skips the file, and records the failure in `report.txt` (`load_errors`).

**Parse and Validate**:

  - Malformed lines or records that fail input validation (`JSON Schema`, `Pydantic`, or `qsv`) are routed directly to `_rejected` (with `table_name` set to the input table name) without calling the transform function.

**Transform and Route (`RecordTransformFn`)**:

  - Each valid record (`dict[str, Any]`) is passed to the `RecordTransformFn` registered in `TABLE_TRANSFORMS` for the input `table_name`.
  - The transform function returns a mapping of `{destination_table_name: [row_dict, ...]}`.
  - Returning `{}` (or empty row lists) drops the record.

**Coerce and Buffer (`DictBuffer`)**:

  - When `loader_file_format` is `parquet` or `iceberg` (`settings.use_pyarrow` is `True`), `_rejected` uses `REJECTED_PYARROW_SCHEMA`, and any destination table with an entry in `settings.schema_map` (either derived from the pipeline's input schema or supplied via `OUTPUT_SCHEMAS`) has its rows coerced against that `pyarrow.Schema` and emitted as a `pyarrow.Table` page.
  - When `loader_file_format` is `jsonl` (`settings.use_pyarrow` is `False`), `settings.schema_map` is `None` and all tables (including `_rejected`) are emitted as `list[dict]` pages.
  - When `loader_file_format` is `file_listing`, file pre-processors, parsers, validators, and table transforms are skipped, and the pipeline writes `file_listing.txt` instead (see [Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md)).

---

## Defining Pre-processor, Transform, and Output Schema Modules

Create an importable Python module exporting `FILE_PREPROCESSORS`, `TABLE_TRANSFORMS`, and/or `OUTPUT_SCHEMAS` using the callable types in [`src/cdm_data_loaders/core/registry.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/registry.py) and [`src/cdm_data_loaders/core/preprocessors.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/preprocessors.py):

**`FILE_PREPROCESSORS`** (`--preprocessors-module` / `CDL_PREPROCESSORS_MODULE`):

  - Accepts either a pipeline-wide sequence `Sequence[FilePreprocessFn]` or a per-table mapping `dict[str, Sequence[FilePreprocessFn] | FilePreprocessFn]`.
  - Each `FilePreprocessFn` has signature `(file_item: FileItemDict) -> FileItemDict | None`.
  - Built-in pre-processor: `validate_md5_sidecar` in [`src/cdm_data_loaders/core/preprocessors.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/preprocessors.py) (enabled by default on `pubmed`, and directly usable via `--preprocessors-module cdm_data_loaders.core.preprocessors`).

**`TABLE_TRANSFORMS`** (`--transforms-module` / `CDL_TRANSFORMS_MODULE`): `dict[str, RecordTransformFn]`

  - Each value is a callable `(record: dict[str, Any]) -> Mapping[str, Sequence[dict[str, Any]]]`:
    - `{}` (or `{"table": []}`) filters out (drops) the record.
    - `{"table": [new_row]}` transforms a row 1-to-1 in place.
    - `{"table": [row_1, row_2]}` unnests or fans out 1-to-N rows into the same table.
    - `{"parent_table": [parent_row], "child_table": [child_1, child_2]}` splits a record across multiple destination tables.
    - `{"_rejected": [rejected_row]}` routes a record to the shared `_rejected` table.
  - When no transform is configured for a table, records are routed unchanged to `{table_name: [record]}`.

**`OUTPUT_SCHEMAS`** (`--output-schemas-module` / `CDL_OUTPUT_SCHEMAS_MODULE`): `dict[str, pa.Schema]`

  - Optional mapping of `{destination_table_name: pyarrow.Schema}` overriding or supplying explicit `pyarrow.Schema` definitions for transformed or split destination tables.

---

## Example 1: Validating or Skipping Files Before Parsing (`FILE_PREPROCESSORS`)

Combine the built-in `validate_md5_sidecar` pre-processor with a custom `FilePreprocessFn` that skips zero-byte placeholder files and rejects files exceeding a size threshold:

```python
from typing import Final

from dlt.common.storages.fsspec_filesystem import FileItemDict

from cdm_data_loaders.core.preprocessors import FilePreprocessError, validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn
from cdm_data_loaders.utils.file_items import source_id

MAX_RAW_BYTES: Final[int] = 10 * 1024 * 1024 * 1024


def check_nonempty_and_size(file_item: FileItemDict) -> FileItemDict | None:
    """Skip empty files and reject files larger than MAX_RAW_BYTES.

    :param file_item: Discovered input file item.
    :type file_item: FileItemDict
    :return: Validated file item, or ``None`` when the file is empty.
    :rtype: FileItemDict | None
    """
    size_in_bytes = int(file_item.get("size_in_bytes") or 0)
    if size_in_bytes == 0:
        return None
    if size_in_bytes > MAX_RAW_BYTES:
        msg = f"File {source_id(file_item)} exceeds {MAX_RAW_BYTES} bytes ({size_in_bytes})"
        raise FilePreprocessError(msg)
    return file_item


FILE_PREPROCESSORS: Final[tuple[FilePreprocessFn, ...]] = (
    check_nonempty_and_size,
    validate_md5_sidecar,
)
```

#### CLI Invocation

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_extract \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name raw_events \
  --table-name events \
  --file-glob "*.jsonl.gz" \
  --preprocessors-module my_package.file_checks \
  --use-destination local_fs
```

---

## Example 2: Filtering Out Data

### Use Case: Dropping Empty NCBI Annotation Reports (`total_count == 0`)

NCBI Datasets API annotation report JSONL files contain summary lines where no annotations were found:

```json
{"reports": [{"annotation": {"gene_id": 1001, "name": "dnaA", "symbol": "dnaA"}}], "total_count": 1}
{"reports": [], "total_count": 0}
```

Both lines are valid against [`annotationReport.schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/ncbi_datasets/annotationReport.schema.json), so `jsonlines_jsonschema` accepts both during validation. To drop records with `total_count == 0` instead of writing empty rows to the `annotation_report` table:

```python
import json
from importlib.resources import files
from typing import Any, Final

from cdm_data_loaders.core.registry import RecordTransformFn

ANNOTATION_REPORT_SCHEMA: Final[dict[str, Any]] = json.loads(
    files("cdm_data_loaders.schemas.ncbi_datasets").joinpath("annotationReport.schema.json").read_text(encoding="utf-8")
)

# entity schemas and table transforms can live in the same module or separate modules
ENTITY_SCHEMAS: Final[dict[str, dict[str, Any]]] = {
    "annotation_report": ANNOTATION_REPORT_SCHEMA,
}


def filter_nonzero_annotation_reports(
    record: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Drop annotation report responses with total_count == 0.

    :param record: Validated annotation report record.
    :type record: dict[str, Any]
    :return: Destination table row mapping.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if record.get("total_count", 0) == 0:
        return {}
    return {"annotation_report": [record]}


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
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

## Example 3: Data Transformation Within a Single Table Row

### Transforming Fields in a Single Table (`xsv` or `jsonlines_extract`)

Suppose an XSV or JSONL pipeline ingests genome quality rows into table `genome_metrics` and you want to:

- filter out low-completeness rows (`completeness < 50.0`),
- normalize the `accession` prefix, and
- compute a derived `quality_score = completeness - (5.0 * contamination)` field within the row.

```python
from typing import Any, Final

from cdm_data_loaders.core.registry import RecordTransformFn


def transform_genome_metrics(
    record: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Filter low-completeness rows and add a derived quality_score column.

    :param record: Validated genome metrics row.
    :type record: dict[str, Any]
    :return: Destination table row mapping.
    :rtype: dict[str, list[dict[str, Any]]]
    """
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


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
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

### Single-Table Row Transformation with a Custom PyArrow Schema (`OUTPUT_SCHEMAS`)

When a single-table transformation adds, removes, or retypes columns relative to the input schema, supply `OUTPUT_SCHEMAS` via `--output-schemas-module` so `DictBuffer` serializes the transformed structure with the updated `pyarrow.Schema`:

```python
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.registry import RecordTransformFn

WIDGET_OUTPUT_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("widget_id", pa.string()),
        pa.field("count", pa.int64()),
        pa.field("is_bulk", pa.bool_()),
    ]
)


def enrich_widget(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Project fields and add a boolean is_bulk flag.

    :param record: Input widget record.
    :type record: dict[str, Any]
    :return: Destination table row mapping.
    :rtype: dict[str, list[dict[str, Any]]]
    """
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


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "widget": enrich_widget,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "widget": WIDGET_OUTPUT_SCHEMA,
}
```

---

## Example 4: Splitting a Single Record Across Multiple Tables

A single `RecordTransformFn` can decompose one input record into rows for multiple destination tables in one pass.

### Multi-Table Split with `DictBuffer` (`jsonlines_extract`, `jsonlines_pydantic`, `xsv`, `xmltodict`)

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
from typing import Any, Final

from cdm_data_loaders.core.registry import RecordTransformFn


def split_assembly_and_contigs(
    record: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Filter empty reports and split assembly metadata from per-contig rows.

    :param record: Input assembly report record.
    :type record: dict[str, Any]
    :return: Parent and child destination table rows.
    :rtype: dict[str, list[dict[str, Any]]]
    """
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


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    # keyed by the input table_name ("assembly_report")
    "assembly_report": split_assembly_and_contigs,
}
```

- The key in `TABLE_TRANSFORMS` (`"assembly_report"`) matches the pipeline's input `table_name` (`--table-name assembly_report` or `ENTITY_MODELS["assembly_report"]`).
- Any malformed or invalid input records are still written to `_rejected` with `table_name = "assembly_report"`.
- Valid records are split and written to the `assemblies` and `assembly_contigs` destination tables.
- If a record has `contigs: []`, `"assembly_contigs": []` is ignored so only `"assemblies"` receives a row.

#### CLI Invocation (`jsonlines_extract` or `xmltodict`)

```sh
# with jsonlines_extract
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_extract \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name assemblies_ds \
  --table-name assembly_report \
  --transforms-module my_package.assembly_split_transforms \
  --use-destination local_fs \
  --loader-file-format parquet

# with xmltodict
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

### Multi-Table Split with PyArrow Schemas (`OUTPUT_SCHEMAS`)

When splitting records and supplying explicit Arrow schemas for output tables, provide a `pyarrow.Schema` for every output table in `OUTPUT_SCHEMAS`:

```python
from typing import Final

import pyarrow as pa

from cdm_data_loaders.core.registry import RecordTransformFn
from my_package.assembly_split_transforms import split_assembly_and_contigs

ASSEMBLIES_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("assembly_id", pa.string()),
        pa.field("organism_name", pa.string()),
        pa.field("contig_count", pa.int64()),
    ]
)

ASSEMBLY_CONTIGS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("assembly_id", pa.string()),
        pa.field("contig_id", pa.string()),
        pa.field("length", pa.int64()),
        pa.field("gc_percent", pa.float64()),
    ]
)

TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "assembly_report": split_assembly_and_contigs,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
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

Pass `preprocessors_module`, `transforms_module`, and `output_schemas_module` directly when constructing any pipeline settings class in Python:

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

---

## Example 4: Vectorized PyArrow Table Transforms and Raw Table Emission (`emit_raw_table` and `TransformContext`)

When ingesting large tabular files via `xsv` (or a preset XSV pipeline such as `uniprot_idmapping`), annotating a transform's first parameter as `pyarrow.Table` keeps the entire batch vectorized in Arrow without materializing per-row Python dicts. Composing `emit_raw_table` with a specialised Arrow transform in `TABLE_TRANSFORMS` emits both `raw_<table_name>` and the standardised destination table in a single pass (as implemented in [`parsers/uniprot/idmapping.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/idmapping.py)):

```python
import pyarrow as pa
import pyarrow.compute as pc

from cdm_data_loaders.core.registry import TableTransformFn, TransformContext
from cdm_data_loaders.core.transforms import emit_raw_table

RAW_ID_MAPPING_SCHEMA = pa.schema(
    [
        pa.field("uniprot_id", pa.string(), nullable=False),
        pa.field("db_reference_type", pa.string(), nullable=False),
        pa.field("db_reference_id", pa.string(), nullable=False),
    ],
    metadata={"comment": "raw ID mapping data from UniProt"},
)

ID_MAPPING_SCHEMA = pa.schema(
    [
        pa.field("uniprot_id", pa.string(), nullable=False),
        pa.field("db_reference", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ],
    metadata={"comment": "Standardised mapping data from UniProt"},
)


def transform_id_mapping(table: pa.Table, ctx: TransformContext) -> dict[str, pa.Table]:
    """Build CURIE-prefixed identifiers and attach the input file basename."""
    if table.num_rows == 0:
        return {}
    uniprot_col = pc.binary_join_element_wise("uniprot:", table.column("uniprot_id"), "")
    db_ref_col = pc.binary_join_element_wise(
        table.column("db_reference_type"),
        table.column("db_reference_id"),
        ":",
    )
    source_col = pa.repeat(pa.scalar(ctx.file_name or "", type=pa.string()), table.num_rows)
    return {
        "id_mapping": pa.Table.from_arrays(
            [uniprot_col, db_ref_col, source_col],
            schema=ID_MAPPING_SCHEMA,
        )
    }


TABLE_TRANSFORMS: dict[str, tuple[TableTransformFn, ...]] = {
    "id_mapping": (emit_raw_table, transform_id_mapping),
}

OUTPUT_SCHEMAS: dict[str, pa.Schema] = {
    "raw_id_mapping": RAW_ID_MAPPING_SCHEMA,
    "id_mapping": ID_MAPPING_SCHEMA,
}
```
