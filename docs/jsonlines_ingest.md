# JSONLines Ingestion Pipelines

Three pipelines under [`src/cdm_data_loaders/pipelines/jsonlines`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines) (backed by [`src/cdm_data_loaders/readers/jsonlines.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/readers/jsonlines.py)) load JSONL files (`.jsonl` and gzip-compressed `.jsonl.gz`) through the shared CTS runner. They share file discovery, pre-processor and table transformation hooks, and error routing, and differ in how records are validated and buffered before loading:

| Pipeline | CLI Entry Point | Validation | Default Output Format |
| --- | --- | --- | --- |
| [`extract_pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_pipeline.py) | [`jsonlines_extract`](cli_reference.md#jsonlines_extract) | None beyond valid JSON | `list[dict]` (`pyarrow.Table` if schema map set) |
| [`extract_jsonschema_validate_pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | [`jsonlines_jsonschema`](cli_reference.md#jsonlines_jsonschema), [`ncbi_datasets`](cli_reference.md#ncbi_datasets) | JSON Schema, per entity table | `pyarrow.Table` (`JSONSchemaToPyArrow`) |
| [`extract_pydantic_validate_pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/jsonlines/extract_pydantic_validate_pipeline.py) | [`jsonlines_pydantic`](cli_reference.md#jsonlines_pydantic) | Pydantic models, per entity table | `pyarrow.Table` (`PydanticToPyArrow`) |

For behavioral specifications and fixture cases, see [JSONLines Extract](jsonlines_extract_specification.md), [JSONLines (JSON Schema)](jsonlines_jsonschema_specification.md), and [JSONLines (Pydantic)](jsonlines_pydantic_specification.md). For all CLI flags and `CDL_*` environment variables, see the [CLI Reference](cli_reference.md).

---

## Extract-Only Pipeline (`jsonlines_extract`)

`jsonlines_extract` loads every record from `input_dir` matching `--file-glob` (default `*.jsonl*`) into the destination table named by `--table-name` (`-t`), with no validation beyond parsing each non-blank line as JSON. Files may live anywhere under `input_dir` (pass a recursive glob such as `--file-glob "**/*.jsonl*"` to include nested subdirectories); there is no per-table subdirectory layout.

Records that fail JSON parsing are routed to `_rejected` with `table_name` set to `--table-name`.

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_extract \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name raw_widgets \
  --table-name widget \
  --use-destination local_fs \
  --use-output-dir-for-pipeline-metadata true \
  --loader-file-format parquet
```

---

## Validated Pipelines (`jsonlines_jsonschema`, `ncbi_datasets`, `jsonlines_pydantic`)

`jsonlines_jsonschema` and `jsonlines_pydantic` share the same per-entity directory layout and `_rejected` table behavior; they differ in how schemas and models are declared, how validation errors are formatted, and how valid records are typed and buffered.

### Input Directory Layout

Each registered table name corresponds to a subdirectory under `input_dir`:

```text
input/
  widget/
    first.jsonl
    second.jsonl.gz
  gadget/
    batch_01.jsonl
```

- Within each `<input_dir>/<table_name>/` subdirectory, files matching `--file-glob` (default `*.jsonl*`, or `**/*.jsonl*` to include nested subdirectories) are read.
- Blank lines are skipped.
- Missing entity subdirectories and empty files produce zero rows without failing the pipeline.
- Duplicate records are retained.
- Write disposition is `append` (default) or `replace`; no merge is performed.

### JSON Schema Pipeline (`jsonlines_jsonschema`)

Provide an importable Python module via `--entity-schemas` (`-m`) that defines `ENTITY_SCHEMAS: dict[str, dict[str, Any]]`. Each key names an entity table and its input subdirectory; each value is a JSON Schema dictionary declaring a supported `$schema` draft:

```python
from typing import Any, Final

ENTITY_SCHEMAS: Final[dict[str, dict[str, Any]]] = {
    "widget": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["widget_id", "count"],
        "properties": {
            "widget_id": {"type": "string", "minLength": 1},
            "count": {"type": "integer", "minimum": 0},
            "collected": {"type": "string", "format": "date"},
        },
    },
}
```

Before any files are read, `jsonlines_jsonschema`:

- Validates the entire `ENTITY_SCHEMAS` registry; missing or unsupported `$schema` drafts or invalid schemas raise pre-execution configuration errors before extraction starts.
- Dereferences `$ref` and `allOf` keywords and converts each entity's JSON Schema to a `pyarrow.Schema` via `JSONSchemaToPyArrow`, unless that table has a configured transform in `--transforms-module` or an explicit schema override in `--output-schemas-module`.

During ingestion:

- Validation uses each schema's declared draft with `jsonschema.FormatChecker()` enabled.
- Valid records are coerced to the target `pyarrow.Schema` and buffered into `pyarrow.Table` pages via `DictBuffer` ([`src/cdm_data_loaders/utils/buffer.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/buffer.py)). Nested objects and arrays are preserved as Arrow `struct` and `list` columns rather than flattened into child tables.

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_jsonschema \
  --entity-schemas my_schemas \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name validated_widgets \
  --use-destination local_fs \
  --use-output-dir-for-pipeline-metadata true \
  --loader-file-format parquet
```

#### NCBI Datasets Preset (`ncbi_datasets`)

The [`ncbi_datasets`](cli_reference.md#ncbi_datasets) CLI entry point runs `jsonlines_jsonschema` with `dataset_name` locked to `"ncbi_datasets"` and precomputed JSON Schemas, PyArrow schemas, and record transforms from [`src/cdm_data_loaders/datasets/ncbi_datasets/hooks.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/datasets/ncbi_datasets/hooks.py):

- `dataset_report`: validates `<input_dir>/dataset_report/` records against `datasetReport.schema.json` and loads them into `dataset_report`.
- `annotation_report`: validates `<input_dir>/annotation_report/` records against `annotationReport.schema.json`, discards records without a non-empty top-level `reports` array via `filter_annotation_report`, and loads the remaining records into `annotation_report`.

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run ncbi_datasets \
  --input-dir ./ncbi_jsonl_input \
  --output-dir ./output \
  --use-destination local_fs \
  --use-output-dir-for-pipeline-metadata true \
  --loader-file-format parquet
```

### Pydantic Pipeline (`jsonlines_pydantic`)

Provide an importable Python module via `--entity-models` (`-m`) that defines `ENTITY_MODELS: dict[str, type[BaseModel]]`. Each key names an entity table and its input subdirectory; each value is the Pydantic `BaseModel` subclass used to validate that table's records:

```python
from datetime import date
from typing import Final

from pydantic import BaseModel


class Widget(BaseModel):
    """Validated widget record model."""

    widget_id: str
    count: int
    collected: date | None = None


ENTITY_MODELS: Final[dict[str, type[BaseModel]]] = {"widget": Widget}
```

Before any files are read, `jsonlines_pydantic`:

- Validates the `ENTITY_MODELS` registry.
- Converts each entity's Pydantic model to a `pyarrow.Schema` via `PydanticToPyArrow` (honoring `--max-recursion-depth` and `--by-alias`), unless that table has a configured transform in `--transforms-module` or an explicit schema override in `--output-schemas-module`.

During ingestion:

- Each parsed JSON record is validated once with `model.model_validate(record)`, so Pydantic's type coercion and field defaults apply.
- Validated models are coerced to the target `pyarrow.Schema` via `cdm_data_loaders.coercion.pydantic.coerce_record_for_pyarrow` (unwrapping `SecretStr` and `SecretBytes`, sorting `set` and `frozenset` values, quantizing `Decimal` fields, normalizing temporal values, and serializing overflowed recursion depths as JSON) and buffered into `pyarrow.Table` pages via `DictBuffer`. When a table transform is configured or `--loader-file-format jsonl` is selected, records are dumped via `model_dump(mode="python", by_alias=settings.by_alias)` before routing, and any `OUTPUT_SCHEMAS` destination tables are still coerced via `coerce_record_for_pyarrow`.

```sh
DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem \
uv run jsonlines_pydantic \
  --entity-models my_models \
  --input-dir ./input \
  --output-dir ./output \
  --dataset-name validated_widgets \
  --use-destination local_fs \
  --use-output-dir-for-pipeline-metadata true \
  --loader-file-format parquet
```

---

## Handling of Rows That Fail JSON Parsing or Validation

All JSONL pipelines route failed records to a single destination table named `_rejected` (`REJECTED_TABLE_NAME = "_rejected"` in [`src/cdm_data_loaders/utils/buffer.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/buffer.py)). If every record in a run is valid, `_rejected` is not created.

### `_rejected` Table Schema

Every row in `_rejected` has the same five columns (`REJECTED_PYARROW_SCHEMA`):

| Column | Arrow Type | Description |
| --- | --- | --- |
| `table_name` | `string` | Target input table the record belonged to (`--table-name` for `jsonlines_extract`, or the entity key in `ENTITY_SCHEMAS` or `ENTITY_MODELS` for the validated pipelines, recorded before any `TableTransform`). |
| `source_file` | `string` | Path to the input file relative to `input_dir` (for example, `widget/batch.jsonl`). |
| `line_no` | `int64` | 1-based physical line number in `source_file`, counting blank lines. |
| `raw_record` | `string` | Stripped raw text of the line as read from the file (never re-serialized from parsed JSON). |
| `error_detail` | `string` | Diagnostic message describing why the line failed JSON parsing or validation. |

### Row Fails JSON Parsing

When a non-blank line is not valid JSON (`json.loads` raises `json.JSONDecodeError`):

- **Validation and transforms are bypassed**: Schema or Pydantic validation and any configured `TableTransform` are skipped.
- **Routed to `_rejected`**: A row is emitted to `_rejected` with `table_name`, `source_file`, the 1-based `line_no`, the stripped unparseable `raw_record`, and `error_detail` set to `str(JSONDecodeError)`:

```json
{
  "table_name": "widget",
  "source_file": "widget/batch.jsonl",
  "line_no": 2,
  "raw_record": "{\"widget_id\": \"w1\", \"count\": }",
  "error_detail": "Expecting value: line 1 column 29 (char 28)"
}
```

### Row Is Valid JSON, Fails Validation

When a line parses as a JSON object but fails schema or model validation:

- **`jsonlines_extract`**: Performs no schema or model validation; any valid JSON object proceeds to transformation and buffering.
- **`jsonlines_jsonschema` and `ncbi_datasets`**: Evaluates the parsed object with `validator.iter_errors(record)` (with `FormatChecker()` active). If errors are found, transforms are skipped and the record is routed to `_rejected` with `error_detail` set to a deterministic JSON-encoded list of error message strings:
  ```json
  {
    "table_name": "widget",
    "source_file": "widget/batch.jsonl",
    "line_no": 3,
    "raw_record": "{\"widget_id\": \"\", \"count\": -1}",
    "error_detail": "[\"-1 is less than the minimum of 0\", \"'' should be non-empty\"]"
  }
  ```
- **`jsonlines_pydantic`**: Evaluates the parsed object with `model.model_validate(record)`. If `pydantic.ValidationError` is raised, transforms are skipped and the record is routed to `_rejected` with `error_detail` set to `json.dumps(e.errors(), default=str)`:
  ```json
  {
    "table_name": "widget",
    "source_file": "widget/batch.jsonl",
    "line_no": 3,
    "raw_record": "{\"widget_id\": \"w1\", \"count\": \"not-an-int\"}",
    "error_detail": "[{\"type\": \"int_parsing\", \"loc\": [\"count\"], \"msg\": \"Input should be a valid integer, unable to parse string as an integer\", \"input\": \"not-an-int\", \"url\": \"https://errors.pydantic.dev/2.13/v/int_parsing\"}]"
  }
  ```

---

## Pre-processors, Transforms, and CLI Options

All JSONL pipelines accept `--preprocessors-module` (`CDL_PREPROCESSORS_MODULE`), `--transforms-module` (`CDL_TRANSFORMS_MODULE`), and `--output-schemas-module` (`CDL_OUTPUT_SCHEMAS_MODULE`). In `jsonlines_jsonschema` and `jsonlines_pydantic`, every key in `TABLE_TRANSFORMS` must match a registered table name in `ENTITY_SCHEMAS` or `ENTITY_MODELS`.

- See [Table and Column Transformations](table_transforms.md) for runnable pre-processor, filter, transform, and multi-table split examples.
- See the [CLI Reference](cli_reference.md) for complete option tables: [`jsonlines_extract`](cli_reference.md#jsonlines_extract), [`jsonlines_jsonschema`](cli_reference.md#jsonlines_jsonschema), [`ncbi_datasets`](cli_reference.md#ncbi_datasets), and [`jsonlines_pydantic`](cli_reference.md#jsonlines_pydantic).
