# JSONL Ingestion Pipelines

Three pipelines under `pipelines/jsonlines/` load JSONL files (`.jsonl` and gzip-compressed `.jsonl.gz`) through the shared CTS runner. They share settings, file discovery, table transformation hooks, and error routing, and differ in how (or whether) records are validated and buffered before loading:

| Pipeline                                                                                                                       | CLI Entry Point                         | Validation                        | Default Output Format                            |
| ------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------- | --------------------------------- | ------------------------------------------------ |
| [extract_pipeline.py](../src/cdm_data_loaders/pipelines/jsonlines/extract_pipeline.py)                                         | `jsonlines_extract`                     | None beyond valid JSON            | `list[dict]` (`pyarrow.Table` if schema map set) |
| [extract_jsonschema_validate_pipeline.py](../src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | `jsonlines_jsonschema`, `ncbi_datasets` | JSON Schema, per entity table     | `pyarrow.Table` (`JSONSchemaToPyArrow`)          |
| [extract_pydantic_validate_pipeline.py](../src/cdm_data_loaders/pipelines/jsonlines/extract_pydantic_validate_pipeline.py)     | `jsonlines_pydantic`                    | Pydantic models, per entity table | `pyarrow.Table` (`PydanticToPyArrow`)            |

All three support plain and gzip-compressed inputs, Parquet, JSONL, or Iceberg output, optional record-level transforms (`--transforms-module`), and a unified `_rejected` table for parse and validation failures.

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

## The Two Validated Pipelines

`jsonlines_jsonschema` and `jsonlines_pydantic` share the same per-entity directory layout and `_rejected` table behavior; they differ in how schemas/models are declared, how validation errors are formatted, and how valid records are typed and buffered.

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
# my_schemas.py
ENTITY_SCHEMAS = {
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
1. Validates the entire `ENTITY_SCHEMAS` registry (`load_registry_dict(..., "ENTITY_SCHEMAS")`). Missing or unsupported `$schema` drafts raise `ValueError`; invalid schemas across all tables are reported together in a `RuntimeError`.
2. Dereferences `$ref` / `allOf` (`dereference_schema`) and converts each entity's JSON Schema to a `pyarrow.Schema` via `JSONSchemaToPyArrow` (`build_output_schema_registry`), unless that table has a configured transform in `--transforms-module` or an explicit schema override in `--output-schemas-module`. Any conversion failures across tables are reported together in a `RuntimeError`.

During ingestion:
- Validation uses each schema's declared draft with `jsonschema.FormatChecker()` enabled.
- Valid records are coerced to the target `pyarrow.Schema` (`coerce_record_for_pyarrow`) and buffered into `pyarrow.Table` pages via `DictBuffer`. Nested objects and arrays are preserved as Arrow `struct` and `list` columns rather than flattened into child tables.

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

The `ncbi_datasets` CLI entry point runs `jsonlines_jsonschema` with `dataset_name` locked to `"ncbi_datasets"` and precomputed JSON Schemas, PyArrow schemas, and record transforms from `cdm_data_loaders.schemas.ncbi_datasets.ncbi_openapi`:

- `dataset_report`: validates `<input_dir>/dataset_report/` records against `datasetReport.schema.json` and loads them into `dataset_report`.
- `annotation_report`: validates `<input_dir>/annotation_report/` records against `annotationReport.schema.json`, discards records without a non-empty top-level `reports` array via `filter_annotation_report`, and loads the remaining records into `annotation_report`.

### Pydantic Pipeline (`jsonlines_pydantic`)

Provide an importable Python module via `--entity-models` (`-m`) that defines `ENTITY_MODELS: dict[str, type[BaseModel]]`. Each key names an entity table and its input subdirectory; each value is the Pydantic `BaseModel` subclass used to validate that table's records:

```python
# my_models.py
from datetime import date
from pydantic import BaseModel


class Widget(BaseModel):
    widget_id: str
    count: int
    collected: date | None = None


ENTITY_MODELS: dict[str, type[BaseModel]] = {"widget": Widget}
```

Before any files are read, `jsonlines_pydantic`:
1. Validates the `ENTITY_MODELS` registry (`load_registry_dict(..., "ENTITY_MODELS")`).
2. Converts each entity's Pydantic model to a `pyarrow.Schema` via `PydanticToPyArrow` (`build_output_schema_registry`), unless that table has a configured transform in `--transforms-module` or an explicit schema override in `--output-schemas-module`. Any conversion failures across tables are reported together in a `RuntimeError`.

During ingestion:
- Each parsed JSON record is validated once with `model.model_validate(record)` and dumped via `model_dump(mode="python")`, so Pydantic's type coercion and field defaults apply.
- By default, valid records are coerced to the target `pyarrow.Schema` and buffered into `pyarrow.Table` pages via `DictBuffer` (keeping nested structures as Arrow `struct` and `list` columns rather than normalizing into `dlt` child tables). If a table transform is configured without a corresponding `OUTPUT_SCHEMAS` entry, records are buffered as `list[dict]` pages with `max_table_nesting=0`.

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

All three JSONL pipelines route failed records to a single destination table named `_rejected` (`REJECTED_TABLE_NAME = "_rejected"`). If every record in a run is valid, `_rejected` is not created.

### `_rejected` Table Schema

Every row in `_rejected` has the same five columns (matching `REJECTED_PYARROW_SCHEMA` when buffered with Arrow):

| Column         | Arrow Type | Description                                                                                                                                                                                                    |
| -------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `table_name`   | `string`   | Target input table the record belonged to (`--table-name` for `jsonlines_extract`, or the entity key in `ENTITY_SCHEMAS` / `ENTITY_MODELS` for the validated pipelines, recorded before any `TableTransform`). |
| `source_file`  | `string`   | Path to the input file relative to `input_dir` (e.g., `widget/batch.jsonl`).                                                                                                                                   |
| `line_no`      | `int64`    | 1-based physical line number in `source_file`, counting blank lines.                                                                                                                                           |
| `raw_record`   | `string`   | Stripped raw text of the line as read from the file (never re-serialized from parsed JSON).                                                                                                                    |
| `error_detail` | `string`   | Diagnostic message describing why the line failed JSON parsing or validation.                                                                                                                                  |

### Case 1: Row Fails JSON Parsing

When a non-blank line is not valid JSON (`json.loads` raises `json.JSONDecodeError`):

1. **Validation and transforms are bypassed**: In `jsonlines_jsonschema` and `jsonlines_pydantic`, the schema or Pydantic validator is not called. Any configured `TableTransform` (`transform_fn`) is also skipped.
2. **Routed to `_rejected`**: A row is emitted to `_rejected` with:
   - `table_name`: the target input table name,
   - `source_file`: the relative file path,
   - `line_no`: the 1-based line number,
   - `raw_record`: the stripped unparseable text,
   - `error_detail`: the plain `str(JSONDecodeError)` message from Python's `json` parser.

Example `_rejected` row for a malformed JSON line in `widget/batch.jsonl`:

```json
{
  "table_name": "widget",
  "source_file": "widget/batch.jsonl",
  "line_no": 2,
  "raw_record": "{\"widget_id\": \"w1\", \"count\": }",
  "error_detail": "Expecting value: line 1 column 29 (char 28)"
}
```

### Case 2: Row Is Valid JSON, Fails Validation

When a line parses as a JSON object but fails schema or model validation:

1. **`jsonlines_extract`**: Does not perform schema or model validation; any valid JSON object proceeds to transformation/buffering.
2. **`jsonlines_jsonschema`**:
   - Evaluates the parsed object with `validator.iter_errors(record)` (with `FormatChecker()` active).
   - If one or more errors are found, any `TableTransform` is skipped and the record is routed to `_rejected`.
   - `raw_record` preserves the original stripped line from the file.
   - `error_detail` is a **JSON-encoded list of error message strings** sorted deterministically (`json.dumps([error.message for error in errors])`):
     ```json
     {
       "table_name": "widget",
       "source_file": "widget/batch.jsonl",
       "line_no": 3,
       "raw_record": "{\"widget_id\": \"\", \"count\": -1}",
       "error_detail": "[\"-1 is less than the minimum of 0\", \"'' should be non-empty\"]"
     }
     ```
3. **`jsonlines_pydantic`**:
   - Evaluates the parsed object with `model.model_validate(record)`.
   - If `pydantic.ValidationError` is raised, any `TableTransform` is skipped and the record is routed to `_rejected`.
   - `raw_record` preserves the original stripped line from the file.
   - `error_detail` is a **JSON-encoded list of Pydantic error dictionaries** (`json.dumps(e.errors(), default=str)`), where each entry includes `type`, `loc`, `msg`, `input`, and optional `ctx` / `url`:
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

## Record Filtering, Transformation, and Multi-Table Splitting

All three JSONL pipelines accept `--transforms-module` (`CDL_TRANSFORMS_MODULE`), pointing to a Python module that defines `TABLE_TRANSFORMS: dict[str, RecordTransformFn]`, and `--output-schemas-module` (`CDL_OUTPUT_SCHEMAS_MODULE`), pointing to a Python module that defines `OUTPUT_SCHEMAS: dict[str, pa.Schema]`.

- Transforms run **after** a line has passed JSON parsing and schema/model validation.
- A transform function (`RecordTransformFn`) can filter out records (by returning `{}`), transform fields within a row, and split one input record across multiple destination tables; custom `pyarrow.Schema` mappings can be supplied via `OUTPUT_SCHEMAS`.
- In `jsonlines_jsonschema` and `jsonlines_pydantic`, every key in `TABLE_TRANSFORMS` must match a registered table name in `ENTITY_SCHEMAS` or `ENTITY_MODELS` (unknown table names raise `ValueError` before extraction begins).

See [table_transforms.md](table_transforms.md) for full details and end-to-end examples.

---

## CLI Options Reference

Standard CTS settings and `CDL_`-prefixed environment variables apply to all three pipelines:

| Option                                         | Env Var                                    | Applies To             | Description                                                                                                                                                                         |
| ---------------------------------------------- | ------------------------------------------ | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--input-dir`, `-i`                            | `CDL_INPUT_DIR`                            | all                    | Input directory (`file://` or local path). Contains JSONL files directly (`jsonlines_extract`) or one subdirectory per entity table (`jsonlines_jsonschema`, `jsonlines_pydantic`). |
| `--output-dir`, `-o`                           | `CDL_OUTPUT_DIR`                           | all                    | Local output directory for `report.txt` and optional pipeline metadata.                                                                                                             |
| `--dataset-name`                               | `CDL_DATASET_NAME`                         | all                    | Non-empty destination dataset name.                                                                                                                                                 |
| `--table-name`, `-t`                           | `CDL_TABLE_NAME`                           | `jsonlines_extract`    | Destination table name for valid records (and `table_name` value in `_rejected`).                                                                                                   |
| `--entity-schemas`, `-m`                       | `CDL_ENTITY_SCHEMAS`                       | `jsonlines_jsonschema` | Dotted import path to a module defining `ENTITY_SCHEMAS: dict[str, dict]`.                                                                                                          |
| `--entity-models`, `-m`                        | `CDL_ENTITY_MODELS`                        | `jsonlines_pydantic`   | Dotted import path to a module defining `ENTITY_MODELS: dict[str, type[BaseModel]]`.                                                                                                |
| `--transforms-module`                          | `CDL_TRANSFORMS_MODULE`                    | all                    | Optional module path defining `TABLE_TRANSFORMS: dict[str, RecordTransformFn]`.                                                                                                     |
| `--output-schemas-module`                      | `CDL_OUTPUT_SCHEMAS_MODULE`                | all                    | Optional module path defining `OUTPUT_SCHEMAS: dict[str, pa.Schema]`.                                                                                                               |
| `--file-glob`, `-g`                            | `CDL_FILE_GLOB`                            | all                    | File glob pattern within each input directory; defaults to `*.jsonl*`.                                                                                                              |
| `--buffer-size`                                | `CDL_BUFFER_SIZE`                          | all                    | Number of rows buffered per table before yielding a page (positive integer, default `100`).                                                                                         |
| `--loader-file-format`, `-f`                   | `CDL_LOADER_FILE_FORMAT`                   | all                    | Output file or table format: `parquet` (default), `jsonl`, or `iceberg`.                                                                                                            |
| `--use-destination`, `-d`                      | `CDL_USE_DESTINATION`                      | all                    | Name of the `dlt` destination block (default `cts`).                                                                                                                                |
| `--use-output-dir-for-pipeline-metadata`, `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | all                    | Store `dlt` pipeline metadata under `<output_dir>/pipeline_metadata` (default `false`).                                                                                             |
| `--dlt-dev-mode`                               | `CDL_DLT_DEV_MODE`                         | all                    | Enable `dlt` dev mode (resets state and appends a timestamp to the dataset name; default `false`).                                                                                  |
| `--log-config-file`                            | `CDL_LOG_CONFIG_FILE`                      | all                    | Optional path to a JSON logging configuration file.                                                                                                                                 |
| `--log-interval`                               | `CDL_LOG_INTERVAL`                         | all                    | Progress log interval in processed entries (default `1000`).                                                                                                                        |

---

## Tests

Run the unit and end-to-end tests for the JSONL reader and pipelines:

```sh
uv run pytest \
  tests/cdm_data_loaders/readers/test_readers_jsonlines.py \
  tests/cdm_data_loaders/pipelines/jsonlines/
```
