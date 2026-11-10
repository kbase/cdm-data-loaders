<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/pipelines/jsonlines_extract/`. Regenerate with `uv run python -m scripts.gen_pipeline_spec` or `uv run python -m scripts.gen_converter_spec`. -->

# JSONLines Extract Pipeline Specification

This document specifies how the **JSONLines Extract ingestion pipeline**
(`cdm_data_loaders.pipelines.jsonlines.extract_pipeline` and `cdm_data_loaders.readers.jsonlines`)
discovers, pre-processes, streams, transforms, and loads JSON Lines (`.jsonl` and `.jsonl.gz`)
files into a single target table (or transform-routed tables) without an upfront schema:

1. **File discovery, `.gz` decompression, and file pre-processing** (`get_matching_files`, `open_file`, `preprocess_and_record`).
2. **Line streaming, blank-line skipping, and dead-letter (`_rejected`) routing** (`route_jsonl_lines`, `REJECTED_PYARROW_SCHEMA`).
3. **Table transforms, PyArrow output schema coercion, and pipeline reporting** (`route_and_coerce_record`, `DictBuffer`, `run_jsonlines_pipeline`).

| Property | Value |
| --- | --- |
| **Base Pipeline Family** | `jsonlines_extract` |
| **Base CLI Entry Point** | `jsonlines_extract` |
| **Base Settings Class** | `cdm_data_loaders.pipelines.jsonlines.settings.JsonlExtractSettings` |
| **Pipeline Runner** | `cdm_data_loaders.pipelines.jsonlines.extract_pipeline.run_jsonlines_pipeline` |
| **Preset Subclasses Covered** | — |
| **Specification Directory** | `specs/pipelines/jsonlines_extract/` |

## Running Smoke Tests Across Environments

```bash
# Run all base and preset smoke tests for jsonlines_extract across built-in environments
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py --smoke-pipeline jsonlines_extract

# Run against a custom environment directory (config.toml, secrets.toml, settings.toml)
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py \
    --smoke-pipeline jsonlines_extract \
    --dlt-config-dir /path/to/env_dlt_config \
    --settings-file /path/to/env_dlt_config/settings.toml
```

---

## 1. Conversion & Pipeline Rules

### 1.1 File Discovery, Decompression, and Pre-processing

<a id="r-jsonl-extract-discovery"></a>

- **R-JSONL-EXTRACT-DISCOVERY**: `run_jsonlines_pipeline` scans `settings.input_dir` using `get_matching_files(settings.input_dir, settings.file_glob)`
  (default `file_glob="*.jsonl*"`), discovering matching files across nested subdirectories in sorted order
  and transparently decompressing `.gz` archives via `open_file`. Non-matching files are ignored.

  *Cases:* [`spec/01_nested_glob_gzip_and_parse_rejections`](#case-spec-01-nested-glob-gzip-and-parse-rejections)

<a id="r-jsonl-extract-preprocess-md5"></a>

- **R-JSONL-EXTRACT-PREPROCESS-MD5**: Before opening a matched file, `preprocess_and_record` runs each callable in `settings.file_preprocessors`
  (resolved from `preprocessors_module`, e.g. `validate_md5_sidecar`). When a pre-processor returns a failure
  reason, the file is skipped, omitted from `ingested_files`, recorded in `report.txt` `load_errors` as
  `"File pre-processing failed for <rel_path>: <reason>"` (`status: completed with errors`), and written to
  `_rejected` with `table_name=settings.table_name`, `source_file=<rel_path>`, `line_no=None`,
  `raw_record=None`, and `error_detail=<reason>`.

  *Cases:* [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing)

### 1.2 Line Streaming, Blank-Line Skipping, and Dead-Letter Routing

<a id="r-jsonl-extract-stream"></a>

- **R-JSONL-EXTRACT-STREAM**: `route_jsonl_lines` streams each file line by line with 1-based line numbers (`enumerate(f, start=1)`),
  skipping empty or whitespace-only lines without recording an error while preserving exact 1-based `line_no`
  offsets for subsequent lines, and parses each non-blank line with `json.loads`.

  *Cases:* [`spec/01_nested_glob_gzip_and_parse_rejections`](#case-spec-01-nested-glob-gzip-and-parse-rejections), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

<a id="r-jsonl-extract-reject"></a>

- **R-JSONL-EXTRACT-REJECT**: Lines that raise `json.JSONDecodeError` are routed to the `_rejected` dead-letter table
  (`REJECTED_PYARROW_SCHEMA`) with `table_name=settings.table_name`, `source_file` set to the POSIX path
  relative to `input_dir`, 1-based `line_no`, stripped `raw_record`, and `error_detail=str(e)`. Valid lines
  in the same file continue loading and the file is recorded in `ingested_files`.

  *Cases:* [`spec/01_nested_glob_gzip_and_parse_rejections`](#case-spec-01-nested-glob-gzip-and-parse-rejections)

| Column | PyArrow `DataType` | `Field.nullable` | Description |
| :--- | :--- | :--- | :--- |
| `table_name` | `pa.string()` | `True` | Target entity table name (`None` for level-mode file pre-processor errors) |
| `source_file` | `pa.string()` | `True` | Input file path relative to `input_dir` |
| `line_no` | `pa.int64()` | `True` | 1-based physical line number in `source_file` (`None` for file-level errors) |
| `raw_record` | `pa.string()` | `True` | Stripped raw input line (`None` for file-level errors) |
| `error_detail` | `pa.string()` | `True` | Parser error message, JSON-encoded list of schema errors, or pre-processor error |

### 1.3 Table Transforms, PyArrow Output Schemas, and Reporting

<a id="r-jsonl-extract-transforms"></a>

- **R-JSONL-EXTRACT-TRANSFORMS**: Each parsed JSON object is routed through `route_and_coerce_record` with `default_table=settings.table_name`,
  `transforms=settings.table_transforms` (`transforms_module`), and `output_schemas=settings.output_schemas`
  (`output_schemas_module`). Transforms may drop records (`{}` / `None`), enrich fields, or fan out records
  across multiple destination tables, and any output table with a registered `pa.Schema` in `OUTPUT_SCHEMAS`
  is buffered in `DictBuffer` (`batch_size=settings.batch_size`) and emitted as a typed `pa.Table`.

  *Cases:* [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

<a id="r-jsonl-extract-report"></a>

- **R-JSONL-EXTRACT-REPORT**: `run_jsonlines_pipeline` loads all emitted table resources and `_rejected` batches into
  `settings.dataset_name` in a single `pipeline.run(resources)` call and writes `report.txt` summarizing
  `status` (`completed` or `completed with errors`), `ingested_files`, `load_errors`, and `_load_info`.

  *Cases:* [`spec/01_nested_glob_gzip_and_parse_rejections`](#case-spec-01-nested-glob-gzip-and-parse-rejections), [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

---

## 2. Case Coverage Matrix

| Suite | Target CLI | Settings Class | Case | Title | Rules | Expected Table Counts |
| --- | --- | --- | --- | --- | --- | --- |
| `spec` | `jsonlines_extract` | `JsonlExtractSettings` | [`01_nested_glob_gzip_and_parse_rejections`](#case-spec-01-nested-glob-gzip-and-parse-rejections) | Nested Subdirectory Globbing, Gzip Decompression, Blank Lines, and JSON Parse Rejections | [`R-JSONL-EXTRACT-DISCOVERY`](#r-jsonl-extract-discovery), [`R-JSONL-EXTRACT-STREAM`](#r-jsonl-extract-stream), [`R-JSONL-EXTRACT-REJECT`](#r-jsonl-extract-reject), [`R-JSONL-EXTRACT-REPORT`](#r-jsonl-extract-report) | `default`: `{'events': 3, '_rejected': 1}` |
| `spec` | `jsonlines_extract` | `JsonlExtractSettings` | [`02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing) | MD5 Sidecar Pre-processing and File-Level `_rejected` Routing | [`R-JSONL-EXTRACT-PREPROCESS-MD5`](#r-jsonl-extract-preprocess-md5), [`R-JSONL-EXTRACT-REPORT`](#r-jsonl-extract-report) | `partial_md5_failure`: `{'events': 1, '_rejected': 1}` |
| `spec` | `jsonlines_extract` | `JsonlExtractSettings` | [`03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas) | Table Transforms, Record Filtering, Multi-Table Fan-Out, and PyArrow Output Schemas | [`R-JSONL-EXTRACT-STREAM`](#r-jsonl-extract-stream), [`R-JSONL-EXTRACT-TRANSFORMS`](#r-jsonl-extract-transforms), [`R-JSONL-EXTRACT-REPORT`](#r-jsonl-extract-report) | `default`: `{'events': 2, 'event_tags': 3}` |

---

## 3. Base Pipeline Specification Cases (`jsonlines_extract` / `spec/`)

**Shared Fixture Module (`specs/pipelines/jsonlines_extract/transforms.py`)**

```python
"""File pre-processors, table transforms, and PyArrow output schemas for JSONLines Extract specification cases."""

from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

HIGH_LATENCY_MS: Final[int] = 100

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

EVENTS_SUMMARY_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("service", pa.string(), nullable=False),
        pa.field("latency_ms", pa.int64(), nullable=False),
        pa.field("is_slow", pa.bool_(), nullable=False),
    ]
)

EVENT_TAGS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("tag", pa.string(), nullable=False),
    ]
)


def transform_event(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Drop ignored events, normalize service names, compute ``is_slow``, and fan out tags to ``event_tags``.

    :param row: Raw parsed JSONL event dictionary.
    :type row: dict[str, Any]
    :return: Mapping of target table names to lists of output row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if row.get("drop") is True:
        return {}
    event_id = str(row["event_id"]).upper()
    service = str(row["service"]).lower()
    latency_ms = int(row["latency_ms"])
    routed: dict[str, list[dict[str, Any]]] = {
        "events": [
            {
                "event_id": event_id,
                "service": service,
                "latency_ms": latency_ms,
                "is_slow": latency_ms >= HIGH_LATENCY_MS,
            }
        ]
    }
    tags = row.get("tags") or []
    if tags:
        routed["event_tags"] = [{"event_id": event_id, "tag": str(tag)} for tag in tags]
    return routed


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "events": transform_event,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "events": EVENTS_SUMMARY_SCHEMA,
    "event_tags": EVENT_TAGS_SCHEMA,
}
```

<a id="case-spec-01-nested-glob-gzip-and-parse-rejections"></a>

### Nested Subdirectory Globbing, Gzip Decompression, Blank Lines, and JSON Parse Rejections (`spec/01_nested_glob_gzip_and_parse_rejections`)

**Suite:** `spec` · **Target CLI:** `jsonlines_extract` · **Settings Class:** `JsonlExtractSettings`  
**Directory:** `specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/`  
**Rules:** [`R-JSONL-EXTRACT-DISCOVERY`](#r-jsonl-extract-discovery), [`R-JSONL-EXTRACT-STREAM`](#r-jsonl-extract-stream), [`R-JSONL-EXTRACT-REJECT`](#r-jsonl-extract-reject), [`R-JSONL-EXTRACT-REPORT`](#r-jsonl-extract-report)

- `file_glob = "**/*.jsonl*"` discovers `batch_a/part1.jsonl` and `batch_b/part2.jsonl.gz` across nested subdirectories while ignoring `batch_b/ignored.txt`.
- `batch_a/part1.jsonl` contains a blank line at line 2 (skipped without error) and a malformed JSON object at line 3 (routed to `_rejected` with `line_no=3` and the verbatim `raw_record`), while valid records on lines 1 and 4 load into `events`.

**Settings (`specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/settings.toml`)**

```toml
dataset_name = "jsonlines_extract_spec"
table_name = "events"
file_glob = "**/*.jsonl*"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `batch_a/part1.jsonl` (`specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/input/batch_a/part1.jsonl`)**

```json
{"event_id": "evt-1", "kind": "login", "latency_ms": 12}

{"event_id": "evt-bad", "kind": }
{"event_id": "evt-2", "kind": "query", "latency_ms": 45}
```

**Input `batch_b/ignored.txt` (`specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/input/batch_b/ignored.txt`)**

```text
Not a JSONL file; ignored by file_glob.
```

**Input `batch_b/part2.jsonl.gz` (`specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/input/batch_b/part2.jsonl.gz`) (decompressed preview)**

```json
{"event_id": "evt-3", "kind": "logout", "latency_ms": 8}
```

**Expected `report.txt` Summary & Row Counts (`{'events': 3, '_rejected': 1}`)**

```yaml
status: completed
ingested_files:
- batch_a/part1.jsonl
- batch_b/part2.jsonl.gz
load_errors: []
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for jsonlines_extract 01_nested_glob_gzip_and_parse_rejections."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("latency_ms", pa.int64(), nullable=False),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "events": pa.Table.from_pylist(
        [
            {"event_id": "evt-1", "kind": "login", "latency_ms": 12},
            {"event_id": "evt-2", "kind": "query", "latency_ms": 45},
            {"event_id": "evt-3", "kind": "logout", "latency_ms": 8},
        ],
        schema=EVENTS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "events",
                "source_file": "batch_a/part1.jsonl",
                "line_no": 3,
                "raw_record": '{"event_id": "evt-bad", "kind": }',
                "error_detail": "Expecting value: line 1 column 33 (char 32)",
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/expected.json`)**

```json
{
  "_rejected": [
    {
      "error_detail": "Expecting value: line 1 column 33 (char 32)",
      "line_no": 3,
      "raw_record": "{\"event_id\": \"evt-bad\", \"kind\": }",
      "source_file": "batch_a/part1.jsonl",
      "table_name": "events"
    }
  ],
  "events": [
    {
      "event_id": "evt-1",
      "kind": "login",
      "latency_ms": 12
    },
    {
      "event_id": "evt-2",
      "kind": "query",
      "latency_ms": 45
    },
    {
      "event_id": "evt-3",
      "kind": "logout",
      "latency_ms": 8
    }
  ]
}
```

---

<a id="case-spec-02-md5-sidecar-preprocessing"></a>

### MD5 Sidecar Pre-processing and File-Level `_rejected` Routing (`spec/02_md5_sidecar_preprocessing`)

**Suite:** `spec` · **Target CLI:** `jsonlines_extract` · **Settings Class:** `JsonlExtractSettings`  
**Directory:** `specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/`  
**Rules:** [`R-JSONL-EXTRACT-PREPROCESS-MD5`](#r-jsonl-extract-preprocess-md5), [`R-JSONL-EXTRACT-REPORT`](#r-jsonl-extract-report)

- `preprocessors_module = "specs.pipelines.jsonlines_extract.transforms"` registers `validate_md5_sidecar`.
- `good.jsonl` matches `good.jsonl.md5` and loads into `events`.
- `bad.jsonl` fails MD5 validation against `bad.jsonl.md5`; the file is skipped before line streaming, omitted from `ingested_files`, recorded in `report.txt` `load_errors` (`status: completed with errors`), and emitted as a file-level row (`line_no=None`, `raw_record=None`) in `_rejected`.

**Settings (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/settings.toml`)**

```toml
dataset_name = "jsonlines_extract_spec"
table_name = "events"
file_glob = "*.jsonl"
preprocessors_module = "specs.pipelines.jsonlines_extract.transforms"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `bad.jsonl` (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/input/bad.jsonl`)**

```json
{"event_id": "evt-99", "kind": "corrupt", "latency_ms": 500}
```

**Input `bad.jsonl.md5` (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/input/bad.jsonl.md5`)**

```text
MD5(bad.jsonl)= 00000000000000000000000000000000
```

**Input `good.jsonl` (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/input/good.jsonl`)**

```json
{"event_id": "evt-10", "kind": "ingest", "latency_ms": 21}
```

**Input `good.jsonl.md5` (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/input/good.jsonl.md5`)**

```text
MD5(good.jsonl)= f5781995631a7e67c13cc02f80053235
```

**Expected `report.txt` Summary & Row Counts (`{'events': 1, '_rejected': 1}`)**

```yaml
status: completed with errors
ingested_files:
- good.jsonl
load_errors:
- 'File pre-processing failed for bad.jsonl: MD5 mismatch for bad.jsonl'
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for jsonlines_extract 02_md5_sidecar_preprocessing."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("latency_ms", pa.int64(), nullable=False),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "events": pa.Table.from_pylist(
        [
            {"event_id": "evt-10", "kind": "ingest", "latency_ms": 21},
        ],
        schema=EVENTS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "events",
                "source_file": "bad.jsonl",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for bad.jsonl: "
                    "expected 00000000000000000000000000000000, got f23fdd271c6003eac5a5e788c4e86b84"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/expected.json`)**

```json
{
  "_rejected": [
    {
      "error_detail": "MD5 mismatch for bad.jsonl: expected 00000000000000000000000000000000, got f23fdd271c6003eac5a5e788c4e86b84",
      "source_file": "bad.jsonl",
      "table_name": "events"
    }
  ],
  "events": [
    {
      "event_id": "evt-10",
      "kind": "ingest",
      "latency_ms": 21
    }
  ]
}
```

---

<a id="case-spec-03-table-transforms-and-output-schemas"></a>

### Table Transforms, Record Filtering, Multi-Table Fan-Out, and PyArrow Output Schemas (`spec/03_table_transforms_and_output_schemas`)

**Suite:** `spec` · **Target CLI:** `jsonlines_extract` · **Settings Class:** `JsonlExtractSettings`  
**Directory:** `specs/pipelines/jsonlines_extract/spec/03_table_transforms_and_output_schemas/`  
**Rules:** [`R-JSONL-EXTRACT-STREAM`](#r-jsonl-extract-stream), [`R-JSONL-EXTRACT-TRANSFORMS`](#r-jsonl-extract-transforms), [`R-JSONL-EXTRACT-REPORT`](#r-jsonl-extract-report)

- `transforms_module = "specs.pipelines.jsonlines_extract.transforms"` and `output_schemas_module = "specs.pipelines.jsonlines_extract.transforms"` register `TABLE_TRANSFORMS["events"]` and `OUTPUT_SCHEMAS` for `events` and `event_tags`.
- `evt-drop` (`drop: true`) is filtered out (`{}`), `evt-1` and `evt-2` are normalized and enriched with `is_slow = latency_ms >= 100`, and `tags` arrays fan out to `event_tags`.

**Settings (`specs/pipelines/jsonlines_extract/spec/03_table_transforms_and_output_schemas/settings.toml`)**

```toml
dataset_name = "jsonlines_extract_spec"
table_name = "events"
file_glob = "*.jsonl"
transforms_module = "specs.pipelines.jsonlines_extract.transforms"
output_schemas_module = "specs.pipelines.jsonlines_extract.transforms"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `events.jsonl` (`specs/pipelines/jsonlines_extract/spec/03_table_transforms_and_output_schemas/input/events.jsonl`)**

```json
{"event_id": "evt-1", "service": "Auth", "latency_ms": 25, "tags": ["prod", "edge"]}
{"event_id": "evt-drop", "service": "Search", "latency_ms": 5, "drop": true, "tags": ["ignored"]}
{"event_id": "evt-2", "service": "Storage", "latency_ms": 140, "tags": ["batch"]}
```

**Expected `report.txt` Summary & Row Counts (`{'events': 2, 'event_tags': 3}`)**

```yaml
status: completed
ingested_files:
- events.jsonl
load_errors: []
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/jsonlines_extract/spec/03_table_transforms_and_output_schemas/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for jsonlines_extract 03_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.jsonlines_extract.transforms import (
    EVENT_TAGS_SCHEMA,
    EVENTS_SUMMARY_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "events": pa.Table.from_pylist(
        [
            {"event_id": "EVT-1", "service": "auth", "latency_ms": 25, "is_slow": False},
            {"event_id": "EVT-2", "service": "storage", "latency_ms": 140, "is_slow": True},
        ],
        schema=EVENTS_SUMMARY_SCHEMA,
    ),
    "event_tags": pa.Table.from_pylist(
        [
            {"event_id": "EVT-1", "tag": "prod"},
            {"event_id": "EVT-1", "tag": "edge"},
            {"event_id": "EVT-2", "tag": "batch"},
        ],
        schema=EVENT_TAGS_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/jsonlines_extract/spec/03_table_transforms_and_output_schemas/expected.json`)**

```json
{
  "event_tags": [
    {
      "event_id": "EVT-1",
      "tag": "edge"
    },
    {
      "event_id": "EVT-1",
      "tag": "prod"
    },
    {
      "event_id": "EVT-2",
      "tag": "batch"
    }
  ],
  "events": [
    {
      "event_id": "EVT-1",
      "is_slow": false,
      "latency_ms": 25,
      "service": "auth"
    },
    {
      "event_id": "EVT-2",
      "is_slow": true,
      "latency_ms": 140,
      "service": "storage"
    }
  ]
}
```

---

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suites and this document are both driven by it.

| Case | Target CLI | Manifest | Runs |
| :--- | :--- | :--- | :--- |
| [`spec/01_nested_glob_gzip_and_parse_rejections`](#case-spec-01-nested-glob-gzip-and-parse-rejections) | `jsonlines_extract` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/main/specs/pipelines/jsonlines_extract/spec/01_nested_glob_gzip_and_parse_rejections/case.yaml) | default |
| [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing) | `jsonlines_extract` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/main/specs/pipelines/jsonlines_extract/spec/02_md5_sidecar_preprocessing/case.yaml) | partial_md5_failure |
| [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas) | `jsonlines_extract` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/main/specs/pipelines/jsonlines_extract/spec/03_table_transforms_and_output_schemas/case.yaml) | default |
