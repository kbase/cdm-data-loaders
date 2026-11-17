# Core Pipeline and KBase CTS Execution Guide

This guide explains how `cdm-data-loaders` pipelines are configured and executed locally and inside containers on the KBase CDM Task Service (CTS), and how they report results, logs, and rejected records.

---

## Execution Modes

Every pipeline under [`src/cdm_data_loaders/pipelines`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines) exposes a `CliRunner` instance (`cli = CliRunner(...)`, defined in [`src/cdm_data_loaders/core/cli.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/cli.py)) registered under `[project.scripts]` in [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml). See the [CLI Reference](cli_reference.md) for the complete list of registered commands and options.

### Local CLI Execution (`uv run`)

Run any registered pipeline entry point directly via `uv`:

```bash
uv run xsv \
  --input-dir /path/to/input_dir \
  --output-dir /path/to/output_dir \
  --use-destination local_fs \
  --dataset-name my_dataset \
  --table-name sample_rows \
  --file-glob "*.tsv*" \
  --schema-file /path/to/schema.json
```

### Container Execution (`scripts/entrypoint.sh`)

Inside the Docker container (or when running [`./scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh)), the first positional argument selects the execution mode:

| Mode | Invocation | Description |
| :--- | :--- | :--- |
| `pipeline` | `./scripts/entrypoint.sh pipeline <cli_command> [args...]` | Copies `/input_dir/dlt_conf/{config.toml,secrets.toml,.pyiceberg.yaml}` into `$HOME/.dlt` / `$HOME/.pyiceberg.yaml` (if present) and runs `uv run <cli_command> [args...]`. |
| `xml_split` | `./scripts/entrypoint.sh xml_split [args...]` | Splits XML files into chunks via `xml_file_splitter`. |
| `bash` | `./scripts/entrypoint.sh bash` | Launches an interactive shell inside the container. |

Container test modes (`test` and `ceph_integration_test`) are covered in the [Development and Testing Guide](development_and_testing.md).

Example container invocation:

```bash
docker run --rm \
  -v /local/input:/input_dir \
  -v /local/output:/output_dir \
  ghcr.io/kbase/cdm-data-loaders:latest \
  pipeline jsonlines_extract \
  --input-dir /input_dir \
  --output-dir /output_dir \
  --use-destination local_fs \
  --dataset-name my_dataset \
  --table-name events \
  --file-glob "*.jsonl.gz"
```

### Programmatic Execution in Python

In Python, instantiate the pipeline's settings class and pass it to the pipeline runner (or call `CliRunner.run()` from [`src/cdm_data_loaders/core/cli.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/cli.py)):

```python
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import (
    JsonlExtractSettings,
    run_jsonlines_pipeline,
)

settings = JsonlExtractSettings(
    input_dir="/path/to/input_dir",
    output_dir="/path/to/output_dir",
    use_destination="local_fs",
    dataset_name="my_dataset",
    table_name="events",
    file_glob="*.jsonl*",
)
load_info = run_jsonlines_pipeline(settings)
```

---

## The KBase CTS Exit-Code-0 Contract

Pipelines in production run on the **KBase CDM Task Service (CTS)**. CTS enforces a strict output-retention rule:

> **CTS only uploads the contents of `/output_dir` (and pipeline artifacts written there) if the container exits with exit code `0`.** If a container exits with a non-zero exit code, CTS captures `STDOUT` and `STDERR` logs, **but discards everything written to `/output_dir`**.

To preserve diagnostic artifacts, `CliRunner` ([`src/cdm_data_loaders/core/cli.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/cli.py)) and `run_pipeline()` ([`src/cdm_data_loaders/pipelines/core.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/core.py)) separate failures by phase:

- **Pre-execution configuration, CLI, and destination validation errors (non-zero exit)**:
  Errors raised during CLI settings parsing (missing required flags, invalid file paths, unknown options, or invalid transform/preprocessor modules) or invalid `use_destination` lookup occur before pipeline extraction starts. Because no output artifacts exist yet, these errors raise and terminate the process with a non-zero exit code so CTS flags the task failure and records `STDERR`.
- **Runtime pipeline and `dlt` load errors (exit code `0`)**:
  Once `pipeline.run()` begins inside `run_pipeline()`, runtime step failures (`PipelineStepFailed`) and failed jobs (`load_info.has_failed_jobs`) are caught and logged. [`save_pipeline_metadata()`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/reporting.py) writes the `LoadInfo` record to `_load_info` and writes `PipelineReport` YAML to `settings.report_path` (`report.txt`) with `status: error` or `status: completed with errors`, `pipeline_err_msg`, and `load_errors`. `CliRunner.__call__()` returns so the container exits with code `0`, preserving `report.txt`, logs, `_load_info`, and `_rejected` tables in `/output_dir` for CTS to upload.

---

## CLI, Environment Variable, and CTS Wildcard Configuration

Pipeline settings models are defined in [`src/cdm_data_loaders/core/settings.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/settings.py) and [`src/cdm_data_loaders/core/fields.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/fields.py). For per-CLI flags, shortcuts, defaults, and model schemas, see the [CLI Reference](cli_reference.md) and the [Python API Reference](api/cdm_data_loaders/core/settings.md).

- **Environment Variable Prefix (`CDL_`)**: Every CLI option can be supplied via a `CDL_`-prefixed environment variable, such as `CDL_DATASET_NAME=my_dataset` or `CDL_INPUT_DIR=/data/in`.
- **Kebab-Case CLI Flags**: Field names use `snake_case` in Python and `--kebab-case` on the CLI (for example, `dataset_name` maps to `--dataset-name`).
- **Pipeline Metadata Location (`-p` / `--use-output-dir-for-pipeline-metadata`)**: Passing `-p` stores `dlt` internal state and schema metadata under `<output_dir>/pipeline_metadata/<pipeline_name>/` and writes `report.txt` (or `file_listing.txt`) under `<output_dir>/pipeline_metadata/`, keeping stateless CTS runs self-contained inside `/output_dir`.
- **Custom Pre-processors, Transforms, and Output Schemas**: File-format ingestion pipelines accept `--preprocessors-module`, `--transforms-module`, and `--output-schemas-module`; see [Table and Column Transformations](table_transforms.md) for runnable examples.
- **CTS-Safe Wildcard Tokens (`FileGlob`)**: The CTS `POST /jobs` API restricts `args` and `environment` values to `^[\w\./][\w\.\,+/-]*$` and rejects raw glob metacharacters (`*`, `?`, `[`, `]`, `!`). `FileGlob` (`--file-glob`, `-g`, `CDL_FILE_GLOB`) expands readable wildcard tokens while keeping raw globs intact for local runs:

| Token | Glob Character | Example CTS Value | Expanded Glob |
| :--- | :--- | :--- | :--- |
| `__STAR__` | `*` | `__STAR__.ext.__STAR__` | `*.ext.*` |
| `__GLOBSTAR__` | `**` | `__GLOBSTAR__/__STAR__.xml.gz` | `**/*.xml.gz` |
| `__QMARK__` | `?` | `batch___QMARK__.csv` | `batch_?.csv` |
| `__LBRACK__` | `[` | `part___LBRACK__0-9__RBRACK__.tsv` | `part_[0-9].tsv` |
| `__RBRACK__` | `]` | `part___LBRACK__0-9__RBRACK__.tsv` | `part_[0-9].tsv` |
| `__BANG__` | `!` | `data___LBRACK____BANG__a__RBRACK__.xml` | `data_[!a].xml` |

Example CTS `POST /jobs` `params.args` payload:

```json
[
  "pipeline",
  "xsv",
  "--input-dir", "/input_dir",
  "--output-dir", "/output_dir",
  "--use-destination", "cts",
  "--dataset-name", "my_dataset",
  "--table-name", "sample_rows",
  "--schema-file", "/input_dir/schema.json",
  "--file-glob", "__STAR__.ext.__STAR__"
]
```

---

## Configuring `dlt` Destinations and Iceberg Catalogs

Every CTS pipeline requires `--use-destination <name>` (or `CDL_USE_DESTINATION=<name>`), where `<name>` matches a configured `dlt` destination entry (`destination.<name>.destination_type`).

### Example `.dlt/config.toml`

```toml
[destination.local_fs]
destination_type = "filesystem"
bucket_url = "/output_dir"

[destination.local_duckdb]
destination_type = "duckdb"
credentials = "/output_dir/warehouse.duckdb"
```

> [!WARNING]
> `dlt` 1.30+ raises a DuckDB `Binder Error: Ambiguous reference to catalog or schema` if the `.duckdb` database filename stem equals `dataset_name`. Always keep the DuckDB database filename distinct from both `pipeline_name` and `dataset_name`.

### Supplying `config.toml` and `secrets.toml`

`dlt` resolves destination credentials and configuration from any of the following locations:

- **Container Input Directory (`/input_dir/dlt_conf/`)**: When running via [`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh) `pipeline ...`, if `/input_dir/dlt_conf` exists, the entrypoint copies `/input_dir/dlt_conf/{config.toml,secrets.toml}` into `$HOME/.dlt/` and `/input_dir/dlt_conf/.pyiceberg.yaml` into `$HOME/.pyiceberg.yaml` before starting the pipeline.
- **Local `.dlt/` Directory**: For local CLI or Python runs, `dlt` reads `.dlt/config.toml` and `.dlt/secrets.toml` in the working directory (or `~/.dlt/`).
- **Environment Variables**: Standard `dlt` environment variables (for example, `DESTINATION__LOCAL_FS__DESTINATION_TYPE=filesystem`, `DESTINATION__LOCAL_FS__BUCKET_URL=/output_dir`, and `RUNTIME__SLACK_INCOMING_HOOK=...`) can be set alongside `CDL_*` variables.

### Configuring Apache Iceberg Catalogs (`--loader-file-format iceberg`)

When `--loader-file-format iceberg` (`-f iceberg`) is passed with a `filesystem` destination, `dlt` writes Apache Iceberg tables (`loader_file_format="parquet"`, `table_format="iceberg"`) using PyIceberg while keeping `_load_info` as `jsonl`.

If no explicit catalog is configured, `dlt` creates a local SQLite catalog (`sql_default`) at `<bucket_url>/iceberg_catalog.db`. To configure an explicit SQLite, PostgreSQL, or REST catalog, supply one of:

- **`[iceberg_catalog]` in `.dlt/config.toml` or `.dlt/secrets.toml`**:
  ```toml
  [iceberg_catalog]
  iceberg_catalog_name = "default"
  iceberg_catalog_type = "sql"
  iceberg_catalog_config = { uri = "sqlite:////output_dir/catalog.db" }
  ```
- **`.pyiceberg.yaml`**: Place `.pyiceberg.yaml` in `$HOME/.pyiceberg.yaml` for local runs, or supply `/input_dir/dlt_conf/.pyiceberg.yaml` for container runs.
- **Environment Variables**: Set PyIceberg environment variables (`PYICEBERG_CATALOG__DEFAULT__URI=...`) or `dlt` environment variables (`ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE=...`).

---

## Output Artifacts, Rejections, and Observability

Every pipeline run produces standardized observability artifacts in `output_dir` and the target `dlt` destination:

### `report.txt` (`PipelineReport`)

At the end of `run_pipeline()`, [`write_pipeline_report()`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/reporting.py) writes a YAML-serialized `PipelineReport` to `settings.report_path` (`<output_dir>/report.txt` or `<output_dir>/pipeline_metadata/report.txt`):

```yaml
pipeline: jsonlines_extract
dataset: my_dataset
started: 2026-10-30 12:00:00+00:00
finished: 2026-10-30 12:00:05+00:00
status: completed # "completed", "completed with errors", or "error"
load_ids:
  - '1730289600.123456'
ingested_files:
  - batch_01.jsonl.gz
pipeline_load_info_errs: null
```

If `pipeline.run()` fails or reports failed jobs, `status` is set to `"error"` or `"completed with errors"` and `pipeline_err_msg` and `load_errors` are populated.

### `file_listing.txt` (`--loader-file-format file_listing`)

Passing `--loader-file-format file_listing` (`-f file_listing`) on any `filesystem`-backed pipeline discovers input files via [`collect_file_listing()`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/filesystem.py) and exits without opening files, running `FILE_PREPROCESSORS` or `TABLE_TRANSFORMS`, initializing a `dlt` destination, writing `report.txt` or `_load_info`, or sending Slack alerts. [`write_file_listing()`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/reporting.py) writes YAML to `settings.file_listing_path` (`<output_dir>/file_listing.txt`, or `<output_dir>/pipeline_metadata/file_listing.txt` when `-p` is set):

```yaml
input_dir_files:
  - batch_01.jsonl.gz
  - notes/readme.txt
  - sub/batch_02.jsonl.gz
matched_files:
  - batch_01.jsonl.gz
  - sub/batch_02.jsonl.gz
```

`input_dir_files` lists all files discovered under `input_dir` via `dlt` `filesystem(..., file_glob="**")` as sorted POSIX relative paths, and `matched_files` lists the subset matched by the pipeline's [`build_filesystem_resource()`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/filesystem.py) glob(s). Pipelines that do not read via `dlt.sources.filesystem` (such as `all_the_bacteria`) reject `file_listing` during settings validation.

### `_load_info` Destination Table

When [`save_pipeline_metadata()`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/reporting.py) runs and `load_info` is present, it writes `[load_info.asdict()]` as JSONL into the `_load_info` table (`max_table_nesting=0`) inside the target dataset so warehouse queries can audit load history inside the destination.

### Dead-Letter Rejection Table (`_rejected`)

Pipelines that validate or parse individual files and records route failures to the unified `_rejected` table (`REJECTED_PYARROW_SCHEMA` in [`src/cdm_data_loaders/core/buffer.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/buffer.py): `table_name`, `source_file`, `line_no`, `raw_record`, `error_detail`) rather than aborting the entire batch:

- **File pre-processors (`FILE_PREPROCESSORS`)**: Raising `FilePreprocessError` (such as an MD5 sidecar mismatch) writes a diagnostic row to `_rejected` with `source_file` and `error_detail` and skips the file.
- **JSONLines and XML pipelines (`jsonlines_extract`, `jsonlines_jsonschema`, `jsonlines_pydantic`, `xmltodict`, `xmltopyarrow`)**: Unparseable records or schema/model validation errors are routed to `_rejected` with `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`.
- **XSV pipelines (`xsv` and `uniprot_idmapping`)**: When `qsv` two-pass validation detects structural errors or JSON Schema violations in a file, diagnostic rows are routed to `_rejected` while valid files continue loading into the target table(s).

### Structured Logging and Slack Alerts

- **Logging ([`src/cdm_data_loaders/utils/logging.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/logging.py))**: `init_logger(log_config_file)` initializes structured JSON logging from `--log-config-file` (or `CDL_LOG_CONFIG_FILE` / the built-in fallback configuration).
- **Slack notifications**: When `runtime.slack_incoming_hook` (`RUNTIME__SLACK_INCOMING_HOOK`) is configured in `dlt`, `run_pipeline()` posts a completion or error summary alert.
