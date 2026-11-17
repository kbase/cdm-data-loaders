# Pipeline E2E and Smoke Testing Across Environments Guide

End-to-end (E2E) and smoke tests execute specification-driven pipeline cases across multiple execution modes and storage backends. For general `pytest` markers, coverage options, and starting the local Ceph RGW container, see the [Development and Testing Guide](development_and_testing.md).

- **Specification-driven smoke suite**: [`tests/e2e/test_pipeline_smoke_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/e2e/test_pipeline_smoke_spec.py)
- **Unified test runner**: [`tests/e2e/framework.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/e2e/framework.py) (`PipelineTestRunner`)
- **Environment specification and staging**: [`tests/helpers/pipeline_env.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/helpers/pipeline_env.py) (`PipelineEnvSpec`, `load_pipeline_env_from_dir`, `stage_input_directory`)
- **`dlt` configuration isolation**: [`tests/helpers/dlt_isolation.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/helpers/dlt_isolation.py) (`isolated_dlt_config`, `load_dlt_toml_files`)

---

## Execution Modes and Storage Backends

`PipelineTestRunner` ([`tests/e2e/framework.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/e2e/framework.py)) executes each pipeline scenario in one of three modes:

| Execution Mode | Description                                                                                                                                                                        | Entry Point                                                                   |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `core`         | In-process execution calling `cdm_data_loaders.core.cli.run_cli`                                                                                                                   | Direct Python call                                                            |
| `cli`          | CLI entrypoint invocation with patched `sys.argv` and reconstructed `LoadInfo`                                                                                                     | Registered `[project.scripts]` CLI (for example, `pubmed` or `ncbi_datasets`) |
| `docker`       | Container execution via [`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh) (`@pytest.mark.docker`, enabled by `--run-docker`) | `docker run --rm ... <image> pipeline <cli_command> ...`                      |

Built-in and custom storage environments in [`tests/e2e/test_pipeline_smoke_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/e2e/test_pipeline_smoke_spec.py):

| Environment ID | Input Storage                                                 | Output and Destination Storage                                             | Markers and Requirements                                       |
| -------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `local_fs`     | Local temporary directory                                     | Local filesystem destination                                               | Always enabled                                                 |
| `s3_moto`      | In-memory Moto S3 bucket (`s3://input-bucket/<run_slug>/...`) | In-memory Moto S3 bucket (`s3://cdm-lake/<run_slug>` and local output dir) | Always enabled                                                 |
| `s3_ceph`      | Local Ceph RGW S3 bucket                                      | Local Ceph RGW S3 bucket                                                   | Requires `-m requires_ceph` and a running Ceph container       |
| `custom`       | Configured via `--dlt-config-dir` and `--settings-file`       | Configured via `--dlt-config-dir` and `--settings-file`                    | Enabled when `--dlt-config-dir` or `--settings-file` is passed |

---

## Running Built-In Environments

### Run All Local and Moto Smoke Scenarios (`local_fs` and `s3_moto`)

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py -m "not requires_ceph"
```

### Filter to a Single Pipeline (`--smoke-pipeline`)

Pass `--smoke-pipeline` to restrict execution to one pipeline (such as `pubmed`, `ncbi_datasets`, `xsv`, or `xmltopyarrow`):

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py \
  --smoke-pipeline pubmed \
  -m "not requires_ceph"
```

### Filter by Environment or Execution Mode (`-k`)

Run only `local_fs` in `core` mode:

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py -k "local_fs and core"
```

Run only `s3_moto` in `cli` mode:

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py -k "s3_moto and cli"
```

### Run Against Local Ceph (`s3_ceph`)

When a local Ceph RGW container is running (see [Development and Testing Guide](development_and_testing.md)), select the `requires_ceph` marker:

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py -m requires_ceph
```

---

## Running Against a Custom Environment

When `--dlt-config-dir` or `--settings-file` is provided on the `pytest` command line, `load_pipeline_env_from_dir()` in [`tests/helpers/pipeline_env.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/helpers/pipeline_env.py) builds a `PipelineEnvSpec` and switches parametrization from the built-in fixtures to that custom environment.

### Create a `dlt` Configuration Directory (`--dlt-config-dir`)

Create a directory (for example, `/tmp/my-env/dlt_conf`) containing `config.toml` and `secrets.toml`:

```toml
# /tmp/my-env/dlt_conf/config.toml
[destination.filesystem]
bucket_url = "s3://my-staging-lake/smoke-tests"
```

```toml
# /tmp/my-env/dlt_conf/secrets.toml
[destination.filesystem.credentials]
endpoint_url = "https://s3.staging.example.org"
aws_access_key_id = "YOUR_ACCESS_KEY"
aws_secret_access_key = "YOUR_SECRET_KEY"
```

Any destination supported by `dlt` (for example, `filesystem`, `duckdb`, `snowflake`, or `postgres`) can be configured inside `config.toml` and `secrets.toml`.

### Create a Settings File (`--settings-file`)

Create a TOML, YAML, or JSON settings file (for example, `/tmp/my-env/settings.toml`) with top-level shared settings or per-pipeline sections under `[pipelines.<pipeline_name>]`:

```toml
# /tmp/my-env/settings.toml
use_destination = "filesystem"
output_dir = "/tmp/my-env/output"
loader_file_format = "parquet"

[pipelines.pubmed]
buffer_size = 500

[pipelines.ncbi_datasets]
buffer_size = 100
```

Settings merge order from lowest to highest precedence:

- Scenario settings defined in [`specs/pipelines`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines) (`settings.toml` and `case.yaml`).
- Top-level keys in `--settings-file` (filtered to valid fields of the target pipeline's `Settings` class).
- Pipeline-specific keys under `[pipelines.<pipeline_name>]` inside `--settings-file`.
- Per-run isolated `input_dir` and `output_dir` paths generated by the test runner.

### Execute the Smoke Test Suite

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py \
  --dlt-config-dir /tmp/my-env/dlt_conf \
  --settings-file /tmp/my-env/settings.toml \
  --smoke-pipeline ncbi_datasets
```

---

## Running with Apache Iceberg (`loader_file_format = "iceberg"` and `.pyiceberg.yaml`)

When testing Iceberg output against a catalog (such as a Polaris REST catalog or SQL catalog) configured via `.pyiceberg.yaml`:

- **Place `.pyiceberg.yaml` in `$HOME` (`~/.pyiceberg.yaml`)**:
  - Standard PyIceberg configuration lives at `$HOME/.pyiceberg.yaml`; `load_pipeline_env_from_dir()` ignores any `.pyiceberg.yaml` placed inside `--dlt-config-dir`.
  - In Docker mode (`--run-docker`), `PipelineTestRunner` copies `$HOME/.pyiceberg.yaml` from the host into `/input_dir/dlt_conf/.pyiceberg.yaml`, and [`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh) copies it to `$HOME/.pyiceberg.yaml` inside the container.
- **Manage Table Lifecycle Across Runs in a Persistent Catalog**:
  - Preset pipelines such as `ncbi_datasets` and `pubmed` use a fixed `dataset_name`, so consecutive test cases targeting a persistent catalog append to existing catalog tables unless tables are dropped before or after each run.
  - Use either `pytest` CLI flags or `settings.toml` keys (at the top level or under `[pipelines.<pipeline_name>]`) to control catalog cleanup:

| CLI Flag                 | `settings.toml` Key           | Behavior                                                                                                               |
| ------------------------ | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `--drop-existing-tables` | `drop_existing_tables = true` | Drops existing tables in the target pipeline's dataset namespace from the Iceberg catalog before each test run starts. |
| `--drop-tables-after`    | `drop_tables_after = true`    | Drops tables added to the Iceberg catalog during the test run when the test finishes (`finally:` block).               |

Example `settings.toml` for running `ncbi_datasets` with Iceberg and a Polaris REST catalog configured in `~/.pyiceberg.yaml`:

```toml
# /tmp/my-env/settings.toml
use_destination = "s3"
loader_file_format = "iceberg"
drop_existing_tables = true
drop_tables_after = true
```

Example `pytest` invocation:

```sh
uv run pytest tests/e2e/test_pipeline_smoke_spec.py \
  --dlt-config-dir /tmp/my-env/dlt_conf \
  --settings-file /tmp/my-env/settings.toml \
  --smoke-pipeline ncbi_datasets \
  --drop-existing-tables \
  --drop-tables-after
```

---

## Running in Docker Mode (`--run-docker`)

Enable the `docker` execution mode by passing `--run-docker` (and `--docker-image` when using a pre-built local tag; if omitted, `PipelineTestRunner` builds `ghcr.io/kbase/cdm-data-loaders:test_container` on demand):

```sh
# build a local image tag if desired
docker build -t cdm-data-loaders:latest .

# run smoke tests in Docker mode
uv run pytest tests/e2e/test_pipeline_smoke_spec.py \
  --dlt-config-dir /tmp/my-env/dlt_conf \
  --settings-file /tmp/my-env/settings.toml \
  --run-docker \
  --docker-image cdm-data-loaders:latest \
  -k "docker"
```

How `PipelineTestRunner` ([`tests/e2e/framework.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/e2e/framework.py)) stages files and mounts directories in `docker` mode:

- **Input directory and `dlt` configuration**: Local `input_dir` contents (along with `dlt_conf/config.toml`, `dlt_conf/secrets.toml`, and `$HOME/.pyiceberg.yaml` when present) are staged on the host and bind-mounted at `/input_dir`, while `output_dir` is bind-mounted at `/output_dir`.
- **Remote URLs (`s3://...`)**: When `--input-dir` is an `s3://` URL, `PipelineTestRunner` passes the remote URL to the CLI command inside the container while mounting `/input_dir` so [`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh) copies `/input_dir/dlt_conf` into `$HOME/.dlt`.
- **Auxiliary schema files and Python modules**: Local files passed via `--xsd-file`, `--schema-file`, `-s`, or `--log-config-file` are staged read-only at `/aux_dir`, and generated schema/model modules (`--entity-models`, `--entity-schemas`, `-m`) are materialized read-only at `/pymodules` on `PYTHONPATH`.

---

## Per-Run Isolation on Shared Buckets

When running against a shared S3 bucket or shared local directory, [`tests/e2e/test_pipeline_smoke_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/e2e/test_pipeline_smoke_spec.py) isolates each test invocation using a per-run `run_slug`:

```text
<pipeline>_<run_id>_<mode>_<8-char-uuid>
```

- **Input files**: Staged under `<input_base>/<run_slug>/` (both local directories and `s3://` prefixes via `stage_input_directory()`).
- **Output reports and logs**: Written under `<output_base>/<run_slug>/`.
- **dlt destination**: Written under `<bucket_url>/<run_slug>` with `preserve_dlt_destination=True` for `parquet`/`jsonl` runs; for `loader_file_format = "iceberg"`, `<bucket_url>` is preserved unchanged so table locations remain under the catalog namespace's allowed warehouse path (use `--drop-existing-tables` / `--drop-tables-after` for per-run isolation).

This prevents table collisions, schema pollution, or report overwrites when multiple scenarios or pipelines target the same bucket.

---

## What Every Run Verifies

Regardless of execution mode (`core`, `cli`, or `docker`) or storage backend (`local_fs`, `s3_moto`, `s3_ceph`, or `custom`), `assert_smoke_run_output()` in [`tests/helpers/table_readers.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/helpers/table_readers.py) verifies:

- **Exit Code Contract**: Even when a run records rejected files or failed records (`status: "completed with errors"`), the CLI and Docker process exit with code `0` so the KBase CDM Task Service (CTS) preserves output artifacts and logs.
- **Summary Report (`report.txt`)**: Parses `<output_dir>/report.txt` and matches `pipeline`, `dataset`, `status`, `ingested_files`, `load_errors`, `pipeline_load_info_errs`, `load_ids`, and `started` / `finished` timestamps against `expected_report` in `case.yaml`.
- **Audit Table (`_load_info`)**: Reads `_load_info` from the `dlt` destination and asserts that the run audit row is present.
- **Destination Tables (`expected_tables.py` and `expected.json`)**: Reads every output table from the `dlt` destination, strips volatile `_dlt_*` metadata columns and unmaterialized `null` fields, checks `expected_counts`, and asserts canonical row equality against both `expected_tables.py` and `expected.json`.
