# Development and Testing Guide

This guide covers local development setup, linting and code generation checks, Spark and Ceph container test environments, `pytest` markers and coverage, and building the documentation site.

- **Project dependencies, lint rules, and `pytest` markers**: [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml)
- **Container entrypoint (`test` and `ceph_integration_test` modes)**: [`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh)
- **Spark container test runner**: [`scripts/dev/run_tests.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/dev/run_tests.sh)
- **Ceph RGW integration Compose configuration**: [`docker-compose.yml`](https://github.com/kbase/cdm-data-loaders/blob/develop/docker-compose.yml)

---

## Development Environment, Formatting, and Linting

Install all dependency groups (`dev`, `docs`, `models`, `mutants`, and `lakehouse`) into the local `uv` virtual environment:

```sh
uv sync --all-groups
```

> [!NOTE]
> Running XSV reader and pipeline tests directly on the host requires the [`qsv`](https://github.com/dathere/qsv) binary on `PATH` (for example, `brew install qsv` on macOS or downloading a release binary from `dathere/qsv`). The repository [`Dockerfile`](https://github.com/kbase/cdm-data-loaders/blob/develop/Dockerfile) installs `qsv` automatically for containerized test runs.

Run formatting, linting, and committed PyArrow schema codegen checks ([`src/cdm_data_loaders/cli/xsd_to_pyarrow.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/cli/xsd_to_pyarrow.py)) before opening a pull request:

```sh
# format code and check lint rules
uv run ruff format
uv run ruff check

# verify committed UniProt, UniRef, and PubMed PyArrow schema.py modules match their bundled XSDs
uv run xsd_to_pyarrow --bundled --check
```

To regenerate the committed `schema.py` modules after updating a bundled XSD file, run `uv run xsd_to_pyarrow --bundled` without `--check`.

---

## Spark Development Container

Code and tests marked with `@pytest.mark.requires_spark` require Apache Spark and Java dependencies provided by the KBase Lakehouse Spark notebook image (`ghcr.io/kbasedatalakehouse/spark_notebook:main`). [`scripts/dev/run_tests.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/dev/run_tests.sh) creates an isolated `uv` virtual environment inside the container pinned to the container's `pyspark` version and executes `pytest`:

```sh
# pull the Spark notebook container image
docker pull ghcr.io/kbasedatalakehouse/spark_notebook:main

# mount the repository and execute the test suite (including requires_spark tests)
docker run -it --rm \
  -v "$(pwd)":/cdm-data-loaders \
  ghcr.io/kbasedatalakehouse/spark_notebook:main \
  /cdm-data-loaders/scripts/dev/run_tests.sh
```

To open an interactive shell or keep the container running so VS Code can attach via the [Dev Containers extension](https://code.visualstudio.com/docs/devcontainers/containers):

```sh
# launch an interactive bash shell inside the Spark container
docker run -it --rm \
  -v "$(pwd)":/cdm-data-loaders \
  ghcr.io/kbasedatalakehouse/spark_notebook:main \
  bash

# or keep the container running in the background to attach an IDE
docker run -d --rm \
  --name cdm-spark-dev \
  -v "$(pwd)":/cdm-data-loaders \
  ghcr.io/kbasedatalakehouse/spark_notebook:main \
  sleep 2d
```

---

## Running Tests and Coverage

Test markers and isolated test environment defaults (`pytest-env`) are configured under `[tool.pytest]` and `[tool.pytest_env]` in [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml), with shared fixtures and Hypothesis profiles in [`tests/conftest.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/conftest.py).

### Local `pytest` Suites

```sh
# standard local unit and integration suite (skips Spark, live Ceph, and docs drift suites)
uv run pytest -m "not requires_spark and not requires_ceph and not docs"

# run with XML coverage output
uv run pytest -m "not requires_spark and not requires_ceph and not docs" --cov=src --cov-report=xml

# run targeted converter, JSONLines, or pipeline specification suites
uv run pytest tests/cdm_data_loaders/converters/
uv run pytest tests/cdm_data_loaders/readers/test_jsonl.py tests/cdm_data_loaders/readers/test_jsonl_jsonschema.py tests/cdm_data_loaders/pipelines/jsonlines/

# run Hypothesis property-based tests (use HYPOTHESIS_PROFILE=ci_thorough for 100 examples)
uv run pytest -m pbt
HYPOTHESIS_PROFILE=ci_thorough uv run pytest -m pbt

# report tests taking 5 seconds or longer (matching CI workflow reporting)
uv run pytest -m "not requires_spark and not requires_ceph and not docs" --durations=0 --durations-min=5.0
```

For multi-environment pipeline smoke tests (`core`, `cli`, and `docker` modes across `local_fs`, `s3_moto`, `s3_ceph`, and custom `--dlt-config-dir` / Iceberg environments), see the [Pipeline E2E and Smoke Testing Across Environments Guide](pipeline_e2e_testing_guide.md).

### Global State Mutation Audit

Tests marked with `@pytest.mark.state_audit` ([`tests/harness/test_global_state_audit.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/harness/test_global_state_audit.py)) are skipped during normal `pytest` runs unless selected with `-m state_audit`. When selected, the suite runs clean `pytest` subprocesses with `--state-audit` ([`tests/helpers/state_audit.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/helpers/state_audit.py)) to verify that module collection and test execution leave zero unreverted mutations in environment variables, `sys.argv`, `sys.path`, `sys.modules`, logging state, `dlt` context, `boto3` sessions, S3 client singletons, `jsonschema` validator registries, module globals, caches, or scoped fixtures:

```sh
# run the full global state mutation audit across all local test slices
uv run pytest -m state_audit

# run a single audit slice (core-and-converters, pipelines, or e2e)
uv run pytest -m state_audit -k e2e

# audit a specific test file or directory during development
uv run pytest --state-audit tests/cdm_data_loaders/validation/test_jsonschema.py
```

### Container `test` Mode

[`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh) provides a `test` command that runs `uv run --all-groups pytest -m "not requires_spark and not requires_ceph and not docs and not state_audit" --ignore=tests/docs --cov=src --cov-report=xml` inside the built image (with `qsv` and all runtime binaries pre-installed):

```sh
docker build -t cdm-data-loaders-test:latest .

docker run --rm \
  -v "$(pwd)":/out \
  cdm-data-loaders-test:latest \
  test --cov-report=xml:/out/coverage.xml
```

Any extra arguments after `test` (such as `tests/e2e/` or `-k xmltopyarrow`) are forwarded to `pytest`.

---

## Ceph S3 and NCBI FTP Integration Tests

Tests marked with `@pytest.mark.requires_ceph` exercise real S3 transfers, bucket operations, and end-to-end NCBI FTP synchronization against a containerized Ceph RADOS Gateway (`ghcr.io/kbasetest/ceph-rgw-test-image:0.1.5`).

### Docker Compose Workflow (Recommended)

[`docker-compose.yml`](https://github.com/kbase/cdm-data-loaders/blob/develop/docker-compose.yml) starts the Ceph RGW container, waits for its OSD and HTTP listener healthcheck, and runs the `ceph_integration_test` entrypoint mode (`pytest -m "requires_ceph" -v`):

```sh
# build the image, start Ceph RGW, and run all requires_ceph integration tests
docker compose up \
  --build \
  --abort-on-container-exit \
  --exit-code-from integration-tests

# stop containers and remove temporary volumes when finished
docker compose down --volumes
```

### Running Against a Local Ceph Container from the Host

When iterating on individual `s3` or `requires_ceph` tests from your local virtual environment, run the Ceph RGW container in the background on `localhost:9000` (matching the default `AWS_ENDPOINT_URL` and credentials in `[tool.pytest_env]` in [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml)):

- **Start the local Ceph RGW container**:
  ```sh
  docker run -d --rm \
    --name ceph-local \
    -p 9000:8080 \
    -e RGW_PORT=8080 \
    -e RGW_ACCESS_KEY=test_access_key \
    -e RGW_SECRET_KEY=test_access_secret \
    ghcr.io/kbasetest/ceph-rgw-test-image:0.1.5
  ```
- **Run Moto-mocked S3 unit tests (no live Ceph required)**:
  ```sh
  uv run pytest -m "s3 and not requires_ceph"
  ```
- **Run live Ceph and NCBI FTP integration tests**:
  ```sh
  # run fast Ceph-backed tests (skipping large multi-file downloads)
  uv run pytest -m "requires_ceph and not slow_test"

  # run the full Ceph integration suite including slow_test cases
  uv run pytest -m requires_ceph
  ```
- **Stop the local Ceph RGW container**:
  ```sh
  docker stop ceph-local
  ```

---

## Building and Verifying Documentation

Documentation is built with [Zensical](https://zensical.org/) ([`zensical.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/zensical.toml)) and rendered dynamically from source code, bundled schemas, and YAML specification manifests via the `docs_macros` package ([`docs_macros/__main__.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/__main__.py)) and Jinja2 templates under [`docs/_templates`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates).

Autogenerated Markdown macro stubs (`docs/cli_reference.md`, `docs/bundled_schemas.md`, `docs/*_specification.md`) and published JSON Schema files (`docs/schemas/`) are `.gitignore`d and materialized on demand before `zensical serve` or `zensical build`.

### Previewing Documentation Locally

```sh
# install documentation dependencies
uv sync --group docs

# materialize autogenerated macro stubs and published JSON Schemas in docs/
uv run python -m docs_macros

# start the live-reloading local preview server
uv run --group docs zensical serve
```

### Running Documentation Verification Checks

Run the full verification sequence enforced by CI (and [`tests/docs`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/docs)) after editing `src/`, `docs_macros/`, `scripts/`, `specs/`, `pyproject.toml`, or `docs/`:

```sh
# run documentation template, CLI flag, Python snippet, link, and nav parity tests
uv run pytest -m docs

# materialize stubs, verify zero drift, and run a strict Zensical site build
uv run python -m docs_macros
uv run python -m docs_macros --check
uv run --group docs zensical build --clean --strict

# clean autogenerated stubs and published schemas from the working tree
uv run python -m docs_macros --clean
```
