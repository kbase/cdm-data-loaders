# Pipeline Specifications Developer Guide

This guide explains how to author, edit, and maintain pipeline smoke test rules, error scenarios, fixture manifests, and generated specification pages under [`specs/pipelines`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines).

---

## Architecture and Directory Layout

Each pipeline family specification suite lives under [`specs/pipelines/<family>`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines) and is loaded by [`tests/pipeline_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/pipeline_spec.py):

```text
specs/pipelines/<family>/
├── rules.yaml                       # Rule catalogue (base rules + preset subclass rules)
├── spec/                            # Generic base pipeline smoke test cases
│   ├── 01_<case_name>/
│   │   ├── case.yaml                # Case manifest (title, rules, notes, runs)
│   │   ├── settings.toml            # Optional case/run Settings overrides (.toml, .yaml, or .json)
│   │   ├── input/                   # Staged input directory tree (files, subdirectories)
│   │   ├── expected_tables.py       # EXPECTED_TABLES: dict[str, pa.Table] (including _rejected if applicable)
│   │   └── expected.json            # Canonical JSON rows keyed by table name
│   └── 02_<error_or_edge_case>/
│       └── ...
└── presets/                         # Domain-specific preset subclass smoke test cases
    └── <preset_pipeline>/
        ├── 01_<case_name>/
        │   ├── case.yaml            # References both base rules (R-*) and preset rules
        │   ├── settings.toml
        │   ├── input/               # Domain fixtures (e.g., .xml.gz + .md5 sidecars)
        │   ├── expected_tables.py
        │   └── expected.json
        └── 02_<error_or_edge_case>/
            └── ...
```

The manifests in [`specs/pipelines`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines) drive both test execution and rendered documentation:

- **End-to-end smoke, unit, and documentation specification tests**: [`tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py), [`tests/cdm_data_loaders/pipelines/xmltopyarrow/test_xmltopyarrow_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/xmltopyarrow/test_xmltopyarrow_spec.py), [`tests/cdm_data_loaders/pipelines/jsonlines/test_jsonlines_jsonschema_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/jsonlines/test_jsonlines_jsonschema_spec.py), [`tests/cdm_data_loaders/pipelines/xsv/test_xsv_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/xsv/test_xsv_spec.py), and [`tests/docs/test_spec_docs.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/docs/test_spec_docs.py)
- **Rendered Markdown specification pages**:
  - [XML to PyArrow Pipeline Specification](xmltopyarrow_specification.md) (`xmltopyarrow` base pipeline and preset subclasses)
  - [XML to Dict Pipeline Specification](xmltodict_specification.md) (`xmltodict` pipeline)
  - [JSONLines Extract Pipeline Specification](jsonlines_extract_specification.md) (`jsonlines_extract` pipeline)
  - [JSONLines (JSON Schema) Pipeline Specification](jsonlines_jsonschema_specification.md) (`jsonlines_jsonschema` base pipeline and `ncbi_datasets` preset subclass)
  - [JSONLines (Pydantic) Pipeline Specification](jsonlines_pydantic_specification.md) (`jsonlines_pydantic` pipeline)
  - [XSV (CSV/TSV) Schema and Pipeline Specification](xsv_specification.md) (`xsv` base pipeline and `uniprot_idmapping` preset subclass)

---

## How Pipeline Rules Are Created and Structured

Rules in `specs/pipelines/<pipeline>/rules.yaml` define the end-to-end behavioral contract of a pipeline across three stages:

- **File Discovery and Pre-processing** (for example, `P1-DISCOVERY`, `P1-MD5-SIDECAR`, `N1-ENTITY-DIRS`):
  - Which files `input_dir` and `file_glob` match.
  - How subdirectories map to entity tables.
  - What file pre-processors (such as `validate_md5_sidecar` in [`src/cdm_data_loaders/core/preprocessors.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/preprocessors.py)) verify before reading a file.
- **Parsing, Validation, Transforms, and Coercion** (for example, `P2-LEVEL-ROUTING`, `N2-JSONSCHEMA-VALIDATE`, `N3-FILTER-ANNOTATION`):
  - How records are parsed and validated against XSD, JSON Schema, Pydantic, or `qsv`.
  - How `TABLE_TRANSFORMS` filter, mutate, or split records across tables.
  - How records are coerced to `OUTPUT_SCHEMAS` or precomputed PyArrow schemas.
- **Error Routing and Pipeline Reporting** (for example, `P3-PREPROCESS-ERROR`, `P3-REPORT`, `N4-REJECTED`, `N4-REPORT`):
  - How file pre-processing failures, syntax errors, and schema validation errors populate `_rejected`.
  - What `report.txt` and `_load_info` record for `status`, `ingested_files`, `load_errors`, and `pipeline_load_info_errs`.

### Rule Catalogue Format (`rules.yaml`)

```yaml
title: XML-to-PyArrow Pipeline Specification
intro: |
  End-to-end specification for the `xmltopyarrow` pipeline family.

sections:
  - id: discovery_and_preprocessing
    title: File Discovery and MD5 Pre-processing
    rules:
      - id: P1-DISCOVERY
        name: Gzip XML discovery
        rule: |
          The pipeline scans `input_dir` for files matching `file_glob` (default `*.xml.gz`).
```

### Bidirectional Rule Invariants

[`tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py) enforces three manifest invariants:

- **Every rule is exercised** (`test_manifests_pass_every_rule_is_exercised`): every rule ID in `rules.yaml` must appear in the `rules:` list of at least one `case.yaml` across `spec/` or `presets/` for that pipeline family.
- **Every referenced rule exists** (`test_manifests_pass_every_case_rule_exists`): every rule ID listed in any `case.yaml` must exist in that family's `rules.yaml`.
- **Preset subclasses inherit base Settings and runner** (`test_manifests_pass_preset_subclasses_inherit_base_settings_and_runner`): every preset pipeline in `presets/<preset>/` must use a `Settings` class that subclasses the family's base `Settings` class and delegates to the same `pipeline_fn` callable.

---

## How Pipeline Errors Are Modeled and Tested

Unlike schema converters (which raise exceptions at schema-translation time), ingestion pipelines running on the KBase CDM Task Service (CTS) exit with code `0` so the container service preserves output tables, `_rejected` rows, `_load_info`, and `report.txt` (see [Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md)).

Pipeline error scenarios are modeled as `case.yaml` runs that verify both `_rejected` rows and `expected_report`:

### `_rejected` Table Rows

Failed files and failed records are routed to the `_rejected` table (`REJECTED_PYARROW_SCHEMA` in [`src/cdm_data_loaders/utils/buffer.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/buffer.py)):

| Column | File Pre-processor Failure | Record Parse or Validation Failure |
| :--- | :--- | :--- |
| `table_name` | Target table name or `None` (multi-table XML level mode) | Entity table name (for example, `"annotation_report"`) |
| `source_file` | Relative path of the rejected file | Relative path of the input file |
| `line_no` | `None` | 1-based line number in the input file |
| `raw_record` | `None` | Raw unparsed text of the rejected line |
| `error_detail` | Pre-processor error message (for example, `"MD5 mismatch for ..."`) | Decode error string or JSON list of schema validation errors |

### `expected_report` in `case.yaml`

Each run in `case.yaml` declares an `expected_report` block checked by `assert_smoke_run_output()` in [`tests/cdm_data_loaders/pipelines/helpers.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/helpers.py):

```yaml
runs:
  - id: mismatched_md5
    input_dir: input
    settings_file: settings.toml
    expected_tables: expected_tables.py
    expected_json: expected.json
    expected_counts:
      _rejected: 1
    expected_report:
      status: completed with errors
      ingested_files: []
      load_errors:
        - "File pre-processing failed for corrupt_sample.xml.gz: MD5 mismatch for corrupt_sample.xml.gz"
      pipeline_load_info_errs: null
```

- **File pre-processing errors** (missing, empty, malformed, or mismatched `.md5` sidecar):
  - Skip reading the failed file and omit it from `ingested_files`.
  - Append `"File pre-processing failed for <file>: <reason>"` to `report.txt` `load_errors`.
  - Set `report.txt` `status` to `"completed with errors"`.
  - Write one provenance row per failed file to `_rejected`.
- **Record parse and schema validation errors** (malformed JSON lines or JSON Schema violations):
  - Keep the file in `ingested_files` because the file itself passed pre-processing and was read.
  - Route valid records in the same file to their destination tables and invalid lines to `_rejected`.
  - Keep `report.txt` `status: completed` when no file pre-processor or `dlt` load job failed.

---

## Common Developer Workflows

Pipeline specification pages render dynamically at build and serve time from [`specs/pipelines`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines) via Zensical's `macros` plugin ([`docs_macros/spec_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/spec_context.py) and [`docs/_templates/pages/pipeline_spec.md.j2`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates/pages/pipeline_spec.md.j2)). Autogenerated stub entrypoints (`docs/*_specification.md`) are `.gitignore`d and materialized before `zensical build` or `zensical serve` by [`docs_macros/__main__.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/__main__.py) (`uv run python -m docs_macros`) and removed with `uv run python -m docs_macros --clean`.

### Adding a New Rule and Smoke Test Case

- **Add or select rule IDs in `specs/pipelines/<pipeline>/rules.yaml`**:
  - Add a new rule entry (`id`, `name`, `rule`) under the appropriate section, or reuse existing rule IDs.
- **Create `specs/pipelines/<pipeline>/spec/<NN_case_name>/` (or `presets/<preset>/<NN_case_name>/`)**:
  - **`input/`**: Place small, deterministic input files under `input/`. When creating `.gz` files or `.md5` sidecars for `pubmed`, compress with `gzip.compress(data, mtime=0)` so the gzip header timestamp is deterministic (`0`) and the MD5 digest never drifts.
  - **`settings.toml`**: Add case-level `Settings` overrides (for example, `loader_file_format = "parquet"` and `buffer_size = 10`).
  - **`expected_tables.py`**: Define `EXPECTED_TABLES: dict[str, pa.Table]` using the pipeline's schemas (and `REJECTED_PYARROW_SCHEMA` when `_rejected` has rows).
  - **`expected.json`**: Provide the canonical JSON representation of each expected table's rows (with `null` fields omitted).
  - **`case.yaml`**: Reference the rule IDs in `rules:` and define one or more `runs:` with `expected_counts` and `expected_report`.
- **Verify fixture invariants, end-to-end execution, and documentation templates**:
  ```sh
  uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py
  uv run pytest -m docs
  uv run python -m docs_macros
  uv run --group docs zensical build --clean --strict
  uv run python -m docs_macros --clean
  ```

### Editing Existing Rules, Cases, or Error Expectations

- **Updating a rule description**:
  - Edit `specs/pipelines/<pipeline>/rules.yaml`; `docs/<pipeline>_specification.md` picks up the updated rule automatically at render time.
- **Updating input fixtures or expected outputs**:
  - Update `input/`, `expected_tables.py`, `expected.json`, and `case.yaml` (`expected_counts` and `expected_report`) together.
  - `test_manifests_pass_expected_tables_validate` checks that every `pa.Table` in `expected_tables.py` passes `table.validate(full=True)`, matches `expected_counts`, and normalizes to `expected.json`.
  - Verify template rendering with `uv run pytest -m docs`, `uv run python -m docs_macros`, `uv run --group docs zensical build --clean --strict`, and `uv run python -m docs_macros --clean`.

### Onboarding a New Pipeline to `specs/pipelines/`

- **Register the pipeline in `PIPELINE_TARGETS`**:
  - Check `PIPELINE_TARGETS` in [`tests/cdm_data_loaders/pipelines/framework.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/pipelines/framework.py) so `resolve_pipeline("<pipeline>")` resolves its CLI command, module path, `Settings` class, and runner function.
- **Create `specs/pipelines/<pipeline>/`**:
  - Add `rules.yaml` and at least one `spec/<case>/` directory with `case.yaml`, `settings.toml`, `input/`, `expected_tables.py`, and `expected.json`.
- **Register the pipeline in [`docs_macros/spec_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/spec_context.py)**:
  - Add a `PipelineSpecConfig` entry to `PIPELINE_SPECS` pointing to `specs/pipelines/<pipeline>` and `docs/<pipeline>_specification.md` (and register any new `generated:` table template in `TABLE_TEMPLATE_REGISTRY` inside [`docs_macros/tables_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/tables_context.py)).
- **Index the specification page**:
  - Add `<pipeline>_specification.md` under `"Specifications"` in [`zensical.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/zensical.toml) (`[project].nav`) and link it under `## Specifications` in [`docs/index.md`](index.md) (`docs/<pipeline>_specification.md` itself is `.gitignore`d and generated on demand from [`docs/_templates/pages/macro_stub.md.j2`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates/pages/macro_stub.md.j2)).
- **Run verification**:
  ```sh
  uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py
  uv run pytest -m docs
  uv run python -m docs_macros
  uv run --group docs zensical build --clean --strict
  uv run python -m docs_macros --clean
  ```
