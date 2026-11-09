# Pipeline Specifications Developer Guide

This guide explains how to author, edit, and maintain pipeline smoke test rules, error scenarios, fixture manifests, and generated specification pages under `specs/pipelines/`.

---

## 1. Architecture & Directory Layout

Each pipeline specification suite lives under `specs/pipelines/<pipeline>/` and is loaded by `tests/pipeline_spec.py`:

```text
specs/pipelines/<pipeline>/
├── rules.yaml                       # Rule catalogue (sections + rule IDs and statements)
├── 01_<case_name>/
│   ├── case.yaml                    # Case manifest (title, rules, notes, runs)
│   ├── settings.toml                # Optional case/run Settings overrides (.toml, .yaml, or .json)
│   ├── input/                       # Staged input directory tree (files, subdirectories, .md5 sidecars)
│   ├── expected_tables.py           # EXPECTED_TABLES: dict[str, pa.Table] (including _rejected if applicable)
│   └── expected.json                # Canonical JSON rows keyed by table name
└── 02_<error_or_edge_case>/
    ├── case.yaml
    ├── settings.toml
    ├── input/
    ├── expected_tables.py
    └── expected.json
```

The manifests in `specs/pipelines/` drive two outputs:

1. **End-to-end smoke tests**: `tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py`
2. **Generated Markdown specification pages**:
   - [PubMed Pipeline Specification](pubmed_pipeline_specification.md)
   - [NCBI Datasets Pipeline Specification](ncbi_datasets_pipeline_specification.md)

---

## 2. How Pipeline Rules Are Created and Structured

Rules in `specs/pipelines/<pipeline>/rules.yaml` define the end-to-end behavioral contract of a pipeline across three stages:

1. **File Discovery & Pre-processing** (for example, `P1-DISCOVERY`, `P1-MD5-SIDECAR`, `N1-ENTITY-DIRS`):
   - Which files `input_dir` and `file_glob` match.
   - How subdirectories map to entity tables.
   - What file pre-processors (such as `validate_md5_sidecar` in `cdm_data_loaders.core.preprocessors`) verify before reading a file.
2. **Parsing, Validation, Transforms & Coercion** (for example, `P2-LEVEL-ROUTING`, `N2-JSONSCHEMA-VALIDATE`, `N3-FILTER-ANNOTATION`):
   - How records are parsed and validated against XSD or JSON Schema.
   - How `TABLE_TRANSFORMS` filter, mutate, or split records across tables.
   - How records are coerced to `OUTPUT_SCHEMAS` or precomputed PyArrow schemas (`PUBMED_SCHEMAS`, `SCHEMAS`).
3. **Error Routing & Pipeline Reporting** (for example, `P3-PREPROCESS-ERROR`, `P3-REPORT`, `N4-REJECTED`, `N4-REPORT`):
   - How file pre-processing failures, JSON syntax errors, and schema validation errors populate `_rejected`.
   - What `report.txt` and `_load_info` record for `status`, `ingested_files`, `load_errors`, and `pipeline_load_info_errs`.

### Rule Catalogue Format (`rules.yaml`)

```yaml
title: PubMed XML-to-PyArrow Pipeline Specification
intro: |
  End-to-end smoke test specification for the `pubmed` pipeline
  (`cdm_data_loaders.pipelines.xmltopyarrow.pipeline:pubmed_cli`, `PubMedXmlToPyArrowSettings`).

sections:
  - id: discovery_and_preprocessing
    title: File Discovery & MD5 Pre-processing
    rules:
      - id: P1-DISCOVERY
        name: Gzip XML discovery
        rule: |
          The pipeline scans `input_dir` for files matching `file_glob` (default `*.xml.gz`).
```

### Bidirectional Rule Invariants

`test_pipeline_smoke_spec.py` enforces two invariants on `rules.yaml` and `case.yaml`:

- **Every rule is exercised** (`test_manifests_pass_every_rule_is_exercised`): every rule ID in `rules.yaml` must appear in the `rules:` list of at least one `case.yaml` for that pipeline.
- **Every referenced rule exists** (`test_manifests_pass_every_case_rule_exists`): every rule ID listed in any `case.yaml` must exist in that pipeline's `rules.yaml`.

---

## 3. How Pipeline Errors Are Modeled and Tested

Unlike schema converters (which raise exceptions at schema-translation time), KBase CTS ingestion pipelines run on the CTS (CDM Task Service) must exit with code `0` so the container service preserves output tables, `_rejected` rows, `_load_info`, and `report.txt` (see [Core Pipeline & KBase CTS Execution Guide](core_cts_execution.md)).

Pipeline error scenarios are modeled as `case.yaml` runs that verify three error outputs:

### 1. `_rejected` Table Rows

Failed files and failed records are routed to the `_rejected` table (`REJECTED_PYARROW_SCHEMA` in `cdm_data_loaders.utils.buffer`) with five columns:

| Column | File Pre-processor Failure | Record Parse / Validation Failure |
| :--- | :--- | :--- |
| `table_name` | Target table name or `None` (multi-table XML level mode) | Entity table name (for example, `"annotation_report"`) |
| `source_file` | Relative path of the rejected file | Relative path of the input file |
| `line_no` | `None` | 1-based line number in the input file |
| `raw_record` | `None` | Raw unparsed text of the rejected line |
| `error_detail` | Pre-processor error message (for example, `"MD5 mismatch for ..."`) | JSON decode error string or JSON list of schema validation errors |

### 2. `expected_report` in `case.yaml`

Each run in `case.yaml` declares an `expected_report` block checked by `assert_smoke_run_output()`:

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
  - Route valid records in the same file to their destination table(s) and invalid lines to `_rejected`.
  - Keep `report.txt` `status: completed` when no file pre-processor or `dlt` load job failed.

---

## 4. Common Developer Workflows

### Adding a New Rule and Smoke Test Case

1. **Add or select rule IDs in `specs/pipelines/<pipeline>/rules.yaml`**:
   - Add a new rule entry (`id`, `name`, `rule`) under the appropriate section, or reuse existing rule IDs.
2. **Create `specs/pipelines/<pipeline>/<NN_case_name>/`**:
   - **`input/`**: Place small, deterministic input files under `input/`.
     - When creating `.gz` files or `.md5` sidecars for `pubmed`, compress with `gzip.compress(data, mtime=0)` so the gzip header timestamp is deterministic (`0`) and the MD5 digest never drifts.
   - **`settings.toml`**: Add case-level `Settings` overrides (for example, `loader_file_format = "parquet"` and `buffer_size = 10`).
   - **`expected_tables.py`**: Define `EXPECTED_TABLES: dict[str, pa.Table]` using the pipeline's schemas (`PUBMED_SCHEMAS`, `SCHEMAS`, and `REJECTED_PYARROW_SCHEMA` when `_rejected` has rows).
   - **`expected.json`**: Provide the canonical JSON representation of each expected table's rows (with `null` fields omitted).
   - **`case.yaml`**: Reference the rule IDs in `rules:` and define one or more `runs:` with `expected_counts` and `expected_report`.
3. **Regenerate the pipeline specification page**:
   ```bash
   uv run python -m scripts.gen_pipeline_spec
   ```
4. **Verify fixture invariants, end-to-end execution, and documentation sync**:
   ```bash
   uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py
   uv run pytest -m docs
   ```

### Editing Existing Rules, Cases, or Error Expectations

- **Updating a rule description**:
  - Edit `specs/pipelines/<pipeline>/rules.yaml` and run `uv run python -m scripts.gen_pipeline_spec`.
- **Updating input fixtures or expected outputs**:
  - Update `input/`, `expected_tables.py`, `expected.json`, and `case.yaml` (`expected_counts` / `expected_report`) together.
  - `test_manifests_pass_expected_tables_validate` checks that every `pa.Table` in `expected_tables.py` passes `table.validate(full=True)`, matches `expected_counts`, and normalizes to `expected.json`.
  - Run `uv run python -m scripts.gen_pipeline_spec` to refresh the embedded fixtures and expected JSON in `docs/<pipeline>_pipeline_specification.md`.

### Onboarding a New Pipeline to `specs/pipelines/`

1. **Ensure the pipeline is registered in `PIPELINE_TARGETS`**:
   - Check `PIPELINE_TARGETS` in `tests/cdm_data_loaders/pipelines/framework.py` so `resolve_pipeline("<pipeline>")` resolves its CLI command, module path, `Settings` class, and runner function.
2. **Create `specs/pipelines/<pipeline>/`**:
   - Add `rules.yaml` and at least one `<case>/` directory with `case.yaml`, `settings.toml`, `input/`, `expected_tables.py`, and `expected.json`.
3. **Register the pipeline in `scripts/gen_pipeline_spec.py`**:
   - Add a `PipelineSpecConfig` entry to `PIPELINE_SPECS` pointing to `specs/pipelines/<pipeline>` and `docs/<pipeline>_pipeline_specification.md`.
4. **Generate and index the specification page**:
   - Run `uv run python -m scripts.gen_pipeline_spec`.
   - Add `docs/<pipeline>_pipeline_specification.md` to `mkdocs.yml` (`nav`), `docs/index.md`, and `README.md`.
5. **Run verification**:
   ```bash
   uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py
   uv run pytest -m docs
   uv run mkdocs build --strict
   ```
