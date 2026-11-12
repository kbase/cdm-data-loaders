<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/pipelines/xsv/`. Regenerate with `uv run python -m scripts.gen_pipeline_spec` or `uv run python -m scripts.gen_converter_spec`. -->

# XSV (CSV/TSV) Schema & Pipeline Specification

This document specifies how an **XSV JSON Schema** (Draft 2020-12 or the KBase Draft 2020-12 XSV metaschema)
and the **XSV ingestion pipeline** (`cdm_data_loaders.pipelines.xsv` and `cdm_data_loaders.readers.jsonschema_xsv`)
determine:

1. **Schema validation and `x-xsv-config` parser configuration** (`validate_jsonschema`, `resolve_xsv_parsing_config`).
2. **The PyArrow `pa.Schema`** produced by `convert_xsv_schema_to_pyarrow`.
3. **Two-pass `qsv` cleaning, null replacement, and row validation** (`clean_validate_file`).
4. **DuckDB typed CSV reading and PyArrow table coercion** (`resolve_duckdb_column_config`, `read_validated_rows`).
5. **Pipeline execution, dead-letter routing (`_rejected`), and table transforms** (`_process_xsv_file`, `run_xsv_pipeline`).
6. **Errors** (`XsvSchemaError` subclasses) raised for invalid or unsupported schemas.

| Property | Value |
| --- | --- |
| **Base Pipeline Family** | `xsv` |
| **Base CLI Entry Point** | `xsv` |
| **Base Settings Class** | `cdm_data_loaders.pipelines.xsv.settings.XsvIngestSettings` |
| **Pipeline Runner** | `cdm_data_loaders.pipelines.xsv.pipeline.run_xsv_pipeline` |
| **Preset Subclasses Covered** | — |
| **Specification Directory** | `specs/pipelines/xsv/` |

## Running Smoke Tests Across Environments

```bash
# Run all base and preset smoke tests for xsv across built-in environments
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py --smoke-pipeline xsv

# Run against a custom environment directory (config.toml, secrets.toml, settings.toml)
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py \
    --smoke-pipeline xsv \
    --dlt-config-dir /path/to/env_dlt_config \
    --settings-file /path/to/env_dlt_config/settings.toml
```

---

## 1. Conversion & Pipeline Rules

### 1.1 Schema Validation and `x-xsv-config` Resolution

<a id="r-xsv-dialect"></a>

- **R-XSV-DIALECT**: The root schema must declare `"$schema": "https://json-schema.org/draft/2020-12/schema"` or the KBase
  XSV metaschema `"https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json"`; missing
  or unsupported `$schema` URIs raise **E1** (`UnsupportedXsvDialectError`).

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref)

<a id="r-xsv-required-cols"></a>

- **R-XSV-REQUIRED-COLS**: Top-level `required` must be a non-empty list of column header strings declaring the ordered columns
  of the XSV file; a missing or empty `required` list raises **E2** (`MissingRequiredColumnsError`).

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols)

<a id="r-xsv-metaschema"></a>

- **R-XSV-METASCHEMA**: Schemas declaring `"https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json"` lazily
  load the custom metaschema (from bundled `schemas/xsv_config/0.1.0/schema.json` or its published URL),
  validate it against Draft 2020-12, and validate the pipeline schema against it (`get_xsv_meta_schema`).
  Standard Draft 2020-12 schemas containing an `x-xsv-config` block validate that block against the
  `x-xsv-config` subschema (`get_xsv_config_schema`). Invalid JSON Schema keywords or malformed
  `x-xsv-config` properties raise **E3** (`InvalidXsvSchemaDefinitionError`).

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols)

<a id="r-xsv-config"></a>

- **R-XSV-CONFIG**: `resolve_xsv_parsing_config` maps `x-xsv-config` keys (after stripping the `x-` prefix and replacing
  `-` with `_`) to `CleanerValidatorArgs` fields per the table below. Schemas without `x-xsv-config`
  fall back to `CleanerValidatorArgs` defaults (`delimiter="\t"`, `comment_char="#"`,
  `missing_header=False`, `quote=None`, `escape=None`, `null_regex=None`, `null_regex_cols=None`).
  `x-quoting-policy` is validated as output metadata and omitted from reader kwargs.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols)

| `x-xsv-config` key | JSON Schema constraint | `CleanerValidatorArgs` field | Default |
| :--- | :--- | :--- | :--- |
| `x-has-header` | `{"type": "boolean"}` | `missing_header` (inverted: `not x-has-header`) | `False` (`x-has-header=True`) |
| `x-delimiter` | `{"type": "string", "minLength": 1, "maxLength": 1}` | `delimiter` | `'\t'` |
| `x-comment-char` | `{"type": "string", "minLength": 1, "maxLength": 1}` | `comment_char` | `'#'` |
| `x-quote` | `{"type": "string", "minLength": 1, "maxLength": 1}` | `quote` | `None` |
| `x-escape` | `{"type": "string", "minLength": 1, "maxLength": 1}` | `escape` | `None` |
| `x-quoting-policy` | `{"type": "string", "enum": ["all", "necessary", "nonnumeric", "never"]}` | *(output metadata only; not passed to `CleanerValidatorArgs`)* | — |
| `x-null-regex` | `{"type": "string"}` | `null_regex` | `None` |
| `x-null-cols` | `{"type": "array", "items": {"type": "string"}, "minItems": 1}` | `null_regex_cols` | `None` |

### 1.2 PyArrow Schema Generation

<a id="r-xsv-deref"></a>

- **R-XSV-DEREF**: `convert_xsv_schema_to_pyarrow` resolves all local `$ref` pointers and merges `allOf` subschemas via
  `dereference_schema` before inspecting properties. An unresolvable `$ref` raises **E4**
  (`UnresolvableXsvRefError`), and a non-struct root or unsupported column construct raises **E5**
  (`UnsupportedXsvArrowConstructError`).

  *Cases:* [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref)

<a id="r-xsv-col-order"></a>

- **R-XSV-COL-ORDER**: Top-level `pa.Schema` fields are emitted in the exact order of top-level `required`, followed by any
  extra `properties` not listed in `required` in their declaration order.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order)

<a id="r-xsv-unlisted-col"></a>

- **R-XSV-UNLISTED-COL**: Any column listed in top-level `required` that is absent from `properties` defaults to
  `{"type": ["string", "null"]}` (`pa.field(col, pa.string(), nullable=True)`).

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order)

<a id="r-xsv-nullability"></a>

- **R-XSV-NULLABILITY**: Top-level `required` defines the file's column headers, not value non-nullability. A column is
  `nullable=False` in PyArrow if and only if it is in `required` and its property schema does not
  permit `null` (via `"null"` in `type`, a nullable `anyOf`/`oneOf` branch, or an untyped `{}` schema).
  Extra `properties` absent from `required` are `nullable=True`.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols)

<a id="r-xsv-scalar-map"></a>

- **R-XSV-SCALAR-MAP**: Column schemas convert to PyArrow types via `JSONSchemaToPyArrow` per the table below. Every scalar
  field carries `metadata["jsonschema_type"]`, and a non-empty `description` is stored in
  `metadata["comment"]`.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref)

| Column JSON Schema | DuckDB CSV `dtype` | `force_not_null` | PyArrow `DataType` | `Field.nullable` |
| :--- | :--- | :--- | :--- | :--- |
| `{"type": "boolean"}` | `BOOLEAN` | `False` | `pa.bool_()` | `False` |
| `{"type": ["boolean", "null"]}` | `BOOLEAN` | `False` | `pa.bool_()` | `True` |
| `{"type": "integer"}` | `BIGINT` | `False` | `pa.int64()` | `False` |
| `{"type": ["integer", "null"]}` | `BIGINT` | `False` | `pa.int64()` | `True` |
| `{"type": "number"}` | `DOUBLE` | `False` | `pa.float64()` | `False` |
| `{"type": ["number", "null"]}` | `DOUBLE` | `False` | `pa.float64()` | `True` |
| `{"type": "string"}` | `VARCHAR` | `True` | `pa.string()` | `False` |
| `{"type": ["string", "null"]}` | `VARCHAR` | `False` | `pa.string()` | `True` |
| `{"type": "integer", "minimum": 0, "maximum": 1000}` | `BIGINT` | `False` | `pa.int64()` | `False` |
| `{"type": "number", "multipleOf": 0.01}` | `DOUBLE` | `False` | `pa.float64()` | `False` |
| `{"type": "string", "format": "date"}` | `DATE` | `False` | `pa.date32()` | `False` |
| `{"type": "string", "format": "date-time"}` | `VARCHAR` | `True` | `pa.string()` | `False` |
| `{"type": "string", "pattern": "^G[0-9]{4}$"}` | `VARCHAR` | `True` | `pa.string()` | `False` |
| `{"anyOf": [{"type": "string"}, {"type": "null"}]}` | `VARCHAR` | `False` | `pa.string()` | `True` |
| *(column in `required`, omitted from `properties`)* | `VARCHAR (auto)` | `False` | `pa.string()` | `True` |

### 1.3 Two-Pass `qsv` Cleaning and Validation

<a id="r-xsv-header"></a>

- **R-XSV-HEADER**: When `x-has-header` is `false` (`missing_header=True`), `clean_validate_file` prepends a synthetic
  header line generated from `required_cols` joined by the configured `delimiter` before running `qsv`.
  When `x-has-header` is `true` (default), the file's first non-comment row serves as the header.

  *Cases:* [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref)

<a id="r-xsv-first-pass"></a>

- **R-XSV-FIRST-PASS**: First-pass validation (`run_qsv_validate(..., first_pass=True)`) runs before `qsv input` and checks
  structural column count against `generate_first_pass_schema` (where every `required` column is
  `{"type": ["string", "null"]}`) using `--no-format-validation --split-ragged --trim` and
  `QSV_COMMENT_CHAR=<comment_char>`, routing ragged rows to `.invalid` and promoting surviving `.valid`
  rows to the cleaning step.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols), [`spec/05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections), [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs)

<a id="r-xsv-clean-input"></a>

- **R-XSV-CLEAN-INPUT**: `run_qsv_input` runs `qsv input --trim-headers --trim-fields --encoding-errors strict --delimiter <delimiter>`
  (plus `--quote` and `--escape` when set) with `QSV_COMMENT_CHAR=<comment_char>`, stripping comment
  lines, trimming header and field whitespace, and normalizing quotes and escapes.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref)

<a id="r-xsv-null-replace"></a>

- **R-XSV-NULL-REPLACE**: When `x-null-regex` is set, `run_qsv_null_replacement` invokes `qsv replace --not-one` to replace
  matching cell values with `""` across all columns, or only across `--select <x-null-cols>` when
  `x-null-cols` is specified.

  *Cases:* [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols)

<a id="r-xsv-second-pass"></a>

- **R-XSV-SECOND-PASS**: Second-pass validation (`run_qsv_validate(..., first_pass=False)`) validates normalized rows against
  the full schema (rewriting the KBase XSV metaschema URI to standard Draft 2020-12 for the `qsv`
  binary). If all rows are valid, `--valid-output` is used directly; if some rows fail (`returncode != 0`)
  but `.valid` retains at least one data row, the surviving rows are promoted and an `ErrorRecord` is
  appended; if no data rows survive, `clean_validate_file` returns `None` and records an `ErrorRecord`.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols), [`spec/05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections), [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs)

### 1.4 DuckDB Reading and PyArrow Coercion

<a id="r-xsv-duckdb-types"></a>

- **R-XSV-DUCKDB-TYPES**: `resolve_duckdb_column_config` maps each field in `pa_schema` to a `DuckDBPyType` via
  `duckdb.from_arrow` (and unlisted columns -> `VARCHAR` via `auto_type_candidates=["VARCHAR"]`).
  Non-nullable string columns are listed in `force_not_null` so empty cells remain `""` rather than `None`.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols), [`spec/05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections), [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs), [`spec/07_table_transforms_and_output_schemas`](#case-spec-07-table-transforms-and-output-schemas)

<a id="r-xsv-arrow-cast"></a>

- **R-XSV-ARROW-CAST**: When no table transform is registered, `read_validated_rows(..., use_pyarrow=True, pa_schema=pa_schema)`
  converts each non-empty DuckDB `RecordBatch` to a `pa.Table`, appends typed null columns for any
  `pa_schema` fields missing from the file, selects `pa_schema.names` in schema order, and casts the
  table to `pa_schema`. Empty or header-only files yield no batches.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes), [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref), [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols), [`spec/05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections), [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs)

### 1.5 Pipeline Execution, Dead-Letter Routing, and Transforms

<a id="r-xsv-glob"></a>

- **R-XSV-GLOB**: `run_xsv_pipeline` discovers files in `settings.input_dir` matching `settings.file_glob` (including
  nested subdirectories) and processes each file independently via `_process_xsv_file`.

  *Cases:* [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order), [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs)

<a id="r-xsv-rejected"></a>

- **R-XSV-REJECTED**: Every `ErrorRecord` recorded while cleaning or validating a file is emitted to the `_rejected` table
  (`REJECTED_PYARROW_SCHEMA`) via `ErrorRecord.to_rejected_row` with `table_name=settings.table_name`,
  `source_file` set to the file path relative to `input_dir`, `line_no=None`, `raw_record=None`, and
  `error_detail` set to the `qsv` diagnostic message.

  *Cases:* [`spec/05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections), [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs)

<a id="r-xsv-transforms"></a>

- **R-XSV-TRANSFORMS**: When `settings.table_transforms` defines a callable for `settings.table_name`, `read_validated_rows`
  yields Python row dicts (`use_pyarrow=False`) routed through `route_and_coerce_record` and
  `DictBuffer`. The transform may mutate rows, drop rows (`None` / `[]`), fan out records across
  multiple tables, and coerce each output table against `settings.output_schemas` (or the default XSV
  PyArrow schema).

  *Cases:* [`spec/07_table_transforms_and_output_schemas`](#case-spec-07-table-transforms-and-output-schemas)

---

## 2. Case Coverage Matrix

| Suite | Target CLI | Settings Class | Case | Title | Rules | Expected Table Counts |
| --- | --- | --- | --- | --- | --- | --- |
| `spec` | `xsv` | `XsvIngestSettings` | [`01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order) | Scalars, Nullability, Column Ordering, and Unlisted Columns | [`R-XSV-DIALECT`](#r-xsv-dialect), [`R-XSV-REQUIRED-COLS`](#r-xsv-required-cols), [`R-XSV-METASCHEMA`](#r-xsv-metaschema), [`R-XSV-CONFIG`](#r-xsv-config), [`R-XSV-COL-ORDER`](#r-xsv-col-order), [`R-XSV-UNLISTED-COL`](#r-xsv-unlisted-col), [`R-XSV-NULLABILITY`](#r-xsv-nullability), [`R-XSV-SCALAR-MAP`](#r-xsv-scalar-map), [`R-XSV-CLEAN-INPUT`](#r-xsv-clean-input), [`R-XSV-FIRST-PASS`](#r-xsv-first-pass), [`R-XSV-SECOND-PASS`](#r-xsv-second-pass), [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-ARROW-CAST`](#r-xsv-arrow-cast), [`R-XSV-GLOB`](#r-xsv-glob) | `default`: `{'spec_table': 2}` |
| `spec` | `xsv` | `XsvIngestSettings` | [`02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes) | Custom Delimiters, Comment Characters, Quotes, and Escapes | [`R-XSV-DIALECT`](#r-xsv-dialect), [`R-XSV-REQUIRED-COLS`](#r-xsv-required-cols), [`R-XSV-METASCHEMA`](#r-xsv-metaschema), [`R-XSV-CONFIG`](#r-xsv-config), [`R-XSV-CLEAN-INPUT`](#r-xsv-clean-input), [`R-XSV-SECOND-PASS`](#r-xsv-second-pass), [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-ARROW-CAST`](#r-xsv-arrow-cast) | `default`: `{'spec_table': 2}` |
| `spec` | `xsv` | `XsvIngestSettings` | [`03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref) | Missing Header Synthesis, `$defs` / `$ref`, and `anyOf` Nullability | [`R-XSV-DIALECT`](#r-xsv-dialect), [`R-XSV-REQUIRED-COLS`](#r-xsv-required-cols), [`R-XSV-METASCHEMA`](#r-xsv-metaschema), [`R-XSV-CONFIG`](#r-xsv-config), [`R-XSV-DEREF`](#r-xsv-deref), [`R-XSV-NULLABILITY`](#r-xsv-nullability), [`R-XSV-SCALAR-MAP`](#r-xsv-scalar-map), [`R-XSV-HEADER`](#r-xsv-header), [`R-XSV-CLEAN-INPUT`](#r-xsv-clean-input), [`R-XSV-FIRST-PASS`](#r-xsv-first-pass), [`R-XSV-SECOND-PASS`](#r-xsv-second-pass), [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-ARROW-CAST`](#r-xsv-arrow-cast) | `default`: `{'spec_table': 3}` |
| `spec` | `xsv` | `XsvIngestSettings` | [`04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols) | Selective Null Regex Replacement (`x-null-regex` and `x-null-cols`) | [`R-XSV-REQUIRED-COLS`](#r-xsv-required-cols), [`R-XSV-METASCHEMA`](#r-xsv-metaschema), [`R-XSV-CONFIG`](#r-xsv-config), [`R-XSV-NULLABILITY`](#r-xsv-nullability), [`R-XSV-FIRST-PASS`](#r-xsv-first-pass), [`R-XSV-NULL-REPLACE`](#r-xsv-null-replace), [`R-XSV-SECOND-PASS`](#r-xsv-second-pass), [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-ARROW-CAST`](#r-xsv-arrow-cast) | `default`: `{'spec_table': 4}` |
| `spec` | `xsv` | `XsvIngestSettings` | [`05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections) | Partial Row Recovery vs Total File Rejection (`_rejected`) | [`R-XSV-FIRST-PASS`](#r-xsv-first-pass), [`R-XSV-SECOND-PASS`](#r-xsv-second-pass), [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-ARROW-CAST`](#r-xsv-arrow-cast), [`R-XSV-REJECTED`](#r-xsv-rejected) | `partial_recovery`: `{'records': 2, '_rejected': 2}`; `total_failure`: `{'_rejected': 1}` |
| `spec` | `xsv` | `XsvIngestSettings` | [`06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs) | Multi-File Glob Discovery Across Subdirectories and Per-File Rejections | [`R-XSV-GLOB`](#r-xsv-glob), [`R-XSV-FIRST-PASS`](#r-xsv-first-pass), [`R-XSV-SECOND-PASS`](#r-xsv-second-pass), [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-ARROW-CAST`](#r-xsv-arrow-cast), [`R-XSV-REJECTED`](#r-xsv-rejected) | `default`: `{'samples': 3, '_rejected': 1}` |
| `spec` | `xsv` | `XsvIngestSettings` | [`07_table_transforms_and_output_schemas`](#case-spec-07-table-transforms-and-output-schemas) | Table Transforms, Row Filtering, Multi-Table Fan-Out, and Output Schemas | [`R-XSV-DUCKDB-TYPES`](#r-xsv-duckdb-types), [`R-XSV-TRANSFORMS`](#r-xsv-transforms) | `with_transforms`: `{'readings': 2, 'alerts': 1}` |

---

## 3. Base Pipeline Specification Cases (`xsv` / `spec/`)

**Shared Fixture Module (`specs/pipelines/xsv/transforms.py`)**

```python
"""Table transforms and PyArrow output schemas for XSV specification case 07."""

from typing import Any

import pyarrow as pa

READINGS_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("celsius", pa.float64(), nullable=False),
        pa.field("kelvin", pa.float64(), nullable=False),
    ]
)

ALERTS_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("celsius", pa.float64(), nullable=False),
    ]
)


def transform_readings(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Filter dropped sensor rows, enrich with Kelvin temperature, and fan out high-temperature alerts."""
    if row["status"] == "drop":
        return {}
    sensor_id = str(row["sensor_id"]).upper()
    celsius = float(row["celsius"])
    routed: dict[str, list[dict[str, Any]]] = {
        "readings": [
            {
                "sensor_id": sensor_id,
                "celsius": celsius,
                "kelvin": round(celsius + 273.15, 2),
            }
        ]
    }
    if celsius >= 100.0:  # noqa: PLR2004
        routed["alerts"] = [{"sensor_id": sensor_id, "celsius": celsius}]
    return routed


TABLE_TRANSFORMS = {"readings": transform_readings}
OUTPUT_SCHEMAS = {"readings": READINGS_SCHEMA, "alerts": ALERTS_SCHEMA}
```

<a id="case-spec-01-scalars-nullability-and-column-order"></a>

### Scalars, Nullability, Column Ordering, and Unlisted Columns (`spec/01_scalars_nullability_and_column_order`)

**Rules:** [R-XSV-DIALECT](#r-xsv-dialect), [R-XSV-REQUIRED-COLS](#r-xsv-required-cols), [R-XSV-METASCHEMA](#r-xsv-metaschema), [R-XSV-CONFIG](#r-xsv-config), [R-XSV-COL-ORDER](#r-xsv-col-order), [R-XSV-UNLISTED-COL](#r-xsv-unlisted-col), [R-XSV-NULLABILITY](#r-xsv-nullability), [R-XSV-SCALAR-MAP](#r-xsv-scalar-map), [R-XSV-CLEAN-INPUT](#r-xsv-clean-input), [R-XSV-FIRST-PASS](#r-xsv-first-pass), [R-XSV-SECOND-PASS](#r-xsv-second-pass), [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-ARROW-CAST](#r-xsv-arrow-cast), [R-XSV-GLOB](#r-xsv-glob)

- The schema omits `x-xsv-config`, so `CleanerValidatorArgs` defaults apply (`delimiter="\t"`, `comment_char="#"`, `missing_header=False`).
- `properties` declares `score` before `id`, but top-level `required` orders the output columns starting with `id`, `count`, `rank32`, `score`, ...
- `unlisted_tag` is listed in `required` but absent from `properties`, so it defaults to `pa.field("unlisted_tag", pa.string(), nullable=True)`.
- `extra_col` is declared in `properties` but absent from `required` and the TSV file; it is appended at the end of `pa.Schema` (`nullable=True`) and filled with `null` values on read.
- `status` (`{"type": "string"}`) is non-nullable (`nullable=False`, `force_not_null`), while `opt_note` (`{"type": ["string", "null"]}`) and `unlisted_tag` allow `null` and become `None` when empty.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ScalarsNullabilityAndOrder",
  "type": "object",
  "required": [
    "id",
    "count",
    "rank32",
    "score",
    "price",
    "active",
    "created_date",
    "updated_at",
    "status",
    "opt_note",
    "unlisted_tag"
  ],
  "properties": {
    "score": {
      "type": "number",
      "description": "Floating-point score."
    },
    "id": {
      "type": "string",
      "title": "RecordId",
      "description": "Primary record identifier."
    },
    "count": {
      "type": "integer"
    },
    "rank32": {
      "type": "integer",
      "minimum": 0,
      "maximum": 1000
    },
    "price": {
      "type": "number",
      "multipleOf": 0.01
    },
    "active": {
      "type": "boolean"
    },
    "created_date": {
      "type": "string",
      "format": "date"
    },
    "updated_at": {
      "type": "string",
      "format": "date-time"
    },
    "status": {
      "type": "string"
    },
    "opt_note": {
      "type": [
        "string",
        "null"
      ]
    },
    "extra_col": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Extra property omitted from required."
    }
  }
}
```

#### Input (`data.tsv`)

```text
# Leading comment stripped by qsv
id	count	rank32	score	price	active	created_date	updated_at	status	opt_note	unlisted_tag
rec-1	42	10	3.5	19.99	true	2025-01-15	2025-01-15T08:30:00Z	 active 	 first note 	alpha
# Interleaved comment line
rec-2	-100	0	-0.25	0.50	false	2025-02-20	2025-02-20T14:00:00Z	inactive
```

#### Converted rows: run `default`

```json
[
  {
    "id": "rec-1",
    "count": 42,
    "rank32": 10,
    "score": 3.5,
    "price": 19.99,
    "active": true,
    "created_date": "2025-01-15",
    "updated_at": "2025-01-15T08:30:00Z",
    "status": "active",
    "opt_note": "first note",
    "unlisted_tag": "alpha",
    "extra_col": null
  },
  {
    "id": "rec-2",
    "count": -100,
    "rank32": 0,
    "score": -0.25,
    "price": 0.5,
    "active": false,
    "created_date": "2025-02-20",
    "updated_at": "2025-02-20T14:00:00Z",
    "status": "inactive",
    "opt_note": null,
    "unlisted_tag": null,
    "extra_col": null
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/01_scalars_nullability_and_column_order/expected_schema.py))

```python
"""Expected PyArrow schema for 01_scalars_nullability_and_column_order."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Primary record identifier.", "jsonschema_type": "string"},
        ),
        pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("rank32", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "score",
            pa.float64(),
            nullable=False,
            metadata={"comment": "Floating-point score.", "jsonschema_type": "number"},
        ),
        pa.field("price", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("active", pa.bool_(), nullable=False, metadata={"jsonschema_type": "boolean"}),
        pa.field("created_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
        pa.field("updated_at", pa.string(), nullable=False, metadata={"jsonschema_type": "string (date-time)"}),
        pa.field("status", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("opt_note", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("unlisted_tag", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "extra_col",
            pa.int64(),
            nullable=True,
            metadata={"comment": "Extra property omitted from required.", "jsonschema_type": "integer"},
        ),
    ]
)
```

---

<a id="case-spec-02-delimiters-comments-quotes-and-escapes"></a>

### Custom Delimiters, Comment Characters, Quotes, and Escapes (`spec/02_delimiters_comments_quotes_and_escapes`)

**Rules:** [R-XSV-DIALECT](#r-xsv-dialect), [R-XSV-REQUIRED-COLS](#r-xsv-required-cols), [R-XSV-METASCHEMA](#r-xsv-metaschema), [R-XSV-CONFIG](#r-xsv-config), [R-XSV-CLEAN-INPUT](#r-xsv-clean-input), [R-XSV-SECOND-PASS](#r-xsv-second-pass), [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-ARROW-CAST](#r-xsv-arrow-cast)

- Declares `"$schema": "https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json"` with `x-delimiter: ";"`, `x-comment-char: "%"`, `x-quote: "'"`, `x-escape: "\\"`, and `x-quoting-policy: "necessary"`.
- Lines starting with `%` are stripped as comments, while `#` inside `sample_id` (`"S#2"`) is preserved as data.
- `qsv input` strips surrounding single quotes (`'`) and unescapes `\'` sequences inside quoted fields.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json",
  "title": "CustomDelimitersQuotesAndEscapes",
  "type": "object",
  "required": [
    "sample_id",
    "measurement",
    "label"
  ],
  "properties": {
    "sample_id": {
      "type": "string"
    },
    "measurement": {
      "type": "number"
    },
    "label": {
      "type": "string"
    }
  },
  "x-xsv-config": {
    "x-delimiter": ";",
    "x-comment-char": "%",
    "x-quote": "'",
    "x-escape": "\\",
    "x-has-header": true,
    "x-quoting-policy": "necessary"
  }
}
```

#### Input (`data.csv`)

```text
% Semicolon-delimited file with single-quote quoting and backslash escaping
sample_id;measurement;label
S-1;12.5;'quoted label'
% Mid-file comment
S#2;8.75;'escaped \'quote\' inside'
```

#### Converted rows: run `default`

```json
[
  {
    "sample_id": "S-1",
    "measurement": 12.5,
    "label": "quoted label"
  },
  {
    "sample_id": "S#2",
    "measurement": 8.75,
    "label": "escaped 'quote' inside"
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/02_delimiters_comments_quotes_and_escapes/expected_schema.py))

```python
"""Expected PyArrow schema for 02_delimiters_comments_quotes_and_escapes."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("measurement", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("label", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
```

---

<a id="case-spec-03-missing-header-and-defs-ref"></a>

### Missing Header Synthesis, `$defs` / `$ref`, and `anyOf` Nullability (`spec/03_missing_header_and_defs_ref`)

**Rules:** [R-XSV-DIALECT](#r-xsv-dialect), [R-XSV-REQUIRED-COLS](#r-xsv-required-cols), [R-XSV-METASCHEMA](#r-xsv-metaschema), [R-XSV-CONFIG](#r-xsv-config), [R-XSV-DEREF](#r-xsv-deref), [R-XSV-NULLABILITY](#r-xsv-nullability), [R-XSV-SCALAR-MAP](#r-xsv-scalar-map), [R-XSV-HEADER](#r-xsv-header), [R-XSV-CLEAN-INPUT](#r-xsv-clean-input), [R-XSV-FIRST-PASS](#r-xsv-first-pass), [R-XSV-SECOND-PASS](#r-xsv-second-pass), [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-ARROW-CAST](#r-xsv-arrow-cast)

- `x-has-header: false` sets `missing_header=True`, causing `clean_validate_file` to prepend a comma-delimited header (`gene_id,reads,organism`) generated from `required`.
- `gene_id` and `organism` resolve through local `$defs`/`$ref` via `dereference_schema`.
- `organism` uses `anyOf: [{"type": "string"}, {"type": "null"}]`, which is recognized as `nullable=True` in PyArrow and nullable `VARCHAR` in DuckDB.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json",
  "title": "MissingHeaderAndDefsRef",
  "type": "object",
  "required": [
    "gene_id",
    "reads",
    "organism"
  ],
  "$defs": {
    "GeneId": {
      "type": "string",
      "pattern": "^G[0-9]{4}$",
      "description": "Standard gene identifier."
    },
    "NullableOrganism": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "description": "Organism name or null."
    }
  },
  "properties": {
    "gene_id": {
      "$ref": "#/$defs/GeneId"
    },
    "reads": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Read count or null when unmeasured."
    },
    "organism": {
      "$ref": "#/$defs/NullableOrganism"
    }
  },
  "x-xsv-config": {
    "x-delimiter": ",",
    "x-has-header": false
  }
}
```

#### Input (`data.csv`)

```text
G0001,1500,Escherichia coli
G0002,,Bacillus subtilis
G0003,0,Saccharomyces cerevisiae
```

#### Converted rows: run `default`

```json
[
  {
    "gene_id": "G0001",
    "reads": 1500,
    "organism": "Escherichia coli"
  },
  {
    "gene_id": "G0002",
    "reads": null,
    "organism": "Bacillus subtilis"
  },
  {
    "gene_id": "G0003",
    "reads": 0,
    "organism": "Saccharomyces cerevisiae"
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/03_missing_header_and_defs_ref/expected_schema.py))

```python
"""Expected PyArrow schema for 03_missing_header_and_defs_ref."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "gene_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Standard gene identifier.", "jsonschema_type": "string"},
        ),
        pa.field(
            "reads",
            pa.int64(),
            nullable=True,
            metadata={"comment": "Read count or null when unmeasured.", "jsonschema_type": "integer"},
        ),
        pa.field(
            "organism",
            pa.string(),
            nullable=True,
            metadata={"comment": "Organism name or null.", "jsonschema_type": "string"},
        ),
    ]
)
```

---

<a id="case-spec-04-null-regex-and-null-cols"></a>

### Selective Null Regex Replacement (`x-null-regex` and `x-null-cols`) (`spec/04_null_regex_and_null_cols`)

**Rules:** [R-XSV-REQUIRED-COLS](#r-xsv-required-cols), [R-XSV-METASCHEMA](#r-xsv-metaschema), [R-XSV-CONFIG](#r-xsv-config), [R-XSV-NULLABILITY](#r-xsv-nullability), [R-XSV-FIRST-PASS](#r-xsv-first-pass), [R-XSV-NULL-REPLACE](#r-xsv-null-replace), [R-XSV-SECOND-PASS](#r-xsv-second-pass), [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-ARROW-CAST](#r-xsv-arrow-cast)

- First-pass validation uses `generate_first_pass_schema` (where all columns accept strings), allowing placeholder strings `"NA"`, `"N/A"`, `"null"`, and `"-"` in numeric columns `depth` and `ph` to pass structural checks.
- `qsv replace` then replaces matches of `^(NA|N/A|null|-)$` with `""` only in `x-null-cols: ["depth", "ph"]` before second-pass validation checks `integer` and `number` types.
- Because `raw_code` is not listed in `x-null-cols`, its `"NA"` and `"-"` values are preserved verbatim.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json",
  "title": "SelectiveNullRegexReplacement",
  "type": "object",
  "required": [
    "site",
    "depth",
    "ph",
    "raw_code"
  ],
  "properties": {
    "site": {
      "type": "string"
    },
    "depth": {
      "type": [
        "integer",
        "null"
      ]
    },
    "ph": {
      "type": [
        "number",
        "null"
      ]
    },
    "raw_code": {
      "type": "string"
    }
  },
  "x-xsv-config": {
    "x-delimiter": "\t",
    "x-null-regex": "^(NA|N/A|null|-)$",
    "x-null-cols": [
      "depth",
      "ph"
    ]
  }
}
```

#### Input (`data.tsv`)

```text
site	depth	ph	raw_code
station-1	10	7.2	NA
station-2	NA	N/A	-
station-3	-	6.8	null
station-4	25	null	OK
```

#### Converted rows: run `default`

```json
[
  {
    "site": "station-1",
    "depth": 10,
    "ph": 7.2,
    "raw_code": "NA"
  },
  {
    "site": "station-2",
    "depth": null,
    "ph": null,
    "raw_code": "-"
  },
  {
    "site": "station-3",
    "depth": null,
    "ph": 6.8,
    "raw_code": "null"
  },
  {
    "site": "station-4",
    "depth": 25,
    "ph": null,
    "raw_code": "OK"
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/04_null_regex_and_null_cols/expected_schema.py))

```python
"""Expected PyArrow schema for 04_null_regex_and_null_cols."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("site", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("depth", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ph", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("raw_code", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
```

---

<a id="case-spec-05-partial-and-total-validation-rejections"></a>

### Partial Row Recovery vs Total File Rejection (`_rejected`) (`spec/05_partial_and_total_validation_rejections`)

**Rules:** [R-XSV-FIRST-PASS](#r-xsv-first-pass), [R-XSV-SECOND-PASS](#r-xsv-second-pass), [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-ARROW-CAST](#r-xsv-arrow-cast), [R-XSV-REJECTED](#r-xsv-rejected)

- In run `partial_recovery` (`partial.csv`), 1 of 4 rows is ragged (too few columns) and is split out by first-pass validation (`"1 out of 4 records invalid."`), and 1 of the remaining 3 rows fails second-pass schema validation (`amount` is `"not-an-int"`, `"1 out of 3 records invalid."`). The 2 valid rows are promoted to `records` while both pass errors are recorded in `_rejected`.
- In run `total_failure` (`all_invalid.csv`), all 2 rows fail second-pass validation (`"bad-int"` and `"not-a-date"`). Because `.valid` contains only the header line, no data rows are emitted to `records` and `_rejected` receives `"2 out of 2 records invalid."`.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json",
  "title": "PartialAndTotalValidationRejections",
  "type": "object",
  "required": [
    "record_id",
    "amount",
    "event_date"
  ],
  "properties": {
    "record_id": {
      "type": "string"
    },
    "amount": {
      "type": "integer"
    },
    "event_date": {
      "type": "string",
      "format": "date"
    }
  },
  "x-xsv-config": {
    "x-delimiter": ",",
    "x-has-header": true
  }
}
```

#### Input (`partial.csv`)

```text
record_id,amount,event_date
R1,100,2025-03-01
R_RAGGED,only_two_cols
R2,not-an-int,2025-03-02
R3,250,2025-03-03
```

#### Converted rows: run `partial_recovery` (`table_name=records`)

```json
[
  {
    "record_id": "R1",
    "amount": 100,
    "event_date": "2025-03-01"
  },
  {
    "record_id": "R3",
    "amount": 250,
    "event_date": "2025-03-03"
  }
]
```

#### Rejected rows (`_rejected`): run `partial_recovery` (`table_name=records`)

```json
[
  {
    "table_name": "records",
    "source_file": "partial.csv",
    "line_no": null,
    "raw_record": null,
    "error_detail": "1 out of 4 records invalid."
  },
  {
    "table_name": "records",
    "source_file": "partial.csv",
    "line_no": null,
    "raw_record": null,
    "error_detail": "1 out of 3 records invalid."
  }
]
```

#### Input (`all_invalid.csv`)

```text
record_id,amount,event_date
R1,bad-int,2025-03-01
R2,100,not-a-date
```

#### Converted rows: run `total_failure` (`table_name=records`)

```json
[]
```

#### Rejected rows (`_rejected`): run `total_failure` (`table_name=records`)

```json
[
  {
    "table_name": "records",
    "source_file": "all_invalid.csv",
    "line_no": null,
    "raw_record": null,
    "error_detail": "2 out of 2 records invalid."
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/05_partial_and_total_validation_rejections/expected_schema.py))

```python
"""Expected PyArrow schema for 05_partial_and_total_validation_rejections."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("record_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("amount", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("event_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
    ]
)
```

---

<a id="case-spec-06-multi-file-glob-and-subdirs"></a>

### Multi-File Glob Discovery Across Subdirectories and Per-File Rejections (`spec/06_multi_file_glob_and_subdirs`)

**Rules:** [R-XSV-GLOB](#r-xsv-glob), [R-XSV-FIRST-PASS](#r-xsv-first-pass), [R-XSV-SECOND-PASS](#r-xsv-second-pass), [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-ARROW-CAST](#r-xsv-arrow-cast), [R-XSV-REJECTED](#r-xsv-rejected)

- `input_files/` contains two subdirectories with same-stem files (`batch_1/data.csv` and `batch_2/data.csv`) plus a non-matching `batch_1/notes.txt` file ignored by `file_glob: "*/*.csv"`.
- `batch_1/data.csv` has 2 valid rows; `batch_2/data.csv` has 1 valid row and 1 ragged row (`3\n`), so its valid row is merged into `samples` and its failure is recorded in `_rejected` with `source_file: "batch_2/data.csv"`.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "MultiFileGlobSamples",
  "type": "object",
  "required": [
    "sample_num",
    "sample_name"
  ],
  "properties": {
    "sample_num": {
      "type": "integer"
    },
    "sample_name": {
      "type": "string"
    }
  },
  "x-xsv-config": {
    "x-delimiter": ",",
    "x-has-header": true
  }
}
```

#### Input (`input_files/batch_1/data.csv`)

```text
sample_num,sample_name
1,alpha
2,beta
```

#### Input (`input_files/batch_1/notes.txt`)

```text
Ignored by file_glob="*/*.csv"
```

#### Input (`input_files/batch_2/data.csv`)

```text
sample_num,sample_name
3
4,delta
```

#### Converted rows: run `default` (`file_glob=*/*.csv`, `table_name=samples`)

```json
[
  {
    "sample_num": 1,
    "sample_name": "alpha"
  },
  {
    "sample_num": 2,
    "sample_name": "beta"
  },
  {
    "sample_num": 4,
    "sample_name": "delta"
  }
]
```

#### Rejected rows (`_rejected`): run `default` (`file_glob=*/*.csv`, `table_name=samples`)

```json
[
  {
    "table_name": "samples",
    "source_file": "batch_2/data.csv",
    "line_no": null,
    "raw_record": null,
    "error_detail": "1 out of 2 records invalid."
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/06_multi_file_glob_and_subdirs/expected_schema.py))

```python
"""Expected PyArrow schema for 06_multi_file_glob_and_subdirs."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("sample_num", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("sample_name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
```

---

<a id="case-spec-07-table-transforms-and-output-schemas"></a>

### Table Transforms, Row Filtering, Multi-Table Fan-Out, and Output Schemas (`spec/07_table_transforms_and_output_schemas`)

**Rules:** [R-XSV-DUCKDB-TYPES](#r-xsv-duckdb-types), [R-XSV-TRANSFORMS](#r-xsv-transforms)

- Configures `transforms_module="specs.pipelines.xsv.transforms"` and `output_schemas_module="specs.pipelines.xsv.transforms"` for `table_name="readings"`.
- Because a transform is registered for `"readings"`, `read_validated_rows` yields Python row dicts (`use_pyarrow=False`) to `route_and_coerce_record` and `DictBuffer`.
- The transform drops rows with `status == "drop"` (`s-2`), uppercases `sensor_id`, computes `kelvin = round(celsius + 273.15, 2)` for `"readings"`, and fans out rows with `celsius >= 100.0` (`s-3`) to `"alerts"`, coercing both tables against `OUTPUT_SCHEMAS`.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SensorReadingsInput",
  "type": "object",
  "required": [
    "sensor_id",
    "celsius",
    "status"
  ],
  "properties": {
    "sensor_id": {
      "type": "string"
    },
    "celsius": {
      "type": "number"
    },
    "status": {
      "type": "string"
    }
  },
  "x-xsv-config": {
    "x-delimiter": ",",
    "x-has-header": true
  }
}
```

#### Input (`data.csv`)

```text
sensor_id,celsius,status
s-1,25.0,ok
s-2,999.0,drop
s-3,120.5,ok
```

#### Converted rows: run `with_transforms` (`table_name=readings`, `transforms_module=specs.pipelines.xsv.transforms`, `output_schemas_module=specs.pipelines.xsv.transforms`)

```json
[
  {
    "sensor_id": "S-1",
    "celsius": 25.0,
    "kelvin": 298.15
  },
  {
    "sensor_id": "S-3",
    "celsius": 120.5,
    "kelvin": 393.65
  }
]
```

#### Converted rows (`alerts`): run `with_transforms` (`table_name=readings`, `transforms_module=specs.pipelines.xsv.transforms`, `output_schemas_module=specs.pipelines.xsv.transforms`)

```json
[
  {
    "sensor_id": "S-3",
    "celsius": 120.5
  }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/07_table_transforms_and_output_schemas/expected_schema.py))

```python
"""Expected default PyArrow schema (from schema.json before output_schemas override) for 07_table_transforms_and_output_schemas."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("celsius", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("status", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
```

---

## 4. Errors (Unsupported / Invalid Input)

Schema generation raises a subclass of `XsvSchemaError` (itself a `RuntimeError`).

| Code | Exception | Condition | Reason | Fixtures |
| :--- | :--- | :--- | :--- | :--- |
| **E1** | `UnsupportedXsvDialectError` | Missing or unsupported `$schema` dialect | Only JSON Schema Draft 2020-12 and the KBase Draft 2020-12 XSV metaschema are supported. | [`missing_schema_keyword.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/missing_schema_keyword.json): No `$schema` keyword.<br>[`unsupported_dialect.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/unsupported_dialect.json): Declares an unknown `$schema` dialect URI. |
| **E2** | `MissingRequiredColumnsError` | Missing or empty `required` column list | XSV schemas must declare a non-empty list of column headers in the top-level `required` array. | [`missing_required.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/missing_required.json): Top-level `required` keyword is omitted.<br>[`empty_required.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/empty_required.json): Top-level `required` is `[]`. |
| **E3** | `InvalidXsvSchemaDefinitionError` | Invalid JSON Schema or `x-xsv-config` block | The document is not valid JSON Schema or contains an invalid `x-xsv-config` parser block. | [`invalid_xsv_config_delimiter.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/invalid_xsv_config_delimiter.json): `x-xsv-config.x-delimiter` is a two-character string `"::"`.<br>[`invalid_xsv_config_unknown_key.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/invalid_xsv_config_unknown_key.json): `x-xsv-config` contains an unrecognized property `"x-unknown"`.<br>[`invalid_json_schema_type.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/invalid_json_schema_type.json): Property declares `"type": "not-a-valid-type"`. |
| **E4** | `UnresolvableXsvRefError` | Unresolvable `$ref` pointer | All `$ref` pointers in an XSV schema must resolve against local `$defs` before conversion. | [`unresolvable_ref.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/unresolvable_ref.json): Property references missing `#/$defs/MissingDef`. |
| **E5** | `UnsupportedXsvArrowConstructError` | Unsupported PyArrow column construct | Every XSV column must convert to a supported PyArrow type under a top-level `object` schema. | [`non_struct_root.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/non_struct_root.json): Root declares `"type": "array"` despite having `required`.<br>[`unsupported_column_construct.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/errors/unsupported_column_construct.json): Column property uses `"$dynamicRef": "#Node"`. |

## 5. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suites and this document are both driven by it.

| Case | Target CLI | Manifest | Runs |
| :--- | :--- | :--- | :--- |
| [`spec/01_scalars_nullability_and_column_order`](#case-spec-01-scalars-nullability-and-column-order) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/01_scalars_nullability_and_column_order/case.yaml) | default |
| [`spec/02_delimiters_comments_quotes_and_escapes`](#case-spec-02-delimiters-comments-quotes-and-escapes) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/02_delimiters_comments_quotes_and_escapes/case.yaml) | default |
| [`spec/03_missing_header_and_defs_ref`](#case-spec-03-missing-header-and-defs-ref) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/03_missing_header_and_defs_ref/case.yaml) | default |
| [`spec/04_null_regex_and_null_cols`](#case-spec-04-null-regex-and-null-cols) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/04_null_regex_and_null_cols/case.yaml) | default |
| [`spec/05_partial_and_total_validation_rejections`](#case-spec-05-partial-and-total-validation-rejections) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/05_partial_and_total_validation_rejections/case.yaml) | partial_recovery, total_failure |
| [`spec/06_multi_file_glob_and_subdirs`](#case-spec-06-multi-file-glob-and-subdirs) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/06_multi_file_glob_and_subdirs/case.yaml) | default |
| [`spec/07_table_transforms_and_output_schemas`](#case-spec-07-table-transforms-and-output-schemas) | `xsv` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/pipelines/xsv/spec/07_table_transforms_and_output_schemas/case.yaml) | with_transforms |
