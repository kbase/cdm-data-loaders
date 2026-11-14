# Schema Converter Specifications Developer Guide

This guide explains how to author, edit, and maintain specification rules, test cases, error catalogs, and generated reference pages under [`specs/schema_converters`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters).

---

## Architecture and Directory Layout

Each schema converter suite lives under [`specs/schema_converters/<converter>`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters):

```text
specs/schema_converters/<converter>/
├── rules.yaml                       # Rule catalogue (sections + rule IDs and statements)
├── spec/
│   ├── 01_<topic>/
│   │   ├── case.yaml                # Case manifest (title, schema, rules, runs)
│   │   ├── schema.<ext>             # Input schema (.json, .py, or .xsd)
│   │   ├── expected_schema.<ext>    # Expected schema (.py, .json, or .yaml)
│   │   ├── data.<ext> / table.py    # Input records (.json, .xml, or PyArrow Table .py)
│   │   ├── expected.json            # Expected coerced/serialized rows as JSON
│   │   └── expected_table.py        # Expected PyArrow Table (EXPECTED_TABLE / TABLE)
│   └── errors/
│       ├── manifest.yaml            # Error fixture manifest (file, code, match, description)
│       └── <error_fixture>.<ext>    # Invalid/unsupported schemas triggering E1..En
└── examples/
    └── <real_world_example>/
        └── case.yaml                # Real-world domain schema cases (PubMed, UniProt, NCBI, etc.)
```

The manifests in [`specs/schema_converters`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters) drive both test execution and rendered documentation:

- **Parametrized specification tests**: [`tests/cdm_data_loaders/converters/facades`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/cdm_data_loaders/converters/facades) (`test_*_spec.py`)
- **Rendered Markdown specification pages**:
  - [JSON Schema to PyArrow Specification](jsonschema_to_pyarrow_specification.md)
  - [Pydantic to PyArrow Specification](pydantic_to_pyarrow_specification.md)
  - [XSD to PyArrow Specification](xsd_to_pyarrow_specification.md)
  - [PyArrow to JSON Schema Specification](pyarrow_to_jsonschema_specification.md)
  - [PyArrow to LinkML Specification](pyarrow_to_linkml_specification.md)

---

## How Rules Are Created and Structured

Rules live in `specs/schema_converters/<converter>/rules.yaml` and are loaded by `load_rules()` in [`tests/converter_spec.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/converter_spec.py).

```yaml
title: XSD to PyArrow Schema and Record Conversion Specification
intro: |
  Target specification for `cdm_data_loaders.converters.facades.xsd_to_pyarrow`.

sections:
  - id: scalars
    title: Scalar Type Mapping (`XSD_SCALAR_SPEC` / `SCALAR_ARROW_TYPES`)
    intro: |
      Built-in XSD primitive and derived atomic types map to PyArrow types as follows:
    generated: xsd_scalar_table
    rules:
      - id: R-SCALAR-BUILTIN
        statement: |
          Every built-in atomic `xs:*` type maps to the PyArrow `DataType` in the table above.
```

- **Sections**: Group related rules under a `title` and `id`. A section can include `generated: <key>` to embed a table rendered from converter code via `TABLE_TEMPLATE_REGISTRY` in [`docs_macros/tables_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/tables_context.py) and [`docs/_templates/tables`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates/tables).
- **Rule IDs**: Use stable, descriptive uppercase identifiers prefixed with `R-` (for example, `R-SCALAR-BUILTIN`, `R-NULL-ATTRIBUTE`, `R-REC-DEPTH`).
- **Rule statements**: State one testable conversion contract per rule.
- **Bidirectional enforcement**:
  - `test_manifests_pass_every_rule_is_exercised` verifies that every rule ID in `rules.yaml` appears in the `rules:` list of at least one `case.yaml`.
  - `test_manifests_pass_every_case_rule_exists` verifies that every rule ID listed in any `case.yaml` is defined in `rules.yaml`.
  - `load_rules()` rejects duplicate rule IDs.

---

## How Errors Are Created and Structured

Converter error codes (`E1`, `E2`, ...) are defined as `ConversionError` subclasses in [`src/cdm_data_loaders/converters/facades`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades) and paired with fixtures in `spec/errors/manifest.yaml`.

### Error Tuple Convention

Each facade module `cdm_data_loaders.converters.facades.<converter>` exports a `<CONVERTER>_ERRORS` tuple of numbered exception subclasses, which `discover_converter_specs()` in [`docs_macros/spec_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/spec_context.py) introspects automatically when rendering the error catalog section. Each error subclass defines:

- `code`: unique identifier (`"E1"`, `"E2"`, ...).
- `title`: short condition label rendered in the error specification.
- Class docstring (`__doc__`): explanation of why the converter rejects the construct.

### Error Manifest (`spec/errors/manifest.yaml`)

Each entry in `spec/errors/manifest.yaml` maps a fixture file in `spec/errors/` to an error code and message regex:

```yaml
errors:
  - file: wildcard_any_element.xsd
    xml_tag: entry
    code: E4
    match: xs:any
    description: "`<xs:sequence><xs:any processContents=\"lax\"/></xs:sequence>`"
```

### Error Invariants Enforced by Tests

- Every class in `<CONVERTER>_ERRORS` has a unique `code`, a non-empty `title`, and a docstring.
- Every `code` in `<CONVERTER>_ERRORS` has at least one entry in `spec/errors/manifest.yaml`.
- Every entry in `spec/errors/manifest.yaml` references a valid `code` in `<CONVERTER>_ERRORS`.
- Every schema file on disk in `spec/errors/` is listed in `spec/errors/manifest.yaml` (`test_manifests_pass_every_error_fixture_listed`).
- Running the converter on each fixture raises the exact subclass `ERRORS_BY_CODE[err.code]` with a message matching `err.match`.

---

## Common Developer Workflows

Specification pages render dynamically at build and serve time from [`specs/schema_converters`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters) via Zensical's `macros` plugin ([`docs_macros/spec_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/spec_context.py) and [`docs/_templates/pages/converter_spec.md.j2`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates/pages/converter_spec.md.j2)). Autogenerated stub entrypoints (`docs/*_specification.md`) are `.gitignore`d and materialized before `zensical build` or `zensical serve` by [`docs_macros/__main__.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/__main__.py) (`uv run python -m docs_macros`) and removed with `uv run python -m docs_macros --clean`.

### Adding a New Rule and Test Case

- **Add the rule to `rules.yaml`**:
  - Add an entry with `id` and `statement` under the appropriate section in `specs/schema_converters/<converter>/rules.yaml`.
- **Create or update a case in `spec/<case>/` (or `examples/<case>/`)**:
  - Reference the new rule ID under `rules:` in `case.yaml`.
  - Provide the input schema (`schema.json`, `schema.py`, or `schema.xsd`), `expected_schema.py` (`EXPECTED_SCHEMA`), input data (`data.json` or `*.xml`), `expected.json`, and `expected_table.py` (`EXPECTED_TABLE`).
- **Run the converter spec, template, and site build checks**:
  ```sh
  uv run pytest tests/cdm_data_loaders/converters/facades/
  uv run pytest -m docs
  uv run python -m docs_macros
  uv run --group docs zensical build --clean --strict
  uv run python -m docs_macros --clean
  ```

### Adding a New Error Code

- **Define the exception subclass**:
  - Subclass the converter's base error class in `src/cdm_data_loaders/converters/facades/<converter>.py` with `code = "E<N>"`, `title = "..."`, and a docstring.
  - Append the new class to `<CONVERTER>_ERRORS` and raise it in the reader, emitter, or facade when the invalid construct is detected.
- **Add an error fixture**:
  - Place a minimal schema file reproducing the error in `specs/schema_converters/<converter>/spec/errors/`.
  - Add an entry in `specs/schema_converters/<converter>/spec/errors/manifest.yaml` with `file`, `code`, `match`, and `description`.
- **Run converter spec, template, and site build checks**:
  ```sh
  uv run pytest tests/cdm_data_loaders/converters/facades/
  uv run pytest -m docs
  uv run python -m docs_macros
  uv run --group docs zensical build --clean --strict
  uv run python -m docs_macros --clean
  ```

### Editing or Removing an Existing Rule or Error

- **Editing a rule statement or error docstring**:
  - Update the text in `rules.yaml` or the error subclass docstring and `title`; `docs/*_specification.md` picks up the change automatically at render time.
- **Editing expected schema or table outputs**:
  - Update `expected_schema.py`, `expected_table.py`, and `expected.json` together so schema validation and table roundtrip assertions agree.
- **Removing a rule or error**:
  - Remove the rule ID from all `case.yaml` files before deleting it from `rules.yaml`.
  - Remove the fixture file from `spec/errors/` and its entry from `spec/errors/manifest.yaml` when removing an error subclass from `<CONVERTER>_ERRORS`.
  - Verify with `uv run pytest -m docs`, `uv run python -m docs_macros`, `uv run --group docs zensical build --clean --strict`, and `uv run python -m docs_macros --clean`.

### Adding a New Converter Specification Suite

- Create `specs/schema_converters/<new_converter>/rules.yaml`, `spec/`, and `spec/errors/manifest.yaml` (where `<new_converter>` matches `cdm_data_loaders.converters.facades.<new_converter>` and `<NEW_CONVERTER>_ERRORS`).
- If the converter introduces a new `generated:` reference table, register its column headers and row builder in `TABLE_TEMPLATE_REGISTRY` inside [`docs_macros/tables_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/tables_context.py) (rendered via [`docs/_templates/tables/reference_table.md.j2`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates/tables/reference_table.md.j2)). (`CONVERTER_SPECS` in [`docs_macros/spec_context.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros/spec_context.py) auto-discovers `<new_converter>` from `specs/schema_converters/` and its facade module.)
- Add `<new_converter>_specification.md` under `"Specifications"` in [`zensical.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/zensical.toml) (`[project].nav`) and link it under `## Specifications` in [`docs/index.md`](index.md) (`docs/<new_converter>_specification.md` itself is `.gitignore`d and generated on demand from [`docs/_templates/pages/macro_stub.md.j2`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates/pages/macro_stub.md.j2)).
- Verify with `uv run pytest -m docs`, `uv run python -m docs_macros`, `uv run --group docs zensical build --clean --strict`, and `uv run python -m docs_macros --clean`.
