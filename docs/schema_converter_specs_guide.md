# Schema Converter Specifications Developer Guide

This guide explains how to author, edit, and maintain specification rules, test cases, error catalogs, and generated reference pages under `specs/schema_converters/`.

---

## 1. Architecture & Directory Layout

Each schema converter (`jsonschema_to_pyarrow`, `pydantic_to_pyarrow`, `xsd_to_pyarrow`) has a specification directory under `specs/schema_converters/<converter>/`:

```text
specs/schema_converters/<converter>/
├── rules.yaml                       # Rule catalogue (sections + rule IDs and statements)
├── spec/
│   ├── 01_<topic>/
│   │   ├── case.yaml                # Case manifest (title, schema, rules, runs)
│   │   ├── schema.<ext>             # Input schema (.json, .py, or .xsd)
│   │   ├── expected_schema.py       # Expected PyArrow Schema (EXPECTED_SCHEMA)
│   │   ├── data.<ext>               # Input records (.json or .xml)
│   │   ├── expected.json            # Expected coerced rows as JSON
│   │   └── expected_table.py        # Expected PyArrow Table (EXPECTED_TABLE)
│   └── errors/
│       ├── manifest.yaml            # Error fixture manifest (file, code, match, description)
│       └── <error_fixture>.<ext>    # Invalid/unsupported schemas triggering E1..En
└── examples/
    └── <real_world_example>/
        └── case.yaml                # Real-world domain schema cases (PubMed, UniProt, NCBI, etc.)
```

The manifests in `specs/schema_converters/` drive two outputs:

1. **Parametrized specification tests**:
   - `tests/cdm_data_loaders/converters/facades/test_jsonschema_to_pyarrow_spec.py`
   - `tests/cdm_data_loaders/converters/facades/test_pydantic_to_pyarrow_spec.py`
   - `tests/cdm_data_loaders/converters/facades/test_xsd_to_pyarrow_spec.py`
2. **Generated Markdown specification pages**:
   - [JSON Schema to PyArrow Specification](jsonschema_to_pyarrow_specification.md)
   - [Pydantic to PyArrow Specification](pydantic_to_pyarrow_specification.md)
   - [XSD to PyArrow Specification](xsd_to_pyarrow_specification.md)

---

## 2. How Rules Are Created and Structured

Rules live in `specs/schema_converters/<converter>/rules.yaml` and are loaded by `load_rules()` in `tests/converter_spec.py`.

```yaml
title: XSD to PyArrow Schema & Record Conversion Specification
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

- **Sections**: Group related rules under a `title` and `id`. A section can include `generated: <key>` to embed a table auto-generated from converter code (such as `xsd_scalar_table`, `jsonschema_scalar_table`, or `pydantic_scalar_table` in `scripts/gen_converter_spec.py`).
- **Rule IDs**: Use stable, descriptive uppercase identifiers prefixed with `R-` (for example, `R-SCALAR-BUILTIN`, `R-NULL-ATTRIBUTE`, `R-REC-DEPTH`).
- **Rule statements**: State one testable conversion contract per rule.
- **Bidirectional enforcement**:
  - `test_manifests_pass_every_rule_is_exercised` verifies that every rule ID in `rules.yaml` appears in the `rules:` list of at least one `case.yaml`.
  - `test_manifests_pass_every_case_rule_exists` verifies that every rule ID listed in any `case.yaml` is defined in `rules.yaml`.
  - `load_rules()` rejects duplicate rule IDs.

---

## 3. How Errors Are Created and Structured

Converter error codes (`E1`, `E2`, ...) are defined as exception subclasses in the converter source code and paired with fixtures in `spec/errors/manifest.yaml`.

### Source Code Error Hierarchy

Each converter defines a base error class and a tuple of numbered subclasses:

| Converter | Source Module | Base Class | Error Tuple |
| :--- | :--- | :--- | :--- |
| `jsonschema_to_pyarrow` | `src/cdm_data_loaders/converters/facades/jsonschema_to_pyarrow.py` | `JSONSchemaToPyArrowError` | `JSONSCHEMA_TO_PYARROW_ERRORS` |
| `pydantic_to_pyarrow` | `src/cdm_data_loaders/converters/facades/pydantic_to_pyarrow.py` | `PydanticToPyArrowError` | `PYDANTIC_TO_PYARROW_ERRORS` |
| `xsd_to_pyarrow` | `src/cdm_data_loaders/converters/facades/xsd_to_pyarrow.py` | `XsdToPyArrowError` | `XSD_TO_PYARROW_ERRORS` |

Each error subclass defines:

- `code`: unique identifier (`"E1"`, `"E2"`, ...).
- `title`: short label rendered in the specification table.
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

- Every class in `*_ERRORS` has a unique `code`, a non-empty `title`, and a docstring.
- Every `code` in `*_ERRORS` has at least one entry in `spec/errors/manifest.yaml`.
- Every entry in `spec/errors/manifest.yaml` references a valid `code` in `*_ERRORS`.
- Every schema file on disk in `spec/errors/` is listed in `spec/errors/manifest.yaml` (`test_manifests_pass_every_error_fixture_listed`).
- Running the converter on each fixture raises the exact subclass `ERRORS_BY_CODE[err.code]` with a message matching `err.match`.

---

## 4. Common Developer Workflows

### Adding a New Rule and Test Case

1. **Add the rule to `rules.yaml`**:
   - Add an entry with `id` and `statement` under the appropriate section in `specs/schema_converters/<converter>/rules.yaml`.
2. **Create or update a case in `spec/<case>/` (or `examples/<case>/`)**:
   - Reference the new rule ID under `rules:` in `case.yaml`.
   - Provide the input schema (`schema.json`, `schema.py`, or `schema.xsd`), `expected_schema.py` (`EXPECTED_SCHEMA`), input data (`data.json` or `*.xml`), `expected.json`, and `expected_table.py` (`EXPECTED_TABLE`).
3. **Regenerate the specification page**:
   ```bash
   uv run python -m scripts.gen_converter_spec
   ```
4. **Run the converter spec and documentation tests**:
   ```bash
   uv run pytest tests/cdm_data_loaders/converters/facades/
   uv run pytest -m docs
   ```

### Adding a New Error Code

1. **Define the exception subclass**:
   - Subclass the converter's base error (`XsdToPyArrowError`, `JSONSchemaToPyArrowError`, or `PydanticToPyArrowError`) with `code = "E<N>"`, `title = "..."`, and a docstring.
   - Append the new class to `XSD_TO_PYARROW_ERRORS`, `JSONSCHEMA_TO_PYARROW_ERRORS`, or `PYDANTIC_TO_PYARROW_ERRORS`.
   - Raise the new subclass in the reader or facade when the invalid construct is detected.
2. **Add an error fixture**:
   - Place a minimal schema file reproducing the error in `specs/schema_converters/<converter>/spec/errors/`.
   - Add an entry in `specs/schema_converters/<converter>/spec/errors/manifest.yaml` with `file`, `code`, `match`, and `description`.
3. **Regenerate the specification page and run tests**:
   ```bash
   uv run python -m scripts.gen_converter_spec
   uv run pytest tests/cdm_data_loaders/converters/facades/
   uv run pytest -m docs
   ```

### Editing or Removing an Existing Rule or Error

- **Editing a rule statement or error docstring**:
  - Update the text in `rules.yaml` or the error subclass docstring/`title`.
  - Run `uv run python -m scripts.gen_converter_spec` to sync `docs/*_specification.md`.
- **Editing expected schema or table outputs**:
  - Update `expected_schema.py`, `expected_table.py`, and `expected.json` together so schema validation and table roundtrip assertions agree.
  - Run `uv run python -m scripts.gen_converter_spec` to update the embedded code blocks in `docs/*_specification.md`.
- **Removing a rule or error**:
  - Remove the rule ID from all `case.yaml` files before deleting it from `rules.yaml`.
  - Remove the fixture file from `spec/errors/` and its entry from `spec/errors/manifest.yaml` when removing an error subclass from `*_ERRORS`.
  - Regenerate docs with `uv run python -m scripts.gen_converter_spec`.

### Adding a New Converter Specification Suite

1. Create `specs/schema_converters/<new_converter>/rules.yaml`, `spec/`, and `spec/errors/manifest.yaml`.
2. Register a `ConverterSpecConfig` entry in `CONFIGS` inside `scripts/gen_converter_spec.py`.
3. Run `uv run python -m scripts.gen_converter_spec` to generate `docs/<new_converter>_specification.md`.
4. Register `docs/<new_converter>_specification.md` in `zensical.toml` (`[project].nav`), `docs/index.md`, and `README.md`.
5. Verify with `uv run pytest -m docs` and `uv run --group docs zensical build --strict`.
