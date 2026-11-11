# AGENTS.md

See `.agents/skills/coding-style` and `.agents/skills/testing` for rules that apply to all code edits.

## Documentation Sync Rules (Mandatory)

Every code change that alters CLI flags, settings classes, public APIs, schemas, or converter behavior MUST update documentation in the same task before completion:

1. **CLI & Settings Changes (`core/settings.py`, `core/fields.py`, `core/transforms.py`, `pipelines/**/settings.py`, `pyproject.toml`)**:
   - Regenerate CLI docs: `uv run python -m scripts.gen_docs`
   - Search `docs/` and `README.md` for any renamed/removed field names, CLI flags (`--*`), or env vars (`CDL_*`) and update all prose/examples.
2. **Converter Rules, Scalar Maps, or Error Classes (`converters/`, `readers/xsd.py`, `specs/schema_converters/*/rules.yaml`)**:
   - Regenerate converter specs: `uv run python -m scripts.gen_converter_spec`
   - If adding a new converter or specification, register it in `scripts/gen_converter_spec.py`, `mkdocs.yml` (`nav`), `docs/index.md`, and `README.md`.
   - See `docs/schema_converter_specs_guide.md` for creating or editing rules, error codes (`E1..En`), and fixture manifests.
3. **Pipeline Specifications & Multi-Environment Smoke Tests (`specs/pipelines/`, `tests/pipeline_spec.py`, `tests/pipeline_env.py`, `tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py`)**:
   - Regenerate pipeline specs: `uv run python -m scripts.gen_pipeline_spec` (or `uv run python -m scripts.gen_converter_spec`)
   - If adding a new pipeline specification, register it in `scripts/gen_pipeline_spec.py`, `mkdocs.yml` (`nav`), `docs/index.md`, and `README.md`.
   - See `docs/pipeline_specs_guide.md` and `docs/pipeline_e2e_testing_guide.md` for creating or editing pipeline rules, error scenarios (`_rejected` and `expected_report`), and running smoke tests across environments (`--dlt-config-dir`, `--settings-file`, `--drop-existing-tables`, `--drop-tables-after`).
4. **Public Symbol / Registry Refactors (`core/`, `pipelines/`, `readers/`, `converters/`, `parsers/`, `utils/`)**:
   - Update module and class/function docstrings in the modified files.
   - Grep `docs/` and `README.md` for all references to modified or removed symbols and update code examples.
5. **Pre-Completion Verification**:
   - Always run `uv run pytest -m docs` and `uv run mkdocs build --strict` before finishing any task that touches `src/`, `scripts/`, `pyproject.toml`, or `docs/`.

## Gotchas

- dlt 1.30 DuckDB destination fails with a Binder Error "Ambiguous reference to catalog or schema" when the db filename stem equals the dataset name; keep the db filename distinct from both pipeline and dataset names.
- dlt 1.30: `AnyDestination` is a quoted TypeAlias (a str at runtime), so `AnyDestination | None` in an evaluated annotation raises TypeError on import. Use `TDestinationReferenceArg` from `dlt.common.destination.reference`.
- pyiceberg SqlCatalog engines leak sqlite connections; use the context-manager protocol (`with load_catalog(...) as catalog`) so `close()` disposes the engine.
- pipelines are run on the KBase CTS (Task Service), which only saves the contents of the output directory (or dlt destination) if the container exits with exit code 0. For any other error code, the container STDERR and STDOUT are saved, but nothing else. As a result, even if pipelines experience failure (e.g. `load_info.has_failed_packages` is true), the container must exit 0 to ensure that logs, parsed data, and other artefacts are not deleted. Errors that occur before a pipeline is run (e.g. argument parsing errors) are not caught, so they should be picked up by the container STDERR/STDOUT.
