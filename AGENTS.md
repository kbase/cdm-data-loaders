# AGENTS.md

See `.agents/skills/coding-style` and `.agents/skills/testing` for rules that apply to all code edits.

## Documentation Sync Rules (Mandatory)

Every code change that alters CLI flags, settings classes, public APIs, schemas, or converter behavior MUST update documentation in the same task before completion:

1. **CLI and Settings Changes (`core/settings.py`, `core/fields.py`, `core/transforms/`, `pipelines/**/settings.py`, `pyproject.toml`)**:
   - `docs/cli_reference.md` and `docs/bundled_schemas.md` are `.gitignore`d macro stubs materialized by `uv run python -m docs_macros` and rendered at build/serve time via `docs_macros` (`docs/_templates/pages/cli_reference.md.j2` and `docs/_templates/pages/bundled_schemas.md.j2`).
   - Search `docs/` and `README.md` for any renamed/removed field names, CLI flags (`--*`), or env vars (`CDL_*`) and update all prose/examples.
2. **Converter Rules, Scalar Maps, or Error Classes (`converters/`, `validation/xsd/`, `specs/schema_converters/*/rules.yaml`)**:
   - Converter specification pages render at build/serve time via `docs_macros` (`docs/_templates/pages/converter_spec.md.j2` and `docs/_templates/tables/*.md.j2`).
   - Converter suites under `specs/schema_converters/<xxx_to_yyy>/` are auto-discovered by `discover_converter_specs()` in `docs_macros/specs/discovery.py`, and `uv run python -m docs_macros` materializes their `.gitignore`d `docs/<xxx_to_yyy>_specification.md` stubs before `zensical build` / `zensical serve`. When adding or removing a converter specification, register/update it in `zensical.toml` (`nav`) and `docs/index.md`.
   - See `docs/schema_converter_specs_guide.md` for creating or editing rules, error codes (`E1..En`), and fixture manifests.
3. **Pipeline Specifications and Multi-Environment Smoke Tests (`specs/pipelines/`, `specs/_loader/pipeline_spec.py`, `tests/helpers/pipeline_env.py`, `tests/e2e/test_pipeline_smoke_spec.py`)**:
   - Pipeline specification pages render at build/serve time via `docs_macros` (`docs/_templates/pages/pipeline_spec.md.j2` and `docs/_templates/tables/*.md.j2`), with `.gitignore`d `docs/<name>_specification.md` stubs materialized by `uv run python -m docs_macros`.
   - Pipeline suites under `specs/pipelines/<name>/` are auto-discovered by `discover_pipeline_specs()` in `docs_macros/specs/discovery.py` from `rules.yaml` front-matter (`pipeline`, `cli_command`, `settings_class`, `pipeline_runner`, `presets`, `use_converter_base_cases`). When adding or removing a pipeline specification, register/update it in `zensical.toml` (`nav`) and `docs/index.md`.
   - See `docs/pipeline_specs_guide.md` and `docs/pipeline_e2e_testing_guide.md` for creating or editing pipeline rules, error scenarios (`_rejected` and `expected_report`), and running smoke tests across environments (`--dlt-config-dir`, `--settings-file`, `--drop-existing-tables`, `--drop-tables-after`).
4. **Public Symbol / Registry Refactors (`core/`, `pipelines/`, `readers/`, `converters/`, `coercion/`, `validation/`, `datasets/`, `io/`, `cli/`, `parsers/`, `utils/`)**:
   - Update module and class/function docstrings in the modified files.
   - Grep `docs/` and `README.md` for all references to modified or removed symbols and update code examples.
5. **Pre-Completion Verification**:
   - Run `uv run pytest -m docs`, `uv run python -m docs_macros`, `uv run --group docs zensical build --clean --strict`, and `uv run python -m docs_macros --clean` before finishing any task that touches `src/`, `docs_macros/`, `scripts/`, `specs/`, `pyproject.toml`, or `docs/`.

## Gotchas

- dlt 1.30 DuckDB destination fails with a Binder Error "Ambiguous reference to catalog or schema" when the db filename stem equals the dataset name; keep the db filename distinct from both pipeline and dataset names.
- dlt 1.30: `AnyDestination` is a quoted TypeAlias (a str at runtime), so `AnyDestination | None` in an evaluated annotation raises TypeError on import. Use `TDestinationReferenceArg` from `dlt.common.destination.reference`.
- pyiceberg SqlCatalog engines leak sqlite connections; use the context-manager protocol (`with load_catalog(...) as catalog`) so `close()` disposes the engine.
- pipelines are run on the KBase CTS (CDM Task Service), which only saves the contents of the output directory (or dlt destination) if the container exits with exit code 0. For any other error code, the container STDERR and STDOUT are saved, but nothing else. As a result, even if pipelines experience failure (e.g. `load_info.has_failed_packages` is true), the container must exit 0 to ensure that logs, parsed data, and other artefacts are not deleted. Errors that occur before a pipeline is run (e.g. argument parsing errors) are not caught, so they should be picked up by the container STDERR/STDOUT.
