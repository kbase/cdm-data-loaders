# AGENTS.md

See `.agents/rules/` for rules that apply to all code edits.

## Gotchas

- dlt 1.30 DuckDB destination fails with a Binder Error "Ambiguous reference to catalog or schema" when the db filename stem equals the dataset name; keep the db filename distinct from both pipeline and dataset names.
- dlt 1.30: `AnyDestination` is a quoted TypeAlias (a str at runtime), so `AnyDestination | None` in an evaluated annotation raises TypeError on import. Use `TDestinationReferenceArg` from `dlt.common.destination.reference`.
- pyiceberg SqlCatalog engines leak sqlite connections; use the context-manager protocol (`with load_catalog(...) as catalog`) so `close()` disposes the engine.
- pipelines are run on the KBase CTS (Task Service), which only saves the contents of the output directory (or dlt destination) if the container exits with exit code 0. For any other error code, the container STDERR and STDOUT are saved, but nothing else. As a result, even if pipelines experience failure (e.g. `load_info.has_failed_packages` is true), the container must exit 0 to ensure that logs, parsed data, and other artefacts are not deleted. Errors that occur before a pipeline is run (e.g. argument parsing errors) are not caught, so they should be picked up by the container STDERR/STDOUT.
