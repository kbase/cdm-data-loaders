"""Load pipeline specification manifests, per-case runs, and expected output tables."""

import json
import sys
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field

from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from specs._loader.yaml_io import CASE_MANIFEST, load_settings_file, load_spec_module, load_yaml_dict

RELATIVE_PATH_SETTINGS_KEYS: Final[frozenset[str]] = frozenset({"xsd_file", "schema_file", "log_config_file"})


class ExpectedReport(BaseModel):
    """Expected contents of ``report.txt`` written by ``save_pipeline_metadata``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: str = "completed"
    ingested_files: tuple[str, ...] = ()
    load_errors: tuple[str, ...] = ()
    pipeline_load_info_errs: str | None = None


class PipelineRun(BaseModel):
    """One execution of a pipeline case: input path, settings, and expected outputs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    input_dir: Path
    input_subdir: str | None = None
    settings_file: Path | None = None
    settings: dict[str, Any] = Field(default_factory=dict)
    expected_report: ExpectedReport = Field(default_factory=ExpectedReport)
    expected_counts: dict[str, int] = Field(default_factory=dict)
    expected_tables: Path | None = None
    expected_json: Path | None = None
    tables_attr: str = "EXPECTED_TABLES"
    primary_table: str | None = None
    expected_rejected: Path | None = None
    extra_expected_json: dict[str, Path] = Field(default_factory=dict)
    smoke: bool = True


class PipelineCase(BaseModel):
    """A pipeline specification fixture directory with its manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pipeline: str
    name: str
    directory: Path
    title: str
    rules: tuple[str, ...]
    notes: str
    runs: tuple[PipelineRun, ...]
    family: str = ""
    suite: str = "spec"

    @property
    def is_preset(self) -> bool:
        """Return whether this case targets a preset subclass rather than the generic base pipeline.

        :return: ``True`` if ``suite`` begins with ``"presets/"``.
        :rtype: bool
        """
        return self.suite.startswith("presets/")

    @property
    def anchor(self) -> str:
        """Return the stable HTML anchor for the case.

        :return: Lowercase kebab-case HTML anchor string.
        :rtype: str
        """
        return f"case-{self.suite}-{self.name}".replace("/", "-").replace("_", "-").lower()


def _load_expected_report(raw: dict[str, Any] | None) -> ExpectedReport:
    """Build an ``ExpectedReport`` from an optional YAML mapping.

    :param raw: Optional ``expected_report`` mapping from ``case.yaml``.
    :type raw: dict[str, Any] | None
    :return: Parsed ``ExpectedReport``.
    :rtype: ExpectedReport
    """
    if not raw:
        return ExpectedReport()
    return ExpectedReport(
        status=str(raw.get("status", "completed")),
        ingested_files=tuple(str(item) for item in raw.get("ingested_files", [])),
        load_errors=tuple(str(item) for item in raw.get("load_errors", [])),
        pipeline_load_info_errs=raw.get("pipeline_load_info_errs"),
    )


def _resolve_relative_path_settings(directory: Path, settings_dict: dict[str, Any]) -> dict[str, Any]:
    """Resolve relative file path fields against the case directory.

    :param directory: Case directory on disk.
    :type directory: Path
    :param settings_dict: Merged settings dictionary.
    :type settings_dict: dict[str, Any]
    :return: Settings dictionary with relative file paths resolved when present on disk.
    :rtype: dict[str, Any]
    """
    resolved = dict(settings_dict)
    for key in RELATIVE_PATH_SETTINGS_KEYS:
        val = resolved.get(key)
        if isinstance(val, str) and val and not Path(val).is_absolute():
            candidate = (directory / val).resolve()
            if candidate.exists():
                resolved[key] = str(candidate)
    return resolved


def _derive_spec_run_settings(
    pipeline: str,
    directory: Path,
    case_raw: dict[str, Any],
    raw_run: dict[str, Any],
    data_path: Path,
) -> tuple[dict[str, Any], str | None, str | None]:
    """Derive ``(settings, primary_table, input_subdir)`` for a ``spec/`` run using ``data`` and ``params``.

    :param pipeline: Pipeline identifier.
    :type pipeline: str
    :param directory: Case directory on disk.
    :type directory: Path
    :param case_raw: Raw case-level YAML mapping.
    :type case_raw: dict[str, Any]
    :param raw_run: Raw run-level YAML mapping.
    :type raw_run: dict[str, Any]
    :param data_path: Resolved path to input data file or directory.
    :type data_path: Path
    :return: Tuple of ``(derived_settings, primary_table, input_subdir)``.
    :rtype: tuple[dict[str, Any], str | None, str | None]
    """
    params = dict(raw_run.get("params") or {})
    schema_rel = case_raw.get("schema")
    schema_path = (directory / str(schema_rel)).resolve() if schema_rel else None

    if pipeline == "xmltopyarrow":
        if "xml_level" not in params and "xml_tag" not in params:
            xml_tag = raw_run.get("xml_tag", case_raw.get("xml_tag"))
            if xml_tag is not None:
                params["xml_tag"] = xml_tag
        if data_path.is_file() and "file_glob" not in params:
            params["file_glob"] = data_path.name
        primary_table = params.get("table_name") or raw_run.get("xml_tag") or case_raw.get("xml_tag")
        settings: dict[str, Any] = {
            "dataset_name": "xml_spec_dataset",
            "loader_file_format": "parquet",
            **({"xsd_file": str(schema_path)} if schema_path else {}),
            **params,
        }
        return settings, (str(primary_table) if primary_table else None), None

    if pipeline == "jsonlines_jsonschema":
        entity_type = params.pop("entity_type", case_raw.get("xml_tag"))
        params.pop("extra_entities", None)
        primary_table = str(entity_type) if entity_type else None
        input_subdir = primary_table if data_path.is_file() else None
        if "entity_schemas" not in params:
            params["entity_schemas"] = "specs.pipelines.jsonlines_jsonschema.sample_registry"
        settings = {
            "dataset_name": "jsonl_spec_dataset",
            "loader_file_format": "parquet",
            **params,
        }
        return settings, primary_table, input_subdir

    if pipeline == "xsv":
        table_name = str(params.get("table_name", "spec_table"))
        if data_path.is_file() and "file_glob" not in params:
            params["file_glob"] = data_path.name
        settings = {
            "dataset_name": "xsv_spec_dataset",
            "loader_file_format": "parquet",
            "table_name": table_name,
            **({"schema_file": str(schema_path)} if schema_path else {}),
            **params,
        }
        return settings, table_name, None

    return params, None, None


def load_expected_json_tables(run: PipelineRun) -> dict[str, list[dict[str, Any]]]:
    """Load expected rows keyed by destination table name for ``run``.

    :param run: Pipeline run specification.
    :type run: PipelineRun
    :raises TypeError: If the JSON payload is neither a dictionary nor a list.
    :return: Mapping of table name to expected row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if run.expected_json is None or not run.expected_json.is_file():
        return {}
    loaded = json.loads(run.expected_json.read_text(encoding="utf-8"))
    if isinstance(loaded, dict):
        return {str(k): list(v) for k, v in loaded.items()}
    if not isinstance(loaded, list):
        msg = f"Expected dict or list in {run.expected_json}, got {type(loaded).__name__}"
        raise TypeError(msg)

    result: dict[str, list[dict[str, Any]]] = {}
    if loaded and run.primary_table:
        result[run.primary_table] = loaded
    for extra_table, extra_path in run.extra_expected_json.items():
        extra_rows = json.loads(extra_path.read_text(encoding="utf-8"))
        if extra_rows:
            result[extra_table] = list(extra_rows)
    if run.expected_rejected is not None and run.expected_rejected.is_file():
        rejected_rows = json.loads(run.expected_rejected.read_text(encoding="utf-8"))
        if rejected_rows:
            result["_rejected"] = list(rejected_rows)
    return result


class _CaseRunContext(BaseModel):
    """Case-level defaults shared across all runs in one ``case.yaml`` manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pipeline: str
    directory: Path
    case_raw: dict[str, Any]
    case_settings_file: Path | None
    case_settings: dict[str, Any]


def _merge_run_settings_and_paths(
    ctx: _CaseRunContext,
    raw: dict[str, Any],
) -> tuple[Path | None, dict[str, Any], Path, str | None, str | None, str | None, str, bool]:
    """Resolve merged settings, input path, and expected-tables metadata for one pipeline run.

    :param ctx: Case-level context shared across runs.
    :type ctx: _CaseRunContext
    :param raw: Raw run YAML dictionary.
    :type raw: dict[str, Any]
    :return: Resolved settings file, settings dict, input path, primary table, input subdir, tables relpath, attr, smoke flag.
    :rtype: tuple[Path | None, dict[str, Any], Path, str | None, str | None, str | None, str, bool]
    """
    run_settings_file_rel = raw.get("settings_file")
    resolved_settings_file = (
        (ctx.directory / str(run_settings_file_rel)).resolve()
        if run_settings_file_rel is not None
        else ctx.case_settings_file
    )
    merged_settings: dict[str, Any] = {}
    if resolved_settings_file is not None:
        merged_settings.update(load_settings_file(resolved_settings_file))
    merged_settings.update(ctx.case_settings)

    if "data" in raw:
        data_path = (ctx.directory / str(raw["data"])).resolve()
        derived_settings, primary_table, input_subdir = _derive_spec_run_settings(
            ctx.pipeline,
            ctx.directory,
            ctx.case_raw,
            raw,
            data_path,
        )
        merged_settings.update(derived_settings)
        expected_tables_rel: str | None = raw.get("expected_table", "expected_table.py")
        tables_attr = str(raw.get("table_attr", "EXPECTED_TABLE"))
        smoke_default = "expected_report" in raw
    else:
        data_path = (ctx.directory / str(raw.get("input_dir", "input"))).resolve()
        primary_table = None
        input_subdir = None
        expected_tables_rel = raw.get("expected_tables", "expected_tables.py")
        tables_attr = str(raw.get("tables_attr", "EXPECTED_TABLES"))
        smoke_default = True

    if "settings" in raw and isinstance(raw["settings"], dict):
        merged_settings.update(raw["settings"])
    merged_settings = _resolve_relative_path_settings(ctx.directory, merged_settings)
    return (
        resolved_settings_file,
        merged_settings,
        data_path,
        primary_table,
        input_subdir,
        expected_tables_rel,
        tables_attr,
        smoke_default,
    )


def _load_pipeline_run(ctx: _CaseRunContext, raw: dict[str, Any]) -> PipelineRun:
    """Build one ``PipelineRun`` from a raw ``case.yaml`` run entry.

    :param ctx: Case-level context shared across runs.
    :type ctx: _CaseRunContext
    :param raw: Raw run YAML dictionary.
    :type raw: dict[str, Any]
    :return: Parsed ``PipelineRun``.
    :rtype: PipelineRun
    """
    (
        resolved_settings_file,
        merged_settings,
        data_path,
        primary_table,
        input_subdir,
        expected_tables_rel,
        tables_attr,
        smoke_default,
    ) = _merge_run_settings_and_paths(ctx, raw)

    expected_json_rel = raw.get("expected_json", "expected.json")
    expected_rejected_rel = raw.get("expected_rejected")
    extra_expected_json = {
        str(k): (ctx.directory / str(v)).resolve() for k, v in (raw.get("extra_expected_json") or {}).items()
    }
    provisional_run = PipelineRun(
        id=str(raw["id"]),
        input_dir=data_path,
        input_subdir=input_subdir,
        settings_file=resolved_settings_file,
        settings=merged_settings,
        expected_report=_load_expected_report(raw.get("expected_report")),
        expected_tables=(ctx.directory / str(expected_tables_rel)).resolve() if expected_tables_rel else None,
        expected_json=(ctx.directory / str(expected_json_rel)).resolve() if expected_json_rel else None,
        tables_attr=tables_attr,
        primary_table=primary_table,
        expected_rejected=(ctx.directory / str(expected_rejected_rel)).resolve() if expected_rejected_rel else None,
        extra_expected_json=extra_expected_json,
        smoke=bool(raw.get("smoke", smoke_default)),
    )
    if "expected_counts" in raw and isinstance(raw["expected_counts"], dict):
        expected_counts = {str(k): int(v) for k, v in raw["expected_counts"].items()}
    else:
        expected_counts = {name: len(rows) for name, rows in load_expected_json_tables(provisional_run).items()}

    return PipelineRun(
        id=provisional_run.id,
        input_dir=provisional_run.input_dir,
        input_subdir=provisional_run.input_subdir,
        settings_file=provisional_run.settings_file,
        settings=provisional_run.settings,
        expected_report=provisional_run.expected_report,
        expected_counts=expected_counts,
        expected_tables=provisional_run.expected_tables,
        expected_json=provisional_run.expected_json,
        tables_attr=provisional_run.tables_attr,
        primary_table=provisional_run.primary_table,
        expected_rejected=provisional_run.expected_rejected,
        extra_expected_json=provisional_run.extra_expected_json,
        smoke=provisional_run.smoke,
    )


def load_pipeline_case(
    manifest: Path,
    *,
    pipeline: str | None = None,
    family: str = "",
    suite: str = "spec",
) -> PipelineCase:
    """Load one ``case.yaml`` pipeline manifest.

    :param manifest: Path to ``case.yaml``.
    :type manifest: Path
    :param pipeline: Optional explicit pipeline name override.
    :type pipeline: str | None
    :param family: Pipeline family name.
    :type family: str
    :param suite: Suite identifier (e.g. ``"spec"`` or ``"presets/pubmed"``).
    :type suite: str
    :return: Parsed ``PipelineCase``.
    :rtype: PipelineCase
    """
    raw = load_yaml_dict(manifest)
    directory = manifest.parent
    resolved_pipeline = str(raw.get("pipeline") or pipeline or directory.parent.name)
    resolved_family = family or resolved_pipeline
    case_settings_file_rel = raw.get("settings_file")
    case_settings_file = (
        (directory / str(case_settings_file_rel)).resolve() if case_settings_file_rel is not None else None
    )
    ctx = _CaseRunContext(
        pipeline=resolved_pipeline,
        directory=directory,
        case_raw=raw,
        case_settings_file=case_settings_file,
        case_settings=dict(raw.get("settings") or {}),
    )

    return PipelineCase(
        pipeline=resolved_pipeline,
        name=directory.name,
        directory=directory,
        title=str(raw["title"]),
        rules=tuple(str(r) for r in raw.get("rules", [])),
        notes=str(raw.get("notes", "")).strip(),
        runs=tuple(_load_pipeline_run(ctx, raw_run) for raw_run in raw["runs"]),
        family=resolved_family,
        suite=suite,
    )


def load_pipeline_cases(pipeline_spec_dir: Path) -> list[PipelineCase]:
    """Load all ``spec/*/case.yaml``, ``presets/*/*/case.yaml``, and ``*/case.yaml`` manifests under ``pipeline_spec_dir``.

    :param pipeline_spec_dir: Pipeline family specification directory.
    :type pipeline_spec_dir: Path
    :return: Ordered list of ``PipelineCase`` instances.
    :rtype: list[PipelineCase]
    """
    family = pipeline_spec_dir.name
    cases: list[PipelineCase] = []

    spec_dir = pipeline_spec_dir / "spec"
    if spec_dir.is_dir():
        cases.extend(
            load_pipeline_case(manifest, pipeline=family, family=family, suite="spec")
            for manifest in sorted(spec_dir.glob(f"*/{CASE_MANIFEST}"))
        )

    presets_dir = pipeline_spec_dir / "presets"
    if presets_dir.is_dir():
        for preset_dir in sorted(p for p in presets_dir.iterdir() if p.is_dir()):
            suite_name = f"presets/{preset_dir.name}"
            cases.extend(
                load_pipeline_case(
                    manifest,
                    pipeline=preset_dir.name,
                    family=family,
                    suite=suite_name,
                )
                for manifest in sorted(preset_dir.glob(f"*/{CASE_MANIFEST}"))
            )

    cases.extend(
        load_pipeline_case(manifest, pipeline=family, family=family, suite="spec")
        for manifest in sorted(pipeline_spec_dir.glob(f"*/{CASE_MANIFEST}"))
        if manifest.parent.name not in {"spec", "presets"}
    )

    return cases


def _extract_raw_tables(mod: object, run: PipelineRun) -> dict[str, Any]:
    """Extract the raw table mapping from a loaded expected-tables module.

    :param mod: Imported Python fixture module.
    :type mod: object
    :param run: Pipeline run specification.
    :type run: PipelineRun
    :raises TypeError: If the expected attribute is neither a dictionary nor a ``pa.Table``.
    :return: Raw dictionary mapping table names to PyArrow tables.
    :rtype: dict[str, Any]
    """
    if hasattr(mod, "EXPECTED_TABLES") and isinstance(mod.EXPECTED_TABLES, dict):
        return dict(mod.EXPECTED_TABLES)
    attr_val = getattr(mod, run.tables_attr)
    if isinstance(attr_val, dict):
        return dict(attr_val)
    if isinstance(attr_val, pa.Table):
        if run.primary_table and run.primary_table in run.expected_counts and attr_val.num_rows > 0:
            return {run.primary_table: attr_val}
        return {}
    msg = f"Expected dict or pa.Table for {run.tables_attr} in {run.expected_tables}, got {type(attr_val).__name__}"
    raise TypeError(msg)


def load_expected_tables(run: PipelineRun) -> dict[str, pa.Table]:
    """Load the ``dict[str, pa.Table]`` mapping from ``run.expected_tables`` and ``run.expected_rejected``.

    :param run: Pipeline run specification.
    :type run: PipelineRun
    :raises TypeError: If any entry in the loaded mapping is not ``str -> pa.Table``.
    :return: Mapping of destination table names to expected ``pa.Table`` instances.
    :rtype: dict[str, pa.Table]
    """
    if run.expected_tables is None:
        return {}

    previous_schema_mod = sys.modules.get("expected_schema")
    schema_sibling = run.expected_tables.parent / "expected_schema.py"
    if run.expected_tables.stem != "expected_schema" and schema_sibling.is_file():
        sys.modules["expected_schema"] = load_spec_module(schema_sibling)
    try:
        mod = load_spec_module(run.expected_tables)
    finally:
        if previous_schema_mod is None:
            sys.modules.pop("expected_schema", None)
        else:
            sys.modules["expected_schema"] = previous_schema_mod

    raw_tables = _extract_raw_tables(mod, run)

    result: dict[str, pa.Table] = {}
    for table_name, table in raw_tables.items():
        if not isinstance(table_name, str) or not isinstance(table, pa.Table):
            msg = (
                f"Expected {run.tables_attr} in {run.expected_tables} to map str -> pa.Table, "
                f"got {type(table_name).__name__} -> {type(table).__name__}"
            )
            raise TypeError(msg)
        result[table_name] = table

    if run.expected_rejected is not None and run.expected_rejected.is_file() and "_rejected" not in result:
        rejected_rows = json.loads(run.expected_rejected.read_text(encoding="utf-8"))
        if rejected_rows:
            result["_rejected"] = pa.Table.from_pylist(rejected_rows, schema=REJECTED_PYARROW_SCHEMA)

    return result
