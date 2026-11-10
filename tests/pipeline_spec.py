"""Load pipeline smoke test specification manifests: rule catalogues, per-case manifests, and expected tables.

A pipeline family specification lives under ``specs/pipelines/<family_name>/``:

- ``rules.yaml``: rule catalogue (sections of rule IDs and statements covering the base pipeline and its presets).
- ``spec/<case_dir>/case.yaml``: synthetic specification cases for the generic base pipeline.
- ``presets/<preset_name>/<case_dir>/case.yaml``: specification cases for preset ``Settings`` subclasses.
- ``<case_dir>/settings.toml`` (or ``.yaml`` / ``.json``): optional per-case settings configuration file.
- ``<case_dir>/expected_tables.py`` (or ``expected_table.py``): expected PyArrow table(s).
- ``<case_dir>/expected.json``: expected JSON representation of output table rows.

The same manifests drive the pipeline unit/smoke tests and the generated pipeline specification documents.
"""

import json
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import yaml

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.converter_spec import CASE_MANIFEST
from tests.pyarrow_spec_helpers import load_module

RELATIVE_PATH_SETTINGS_KEYS: Final[frozenset[str]] = frozenset({"xsd_file", "schema_file", "log_config_file"})


@dataclass(frozen=True, slots=True)
class ExpectedReport:
    """Expected contents of ``report.txt`` written by ``save_pipeline_metadata``."""

    status: str = "completed"
    ingested_files: tuple[str, ...] = ()
    load_errors: tuple[str, ...] = ()
    pipeline_load_info_errs: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineRun:
    """One execution of a pipeline case: input path, settings, and expected outputs."""

    id: str
    input_dir: Path
    input_subdir: str | None = None
    settings_file: Path | None = None
    settings: dict[str, Any] = field(default_factory=dict)
    expected_report: ExpectedReport = field(default_factory=ExpectedReport)
    expected_counts: dict[str, int] = field(default_factory=dict)
    expected_tables: Path | None = None
    expected_json: Path | None = None
    tables_attr: str = "EXPECTED_TABLES"
    primary_table: str | None = None
    expected_rejected: Path | None = None
    extra_expected_json: dict[str, Path] = field(default_factory=dict)
    smoke: bool = True


@dataclass(frozen=True, slots=True)
class PipelineCase:
    """A pipeline specification fixture directory with its manifest."""

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
        """Return whether this case targets a preset subclass rather than the generic base pipeline."""
        return self.suite.startswith("presets/")

    @property
    def anchor(self) -> str:
        """Return the stable HTML anchor for the case."""
        return f"case-{self.suite}-{self.name}".replace("/", "-").replace("_", "-").lower()


def _load_yaml_dict(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)
    if not isinstance(loaded, dict):
        msg = f"{path}: expected a mapping at the top level"
        raise TypeError(msg)
    return loaded


def load_settings_file(path: Path) -> dict[str, Any]:
    """Load a settings configuration dictionary from a TOML, YAML, or JSON file.

    :param path: path to a ``.toml``, ``.yaml``, ``.yml``, or ``.json`` settings file
    :type path: Path
    :raises ValueError: if the file extension is unsupported
    :raises TypeError: if the parsed document is not a mapping
    :return: dictionary of settings values
    :rtype: dict[str, Any]
    """
    suffix = path.suffix.lower()
    if suffix not in {".toml", ".yaml", ".yml", ".json"}:
        msg = f"Unsupported settings file extension {suffix!r} for {path}; expected .toml, .yaml, .yml, or .json"
        raise ValueError(msg)
    text = path.read_text(encoding="utf-8")
    if suffix == ".toml":
        loaded: Any = tomllib.loads(text)
    elif suffix in {".yaml", ".yml"}:
        loaded = yaml.safe_load(text)
    else:
        loaded = json.loads(text)

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        msg = f"{path}: expected a mapping at the top level, got {type(loaded).__name__}"
        raise TypeError(msg)
    return dict(loaded)


def _load_expected_report(raw: dict[str, Any] | None) -> ExpectedReport:
    if not raw:
        return ExpectedReport()
    return ExpectedReport(
        status=str(raw.get("status", "completed")),
        ingested_files=tuple(str(item) for item in raw.get("ingested_files", [])),
        load_errors=tuple(str(item) for item in raw.get("load_errors", [])),
        pipeline_load_info_errs=raw.get("pipeline_load_info_errs"),
    )


def _resolve_relative_path_settings(directory: Path, settings_dict: dict[str, Any]) -> dict[str, Any]:
    """Resolve relative file path fields against the case directory."""
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
    """Derive ``(settings, primary_table, input_subdir)`` for a ``spec/`` run using ``data`` and ``params``."""
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
    """Load expected rows keyed by destination table name for ``run``."""
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


def _load_pipeline_run(
    pipeline: str,
    directory: Path,
    case_raw: dict[str, Any],
    raw: dict[str, Any],
    case_settings_file: Path | None,
    case_settings: dict[str, Any],
) -> PipelineRun:
    run_settings_file_rel = raw.get("settings_file")
    resolved_settings_file = (
        (directory / str(run_settings_file_rel)).resolve() if run_settings_file_rel is not None else case_settings_file
    )
    merged_settings: dict[str, Any] = {}
    if resolved_settings_file is not None:
        merged_settings.update(load_settings_file(resolved_settings_file))
    merged_settings.update(case_settings)

    if "data" in raw:
        data_path = (directory / str(raw["data"])).resolve()
        derived_settings, primary_table, input_subdir = _derive_spec_run_settings(
            pipeline,
            directory,
            case_raw,
            raw,
            data_path,
        )
        merged_settings.update(derived_settings)
        expected_tables_rel = raw.get("expected_table", "expected_table.py")
        tables_attr = str(raw.get("table_attr", "EXPECTED_TABLE"))
        smoke_default = "expected_report" in raw
    else:
        data_path = (directory / str(raw.get("input_dir", "input"))).resolve()
        primary_table = None
        input_subdir = None
        expected_tables_rel = raw.get("expected_tables", "expected_tables.py")
        tables_attr = str(raw.get("tables_attr", "EXPECTED_TABLES"))
        smoke_default = True

    if "settings" in raw and isinstance(raw["settings"], dict):
        merged_settings.update(raw["settings"])
    merged_settings = _resolve_relative_path_settings(directory, merged_settings)

    expected_json_rel = raw.get("expected_json", "expected.json")
    expected_rejected_rel = raw.get("expected_rejected")
    extra_expected_json = {
        str(k): (directory / str(v)).resolve() for k, v in (raw.get("extra_expected_json") or {}).items()
    }
    expected_tables_path = (directory / str(expected_tables_rel)).resolve() if expected_tables_rel else None
    expected_json_path = (directory / str(expected_json_rel)).resolve() if expected_json_rel else None
    expected_rejected_path = (directory / str(expected_rejected_rel)).resolve() if expected_rejected_rel else None

    provisional_run = PipelineRun(
        id=str(raw["id"]),
        input_dir=data_path,
        input_subdir=input_subdir,
        settings_file=resolved_settings_file,
        settings=merged_settings,
        expected_report=_load_expected_report(raw.get("expected_report")),
        expected_tables=expected_tables_path,
        expected_json=expected_json_path,
        tables_attr=tables_attr,
        primary_table=primary_table,
        expected_rejected=expected_rejected_path,
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
    """Load one ``case.yaml`` pipeline manifest."""
    raw = _load_yaml_dict(manifest)
    directory = manifest.parent
    resolved_pipeline = str(raw.get("pipeline") or pipeline or directory.parent.name)
    resolved_family = family or resolved_pipeline
    case_settings_file_rel = raw.get("settings_file")
    case_settings_file = (
        (directory / str(case_settings_file_rel)).resolve() if case_settings_file_rel is not None else None
    )
    case_settings = dict(raw.get("settings") or {})

    return PipelineCase(
        pipeline=resolved_pipeline,
        name=directory.name,
        directory=directory,
        title=str(raw["title"]),
        rules=tuple(str(r) for r in raw.get("rules", [])),
        notes=str(raw.get("notes", "")).strip(),
        runs=tuple(
            _load_pipeline_run(
                resolved_pipeline,
                directory,
                raw,
                raw_run,
                case_settings_file,
                case_settings,
            )
            for raw_run in raw["runs"]
        ),
        family=resolved_family,
        suite=suite,
    )


def load_pipeline_cases(pipeline_spec_dir: Path) -> list[PipelineCase]:
    """Load all ``spec/*/case.yaml``, ``presets/*/*/case.yaml``, and ``*/case.yaml`` manifests under ``pipeline_spec_dir``."""
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
    """Extract the raw table mapping from a loaded expected-tables module."""
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
    """Load the ``dict[str, pa.Table]`` mapping from ``run.expected_tables`` (and ``run.expected_rejected`` if set)."""
    if run.expected_tables is None:
        return {}

    previous_schema_mod = sys.modules.get("expected_schema")
    schema_sibling = run.expected_tables.parent / "expected_schema.py"
    if run.expected_tables.stem != "expected_schema" and schema_sibling.is_file():
        sys.modules["expected_schema"] = load_module(schema_sibling)
    try:
        mod = load_module(run.expected_tables)
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
