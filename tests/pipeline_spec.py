"""Load pipeline smoke test specification manifests: rule catalogues, per-case manifests, and expected tables.

A pipeline specification lives under ``specs/pipelines/<pipeline_name>/``:

- ``rules.yaml``: rule catalogue (sections of rule IDs and statements).
- ``<case_dir>/case.yaml``: one manifest per fixture directory listing the rules it exercises and its runs.
- ``<case_dir>/settings.toml`` (or ``.yaml`` / ``.json``): optional per-case settings configuration file.
- ``<case_dir>/expected_tables.py``: expected ``dict[str, pa.Table]`` mapping table names to PyArrow tables.
- ``<case_dir>/expected.json``: expected JSON representation of output tables (non-null fields).

The same manifests drive the pipeline smoke tests and the generated pipeline specification documents.
"""

import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml

from tests.converter_spec import CASE_MANIFEST, Rule, RuleCatalogue, Section, load_rules
from tests.pyarrow_spec_helpers import load_fixture_attr

__all__ = [
    "CASE_MANIFEST",
    "ExpectedReport",
    "PipelineCase",
    "PipelineRun",
    "Rule",
    "RuleCatalogue",
    "Section",
    "load_expected_tables",
    "load_pipeline_case",
    "load_pipeline_cases",
    "load_rules",
    "load_settings_file",
]


@dataclass(frozen=True, slots=True)
class ExpectedReport:
    """Expected contents of ``report.txt`` written by ``save_pipeline_metadata``."""

    status: str = "completed"
    ingested_files: tuple[str, ...] = ()
    load_errors: tuple[str, ...] = ()
    pipeline_load_info_errs: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineRun:
    """One execution of a pipeline case: input directory, settings, and expected outputs."""

    id: str
    input_dir: Path
    settings_file: Path | None = None
    settings: dict[str, Any] = field(default_factory=dict)
    expected_report: ExpectedReport = field(default_factory=ExpectedReport)
    expected_counts: dict[str, int] = field(default_factory=dict)
    expected_tables: Path | None = None
    expected_json: Path | None = None
    tables_attr: str = "EXPECTED_TABLES"


@dataclass(frozen=True, slots=True)
class PipelineCase:
    """A pipeline smoke test fixture directory with its manifest."""

    pipeline: str
    name: str
    directory: Path
    title: str
    rules: tuple[str, ...]
    notes: str
    runs: tuple[PipelineRun, ...]

    @property
    def anchor(self) -> str:
        """Stable HTML anchor for the case."""
        return f"case-{self.pipeline}-{self.name}".replace("_", "-").lower()


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


def _load_pipeline_run(
    directory: Path,
    raw: dict[str, Any],
    case_settings_file: Path | None,
    case_settings: dict[str, Any],
) -> PipelineRun:
    run_settings_file_rel = raw.get("settings_file")
    resolved_settings_file = (
        (directory / str(run_settings_file_rel)).resolve()
        if run_settings_file_rel is not None
        else case_settings_file
    )
    merged_settings: dict[str, Any] = {}
    if resolved_settings_file is not None:
        merged_settings.update(load_settings_file(resolved_settings_file))
    merged_settings.update(case_settings)
    if "settings" in raw and isinstance(raw["settings"], dict):
        merged_settings.update(raw["settings"])

    expected_tables_rel = raw.get("expected_tables", "expected_tables.py")
    expected_json_rel = raw.get("expected_json", "expected.json")
    expected_counts = {str(k): int(v) for k, v in (raw.get("expected_counts") or {}).items()}

    return PipelineRun(
        id=str(raw["id"]),
        input_dir=(directory / str(raw.get("input_dir", "input"))).resolve(),
        settings_file=resolved_settings_file,
        settings=merged_settings,
        expected_report=_load_expected_report(raw.get("expected_report")),
        expected_counts=expected_counts,
        expected_tables=(directory / str(expected_tables_rel)).resolve() if expected_tables_rel else None,
        expected_json=(directory / str(expected_json_rel)).resolve() if expected_json_rel else None,
        tables_attr=str(raw.get("tables_attr", "EXPECTED_TABLES")),
    )


def load_pipeline_case(manifest: Path, pipeline: str | None = None) -> PipelineCase:
    """Load one ``case.yaml`` pipeline smoke test manifest."""
    raw = _load_yaml_dict(manifest)
    directory = manifest.parent
    resolved_pipeline = str(raw.get("pipeline") or pipeline or directory.parent.name)
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
            _load_pipeline_run(directory, raw_run, case_settings_file, case_settings) for raw_run in raw["runs"]
        ),
    )


def load_pipeline_cases(pipeline_spec_dir: Path) -> list[PipelineCase]:
    """Load every ``<pipeline_spec_dir>/*/case.yaml`` manifest, sorted by directory name."""
    return [
        load_pipeline_case(manifest, pipeline=pipeline_spec_dir.name)
        for manifest in sorted(pipeline_spec_dir.glob(f"*/{CASE_MANIFEST}"))
    ]


def load_expected_tables(run: PipelineRun) -> dict[str, pa.Table]:
    """Load the ``dict[str, pa.Table]`` mapping from ``run.expected_tables``."""
    if run.expected_tables is None:
        return {}
    raw_tables = load_fixture_attr(run.expected_tables, run.tables_attr, dict)
    result: dict[str, pa.Table] = {}
    for table_name, table in raw_tables.items():
        if not isinstance(table_name, str) or not isinstance(table, pa.Table):
            msg = (
                f"Expected {run.tables_attr} in {run.expected_tables} to map str -> pa.Table, "
                f"got {type(table_name).__name__} -> {type(table).__name__}"
            )
            raise TypeError(msg)
        result[table_name] = table
    return result
