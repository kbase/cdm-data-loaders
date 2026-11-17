"""Unified manifest, rule coverage, fixture existence, and error code tests for all specs."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.exceptions.facades.jsonschema_to_pyarrow import (
    JSONSCHEMA_TO_PYARROW_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_jsonschema import (
    PYARROW_TO_JSONSCHEMA_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_linkml import (
    PYARROW_TO_LINKML_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.pydantic_to_pyarrow import (
    PYDANTIC_TO_PYARROW_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import XSD_TO_PYARROW_ERRORS
from cdm_data_loaders.core.cli import SpecError
from cdm_data_loaders.core.pipeline_catalog import PipelineSpec, resolve_pipeline
from cdm_data_loaders.pipelines.jsonlines.exceptions import JSONLINES_JSONSCHEMA_ERRORS
from cdm_data_loaders.pipelines.xmltopyarrow.exceptions import XMLTOPYARROW_CONFIG_ERRORS
from cdm_data_loaders.pipelines.xsv.exceptions import XSV_SCHEMA_ERRORS
from specs._loader.converter_spec import load_cases, load_error_cases, load_rules
from specs._loader.pipeline_spec import PipelineCase, PipelineRun, load_pipeline_cases
from specs._loader.yaml_io import load_yaml_dict
from tests.helpers.paths_and_constants import PIPELINE_SPECS_DIR, SCHEMA_CONVERTER_SPECS_DIR
from tests.helpers.table_readers import (
    canonicalize_table_rows,
    load_expected_json_tables,
    load_expected_tables,
    normalize_expected_table_rows,
)


@dataclass(frozen=True, slots=True)
class SchemaSpecTarget:
    """Converter or hybrid pipeline spec directory with numbered error classes and error fixtures."""

    name: str
    data_dir: Path
    suites: tuple[str, ...]
    error_classes: tuple[type[ConversionError] | type[SpecError], ...]
    error_glob: str
    is_pipeline_hybrid: bool = False


SCHEMA_SPEC_TARGETS: Final[tuple[SchemaSpecTarget, ...]] = (
    SchemaSpecTarget(
        name="jsonschema_to_pyarrow",
        data_dir=(SCHEMA_CONVERTER_SPECS_DIR / "jsonschema_to_pyarrow").resolve(),
        suites=("spec", "examples"),
        error_classes=JSONSCHEMA_TO_PYARROW_ERRORS,
        error_glob="*.json",
    ),
    SchemaSpecTarget(
        name="pyarrow_to_jsonschema",
        data_dir=(SCHEMA_CONVERTER_SPECS_DIR / "pyarrow_to_jsonschema").resolve(),
        suites=("spec", "examples"),
        error_classes=PYARROW_TO_JSONSCHEMA_ERRORS,
        error_glob="*.py",
    ),
    SchemaSpecTarget(
        name="pyarrow_to_linkml",
        data_dir=(SCHEMA_CONVERTER_SPECS_DIR / "pyarrow_to_linkml").resolve(),
        suites=("spec", "examples"),
        error_classes=PYARROW_TO_LINKML_ERRORS,
        error_glob="*.py",
    ),
    SchemaSpecTarget(
        name="pydantic_to_pyarrow",
        data_dir=(SCHEMA_CONVERTER_SPECS_DIR / "pydantic_to_pyarrow").resolve(),
        suites=("spec", "examples"),
        error_classes=PYDANTIC_TO_PYARROW_ERRORS,
        error_glob="*.py",
    ),
    SchemaSpecTarget(
        name="xsd_to_pyarrow",
        data_dir=(SCHEMA_CONVERTER_SPECS_DIR / "xsd_to_pyarrow").resolve(),
        suites=("spec", "examples"),
        error_classes=XSD_TO_PYARROW_ERRORS,
        error_glob="*.xsd",
    ),
    SchemaSpecTarget(
        name="jsonlines_jsonschema",
        data_dir=(PIPELINE_SPECS_DIR / "jsonlines_jsonschema").resolve(),
        suites=("spec",),
        error_classes=JSONLINES_JSONSCHEMA_ERRORS,
        error_glob="*.json",
        is_pipeline_hybrid=True,
    ),
    SchemaSpecTarget(
        name="xmltopyarrow",
        data_dir=(PIPELINE_SPECS_DIR / "xmltopyarrow").resolve(),
        suites=("spec",),
        error_classes=XMLTOPYARROW_CONFIG_ERRORS,
        error_glob="*.yaml",
        is_pipeline_hybrid=True,
    ),
    SchemaSpecTarget(
        name="xsv",
        data_dir=(PIPELINE_SPECS_DIR / "xsv").resolve(),
        suites=("spec",),
        error_classes=XSV_SCHEMA_ERRORS,
        error_glob="*.json",
        is_pipeline_hybrid=True,
    ),
)

PIPELINE_FAMILY_DIRS: Final[dict[str, Path]] = {
    entry.name: entry.resolve()
    for entry in sorted(PIPELINE_SPECS_DIR.iterdir())
    if entry.is_dir() and (entry / "rules.yaml").is_file()
}


def _pipeline_presets_by_family() -> dict[str, tuple[str, ...]]:
    """Read declared preset names from ``rules.yaml`` front-matter and ``presets/*`` subdirectories.

    :return: Mapping from pipeline family name to sorted preset names.
    :rtype: dict[str, tuple[str, ...]]
    """
    result: dict[str, tuple[str, ...]] = {}
    for family_name, family_dir in PIPELINE_FAMILY_DIRS.items():
        raw = load_yaml_dict(family_dir / "rules.yaml")
        declared = [str(item) for item in (raw.get("presets") or ())]
        presets_dir = family_dir / "presets"
        if presets_dir.is_dir():
            for child in sorted(presets_dir.iterdir()):
                if child.is_dir() and not child.name.startswith(".") and child.name not in declared:
                    declared.append(child.name)
        result[family_name] = tuple(declared)
    return result


PIPELINE_PRESETS_BY_FAMILY: Final[dict[str, tuple[str, ...]]] = _pipeline_presets_by_family()
PIPELINE_PRESET_PAIRS: Final[tuple[tuple[str, str], ...]] = tuple(
    (family_name, preset_name)
    for family_name in sorted(PIPELINE_FAMILY_DIRS)
    for preset_name in PIPELINE_PRESETS_BY_FAMILY[family_name]
)
PIPELINE_CASES_BY_FAMILY: Final[dict[str, list[PipelineCase]]] = {
    name: load_pipeline_cases(path) for name, path in PIPELINE_FAMILY_DIRS.items()
}
ALL_PIPELINE_CASES: Final[list[PipelineCase]] = [case for cases in PIPELINE_CASES_BY_FAMILY.values() for case in cases]
ALL_EXPECTED_TABLE_RUNS: Final[list[tuple[PipelineCase, PipelineRun]]] = [
    (case, run)
    for case in ALL_PIPELINE_CASES
    for run in case.runs
    if run.expected_tables is not None or run.expected_json is not None
]


@pytest.mark.parametrize("target", SCHEMA_SPEC_TARGETS, ids=[t.name for t in SCHEMA_SPEC_TARGETS])
def test_schema_spec_manifests_rules_files_and_error_codes(target: SchemaSpecTarget) -> None:
    """Verify rule coverage, fixture file existence, error fixture listing, and error code uniqueness."""
    catalogue = load_rules(target.data_dir / "rules.yaml")
    cases = [case for suite in target.suites for case in load_cases(target.data_dir / suite)]
    error_cases = load_error_cases(target.data_dir / "spec" / "errors" / "manifest.yaml")

    if not target.is_pipeline_hybrid:
        unknown = {f"{c.suite}/{c.name}": sorted(set(c.rules) - catalogue.rules.keys()) for c in cases}
        assert {k: v for k, v in unknown.items() if v} == {}
        used_rules = {rule_id for c in cases for rule_id in c.rules}
        assert sorted(catalogue.rules.keys() - used_rules) == []

    expected_files = [p for c in cases for p in (c.schema, c.expected_schema)]
    expected_files += [p for c in cases for r in c.runs for p in (r.expected_json, r.expected_table)]
    expected_files += [r.expected_rejected for c in cases for r in c.runs if r.expected_rejected is not None]
    expected_files += [extra for c in cases for r in c.runs for extra in r.extra_expected_json.values()]
    expected_files += [err.file for err in error_cases]
    assert [str(p) for p in expected_files if not p.is_file()] == []

    data_paths = [r.data for c in cases for r in c.runs]
    if target.is_pipeline_hybrid:
        assert [str(p) for p in data_paths if not p.exists()] == []
    else:
        assert [str(p) for p in data_paths if not p.is_file()] == []

    listed_errors = {err.file for err in error_cases}
    on_disk_errors = {
        p for p in (target.data_dir / "spec" / "errors").glob(target.error_glob) if p.name != "manifest.yaml"
    }
    assert sorted(str(p) for p in on_disk_errors - listed_errors) == []

    errors_by_code = {cls.code: cls for cls in target.error_classes}
    assert len(errors_by_code) == len(target.error_classes)
    assert all(cls.code and cls.title and cls.__doc__ for cls in target.error_classes)
    manifest_codes = {err.code for err in error_cases}
    assert sorted(manifest_codes - errors_by_code.keys()) == []
    assert sorted(errors_by_code.keys() - manifest_codes) == []


@pytest.fixture
def base_pipeline_spec(family_name: str) -> PipelineSpec:
    """Resolve the base ``PipelineSpec`` for ``family_name``.

    :param family_name: Pipeline family command name.
    :type family_name: str
    :return: Resolved base ``PipelineSpec``.
    :rtype: PipelineSpec
    """
    return resolve_pipeline(family_name)


@pytest.mark.parametrize("family_name", sorted(PIPELINE_FAMILY_DIRS), ids=str)
def test_pipeline_family_rules_pass(family_name: str) -> None:
    """Verify pipeline family rule coverage and base pipeline registration."""
    catalogue = load_rules(PIPELINE_FAMILY_DIRS[family_name] / "rules.yaml")
    cases = PIPELINE_CASES_BY_FAMILY[family_name]
    unknown = {f"{c.pipeline}/{c.name}": sorted(set(c.rules) - catalogue.rules.keys()) for c in cases}
    assert {k: v for k, v in unknown.items() if v} == {}

    exercised = {rid for c in cases for rid in c.rules}
    assert sorted(set(catalogue.rules) - exercised) == []

    base_resolved = resolve_pipeline(family_name)
    assert base_resolved.settings_cls is not None
    assert base_resolved.pipeline_fn is not None


@pytest.mark.parametrize(
    ("family_name", "preset_name"),
    [pytest.param(fam, preset, id=f"{fam}-{preset}") for fam, preset in PIPELINE_PRESET_PAIRS],
)
def test_pipeline_preset_inheritance_pass(
    family_name: str,
    preset_name: str,
    base_pipeline_spec: PipelineSpec,
) -> None:
    """Verify each preset pipeline subclasses its family settings class and shares its runner."""
    assert base_pipeline_spec.cli_command == family_name
    assert base_pipeline_spec.settings_cls is not None
    preset_resolved = resolve_pipeline(preset_name)
    assert preset_resolved.settings_cls is not None
    assert issubclass(preset_resolved.settings_cls, base_pipeline_spec.settings_cls)
    assert preset_resolved.settings_cls is not base_pipeline_spec.settings_cls
    assert preset_resolved.pipeline_fn is base_pipeline_spec.pipeline_fn


@pytest.mark.parametrize("case", ALL_PIPELINE_CASES, ids=[f"{c.pipeline}-{c.name}" for c in ALL_PIPELINE_CASES])
def test_pipeline_case_files_exist(case: PipelineCase) -> None:
    """Verify every input path, settings file, ``expected_tables.py``, and ``expected.json`` exists on disk."""
    assert case.runs, f"{case.pipeline}/{case.name} has no runs"
    for run in case.runs:
        assert run.input_dir.exists(), f"Missing input_dir: {run.input_dir}"
        if run.input_dir.is_dir():
            assert any(p.is_file() for p in run.input_dir.rglob("*")), f"Empty input_dir: {run.input_dir}"
        if run.settings_file is not None:
            assert run.settings_file.is_file(), f"Missing settings_file: {run.settings_file}"
        if run.expected_tables is not None:
            assert run.expected_tables.is_file(), f"Missing expected_tables: {run.expected_tables}"
        if run.expected_json is not None:
            assert run.expected_json.is_file(), f"Missing expected_json: {run.expected_json}"


@pytest.mark.parametrize(
    ("case", "run"),
    ALL_EXPECTED_TABLE_RUNS,
    ids=[f"{c.pipeline}-{c.name}-{r.id}" for c, r in ALL_EXPECTED_TABLE_RUNS],
)
def test_pipeline_expected_tables_validate(case: PipelineCase, run: PipelineRun) -> None:
    """Verify every expected PyArrow table is valid and matches ``expected_counts`` and ``expected.json``."""
    assert case.family in PIPELINE_FAMILY_DIRS
    assert run.expected_tables is not None
    assert run.expected_json is not None

    tables = load_expected_tables(run)
    for table in tables.values():
        table.validate(full=True)

    assert {name: table.num_rows for name, table in tables.items()} == run.expected_counts

    expected_json_data = load_expected_json_tables(run)
    normalized_from_tables = {name: normalize_expected_table_rows(table) for name, table in sorted(tables.items())}
    normalized_from_json = {name: canonicalize_table_rows(rows) for name, rows in sorted(expected_json_data.items())}
    assert normalized_from_tables == normalized_from_json
