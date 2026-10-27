"""Specification tests for JSON Schema-to-PyArrow schema generation and JSON-to-Arrow record coercion.

Cases, rules and error fixtures are declared in manifests under ``tests/data/jsonschema`` (``rules.yaml``,
``*/case.yaml`` and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/jsonschema_to_pyarrow_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from _pytest.mark.structures import ParameterSet
from jsonschema import Draft202012Validator

from cdm_data_loaders.converters.jsonschema_to_pyarrow import (
    JSONSCHEMA_TO_PYARROW_ERRORS,
    JSONSchemaToPyArrowError,
    RecursiveNonJsonTypeError,
    coerce_record_for_pyarrow,
    generate_pyarrow_schema,
)
from scripts.gen_converter_spec import JSONSCHEMA_TO_PYARROW, render
from tests.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases, load_rules
from tests.pyarrow_spec_helpers import (
    assert_expected_table_valid,
    assert_rows_and_table_match_expected,
    assert_schema_matches_expected,
)

JSONSCHEMA_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "jsonschema"
RULES = load_rules(JSONSCHEMA_DATA_DIR / "rules.yaml")
CASES: list[Case] = load_cases(JSONSCHEMA_DATA_DIR / "spec") + load_cases(JSONSCHEMA_DATA_DIR / "examples")
ERROR_CASES: list[ErrorCase] = load_error_cases(JSONSCHEMA_DATA_DIR / "spec" / "errors" / "manifest.yaml")
ERRORS_BY_CODE: dict[str, type[JSONSchemaToPyArrowError]] = {cls.code: cls for cls in JSONSCHEMA_TO_PYARROW_ERRORS}

RUN_PARAMS: list[ParameterSet] = [
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
]
MAXIMAL_RUN_PARAMS: list[ParameterSet] = [
    pytest.param(run.data, id=f"{case.name}-{run.id}") for case in CASES for run in case.runs if run.id == "maximal"
]
ERROR_PARAMS: list[ParameterSet] = [pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES]


def _load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = f"Expected a JSON object in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _load_json_records(path: Path) -> list[dict[str, Any]]:
    """Load a list of JSON record objects from disk."""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        msg = f"Expected a list of JSON records in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _max_recursion_depth(run: Run) -> int:
    return int(run.params.get("max_recursion_depth", 2))


def _collect_multi_item_lists(value: object, path: str = "$") -> dict[str, list[str]]:
    """Collect canonical JSON element signatures for every list with more than one element."""
    lists_by_path: dict[str, list[str]] = {}
    if isinstance(value, list):
        if len(value) > 1:
            lists_by_path[path] = [json.dumps(item, sort_keys=True) for item in value]
        for idx, item in enumerate(value):
            lists_by_path.update(_collect_multi_item_lists(item, f"{path}[{idx}]"))
    elif isinstance(value, dict):
        for key, child in value.items():
            lists_by_path.update(_collect_multi_item_lists(child, f"{path}.{key}"))
    return lists_by_path


# Manifest consistency


def test_manifests_pass_every_case_rule_exists() -> None:
    """Every rule referenced by a case manifest is defined in rules.yaml."""
    unknown = {f"{case.suite}/{case.name}": sorted(set(case.rules) - RULES.rules.keys()) for case in CASES}
    assert {name: ids for name, ids in unknown.items() if ids} == {}


def test_manifests_pass_every_rule_is_exercised() -> None:
    """Every rule in rules.yaml is exercised by at least one case."""
    used = {rule_id for case in CASES for rule_id in case.rules}
    assert sorted(RULES.rules.keys() - used) == []


def test_manifests_pass_every_case_file_exists() -> None:
    """Every file referenced by a case or error manifest exists."""
    paths = [p for case in CASES for p in (case.schema, case.expected_schema)]
    paths += [p for case in CASES for run in case.runs for p in (run.data, run.expected_json, run.expected_table)]
    paths += [err.file for err in ERROR_CASES]
    assert [str(p) for p in paths if not p.is_file()] == []


def test_manifests_pass_every_error_fixture_listed() -> None:
    """Every JSON file in spec/errors is listed in the error manifest."""
    listed = {err.file for err in ERROR_CASES}
    on_disk = set((JSONSCHEMA_DATA_DIR / "spec" / "errors").glob("*.json"))
    assert sorted(str(p) for p in on_disk - listed) == []


def test_manifests_pass_error_codes_unique_and_covered() -> None:
    """Error codes are unique, every manifest entry uses a known code, and every code has a fixture."""
    assert len(ERRORS_BY_CODE) == len(JSONSCHEMA_TO_PYARROW_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in JSONSCHEMA_TO_PYARROW_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


def test_specification_document_pass_up_to_date() -> None:
    """The committed specification document matches the output of scripts/gen_converter_spec.py."""
    assert JSONSCHEMA_TO_PYARROW.output.read_text(encoding="utf-8") == render(JSONSCHEMA_TO_PYARROW), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )


# Fixture validity


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_fixtures_pass_records_and_expected_table_validate(case: Case, run: Run) -> None:
    """Input records validate against the schema and the expected table matches the expected JSON and schema."""
    schema = _load_json_object(case.schema)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    records = _load_json_records(run.data)
    for record in records:
        validator.validate(record)
    assert_expected_table_valid(
        records,
        run.expected_json,
        run.expected_table,
        expected_count=run.expected_count,
        schema_attr=run.schema_attr,
        table_attr=run.table_attr,
    )


@pytest.mark.parametrize("data_path", MAXIMAL_RUN_PARAMS)
def test_maximal_examples_pass_repeated_items_have_distinct_content(data_path: Path) -> None:
    """Every multi-element array in a maximal JSON example contains distinct elements."""
    lists_by_path = _collect_multi_item_lists(_load_json_records(data_path))
    duplicates = {path: sigs for path, sigs in lists_by_path.items() if len(set(sigs)) < len(sigs)}
    assert not duplicates


# Schema generation and coercion


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_generate_pyarrow_schema_pass_cases(case: Case, run: Run) -> None:
    """generate_pyarrow_schema produces the exact expected PyArrow schema."""
    pa_schema = generate_pyarrow_schema(_load_json_object(case.schema), max_recursion_depth=_max_recursion_depth(run))
    assert_schema_matches_expected(pa_schema, case.expected_schema, run.schema_attr)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_coerce_record_for_pyarrow_pass_cases(case: Case, run: Run) -> None:
    """Records coerce to the expected rows, build the expected table and round-trip through Parquet."""
    depth = _max_recursion_depth(run)
    pa_schema = generate_pyarrow_schema(_load_json_object(case.schema), max_recursion_depth=depth)
    rows = [
        coerce_record_for_pyarrow(record, pa_schema, max_recursion_depth=depth)
        for record in _load_json_records(run.data)
    ]
    assert_rows_and_table_match_expected(
        rows, pa_schema, run.expected_json, run.expected_table, table_attr=run.table_attr
    )


def test_generate_pyarrow_schema_pass_kbase_xsv_metaschema_dialect() -> None:
    """The KBase Draft 2020-12 XSV metaschema URI is accepted identically to standard Draft 2020-12."""
    fixture_dir = JSONSCHEMA_DATA_DIR / "spec" / "01_scalars_and_formats"
    schema = _load_json_object(fixture_dir / "schema.json")
    schema["$schema"] = "https://json-schema.kbase.us/draft/2020-12-xsv/schema"
    assert_schema_matches_expected(generate_pyarrow_schema(schema), fixture_dir / "expected_schema.py")


# Errors


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_pyarrow_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Each error fixture raises the JSONSchemaToPyArrowError subclass for its code."""
    schema = _load_json_object(err.file)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_pyarrow_schema(schema)


@pytest.mark.parametrize(
    "forbidden_leaf_schema",
    [
        pytest.param({"type": "string", "format": "date"}, id="format-date"),
        pytest.param({"type": "string", "contentEncoding": "base64"}, id="contentEncoding-base64"),
        pytest.param({"type": "string", "contentEncoding": "base16"}, id="contentEncoding-base16"),
    ],
)
def test_generate_pyarrow_schema_fail_non_json_serializable_type_under_recursion(
    forbidden_leaf_schema: dict[str, str],
) -> None:
    """Date and Base64/Base16 types inside a recursive $defs cycle raise RecursiveNonJsonTypeError (E10)."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"root": {"$ref": "#/$defs/Node"}},
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {"payload": forbidden_leaf_schema, "child": {"$ref": "#/$defs/Node"}},
            },
        },
    }
    with pytest.raises(RecursiveNonJsonTypeError, match="recursive"):
        generate_pyarrow_schema(schema)
