"""Specification tests for XSD-to-PyArrow schema generation and XML-to-Arrow conversion.

Cases, rules and error fixtures are declared in manifests under ``tests/data/xsd`` (``rules.yaml``,
``*/case.yaml`` and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/xsd_to_pyarrow_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import copy
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pytest
import xmlschema
from _pytest.mark.structures import ParameterSet
from lxml.etree import Element, parse, tostring

from cdm_data_loaders.readers.xsd import (
    XSD_TO_PYARROW_ERRORS,
    RecursiveNonJsonTypeError,
    XsdToPyArrowError,
    convert_xml_element,
    generate_pyarrow_schema,
    load_schema,
)
from scripts.gen_converter_spec import XSD_TO_PYARROW, render
from tests.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases, load_rules
from tests.pyarrow_spec_helpers import (
    assert_expected_table_valid,
    assert_rows_and_table_match_expected,
    assert_schema_matches_expected,
    list_field,
)

XSD_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "xsd"
RULES = load_rules(XSD_DATA_DIR / "rules.yaml")
CASES: list[Case] = load_cases(XSD_DATA_DIR / "spec") + load_cases(XSD_DATA_DIR / "examples")
ERROR_CASES: list[ErrorCase] = load_error_cases(XSD_DATA_DIR / "spec" / "errors" / "manifest.yaml")
ERRORS_BY_CODE: dict[str, type[XsdToPyArrowError]] = {cls.code: cls for cls in XSD_TO_PYARROW_ERRORS}

RUN_PARAMS: list[ParameterSet] = [
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
]
MAXIMAL_RUN_PARAMS: list[ParameterSet] = [
    pytest.param(run.data, id=f"{case.name}-{run.id}") for case in CASES for run in case.runs if run.id == "maximal"
]
ERROR_PARAMS: list[ParameterSet] = [pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES]


def _local_tag(tag: str) -> str:
    """Strip an optional Clark-notation '{namespace}' prefix from an XML tag."""
    return tag.rsplit("}", maxsplit=1)[-1] if "}" in tag else tag


def _extract_matching_elements(xml_path: Path, xml_tag: str) -> list[Element]:
    """Parse xml_path and return all elements matching xml_tag at the root or as a direct child of root."""
    local_tag = _local_tag(xml_tag)
    root = parse(str(xml_path)).getroot()
    if _local_tag(root.tag) == local_tag:
        return [root]
    return [child for child in root if isinstance(child.tag, str) and _local_tag(child.tag) == local_tag]


def _max_recursion_depth(run: Run) -> int:
    return int(run.params.get("max_recursion_depth", 2))


def _xml_tag(run: Run) -> str:
    assert run.xml_tag is not None
    return run.xml_tag


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
    """Every file referenced by a case manifest exists."""
    paths = [p for case in CASES for p in (case.schema, case.expected_schema)]
    paths += [p for case in CASES for run in case.runs for p in (run.data, run.expected_json, run.expected_table)]
    paths += [err.file for err in ERROR_CASES]
    assert [str(p) for p in paths if not p.is_file()] == []


def test_manifests_pass_error_codes_unique_and_covered() -> None:
    """Error codes are unique, every error manifest entry uses a known code, and every code has a fixture."""
    assert len(ERRORS_BY_CODE) == len(XSD_TO_PYARROW_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in XSD_TO_PYARROW_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


def test_specification_document_pass_up_to_date() -> None:
    """The committed specification document matches the output of scripts/gen_converter_spec.py."""
    assert XSD_TO_PYARROW.output.read_text(encoding="utf-8") == render(XSD_TO_PYARROW), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )


# Fixture validity


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_fixtures_pass_xml_and_expected_table_validate(case: Case, run: Run) -> None:
    """Input XML validates against the XSD and the expected table matches the expected JSON and schema."""
    load_schema(case.schema).validate(str(run.data))
    assert_expected_table_valid(
        _extract_matching_elements(run.data, _xml_tag(run)),
        run.expected_json,
        run.expected_table,
        expected_count=run.expected_count,
    )


@pytest.mark.parametrize("xml_path", MAXIMAL_RUN_PARAMS)
def test_maximal_examples_pass_repeated_elements_have_distinct_content(xml_path: Path) -> None:
    """Every repeated element in a maximal XML example contains distinct data."""
    root = parse(str(xml_path)).getroot()
    by_tag: dict[str, list[str]] = defaultdict(list)
    for elem in root.iter():
        if elem is root or not isinstance(elem.tag, str):
            continue
        clone = copy.deepcopy(elem)
        for node in clone.iter():
            if node.text is not None:
                node.text = node.text.strip()
            if node.tail is not None:
                node.tail = node.tail.strip()
        by_tag[_local_tag(elem.tag)].append(tostring(clone, encoding="unicode"))

    duplicates = {tag: sigs for tag, sigs in by_tag.items() if len(sigs) > 1 and len(set(sigs)) < len(sigs)}
    assert not duplicates


# Schema generation and conversion


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_generate_pyarrow_schema_pass_cases(case: Case, run: Run) -> None:
    """generate_pyarrow_schema produces the exact expected PyArrow schema."""
    pa_schema = generate_pyarrow_schema(
        load_schema(case.schema), _xml_tag(run), max_recursion_depth=_max_recursion_depth(run)
    )
    assert_schema_matches_expected(pa_schema, case.expected_schema, run.schema_attr)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_convert_xml_element_pass_cases(case: Case, run: Run) -> None:
    """XML elements convert to the expected rows, build the expected table and round-trip through Parquet."""
    schema = load_schema(case.schema)
    depth = _max_recursion_depth(run)
    xml_tag = _xml_tag(run)
    pa_schema = generate_pyarrow_schema(schema, xml_tag, max_recursion_depth=depth)
    rows = [
        convert_xml_element(elem, schema, xml_tag, max_recursion_depth=depth)
        for elem in _extract_matching_elements(run.data, xml_tag)
    ]
    assert_rows_and_table_match_expected(
        rows, pa_schema, run.expected_json, run.expected_table, table_attr=run.table_attr
    )


def test_generate_pyarrow_schema_pass_mutual_and_self_referential_xsds(test_data_dir: Path) -> None:
    """self_referential.xsd and mutual_recursion.xsd terminate at max_recursion_depth."""
    pa_self = generate_pyarrow_schema(
        load_schema(test_data_dir / "xsd" / "self_referential.xsd"), "node", max_recursion_depth=2
    )
    expected_self = pa.schema(
        [list_field("child", pa.struct([list_field("child", pa.string(), nullable=True)]), nullable=True)]
    )
    assert pa_self.equals(expected_self, check_metadata=True)

    pa_mutual = generate_pyarrow_schema(
        load_schema(test_data_dir / "xsd" / "mutual_recursion.xsd"), "a", max_recursion_depth=1
    )
    expected_mutual = pa.schema(
        [list_field("b", pa.struct([pa.field("a", pa.string(), nullable=True)]), nullable=True)]
    )
    assert pa_mutual.equals(expected_mutual, check_metadata=True)


# Errors


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_pyarrow_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Each error fixture raises the XsdToPyArrowError subclass for its code."""
    assert err.xml_tag is not None
    schema = load_schema(err.file)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_pyarrow_schema(schema, err.xml_tag)


@pytest.mark.parametrize(
    "forbidden_xsd_type",
    [
        pytest.param("xs:date", id="xs-date"),
        pytest.param("xs:dateTime", id="xs-dateTime"),
        pytest.param("xs:hexBinary", id="xs-hexBinary"),
        pytest.param("xs:base64Binary", id="xs-base64Binary"),
    ],
)
@pytest.mark.parametrize("position", ["child", "attribute"])
def test_generate_pyarrow_schema_fail_non_json_serializable_type_under_recursion(
    forbidden_xsd_type: str,
    position: str,
) -> None:
    """Date, dateTime and binary types under a recursive element raise RecursiveNonJsonTypeError (E9)."""
    extra_child = (
        f'<xs:element name="payload" type="{forbidden_xsd_type}" minOccurs="0"/>' if position == "child" else ""
    )
    extra_attr = f'<xs:attribute name="attrPayload" type="{forbidden_xsd_type}"/>' if position == "attribute" else ""
    xsd_text = f"""<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="NodeType">
        <xs:sequence>
          {extra_child}
          <xs:element name="node" type="NodeType" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
        {extra_attr}
      </xs:complexType>
      <xs:element name="root" type="NodeType"/>
    </xs:schema>
    """
    with pytest.raises(RecursiveNonJsonTypeError, match="recursive type"):
        generate_pyarrow_schema(xmlschema.XMLSchema(xsd_text), "root")
