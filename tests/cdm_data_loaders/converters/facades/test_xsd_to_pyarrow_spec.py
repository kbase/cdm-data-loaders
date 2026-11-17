"""Specification tests for XSD-to-PyArrow schema generation and XML-to-Arrow conversion.

Cases, rules and error fixtures are declared in manifests under ``specs/schema_converters/xsd_to_pyarrow``
(``rules.yaml``, ``*/case.yaml`` and ``spec/errors/manifest.yaml``). The same manifests render
``docs/xsd_to_pyarrow_specification.md`` via ``docs_macros`` Jinja2 templates.
"""

import copy
from collections import defaultdict
from pathlib import Path
from typing import Final

import pyarrow as pa
import pytest
import xmlschema
from _pytest.mark.structures import ParameterSet
from frozendict import frozendict
from lxml.etree import _Element, parse, tostring

from cdm_data_loaders.cli.xsd_to_pyarrow import SchemaCodegenConfig, render_schema_module
from cdm_data_loaders.coercion.xml import convert_xml_element
from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import to_upper_snake
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import (
    ERROR_BY_CODE,
    RecursiveNonJsonTypeError,
    XsdToPyArrowError,
)
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import generate_pyarrow_schema
from cdm_data_loaders.utils.xml_utils import local_name
from cdm_data_loaders.validation.xsd.traversal import load_schema
from specs._loader.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases
from tests.helpers.paths_and_constants import BASE_DIR
from tests.helpers.pyarrow_spec import (
    assert_expected_table_valid,
    assert_rows_and_table_match_expected,
    assert_schema_matches_expected,
    list_field,
    load_module,
)

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()
CASES: Final[tuple[Case, ...]] = tuple(load_cases(XSD_DATA_DIR / "spec") + load_cases(XSD_DATA_DIR / "examples"))
ERROR_CASES: Final[tuple[ErrorCase, ...]] = tuple(load_error_cases(XSD_DATA_DIR / "spec" / "errors" / "manifest.yaml"))
ERRORS_BY_CODE: Final[frozendict[str, type[XsdToPyArrowError]]] = ERROR_BY_CODE

RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
)
MAXIMAL_RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(run.data, id=f"{case.name}-{run.id}") for case in CASES for run in case.runs if run.id == "maximal"
)
ERROR_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES
)


def _extract_matching_elements(xml_path: Path, xml_tag: str) -> list[_Element]:
    """Parse ``xml_path`` and return elements matching ``xml_tag`` at the root or as direct children of root.

    :param xml_path: Path to the XML fixture file.
    :type xml_path: Path
    :param xml_tag: Target XML element tag name.
    :type xml_tag: str
    :return: Matching lxml elements.
    :rtype: list[_Element]
    """
    local_tag = local_name(xml_tag)
    root = parse(str(xml_path)).getroot()
    if local_name(root.tag) == local_tag:
        return [root]
    return [child for child in root if isinstance(child.tag, str) and local_name(child.tag) == local_tag]


def _max_recursion_depth(run: Run) -> int:
    """Return the configured ``max_recursion_depth`` for ``run`` (default ``2``).

    :param run: Converter specification run.
    :type run: Run
    :return: Maximum recursion depth.
    :rtype: int
    """
    return int(run.params.get("max_recursion_depth", 2))


def _xml_tag(run: Run) -> str:
    """Return the required ``xml_tag`` declared on ``run``.

    :param run: Converter specification run.
    :type run: Run
    :return: XML element tag name.
    :rtype: str
    """
    assert run.xml_tag is not None
    return run.xml_tag


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
        by_tag[local_name(elem.tag)].append(tostring(clone, encoding="unicode"))

    duplicates = {tag: sigs for tag, sigs in by_tag.items() if len(sigs) > 1 and len(set(sigs)) < len(sigs)}
    assert not duplicates


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_generate_pyarrow_schema_pass_cases(case: Case, run: Run) -> None:
    """generate_pyarrow_schema produces the exact expected PyArrow schema."""
    pa_schema = generate_pyarrow_schema(
        load_schema(case.schema), _xml_tag(run), max_recursion_depth=_max_recursion_depth(run)
    )
    assert_schema_matches_expected(pa_schema, case.expected_schema, run.schema_attr)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_render_schema_module_pass_cases(case: Case, run: Run, tmp_path: Path) -> None:
    """render_schema_module emits executable Python whose SCHEMA and SCHEMAS match the expected PyArrow schema."""
    xml_tag = _xml_tag(run)
    local_tag = local_name(xml_tag)
    out_path = tmp_path / "generated_schema.py"
    source = render_schema_module(
        SchemaCodegenConfig(
            xsd_file=case.schema,
            output=out_path,
            docstring=f"Generated schema for {case.name} ({run.id}).",
            xml_level=None,
            xml_tag=xml_tag,
            max_recursion_depth=_max_recursion_depth(run),
        )
    )
    out_path.write_text(source, encoding="utf-8")
    mod = load_module(out_path)

    generated_schema = mod.SCHEMA
    assert isinstance(generated_schema, pa.Schema)
    assert_schema_matches_expected(generated_schema, case.expected_schema, run.schema_attr)

    schemas_map = mod.SCHEMAS
    assert isinstance(schemas_map, dict)
    assert schemas_map[local_tag] is generated_schema

    if local_tag != "entry":
        tag_schema_var = f"{to_upper_snake(local_tag)}_SCHEMA"
        assert getattr(mod, tag_schema_var) is generated_schema

    struct_vars = [val for name, val in vars(mod).items() if name.endswith("_STRUCT")]
    assert len(struct_vars) >= 1
    assert all(isinstance(val, pa.StructType) for val in struct_vars)


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


@pytest.mark.parametrize(
    ("xsd_filename", "xml_tag", "depth", "expected"),
    [
        pytest.param(
            "self_referential.xsd",
            "node",
            2,
            pa.schema(
                [list_field("child", pa.struct([list_field("child", pa.string(), nullable=True)]), nullable=True)]
            ),
            id="self-referential",
        ),
        pytest.param(
            "mutual_recursion.xsd",
            "a",
            1,
            pa.schema([list_field("b", pa.struct([pa.field("a", pa.string(), nullable=True)]), nullable=True)]),
            id="mutual-recursion",
        ),
    ],
)
def test_generate_pyarrow_schema_pass_mutual_and_self_referential_xsds(
    xsd_filename: str,
    xml_tag: str,
    depth: int,
    expected: pa.Schema,
) -> None:
    """self_referential.xsd and mutual_recursion.xsd terminate at max_recursion_depth."""
    actual = generate_pyarrow_schema(
        load_schema(XSD_DATA_DIR / "spec" / xsd_filename), xml_tag, max_recursion_depth=depth
    )
    assert actual.equals(expected, check_metadata=True)


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_pyarrow_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Each error fixture raises the XsdToPyArrowError subclass for its code."""
    assert err.xml_tag is not None
    schema = load_schema(err.file)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_pyarrow_schema(schema, err.xml_tag)


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_render_schema_module_fail_error_manifest(err: ErrorCase) -> None:
    """Each error fixture raises the XsdToPyArrowError subclass for its code when rendering a schema module."""
    assert err.xml_tag is not None
    config = SchemaCodegenConfig(
        xsd_file=err.file,
        output=Path("error_schema.py"),
        docstring="Error fixture schema.",
        xml_level=None,
        xml_tag=err.xml_tag,
    )
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        render_schema_module(config)


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
