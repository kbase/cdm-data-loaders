"""Specification tests for XSD-to-PyArrow schema generation and XML-to-Arrow conversion.

Cases, rules and error fixtures are declared in manifests under ``specs/schema_converters/xsd_to_pyarrow``
(``rules.yaml``, ``*/case.yaml`` and ``spec/errors/manifest.yaml``). The same manifests generate
``docs/xsd_to_pyarrow_specification.md`` via ``scripts/gen_converter_spec.py``.
"""

import copy
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pytest
import xmlschema
from _pytest.mark.structures import ParameterSet
from lxml.etree import Element, fromstring, parse, tostring
from pydantic import ValidationError

import scripts.gen_pyarrow_schema as gen_pyarrow_schema_module
from cdm_data_loaders.coercion.xml import (
    convert_xml_element,
    make_xml_level_converters,
)
from cdm_data_loaders.converters.cli.xsd_to_pyarrow import (
    SchemaCodegenConfig,
    render_schema_module,
)
from cdm_data_loaders.converters.cli.xsd_to_pyarrow import (
    cli as xsd_to_pyarrow_cli,
)
from cdm_data_loaders.converters.emitters.dlt import DltEmitter
from cdm_data_loaders.converters.emitters.json_schema import JsonSchemaEmitter
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter, assemble_pyarrow_schema
from cdm_data_loaders.converters.emitters.pyarrow_codegen import (
    _decode_metadata,
    _fmt_str,
    _format_field_indented,
    _format_schema_exports,
    _format_struct_assignment,
    _IRComponentCollector,
    _RenderedFieldSpec,
    _to_upper_snake,
)
from cdm_data_loaders.converters.emitters.pyspark import PySparkEmitter
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import (
    XSD_TO_PYARROW_ERRORS,
    NonStructXmlTagError,
    RecursiveNonJsonTypeError,
    XmlTagNotFoundError,
    XsdToPyArrow,
    XsdToPyArrowError,
    generate_pyarrow_schema,
    generate_pyarrow_schemas_at_level,
    generate_pyarrow_struct,
)
from cdm_data_loaders.converters.readers.xsd import (
    XsdReader,
    load_schema,
    resolve_elements_at_level,
    resolve_root_elements,
)
from cdm_data_loaders.schemas.pubmed.schema import (
    DELETE_CITATION_STRUCT,
    MEDLINE_CITATION_STRUCT,
    PMID_STRUCT,
    PUBMED_ARTICLE_STRUCT,
    PUBMED_BOOK_ARTICLE_STRUCT,
    PUBMED_BOOK_DATA_STRUCT,
    PUBMED_DATA_STRUCT,
)
from cdm_data_loaders.schemas.pubmed.schema import (
    SCHEMAS as PUBMED_SCHEMAS,
)
from cdm_data_loaders.schemas.uniprot.schema import (
    ENTRY_STRUCT as UNIPROT_ENTRY_STRUCT,
)
from cdm_data_loaders.schemas.uniprot.schema import (
    EVIDENCED_STRING_STRUCT,
    PROTEIN_STRUCT,
)
from cdm_data_loaders.schemas.uniprot.schema import (
    SCHEMAS as UNIPROT_SCHEMAS,
)
from cdm_data_loaders.schemas.uniref.schema import (
    DB_REFERENCE_STRUCT,
    ENTRY_STRUCT,
    MEMBER_STRUCT,
    PROPERTY_STRUCT,
    SEQUENCE_STRUCT,
)
from cdm_data_loaders.schemas.uniref.schema import (
    SCHEMAS as UNIREF_SCHEMAS,
)
from cdm_data_loaders.utils.xml_utils import local_name
from scripts.gen_converter_spec import XSD_TO_PYARROW, render
from scripts.gen_pyarrow_schema import (
    CONFIGS as SCHEMA_CODEGEN_CONFIGS,
)
from scripts.gen_pyarrow_schema import (
    main as gen_pyarrow_schema_main,
)
from tests.conftest import BASE_DIR
from tests.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases, load_rules
from tests.pyarrow_spec_helpers import (
    assert_expected_table_valid,
    assert_rows_and_table_match_expected,
    assert_schema_matches_expected,
    list_field,
)

XSD_DATA_DIR = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()
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


def _extract_matching_elements(xml_path: Path, xml_tag: str) -> list[Element]:
    """Parse xml_path and return all elements matching xml_tag at the root or as a direct child of root."""
    local_tag = local_name(xml_tag)
    root = parse(str(xml_path)).getroot()
    if local_name(root.tag) == local_tag:
        return [root]
    return [child for child in root if isinstance(child.tag, str) and local_name(child.tag) == local_tag]


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


def test_manifests_pass_every_error_fixture_listed() -> None:
    """Every XSD file in spec/errors is listed in the error manifest."""
    listed = {err.file for err in ERROR_CASES}
    on_disk = set((XSD_DATA_DIR / "spec" / "errors").glob("*.xsd"))
    assert sorted(str(p) for p in on_disk - listed) == []


def test_manifests_pass_error_codes_unique_and_covered() -> None:
    """Error codes are unique, every error manifest entry uses a known code, and every code has a fixture."""
    assert len(ERRORS_BY_CODE) == len(XSD_TO_PYARROW_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in XSD_TO_PYARROW_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


@pytest.mark.docs
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
        by_tag[local_name(elem.tag)].append(tostring(clone, encoding="unicode"))

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
def test_render_schema_module_pass_cases(case: Case, run: Run) -> None:
    """render_schema_module emits executable Python whose SCHEMA and SCHEMAS match the expected PyArrow schema."""
    xml_tag = _xml_tag(run)
    local_tag = local_name(xml_tag)
    source = render_schema_module(
        SchemaCodegenConfig(
            xsd_file=case.schema,
            output=Path("generated_schema.py"),
            docstring=f"Generated schema for {case.name} ({run.id}).",
            xml_level=None,
            xml_tag=xml_tag,
            max_recursion_depth=_max_recursion_depth(run),
        )
    )
    namespace: dict[str, object] = {}
    exec(source, namespace)  # noqa: S102

    generated_schema = namespace["SCHEMA"]
    assert isinstance(generated_schema, pa.Schema)
    assert_schema_matches_expected(generated_schema, case.expected_schema, run.schema_attr)

    schemas_map = namespace["SCHEMAS"]
    assert isinstance(schemas_map, dict)
    assert schemas_map[local_tag] is generated_schema

    if local_tag != "entry":
        tag_schema_var = f"{_to_upper_snake(local_tag)}_SCHEMA"
        assert namespace[tag_schema_var] is generated_schema

    struct_vars = [val for name, val in namespace.items() if name.endswith("_STRUCT")]
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


# Errors


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


@pytest.mark.parametrize(
    ("xsd_rel_path", "root_tag", "expected_roots"),
    [
        pytest.param("examples/uniprot/uniprot.xsd", None, ["uniprot"], id="uniprot-deduced"),
        pytest.param(
            "examples/uniref/uniref.xsd",
            None,
            ["UniRef100", "UniRef90", "UniRef50", "UniRef"],
            id="uniref-deduced",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            None,
            ["PubmedArticleSet", "BookDocumentSet", "PubmedBookArticleSet", "ContractNumber", "URL"],
            id="pubmed-deduced",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            "PubmedArticleSet",
            ["PubmedArticleSet"],
            id="pubmed-explicit-root-tag",
        ),
    ],
)
def test_resolve_root_elements_pass_deduces_or_resolves_roots(
    xsd_rel_path: str,
    root_tag: str | None,
    expected_roots: list[str],
) -> None:
    """resolve_root_elements deduces unreferenced global roots or resolves an explicit root_tag."""
    schema = load_schema(XSD_DATA_DIR / xsd_rel_path)
    roots = resolve_root_elements(schema, root_tag=root_tag)
    assert [local_name(elem.name) for elem in roots if elem.name is not None] == expected_roots


@pytest.mark.parametrize(
    ("xsd_rel_path", "level", "root_tag", "expected_tags"),
    [
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            1,
            "PubmedArticleSet",
            ["PubmedArticle", "PubmedBookArticle", "DeleteCitation"],
            id="pubmed-level-1",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            2,
            "PubmedArticleSet",
            ["MedlineCitation", "PubmedData", "BookDocument", "PubmedBookData", "PMID"],
            id="pubmed-level-2",
        ),
        pytest.param(
            "examples/uniprot/uniprot.xsd",
            1,
            None,
            ["entry", "copyright"],
            id="uniprot-level-1",
        ),
        pytest.param(
            "examples/uniprot/uniprot.xsd",
            2,
            None,
            [
                "accession",
                "name",
                "protein",
                "gene",
                "organism",
                "organismHost",
                "geneLocation",
                "reference",
                "comment",
                "dbReference",
                "proteinExistence",
                "keyword",
                "feature",
                "evidence",
                "sequence",
            ],
            id="uniprot-level-2",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            0,
            "UniRef50",
            ["UniRef50"],
            id="uniref-level-0",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            1,
            None,
            ["entry"],
            id="uniref-level-1",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            2,
            None,
            ["name", "property", "representativeMember", "member"],
            id="uniref-level-2",
        ),
    ],
)
def test_resolve_elements_at_level_pass_level_1_and_level_2(
    xsd_rel_path: str,
    level: int,
    root_tag: str | None,
    expected_tags: list[str],
) -> None:
    """resolve_elements_at_level deduces all element declarations at level 0, 1, and 2."""
    schema = load_schema(XSD_DATA_DIR / xsd_rel_path)
    elements = resolve_elements_at_level(schema, level, root_tag=root_tag)
    assert list(elements.keys()) == expected_tags


@pytest.mark.parametrize(
    ("level", "exc_cls", "match"),
    [
        pytest.param(-1, ValueError, "level must be >= 0", id="negative-level"),
        pytest.param(10, XmlTagNotFoundError, "No XML elements found at level 10", id="beyond-max-depth"),
    ],
)
def test_resolve_elements_at_level_fail_invalid_or_empty_level(
    level: int,
    exc_cls: type[Exception],
    match: str,
) -> None:
    """resolve_elements_at_level raises ValueError for level < 0 and XmlTagNotFoundError beyond max depth."""
    schema = load_schema(XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd")
    with pytest.raises(exc_cls, match=match):
        resolve_elements_at_level(schema, level, error_type=XsdToPyArrowError, error_by_code=ERRORS_BY_CODE)


def test_resolve_root_elements_fail_or_cyclic_global_elements() -> None:
    """resolve_root_elements raises XmlTagNotFoundError on empty schemas and falls back to all roots on cycles."""
    empty_schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:simpleType name="OnlyType"><xs:restriction base="xs:string"/></xs:simpleType>
        </xs:schema>
        """
    )
    with pytest.raises(XmlTagNotFoundError, match="No global elements found in XSD schema"):
        resolve_root_elements(empty_schema, error_type=XsdToPyArrowError, error_by_code=ERRORS_BY_CODE)

    cyclic_schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="alpha">
            <xs:complexType><xs:sequence><xs:element ref="beta" minOccurs="0"/></xs:sequence></xs:complexType>
          </xs:element>
          <xs:element name="beta">
            <xs:complexType><xs:sequence><xs:element ref="alpha" minOccurs="0"/></xs:sequence></xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    assert [local_name(e.name) for e in resolve_root_elements(cyclic_schema) if e.name] == ["alpha", "beta"]


def test_generate_pyarrow_struct_and_assemble_pyarrow_schema_pass_components() -> None:
    """generate_pyarrow_struct builds reusable pa.StructType components that assemble into table schemas."""
    uniref_xsd = load_schema(XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd")
    cache: dict[object, pa.StructType] = {}
    prop_struct = generate_pyarrow_struct(uniref_xsd, "property", struct_cache=cache)
    entry_struct = generate_pyarrow_struct(uniref_xsd, "entry", struct_cache=cache)

    assert prop_struct == PROPERTY_STRUCT
    assert entry_struct == ENTRY_STRUCT
    assert DB_REFERENCE_STRUCT.field("property").type.value_type == PROPERTY_STRUCT
    assert MEMBER_STRUCT.field("dbReference").type == DB_REFERENCE_STRUCT
    assert MEMBER_STRUCT.field("sequence").type == SEQUENCE_STRUCT
    assert assemble_pyarrow_schema(entry_struct).equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    pubmed_xsd = load_schema(XSD_DATA_DIR / "examples" / "pubmed_article" / "pubmed.xsd")
    pubmed_level_1 = generate_pyarrow_schemas_at_level(pubmed_xsd, 1, root_tag="PubmedArticleSet")
    assert set(pubmed_level_1.keys()) == {"PubmedArticle", "PubmedBookArticle", "DeleteCitation"}
    for tag, expected_schema in PUBMED_SCHEMAS.items():
        assert pubmed_level_1[tag].equals(expected_schema, check_metadata=True)
    assert PUBMED_ARTICLE_STRUCT.field("MedlineCitation").type == MEDLINE_CITATION_STRUCT
    assert PUBMED_ARTICLE_STRUCT.field("PubmedData").type == PUBMED_DATA_STRUCT
    assert PUBMED_BOOK_ARTICLE_STRUCT.field("PubmedBookData").type == PUBMED_BOOK_DATA_STRUCT
    assert DELETE_CITATION_STRUCT.field("PMID").type.value_type == PMID_STRUCT

    uniprot_xsd = load_schema(XSD_DATA_DIR / "examples" / "uniprot" / "uniprot.xsd")
    uniprot_level_1 = generate_pyarrow_schemas_at_level(uniprot_xsd, 1)
    assert set(uniprot_level_1.keys()) == {"entry", "copyright"}
    for tag, expected_schema in UNIPROT_SCHEMAS.items():
        assert uniprot_level_1[tag].equals(expected_schema, check_metadata=True)
    assert UNIPROT_ENTRY_STRUCT.field("protein").type == PROTEIN_STRUCT
    assert PROTEIN_STRUCT.field("allergenName").type == EVIDENCED_STRING_STRUCT


def test_component_schema_helpers_fail_non_struct_or_empty_inputs() -> None:
    """assemble_pyarrow_schema, generate_pyarrow_struct, and generate_pyarrow_schemas_at_level reject invalid inputs."""
    with pytest.raises(ValueError, match="Cannot assemble a PyArrow schema from an empty component"):
        assemble_pyarrow_schema([])

    uniprot_xsd = load_schema(XSD_DATA_DIR / "examples" / "uniprot" / "uniprot.xsd")
    with pytest.raises(NonStructXmlTagError, match="must have a complex type with attributes or child elements"):
        generate_pyarrow_struct(uniprot_xsd, "copyright")

    empty_marker_child_schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="root">
            <xs:complexType>
              <xs:sequence>
                <xs:element name="flag"><xs:complexType/></xs:element>
              </xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    with pytest.raises(NonStructXmlTagError, match="empty complex type"):
        generate_pyarrow_schemas_at_level(empty_marker_child_schema, 1)


@pytest.mark.parametrize(
    "config",
    SCHEMA_CODEGEN_CONFIGS,
    ids=[c.output.parent.name for c in SCHEMA_CODEGEN_CONFIGS],
)
def test_precomputed_schema_modules_pass_match_generator_output(config: SchemaCodegenConfig) -> None:
    """Committed uniref, uniprot, and pubmed schema.py modules match scripts.gen_pyarrow_schema output."""
    assert config.output.read_text(encoding="utf-8") == render_schema_module(config)


def test_gen_pyarrow_schema_cli_pass_and_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """xsd_to_pyarrow CLI and scripts.gen_pyarrow_schema.main support custom CLI args, --check, and default write mode."""
    assert gen_pyarrow_schema_main(["--check"]) == 0

    uniref_xsd = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"
    level_out = tmp_path / "uniref_level.py"
    xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(uniref_xsd), "--output", str(level_out)])
    assert "ENTRY_STRUCT: Final[pa.StructType]" in level_out.read_text(encoding="utf-8")

    tag_out = tmp_path / "uniref_tag.py"
    xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(uniref_xsd), "--xml-tag", "property", "--output", str(tag_out)])
    assert "PROPERTY_SCHEMA: Final[pa.Schema]" in tag_out.read_text(encoding="utf-8")
    assert "SCHEMA: Final[pa.Schema] = PROPERTY_SCHEMA" in tag_out.read_text(encoding="utf-8")

    with pytest.raises(ValidationError):
        xsd_to_pyarrow_cli.run(cli_args=["--xsd-file", str(uniref_xsd)])

    stale_target = tmp_path / "configured_schema.py"
    temp_cfg = SchemaCodegenConfig(
        xsd_file=uniref_xsd,
        output=stale_target,
        docstring="Temporary configured schema.",
        xml_level=1,
    )
    monkeypatch.setattr(gen_pyarrow_schema_module, "CONFIGS", (temp_cfg,))
    assert gen_pyarrow_schema_main(["--check"]) == 1
    assert "stale:" in capsys.readouterr().err

    assert gen_pyarrow_schema_main([]) == 0
    assert stale_target.is_file()
    assert gen_pyarrow_schema_main(["--check"]) == 0


def test_render_schema_module_pass_codegen_rules_and_formatting(tmp_path: Path) -> None:
    """render_schema_module and formatting helpers cover naming collisions, nillable variants, and line wrapping."""
    assert _fmt_str(r"path\to\item") == r'"path\\to\\item"'
    assert _fmt_str('quoted "value"') == "'quoted \"value\"'"
    assert _to_upper_snake("Type", strip_type_suffix=True) == "TYPE"
    assert _decode_metadata({b"xsd_type": b"xs:int", "comment": "doc"}) == {"comment": "doc", "xsd_type": "xs:int"}

    single_meta_spec = _RenderedFieldSpec(
        name="longCommentStruct",
        type_expr="SHARED_ITEM_STRUCT",
        nullable=True,
        metadata={"comment": "A" * 110},
    )
    formatted_single_meta = _format_field_indented(single_meta_spec, indent=8, trailing_comma=True)
    assert "metadata={\n" in formatted_single_meta
    assert f'"comment": "{"A" * 110}"\n' in formatted_single_meta

    medium_field = _RenderedFieldSpec(
        name="mediumField",
        type_expr="pa.string()",
        nullable=False,
        metadata={"xsd_type": "xs:string"},
    )
    wrapped_one_line = _format_struct_assignment(
        "VERY_LONG_COMPONENT_IDENTIFIER_FOR_SINGLE_FIELD_WRAP_TEST_STRUCT",
        [medium_field],
    )
    assert wrapped_one_line.startswith("VERY_LONG_COMPONENT_IDENTIFIER_FOR_SINGLE_FIELD_WRAP_TEST_STRUCT")
    assert "\n    [pa.field(" in wrapped_one_line

    wrapped_multi_line = _format_struct_assignment(
        "SHORT_STRUCT",
        [single_meta_spec],
    )
    assert "pa.struct(\n    [\n        pa.field(" in wrapped_multi_line

    short_scalar_exports = _format_schema_exports(
        {"s": _RenderedFieldSpec(name="#text", type_expr="pa.string()", nullable=True, metadata=None)},
        {},
    )
    assert (
        'S_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema([pa.field("#text", pa.string(), nullable=True)])'
        in short_scalar_exports
    )

    xsd_path = tmp_path / "codegen_edge_cases.xsd"
    long_doc = "Documentation text that exceeds one hundred and twenty characters on a single line " * 2
    xsd_path.write_text(
        f"""<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:complexType name="sharedItemType">
            <xs:sequence>
              <xs:element name="code" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
          <xs:complexType name="recNodeType">
            <xs:sequence>
              <xs:element name="descriptiveLabelWithLongName" type="xs:string" minOccurs="0"/>
              <xs:element name="secondaryDescriptiveAttribute" type="xs:string" minOccurs="0"/>
              <xs:element name="nextNode" type="recNodeType" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
          <xs:element name="root">
            <xs:complexType>
              <xs:sequence>
                <xs:element name="primaryRecord" type="sharedItemType"/>
                <xs:element name="secondaryRecord" type="sharedItemType"/>
                <xs:element name="nillableRecord" type="sharedItemType" nillable="true"/>
                <xs:element name="container">
                  <xs:complexType>
                    <xs:sequence>
                      <xs:element name="reqItem" type="sharedItemType"/>
                      <xs:element name="nilItem" type="sharedItemType" nillable="true"/>
                      <xs:element name="tree" type="recNodeType" minOccurs="0"/>
                      <xs:element name="sharedItem">
                        <xs:complexType>
                          <xs:attribute name="attrOnly" type="xs:string"/>
                        </xs:complexType>
                      </xs:element>
                      <xs:element name="branchA">
                        <xs:complexType>
                          <xs:sequence>
                            <xs:element name="dup">
                              <xs:complexType>
                                <xs:sequence><xs:element name="x" type="xs:int"/></xs:sequence>
                              </xs:complexType>
                            </xs:element>
                          </xs:sequence>
                        </xs:complexType>
                      </xs:element>
                      <xs:element name="branchB">
                        <xs:complexType>
                          <xs:sequence>
                            <xs:element name="dup">
                              <xs:complexType>
                                <xs:sequence><xs:element name="y" type="xs:string"/></xs:sequence>
                              </xs:complexType>
                            </xs:element>
                          </xs:sequence>
                        </xs:complexType>
                      </xs:element>
                    </xs:sequence>
                  </xs:complexType>
                </xs:element>
                <xs:element name="branchA_dup">
                  <xs:complexType>
                    <xs:attribute name="flag" type="xs:boolean"/>
                  </xs:complexType>
                </xs:element>
                <xs:element name="scalarBanner" type="xs:string">
                  <xs:annotation><xs:documentation>{long_doc}</xs:documentation></xs:annotation>
                </xs:element>
              </xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """,
        encoding="utf-8",
    )

    source = render_schema_module(
        SchemaCodegenConfig(
            xsd_file=xsd_path,
            output=tmp_path / "edge_schema.py",
            docstring="Edge-case schema module.",
            xml_level=1,
            max_recursion_depth=2,
        )
    )
    assert "SHARED_ITEM_STRUCT: Final[pa.StructType]" in source
    assert "SHARED_ITEM_NILLABLE_STRUCT: Final[pa.StructType]" in source
    assert "CONTAINER_SHARED_ITEM_STRUCT: Final[pa.StructType]" in source
    assert "BRANCH_A_DUP_STRUCT: Final[pa.StructType]" in source
    assert "BRANCH_B_DUP_STRUCT: Final[pa.StructType]" in source
    assert "BRANCH_A_DUP_2_STRUCT: Final[pa.StructType]" in source
    assert "SCALAR_BANNER_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(\n" in source
    assert "SCHEMAS: Final[dict[str, pa.Schema]] = {\n" in source

    namespace: dict[str, object] = {}
    exec(source, namespace)  # noqa: S102
    schemas_map = namespace["SCHEMAS"]
    assert isinstance(schemas_map, dict)
    expected_level_schemas = generate_pyarrow_schemas_at_level(load_schema(xsd_path), 1, max_recursion_depth=2)
    for table_name, expected_schema in expected_level_schemas.items():
        assert schemas_map[table_name].equals(expected_schema, check_metadata=True)


@pytest.mark.parametrize(
    ("xml_level", "xml_tag", "match"),
    [
        pytest.param(1, None, "empty complex type", id="empty-marker-at-level"),
        pytest.param(None, "mixedLeaf", "resolves to a struct", id="mixed-leaf-tag"),
    ],
)
def test_render_schema_module_fail_empty_marker_at_level(
    tmp_path: Path,
    xml_level: int | None,
    xml_tag: str | None,
    match: str,
) -> None:
    """render_schema_module raises NonStructXmlTagError for empty markers at level or mixed/simpleContent xml_tag."""
    xsd_path = tmp_path / "empty_marker_level.xsd"
    xsd_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="root">
            <xs:complexType>
              <xs:sequence>
                <xs:element name="emptyMarker"><xs:complexType/></xs:element>
                <xs:element name="mixedLeaf">
                  <xs:complexType mixed="true">
                    <xs:sequence><xs:element name="b" type="xs:string" minOccurs="0"/></xs:sequence>
                  </xs:complexType>
                </xs:element>
              </xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """,
        encoding="utf-8",
    )
    with pytest.raises(NonStructXmlTagError, match=match):
        render_schema_module(
            SchemaCodegenConfig(
                xsd_file=xsd_path,
                output=tmp_path / "out.py",
                docstring="Empty marker or mixed leaf test.",
                xml_level=xml_level,
                xml_tag=xml_tag,
            )
        )


@pytest.mark.parametrize(
    ("mode", "expected_field_names"),
    [
        pytest.param("untyped_attribute", ["_id"], id="untyped-attribute"),
        pytest.param("simple_content_non_xsd_type", ["_id", "#text"], id="simple-content-non-xsd-type"),
        pytest.param("missing_model_group", ["_id"], id="missing-model-group"),
    ],
)
def test_struct_field_specs_pass_defensive_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    expected_field_names: list[str],
) -> None:
    """_ComponentCollector._build_struct_field_specs and generate_pyarrow_struct handle defensive XSD fallbacks."""
    schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="record">
            <xs:complexType>
              <xs:attribute name="id" type="xs:int" use="required"/>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    record_el = schema.elements["record"]
    complex_type = record_el.type
    assert isinstance(complex_type, xmlschema.validators.XsdComplexType)

    if mode == "untyped_attribute":
        monkeypatch.setattr(complex_type.attributes["id"], "type", None)
    elif mode == "simple_content_non_xsd_type":
        monkeypatch.setattr(complex_type, "has_simple_content", lambda: True)
        monkeypatch.setattr(complex_type, "content", None)
    else:
        monkeypatch.setattr(complex_type, "has_simple_content", lambda: False)
        monkeypatch.setattr(complex_type, "content", None)

    doc = XsdReader(xml_tag="record", max_recursion_depth=2).read(schema, "record")
    pa_schema = PyArrowEmitter(max_nesting=100).emit(doc)
    collector = _IRComponentCollector()
    key = collector.collect_document("record", doc, pa_schema)
    assert isinstance(key, tuple)
    specs = collector.components[key].fields
    assert [spec.name for spec in specs] == expected_field_names
    assert specs[-1].type_expr == ("pa.string()" if mode != "missing_model_group" else "pa.int32()")
    assert specs[-1].metadata == {"xsd_type": "xs:string" if mode != "missing_model_group" else "xs:int"}

    struct_type = generate_pyarrow_struct(schema, "record")
    assert [field.name for field in struct_type] == expected_field_names
    expected_pa_type = pa.string() if mode != "missing_model_group" else pa.int32()
    assert struct_type[expected_field_names[-1]].type == expected_pa_type


def test_make_xml_level_converters_pass_struct_and_leaf_elements() -> None:
    """make_xml_level_converters converts both complex struct elements and scalar leaf elements at a level."""
    uniprot_xsd = load_schema(XSD_DATA_DIR / "examples" / "uniprot" / "uniprot.xsd")
    converters = make_xml_level_converters(uniprot_xsd, 1)
    assert set(converters.keys()) == {"entry", "copyright"}

    copyright_elem = fromstring(
        b'<copyright xmlns="https://uniprot.org/uniprot">Copyrighted by the UniProt Consortium</copyright>'
    )
    assert converters["copyright"](copyright_elem) == {"#text": "Copyrighted by the UniProt Consortium"}
    table = pa.Table.from_pylist([converters["copyright"](copyright_elem)], schema=UNIPROT_SCHEMAS["copyright"])
    assert table.to_pylist() == [{"#text": "Copyrighted by the UniProt Consortium"}]


def test_xsd_reader_and_xsd_to_pyarrow_facade_pass_ir_and_emitters(
    tmp_path: Path,
) -> None:
    """XsdReader builds SchemaDocument IR with source='xsd' provenance and emits to PyArrow, Python files, PySpark, dlt, and JSON Schema."""
    uniref_path = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"
    reader = XsdReader(xml_tag="entry")
    doc = reader.read(uniref_path)

    assert doc.name == "entry"
    assert doc.provenance.source == "xsd"
    assert doc.root.type == "object"
    assert not doc.root.nullable

    arrow_schema = PyArrowEmitter(max_nesting=100).emit(doc)
    assert arrow_schema.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    facade = XsdToPyArrow(xml_tag="entry")
    assert facade.convert(uniref_path).equals(UNIREF_SCHEMAS["entry"], check_metadata=True)
    empty_table = facade.convert_to_table(uniref_path)
    assert empty_table.num_rows == 0
    assert empty_table.schema.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    py_src = facade.convert_to_python(uniref_path)
    ns: dict[str, object] = {}
    exec(py_src, ns)  # noqa: S102
    assert isinstance(ns["SCHEMA"], pa.Schema)
    assert ns["SCHEMA"].equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    out_file = tmp_path / "uniref_entry_schema.py"
    assert facade.convert_to_python_file(uniref_path, out_file) == out_file
    assert out_file.read_text(encoding="utf-8") == py_src

    level_file = tmp_path / "uniref_level_schema.py"
    assert facade.convert_level_to_python_file(uniref_path, level_file, 1) == level_file
    assert level_file.read_text(encoding="utf-8") == facade.convert_level_to_python(uniref_path, 1)

    spark_schema = PySparkEmitter(max_nesting=100).emit(doc)
    assert "_id" in {f.name for f in spark_schema.fields}

    dlt_stored = DltEmitter(schema_name="uniref_entry").emit(doc)
    assert "_id" in dlt_stored["tables"]["uniref_entry"]["columns"]

    json_schema = JsonSchemaEmitter().emit(doc)
    assert json_schema["type"] == "object"
    assert "_id" in json_schema["properties"]

    with pytest.raises(ValueError, match="max_recursion_depth"):
        XsdReader(max_recursion_depth=0)
    with pytest.raises(TypeError, match="max_recursion_depth"):
        XsdReader(max_recursion_depth=True)  # type: ignore[arg-type]
    with pytest.raises(XmlTagNotFoundError, match="xml_tag must be specified"):
        XsdReader(error_type=XsdToPyArrowError, error_by_code=ERRORS_BY_CODE).read(uniref_path)
