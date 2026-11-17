"""Unit tests for PyArrow Python schema code generation and struct component assembly."""

from pathlib import Path
from typing import Final

import pyarrow as pa
import pytest
import xmlschema
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema

from cdm_data_loaders.cli.xsd_to_pyarrow import SchemaCodegenConfig, render_schema_module
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.emitters.pyarrow_codegen.collector import IRComponentCollector
from cdm_data_loaders.converters.emitters.pyarrow_codegen.formatting import (
    format_field_indented,
    format_schema_exports,
    format_struct_assignment,
    render_scalar_type_expr,
)
from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import (
    RenderedFieldSpec,
    decode_sorted_metadata,
    fmt_str,
    to_upper_snake,
)
from cdm_data_loaders.converters.emitters.pyarrow_codegen.renderer import (
    render_python_schema_module,
    write_python_schema_file,
)
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import NonStructXmlTagError
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import (
    generate_pyarrow_schemas_at_level,
    generate_pyarrow_struct,
)
from cdm_data_loaders.converters.sources.dlt import DltReader
from cdm_data_loaders.converters.sources.iceberg import IcebergReader
from cdm_data_loaders.converters.sources.xsd import XsdReader
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
from cdm_data_loaders.utils.arrow_schema import assemble_pyarrow_schema
from cdm_data_loaders.validation.xsd.traversal import load_schema
from tests._support.paths_and_constants import BASE_DIR
from tests._support.pyarrow_spec import load_module

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()


def test_render_python_schema_module_and_write_file_pass_iceberg_and_dlt(tmp_path: Path) -> None:
    """render_python_schema_module and write_python_schema_file emit executable Python modules."""
    iceberg_schema = Schema(
        iceberg_types.NestedField(1, "id", iceberg_types.LongType(), required=True),
        iceberg_types.NestedField(2, "price", iceberg_types.DecimalType(10, 2)),
        iceberg_types.NestedField(3, "hash", iceberg_types.FixedType(16)),
        iceberg_types.NestedField(4, "at", iceberg_types.TimeType()),
        iceberg_types.NestedField(
            5,
            "inner",
            iceberg_types.StructType(iceberg_types.NestedField(6, "x", iceberg_types.IntegerType(), required=True)),
        ),
    )
    iceberg_doc = IcebergReader().read(iceberg_schema)
    emitter = PyArrowEmitter()

    source = render_python_schema_module(emitter, {"iceberg_record": iceberg_doc})
    iceberg_out = tmp_path / "iceberg_schema.py"
    iceberg_out.write_text(source, encoding="utf-8")
    iceberg_mod = load_module(iceberg_out)
    assert iceberg_mod.SCHEMA.equals(emitter.emit(iceberg_doc), check_metadata=True)
    assert iceberg_mod.ICEBERG_RECORD_SCHEMA.equals(emitter.emit(iceberg_doc), check_metadata=True)

    dlt_docs = DltReader().read(
        {
            "orders": {
                "columns": {
                    "id": {"data_type": "bigint", "precision": 16, "nullable": False},
                    "balance": {"data_type": "wei"},
                    "seen": {"data_type": "timestamp", "precision": 3},
                }
            },
            "items": {
                "columns": {
                    "sku": {"data_type": "text", "nullable": False},
                    "raw": {"data_type": "binary"},
                }
            },
        }
    )
    out_file = tmp_path / "dlt_schemas.py"
    written_path = write_python_schema_file(emitter, dlt_docs, out_file)
    assert written_path == out_file.resolve()

    dlt_mod = load_module(out_file)
    assert set(dlt_mod.SCHEMAS) == {"orders", "items"}
    assert dlt_mod.SCHEMAS["orders"].equals(emitter.emit(dlt_docs["orders"]), check_metadata=True)
    assert dlt_mod.SCHEMAS["items"].equals(emitter.emit(dlt_docs["items"]), check_metadata=True)


@pytest.mark.parametrize(
    ("data_type", "expected_expr"),
    [
        pytest.param(pa.string(), "pa.string()", id="string"),
        pytest.param(pa.large_string(), "pa.large_string()", id="large-string"),
        pytest.param(pa.binary(16), "pa.binary(16)", id="fixed-binary"),
        pytest.param(pa.decimal128(10, 2), "pa.decimal128(10, 2)", id="decimal128"),
        pytest.param(pa.decimal256(76, 0), "pa.decimal256(76, 0)", id="decimal256"),
        pytest.param(pa.timestamp("us", tz="UTC"), 'pa.timestamp("us", tz="UTC")', id="timestamp-utc"),
        pytest.param(pa.timestamp("ms"), 'pa.timestamp("ms")', id="timestamp-naive"),
        pytest.param(pa.time32("ms"), 'pa.time32("ms")', id="time32"),
        pytest.param(pa.time64("us"), 'pa.time64("us")', id="time64"),
    ],
)
def test_render_scalar_type_expr_pass_types(data_type: pa.DataType, expected_expr: str) -> None:
    """render_scalar_type_expr formats PyArrow scalar types as Python constructor calls."""
    assert render_scalar_type_expr(data_type) == expected_expr


@pytest.mark.parametrize(
    ("raw_name", "strip_suffix", "expected"),
    [
        pytest.param("PubmedArticleType", True, "PUBMED_ARTICLE", id="strip-type-suffix"),
        pytest.param("PubmedArticleType", False, "PUBMED_ARTICLE_TYPE", id="keep-type-suffix"),
        pytest.param("123_tag", False, "F_123_TAG", id="leading-digit"),
        pytest.param("---", False, "FIELD", id="non-alnum"),
    ],
)
def test_to_upper_snake_pass_identifiers(raw_name: str, strip_suffix: bool, expected: str) -> None:
    """to_upper_snake normalizes identifiers to valid UPPER_SNAKE_CASE constants."""
    assert to_upper_snake(raw_name, strip_type_suffix=strip_suffix) == expected


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


def test_render_schema_module_pass_codegen_rules_and_formatting(tmp_path: Path) -> None:
    """render_schema_module and formatting helpers cover naming collisions, nillable variants, and line wrapping."""
    assert fmt_str(r"path\to\item") == r'"path\\to\\item"'
    assert fmt_str('quoted "value"') == "'quoted \"value\"'"
    assert to_upper_snake("Type", strip_type_suffix=True) == "TYPE"
    assert decode_sorted_metadata({b"xsd_type": b"xs:int", "comment": "doc"}) == {
        "comment": "doc",
        "xsd_type": "xs:int",
    }

    single_meta_spec = RenderedFieldSpec(
        name="longCommentStruct",
        type_expr="SHARED_ITEM_STRUCT",
        nullable=True,
        metadata={"comment": "A" * 110},
    )
    formatted_single_meta = format_field_indented(single_meta_spec, indent=8, trailing_comma=True)
    assert "metadata={\n" in formatted_single_meta
    assert f'"comment": "{"A" * 110}"\n' in formatted_single_meta

    medium_field = RenderedFieldSpec(
        name="mediumField",
        type_expr="pa.string()",
        nullable=False,
        metadata={"xsd_type": "xs:string"},
    )
    wrapped_one_line = format_struct_assignment(
        "VERY_LONG_COMPONENT_IDENTIFIER_FOR_SINGLE_FIELD_WRAP_TEST_STRUCT",
        [medium_field],
    )
    assert wrapped_one_line.startswith("VERY_LONG_COMPONENT_IDENTIFIER_FOR_SINGLE_FIELD_WRAP_TEST_STRUCT")
    assert "\n    [pa.field(" in wrapped_one_line

    wrapped_multi_line = format_struct_assignment(
        "SHORT_STRUCT",
        [single_meta_spec],
    )
    assert "pa.struct(\n    [\n        pa.field(" in wrapped_multi_line

    short_scalar_exports = format_schema_exports(
        {"s": RenderedFieldSpec(name="#text", type_expr="pa.string()", nullable=True, metadata=None)},
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

    edge_out = tmp_path / "edge_schema.py"
    source = render_schema_module(
        SchemaCodegenConfig(
            xsd_file=xsd_path,
            output=edge_out,
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

    edge_out.write_text(source, encoding="utf-8")
    edge_mod = load_module(edge_out)
    schemas_map = edge_mod.SCHEMAS
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
    """IRComponentCollector and generate_pyarrow_struct handle defensive XSD fallbacks."""
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
    collector = IRComponentCollector()
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
