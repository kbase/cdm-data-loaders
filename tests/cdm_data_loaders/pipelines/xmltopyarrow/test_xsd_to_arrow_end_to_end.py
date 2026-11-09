"""End-to-end specification tests: XSD + XML -> process_xml_file_to_pyarrow -> PyArrow Table batches."""

import datetime as dt
import json
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from cdm_data_loaders.converters.facades.xsd_to_pyarrow import generate_pyarrow_schema
from cdm_data_loaders.converters.readers.xsd import load_schema
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from cdm_data_loaders.readers.xml import process_xml_file_to_pyarrow
from tests.conftest import XSD_SPEC_DIR
from tests.helpers import strip_none_values


def _json_default(obj: object) -> str:
    """Serialize Python date and bytes values from Arrow tables for comparison with expected JSON."""
    if isinstance(obj, dt.date):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


@pytest.mark.parametrize(
    ("xsd_rel_path", "xml_rel_path", "expected_rel_path", "xml_tag"),
    [
        pytest.param(
            "spec/01_scalars_and_simple_types/schema.xsd",
            "spec/01_scalars_and_simple_types/data.xml",
            "spec/01_scalars_and_simple_types/expected.json",
            "record",
            id="spec-01-scalars-and-simple-types",
        ),
        pytest.param(
            "spec/02_namespaces_and_nillable/schema.xsd",
            "spec/02_namespaces_and_nillable/data.xml",
            "spec/02_namespaces_and_nillable/expected.json",
            "entry",
            id="spec-02-namespaces-and-nillable",
        ),
        pytest.param(
            "spec/03_mixed_content_raw_text/schema.xsd",
            "spec/03_mixed_content_raw_text/data.xml",
            "spec/03_mixed_content_raw_text/expected.json",
            "article",
            id="spec-03-mixed-content-raw-text",
        ),
        pytest.param(
            "spec/04_recursion_depth/schema.xsd",
            "spec/04_recursion_depth/data.xml",
            "spec/04_recursion_depth/expected.json",
            "record",
            id="spec-04-recursion-depth",
        ),
        pytest.param(
            "spec/05_groups_and_compositors/schema.xsd",
            "spec/05_groups_and_compositors/data.xml",
            "spec/05_groups_and_compositors/expected.json",
            "item",
            id="spec-05-groups-and-compositors",
        ),
        pytest.param(
            "spec/06_context_dependent_paths/schema.xsd",
            "spec/06_context_dependent_paths/data.xml",
            "spec/06_context_dependent_paths/expected.json",
            "root",
            id="spec-06-context-dependent-paths",
        ),
        pytest.param(
            "spec/07_multi_root_shared_type/schema.xsd",
            "spec/07_multi_root_shared_type/data.xml",
            "spec/07_multi_root_shared_type/expected.json",
            "entry",
            id="spec-07-multi-root-shared-type",
        ),
        pytest.param(
            "spec/08_nullability_and_empty_tags/schema.xsd",
            "spec/08_nullability_and_empty_tags/data.xml",
            "spec/08_nullability_and_empty_tags/expected.json",
            "entry",
            id="spec-08-nullability-and-empty-tags",
        ),
        pytest.param(
            "spec/09_empty_marker_elements/schema.xsd",
            "spec/09_empty_marker_elements/data.xml",
            "spec/09_empty_marker_elements/expected.json",
            "book",
            id="spec-09-empty-marker-elements",
        ),
        pytest.param(
            "spec/10_combinations/schema.xsd",
            "spec/10_combinations/data.xml",
            "spec/10_combinations/expected.json",
            "record",
            id="spec-10-combinations",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            "examples/uniref/minimal.xml",
            "examples/uniref/minimal_expected.json",
            "entry",
            id="example-uniref-minimal",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            "examples/uniref/maximal.xml",
            "examples/uniref/maximal_expected.json",
            "entry",
            id="example-uniref-maximal",
        ),
        pytest.param(
            "examples/uniprot/uniprot.xsd",
            "examples/uniprot/minimal.xml",
            "examples/uniprot/minimal_expected.json",
            "entry",
            id="example-uniprot-minimal",
        ),
        pytest.param(
            "examples/uniprot/uniprot.xsd",
            "examples/uniprot/maximal.xml",
            "examples/uniprot/maximal_expected.json",
            "entry",
            id="example-uniprot-maximal",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            "examples/pubmed_article/minimal.xml",
            "examples/pubmed_article/minimal_expected.json",
            "PubmedArticle",
            id="example-pubmed-article-minimal",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            "examples/pubmed_article/maximal.xml",
            "examples/pubmed_article/maximal_expected.json",
            "PubmedArticle",
            id="example-pubmed-article-maximal",
        ),
        pytest.param(
            "examples/pubmed_book_article/pubmed.xsd",
            "examples/pubmed_book_article/minimal.xml",
            "examples/pubmed_book_article/minimal_expected.json",
            "PubmedBookArticle",
            id="example-pubmed-book-article-minimal",
        ),
        pytest.param(
            "examples/pubmed_book_article/pubmed.xsd",
            "examples/pubmed_book_article/maximal.xml",
            "examples/pubmed_book_article/maximal_expected.json",
            "PubmedBookArticle",
            id="example-pubmed-book-article-maximal",
        ),
    ],
)
def test_process_xml_file_to_pyarrow_pass_yields_arrow_tables_for_spec_and_examples(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    *,
    xsd_rel_path: str,
    xml_rel_path: str,
    expected_rel_path: str,
    xml_tag: str,
) -> None:
    """process_xml_file_to_pyarrow yields pa.Table batches matching the XSD PyArrow schema and expected JSON."""
    xsd_path = XSD_SPEC_DIR / xsd_rel_path
    xml_path = XSD_SPEC_DIR / xml_rel_path
    expected = json.loads((XSD_SPEC_DIR / expected_rel_path).read_text(encoding="utf-8"))

    settings = settings_factory(
        table_name=xml_tag,
        xml_tag=xml_tag,
        xsd_file=str(xsd_path),
    )

    items = list(process_xml_file_to_pyarrow(xml_path, settings))
    assert len(items) == 1
    assert items[0].meta.table_name == xml_tag
    assert isinstance(items[0].data, pa.Table)

    table: pa.Table = items[0].data
    expected_pa_schema = generate_pyarrow_schema(load_schema(xsd_path), xml_tag, max_recursion_depth=2)
    assert table.schema.equals(expected_pa_schema, check_metadata=True)
    assert table.num_rows == len(expected)

    actual_rows = json.loads(json.dumps(strip_none_values(table.to_pylist()), default=_json_default))
    assert actual_rows == expected


def test_process_xml_file_to_pyarrow_pass_pubmed_level_1_multi_table_files(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    tmp_path: Path,
) -> None:
    """Level-1 parsing of PubMed XML routes PubmedArticle, PubmedBookArticle, and DeleteCitation into typed Arrow tables."""
    xsd_path = XSD_SPEC_DIR / "examples" / "pubmed_article" / "pubmed.xsd"
    article_xml = (XSD_SPEC_DIR / "examples" / "pubmed_article" / "minimal.xml").read_text(encoding="utf-8")
    book_xml = (XSD_SPEC_DIR / "examples" / "pubmed_book_article" / "minimal.xml").read_text(encoding="utf-8")

    article_body = article_xml.split('<PubmedArticleSet xmlns="http://www.w3.org/namespace/">', 1)[1].rsplit(
        "</PubmedArticleSet>", 1
    )[0]
    book_body = book_xml.split('<PubmedBookArticleSet xmlns="http://www.w3.org/namespace/">', 1)[1].rsplit(
        "</PubmedBookArticleSet>", 1
    )[0]
    combined_xml_path = tmp_path / "pubmed_combined.xml"
    combined_xml_path.write_text(
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<PubmedArticleSet xmlns="http://www.w3.org/namespace/">\n'
        f"{article_body}"
        f"{book_body}"
        '  <DeleteCitation>\n    <PMID Version="1">99999999</PMID>\n  </DeleteCitation>\n'
        "</PubmedArticleSet>\n",
        encoding="utf-8",
    )

    settings = settings_factory(
        xsd_file=str(xsd_path),
        xml_level=1,
        root_tag="PubmedArticleSet",
    )

    items = list(process_xml_file_to_pyarrow(combined_xml_path, settings))
    by_table: dict[str, pa.Table] = {item.meta.table_name: item.data for item in items}

    assert {name: table.num_rows for name, table in by_table.items()} == {
        "PubmedArticle": 1,
        "PubmedBookArticle": 1,
        "DeleteCitation": 1,
    }
    for table_name, table in by_table.items():
        assert table.schema.equals(settings.pyarrow_schemas[table_name], check_metadata=True)
    assert strip_none_values(by_table["DeleteCitation"].to_pylist()) == [
        {"PMID": [{"_Version": "1", "#text": "99999999"}]}
    ]


@pytest.mark.parametrize(
    ("domain", "xml_level", "expected_table_names", "expected_text_table", "expected_text_rows"),
    [
        pytest.param(
            "uniprot",
            1,
            {"entry", "copyright"},
            "copyright",
            [{"#text": "Copyrighted by the UniProt Consortium"}],
            id="uniprot-level-1",
        ),
        pytest.param(
            "uniref",
            2,
            {"name", "representativeMember"},
            "name",
            [{"#text": "Cluster: Glycogen synthase"}],
            id="uniref-level-2",
        ),
    ],
)
def test_process_xml_file_to_pyarrow_pass_uniprot_level_1_and_uniref_level_2(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    tmp_path: Path,
    domain: str,
    xml_level: int,
    expected_table_names: set[str],
    expected_text_table: str,
    expected_text_rows: list[dict[str, str]],
) -> None:
    """Level-1 parsing on UniProt yields entry and copyright tables; level-2 parsing on UniRef yields child tables."""
    if domain == "uniprot":
        minimal_uniprot = (XSD_SPEC_DIR / "examples" / "uniprot" / "minimal.xml").read_text(encoding="utf-8")
        xml_path = tmp_path / "uniprot_with_copyright.xml"
        xml_path.write_text(
            minimal_uniprot.replace(
                "</uniprot>",
                "  <copyright>Copyrighted by the UniProt Consortium</copyright>\n</uniprot>",
            ),
            encoding="utf-8",
        )
    else:
        xml_path = XSD_SPEC_DIR / "examples" / "uniref" / "minimal.xml"

    settings = settings_factory(
        xsd_file=str(XSD_SPEC_DIR / "examples" / domain / f"{domain}.xsd"),
        xml_level=xml_level,
    )
    items = list(process_xml_file_to_pyarrow(xml_path, settings))
    tables: dict[str, pa.Table] = {item.meta.table_name: item.data for item in items}
    assert set(tables.keys()) == expected_table_names
    assert tables[expected_text_table].to_pylist() == expected_text_rows
    for table_name, table in tables.items():
        assert table.num_rows == 1
        assert table.schema.equals(settings.pyarrow_schemas[table_name], check_metadata=True)
