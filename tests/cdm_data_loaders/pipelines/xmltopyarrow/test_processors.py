"""Unit tests for ``process_xml_file_to_pyarrow`` in ``cdm_data_loaders.pipelines.xmltopyarrow.processors``."""

import gzip
import logging
from collections.abc import Generator
from pathlib import Path
from typing import Final
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import xmlschema
from dlt.common.storages.fsspec_filesystem import FileItemDict
from lxml.etree import XMLSyntaxError, _Element

import cdm_data_loaders.coercion.xml as xml_coercion_module
import cdm_data_loaders.pipelines.xml_common as xml_common_module
import cdm_data_loaders.readers.xml as xml_module
from cdm_data_loaders.coercion.xml import (
    make_xml_element_converter,
    make_xml_level_converters,
)
from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import (
    generate_pyarrow_schema,
    generate_pyarrow_schemas_at_level,
)
from cdm_data_loaders.core.buffer import DictBuffer
from cdm_data_loaders.core.fields import BUFFER_SIZE, DEFAULTS, LOG_INTERVAL
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.pipelines.xmltopyarrow.processors import process_xml_file_to_pyarrow
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from tests._support.fs_backends import FsBackend
from tests._support.xml_samples import (
    PEOPLE_XML_2,
    PEOPLE_XML_5,
    PEOPLE_XML_EMPTY,
    PEOPLE_XML_MALFORMED,
    PEOPLE_XML_TO_PYARROW_PARSED,
    PEOPLE_XSD,
    UNIREF_LIKE_ENTRY_XML,
    table_and_data,
    write_xml_file,
)

MULTI_ENTITY_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="catalog">
        <xs:complexType>
            <xs:choice maxOccurs="unbounded">
                <xs:element name="book">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="xs:string"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
                <xs:element name="magazine">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="issue" type="xs:integer"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
                <xs:element name="copyright" type="xs:string"/>
            </xs:choice>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

MULTI_ENTITY_XML: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <book id="b1"><title>Dune</title></book>
    <magazine id="m1"><issue>42</issue></magazine>
    <book id="b2"><title>Foundation</title></book>
    <copyright>Copyright 2026</copyright>
</catalog>
"""


def fake_xmltopyarrow_settings(
    *,
    table_name: str = "people",
    xml_tag: str = "person",
    xsd_text: str = PEOPLE_XSD,
    buffer_size: int = DEFAULTS[BUFFER_SIZE],
    log_interval: int = DEFAULTS[LOG_INTERVAL],
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> XmlToPyArrowSettings:
    """Build a ``MagicMock`` ``XmlToPyArrowSettings`` configured for single-tag mode.

    :param table_name: Target table name.
    :type table_name: str
    :param xml_tag: XML element tag name to stream.
    :type xml_tag: str
    :param xsd_text: Raw XSD document text.
    :type xsd_text: str
    :param buffer_size: Maximum buffered rows per page.
    :type buffer_size: int
    :param log_interval: Entry count interval for progress logs.
    :type log_interval: int
    :param max_recursion_depth: Maximum XSD recursion depth.
    :type max_recursion_depth: int
    :return: Configured ``XmlToPyArrowSettings`` mock.
    :rtype: XmlToPyArrowSettings
    """
    settings = MagicMock(spec=XmlToPyArrowSettings)
    settings.buffer_size = buffer_size
    settings.log_interval = log_interval
    settings.use_pyarrow = True
    settings.get_table_transform.return_value = None
    settings.table_name = table_name
    settings.xml_tag = xml_tag
    settings.xml_level = None
    settings.root_tag = None
    settings.max_recursion_depth = max_recursion_depth
    schema = xmlschema.XMLSchema(xsd_text)
    settings.xsd_schema = schema
    pyarrow_schema = generate_pyarrow_schema(
        schema,
        xml_tag,
        max_recursion_depth=max_recursion_depth,
    )
    settings.pyarrow_schema = pyarrow_schema
    settings.pyarrow_schemas = None
    settings.schema_map = ArrowSchemaRegistry(registry={table_name: pyarrow_schema})
    settings.xml_element_converter = make_xml_element_converter(
        schema,
        xml_tag,
        max_recursion_depth=max_recursion_depth,
    )
    settings.xml_level_converters = None
    return settings


def fake_xmltopyarrow_level_settings(
    *,
    xml_level: int = 1,
    root_tag: str | None = None,
    xsd_text: str = MULTI_ENTITY_XSD,
    buffer_size: int = DEFAULTS[BUFFER_SIZE],
    log_interval: int = DEFAULTS[LOG_INTERVAL],
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> XmlToPyArrowSettings:
    """Build a ``MagicMock`` ``XmlToPyArrowSettings`` configured for level-based multi-table mode.

    :param xml_level: Target XML nesting depth.
    :type xml_level: int
    :param root_tag: Optional root XML tag anchor.
    :type root_tag: str | None
    :param xsd_text: Raw XSD document text.
    :type xsd_text: str
    :param buffer_size: Maximum buffered rows per page.
    :type buffer_size: int
    :param log_interval: Entry count interval for progress logs.
    :type log_interval: int
    :param max_recursion_depth: Maximum XSD recursion depth.
    :type max_recursion_depth: int
    :return: Configured ``XmlToPyArrowSettings`` mock.
    :rtype: XmlToPyArrowSettings
    """
    settings = MagicMock(spec=XmlToPyArrowSettings)
    settings.buffer_size = buffer_size
    settings.log_interval = log_interval
    settings.use_pyarrow = True
    settings.get_table_transform.return_value = None
    settings.table_name = None
    settings.xml_tag = None
    settings.xml_level = xml_level
    settings.root_tag = root_tag
    settings.max_recursion_depth = max_recursion_depth
    schema = xmlschema.XMLSchema(xsd_text)
    settings.xsd_schema = schema
    settings.pyarrow_schema = None
    pyarrow_schemas = generate_pyarrow_schemas_at_level(
        schema,
        xml_level,
        root_tag=root_tag,
        max_recursion_depth=max_recursion_depth,
    )
    settings.pyarrow_schemas = pyarrow_schemas
    settings.schema_map = ArrowSchemaRegistry(registry=pyarrow_schemas)
    settings.xml_element_converter = None
    settings.xml_level_converters = make_xml_level_converters(
        schema,
        xml_level,
        root_tag=root_tag,
        max_recursion_depth=max_recursion_depth,
    )
    return settings


@pytest.mark.parametrize(
    ("buffer_size", "expected_batch_sizes"),
    [
        pytest.param(1, [1, 1, 1, 1, 1], id="one-row-batches"),
        pytest.param(2, [2, 2, 1], id="two-row-batches-with-remainder"),
        pytest.param(3, [3, 2], id="three-row-batches-with-remainder"),
        pytest.param(10, [5], id="larger-than-input"),
    ],
)
def test_process_xml_file_to_pyarrow_pass_buffer_size_controls_batching(
    tmp_path: Path,
    buffer_size: int,
    expected_batch_sizes: list[int],
) -> None:
    """Rows are yielded as Arrow tables in batches of at most buffer_size."""
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)

    items = table_and_data(process_xml_file_to_pyarrow(file_path, fake_xmltopyarrow_settings(buffer_size=buffer_size)))
    batches = [batch.to_pylist() for _, batch in items]

    assert [len(rows) for rows in batches] == expected_batch_sizes
    assert [row for rows in batches for row in rows] == PEOPLE_XML_TO_PYARROW_PARSED
    assert {table_name for table_name, _ in items} == {"people"}


@pytest.mark.parametrize("gzip_compress", [False, True], ids=["plain", "gzip"])
def test_process_xml_file_to_pyarrow_pass_gzip_file(tmp_path: Path, gzip_compress: bool) -> None:
    """Gzip-compressed XML input (.gz suffix) is decompressed and coerced into Arrow tables."""
    filename = "compressed.xml.gz" if gzip_compress else "plain.xml"
    path = write_xml_file(tmp_path, filename, PEOPLE_XML_5, gzip_compress=gzip_compress)

    items = table_and_data(process_xml_file_to_pyarrow(path, fake_xmltopyarrow_settings()))
    rows = [row for _, batch in items for row in batch.to_pylist()]
    assert rows == PEOPLE_XML_TO_PYARROW_PARSED


def test_process_xml_file_to_pyarrow_pass_logs_reading_info_message_once(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """INFO-level 'Reading from <path>' message is logged once with the file path."""
    caplog.set_level(logging.INFO)
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)
    list(process_xml_file_to_pyarrow(file_path, fake_xmltopyarrow_settings()))

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(info_records) == 1
    assert info_records[0].msg == "Reading from %s"
    assert info_records[0].args == (str(file_path),)


def test_process_xml_file_to_pyarrow_pass_no_matching_elements_logs_no_progress(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A well-formed file with no matching elements yields nothing and logs no progress."""
    caplog.set_level(logging.DEBUG)
    file_path = write_xml_file(tmp_path, "empty.xml", PEOPLE_XML_EMPTY)
    items = list(process_xml_file_to_pyarrow(file_path, fake_xmltopyarrow_settings()))

    assert items == []
    progress_records = [r for r in caplog.records if r.msg.startswith("Processed")]
    assert progress_records == []


@pytest.mark.parametrize(
    ("log_interval", "expected_interim_counts", "expected_final_count"),
    [
        pytest.param(1, [1, 2, 3, 4, 5], None, id="divides-evenly-every-entry"),
        pytest.param(5, [5], None, id="divides-evenly-equal-to-count"),
        pytest.param(2, [2, 4], 5, id="remainder-leaves-trailing-entries"),
        pytest.param(10, [], 5, id="interval-larger-than-entry-count"),
    ],
)
def test_process_xml_file_to_pyarrow_pass_log_interval_boundary(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    log_interval: int,
    expected_interim_counts: list[int],
    expected_final_count: int | None,
) -> None:
    """Entry counts logged at interim checkpoints and trailing summary match log_interval."""
    caplog.set_level(logging.DEBUG)
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)
    list(process_xml_file_to_pyarrow(file_path, fake_xmltopyarrow_settings(log_interval=log_interval)))

    interim_counts = [r.args[0] for r in caplog.records if r.msg == "Processed %d entries"]
    final_records = [r for r in caplog.records if r.msg == "Processed %d entries from %s"]

    assert interim_counts == expected_interim_counts
    if expected_final_count is None:
        assert final_records == []
    else:
        assert len(final_records) == 1
        assert final_records[0].args == (expected_final_count, file_path.name)


def test_process_xml_file_to_pyarrow_fail_missing_file(tmp_path: Path) -> None:
    """A missing file path raises FileNotFoundError when the generator is consumed."""
    missing_path = tmp_path / "does_not_exist.xml"
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        list(process_xml_file_to_pyarrow(missing_path, fake_xmltopyarrow_settings()))


def test_process_xml_file_to_pyarrow_fail_malformed_xml(tmp_path: Path) -> None:
    """Malformed XML raises lxml XMLSyntaxError during streaming."""
    file_path = write_xml_file(tmp_path, "malformed.xml", PEOPLE_XML_MALFORMED)
    with pytest.raises(XMLSyntaxError, match="Opening and ending tag mismatch"):
        list(process_xml_file_to_pyarrow(file_path, fake_xmltopyarrow_settings()))


def test_process_xml_file_to_pyarrow_pass_custom_table_name_and_xml_tag_are_respected(tmp_path: Path) -> None:
    """Rows land under settings.table_name as a PyArrow Table matched by settings.xml_tag."""
    file_path = write_xml_file(tmp_path, "two.xml", PEOPLE_XML_2)
    settings = fake_xmltopyarrow_settings(table_name="library_entries", xml_tag="person")

    tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))

    assert [table_name for table_name, _ in tagged] == ["library_entries"]
    assert isinstance(tagged[0][1], pa.Table)
    assert tagged[0][1].to_pylist() == PEOPLE_XML_TO_PYARROW_PARSED[:2]


def test_process_xml_file_to_pyarrow_pass_resolves_default_namespace_on_unprefixed_tag(tmp_path: Path) -> None:
    """An unprefixed xml_tag resolves the root default namespace via parse_head_matter."""
    namespace = "http://example.com/people"
    namespaced_xsd = f"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="{namespace}"
           xmlns="{namespace}"
           elementFormDefault="qualified">
    <xs:element name="people">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="person" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="name" type="xs:string"/>
                            <xs:element name="email" type="xs:string" minOccurs="0"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""
    namespaced_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<people xmlns="{namespace}">
    <person id="1"><name>Anne Example</name><email>a@example.com</email></person>
    <person id="2"><name>Belinda Carlisle</name><email>b@example.com</email></person>
</people>
"""
    file_path = write_xml_file(tmp_path, "namespaced.xml", namespaced_xml)
    settings = fake_xmltopyarrow_settings(table_name="people", xml_tag="person", xsd_text=namespaced_xsd)

    tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))

    assert [table_name for table_name, _ in tagged] == ["people"]
    assert tagged[0][1].to_pylist() == PEOPLE_XML_TO_PYARROW_PARSED[:2]


@pytest.mark.parametrize("relative_path", ["namespaced.xml", "sub/namespaced.xml.gz"])
def test_process_xml_file_to_pyarrow_pass_default_namespace_file_item_opened_once(
    fs_backend: FsBackend,
    relative_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default-namespace resolution and streaming share one open handle across all backends."""
    namespace = "http://example.com/people"
    namespaced_xsd = PEOPLE_XSD.replace(
        'xmlns:xs="http://www.w3.org/2001/XMLSchema"',
        f'xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="{namespace}" xmlns="{namespace}" '
        'elementFormDefault="qualified"',
    )
    data = PEOPLE_XML_2.replace("<people>", f'<people xmlns="{namespace}">').encode("utf-8")
    fs_backend.put(relative_path, gzip.compress(data) if relative_path.endswith(".gz") else data)
    settings = fake_xmltopyarrow_settings(table_name="people", xml_tag="person", xsd_text=namespaced_xsd)
    item = fs_backend.file_item(relative_path)
    open_spy = MagicMock(wraps=item.open)
    monkeypatch.setattr(item, "open", open_spy)

    tagged = table_and_data(process_xml_file_to_pyarrow(item, settings))

    assert open_spy.call_count == 1
    assert [table_name for table_name, _ in tagged] == ["people"]
    assert tagged[0][1].to_pylist() == PEOPLE_XML_TO_PYARROW_PARSED[:2]


def test_process_xml_file_to_pyarrow_pass_with_xsd_file_yields_arrow_table_with_schema_lists(
    tmp_path: Path,
    test_data_dir: Path,
) -> None:
    """process_xml_file_to_pyarrow yields a PyArrow Table with schema-driven list and struct fields."""
    file_path = write_xml_file(tmp_path, "entry.xml", UNIREF_LIKE_ENTRY_XML)
    settings = fake_xmltopyarrow_settings(
        table_name="entry",
        xml_tag="entry",
        xsd_text=(test_data_dir / "xsd" / "uniref_like.xsd").read_text(encoding="utf-8"),
        buffer_size=100,
        log_interval=1000,
    )

    items = table_and_data(process_xml_file_to_pyarrow(file_path, settings))

    assert len(items) == 1
    table_name, table = items[0]
    assert table_name == "entry"
    assert isinstance(table, pa.Table)
    assert settings.schema_map is not None
    assert table.schema.equals(settings.schema_map.registry["entry"], check_metadata=True)
    assert table.to_pylist() == [
        {
            "property": [{"_type": "member count", "_value": "1"}],
            "representativeMember": {
                "dbReference": {"property": [{"_type": "isSeed", "_value": "true"}]},
                "sequence": None,
            },
            "member": None,
        }
    ]


def test_process_xml_file_to_pyarrow_pass_clark_notation_tag_skips_head_matter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """process_xml_file_to_pyarrow skips parse_head_matter when xml_tag uses Clark notation."""
    namespaced_xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="http://example.org/ns"
           xmlns:tns="http://example.org/ns"
           elementFormDefault="qualified">
    <xs:element name="people">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="person" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="name" type="xs:string"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""
    namespaced_xml = """<?xml version="1.0" encoding="UTF-8"?>
<people xmlns="http://example.org/ns">
    <person id="1"><name>Anne Example</name></person>
</people>
"""
    xml_path = write_xml_file(tmp_path, "people_ns.xml", namespaced_xml)
    clark_tag = "{http://example.org/ns}person"
    settings = fake_xmltopyarrow_settings(
        table_name="people",
        xml_tag=clark_tag,
        xsd_text=namespaced_xsd,
    )

    head_matter_mock = MagicMock()
    monkeypatch.setattr(xml_module, "parse_head_matter", head_matter_mock)

    items = list(process_xml_file_to_pyarrow(xml_path, settings))

    head_matter_mock.assert_not_called()
    assert len(items) == 1
    assert items[0].data.to_pylist() == [{"_id": "1", "name": "Anne Example"}]


def test_process_xml_file_to_pyarrow_pass_clears_element_before_yielding_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Streamed lxml elements are cleared before a flushed batch is yielded."""
    file_path = write_xml_file(tmp_path, "two.xml", PEOPLE_XML_2)
    seen_elements: list[_Element] = []
    real_stream_xml_file = xml_module.stream_xml_file

    def _tracking_stream_xml_file(fp: Path | str | FileItemDict, tag: str) -> Generator[_Element]:
        for elem in real_stream_xml_file(fp, tag):
            seen_elements.append(elem)
            yield elem

    monkeypatch.setattr(xml_module, "stream_xml_file", _tracking_stream_xml_file)
    gen = process_xml_file_to_pyarrow(file_path, fake_xmltopyarrow_settings(buffer_size=1))

    first_batch = next(gen)

    assert len(first_batch.data.to_pylist()) == 1
    assert len(seen_elements) == 1
    first_elem = seen_elements[0]
    assert len(first_elem) == 0
    assert dict(first_elem.attrib) == {}


def test_process_xml_file_to_pyarrow_pass_prepares_xsd_target_traversal_at_most_once_per_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """XSD target resolution and particle validation are not repeated per XML element."""
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)
    settings = fake_xmltopyarrow_settings(table_name="people", xml_tag="person")
    prepare_spy = MagicMock(wraps=xml_coercion_module.prepare_target_traversal)
    monkeypatch.setattr(xml_coercion_module, "prepare_target_traversal", prepare_spy)

    tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))

    assert [table_name for table_name, _ in tagged] == ["people"]
    assert tagged[0][1].to_pylist() == PEOPLE_XML_TO_PYARROW_PARSED
    assert prepare_spy.call_count <= 1


def test_process_xml_file_to_pyarrow_pass_uses_dict_buffer_with_table_schema_map(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """process_xml_file_to_pyarrow constructs DictBuffer with settings.schema_map."""
    assert not hasattr(xml_module, "ArrowDictBuffer")
    assert not hasattr(xml_module, "create_dict_buffer")
    file_path = write_xml_file(tmp_path, "two.xml", PEOPLE_XML_2)
    settings = fake_xmltopyarrow_settings(table_name="people", xml_tag="person")
    captured_schema_maps: list[ArrowSchemaRegistry | None] = []
    real_dict_buffer = xml_common_module.DictBuffer

    def _recording_dict_buffer(
        max_items: int = DEFAULTS[BUFFER_SIZE],
        schema_map: ArrowSchemaRegistry | None = None,
        *,
        use_pyarrow: bool = True,
    ) -> DictBuffer:
        captured_schema_maps.append(schema_map)
        return real_dict_buffer(max_items=max_items, schema_map=schema_map, use_pyarrow=use_pyarrow)

    monkeypatch.setattr(xml_common_module, "DictBuffer", _recording_dict_buffer)

    list(process_xml_file_to_pyarrow(file_path, settings))

    assert captured_schema_maps == [settings.schema_map]


def test_process_xml_file_to_pyarrow_pass_level_mode_routes_elements_to_tables_by_tag(tmp_path: Path) -> None:
    """process_xml_file_to_pyarrow in xml_level=1 mode routes child elements to tag-named Arrow tables."""
    file_path = write_xml_file(tmp_path, "catalog.xml", MULTI_ENTITY_XML)
    settings = fake_xmltopyarrow_level_settings(xml_level=1)

    tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))
    by_table = dict(tagged)

    assert set(by_table.keys()) == {"book", "magazine", "copyright"}
    assert settings.pyarrow_schemas is not None
    assert by_table["book"].schema.equals(settings.pyarrow_schemas["book"], check_metadata=True)
    assert by_table["magazine"].schema.equals(settings.pyarrow_schemas["magazine"], check_metadata=True)
    assert by_table["copyright"].schema.equals(settings.pyarrow_schemas["copyright"], check_metadata=True)
    assert by_table["book"].to_pylist() == [
        {"_id": "b1", "title": "Dune"},
        {"_id": "b2", "title": "Foundation"},
    ]
    assert by_table["magazine"].to_pylist() == [{"_id": "m1", "issue": 42}]
    assert by_table["copyright"].to_pylist() == [{"#text": "Copyright 2026"}]


def test_process_xml_file_to_pyarrow_pass_level_2_routes_grandchildren_to_tables(tmp_path: Path) -> None:
    """process_xml_file_to_pyarrow in xml_level=2 mode routes grandchildren to tag-named Arrow tables."""
    file_path = write_xml_file(tmp_path, "catalog.xml", MULTI_ENTITY_XML)
    settings = fake_xmltopyarrow_level_settings(xml_level=2)

    tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))
    by_table = dict(tagged)

    assert set(by_table.keys()) == {"title", "issue"}
    assert by_table["title"].to_pylist() == [{"#text": "Dune"}, {"#text": "Foundation"}]
    assert by_table["issue"].to_pylist() == [{"#text": 42}]


def test_process_xml_file_to_pyarrow_fail_level_mode_unexpected_xml_tag(tmp_path: Path) -> None:
    """process_xml_file_to_pyarrow in level mode raises ValueError when an XML tag is undeclared at that level."""
    xml_with_unknown = """<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <newspaper id="n1"/>
</catalog>
"""
    file_path = write_xml_file(tmp_path, "bad_catalog.xml", xml_with_unknown)
    settings = fake_xmltopyarrow_level_settings(xml_level=1)

    with pytest.raises(ValueError, match="Unexpected XML element <newspaper> at level 1"):
        list(process_xml_file_to_pyarrow(file_path, settings))


@pytest.mark.parametrize("level_mode", [False, True], ids=["single-tag", "level-mode"])
def test_process_xml_file_to_pyarrow_pass_use_pyarrow_false_yields_lists_of_dicts(
    tmp_path: Path,
    level_mode: bool,
) -> None:
    """When settings.use_pyarrow is False, process_xml_file_to_pyarrow yields list[dict] instead of pa.Table."""
    if level_mode:
        file_path = write_xml_file(tmp_path, "catalog.xml", MULTI_ENTITY_XML)
        settings = fake_xmltopyarrow_level_settings(xml_level=1)
        settings.use_pyarrow = False
        settings.schema_map = None
        tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))
        by_table = dict(tagged)
        assert all(isinstance(page, list) for page in by_table.values())
        assert by_table["book"] == [{"_id": "b1", "title": "Dune"}, {"_id": "b2", "title": "Foundation"}]
        assert by_table["magazine"] == [{"_id": "m1", "issue": 42}]
        assert by_table["copyright"] == [{"#text": "Copyright 2026"}]
    else:
        file_path = write_xml_file(tmp_path, "two.xml", PEOPLE_XML_2)
        settings = fake_xmltopyarrow_settings(table_name="people", xml_tag="person")
        settings.use_pyarrow = False
        settings.schema_map = None
        tagged = table_and_data(process_xml_file_to_pyarrow(file_path, settings))
        assert len(tagged) == 1
        table_name, data = tagged[0]
        assert table_name == "people"
        assert isinstance(data, list)
        assert data == PEOPLE_XML_TO_PYARROW_PARSED[:2]
