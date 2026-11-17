"""Sample XML/XSD documents and XML test file helpers shared across reader and pipeline tests."""

import gzip
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Final

import pytest
from _pytest.mark.structures import ParameterSet
from dlt.extract.items import DataItemWithMeta, TableNameMeta
from lxml.etree import XMLSyntaxError

SIMPLE_LIBRARY_XML: Final[str] = """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
    <book id="3"><title>The Tommyknockers</title></book>
</library>
"""

TWO_BOOK_LIBRARY_XML: Final[str] = """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
</library>
"""

LIBRARY_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="library">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="book" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="xs:string"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

MISMATCHED_TAG_XML: Final[str] = '<library><book id="1"><title>Broken</title></library>'

XML_OPEN_ERROR_CASES: Final[tuple[ParameterSet, ...]] = (
    pytest.param("missing_file", FileNotFoundError, None, id="missing-file"),
    pytest.param("missing_gzip_file", FileNotFoundError, None, id="missing-gzip-file"),
    pytest.param("empty_file", XMLSyntaxError, None, id="empty-file"),
    pytest.param("malformed_xml", XMLSyntaxError, None, id="malformed-xml"),
    pytest.param("non_gzip_with_gz_ext", OSError, "Not a gzipped file", id="non-gzip-with-gz-ext"),
)

PEOPLE_XML_2: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<people>
    <person id="1">
        <name>Anne Example</name>
        <email>a@example.com</email>
    </person>
    <person id="2">
        <name>Belinda Carlisle</name>
        <email>b@example.com</email>
    </person>
</people>
"""

PEOPLE_XML_5: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<people>
    <person id="1"><name>Anne Example</name><email>a@example.com</email></person>
    <person id="2"><name>Belinda Carlisle</name><email>b@example.com</email></person>
    <person id="3"><name>Carol Singer</name><email>c@example.com</email></person>
    <person id="4"><name>Debbie Downer</name><email>d@example.com</email></person>
    <person id="5"><name>Ex Ample</name><email>e@example.com</email></person>
</people>
"""

PEOPLE_PARSED: Final[list[dict[str, str]]] = [
    {"id": "1", "name": "Anne Example", "email": "a@example.com"},
    {"id": "2", "name": "Belinda Carlisle", "email": "b@example.com"},
    {"id": "3", "name": "Carol Singer", "email": "c@example.com"},
    {"id": "4", "name": "Debbie Downer", "email": "d@example.com"},
    {"id": "5", "name": "Ex Ample", "email": "e@example.com"},
]

PEOPLE_XML_TO_DICT_PARSED: Final[list[dict[str, dict[str, str]]]] = [
    {"person": {"_id": p["id"], "name": p["name"], "email": p["email"]}} for p in PEOPLE_PARSED
]

PEOPLE_XML_TO_PYARROW_PARSED: Final[list[dict[str, str]]] = [
    {"_id": p["id"], "name": p["name"], "email": p["email"]} for p in PEOPLE_PARSED
]

PEOPLE_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
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

PEOPLE_XML_EMPTY: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<people>
</people>
"""

PEOPLE_XML_MALFORMED: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<people>
    <person id="1"><name>Anne Example</name></person>
    <person id="2"><name>Belinda Carlisle</name></persoo>
</people>
"""

UNIREF_LIKE_ENTRY_XML: Final[str] = """<?xml version="1.0"?>
<UniRef50>
    <entry>
        <property type="member count" value="1"/>
        <representativeMember>
            <dbReference>
                <property type="isSeed" value="true"/>
            </dbReference>
        </representativeMember>
    </entry>
</UniRef50>
"""


def prepare_xml_error_path(scenario: str, tmp_path: Path, xml_path_factory: Callable[..., Path]) -> Path:
    """Create or return the path for one of the standard XML open/parse failure scenarios.

    :param scenario: Scenario identifier.
    :type scenario: str
    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :param xml_path_factory: Factory writing XML content to disk.
    :type xml_path_factory: Callable[..., Path]
    :return: Path triggering the error scenario.
    :rtype: Path
    :raises ValueError: If ``scenario`` is unknown.
    """
    if scenario == "missing_file":
        return tmp_path / "does_not_exist.xml"
    if scenario == "missing_gzip_file":
        return tmp_path / "does_not_exist.xml.gz"
    if scenario == "empty_file":
        return xml_path_factory("")
    if scenario == "malformed_xml":
        return xml_path_factory("not xml at all <<<")
    if scenario == "mismatched_tag":
        return xml_path_factory(MISMATCHED_TAG_XML)
    if scenario == "non_gzip_with_gz_ext":
        return xml_path_factory("<library/>", gzip_compress=False, filename="not_really_gzipped.xml.gz")
    msg = f"Unknown scenario: {scenario}"
    raise ValueError(msg)


def write_xml_file(directory: Path, filename: str, content: str, *, gzip_compress: bool = False) -> Path:
    """Write an XML string to ``directory / filename``, with optional gzip compression.

    :param directory: Target directory.
    :type directory: Path
    :param filename: Target filename.
    :type filename: str
    :param content: Raw XML text.
    :type content: str
    :param gzip_compress: Whether to gzip-compress the payload.
    :type gzip_compress: bool
    :return: Written file path.
    :rtype: Path
    """
    file_path = directory / filename
    data = content.encode("utf-8")
    if gzip_compress:
        with gzip.open(file_path, "wb") as handle:
            handle.write(data)
    else:
        file_path.write_bytes(data)
    return file_path


def table_and_data(items: Iterable[DataItemWithMeta]) -> list[tuple[str, Any]]:
    """Flatten ``DataItemWithMeta`` items into ``(table_name, data)`` pairs.

    :param items: Yielded ``dlt`` data items.
    :type items: Iterable[DataItemWithMeta]
    :return: List of ``(table_name, payload)`` pairs.
    :rtype: list[tuple[str, Any]]
    """
    result: list[tuple[str, Any]] = []
    for item in items:
        assert isinstance(item, DataItemWithMeta)
        assert isinstance(item.meta, TableNameMeta)
        result.append((item.meta.table_name, item.data))
    return result
