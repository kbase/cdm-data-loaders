"""Unit tests for ``process_xml_file_to_dict`` in ``cdm_data_loaders.pipelines.xmltodict.processors``."""

import gzip
import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock

import pytest
import xmltodict
from dlt.common.storages.fsspec_filesystem import FileItemDict
from lxml.etree import XMLSyntaxError, _Element, tostring

import cdm_data_loaders.readers.xml as xml_module
from cdm_data_loaders.core.fields import BUFFER_SIZE, DEFAULTS, LOG_INTERVAL
from cdm_data_loaders.pipelines.xmltodict.processors import process_xml_file_to_dict
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.readers.xml import DEFAULT_XMLTODICT_ARGS
from tests._support.fs_backends import FsBackend
from tests._support.xml_samples import (
    PEOPLE_XML_2,
    PEOPLE_XML_5,
    PEOPLE_XML_EMPTY,
    PEOPLE_XML_MALFORMED,
    PEOPLE_XML_TO_DICT_PARSED,
    UNIREF_LIKE_ENTRY_XML,
    table_and_data,
    write_xml_file,
)

UNIPROT_NS: Final[str] = "http://uniprot.org/uniprot"
UNIPROT_XML_2: Final[str] = f"""<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="{UNIPROT_NS}">
    <entry dataset="Swiss-Prot">
        <accession>P12345</accession>
        <name>TEST_HUMAN</name>
        <organism>
            <name type="scientific">Homo sapiens</name>
        </organism>
    </entry>
    <entry dataset="Swiss-Prot">
        <accession>Q67890</accession>
        <name>TEST2_HUMAN</name>
        <organism>
            <name type="scientific">Homo sapiens</name>
        </organism>
    </entry>
</uniprot>
"""


def fake_xmltodict_settings(
    table_name: str = "people",
    xml_tag: str = "person",
    buffer_size: int = DEFAULTS[BUFFER_SIZE],
    log_interval: int = DEFAULTS[LOG_INTERVAL],
) -> XmlToDictSettings:
    """Build a ``MagicMock`` ``XmlToDictSettings`` for ``process_xml_file_to_dict`` unit tests.

    :param table_name: Target table name.
    :type table_name: str
    :param xml_tag: XML element tag name to stream.
    :type xml_tag: str
    :param buffer_size: Maximum buffered rows per page.
    :type buffer_size: int
    :param log_interval: Entry count interval for progress logs.
    :type log_interval: int
    :return: Configured ``XmlToDictSettings`` mock.
    :rtype: XmlToDictSettings
    """
    settings = MagicMock(spec=XmlToDictSettings)
    settings.buffer_size = buffer_size
    settings.log_interval = log_interval
    settings.use_pyarrow = True
    settings.get_table_transform.return_value = None
    settings.schema_map = None
    settings.table_name = table_name
    settings.xml_tag = xml_tag
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
def test_process_xml_file_to_dict_pass_buffer_size_controls_batching(
    tmp_path: Path,
    buffer_size: int,
    expected_batch_sizes: list[int],
) -> None:
    """Rows are yielded in batches of at most buffer_size with a final partial batch."""
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)

    items = table_and_data(process_xml_file_to_dict(file_path, fake_xmltodict_settings(buffer_size=buffer_size)))
    batches = [batch for _, batch in items]

    assert [len(rows) for rows in batches] == expected_batch_sizes
    assert [row for rows in batches for row in rows] == PEOPLE_XML_TO_DICT_PARSED
    assert {table_name for table_name, _ in items} == {"people"}


@pytest.mark.parametrize("gzip_compress", [False, True], ids=["plain", "gzip"])
def test_process_xml_file_to_dict_pass_gzip_file(tmp_path: Path, gzip_compress: bool) -> None:
    """Gzip-compressed XML input (.gz suffix) is decompressed and parsed."""
    filename = "compressed.xml.gz" if gzip_compress else "plain.xml"
    path = write_xml_file(tmp_path, filename, PEOPLE_XML_5, gzip_compress=gzip_compress)

    items = table_and_data(process_xml_file_to_dict(path, fake_xmltodict_settings()))
    rows = [row for _, batch in items for row in batch]
    assert rows == PEOPLE_XML_TO_DICT_PARSED


def test_process_xml_file_to_dict_pass_logs_reading_info_message_once(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """INFO-level 'Reading from <path>' message is logged once with the file path."""
    caplog.set_level(logging.INFO)
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)
    list(process_xml_file_to_dict(file_path, fake_xmltodict_settings()))

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(info_records) == 1
    assert info_records[0].msg == "Reading from %s"
    assert info_records[0].args == (str(file_path),)


def test_process_xml_file_to_dict_pass_no_matching_elements_logs_no_progress(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A well-formed file with no matching elements yields nothing and logs no progress."""
    caplog.set_level(logging.DEBUG)
    file_path = write_xml_file(tmp_path, "empty.xml", PEOPLE_XML_EMPTY)
    items = list(process_xml_file_to_dict(file_path, fake_xmltodict_settings()))

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
def test_process_xml_file_to_dict_pass_log_interval_boundary(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    log_interval: int,
    expected_interim_counts: list[int],
    expected_final_count: int | None,
) -> None:
    """Entry counts logged at interim checkpoints and trailing summary match log_interval."""
    caplog.set_level(logging.DEBUG)
    file_path = write_xml_file(tmp_path, "five.xml", PEOPLE_XML_5)
    list(process_xml_file_to_dict(file_path, fake_xmltodict_settings(log_interval=log_interval)))

    interim_counts = [r.args[0] for r in caplog.records if r.msg == "Processed %d entries"]
    final_records = [r for r in caplog.records if r.msg == "Processed %d entries from %s"]

    assert interim_counts == expected_interim_counts
    if expected_final_count is None:
        assert final_records == []
    else:
        assert len(final_records) == 1
        assert final_records[0].args == (expected_final_count, file_path.name)


def test_process_xml_file_to_dict_fail_missing_file(tmp_path: Path) -> None:
    """A missing file path raises FileNotFoundError when the generator is consumed."""
    missing_path = tmp_path / "does_not_exist.xml"
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        list(process_xml_file_to_dict(missing_path, fake_xmltodict_settings()))


def test_process_xml_file_to_dict_fail_malformed_xml(tmp_path: Path) -> None:
    """Malformed XML raises lxml XMLSyntaxError during streaming."""
    file_path = write_xml_file(tmp_path, "malformed.xml", PEOPLE_XML_MALFORMED)
    with pytest.raises(XMLSyntaxError, match="Opening and ending tag mismatch"):
        list(process_xml_file_to_dict(file_path, fake_xmltodict_settings()))


@pytest.mark.parametrize("gzip_compress", [False, True], ids=["plain", "gzip"])
def test_process_xml_file_to_dict_pass_matches_xmltodict_reference(tmp_path: Path, gzip_compress: bool) -> None:
    """process_xml_file_to_dict matches direct xmltodict parsing of each streamed entry."""
    filename = "uniprot.xml.gz" if gzip_compress else "uniprot.xml"
    file_path = write_xml_file(tmp_path, filename, UNIPROT_XML_2, gzip_compress=gzip_compress)
    xml_tag = f"{{{UNIPROT_NS}}}entry"

    raw_entries = [
        xmltodict.parse(tostring(entry), **DEFAULT_XMLTODICT_ARGS)["entry"]  # pyright: ignore[reportArgumentType]
        for entry in xml_module.stream_xml_file(file_path, xml_tag)
    ]
    assert any(key.startswith("_xmlns") for entry in raw_entries for key in entry)
    expected_rows = [{"entry": {k: v for k, v in entry.items() if not k.startswith("_xmlns")}} for entry in raw_entries]

    settings = fake_xmltodict_settings(table_name="some_table", xml_tag=xml_tag)
    tagged = table_and_data(process_xml_file_to_dict(file_path, settings))

    assert tagged == [("some_table", expected_rows)]


def test_process_xml_file_to_dict_pass_strips_only_xmlns_prefixed_keys(tmp_path: Path) -> None:
    """xmlns/xmlns:xsi attributes are stripped while non-namespace underscore-prefixed keys remain."""
    namespace = "http://example.com/ns"
    xml_with_xmlns_and_lookalike = (
        '<?xml version="1.0"?>\n'
        f'<person xmlns="{namespace}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'id="1" xsinil="true"><name>Anne Example</name></person>\n'
    )
    file_path = write_xml_file(tmp_path, "namespaced.xml", xml_with_xmlns_and_lookalike)
    settings = fake_xmltodict_settings(xml_tag=f"{{{namespace}}}person")

    items = table_and_data(process_xml_file_to_dict(file_path, settings))

    assert items == [
        (
            "people",
            [{"person": {"_id": "1", "_xsinil": "true", "name": "Anne Example"}}],
        )
    ]


def test_process_xml_file_to_dict_pass_custom_table_name_and_xml_tag_are_respected(tmp_path: Path) -> None:
    """Rows land under settings.table_name matched by settings.xml_tag."""
    file_path = write_xml_file(tmp_path, "two.xml", PEOPLE_XML_2)
    settings = fake_xmltodict_settings(table_name="library_entries", xml_tag="person")

    tagged = table_and_data(process_xml_file_to_dict(file_path, settings))

    assert [table_name for table_name, _ in tagged] == ["library_entries"]
    assert tagged[0][1] == PEOPLE_XML_TO_DICT_PARSED[:2]


def test_process_xml_file_to_dict_pass_keeps_xmltodict_default_dicts(tmp_path: Path) -> None:
    """process_xml_file_to_dict retains xmltodict single-occurrence dicts."""
    file_path = write_xml_file(tmp_path, "entry.xml", UNIREF_LIKE_ENTRY_XML)
    settings = fake_xmltodict_settings(table_name="entry", xml_tag="entry", buffer_size=100, log_interval=1000)

    items = table_and_data(process_xml_file_to_dict(file_path, settings))

    assert items == [
        (
            "entry",
            [
                {
                    "entry": {
                        "property": {"_type": "member count", "_value": "1"},
                        "representativeMember": {
                            "dbReference": {"property": {"_type": "isSeed", "_value": "true"}},
                        },
                    }
                }
            ],
        )
    ]


@pytest.mark.parametrize("relative_path", ["two.xml", "sub/two.xml.gz"])
def test_process_xml_file_to_dict_pass_file_item_any_backend(fs_backend: FsBackend, relative_path: str) -> None:
    """process_xml_file_to_dict reads a file item on every backend with the same output as a local file."""
    data = PEOPLE_XML_2.encode("utf-8")
    fs_backend.put(relative_path, gzip.compress(data) if relative_path.endswith(".gz") else data)

    tagged = table_and_data(process_xml_file_to_dict(fs_backend.file_item(relative_path), fake_xmltodict_settings()))

    assert tagged == [("people", PEOPLE_XML_TO_DICT_PARSED[:2])]


def test_process_xml_file_to_dict_pass_skips_empty_xmltodict_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An element whose xmltodict.parse output is empty is skipped and not buffered."""
    file_path = write_xml_file(tmp_path, "two.xml", PEOPLE_XML_2)
    settings = fake_xmltodict_settings(table_name="people", xml_tag="person")
    original_parse = xmltodict.parse
    call_count = 0

    def _parse_skipping_first(*args: object, **kwargs: object) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {}
        return original_parse(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(xml_module.xmltodict, "parse", _parse_skipping_first)

    items = table_and_data(process_xml_file_to_dict(file_path, settings))

    assert items == [("people", [PEOPLE_XML_TO_DICT_PARSED[1]])]


def test_process_xml_file_to_dict_pass_clears_element_before_yielding_batch(
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
    gen = process_xml_file_to_dict(file_path, fake_xmltodict_settings(buffer_size=1))

    first_batch = next(gen)

    assert len(first_batch.data) == 1
    assert len(seen_elements) == 1
    first_elem = seen_elements[0]
    assert len(first_elem) == 0
    assert dict(first_elem.attrib) == {}
