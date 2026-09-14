"""Tests for the xml_to_dict_reader transformer."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.sources.filesystem import filesystem
from lxml.etree import XMLSyntaxError

from cdm_data_loaders.pipelines.xml_to_dict_ingest import xml_to_dict_reader


def read_dir(
    directory: Path,
    settings: Any,  # noqa: ANN401
    file_glob: str = "*.xml*",
) -> list[Any]:
    """Run the raw xml_to_dict_reader function over every file in directory. Return the yielded batches."""
    items = list(filesystem(bucket_url=str(directory), file_glob=file_glob))
    return list(xml_to_dict_reader.__wrapped__(iter(items), settings))


def batch_records(batch: Any) -> list[dict[str, Any]]:  # noqa: ANN401
    """Return the rows carried by one yielded batch, unwrapping dlt's meta wrapper."""
    return batch.data if hasattr(batch, "data") else batch


def test_xml_to_dict_reader_pass_single_file_yields_one_dict_per_element(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """Each `xml_tag` element in one file becomes one dict, in document order."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
</library>
"""
    )

    batches = read_dir(directory, settings_factory())

    assert len(batches) == 1
    records = batch_records(batches[0])
    assert [record["book"]["@id"] for record in records] == ["1", "2"]
    assert records[0]["book"]["title"] == "The Shining"
    assert records[1]["book"]["title"] == "The Stand"


def test_xml_to_dict_reader_pass_filters_out_non_matching_tags(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """Only elements matching the requested tag are yielded when multiple tag types are interleaved."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<catalog>
    <book id="1"><title>Cell</title></book>
    <magazine id="1"><title>National Geographic</title></magazine>
    <book id="2"><title>Cujo</title></book>
</catalog>
"""
    )

    batches = read_dir(directory, settings_factory())

    records = batch_records(batches[0])
    assert [record["book"]["@id"] for record in records] == ["1", "2"]
    assert records[0]["book"]["title"] == "Cell"


def test_xml_to_dict_reader_pass_no_matching_tag_yields_no_batches(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """A tag absent from a well-formed file produces no output rows."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
</library>
"""
    )

    batches = read_dir(directory, settings_factory(xml_tag="chapter"))

    assert batches == []


def test_xml_to_dict_reader_pass_multiple_files_concatenated_in_order(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """Records from every matching file are yielded, one file at a time."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library><book id="1"><title>Cell</title></book></library>
""",
        """<?xml version="1.0"?>
<library><book id="2"><title>Cujo</title></book></library>
""",
    )

    batches = read_dir(directory, settings_factory(buffer_size=100))

    all_records = [record for batch in batches for record in batch_records(batch)]
    assert [record["book"]["@id"] for record in all_records] == ["1", "2"]


def test_xml_to_dict_reader_pass_gzip_file_is_decompressed(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """A file named with a .gz suffix is transparently decompressed and parsed."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library><book id="1"><title>The Shining</title></book></library>
""",
        gzip_compress=True,
    )

    batches = read_dir(directory, settings_factory())

    records = batch_records(batches[0])
    assert [record["book"]["@id"] for record in records] == ["1"]


def test_xml_to_dict_reader_pass_buffer_size_controls_batch_boundaries(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """A full buffer is flushed as one batch; the remainder is flushed at the end of the file."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
    <book id="3"><title>The Tommyknockers</title></book>
</library>
"""
    )

    batches = read_dir(directory, settings_factory(buffer_size=2))

    assert len(batches) == 2  # noqa: PLR2004
    assert [record["book"]["@id"] for record in batch_records(batches[0])] == ["1", "2"]
    assert [record["book"]["@id"] for record in batch_records(batches[1])] == ["3"]


def test_xml_to_dict_reader_pass_yields_batches_marked_with_table_name(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """Each yielded batch carries the configured table name as dlt meta."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library><book id="1"><title>The Shining</title></book></library>
"""
    )

    batches = read_dir(directory, settings_factory(table_name="my_books"))

    assert len(batches) == 1
    assert batches[0].meta.table_name == "my_books"


def test_xml_to_dict_reader_pass_empty_file_yields_no_batches(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """An XML file whose root element has no matching children produces no output rows."""
    directory = xml_dir_factory('<?xml version="1.0"?>\n<library/>\n')

    batches = read_dir(directory, settings_factory())

    assert batches == []


def test_xml_to_dict_reader_pass_nested_content_becomes_nested_dict(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """Child elements of the captured tag are preserved as nested dicts."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library>
    <book id="1">
        <title>Cell</title>
        <author><name>Stephen King</name></author>
    </book>
</library>
"""
    )

    batches = read_dir(directory, settings_factory())

    records = batch_records(batches[0])
    assert records[0]["book"]["author"] == {"name": "Stephen King"}


def test_xml_to_dict_reader_fail_malformed_xml_raises_syntax_error(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any]
) -> None:
    """Malformed XML (mismatched closing tags) raises lxml's XMLSyntaxError rather than yielding partial data."""
    directory = xml_dir_factory('<library><book id="1"><title>Broken</title></library>')

    with pytest.raises(XMLSyntaxError):
        read_dir(directory, settings_factory())


def test_xml_to_dict_reader_fail_missing_file_raises(tmp_path: Path, settings_factory: Callable[..., Any]) -> None:
    """A file item whose file_url does not exist raises FileNotFoundError."""
    missing_file = tmp_path / "missing.xml"
    missing_file.parent.mkdir(exist_ok=True)

    items = list(filesystem(bucket_url=str(missing_file), file_glob="*"))
    for item in items:
        with pytest.raises(FileNotFoundError):
            list(xml_to_dict_reader.__wrapped__(iter([item]), settings_factory()))


def test_xml_to_dict_reader_pass_logs_reading_and_processed_messages(
    xml_dir_factory: Callable[..., Path], settings_factory: Callable[..., Any], caplog: pytest.LogCaptureFixture
) -> None:
    """Reading from each file is logged, and per-file processed counts are logged at debug level."""
    directory = xml_dir_factory(
        """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
    <book id="3"><title>The Tommyknockers</title></book>
</library>
"""
    )
    caplog.set_level(logging.DEBUG)

    read_dir(directory, settings_factory(buffer_size=2))

    messages = [record.message for record in caplog.records]
    assert any(message.startswith("Reading from") for message in messages)
    assert any(message.startswith("Processed 3 entries") for message in messages)
