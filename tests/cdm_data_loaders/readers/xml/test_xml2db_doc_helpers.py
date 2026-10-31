"""Unit tests for the private helpers in `cdm_data_loaders.readers.xml2db_doc`."""

from io import BytesIO
from pathlib import Path
from typing import Any

from xml2db import DataModel, Document

from cdm_data_loaders.readers.xml2db_doc import (
    _content_addressed_key,
    _xml2db_local_key,
    _xml2db_scalar,
    flatten_xml2db_document,
    is_xml2db_content_addressed_table,
)
from tests.xml_samples import TWO_BOOK_LIBRARY_XML

# _xml2db_scalar


def test_xml2db_scalar_pass_converts_bytes_to_hex() -> None:
    """Raw bytes (xml2db's record hash) are converted to a hex string."""
    assert _xml2db_scalar(b"\xde\xad\xbe\xef") == "deadbeef"


def test_xml2db_scalar_pass_passes_through_non_bytes() -> None:
    """Non-bytes values are returned unchanged."""
    some_integer = 7
    assert _xml2db_scalar("abc") == "abc"
    assert _xml2db_scalar(some_integer) == some_integer
    assert _xml2db_scalar(None) is None
    assert _xml2db_scalar([1, 2]) == [1, 2]


def test_xml2db_scalar_pass_empty_bytes() -> None:
    """Empty bytes convert to an empty string."""
    assert _xml2db_scalar(b"") == ""


# _xml2db_local_key


def test_xml2db_local_key_pass_namespaces_raw_value() -> None:
    """A raw Document-local key is namespaced with the file key."""
    file_key = "file.xml:3"
    raw_pk = 7
    assert _xml2db_local_key(file_key, raw_pk) == f"{file_key}:{raw_pk}"


def test_xml2db_local_key_pass_none_stays_none() -> None:
    """A None raw key maps to None (an absent reference)."""
    assert _xml2db_local_key("file.xml:3", None) is None


def test_xml2db_local_key_pass_empty_file_key() -> None:
    """An empty file key still namespaces, so distinct raw keys stay distinct."""
    assert _xml2db_local_key("", 1) == ":1"


# _content_addressed_key


def test_content_addressed_key_pass_includes_table_and_hash_hex() -> None:
    """The key is `<table name>:<hash hex>`, so cross-table hash collisions cannot collide."""
    assert _content_addressed_key("entry", b"\x01\x02") == "entry:0102"


def test_content_addressed_key_pass_identical_content_identical_key() -> None:
    """The same hash bytes always produce the same key, whatever the Document."""
    assert _content_addressed_key("property", b"\xab" * 20) == _content_addressed_key("property", b"\xab" * 20)


# is_xml2db_content_addressed_table


def test_is_xml2db_content_addressed_table_pass_reused_non_root_table(uniref_model: DataModel) -> None:
    """Every reused table that is not the root qualifies."""
    for table_name in ("entryType", "propertyType", "memberType"):
        table = uniref_model.tables[table_name]
        assert table.is_reused
        assert is_xml2db_content_addressed_table(uniref_model, table)


def test_is_xml2db_content_addressed_table_fail_root_table_never_qualifies(
    uniref_model: DataModel,
) -> None:
    """The root table is reused but its hash is not stable across chunks, so it never qualifies."""
    root_table = uniref_model.tables[uniref_model.root_table]
    assert root_table.is_reused
    assert not is_xml2db_content_addressed_table(uniref_model, root_table)


def test_is_xml2db_content_addressed_table_fail_non_reused_table(library_xsd: Path) -> None:
    """A table configured with reuse: false does not qualify.

    The small library schema is used here: xml2db rejects a reuse:false table with more than one
    parent, and in the uniref schema every table below the root has two (its parent plus a
    junction).
    """
    model_config = {"tables": {"book": {"reuse": False}}}
    model = DataModel(xsd_file=str(library_xsd), short_name="library_test", model_config=model_config)
    book_table = model.tables["book"]
    assert not book_table.is_reused
    assert not is_xml2db_content_addressed_table(model, book_table)


# flatten_xml2db_document, non-reused tables


def test_flatten_xml2db_document_pass_non_reused_table_parent_fk_is_namespaced(
    library_xsd: Path,
) -> None:
    """A non-reused table's parent fk is rewritten to a file-namespaced key.

    xml2db adds a `temp_fk_parent_<parent>` column to every record of a table configured with
    `reuse: false`. Its raw value is a Document-local integer that restarts from 1 for every
    Document, so a raw value would collide across files and chunks; it must be namespaced by
    the file key like every other non-content-addressed key.
    """
    model_config = {"tables": {"book": {"reuse": False}}}
    model = DataModel(xsd_file=str(library_xsd), short_name="library_test", model_config=model_config)

    def _flatten(file_key: str) -> dict[str, list[dict[str, Any]]]:
        document = Document(model)
        document.parse_xml(BytesIO(TWO_BOOK_LIBRARY_XML.encode("utf-8")), skip_validation=True, iterparse=True)
        return flatten_xml2db_document(model, document, file_key=file_key)

    tables = _flatten("file_one.xml")

    assert tables["book"], "no book rows produced"
    for row in tables["book"]:
        fk_parent = row["fk_parent_library"]
        assert isinstance(fk_parent, str)
        assert fk_parent.startswith("file_one.xml:")
