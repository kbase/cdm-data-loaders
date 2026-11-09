"""Tests for `stream_xml_file` from cdm_data_loaders.readers.xml."""

import gzip
from collections.abc import Callable
from pathlib import Path

import pytest
from lxml.etree import XMLSyntaxError, tostring

from cdm_data_loaders.readers.xml import stream_xml_file, stream_xml_file_at_level
from tests.cdm_data_loaders.readers.xml.conftest import XML_OPEN_ERROR_CASES, prepare_xml_error_path
from tests.fs_backends import FsBackend
from tests.xml_samples import SIMPLE_LIBRARY_XML

MIXED_TAGS_XML = """<?xml version="1.0"?>
<catalog>
    <book id="1"><title>Cell</title></book>
    <magazine id="1"><title>National Geographic</title></magazine>
    <book id="2"><title>Cujo</title></book>
</catalog>
"""

NAMESPACE_URI = "http://example.com/lib"
NAMESPACED_SIMPLE_XML = SIMPLE_LIBRARY_XML.replace("<library>", f'<library xmlns="{NAMESPACE_URI}">')

NAMESPACED_MIXED_TAG_XML = MIXED_TAGS_XML.replace("<catalog>", f'<catalog xmlns="{NAMESPACE_URI}">')

EXPECTED_SIMPLE_TAGS_IDS = [("book", "1"), ("book", "2"), ("book", "3")]
EXPECTED_MIXED_TAGS_IDS = [("book", "1"), ("book", "2")]


@pytest.mark.parametrize("as_path", [True, False])
def test_stream_xml_file_pass_yields_matching_elements_in_order(
    xml_path_factory: Callable[..., Path], as_path: bool
) -> None:
    """Verify elements matching the requested tag are yielded in document order, for both Path and str inputs."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)
    file_path: Path | str = path if as_path else str(path)

    tags_ids = [(elem.tag, elem.get("id")) for elem in stream_xml_file(file_path, "book")]

    assert tags_ids == EXPECTED_SIMPLE_TAGS_IDS


@pytest.mark.parametrize("gzip_compress", [False, True], ids=["plain", "gzip"])
def test_stream_xml_file_pass_gzip_file_yields_same_elements_as_plain_file(
    xml_path_factory: Callable[..., Path],
    gzip_compress: bool,
) -> None:
    """Verify a gzip-compressed (.gz) file is transparently decompressed and parsed identically to a plain file."""
    filename = "compressed.xml" if gzip_compress else "plain.xml"
    path = xml_path_factory(SIMPLE_LIBRARY_XML, gzip_compress=gzip_compress, filename=filename)

    ids = [(elem.tag, elem.get("id")) for elem in stream_xml_file(path, "book")]

    assert ids == EXPECTED_SIMPLE_TAGS_IDS


def test_stream_xml_file_pass_filters_out_non_matching_tags(xml_path_factory: Callable[..., Path]) -> None:
    """Verify only elements matching the requested tag are yielded when multiple tag types are interleaved."""
    path = xml_path_factory(MIXED_TAGS_XML)

    tags_seen = [(elem.tag, elem.get("id")) for elem in stream_xml_file(path, "book")]

    assert tags_seen == EXPECTED_MIXED_TAGS_IDS


def test_stream_xml_file_pass_no_matching_tag_yields_nothing(xml_path_factory: Callable[..., Path]) -> None:
    """Verify requesting a tag absent from a well-formed file yields no elements, rather than erroring."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)

    results = list(stream_xml_file(path, "chapter"))

    assert results == []


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("library", [("library", None)]),
        ("title", [("title", "The Shining"), ("title", "The Stand"), ("title", "The Tommyknockers")]),
    ],
)
def test_stream_xml_file_pass_other_tags(xml_path_factory: Callable[..., Path], tag: str, expected: list) -> None:
    """Verify that other elements in the XML structure can also be found by stream_xml_file."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)
    results = [(elem.tag, elem.text) for elem in stream_xml_file(path, tag)]
    assert results == expected


@pytest.mark.parametrize(
    ("input_xml", "expected"),
    [(NAMESPACED_SIMPLE_XML, EXPECTED_SIMPLE_TAGS_IDS), (NAMESPACED_MIXED_TAG_XML, EXPECTED_MIXED_TAGS_IDS)],
)
def test_stream_xml_file_pass_namespaced_tag_matches_default_namespace_elements(
    xml_path_factory: Callable[..., Path], input_xml: str, expected: list[tuple[str, str]]
) -> None:
    """Verify a brace-qualified tag (e.g. '{ns}book') correctly matches elements under a default XML namespace."""
    file_path = xml_path_factory(input_xml)
    namespaced_tag = f"{{{NAMESPACE_URI}}}book"

    tags_ids = [(elem.tag, elem.get("id")) for elem in stream_xml_file(file_path, namespaced_tag)]
    assert tags_ids == [(namespaced_tag, el[1]) for el in expected]


def test_stream_xml_file_pass_bare_tag_does_not_match_namespaced_elements(
    xml_path_factory: Callable[..., Path],
) -> None:
    """Verify a plain (non-namespace-qualified) tag does NOT match elements declared under an XML namespace."""
    path = xml_path_factory(NAMESPACED_SIMPLE_XML)

    results = list(stream_xml_file(path, "book"))

    assert results == []


def test_stream_xml_file_pass_blank_text_between_child_elements_is_removed(
    xml_path_factory: Callable[..., Path],
) -> None:
    """Verify indentation whitespace between child elements is stripped due to `remove_blank_text=True`."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)

    book = next(stream_xml_file(path, "book"))

    assert book.text is None


def test_stream_xml_file_pass_element_content_intact_before_clear(xml_path_factory: Callable[..., Path]) -> None:
    """Verify a yielded element still has its title/attributes intact at the moment it's received by the caller."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)
    gen = stream_xml_file(path, "book")

    first = next(gen)

    assert first.get("id") == "1"
    assert first.find("title").text == "The Shining"


def test_stream_xml_file_pass_previously_yielded_elements_are_cleared_after_advancing(
    xml_path_factory: Callable[..., Path],
) -> None:
    """Verify each element is cleared (text/children removed) once the generator advances past it, confirming streaming memory behavior."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)

    all_elements = list(stream_xml_file(path, "book"))

    assert all(elem.text is None for elem in all_elements)
    assert all(len(elem) == 0 for elem in all_elements)
    assert all(elem.find("title") is None for elem in all_elements)


@pytest.mark.parametrize("relative_path", ["library.xml", "nested/dir/library.xml.gz"])
def test_stream_xml_file_pass_file_item_any_backend(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify elements are streamed from a file item on every backend, plain or gzipped."""
    data = SIMPLE_LIBRARY_XML.encode("utf-8")
    fs_backend.put(relative_path, gzip.compress(data) if relative_path.endswith(".gz") else data)

    tags_ids = [(elem.tag, elem.get("id")) for elem in stream_xml_file(fs_backend.file_item(relative_path), "book")]

    assert tags_ids == EXPECTED_SIMPLE_TAGS_IDS


def test_stream_xml_file_pass_open_handle_left_open(xml_path_factory: Callable[..., Path]) -> None:
    """Verify an open handle is streamed from and left open for the caller to close."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)

    with path.open("rb") as fh:
        tags_ids = [(elem.tag, elem.get("id")) for elem in stream_xml_file(fh, "book")]
        assert not fh.closed

    assert tags_ids == EXPECTED_SIMPLE_TAGS_IDS


def test_stream_xml_file_fail_missing_remote_object(remote_fs_backend: FsBackend) -> None:
    """Verify a remote object deleted after listing raises FileNotFoundError, as a missing local file does."""
    remote_fs_backend.put("gone.xml", SIMPLE_LIBRARY_XML.encode("utf-8"))
    item = remote_fs_backend.file_item("gone.xml")
    remote_fs_backend.fs.rm(remote_fs_backend.path("gone.xml"))

    with pytest.raises(FileNotFoundError):
        list(stream_xml_file(item, "book"))


@pytest.mark.parametrize(
    ("scenario", "expected_exc", "match"),
    [*XML_OPEN_ERROR_CASES, pytest.param("mismatched_tag", XMLSyntaxError, None, id="mismatched-tag")],
)
def test_stream_xml_file_fail_on_open_or_syntax_error(
    scenario: str,
    expected_exc: type[Exception],
    match: str | None,
    tmp_path: Path,
    xml_path_factory: Callable[..., Path],
) -> None:
    """Verify missing, empty, malformed, or mis-named gzip files raise the expected error."""
    path = prepare_xml_error_path(scenario, tmp_path, xml_path_factory)

    with pytest.raises(expected_exc, match=match):
        list(stream_xml_file(path, "book"))


@pytest.mark.parametrize(
    ("xml", "tag", "expected"),
    [
        pytest.param("<root/>", "node", [], id="no-match"),
        pytest.param(
            '<root><node id="1"/><node id="2"/></root>',
            "node",
            [b'<node id="1"/>', b'<node id="2"/>'],
            id="siblings",
        ),
        pytest.param(
            '<root><a><node id="1"/></a><b><node id="2"/></b></root>',
            "node",
            [b'<node id="1"/>', b'<node id="2"/>'],
            id="matches-under-different-parents",
        ),
        pytest.param(
            '<root><node id="1"><node id="2"/></node></root>',
            "node",
            [b'<node id="1"><node id="2"/></node>'],
            id="nested-once",
        ),
        pytest.param(
            '<root><node id="1"><a>x</a><node id="2"/><node id="3"/></node></root>',
            "node",
            [b'<node id="1"><a>x</a><node id="2"/><node id="3"/></node>'],
            id="nested-with-earlier-siblings",
        ),
        pytest.param(
            '<root><node id="1"><node id="2"><node id="3"/></node></node></root>',
            "node",
            [b'<node id="1"><node id="2"><node id="3"/></node></node>'],
            id="nested-three-levels",
        ),
        pytest.param(
            '<root><node id="1"><node id="1a"/></node><node id="2"/></root>',
            "node",
            [b'<node id="1"><node id="1a"/></node>', b'<node id="2"/>'],
            id="nested-then-sibling",
        ),
        pytest.param(
            '<node id="r"><node id="c"/></node>',
            "node",
            [b'<node id="r"><node id="c"/></node>'],
            id="root-is-a-match",
        ),
        pytest.param(
            '<root xmlns="urn:x"><node id="1"><node id="2"/></node></root>',
            "{urn:x}node",
            [b'<node xmlns="urn:x" id="1"><node id="2"/></node>'],
            id="nested-in-default-namespace",
        ),
    ],
)
@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "gzip"])
def test_stream_xml_file_pass_yields_outermost_matches(
    tmp_path: Path,
    write_gzip_file: Callable[[Path, str, str], Path],
    xml: str,
    tag: str,
    expected: list[bytes],
    compressed: bool,
) -> None:
    """Only the outermost matching elements are yielded, each with all of its content."""
    if compressed:
        file_path = write_gzip_file(tmp_path, "doc.xml.gz", xml)
    else:
        file_path = tmp_path / "doc.xml"
        file_path.write_text(xml, encoding="utf-8")

    assert [tostring(elem, with_tail=False) for elem in stream_xml_file(file_path, tag)] == expected


@pytest.mark.parametrize(
    ("xml", "level", "expected"),
    [
        pytest.param(
            '<root><a id="1"/><b id="2"/></root>',
            0,
            [b'<root><a id="1"/><b id="2"/></root>'],
            id="level-0-root",
        ),
        pytest.param(
            '<root><book id="1"><title>Cell</title></book><magazine id="1"/><book id="2"/></root>',
            1,
            [b'<book id="1"><title>Cell</title></book>', b'<magazine id="1"/>', b'<book id="2"/>'],
            id="level-1-heterogeneous-siblings",
        ),
        pytest.param(
            '<root><a id="1"><x>1</x><y>2</y></a><b id="2"><z>3</z></b></root>',
            2,
            [b"<x>1</x>", b"<y>2</y>", b"<z>3</z>"],
            id="level-2-grandchildren",
        ),
        pytest.param(
            "<root><a><b/></a></root>",
            3,
            [],
            id="level-beyond-max-depth",
        ),
    ],
)
@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "gzip"])
def test_stream_xml_file_at_level_pass_yields_all_elements_at_depth(
    tmp_path: Path,
    write_gzip_file: Callable[[Path, str, str], Path],
    xml: str,
    level: int,
    expected: list[bytes],
    compressed: bool,
) -> None:
    """stream_xml_file_at_level yields every element at exact depth `level` in document order."""
    if compressed:
        file_path = write_gzip_file(tmp_path, "doc.xml.gz", xml)
    else:
        file_path = tmp_path / "doc.xml"
        file_path.write_text(xml, encoding="utf-8")

    assert [tostring(elem, with_tail=False) for elem in stream_xml_file_at_level(file_path, level)] == expected


def test_stream_xml_file_at_level_pass_clears_elements_and_ancestors_after_advancing(
    xml_path_factory: Callable[..., Path],
) -> None:
    """stream_xml_file_at_level clears yielded elements once the generator advances past them."""
    path = xml_path_factory(MIXED_TAGS_XML)
    all_elements = list(stream_xml_file_at_level(path, 1))

    assert [elem.tag for elem in all_elements] == ["book", "magazine", "book"]
    assert all(elem.text is None for elem in all_elements)
    assert all(len(elem) == 0 for elem in all_elements)


def test_stream_xml_file_at_level_fail_negative_level(xml_path_factory: Callable[..., Path]) -> None:
    """stream_xml_file_at_level raises ValueError when level < 0."""
    path = xml_path_factory(SIMPLE_LIBRARY_XML)
    with pytest.raises(ValueError, match="level must be >= 0"):
        list(stream_xml_file_at_level(path, -1))
