"""Tests for the xml_utils module."""

import pytest
from lxml.etree import Element, XMLParser, fromstring

from cdm_data_loaders.utils.xml_utils import dump_xml_element, get_text, local_name


def make_element_with_text(raw_text: str | None) -> Element:
    """Create an element whose text is set to raw_text."""
    elem = Element("tag")
    elem.text = raw_text
    return elem


@pytest.mark.parametrize("default", [None, "n/a"], ids=["default_none", "default_custom"])
@pytest.mark.parametrize("raw_text", ["hello", "  hello  "], ids=["no_padding", "padded"])
def test_get_text_pass_returns_stripped_text(raw_text: str, default: str | None) -> None:
    """Non-empty element text is returned stripped regardless of the default."""
    assert get_text(make_element_with_text(raw_text), default) == "hello"


@pytest.mark.parametrize("default", [None, "n/a"], ids=["default_none", "default_custom"])
@pytest.mark.parametrize(
    "missing_text",
    [None, "", "   "],
    ids=["elem_none", "text_none", "text_whitespace"],
)
def test_get_text_pass_returns_default_when_text_missing(missing_text: str | None, default: str | None) -> None:
    """A None element or missing/whitespace-only text yields the default value."""
    elem = None if missing_text is None else make_element_with_text(missing_text)
    assert get_text(elem, default) == default


def test_get_text_fail_rejects_non_element() -> None:
    """A non-Element input raises AttributeError."""
    with pytest.raises(AttributeError):
        get_text(123)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("{http://uniprot.org/uniprot}entry", "entry"),
        ("{http://uniprot.org/uniref}", ""),
        ("{}entry", "entry"),
        ("{http://example.org/{nested}}item", "item"),
        ("entry", "entry"),
        ("", ""),
        (None, None),
    ],
    ids=[
        "clark_notation",
        "namespace_only_empty_string",
        "empty_namespace_braces",
        "nested_braces_in_namespace",
        "unprefixed_tag",
        "empty_string",
        "non_string_input",
    ],
)
def test_local_name_pass_extracts_local_part(tag: str | None, expected: str | None) -> None:
    """local_name strips Clark-notation namespaces and preserves unprefixed names."""
    assert local_name(tag) == expected  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize(
    ("xml", "expected"),
    [
        (
            "<entry xmlns='https://uniprot.org/uniprot'><name>A4_HUMAN</name></entry>",
            "<entry><name>A4_HUMAN</name></entry>",
        ),
        (
            (
                '<entry xmlns="https://uniprot.org/uniprot" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
                "<accession>P12345</accession></entry>"
            ),
            "<entry><accession>P12345</accession></entry>",
        ),
        (
            "<entry xmlns='http://uniprot.org/uniref'><name>Cluster: x</name></entry>",
            "<entry><name>Cluster: x</name></entry>",
        ),
        (
            "<entry xmlns='https://uniprot.org/uniref'><name>Cluster: y</name></entry>",
            "<entry><name>Cluster: y</name></entry>",
        ),
        (
            (
                '<entry xmlns="http://uniprot.org/uniref" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
                "<name>Cluster: z</name></entry>"
            ),
            "<entry><name>Cluster: z</name></entry>",
        ),
        ("<entry xmlns='https://uniprot.org/uniprot'/>", "<entry/>"),
        ("<entry xmlns='http://uniprot.org/uniref'/>", "<entry/>"),
    ],
)
def test_dump_xml_element_pass_strips_namespace_declarations(xml: str, expected: str) -> None:
    """Test that namespace declarations are stripped when dumping XML to a string."""
    dumped = dump_xml_element(fromstring(xml, parser=XMLParser(remove_blank_text=True)))
    assert dumped == expected
