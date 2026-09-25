"""Tests for the xml_utils module."""

from lxml.etree import fromstring

from cdm_data_loaders.utils.xml_utils import (
    get_attr,
    get_text,
)


def test_get_text_pass_returns_stripped_text() -> None:
    """Returns text content without surrounding whitespace."""
    element = fromstring("<entry>  value  </entry>")

    assert get_text(element) == "value"


def test_get_text_pass_returns_default_for_missing_or_blank_text() -> None:
    """Returns the default when text is absent, empty, or whitespace only."""
    empty_element = fromstring("<entry />")
    blank_element = fromstring("<entry>   </entry>")

    assert get_text(None, "fallback") == "fallback"
    assert get_text(empty_element, "fallback") == "fallback"
    assert get_text(blank_element, "fallback") == "fallback"


def test_get_attr_pass_returns_stripped_attribute_value() -> None:
    """Returns attribute content without surrounding whitespace."""
    element = fromstring('<entry name="  value  " />')

    assert get_attr(element, "name") == "value"


def test_get_attr_pass_returns_default_for_missing_element_or_attribute() -> None:
    """Returns the default when the element or requested attribute is absent."""
    element = fromstring("<entry />")

    assert get_attr(None, "name", "fallback") == "fallback"
    assert get_attr(element, "name", "fallback") == "fallback"


def test_get_attr_pass_preserves_empty_attribute_value() -> None:
    """Returns an empty string for an explicitly empty attribute."""
    element = fromstring('<entry name="" />')

    assert get_attr(element, "name", "fallback") == ""
