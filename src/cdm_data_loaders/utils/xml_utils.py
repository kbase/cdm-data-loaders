"""Shared XML helper utilities used by XML readers and UniProt/UniRef parsers.

This module centralizes common operations:
- Safe text extraction
- Local (namespace-stripped) XML tag/attribute name extraction
- Namespace-stripped XML element string serialization
"""

from pathlib import Path

from lxml.etree import Element, tounicode

_STRIPPED_XMLNS = (
    ' xmlns="https://uniprot.org/uniprot"',
    ' xmlns="https://uniprot.org/uniref"',
    ' xmlns="http://uniprot.org/uniref"',
    ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
)


def dump_xml_element(element: Element) -> str:
    """Dump an XML element as a string with UniProt/UniRef/XSI namespace declarations stripped.

    :param element: the XML element
    :type element: Element
    :return: the element as a string
    :rtype: str
    """
    result = tounicode(element)
    for xmlns_decl in _STRIPPED_XMLNS:
        result = result.replace(xmlns_decl, "")
    return result


def get_text(elem: Element | None, default: str | None = None) -> str | None:
    """Return elem.text if exists and non-empty."""
    if elem is None:
        return default
    if elem.text is None:
        return default
    text = elem.text.strip()
    return text or default


def local_name(tag: str) -> str:
    """Return the local (namespace-stripped) part of an XML tag or attribute name.

    Handles Clark notation (``"{http://example.com}entry"`` -> ``"entry"``) and returns
    already-unprefixed names unchanged.
    """
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) and "}" in tag else tag


def make_parse_error(entry: Element, error: str | Exception, file_path: str | Path) -> dict[str, list[dict[str, str]]]:
    """Build a single-row ``_parse_error`` table payload for a failed XML entry."""
    return {"_parse_error": [{"xml": dump_xml_element(entry), "error": str(error), "source_file": str(file_path)}]}
