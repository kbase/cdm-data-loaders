"""Streaming XML reader utilities.

Files are accepted as a local path, a ``dlt`` ``FileItemDict`` (supporting any ``fsspec`` protocol), or an open binary
handle. ``FileItemDict.open`` decompresses ``*.gz`` files automatically.
"""

from collections.abc import Generator, Iterator
from logging import Logger, getLogger
from typing import IO, Any, Final

import xmltodict
from frozendict import frozendict
from lxml.etree import Element, _Element, iterparse, tostring

from cdm_data_loaders.core.file_items import FileSource, open_binary, source_url

logger: Logger = getLogger(__name__)

DEFAULT_XMLTODICT_ARGS: Final[frozendict[str, str | bool | int | None]] = frozendict({"attr_prefix": "_"})


def parse_head_matter(file_path: FileSource | IO[bytes]) -> dict[str, str]:
    """Parse the namespace declarations from the head matter of an XML file.

    Reads the beginning of an XML document (gzipped or plain) and collects all XML namespace declarations
    (``xmlns`` / ``xmlns:prefix`` attributes) in scope on the root element. Parsing stops once the root element opens,
    avoiding loading the full document into memory.

    The returned mapping uses namespace prefixes as keys and namespace URIs as values. The default (unprefixed)
    namespace is stored under ``""``.

    Given an open handle, parsing starts at the handle's position and leaves the handle open; call ``seek(0)`` before
    reading it again.

    :param file_path: Local path, ``dlt`` file item, or open binary handle (plain or gzipped).
    :type file_path: FileSource | IO[bytes]
    :raises FileNotFoundError: If the file does not exist.
    :raises lxml.etree.XMLSyntaxError: If the file is empty or malformed.
    :return: Mapping of namespace prefix to namespace URI.
    :rtype: dict[str, str]
    """
    logger.debug("Parsing head matter from %s", source_url(file_path))

    namespaces: dict[str, str] = {}
    with open_binary(file_path) as fh:
        ctx = iterparse(fh, events=("start-ns", "start"))
        for event, elem in ctx:
            if event == "start-ns":
                prefix, uri = elem
                namespaces[prefix] = uri
                continue

            # first "start" event is the root element; all root start-ns events have arrived
            break

    logger.debug("Found %d namespace declaration(s) in %s", len(namespaces), source_url(file_path))
    return namespaces


def clear_xml_element(elem: Element) -> None:
    """Clear an XML element's subtree and prune preceding siblings from its parent.

    :param elem: Streamed ``lxml`` element to clear.
    :type elem: Element
    """
    if not isinstance(elem, _Element):
        return
    elem.clear()
    parent = elem.getparent()
    while parent is not None and elem.getprevious() is not None and len(parent) > 0:
        del parent[0]


def stream_xml_file(file_path: FileSource | IO[bytes], element_with_ns: str) -> Generator[Element, Any]:
    """Stream the outermost matching XML elements from a file.

    A matching element nested inside another matching element is yielded as part of the outer element. Given an open
    handle, streaming starts at the handle's position and leaves the handle open.

    :param file_path: Local path, ``dlt`` file item, or open binary handle (plain or gzipped).
    :type file_path: FileSource | IO[bytes]
    :param element_with_ns: Element name (including namespace in braces) to match, e.g. ``f"{{{UNIPROT_NS}}}entry"``.
    :type element_with_ns: str
    :yield: Matching ``lxml`` elements from the file.
    :rtype: Generator[Element, Any]
    """
    logger.debug("Streaming XML from %s", source_url(file_path))

    with open_binary(file_path) as f:
        depth = 0
        for event, elem in iterparse(f, events=("start", "end"), tag=element_with_ns, remove_blank_text=True):
            if event == "start":
                depth += 1
                continue
            depth -= 1
            if depth:
                continue
            yield elem
            clear_xml_element(elem)


def stream_xml_file_at_level(file_path: FileSource | IO[bytes], level: int) -> Generator[Element, Any]:
    """Stream all XML elements at depth ``level`` relative to the root element.

    Depth ``0`` yields the root element; depth ``1`` yields direct children of the root element; depth ``2`` yields
    grandchildren, and so on. Given an open handle, streaming starts at the handle's position and leaves it open.

    :param file_path: Local path, ``dlt`` file item, or open binary handle (plain or gzipped).
    :type file_path: FileSource | IO[bytes]
    :param level: Non-negative depth relative to the root element.
    :type level: int
    :yield: Elements at depth ``level`` from the file.
    :rtype: Generator[Element, Any]
    """
    if level < 0:
        msg = f"level must be >= 0, got {level}"
        raise ValueError(msg)

    logger.debug("Streaming XML at level %d from %s", level, source_url(file_path))

    with open_binary(file_path) as f:
        current_depth = -1
        for event, elem in iterparse(f, events=("start", "end"), remove_blank_text=True):
            if event == "start":
                current_depth += 1
                continue
            if current_depth == level:
                yield elem
                clear_xml_element(elem)
            elif 0 < current_depth < level:
                clear_xml_element(elem)
            current_depth -= 1


def resolve_stream_tag(fh: IO[bytes], xml_tag: str) -> str:
    """Resolve an unprefixed XML tag against the default namespace in the head matter of ``fh``.

    Reads from the handle's position; the caller rewinds the handle before streaming.

    :param fh: Open binary handle positioned at the start of the XML document.
    :type fh: IO[bytes]
    :param xml_tag: Raw or Clark-notation XML tag name.
    :type xml_tag: str
    :return: Clark-notation XML tag when a default namespace is present, else ``xml_tag``.
    :rtype: str
    """
    if not xml_tag.startswith("{"):
        default_ns = parse_head_matter(fh).get("")
        if default_ns:
            return f"{{{default_ns}}}{xml_tag}"
    return xml_tag


def stream_elements(file_path: FileSource, xml_tag: str, *, resolve_default_ns: bool) -> Iterator[Element]:
    """Stream ``xml_tag`` elements from ``file_path``, opening the file once.

    When ``resolve_default_ns`` is ``True`` and ``xml_tag`` is unprefixed, the head matter is read from the handle,
    rewound with ``seek(0)``, and streamed from the same handle.

    :param file_path: Local path or ``dlt`` file item.
    :type file_path: FileSource
    :param xml_tag: Target XML tag name.
    :type xml_tag: str
    :param resolve_default_ns: When ``True``, resolve unprefixed ``xml_tag`` against the root default namespace.
    :type resolve_default_ns: bool
    :yield: Streamed ``lxml`` elements matching ``xml_tag``.
    :rtype: Iterator[Element]
    """
    if not resolve_default_ns or xml_tag.startswith("{"):
        yield from stream_xml_file(file_path, xml_tag)
        return

    with open_binary(file_path) as fh:
        stream_tag = resolve_stream_tag(fh, xml_tag)
        fh.seek(0)
        yield from stream_xml_file(fh, stream_tag)


def parse_element_to_dict(element: Element) -> dict[str, Any] | None:
    """Parse an XML element with ``xmltodict`` and strip root-level ``_xmlns`` declarations.

    :param element: Streamed ``lxml`` element.
    :type element: Element
    :return: Parsed element dictionary or ``None`` if empty.
    :rtype: dict[str, Any] | None
    """
    parsed_element = xmltodict.parse(tostring(element), **DEFAULT_XMLTODICT_ARGS)  # pyright: ignore[reportArgumentType]
    if not parsed_element:
        return None
    for k, v in parsed_element.items():
        parsed_element[k] = {kv: val for kv, val in v.items() if not kv.startswith("_xmlns")}
    return parsed_element
