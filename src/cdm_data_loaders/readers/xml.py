"""Common reusable XML pipeline elements for the xmltodict-based pipelines.

Files are given as a local path or as a dlt ``FileItemDict``; file items work for any fsspec protocol (local, S3,
...). Every file is opened with ``FileItemDict.open``, which decompresses ``*.gz`` files.
"""

import functools
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from logging import Logger, getLogger
from typing import IO, Any

import xmltodict
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta
from lxml.etree import Element, _Element, iterparse, tostring

from cdm_data_loaders.core.registry import RecordTransformFn
from cdm_data_loaders.core.settings import CtsSettings, XmlIngestSettings
from cdm_data_loaders.core.transforms import route_and_coerce_record
from cdm_data_loaders.pipelines.core import build_per_file_filesystem_resource
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from cdm_data_loaders.utils.buffer import DictBuffer
from cdm_data_loaders.utils.file_items import FileSource, open_binary, source_id, source_url
from cdm_data_loaders.utils.xml_utils import local_name

logger: Logger = getLogger(__name__)

DEFAULT_XMLTODICT_ARGS: dict[str, str | bool | int | None] = {"attr_prefix": "_"}


def parse_head_matter(file_path: FileSource | IO[bytes]) -> dict[str, str]:
    """Parse the namespace declarations from the head matter of an XML file.

    Reads the beginning of an XML document (gzipped or plain) and collects all XML
    namespace declarations (``xmlns`` / ``xmlns:prefix`` attributes) that are in scope
    on the root element. Parsing stops as soon as the root element is fully opened, so
    the whole document is never loaded into memory.

    The returned mapping uses namespace prefixes as keys and namespace URIs as values.
    The default (unprefixed) namespace is stored under the empty-string key ``""``.

    Given an open handle, parsing starts at the handle's current position and the handle is
    left open, positioned wherever parsing stopped; call ``seek(0)`` before reading it again.

    :param file_path: local path, dlt file item, or open binary handle; the file can be gzipped or not.
    :type file_path: FileSource | IO[bytes]
    :raises FileNotFoundError: if the file does not exist.
    :raises lxml.etree.XMLSyntaxError: if the file is empty or malformed.
    :return: mapping of namespace prefix to namespace URI.
    :rtype: dict[str, str]
    """
    logger.debug("Parsing head matter from %s", source_url(file_path))

    namespaces: dict[str, str] = {}
    with open_binary(file_path) as fh:
        ctx = iterparse(fh, events=("start-ns", "start"))
        for event, elem in ctx:
            if event == "start-ns":
                # elem is a (prefix, uri) tuple; the default namespace has an empty prefix.
                prefix, uri = elem
                namespaces[prefix] = uri
                continue

            # The first "start" event corresponds to the root element; by this point every
            # namespace declared on the root has already been emitted as a "start-ns" event.
            break

    logger.debug("Found %d namespace declaration(s) in %s", len(namespaces), source_url(file_path))
    return namespaces


def _clear_element(elem: Element) -> None:
    """Clear an XML element's subtree and prune preceding siblings from its parent."""
    if not isinstance(elem, _Element):
        return
    elem.clear()
    parent = elem.getparent()
    while parent is not None and elem.getprevious() is not None and len(parent) > 0:
        del parent[0]


def stream_xml_file(file_path: FileSource | IO[bytes], element_with_ns: str) -> Generator[Element, Any]:
    """Stream the outermost matching XML elements from a file.

    A matching element nested inside another matching element is yielded as part of the outer element.

    Given an open handle, streaming starts at the handle's current position and the handle is left open.

    :param file_path: local path, dlt file item, or open binary handle; the file can be gzipped or not.
    :type  file_path: FileSource | IO[bytes]
    :param element_with_ns: name of the element (including namespace, in braces) to return; e.g. f"{{{UNIPROT_NS}}}entry"
    :type  element_with_ns: str
    :yield: elements from the file
    :rtype: Generator[Element, Any]
    """
    logger.debug("Streaming XML from %s", source_url(file_path))

    with open_binary(file_path) as f:
        # number of open matching elements; 0 means the parser is outside any match
        depth = 0
        for event, elem in iterparse(f, events=("start", "end"), tag=element_with_ns, remove_blank_text=True):
            if event == "start":
                depth += 1
                continue
            depth -= 1
            if depth:
                # nested match: leave it in place as part of the enclosing match
                continue
            yield elem
            _clear_element(elem)


def stream_xml_file_at_level(file_path: FileSource | IO[bytes], level: int) -> Generator[Element, Any]:
    """Stream all XML elements at depth ``level`` relative to the root element.

    Depth ``0`` yields the root element; depth ``1`` yields all direct children of the root
    element; depth ``2`` yields all grandchildren of the root element, and so on.

    Given an open handle, streaming starts at the handle's current position and the handle is left open.

    :param file_path: local path, dlt file item, or open binary handle; the file can be gzipped or not.
    :type  file_path: FileSource | IO[bytes]
    :param level: non-negative depth relative to the root element
    :type  level: int
    :yield: elements at depth ``level`` from the file
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
                _clear_element(elem)
            elif 0 < current_depth < level:
                _clear_element(elem)
            current_depth -= 1


def _log_interval_progress(n_entries: int, log_interval: int) -> None:
    """Log the processed-entry count every log_interval elements."""
    if (n_entries + 1) % log_interval == 0:
        logger.debug("Processed %d entries", n_entries + 1)


def _log_interval_final(n_entries: int, file_path: FileSource, log_interval: int) -> None:
    """Log the trailing entry count if the final checkpoint was not an interval boundary."""
    if n_entries >= 0 and (n_entries + 1) % log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, source_id(file_path))


def _resolve_stream_tag(fh: IO[bytes], xml_tag: str) -> str:
    """Resolve an unprefixed XML tag against the default namespace in the head matter of ``fh``.

    Reads from the handle's current position; the caller rewinds the handle afterwards.
    """
    if not xml_tag.startswith("{"):
        default_ns = parse_head_matter(fh).get("")
        if default_ns:
            return f"{{{default_ns}}}{xml_tag}"
    return xml_tag


def _stream_elements(file_path: FileSource, xml_tag: str, *, resolve_default_ns: bool) -> Iterator[Element]:
    """Stream the ``xml_tag`` elements from ``file_path``, opening the file once.

    When the tag must be resolved against the file's default namespace, the head matter is read
    from the same handle, which is then rewound with ``seek(0)`` and streamed. The handles that
    ``FileItemDict.open`` returns (local, fsspec remote, gzip) all support seeking; for remote files,
    rewinding is usually served from the already-downloaded first block rather than a new request.
    """
    if not resolve_default_ns or xml_tag.startswith("{"):
        yield from stream_xml_file(file_path, xml_tag)
        return

    with open_binary(file_path) as fh:
        stream_tag = _resolve_stream_tag(fh, xml_tag)
        fh.seek(0)
        yield from stream_xml_file(fh, stream_tag)


def _parse_element_to_dict(element: Element) -> dict[str, Any] | None:
    """Parse an XML element with ``xmltodict`` and strip root-level ``_xmlns`` declarations."""
    parsed_element = xmltodict.parse(tostring(element), **DEFAULT_XMLTODICT_ARGS)
    if not parsed_element:
        return None
    for k, v in parsed_element.items():
        # remove the xmlns declarations
        parsed_element[k] = {kv: val for kv, val in v.items() if not kv.startswith("_xmlns")}
    return parsed_element


def _stream_and_buffer_element_iter(
    file_path: FileSource,
    elements: Iterable[Element],
    log_interval: int,
    buffer: DictBuffer,
    buffer_element_fn: Callable[[Element], Iterable[DataItemWithMeta]],
) -> Generator[DataItemWithMeta]:
    """Iterate over streamed XML ``elements``, buffer parsed items, log progress, and flush.

    :param file_path: local path or dlt file item
    :type  file_path: FileSource
    :param elements: iterable of streamed XML elements
    :type  elements: Iterable[Element]
    :param log_interval: entry interval at which progress is logged
    :type  log_interval: int
    :param buffer: dictionary or PyArrow batch buffer
    :type  buffer: DictBuffer
    :param buffer_element_fn: callback converting an element and adding records to buffer
    :type  buffer_element_fn: Callable[[Element], Iterable[DataItemWithMeta]]
    :yield: flushed pages of items or PyArrow tables
    :rtype : Generator[DataItemWithMeta]
    """
    logger.info("Reading from %s", source_url(file_path))
    n_entries = -1
    for n_entries, element in enumerate(elements):
        flushed_items = list(buffer_element_fn(element))
        _clear_element(element)
        yield from flushed_items
        _log_interval_progress(n_entries, log_interval)

    _log_interval_final(n_entries, file_path, log_interval)
    yield from buffer.flush()


def _stream_and_buffer_xml_elements(  # noqa: PLR0913
    file_path: FileSource,
    xml_tag: str,
    log_interval: int,
    buffer: DictBuffer,
    buffer_element_fn: Callable[[Element], Iterable[DataItemWithMeta]],
    *,
    resolve_default_ns: bool = False,
) -> Generator[DataItemWithMeta]:
    """Stream matching XML elements from ``file_path``, buffer parsed items, log progress, and flush."""
    yield from _stream_and_buffer_element_iter(
        file_path=file_path,
        elements=_stream_elements(file_path, xml_tag, resolve_default_ns=resolve_default_ns),
        log_interval=log_interval,
        buffer=buffer,
        buffer_element_fn=buffer_element_fn,
    )


def _buffer_level_pyarrow_element(
    element: Element,
    *,
    level: int,
    converters: Mapping[str, Callable[[Element], dict[str, Any]]],
    settings: XmlToPyArrowSettings,
    buffer: DictBuffer,
) -> Iterable[DataItemWithMeta]:
    """Convert a level-streamed XML element by its tag name and add the resulting rows to ``buffer``.

    :param element: streamed XML element at ``level``
    :type  element: Element
    :param level: XML depth level relative to the root element
    :type  level: int
    :param converters: mapping from tag name to element-to-dict converter
    :type  converters: Mapping[str, Callable[[Element], dict[str, Any]]]
    :param settings: pipeline configuration
    :type  settings: XmlToPyArrowSettings
    :param buffer: multi-table PyArrow batch buffer
    :type  buffer: DictBuffer
    :return: flushed PyArrow table batches, if any
    :rtype : Iterable[DataItemWithMeta]
    """
    tag_name = local_name(element.tag)
    converter = converters.get(tag_name)
    if converter is None:
        msg = f"Unexpected XML element <{tag_name}> at level {level} not declared in XSD schema"
        raise ValueError(msg)
    transform = settings.get_table_transform(tag_name)
    return buffer.add_items(
        route_and_coerce_record(
            converter(element),
            tag_name,
            transform=transform,
            schema_map=buffer.schema_map,
        )
    )


def _buffer_single_tag_pyarrow_element(
    element: Element,
    *,
    convert_element: Callable[[Element], dict[str, Any]],
    table_name: str,
    transform: RecordTransformFn | None,
    buffer: DictBuffer,
) -> Iterable[DataItemWithMeta]:
    """Convert a single-tag XML element and add the resulting rows to ``buffer``.

    :param element: streamed XML element matching ``xml_tag``
    :type  element: Element
    :param convert_element: XSD-driven element-to-dict converter
    :type  convert_element: Callable[[Element], dict[str, Any]]
    :param table_name: target table name
    :type  table_name: str
    :param transform: optional table transform
    :type  transform: RecordTransformFn | None
    :param buffer: PyArrow batch buffer
    :type  buffer: DictBuffer
    :return: flushed PyArrow table batches, if any
    :rtype : Iterable[DataItemWithMeta]
    """
    return buffer.add_items(
        route_and_coerce_record(
            convert_element(element),
            table_name,
            transform=transform,
            schema_map=buffer.schema_map,
        )
    )


def _process_xml_file_to_pyarrow_at_level(
    file_path: FileSource,
    settings: XmlToPyArrowSettings,
    level: int,
) -> Generator[DataItemWithMeta]:
    """Stream XML elements at ``level``, convert each by its tag name, and yield PyArrow batches.

    :param file_path: local path or dlt file item
    :type  file_path: FileSource
    :param settings: pipeline configuration
    :type  settings: XmlToPyArrowSettings
    :param level: XML depth level relative to the root element
    :type  level: int
    :yield: pages of PyArrow tables
    :rtype : Generator[DataItemWithMeta]
    """
    buffer = DictBuffer(
        schema_map=settings.schema_map,
        max_items=settings.buffer_size,
    )
    converters = settings.xml_level_converters

    yield from _stream_and_buffer_element_iter(
        file_path=file_path,
        elements=stream_xml_file_at_level(file_path, level),
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=functools.partial(
            _buffer_level_pyarrow_element,
            level=level,
            converters=converters,
            settings=settings,
            buffer=buffer,
        ),
    )


def process_xml_file_to_pyarrow(file_path: FileSource, settings: XmlToPyArrowSettings) -> Generator[DataItemWithMeta]:
    """Convert XML elements to PyArrow table batches using an XSD schema.

    When ``settings.xml_level`` is set, all elements at that depth are streamed, converted using
    ``settings.xsd_schema``, and routed to tables named after each element's local tag name
    (with PyArrow schemas from ``settings.schema_map``). Otherwise, elements matching
    ``settings.xml_tag`` are streamed and buffered into ``settings.table_name``.

    :param file_path: local path or dlt file item
    :type  file_path: FileSource
    :param settings: pipeline config, including xml_tag/table_name or xml_level, xsd_schema, schema_map, and buffer_size
    :type  settings: XmlToPyArrowSettings
    :yield: pages of PyArrow tables
    :rtype: Generator[DataItemWithMeta]
    """
    xml_level = getattr(settings, "xml_level", None)
    if isinstance(xml_level, int):
        yield from _process_xml_file_to_pyarrow_at_level(file_path, settings, xml_level)
        return

    if settings.table_name is None or settings.xml_tag is None:
        err_msg = "Expected table_name and xml_tag when xml_level is not set"
        raise RuntimeError(err_msg)

    convert_element = settings.xml_element_converter
    if convert_element is None:
        err_msg = "Expected xml_element_converter when xml_level is not set"
        raise RuntimeError(err_msg)

    transform = settings.get_table_transform(settings.table_name)
    schema_map = settings.schema_map
    buffer = DictBuffer(
        schema_map=schema_map,
        max_items=settings.buffer_size,
    )
    yield from _stream_and_buffer_xml_elements(
        file_path=file_path,
        xml_tag=settings.xml_tag,
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=functools.partial(
            _buffer_single_tag_pyarrow_element,
            convert_element=convert_element,
            table_name=settings.table_name,
            transform=transform,
            buffer=buffer,
        ),
        resolve_default_ns=True,
    )


def process_xml_file_to_dict(file_path: FileSource, settings: XmlToDictSettings) -> Generator[DataItemWithMeta]:
    """Convert XML elements to dictionary form using ``xmltodict.parse``.

    :param file_path: local path or dlt file item
    :type  file_path: FileSource
    :param settings: pipeline config, including xml_tag, table_name, and buffer_size
    :type  settings: XmlToDictSettings
    :yield: pages of dictionary items
    :rtype: Generator[DataItemWithMeta]
    """
    transform = settings.get_table_transform(settings.table_name)
    schema_map = settings.schema_map
    buffer = DictBuffer(max_items=settings.buffer_size, schema_map=schema_map)

    def _buffer_element(element: Element) -> Generator[DataItemWithMeta]:
        parsed_element = _parse_element_to_dict(element)
        if parsed_element:
            yield from buffer.add_items(
                route_and_coerce_record(
                    parsed_element,
                    settings.table_name,
                    transform=transform,
                    schema_map=schema_map,
                )
            )

    yield from _stream_and_buffer_xml_elements(
        file_path=file_path,
        xml_tag=settings.xml_tag,
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=_buffer_element,
    )


def process_xml_file(
    file_path: FileSource,
    settings: CtsSettings,
    xml_tag: str,
    parse_fn: Callable,
) -> Generator[DataItemWithMeta, Any]:
    """Core generator shared by XML-based dlt pipeline resources.

    This processor is expected to return a dictionary of table names and lists of rows, unlike the
    xmltodict parser, which creates a single dictionary for each element.

    ``parse_fn`` receives the file's source id (see ``cdm_data_loaders.utils.file_items.source_id``) as
    ``file_path``: the item's relative path for a file item, or the file name for a local path. The
    recorded source file therefore does not depend on where the input lives.

    :param file_path: local path or dlt file item for the XML file to be processed
    :type  file_path: FileSource
    :param settings: pipeline config with buffer_size and log_interval
    :type  settings: CtsSettings
    :param xml_tag: XML element tag to stream
    :type  xml_tag: str
    :param parse_fn: callable(entry, file_path) -> dict[str, rows]
    :type  parse_fn: Callable
    :yield: table-tagged rows
    :rtype: Generator[DataItemWithMeta, Any]
    """
    buffer = DictBuffer(max_items=settings.buffer_size)
    file_source_id = source_id(file_path)
    yield from _stream_and_buffer_xml_elements(
        file_path=file_path,
        xml_tag=xml_tag,
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=lambda element: buffer.add_items(parse_fn(entry=element, file_path=file_source_id)),
    )


def build_xml_file_resource(
    settings: XmlIngestSettings,
    xml_tag: str,
    parse_fn: Callable,
    resource_name: str,
) -> DltResource:
    """Build a dlt resource that parses the XML files in `settings.input_dir`.

    Files are read using the dlt filesystem source.

    :param settings: pipeline config with input_dir, file_glob, loader_file_format, buffer_size and log_interval
    :type  settings: XmlIngestSettings
    :param xml_tag: XML element tag to stream
    :type  xml_tag: str
    :param parse_fn: function for parsing the XML
    :type  parse_fn: Callable
    :param resource_name: name of the resulting dlt resource
    :type  resource_name: str
    :return: resource yielding table-tagged rows
    :rtype: DltResource
    """
    return build_per_file_filesystem_resource(
        settings,
        process_xml_file,
        xml_tag,
        parse_fn,
        name=resource_name,
        file_format=str(settings.loader_file_format),
        parallelized=False,
    )
