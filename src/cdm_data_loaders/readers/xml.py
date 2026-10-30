"""Common reusable XML pipeline elements for the xmltodict-based pipelines."""

from collections.abc import Callable, Generator, Iterable
from logging import Logger, getLogger
from typing import Any

import dlt
import xmltodict
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta
from lxml.etree import Element, iterparse, tostring

from cdm_data_loaders.core.fields import DEFAULT_XML_FILE_GLOB
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.input_source import build_input_files
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from cdm_data_loaders.readers.xsd import convert_xml_element
from cdm_data_loaders.utils.buffer import ArrowListBuffer, DictBuffer, ListBuffer
from cdm_data_loaders.utils.file_items import InputFile, open_input, source_label, source_name

logger: Logger = getLogger(__name__)

DEFAULT_XMLTODICT_ARGS: dict[str, str | bool | int | None] = {"attr_prefix": "_"}


def parse_head_matter(file_path: InputFile) -> dict[str, str]:
    """Parse the namespace declarations from the head matter of an XML file.

    Reads the beginning of an XML document (gzipped or plain) and collects all XML
    namespace declarations (``xmlns`` / ``xmlns:prefix`` attributes) that are in scope
    on the root element. Parsing stops as soon as the root element is fully opened, so
    the whole document is never loaded into memory.

    The returned mapping uses namespace prefixes as keys and namespace URIs as values.
    The default (unprefixed) namespace is stored under the empty-string key ``""``.

    :param file_path: local path or dlt file item for the XML file; the file can be gzipped or not.
    :type file_path: InputFile
    :raises FileNotFoundError: if the file does not exist.
    :raises lxml.etree.XMLSyntaxError: if the file is empty or malformed.
    :return: mapping of namespace prefix to namespace URI.
    :rtype: dict[str, str]
    """
    label = source_label(file_path)
    logger.debug("Parsing head matter from %s", label)

    namespaces: dict[str, str] = {}
    with open_input(file_path) as fh:
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

    logger.debug("Found %d namespace declaration(s) in %s", len(namespaces), label)
    return namespaces


def stream_xml_file(file_path: InputFile, element_with_ns: str) -> Generator[Element, Any]:
    """Stream the outermost matching XML elements from a file.

    A matching element nested inside another matching element is yielded as part of the outer element.

    :param file_path: local path or dlt file item for the XML file; file can be gzipped or not.
    :type  file_path: InputFile
    :param element_with_ns: name of the element (including namespace, in braces) to return; e.g. f"{{{UNIPROT_NS}}}entry"
    :type  element_with_ns: str
    :yield: elements from the file
    :rtype: Generator[Element, Any]
    """
    logger.debug("Streaming XML from %s", source_label(file_path))

    with open_input(file_path) as f:
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
            logger.debug(elem)
            yield elem
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]


def _log_interval_progress(n_entries: int, log_interval: int) -> None:
    """Log the processed-entry count every log_interval elements."""
    if (n_entries + 1) % log_interval == 0:
        logger.debug("Processed %d entries", n_entries + 1)


def _log_interval_final(n_entries: int, file_path: InputFile, log_interval: int) -> None:
    """Log the trailing entry count if the final checkpoint was not an interval boundary."""
    if n_entries >= 0 and (n_entries + 1) % log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, source_name(file_path))


def _resolve_stream_tag(file_path: InputFile, xml_tag: str) -> str:
    """Resolve an unprefixed XML tag against the file's default namespace if present."""
    if not xml_tag.startswith("{"):
        default_ns = parse_head_matter(file_path).get("")
        if default_ns:
            return f"{{{default_ns}}}{xml_tag}"
    return xml_tag


def _parse_element_to_dict(element: Element) -> dict[str, Any] | None:
    """Parse an XML element with ``xmltodict`` and strip root-level ``_xmlns`` declarations."""
    parsed_element = xmltodict.parse(tostring(element), **DEFAULT_XMLTODICT_ARGS)
    if not parsed_element:
        return None
    for k, v in parsed_element.items():
        # remove the xmlns declarations
        parsed_element[k] = {kv: val for kv, val in v.items() if not kv.startswith("_xmlns")}
    return parsed_element


def _stream_and_buffer_xml_elements(  # noqa: PLR0913
    file_path: InputFile,
    xml_tag: str,
    log_interval: int,
    buffer: ArrowListBuffer | DictBuffer | ListBuffer,
    buffer_element_fn: Callable[[Element], Iterable[DataItemWithMeta]],
    *,
    resolve_default_ns: bool = False,
) -> Generator[DataItemWithMeta]:
    """Stream matching XML elements from ``file_path``, buffer parsed items, log progress, and flush."""
    logger.info("Reading from %s", source_label(file_path))
    stream_tag = _resolve_stream_tag(file_path, xml_tag) if resolve_default_ns else xml_tag
    n_entries = -1
    for n_entries, element in enumerate(stream_xml_file(file_path, stream_tag)):
        yield from buffer_element_fn(element)
        _log_interval_progress(n_entries, log_interval)

    _log_interval_final(n_entries, file_path, log_interval)
    yield from buffer.flush()


def process_xml_file_to_pyarrow(settings: XmlToPyArrowSettings, file_path: InputFile) -> Generator[DataItemWithMeta]:
    """Convert XML elements to PyArrow table batches using an XSD schema.

    Each matching element is converted using ``settings.xsd_schema`` and buffered into PyArrow
    tables matching ``settings.pyarrow_schema``.

    Use process_xml_file for parsing functions that return data spread across several tables.

    :param settings: pipeline config, including xml_tag, table_name, xsd_schema, pyarrow_schema, and buffer_size
    :type settings: XmlToPyArrowSettings
    :param file_path: local path or dlt file item for the XML file
    :type file_path: InputFile
    :yield: pages of PyArrow tables
    :rtype: Generator[DataItemWithMeta]
    """
    arrow_buffer = ArrowListBuffer(
        table_to_schema_map={settings.table_name: settings.pyarrow_schema},
        max_items=settings.buffer_size,
    )
    yield from _stream_and_buffer_xml_elements(
        file_path=file_path,
        xml_tag=settings.xml_tag,
        log_interval=settings.log_interval,
        buffer=arrow_buffer,
        buffer_element_fn=lambda element: arrow_buffer.add_item(
            convert_xml_element(
                element,
                settings.xsd_schema,
                settings.xml_tag,
                max_recursion_depth=settings.max_recursion_depth,
            )
        ),
        resolve_default_ns=True,
    )


def process_xml_file_to_dict(settings: XmlToDictSettings, file_path: InputFile) -> Generator[DataItemWithMeta]:
    """Convert XML elements to dictionary form using ``xmltodict.parse``.

    Each matching element is converted with ``xmltodict.parse`` and buffered as dicts.

    Use process_xml_file for parsing functions that return data spread across several tables.

    :param settings: pipeline config, including xml_tag, table_name, and buffer_size
    :type settings: XmlToDictSettings
    :param file_path: local path or dlt file item for the XML file
    :type file_path: InputFile
    :yield: pages of dictionary items
    :rtype: Generator[DataItemWithMeta]
    """
    buffer = ListBuffer(table_name=settings.table_name, max_items=settings.buffer_size)

    def _buffer_element(element: Element) -> Generator[DataItemWithMeta]:
        parsed_element = _parse_element_to_dict(element)
        if parsed_element:
            yield from buffer.add_item(parsed_element)

    yield from _stream_and_buffer_xml_elements(
        file_path=file_path,
        xml_tag=settings.xml_tag,
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=_buffer_element,
    )


def process_xml_file(
    settings: CtsSettings,
    xml_tag: str,
    parse_fn: Callable,
    file_path: InputFile,
) -> Generator[DataItemWithMeta, Any]:
    """Core generator shared by XML-based dlt pipeline resources.

    This processor is expected to return a dictionary of table names and lists of rows, unlike the
    xmltodict parser, which creates a single dictionary for each element.

    `parse_fn` receives the file's provenance label as `file_path`: the item's `relative_path` for
    dlt file items, or the path as given for local paths.

    :param settings: pipeline config with buffer_size and log_interval
    :type  settings: CtsSettings
    :param xml_tag: XML element tag to stream
    :type  xml_tag: str
    :param parse_fn: callable(entry, file_path) -> dict[str, rows]
    :type  parse_fn: Callable
    :param file_path: local path or dlt file item for the XML file to be processed
    :type  file_path: InputFile
    :yield: table-tagged rows
    :rtype: Generator[DataItemWithMeta, Any]
    """
    buffer = DictBuffer(max_items=settings.buffer_size)
    label = source_label(file_path)
    yield from _stream_and_buffer_xml_elements(
        file_path=file_path,
        xml_tag=xml_tag,
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=lambda element: buffer.add_items(parse_fn(entry=element, file_path=label)),
    )


def process_xml_file_items(
    items: Iterable[FileItemDict],
    settings: CtsSettings,
    xml_tag: str,
    parse_fn: Callable,
) -> Generator[DataItemWithMeta, Any]:
    """Process each file item emitted by the dlt filesystem source.

    Each file is streamed from the item itself, and its `relative_path` is recorded as the source file,
    so recorded source file names do not depend on where the input files are stored.

    :param items: file items to read
    :type  items: Iterable[FileItemDict]
    :param settings: pipeline config with buffer_size and log_interval
    :type  settings: CtsSettings
    :param xml_tag: XML element tag to stream
    :type  xml_tag: str
    :param parse_fn: function for parsing the XML
    :type  parse_fn: Callable
    :yield: table-tagged rows
    :rtype: Generator[DataItemWithMeta, Any]
    """
    for file_item in items:
        yield from process_xml_file(
            settings=settings,
            xml_tag=xml_tag,
            parse_fn=parse_fn,
            file_path=file_item,
        )


def build_xml_file_resource(
    settings: CtsSettings,
    xml_tag: str,
    parse_fn: Callable,
    resource_name: str,
) -> DltResource:
    """Build a dlt resource that parses the pipeline's XML input files.

    Files are listed from `settings.use_resource` if set, otherwise from `settings.input_dir`.

    :param settings: pipeline config with input_dir or use_resource, buffer_size and log_interval
    :type  settings: CtsSettings
    :param xml_tag: XML element tag to stream
    :type  xml_tag: str
    :param parse_fn: function for parsing the XML
    :type  parse_fn: Callable
    :param resource_name: name of the resulting dlt resource
    :type  resource_name: str
    :return: resource yielding table-tagged rows
    :rtype: DltResource
    """
    files = build_input_files(settings, file_glob=DEFAULT_XML_FILE_GLOB)
    resource = dlt.transformer(
        process_xml_file_items,
        data_from=files,
        name=resource_name,
        file_format="parquet",
        parallelized=True,
    )
    resource.bind(settings, xml_tag, parse_fn)
    return resource
