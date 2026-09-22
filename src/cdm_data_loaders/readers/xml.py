"""Common reusable XML pipeline elements."""

import gzip
from collections.abc import Callable, Generator
from logging import Logger, getLogger
from pathlib import Path
from typing import Any

import xmltodict
from dlt.extract.items import DataItemWithMeta
from frozendict import frozendict
from lxml.etree import Element, iterparse, tostring

from cdm_data_loaders.core.settings import BatchedFileInputSettings
from cdm_data_loaders.pipelines.xml_to_dict.settings import XmlToDictSettings
from cdm_data_loaders.utils.batcher import get_file_batches
from cdm_data_loaders.utils.buffer import DictBuffer, ListBuffer

logger: Logger = getLogger(__name__)

DEFAULT_XMLTODICT_ARGS = frozendict({"attr_prefix": "_"})

type XmlToDictValue = str | dict[str, "XmlToDictValue"] | list["XmlToDictValue"] | None
type XmlToDictResult = DataItemWithMeta | BaseException | None

XMLTODICT_QUEUE_POLL_SECONDS = 0.1


def parse_head_matter(file_path: str | Path) -> dict[str, str]:
    """Parse the namespace declarations from the head matter of an XML file.

    Reads the beginning of an XML document (gzipped or plain) and collects all XML
    namespace declarations (``xmlns`` / ``xmlns:prefix`` attributes) that are in scope
    on the root element. Parsing stops as soon as the root element is fully opened, so
    the whole document is never loaded into memory.

    The returned mapping uses namespace prefixes as keys and namespace URIs as values.
    The default (unprefixed) namespace is stored under the empty-string key ``""``.

    :param file_path: path to the XML file; the file can be gzipped or not.
    :type file_path: str | Path
    :raises FileNotFoundError: if the file does not exist.
    :raises lxml.etree.XMLSyntaxError: if the file is empty or malformed.
    :return: mapping of namespace prefix to namespace URI.
    :rtype: dict[str, str]
    """
    if isinstance(file_path, Path):
        file_path = str(file_path)
    logger.debug("Parsing head matter from %s", file_path)

    open_fn = open
    if file_path.endswith(".gz"):
        open_fn = gzip.open

    namespaces: dict[str, str] = {}
    with open_fn(file_path, "rb") as fh:
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

    logger.debug("Found %d namespace declaration(s) in %s", len(namespaces), file_path)
    return namespaces


def stream_xml_file(file_path: str | Path, element_with_ns: str) -> Generator[Element, Any]:
    """Stream XML elements from a file.

    :param file_path: path to the XML file; file can be gzipped or not.
    :type  file_path: str | Path
    :param element_with_ns: name of the element (including namespace, in braces) to return; e.g. f"{{{UNIPROT_NS}}}entry"
    :type  element_with_ns: str
    :yield: elements from the file
    :rtype: Generator[Element, Any]
    """
    if isinstance(file_path, Path):
        file_path = str(file_path)
    logger.debug("Streaming XML from %s", file_path)
    open_fn = gzip.open if file_path.endswith(".gz") else open

    with open_fn(file_path, "rb") as f:
        for _, elem in iterparse(f, tag=(element_with_ns), remove_blank_text=True):
            logger.debug(elem)
            yield elem
            elem.clear()


def process_xml_file_to_dict(settings: XmlToDictSettings, file_path: Path) -> Generator[DataItemWithMeta]:
    """Generator for converting XML to dictionary form.

    Note: each incoming element produces a single dictionary as output.

    Use process_xml_file for parsing functions that return data spread across several tables.

    :param settings: pipeline config, including xml_tag, table_name, and buffer_size
    :type settings: XmlToDictSettings
    :param file_path: file path, as a string
    :type file_item: FileItemDict
    :yield: pages of dictionary items
    :rtype: Generator[DataItemWithMeta]
    """
    logger.info("Reading from %s", str(file_path))
    n_entries = -1

    buffer = ListBuffer(table_name=settings.table_name, max_items=settings.buffer_size)
    for n_entries, element in enumerate(stream_xml_file(file_path, settings.xml_tag)):
        parsed_element = xmltodict.parse(tostring(element), **DEFAULT_XMLTODICT_ARGS, **settings.xmltodict_args)
        if parsed_element:
            for k, v in parsed_element.items():
                # remove the xmlns declarations
                parsed_element[k] = {kv: val for kv, val in v.items() if not kv.startswith("_xmlns")}
            yield from buffer.add_item(parsed_element)
        if (n_entries + 1) % settings.log_interval == 0:
            logger.debug("Processed %d entries", n_entries + 1)

    if n_entries >= 0 and (n_entries + 1) % settings.log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, file_path.name)

    yield from buffer.flush()


def process_xml_file(
    settings: BatchedFileInputSettings,
    xml_tag: str,
    parse_fn: Callable,
    file_path: Path,
) -> Generator[DataItemWithMeta, Any]:
    """Core generator shared by XML-based dlt pipeline resources.

    This processor is expected to return a dictionary of table names and lists of rows, unlike the
    xml_to_dict parser, which creates a single dictionary for each element.

    :param settings: pipeline config with input_dir and start_at
    :type  settings: BatchedFileInputSettings
    :param xml_tag: XML element tag to stream
    :type  xml_tag: str
    :param parse_fn: callable(element, timestamp, file_path) -> dict[str, rows]
    :type  parse_fn: Callable
    :param file_path: path to the XML file to be processed
    :type  file_path: Path
    :yield: table-tagged rows
    :rtype: Generator[DataItemWithMeta, Any]
    """
    logger.info("Reading from %s", str(file_path))
    n_entries = -1
    buffer = DictBuffer(max_items=settings.buffer_size)
    for n_entries, element in enumerate(stream_xml_file(file_path, xml_tag)):
        parsed_element = parse_fn(entry=element, file_path=file_path)
        yield from buffer.add_items(parsed_element)

        if (n_entries + 1) % settings.log_interval == 0:
            logger.debug("Processed %d entries", n_entries + 1)

    if (n_entries + 1) % settings.log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, file_path.name)

    yield from buffer.flush()


def process_xml_file_batches(
    settings: BatchedFileInputSettings,
    xml_tag: str,
    parse_fn: Callable,
) -> Generator[DataItemWithMeta, Any]:
    """Generator that uses the NumericFileSequenceBatcher to generate a list of XML files to process.

    :param settings: pipeline config with input_dir and start_at
    :type settings: BatchedFileInputSettings
    :param xml_tag: XML element tag to stream
    :type xml_tag: str
    :param parse_fn: function for parsing the XML
    :type parse_fn: Callable
    """
    for files in get_file_batches(settings):
        for file_path in files:
            yield from process_xml_file(
                settings=settings,
                xml_tag=xml_tag,
                parse_fn=parse_fn,
                file_path=file_path,
            )
