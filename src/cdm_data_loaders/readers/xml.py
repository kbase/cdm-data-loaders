"""Common reusable XML pipeline elements."""

import gzip
from collections.abc import Callable, Generator
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

import dlt
import xmltodict
from dlt.extract.items import DataItemWithMeta
from lxml.etree import Element, iterparse, tostring

from cdm_data_loaders.core.settings import BatchedFileInputSettings
from cdm_data_loaders.utils.batcher import get_file_batches

DEFAULT_BUFFER_SIZE: Final[int] = 100
DEFAULT_LOG_INTERVAL: Final[int] = 1000

logger: Logger = getLogger(__name__)


def stream_xml_file(file_path: str | Path, element_with_ns: str) -> Generator[Element, Any]:
    """Stream XML elements from a file.

    :param file_path: path to the XML file; file can be gzipped or not.
    :type file_path: str | Path
    :param element_with_ns: name of the element (including namespace, in braces) to return; e.g. f"{{{UNIPROT_NS}}}entry"
    :type element_with_ns: str
    :yield: elements from the file
    :rtype: Generator[Element, Any]
    """
    if isinstance(file_path, Path):
        file_path = str(file_path)
    logger.debug("Streaming XML from %s", file_path)
    open_fn = open
    if file_path.endswith(".gz"):
        open_fn = gzip.open

    with open_fn(file_path, "rb") as f:
        for _, elem in iterparse(f, tag=(element_with_ns), remove_blank_text=True):
            logger.debug(elem)
            yield elem
            elem.clear()


def process_xml_file(
    file_path: Path,
    xml_tag: str,
    parse_fn: Callable,
    buffer_size: int = 100,
    log_interval: int = 1000,
) -> Generator[DataItemWithMeta, Any]:
    """Core generator shared by XML-based dlt pipeline resources.

    :param file_path: path to the XML file to be processed
    :type file_path: Path
    :param xml_tag: XML element tag to stream
    :type xml_tag: str
    :param parse_fn: callable(entry, timestamp, file_path) -> dict[str, rows]
    :type parse_fn: Callable
    :param buffer_size: number of elements to amass before yielding data, defaults to 100
    :type buffer_size: int, optional
    :param log_interval: log a progress message every N entries
    :type log_interval: int
    :yield: table-tagged rows
    :rtype: Generator[DataItemWithMeta, Any]
    """
    if buffer_size <= 0:
        buffer_size = DEFAULT_BUFFER_SIZE
    if log_interval <= 0:
        log_interval = DEFAULT_LOG_INTERVAL

    logger.info("Reading from %s", str(file_path))
    n_entries = -1
    dict_buffer: dict[str, list[Any]] = {}
    for n_entries, entry in enumerate(stream_xml_file(file_path, xml_tag)):
        parsed_entry = parse_fn(entry=entry, file_path=file_path)
        for table, rows in parsed_entry.items():
            if table not in dict_buffer:
                dict_buffer[table] = []
            dict_buffer[table].extend(rows)
            if len(dict_buffer[table]) >= buffer_size:
                yield dlt.mark.with_table_name(dict_buffer[table], table)
                dict_buffer[table] = []
        if (n_entries + 1) % log_interval == 0:
            logger.debug("Processed %d entries", n_entries + 1)
    if (n_entries + 1) % log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, file_path.name)

    for table, rows in dict_buffer.items():
        if rows:
            yield dlt.mark.with_table_name(rows, table)


def process_xml_file_batches(
    settings: BatchedFileInputSettings,
    xml_tag: str,
    parse_fn: Callable,
    buffer_size: int = 100,
    log_interval: int = 1000,
) -> Generator[DataItemWithMeta, Any]:
    """Generator that uses the NumericFileSequenceBatcher to generate a list of XML files to process.

    :param settings: pipeline config with input_dir and start_at
    :type settings: BatchedFileInputSettings
    :param xml_tag: XML element tag to stream
    :type xml_tag: str
    :param parse_fn: function for parsing the XML
    :type parse_fn: Callable
    :param log_interval: log a progress message every N entries (implemented in parse_fn)
    :type log_interval: int
    """
    for files in get_file_batches(settings):
        for file_path in files:
            yield from process_xml_file(
                file_path, xml_tag, parse_fn, buffer_size=buffer_size, log_interval=log_interval
            )


def xml_to_dict_parse_fn(table_name: str) -> Callable[..., dict[str, list[dict[str, Any]]]]:
    """XML parsing function that executes xmltodict on each element in an XML file.

    :param table_name: table to export parsed data to
    :type table_name: str
    :return: parse function
    :rtype: Callable[..., dict[str, list[dict[str, Any]]]]
    """

    def parse_fn(entry: Element, file_path: Path) -> dict[str, list[dict[str, Any]]]:  # noqa: ARG001
        """A parse_fn that runs xmltodict on the given XML entity."""
        return {table_name: [xmltodict.parse(tostring(entry))]}

    return parse_fn


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
