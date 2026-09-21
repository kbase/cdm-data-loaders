"""Common reusable XML pipeline elements."""

import gzip
from collections.abc import Callable, Generator, Mapping
from io import BytesIO
from logging import Logger, getLogger
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Any, cast
import dlt
import xmltodict
from dlt.extract.items import DataItemWithMeta
from frozendict import frozendict
from lxml.etree import Element, iterparse, tostring
from xml2db import DataModel, Document

from cdm_data_loaders.core.settings import BatchedFileInputSettings
from cdm_data_loaders.pipelines.xml2db.settings import Xml2DbSettings
from cdm_data_loaders.pipelines.xml_to_dict.settings import XmlToDictSettings
from cdm_data_loaders.utils.batcher import get_file_batches
from cdm_data_loaders.utils.buffer import DictBuffer, ListBuffer

logger: Logger = getLogger(__name__)

DEFAULT_XMLTODICT_ARGS = frozendict({"attr_prefix": "_"})

type XmlToDictValue = str | dict[str, "XmlToDictValue"] | list["XmlToDictValue"] | None
type XmlToDictResult = DataItemWithMeta | BaseException | None

XMLTODICT_QUEUE_POLL_SECONDS = 0.1

XML2DB_TEMP_PK_PREFIX = "temp_pk_"
XML2DB_TEMP_FK_PREFIX = "temp_fk_"


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
        parsed_element = xmltodict.parse(tostring(element), **DEFAULT_XMLTODICT_ARGS)
        if parsed_element:
            for k, v in parsed_element.items():
                parsed_element[k] = {kv: val for kv, val in v.items() if not kv.startswith("_xmlns")}
            yield from buffer.add_item(parsed_element)
        if (n_entries + 1) % settings.log_interval == 0:
            logger.debug("Processed %d entries", n_entries + 1)

    if n_entries >= 0 and (n_entries + 1) % settings.log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, file_path.name)

    yield from buffer.flush()


def _xmltodict_tag_name(xml_tag: str) -> str:
    """Convert an lxml expanded tag name to xmltodict's namespace-aware form."""
    if xml_tag.startswith("{"):
        return xml_tag.removeprefix("{")
    return xml_tag


def _xml_tag_local_name(xml_tag: str) -> str:
    """Return the local name from either an expanded or unqualified XML tag."""
    return xml_tag.rsplit("}", maxsplit=1)[-1]


def _strip_xmltodict_namespaces(parsed_value: XmlToDictValue) -> XmlToDictValue:
    """Remove namespace URIs from xmltodict's namespace-aware keys."""
    if isinstance(parsed_value, Mapping):
        return {
            key.rsplit("}", maxsplit=1)[-1]: _strip_xmltodict_namespaces(value) for key, value in parsed_value.items()
        }
    if isinstance(parsed_value, list):
        return [_strip_xmltodict_namespaces(item) for item in parsed_value]
    return parsed_value


def _publish_xmltodict_result(result_queue: Queue[XmlToDictResult], cancelled: Event, result: XmlToDictResult) -> bool:
    """Put a parse result on the queue unless downstream iteration has stopped."""
    while not cancelled.is_set():
        try:
            result_queue.put(result, timeout=XMLTODICT_QUEUE_POLL_SECONDS)
        except Full:
            continue
        return True
    return False


def _parse_xmltodict_file(
    settings: XmlToDictSettings,
    file_path: Path,
    result_queue: Queue[XmlToDictResult],
    cancelled: Event,
) -> None:
    """Parse XML entries and publish complete buffered pages in document order."""
    buffer = ListBuffer(table_name=settings.table_name, max_items=settings.buffer_size)
    xmltodict_tag = _xmltodict_tag_name(settings.xml_tag)
    n_entries = 0

    def publish(result: XmlToDictResult) -> bool:
        """Publish a page, error, or completion sentinel from the parser worker."""
        return _publish_xmltodict_result(result_queue, cancelled, result)

    def handle_item(path: Any, item: Any) -> bool:  # noqa: ANN401
        """Buffer a matching root-child element and continue streaming."""
        nonlocal n_entries
        if path[-1][0] != xmltodict_tag:
            return not cancelled.is_set()

        parsed_entry = (
            cast("dict[str, XmlToDictValue]", _strip_xmltodict_namespaces(item))
            if _xml_tag_local_name(settings.xml_tag) == "entry"
            else {}
        )
        for page in buffer.add_item(
            {"entry": {key: value for key, value in parsed_entry.items() if not key.startswith("_xmlns")}}
        ):
            if not publish(page):
                return False

        n_entries += 1
        if n_entries % settings.log_interval == 0:
            logger.debug("Processed %d entries", n_entries)
        return not cancelled.is_set()

    try:
        open_fn = gzip.open if file_path.name.endswith(".gz") else open
        with open_fn(file_path, "rb") as file_handle:
            xmltodict.parse(
                file_handle,
                **DEFAULT_XMLTODICT_ARGS,
                process_namespaces=True,
                namespace_separator="}",
                item_depth=2,
                item_callback=handle_item,
            )

        if n_entries and n_entries % settings.log_interval != 0:
            logger.debug("Processed %d entries from %s", n_entries, file_path.name)
        for page in buffer.flush():
            if not publish(page):
                return
    except BaseException as error:  # noqa: BLE001
        if not cancelled.is_set():
            publish(error)
    finally:
        publish(None)


def process_xml_file_with_xmltodict(settings: XmlToDictSettings, file_path: Path) -> Generator[DataItemWithMeta]:
    """Convert configured XML elements to dictionary form with xmltodict alone.

    Expects the configured elements to be direct children of the document root. It
    parses completed child elements through xmltodict's streaming callback.

    :param settings: pipeline config, including xml_tag, table_name, and buffer_size.
    :type settings: XmlToDictSettings
    :param file_path: path to the XML file, optionally gzip-compressed.
    :type file_path: Path
    :yield: pages of dictionary items.
    :rtype: Generator[DataItemWithMeta]
    """
    logger.info("Reading from %s", str(file_path))
    result_queue: Queue[XmlToDictResult] = Queue(maxsize=1)
    cancelled = Event()
    worker = Thread(
        target=_parse_xmltodict_file,
        args=(settings, file_path, result_queue, cancelled),
        name=f"xmltodict-{file_path.name}",
        daemon=True,
    )
    worker.start()
    try:
        while True:
            try:
                result = result_queue.get(timeout=XMLTODICT_QUEUE_POLL_SECONDS)
            except Empty:
                if not worker.is_alive():
                    break
                continue
            if result is None:
                break
            if isinstance(result, BaseException):
                raise result
            yield result
    finally:
        cancelled.set()
        worker.join()


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


def build_xml2db_model(
    xsd_file: str | Path,
    short_name: str = "xml2db",
    model_config: Mapping[str, Any] | None = None,
) -> DataModel:
    """Build an xml2db `DataModel` from an XSD file.

    Building the model parses and simplifies the whole XSD into a set of tables, relations, and
    field transforms; this is relatively expensive (though still fast: well under a second for a
    schema the size of UniRef's), so it should be done once per pipeline run and the resulting
    `DataModel` reused for every file, rather than rebuilt per file.

    :param xsd_file: path to the XSD file describing the XML documents to be parsed.
    :type xsd_file: str | Path
    :param short_name: short identifier for the schema/data flow. Used to name the virtual root
        table xml2db creates when the schema declares more than one top-level element.
    :type short_name: str
    :param model_config: optional xml2db model configuration overrides, e.g. per-table `reuse`
        settings; see xml2db's `ModelConfig` documentation.
    :type model_config: Mapping[str, Any] | None
    :return: an xml2db `DataModel` built from the schema (not connected to any database).
    :rtype: DataModel
    """
    return DataModel(
        xsd_file=str(xsd_file),
        short_name=short_name,
        model_config=dict(model_config) if model_config else {},
    )


def _xml2db_scalar(value: Any) -> Any:  # noqa: ANN401
    """Make an xml2db field value JSON/parquet-serialisable.

    xml2db's per-record hash column (`xml2db_record_hash`, by default) holds raw `bytes`, which
    most dlt loader file formats cannot serialise directly; every other field value is left as-is.

    :param value: a raw value from an xml2db flat-data record.
    :type value: Any
    :return: a JSON/parquet-serialisable equivalent of `value`.
    :rtype: Any
    """
    if isinstance(value, bytes):
        return value.hex()
    return value


def _xml2db_local_key(file_key: str, value: Any) -> Any:  # noqa: ANN401
    """Namespace an xml2db temporary primary/foreign key value so it is unique across files.

    xml2db numbers primary keys per table starting at 1 for each `Document` instance it parses.
    Since `process_xml_file_with_xml2db` parses each file into its own `Document` (so that files
    can be streamed and processed independently, e.g. across dlt's parallel workers), two
    different files would otherwise produce colliding keys once their rows are combined
    downstream. Note that this also means deduplication of "reused" records only happens within a
    single file's scope, not across the whole pipeline run.

    :param file_key: a value unique to the source file (e.g. its file name).
    :type file_key: str
    :param value: the raw (file-local) integer key value, or None.
    :type value: Any
    :return: a globally-unique string key, or None if `value` is None.
    :rtype: Any
    """
    if value is None:
        return None
    return f"{file_key}:{value}"


def _rename_xml2db_record(record: Mapping[str, Any], file_key: str) -> dict[str, Any]:
    """Rewrite one xml2db flat-data record's temp pk/fk columns into namespaced string keys.

    :param record: a single record from an xml2db `Document.data[...]["records"]` list.
    :type record: Mapping[str, Any]
    :param file_key: a value unique to the source file, used to namespace primary/foreign keys.
    :type file_key: str
    :return: the record with `temp_pk_*`/`temp_fk_*` columns renamed (dropping the `temp_` prefix)
        and their values namespaced, and all other values made serialisable.
    :rtype: dict[str, Any]
    """
    row: dict[str, Any] = {}
    for column, value in record.items():
        if column.startswith((XML2DB_TEMP_PK_PREFIX, XML2DB_TEMP_FK_PREFIX)):
            row[column.removeprefix("temp_")] = _xml2db_local_key(file_key, value)
        else:
            row[column] = _xml2db_scalar(value)
    return row


def flatten_xml2db_document(model: DataModel, document: Document, file_key: str) -> dict[str, list[dict[str, Any]]]:
    """Flatten a parsed xml2db `Document` into a `table_name -> rows` mapping.

    xml2db represents a parsed document as one set of records per XSD-derived table (keyed by
    XSD type, not by the table name it would use in a database), plus, for every list-valued
    relation to a reused/deduplicated child table, a many-to-many junction table (nested under
    `relations_n`). This collects both into a single flat dict keyed by table name, ready to be
    handed to a `DictBuffer`.

    :param model: the `DataModel` used to parse `document`.
    :type model: DataModel
    :param document: a parsed xml2db `Document`.
    :type document: Document
    :param file_key: a value unique to the source file, used to namespace primary/foreign keys.
    :type file_key: str
    :return: mapping of table name to list of row dicts.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    tables: dict[str, list[dict[str, Any]]] = {}
    for type_name, table_data in document.data.items():
        table_name = model.tables[type_name].name
        rows = tables.setdefault(table_name, [])
        rows.extend(_rename_xml2db_record(record, file_key) for record in table_data.get("records", []))

        for rel_table_name, rel_data in table_data.get("relations_n", {}).items():
            rel_rows = tables.setdefault(rel_table_name, [])
            rel_rows.extend(_rename_xml2db_record(record, file_key) for record in rel_data.get("records", []))

    return tables


def process_xml_file_with_xml2db(
    settings: Xml2DbSettings,
    model: DataModel,
    file_path: Path,
) -> Generator[DataItemWithMeta, Any]:
    """Parse a whole XML file with xml2db and yield pages of rows, one page per output table.

    Unlike `process_xml_file_to_dict`/`process_xml_file_with_xmltodict`, which stream and buffer
    one XML tag/element at a time, xml2db parses and flattens an entire XML document (against the
    schema described by `model`) in a single pass, producing rows for every XSD-derived table
    (plus many-to-many junction tables for repeated child elements), not just one row per matched
    tag. Buffering therefore happens per *file* rather than per element: `settings.buffer_size`
    still caps how many rows accumulate per table before a page is yielded, but because a whole
    file's worth of rows for a table are added in a single step, a single large file can still
    produce a page bigger than `buffer_size` for its most common tables.

    :param settings: pipeline config, including buffer_size and skip_xml_validation.
    :type settings: Xml2DbSettings
    :param model: the xml2db DataModel built from the target XSD.
    :type model: DataModel
    :param file_path: path to the XML file to parse; the file can be gzipped or not.
    :type file_path: Path
    :yield: pages of table-tagged rows.
    :rtype: Generator[DataItemWithMeta, Any]
    """
    logger.info("Reading from %s", str(file_path))
    file_path_str = str(file_path)

    document = Document(model)
    if file_path_str.endswith(".gz"):
        with gzip.open(file_path, "rb") as file_handle:
            xml_source: str | BytesIO = BytesIO(file_handle.read())
    else:
        xml_source = file_path_str
    document.parse_xml(xml_source, skip_validation=settings.skip_xml_validation, iterparse=True)

    tables = flatten_xml2db_document(model, document, file_key=file_path.name)
    for table_name, rows in tables.items():
        yield dlt.mark.with_table_name(rows, table_name)

    n_rows = sum(len(rows) for rows in tables.values())
    logger.debug("Processed %d rows across %d tables from %s", n_rows, len(tables), file_path.name)
