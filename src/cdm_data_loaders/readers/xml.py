"""Common reusable XML pipeline elements."""

import gzip
from collections.abc import Callable, Generator, Mapping
from io import BytesIO
from logging import Logger, getLogger
from pathlib import Path
from typing import Any

import xmltodict
from dlt.extract.items import DataItemWithMeta
from frozendict import frozendict
from lxml.etree import Element, XMLSyntaxError, iterparse, tostring
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


def build_xml2db_model(
    xsd_file: str | Path,
    short_name: str = "xml2db",
    model_config: Mapping[str, Any] | None = None,
) -> DataModel:
    """Build an xml2db `DataModel` from an XSD file.

    This parses and simplifies the whole XSD into tables, relations, and field transforms. It
    takes well under a second for a schema the size of UniRef's, but build the model once per
    pipeline run. Reuse the same `DataModel` for every file; do not rebuild it per file.

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

    Converts xml2db's hash column (`xml2db_record_hash`, by default) from bytes
    into a format that dlt can serialise.

    :param value: a raw value from an xml2db flat-data record.
    :type value: Any
    :return: a JSON/parquet-serialisable equivalent of `value`.
    :rtype: Any
    """
    if isinstance(value, bytes):
        return value.hex()
    return value


def _xml2db_local_key(file_key: str, value: Any) -> Any:  # noqa: ANN401
    """Build a namespaced key for a table row that is not content-addressed.

    xml2db numbers each table's primary keys from 1, starting fresh for every `Document`
    instance it parses. `process_xml_file_with_xml2db` parses each file, and each chunk within a
    file, into its own `Document`. Their raw keys would collide once combined, unless namespaced.

    Two kinds of table use this scheme instead of `_content_addressed_key`:

    - The schema's root table. See `is_xml2db_content_addressed_table` for why its content hash
      is not stable across chunks.
    - Any table configured with `reuse: false` in `xml2db_config_file`. xml2db never
      deduplicates such a table, so a namespaced counter is enough.

    :param file_key: a value unique to the source file (and, in chunked mode, the chunk within
        it), e.g. `"<file name>"` or `"<file name>:<chunk index>"`.
    :type file_key: str
    :param value: the raw (Document-local) integer key value, or None.
    :type value: Any
    :return: a namespaced string key, or None if `value` is None.
    :rtype: Any
    """
    if value is None:
        return None
    return f"{file_key}:{value}"


def _content_addressed_key(table_name: str, record_hash: bytes) -> str:
    """Build a content-addressed key for a "reused" (deduplicated) table's row.

    xml2db deduplicates "reused" rows by content hash, but only within one `Document`. Two
    `Document` instances (two files, or two chunks of one file) can each parse a row with
    identical content and give it a different temporary primary key.

    Deriving the key from the row's own content hash fixes this:

    - Identical content always gets the same key, in any `Document`.
    - Every foreign key referencing the row is correct as soon as it is written. Nothing needs
      rewriting later.
    - Any literal duplicate rows that reach the destination are byte-for-byte identical. It is
      safe to keep any one of them and drop the rest.

    See `cdm_data_loaders.pipelines.xml2db.compaction` for the step that drops those duplicates.

    :param table_name: the destination table name (`DataModelTable.name`). Included so a row in
        one table can never collide with a row in another table that hashes the same.
    :type table_name: str
    :param record_hash: the raw `xml2db_record_hash` bytes xml2db computed for this record.
    :type record_hash: bytes
    :return: a content-addressed string key, stable across every `Document` that parses the same
        content.
    :rtype: str
    """
    return f"{table_name}:{record_hash.hex()}"


def is_xml2db_content_addressed_table(model: DataModel, table: Any) -> bool:  # noqa: ANN401
    """Whether a table's rows should get a content-addressed key.

    True for every "reused" table except the schema's root table.

    xml2db's `xml2db_record_hash` for a table with a many-to-many relation includes the hash of
    every child attached to it (see xml2db's `XMLConverter._compute_hash_deduplicate`). That
    breaks content-addressing for the root table only:

    - A chunk's synthetic root (see `iter_xml2db_chunk_fragments`) has only that chunk's entries
      attached, never the whole file's.
    - So its hash differs from every other chunk's root, even when the root's own columns
      (`releaseDate`, `version`) are identical.
    - Content-addressing the root would not let two `Document`s' root rows collapse. It would
      just relabel an already Document-scoped key.

    Every table nested inside the chunked element is unaffected: its own subtree, and so its
    hash, is fully decided within whichever single chunk contains it. Content-addressing is safe
    and effective for all of them.

    :param model: the `DataModel` `table` belongs to.
    :type model: DataModel
    :param table: the table to check.
    :type table: Any
    :return: True if `table`'s rows should use a content-addressed key.
    :rtype: bool
    """
    return table.is_reused and table.type_name != model.root_table


def _build_xml2db_key_map(model: DataModel, document: Document, file_key: str) -> dict[str, dict[Any, Any]]:
    """Map every record's raw (Document-local) temp primary key to its downstream key.

    Built once per `Document`, before any record is rewritten. A foreign key column can then
    look up the referenced row's downstream key by its raw integer value, regardless of the
    order tables appear in `document.data`.

    The downstream key is content-addressed if the referenced table qualifies (see
    `is_xml2db_content_addressed_table`), or namespaced by file/chunk otherwise (see
    `_xml2db_local_key`).

    :param model: the `DataModel` used to parse `document`.
    :type model: DataModel
    :param document: a parsed xml2db `Document`.
    :type document: Document
    :param file_key: a value unique to the source file (and chunk, in chunked mode).
    :type file_key: str
    :return: mapping of XSD type name to a `{raw temp pk: downstream key}` mapping for that type.
    :rtype: dict[str, dict[Any, Any]]
    """
    hash_column = model.model_config["record_hash_column_name"]
    key_maps: dict[str, dict[Any, Any]] = {}
    for type_name, table_data in document.data.items():
        table = model.tables[type_name]
        pk_column = f"temp_pk_{table.name}"
        key_map: dict[Any, Any] = {}
        for record in table_data.get("records", []):
            raw_pk = record[pk_column]
            key_map[raw_pk] = (
                _content_addressed_key(table.name, record[hash_column])
                if is_xml2db_content_addressed_table(model, table)
                else _xml2db_local_key(file_key, raw_pk)
            )
        key_maps[type_name] = key_map
    return key_maps


def _rewrite_xml2db_record(
    record: Mapping[str, Any],
    table: Any,  # noqa: ANN401  (xml2db.table.DataModelTable / a reuse/duplicate subclass thereof)
    key_maps: Mapping[str, Mapping[Any, Any]],
) -> dict[str, Any]:
    """Rewrite one xml2db flat-data record's temp pk/fk columns into their downstream keys.

    :param record: a single record from an xml2db `Document.data[type_name]["records"]` list.
    :type record: Mapping[str, Any]
    :param table: the `DataModelTable` (or `is_reused`/duplicated subclass) `record` belongs to.
    :type table: Any
    :param key_maps: the `{type_name: {raw temp pk: downstream key}}` mapping built by
        `_build_xml2db_key_map` for the same `Document` `record` came from.
    :type key_maps: Mapping[str, Mapping[Any, Any]]
    :return: the record with `temp_pk_*`/`temp_fk_*` columns rewritten to `pk_*`/`fk_*` columns
        holding downstream keys, and all other values made serialisable.
    :rtype: dict[str, Any]
    """
    pk_column = f"temp_pk_{table.name}"
    fk_targets = {
        f"temp_{field.field_name}": field.other_table.type_name
        for field_type, _rel_name, field in table.fields
        if field_type == "rel1"
    }

    row: dict[str, Any] = {}
    for column, value in record.items():
        if column == pk_column:
            row[f"pk_{table.name}"] = key_maps[table.type_name][value]
        elif column in fk_targets:
            target_type = fk_targets[column]
            row[column.removeprefix("temp_")] = None if value is None else key_maps[target_type][value]
        else:
            row[column] = _xml2db_scalar(value)
    return row


def _rewrite_xml2db_junction_record(
    record: Mapping[str, Any],
    table: Any,  # noqa: ANN401  (the "parent" side DataModelTable that owns relation `rel`)
    rel: Any,  # noqa: ANN401  (xml2db.table.DataModelRelationN)
    key_maps: Mapping[str, Mapping[Any, Any]],
) -> dict[str, Any]:
    """Rewrite one many-to-many junction row's two fk columns into their downstream keys.

    :param record: a record from `document.data[type_name]["relations_n"][rel.rel_table_name]`.
    :type record: Mapping[str, Any]
    :param table: the "parent" side `DataModelTable` that owns relation `rel`.
    :type table: Any
    :param rel: the `DataModelRelationN` object describing the many-to-many relation.
    :type rel: Any
    :param key_maps: the `{type_name: {raw temp pk: downstream key}}` mapping built by
        `_build_xml2db_key_map` for the same `Document` `record` came from.
    :type key_maps: Mapping[str, Mapping[Any, Any]]
    :return: the record with its two `temp_fk_*` columns rewritten to `fk_*` columns holding
        downstream keys.
    :rtype: dict[str, Any]
    """
    parent_column = f"temp_fk_{table.name}"
    other_column = f"temp_fk_{rel.other_table.name}"

    row: dict[str, Any] = {}
    for column, value in record.items():
        if column == parent_column:
            row[f"fk_{table.name}"] = key_maps[table.type_name][value]
        elif column == other_column:
            row[f"fk_{rel.other_table.name}"] = key_maps[rel.other_table.type_name][value]
        else:
            row[column] = _xml2db_scalar(value)
    return row


def _local_name(tag: str) -> str:
    """Return the local (namespace-stripped) part of an lxml expanded tag name.

    xml2db matches elements against its data model by local name only (see
    `XMLConverter._parse_iterative`). So callers of `iter_xml2db_chunk_fragments` may supply
    either a bare local name or a full Clark-notation expanded name.

    :param tag: an XML tag, optionally in `{namespace}local` Clark notation.
    :type tag: str
    :return: the local part of `tag`, with any namespace stripped.
    :rtype: str
    """
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) and tag.startswith("{") else tag


def iter_xml2db_chunk_fragments(
    file_path: str | Path,
    child_tag: str,
    chunk_size: int,
) -> Generator[bytes, Any]:
    """Stream an XML file and yield small, self-contained XML fragments, one per chunk.

    xml2db's `Document.parse_xml` loads a whole document tree into memory in one call. It has no
    way to flush partial results. Parsing one huge file directly risks running out of memory.

    This generator lets `process_xml_file_with_xml2db` parse a file in bounded-size pieces
    instead:

    - It streams the source with `lxml.etree.iterparse`.
    - For every `chunk_size` completed `child_tag` elements, it serializes a copy of the file's
      own root element, containing just that batch of children.

    Each fragment is a complete, well-formed XML document. xml2db can parse it exactly as if it
    were a smaller whole file: xml2db's data model keys elements by local name only, not by depth
    or namespace, so a `<root ...>{children}</root>` fragment parses the same as the equivalent
    slice of the original file. This includes the root-level table's own columns, copied from the
    fragment's root attributes.

    This assumes the shape described in the pipeline settings: one root element, directly
    followed by many repeated elements of a single type (`child_tag`). Any other top-level
    siblings stay attached to the source tree; they are not moved into a fragment, so this
    function does not bound their memory use. This is fine for schemas like UniRef, where `entry`
    is the only top-level child. A schema with several large, independently repeated top-level
    element types would need this function extended.

    Elements are moved, not copied, out of the source tree into each fragment. `Element.append`
    re-parents an element rather than duplicating it, so the source tree never holds more than a
    handful of already-processed elements at once. Overall memory use is governed by
    `chunk_size`, not by the source file's total size.

    :param file_path: path to the XML file to stream; the file can be gzipped or not.
    :type file_path: str | Path
    :param child_tag: the repeated child element to chunk by. May be a bare local name (e.g.
        `"entry"`) or an lxml Clark-notation expanded name (e.g.
        `"{http://uniprot.org/uniref}entry"`); only the local name is used for matching.
    :type child_tag: str
    :param chunk_size: maximum number of `child_tag` elements per yielded fragment. The final
        fragment may contain fewer if the total is not a multiple of `chunk_size`.
    :type chunk_size: int
    :yield: serialized XML fragments, each a copy of the source root containing up to
        `chunk_size` of its `child_tag` children.
    :rtype: Generator[bytes, Any]
    """
    if isinstance(file_path, Path):
        file_path = str(file_path)
    child_local_name = _local_name(child_tag)
    open_fn = gzip.open if file_path.endswith(".gz") else open

    with open_fn(file_path, "rb") as fh:
        context = iterparse(fh, events=("start", "end"), remove_blank_text=True)
        try:
            _, root_elem = next(context)  # the very first event is always the root's own "start"
        except (StopIteration, XMLSyntaxError):
            # empty or root-less input: nothing to chunk.
            return

        chunk_root = Element(root_elem.tag, attrib=root_elem.attrib, nsmap=root_elem.nsmap)
        for event, elem in context:
            if event != "end" or _local_name(elem.tag) != child_local_name:
                continue
            chunk_root.append(elem)
            if len(chunk_root) >= chunk_size:
                yield tostring(chunk_root)
                chunk_root = Element(root_elem.tag, attrib=root_elem.attrib, nsmap=root_elem.nsmap)

        if len(chunk_root):
            yield tostring(chunk_root)


def flatten_xml2db_document(model: DataModel, document: Document, file_key: str) -> dict[str, list[dict[str, Any]]]:
    """Flatten a parsed xml2db `Document` into a `table_name -> rows` mapping.

    xml2db represents a parsed document as one set of records per XSD-derived table, keyed by
    XSD type rather than by the table name it would use in a database. It also produces a
    many-to-many junction table for every list-valued relation to a reused child table, nested
    under `relations_n`. This function collects both into a single flat dict keyed by table
    name, ready to be handed to a `DictBuffer`.

    It also rewrites every primary/foreign key along the way (see `_build_xml2db_key_map`):

    - "Reused" tables get a content-addressed key, derived from their own `xml2db_record_hash`.
      Identical content parsed by different `Document` instances (different files, or different
      chunks of one file) gets the same key. Every foreign key into it is correct immediately.
      Any literal duplicate rows that result are safe to drop later without touching any other
      table (see `cdm_data_loaders.pipelines.xml2db.compaction`).
    - Other tables keep a simple key namespaced by file/chunk.

    :param model: the `DataModel` used to parse `document`.
    :type model: DataModel
    :param document: a parsed xml2db `Document`.
    :type document: Document
    :param file_key: a value unique to the source file (and chunk, in chunked mode), used to
        namespace the primary/foreign keys of non-reused tables.
    :type file_key: str
    :return: mapping of table name to list of row dicts.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    key_maps = _build_xml2db_key_map(model, document, file_key)

    tables: dict[str, list[dict[str, Any]]] = {}
    for type_name, table_data in document.data.items():
        table = model.tables[type_name]
        rows = tables.setdefault(table.name, [])
        rows.extend(_rewrite_xml2db_record(record, table, key_maps) for record in table_data.get("records", []))

        for rel in table.relations_n.values():
            if not rel.other_table.is_reused:
                continue
            rel_data = table_data.get("relations_n", {}).get(rel.rel_table_name)
            if not rel_data:
                continue
            rel_rows = tables.setdefault(rel.rel_table_name, [])
            rel_rows.extend(
                _rewrite_xml2db_junction_record(record, table, rel, key_maps) for record in rel_data.get("records", [])
            )

    return tables


def process_xml_file_with_xml2db(
    settings: Xml2DbSettings,
    model: DataModel,
    file_path: Path,
) -> Generator[DataItemWithMeta, Any]:
    """Parse a whole XML file with xml2db and yield pages of rows, one page per output table.

    `process_xml_file_to_dict`/`process_xml_file_with_xmltodict` stream and buffer one XML
    tag/element at a time. xml2db works differently: it parses and flattens the whole document
    (using the schema in `model`) in a single pass, producing rows for every XSD-derived table,
    plus many-to-many junction tables for repeated child elements. So buffering happens per
    *file*, not per element. `settings.buffer_size` still caps how many rows accumulate per
    table before a page is yielded, but a whole file's rows for a table are added in one step. A
    single large file can still produce a page bigger than `buffer_size` for its most common
    tables.

    :param settings: pipeline config, including buffer_size and skip_xml_validation.
    :type settings: Xml2DbSettings
    :param model: the xml2db DataModel built from the target XSD.
    :type model: DataModel
    :param file_path: path to the XML file to parse; the file can be gzipped or not.
    :type file_path: Path
    :yield: pages of table-tagged rows.
    :rtype: Generator[DataItemWithMeta, Any]

    .. note::
        When `settings.chunk_element_tag` is set, the file is parsed in bounded-size pieces via
        `iter_xml2db_chunk_fragments` instead. Peak memory use is then governed by
        `settings.chunk_size`, not the file's total size (see that function for the constraints
        this places on the schema). Rows are still buffered and yielded exactly as in the
        whole-file case, just with a fresh, discarded `Document` per chunk instead of one per
        file.

        The trade-off: xml2db deduplicates "reused" tables (e.g. `property`) by content hash,
        but only within one `Document`. Chunked mode only deduplicates within each chunk, not
        across the whole file. `flatten_xml2db_document` works around most of this by deriving
        each "reused" row's key from its own content hash (see `_content_addressed_key`):
        identical content parsed by different chunks always gets the same key, so every foreign
        key into it is correct immediately. A chunked run may still contain more (never fewer)
        literal rows than an unchunked run, until something collapses same-keyed duplicates
        (e.g. `cdm_data_loaders.pipelines.xml2db.compaction.compact_reused_tables`, run
        automatically by `run_xml2db_ingest_pipeline` when `Xml2DbSettings.compact_reused_tables`
        is set). Many-to-many junction tables have no primary key of their own, so chunking never
        duplicates them either way.
    """
    logger.info("Reading from %s", str(file_path))
    buffer = DictBuffer(max_items=settings.buffer_size)

    if settings.chunk_element_tag:
        n_rows = 0
        n_chunks = 0
        for n_chunks, chunk_bytes in enumerate(
            iter_xml2db_chunk_fragments(file_path, settings.chunk_element_tag, settings.chunk_size), start=1
        ):
            document = Document(model)
            document.parse_xml(BytesIO(chunk_bytes), skip_validation=settings.skip_xml_validation, iterparse=True)
            tables = flatten_xml2db_document(model, document, file_key=f"{file_path.name}:{n_chunks}")
            n_rows += sum(len(rows) for rows in tables.values())
            yield from buffer.add_items(tables)

        yield from buffer.flush()
        logger.debug("Processed %d rows across %d chunks from %s", n_rows, n_chunks, file_path.name)
        return

    file_path_str = str(file_path)
    document = Document(model)
    if file_path_str.endswith(".gz"):
        with gzip.open(file_path, "rb") as file_handle:
            xml_source: str | BytesIO = BytesIO(file_handle.read())
    else:
        xml_source = file_path_str
    document.parse_xml(xml_source, skip_validation=settings.skip_xml_validation, iterparse=True)

    tables = flatten_xml2db_document(model, document, file_key=file_path.name)
    yield from buffer.add_items(tables)
    yield from buffer.flush()

    n_rows = sum(len(rows) for rows in tables.values())
    logger.debug("Processed %d rows across %d tables from %s", n_rows, len(tables), file_path.name)
