"""Per-file XML-to-PyArrow streaming processors for single-tag and level-based modes."""

import functools
from collections.abc import Generator, Iterable

from dlt.extract.items import DataItemWithMeta
from lxml.etree import Element

from cdm_data_loaders.core.buffer import DictBuffer, make_dict_buffer
from cdm_data_loaders.core.file_items import FileSource, source_id
from cdm_data_loaders.core.transforms.routing import route_and_coerce_record
from cdm_data_loaders.pipelines.xml_common import (
    process_single_tag_xml_file,
    stream_and_buffer_element_iter,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from cdm_data_loaders.readers.xml import stream_xml_file_at_level
from cdm_data_loaders.utils.xml_utils import local_name


def _buffer_level_pyarrow_element(
    element: Element,
    *,
    level: int,
    settings: XmlToPyArrowSettings,
    buffer: DictBuffer,
    source_file: str | None = None,
) -> Iterable[DataItemWithMeta]:
    """Convert a level-streamed XML element by its tag name and add the resulting rows to ``buffer``.

    :param element: Streamed XML element at ``level``.
    :type element: Element
    :param level: XML depth level relative to the root element.
    :type level: int
    :param settings: Pipeline configuration.
    :type settings: XmlToPyArrowSettings
    :param buffer: Multi-table PyArrow batch buffer.
    :type buffer: DictBuffer
    :param source_file: Relative path or identifier of the XML file being parsed, if any.
    :type source_file: str | None
    :return: Flushed PyArrow table batches, if any.
    :rtype: Iterable[DataItemWithMeta]
    """
    tag_name = local_name(element.tag)
    converter = settings.xml_level_converters.get(tag_name)
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
            source_file=source_file,
        )
    )


def _process_xml_file_to_pyarrow_at_level(
    file_path: FileSource,
    settings: XmlToPyArrowSettings,
    level: int,
) -> Generator[DataItemWithMeta]:
    """Stream XML elements at ``level``, convert each by its tag name, and yield PyArrow batches.

    :param file_path: Local path or ``dlt`` file item.
    :type file_path: FileSource
    :param settings: Pipeline configuration.
    :type settings: XmlToPyArrowSettings
    :param level: XML depth level relative to the root element.
    :type level: int
    :yield: Pages of PyArrow tables.
    :rtype: Generator[DataItemWithMeta]
    """
    buffer = make_dict_buffer(settings, DictBuffer)

    yield from stream_and_buffer_element_iter(
        file_path=file_path,
        elements=stream_xml_file_at_level(file_path, level),
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=functools.partial(
            _buffer_level_pyarrow_element,
            level=level,
            settings=settings,
            buffer=buffer,
            source_file=source_id(file_path),
        ),
    )


def process_xml_file_to_pyarrow(file_path: FileSource, settings: XmlToPyArrowSettings) -> Generator[DataItemWithMeta]:
    """Convert XML elements to PyArrow table batches using an XSD schema.

    When ``settings.xml_level`` is set, all elements at that depth are streamed, converted using
    ``settings.xsd_schema``, and routed to tables named after each element's local tag name (with PyArrow schemas from
    ``settings.schema_map``). Otherwise, elements matching ``settings.xml_tag`` are streamed and buffered into
    ``settings.table_name``.

    :param file_path: Local path or ``dlt`` file item.
    :type file_path: FileSource
    :param settings: Pipeline configuration including ``xml_tag``/``table_name`` or ``xml_level``, ``xsd_schema``, and ``schema_map``.
    :type settings: XmlToPyArrowSettings
    :yield: Pages of PyArrow tables or row dictionaries.
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

    yield from process_single_tag_xml_file(
        file_path,
        settings,
        convert_element,
        resolve_default_ns=True,
    )
