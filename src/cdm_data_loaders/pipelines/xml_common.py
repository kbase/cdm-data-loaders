"""Shared XML CLI shortcuts, globs, base settings, and streaming batch processors for XML pipelines."""

import functools
from collections.abc import Callable, Generator, Iterable
from logging import Logger, getLogger
from typing import Annotated, Any, Final

from dlt.extract.items import DataItemWithMeta
from frozendict import frozendict
from lxml.etree import Element
from pydantic import Field, FilePath
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.buffer import DictBuffer, make_dict_buffer
from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    FileGlob,
    NonEmptyStr,
    TableName,
    file_glob_description,
)
from cdm_data_loaders.core.file_items import FileSource, source_id, source_url
from cdm_data_loaders.core.registry import TableTransformFn, TransformContext
from cdm_data_loaders.core.settings import (
    FILE_FORMAT_CLI_SHORTCUTS,
    FileFormatIngestSettings,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.core.transforms.routing import route_and_coerce_record
from cdm_data_loaders.core.transforms.settings_mixin import TransformSettingsMixin
from cdm_data_loaders.readers.xml import clear_xml_element, stream_elements

logger: Logger = getLogger(__name__)

DEFAULT_XML_FILE_GLOB: Final[str] = "*.xml*"
XML_GZ_FILE_GLOB: Final[str] = "*.xml.gz"

XML_CLI_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    {
        **FILE_FORMAT_CLI_SHORTCUTS,
        FILE_GLOB: "g",
    }
)

XmlTag = Annotated[
    NonEmptyStr,
    Field(
        description="XML tag to capture the contents of",
    ),
]
XsdFile = Annotated[
    FilePath,
    Field(
        description="Path to the XSD file describing the XML documents to be imported.",
    ),
]


class XmlIngestSettings(FileFormatIngestSettings):
    """Configuration for ``dlt`` pipelines that import XML files."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(cli_shortcuts=XML_CLI_SHORTCUTS)

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_XML_FILE_GLOB,
            description=file_glob_description("Glob pattern for XML files"),
        ),
    ]


class SingleTableXmlIngestSettings(XmlIngestSettings, TransformSettingsMixin):
    """Configuration for ``dlt`` pipelines that import XML elements into a single target table."""

    table_name: TableName
    xml_tag: XmlTag


def _log_interval_progress(n_entries: int, log_interval: int) -> None:
    """Log the processed-entry count every ``log_interval`` elements.

    :param n_entries: Zero-based index of the processed entry.
    :type n_entries: int
    :param log_interval: Entry interval at which progress is logged.
    :type log_interval: int
    """
    if (n_entries + 1) % log_interval == 0:
        logger.debug("Processed %d entries", n_entries + 1)


def _log_interval_final(n_entries: int, file_path: FileSource, log_interval: int) -> None:
    """Log the trailing entry count if the final checkpoint was not an interval boundary.

    :param n_entries: Zero-based index of the last processed entry (``-1`` if empty).
    :type n_entries: int
    :param file_path: Input XML file source.
    :type file_path: FileSource
    :param log_interval: Entry interval at which progress is logged.
    :type log_interval: int
    """
    if n_entries >= 0 and (n_entries + 1) % log_interval != 0:
        logger.debug("Processed %d entries from %s", n_entries + 1, source_id(file_path))


def stream_and_buffer_element_iter(
    file_path: FileSource,
    elements: Iterable[Element],
    log_interval: int,
    buffer: DictBuffer,
    buffer_element_fn: Callable[[Element], Iterable[DataItemWithMeta]],
) -> Generator[DataItemWithMeta]:
    """Iterate over streamed XML ``elements``, buffer parsed items, log progress, and flush.

    :param file_path: Local path or ``dlt`` file item.
    :type file_path: FileSource
    :param elements: Iterable of streamed XML elements.
    :type elements: Iterable[Element]
    :param log_interval: Entry interval at which progress is logged.
    :type log_interval: int
    :param buffer: Dictionary or PyArrow batch buffer.
    :type buffer: DictBuffer
    :param buffer_element_fn: Callback converting an element and adding records to ``buffer``.
    :type buffer_element_fn: Callable[[Element], Iterable[DataItemWithMeta]]
    :yield: Flushed pages of items or PyArrow tables.
    :rtype: Generator[DataItemWithMeta]
    """
    logger.info("Reading from %s", source_url(file_path))
    n_entries = -1
    for n_entries, element in enumerate(elements):
        flushed_items = list(buffer_element_fn(element))
        clear_xml_element(element)
        yield from flushed_items
        _log_interval_progress(n_entries, log_interval)

    _log_interval_final(n_entries, file_path, log_interval)
    yield from buffer.flush()


def buffer_single_tag_element(
    element: Element,
    *,
    convert_element: Callable[[Element], dict[str, Any] | None],
    transform: TableTransformFn | None,
    buffer: DictBuffer,
    ctx: TransformContext,
) -> Iterable[DataItemWithMeta]:
    """Convert a single-tag XML element and add the resulting rows to ``buffer``.

    :param element: Streamed XML element matching ``xml_tag``.
    :type element: Element
    :param convert_element: Element-to-dict converter (returns ``None`` to skip).
    :type convert_element: Callable[[Element], dict[str, Any] | None]
    :param transform: Optional table transform.
    :type transform: TableTransformFn | None
    :param buffer: PyArrow or dictionary batch buffer.
    :type buffer: DictBuffer
    :param ctx: Transform context carrying target table name and source file identifier.
    :type ctx: TransformContext
    :return: Flushed table batches, if any.
    :rtype: Iterable[DataItemWithMeta]
    """
    parsed_element = convert_element(element)
    if parsed_element is None:
        return ()
    return buffer.add_items(
        route_and_coerce_record(
            parsed_element,
            ctx.table_name,
            transform=transform,
            schema_map=buffer.schema_map,
            source_file=ctx.source_file,
        )
    )


def process_single_tag_xml_file(
    file_path: FileSource,
    settings: Any,  # noqa: ANN401
    convert_element: Callable[[Element], dict[str, Any] | None],
    *,
    resolve_default_ns: bool = False,
) -> Generator[DataItemWithMeta]:
    """Stream single-tag XML elements, convert each with ``convert_element``, and yield buffered pages.

    :param file_path: Local path or ``dlt`` file item.
    :type file_path: FileSource
    :param settings: XML pipeline settings carrying ``table_name``, ``xml_tag``, ``log_interval``, and buffer config.
    :type settings: Any
    :param convert_element: Function converting an XML element into a record dict (or ``None`` to skip).
    :type convert_element: Callable[[Element], dict[str, Any] | None]
    :param resolve_default_ns: Whether to resolve an unprefixed tag against the file's default namespace.
    :type resolve_default_ns: bool
    :yield: Buffered pages of rows or PyArrow tables.
    :rtype: Generator[DataItemWithMeta]
    """
    if settings.table_name is None or settings.xml_tag is None:
        err_msg = "Expected table_name and xml_tag when xml_level is not set"
        raise RuntimeError(err_msg)

    transform = settings.get_table_transform(settings.table_name)
    buffer = make_dict_buffer(settings, DictBuffer)
    ctx = TransformContext.for_source(settings.table_name, source_id(file_path))
    yield from stream_and_buffer_element_iter(
        file_path=file_path,
        elements=stream_elements(file_path, settings.xml_tag, resolve_default_ns=resolve_default_ns),
        log_interval=settings.log_interval,
        buffer=buffer,
        buffer_element_fn=functools.partial(
            buffer_single_tag_element,
            convert_element=convert_element,
            transform=transform,
            buffer=buffer,
            ctx=ctx,
        ),
    )
