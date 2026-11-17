"""Per-file XML-to-dict streaming processor for the xmltodict pipeline."""

from collections.abc import Generator

from dlt.extract.items import DataItemWithMeta

from cdm_data_loaders.core.file_items import FileSource
from cdm_data_loaders.pipelines.xml_common import process_single_tag_xml_file
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.readers.xml import parse_element_to_dict


def process_xml_file_to_dict(file_path: FileSource, settings: XmlToDictSettings) -> Generator[DataItemWithMeta]:
    """Convert XML elements to dictionary form using ``xmltodict.parse``.

    :param file_path: Local path or ``dlt`` file item.
    :type file_path: FileSource
    :param settings: Pipeline configuration including ``xml_tag``, ``table_name``, and ``buffer_size``.
    :type settings: XmlToDictSettings
    :yield: Pages of dictionary items.
    :rtype: Generator[DataItemWithMeta]
    """
    yield from process_single_tag_xml_file(
        file_path,
        settings,
        parse_element_to_dict,
    )
