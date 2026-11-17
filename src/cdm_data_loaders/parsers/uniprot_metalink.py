"""Parser for UniProt metalink XML files.

These metadata files provide file sizes, checksums, and mirror download URLs for UniProt releases.
"""

from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final
from xml.etree.ElementTree import Element

from defusedxml.ElementTree import parse

NS: Final[dict[str, str]] = {"": "http://www.metalinker.org/"}

logger: Logger = getLogger(__name__)


def parse_metalink(metalink_xml_path: Path | str) -> Element:
    """Parse a metalink XML file and return its root element.

    :param metalink_xml_path: path to the metalink XML file
    :type metalink_xml_path: Path | str
    :return: parsed XML root element
    :rtype: Element
    :raises RuntimeError: if no root element is found in the document
    """
    document = parse(str(metalink_xml_path))
    root = document.getroot()
    if root is not None:
        return root
    msg = f"Could not find root in metalink file: {metalink_xml_path!s}"
    logger.error(msg)
    raise RuntimeError(msg)


def generate_data_source_table(metalink_xml_path: Path | str) -> dict[str, Any]:
    """Extract publisher, license, and version metadata from a metalink XML file.

    :param metalink_xml_path: path to the metalink XML file
    :type metalink_xml_path: Path | str
    :return: dictionary with ``license``, ``publisher``, ``resource_type``, and ``version`` keys
    :rtype: dict[str, Any]
    :raises RuntimeError: if any required element is missing from the metalink document
    """
    root = parse_metalink(metalink_xml_path)
    data_source: dict[str, Any] = {
        "license": root.findtext("./license/name", namespaces=NS),
        "publisher": root.findtext("./publisher/name", namespaces=NS),
        "resource_type": "dataset",
        "version": root.findtext("./version", namespaces=NS),
    }
    missing = [k for k, v in data_source.items() if not v]
    if missing:
        msg = f"Missing required elements from metalink file: {', '.join(missing)}"
        logger.error(msg)
        raise RuntimeError(msg)

    return data_source


def get_files(metalink_xml_path: Path | str, files_to_find: list[str] | None = None) -> dict[str, Any]:
    """Extract file download metadata from a metalink XML file.

    :param metalink_xml_path: path to the metalink XML file
    :type metalink_xml_path: Path | str
    :param files_to_find: optional allowlist of file names to extract
    :type files_to_find: list[str] | None
    :return: mapping of file name to size, checksum, checksum type, and US mirror URL
    :rtype: dict[str, Any]
    """
    root = parse_metalink(metalink_xml_path)

    if files_to_find is not None and files_to_find == []:
        logger.warning("Empty file list supplied to get_files: aborting.")
        return {}

    files: dict[str, Any] = {}
    for file_elem in root.findall("./files/file", NS):
        name = file_elem.get("name")
        if name is None or (files_to_find and name not in files_to_find):
            continue

        size = file_elem.findtext("./size", namespaces=NS)
        checksum = file_elem.find("./verification/hash", NS)
        if checksum is not None:
            checksum_fn = checksum.get("type")
            checksum_value = checksum.text
        else:
            checksum_fn = checksum_value = None
        dl_url = file_elem.findtext("./resources/url[@location='us']", namespaces=NS)
        files[name] = {
            "name": name,
            "size": size,
            "checksum": checksum_value,
            "checksum_fn": checksum_fn,
            "url": dl_url,
        }

    if files_to_find:
        not_found = {f for f in files_to_find if f not in files}
        if not_found:
            msg = "The following files were not found: " + ", ".join(sorted(not_found))
            logger.warning(msg)

    return files
