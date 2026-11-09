"""File-level pre-processors for validating or filtering input files before parsing."""

import re
from collections.abc import Iterable
from typing import Any, Final

from dlt.common.storages.fsspec_filesystem import FileItemDict

from cdm_data_loaders.core.registry import (
    FilePreprocessFn,
    PreprocessorRegistry,
    validate_registry_table_names,
)
from cdm_data_loaders.utils.checksums import HASH_CHUNK_BYTES, compute_stream_checksum
from cdm_data_loaders.utils.file_items import is_local, source_id

MD5_BSD_RE: Final[re.Pattern[str]] = re.compile(r"^MD5\s*\(.+\)\s*=\s*(?P<digest>[0-9a-fA-F]{32})$")
MD5_GNU_OR_BARE_RE: Final[re.Pattern[str]] = re.compile(r"^(?P<digest>[0-9a-fA-F]{32})(?:\s+\*?.+)?$")


class FilePreprocessError(Exception):
    """Raised when a file fails pre-processing validation."""


def parse_md5_sidecar(content: str, source_file: str) -> str:
    """Extract the 32-character hex MD5 digest from sidecar file text.

    Supports BSD/NCBI format (``MD5(file.xml.gz)= <hex>``), GNU ``md5sum`` format
    (``<hex>  file.xml.gz`` or ``<hex> *file.xml.gz``), and bare 32-character hex digests.

    :param content: raw text content of the ``.md5`` sidecar file
    :type content: str
    :param source_file: relative path of the data file being validated
    :type source_file: str
    :return: lowercase 32-character hexadecimal MD5 digest
    :rtype: str
    :raises FilePreprocessError: if ``content`` is empty or does not contain a valid MD5 digest
    """
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = MD5_BSD_RE.match(line) or MD5_GNU_OR_BARE_RE.match(line)
        if match is not None:
            return match.group("digest").lower()
        msg = f"Invalid MD5 sidecar content for {source_file}: {line!r}"
        raise FilePreprocessError(msg)

    msg = f"Empty MD5 sidecar file for {source_file}"
    raise FilePreprocessError(msg)


def validate_md5_sidecar(file_item: FileItemDict) -> FileItemDict:
    """Validate ``file_item`` against its accompanying ``<file>.md5`` sidecar file.

    :param file_item: dlt file item to validate
    :type file_item: FileItemDict
    :return: the validated ``file_item``
    :rtype: FileItemDict
    :raises FilePreprocessError: if the sidecar file is missing, unreadable, malformed,
        or does not match the MD5 digest of the raw file bytes
    """
    src_id = source_id(file_item)
    base_path = file_item.local_file_path if is_local(file_item) else file_item["file_url"]
    md5_path = f"{base_path}.md5"

    try:
        with file_item.fsspec.open(md5_path, mode="rt", encoding="utf-8") as fh:
            sidecar_text = fh.read()
    except FileNotFoundError as exc:
        msg = f"Missing MD5 sidecar file for {src_id}: {md5_path}"
        raise FilePreprocessError(msg) from exc
    except OSError as exc:
        msg = f"Failed to read MD5 sidecar file for {src_id}: {exc}"
        raise FilePreprocessError(msg) from exc

    expected_md5 = parse_md5_sidecar(sidecar_text, src_id)

    with file_item.open(mode="rb", compression="disable") as raw_fh:
        actual_md5 = compute_stream_checksum(raw_fh, "md5", chunk_size=HASH_CHUNK_BYTES).lower()

    if actual_md5 != expected_md5:
        msg = f"MD5 mismatch for {src_id}: expected {expected_md5}, got {actual_md5}"
        raise FilePreprocessError(msg)

    return file_item


def validate_file_preprocessors(
    settings: Any,  # noqa: ANN401
    valid_tables: Iterable[str],
) -> None:
    """Ensure every table key in ``settings.file_preprocessors`` matches a configured input table.

    :param settings: pipeline settings instance
    :type settings: Any
    :param valid_tables: valid destination/input table names for the pipeline
    :type valid_tables: Iterable[str]
    :raises ValueError: if ``settings.file_preprocessors`` contains unknown table names
    """
    file_preprocessors: PreprocessorRegistry | None = getattr(settings, "file_preprocessors", None)
    if file_preprocessors is None or not file_preprocessors.registry:
        return
    validate_registry_table_names(file_preprocessors.registry, valid_tables, "preprocessors_module")


FILE_PREPROCESSORS: Final[tuple[FilePreprocessFn, ...]] = (validate_md5_sidecar,)
