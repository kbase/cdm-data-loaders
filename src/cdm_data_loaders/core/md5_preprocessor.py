"""MD5 sidecar file pre-processor for validating raw input files before parsing."""

from typing import Final

from dlt.common.storages.fsspec_filesystem import FileItemDict

from cdm_data_loaders.core.file_items import is_local, source_id
from cdm_data_loaders.core.preprocessors import FilePreprocessError
from cdm_data_loaders.core.registry import FilePreprocessFn
from cdm_data_loaders.utils.checksums import (
    HASH_CHUNK_BYTES,
    compute_stream_checksum,
)
from cdm_data_loaders.utils.checksums import (
    parse_md5_sidecar as _parse_md5_sidecar_text,
)


def parse_md5_sidecar(content: str, source_file: str) -> str:
    """Extract the 32-character hex MD5 digest from sidecar file text.

    :param content: raw text content of the ``.md5`` sidecar file
    :type content: str
    :param source_file: relative path of the data file being validated
    :type source_file: str
    :return: lowercase 32-character hexadecimal MD5 digest
    :rtype: str
    :raises FilePreprocessError: if ``content`` is empty or does not contain a valid MD5 digest
    """
    return _parse_md5_sidecar_text(content, source_file, error_cls=FilePreprocessError)


def validate_md5_sidecar(file_item: FileItemDict) -> FileItemDict:
    """Validate ``file_item`` against its accompanying ``<file>.md5`` sidecar file.

    :param file_item: dlt file item to validate
    :type file_item: FileItemDict
    :return: validated ``file_item``
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


FILE_PREPROCESSORS: Final[tuple[FilePreprocessFn, ...]] = (validate_md5_sidecar,)
