"""General-purpose file checksum utilities.

Provides MD5 and CRC64/NVME checksum computation and verification for local
files.  These are protocol-agnostic primitives used by download pipelines
and S3 metadata workflows.
"""

import base64
import hashlib
from pathlib import Path

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger

logger = get_cdm_logger()


def compute_md5(file_path: str | Path) -> str:
    """Compute the MD5 hex digest of a file.

    :param file_path: path to the file
    :return: lowercase hex MD5 string
    """
    md5_hash = hashlib.md5()  # noqa: S324
    with Path(file_path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def verify_md5(file_path: str | Path, expected_md5: str) -> bool:
    """Verify a file's MD5 checksum against an expected value.

    :param file_path: path to the file
    :param expected_md5: expected lowercase hex MD5 string
    :return: True if the checksum matches
    """
    return compute_md5(file_path) == expected_md5


def compute_crc64nvme(file_path: str | Path) -> str:
    """Compute the CRC64/NVME checksum of a file.

    Returns the base64-encoded string matching the format used by S3-native
    checksums (``ChecksumCRC64NVME``).

    :param file_path: path to the file
    :return: base64-encoded CRC64/NVME checksum
    """
    from awscrt.checksums import crc64nvme as _crc64nvme  # noqa: PLC0415

    crc = 0
    with Path(file_path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            crc = _crc64nvme(chunk, crc)
    return base64.b64encode(crc.to_bytes(8, byteorder="big")).decode()
