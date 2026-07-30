"""Checksum shortcuts, built on top of file_transfer.checksums.py's Hasher abstraction."""

from pathlib import Path

from cdm_data_loaders.utils.file_transfer.checksums import compute_file_checksum


# Backwards-compatible thin wrappers, if other code still imports these names.
def compute_md5(file_path: str | Path) -> str:
    return compute_file_checksum(file_path, "md5")


def verify_md5(file_path: str | Path, expected_md5: str) -> bool:
    return compute_md5(file_path) == expected_md5


def compute_crc64nvme(file_path: str | Path) -> str:
    return compute_file_checksum(file_path, "crc64nvme")
