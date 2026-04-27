"""Tests for utils.checksums module — MD5 and CRC64/NVME checksum utilities."""

import base64
import hashlib
from pathlib import Path

import pytest

from cdm_data_loaders.utils.checksums import compute_md5, verify_md5

_EXPECTED_CRC64_BYTE_LEN = 8


class TestComputeMd5:
    """Test MD5 computation."""

    def test_correct_hash(self, tmp_path: Path) -> None:
        """Verify MD5 matches hashlib reference."""
        f = tmp_path / "test.bin"
        f.write_bytes(b"Hello, World!")
        assert compute_md5(f) == hashlib.md5(b"Hello, World!").hexdigest()  # noqa: S324

    def test_empty_file(self, tmp_path: Path) -> None:
        """Verify MD5 of an empty file."""
        f = tmp_path / "empty"
        f.write_bytes(b"")
        assert compute_md5(f) == hashlib.md5(b"").hexdigest()  # noqa: S324

    def test_accepts_str_path(self, tmp_path: Path) -> None:
        """Verify compute_md5 accepts a string path."""
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")
        assert compute_md5(str(f)) == hashlib.md5(b"data").hexdigest()  # noqa: S324


class TestVerifyMd5:
    """Test MD5 verification."""

    def test_correct(self, tmp_path: Path) -> None:
        """Verify True when MD5 matches."""
        f = tmp_path / "test.bin"
        f.write_bytes(b"Hello, World!")
        expected = hashlib.md5(b"Hello, World!").hexdigest()  # noqa: S324
        assert verify_md5(f, expected) is True

    def test_incorrect(self, tmp_path: Path) -> None:
        """Verify False when MD5 does not match."""
        f = tmp_path / "test.bin"
        f.write_bytes(b"Hello, World!")
        assert verify_md5(f, "0000000000000000") is False


class TestComputeCrc64nvme:
    """Test CRC64/NVME computation (skipped if awscrt unavailable)."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_awscrt(self) -> None:
        pytest.importorskip("awscrt")

    def test_returns_base64(self, tmp_path: Path) -> None:
        """Verify CRC64/NVME returns an 8-byte base64 string."""
        from cdm_data_loaders.utils.checksums import compute_crc64nvme  # noqa: PLC0415

        f = tmp_path / "test.bin"
        f.write_bytes(b"Hello, World!")
        crc = compute_crc64nvme(f)
        decoded = base64.b64decode(crc)
        assert len(decoded) == _EXPECTED_CRC64_BYTE_LEN

    def test_deterministic(self, tmp_path: Path) -> None:
        """Verify repeated calls return the same checksum."""
        from cdm_data_loaders.utils.checksums import compute_crc64nvme  # noqa: PLC0415

        f = tmp_path / "test.bin"
        f.write_bytes(b"test data for checksum")
        assert compute_crc64nvme(f) == compute_crc64nvme(f)
