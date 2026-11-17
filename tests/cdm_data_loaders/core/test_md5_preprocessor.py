"""Unit tests for ``cdm_data_loaders.core.md5_preprocessor``."""

import gzip
from typing import Final
from unittest.mock import patch

import pytest

from cdm_data_loaders.core.md5_preprocessor import (
    FILE_PREPROCESSORS,
    parse_md5_sidecar,
    validate_md5_sidecar,
)
from cdm_data_loaders.core.preprocessors import FilePreprocessError
from cdm_data_loaders.utils.checksums import new_hasher
from tests.helpers.fs_backends import FsBackend

SAMPLE_DIGEST: Final[str] = "d41d8cd98f00b204e9800998ecf8427e"


def _md5_hex(data: bytes) -> str:
    """Compute the MD5 hex digest of ``data``.

    :param data: raw byte sequence
    :type data: bytes
    :return: lowercase MD5 hex digest
    :rtype: str
    """
    hasher = new_hasher("md5")
    hasher.update(data)
    return hasher.hexdigest()


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        pytest.param(
            f"MD5(pubmed26n0001.xml.gz)= {SAMPLE_DIGEST}\n",
            SAMPLE_DIGEST,
            id="ncbi-bsd-format",
        ),
        pytest.param(
            f"\n  MD5 ( pubmed26n0001.xml.gz ) = {SAMPLE_DIGEST.upper()}  \n",
            SAMPLE_DIGEST,
            id="ncbi-bsd-uppercase-and-whitespace",
        ),
        pytest.param(
            f"{SAMPLE_DIGEST}  pubmed26n0001.xml.gz\n",
            SAMPLE_DIGEST,
            id="gnu-md5sum-two-spaces",
        ),
        pytest.param(
            f"{SAMPLE_DIGEST} *pubmed26n0001.xml.gz\n",
            SAMPLE_DIGEST,
            id="gnu-md5sum-binary-star",
        ),
        pytest.param(
            f"  {SAMPLE_DIGEST.upper()} \n",
            SAMPLE_DIGEST,
            id="bare-hex-digest",
        ),
    ],
)
def test_parse_md5_sidecar_pass_formats(content: str, expected: str) -> None:
    """Extract lowercase 32-char hex digests across BSD, GNU md5sum, and bare formats."""
    assert parse_md5_sidecar(content, "pubmed26n0001.xml.gz") == expected


@pytest.mark.parametrize(
    ("content", "match"),
    [
        pytest.param("", "Empty MD5 sidecar file for data.xml.gz", id="empty"),
        pytest.param("   \n\t\n  ", "Empty MD5 sidecar file for data.xml.gz", id="whitespace-only"),
        pytest.param("not-a-valid-md5", "Invalid MD5 sidecar content for data.xml.gz", id="malformed-text"),
        pytest.param("abc123", "Invalid MD5 sidecar content for data.xml.gz", id="short-hex"),
        pytest.param(
            f"SHA256(data.xml.gz)= {SAMPLE_DIGEST}",
            "Invalid MD5 sidecar content for data.xml.gz",
            id="wrong-algorithm-prefix",
        ),
    ],
)
def test_parse_md5_sidecar_fail_invalid_content(content: str, match: str) -> None:
    """Raise FilePreprocessError on empty or malformed sidecar content."""
    with pytest.raises(FilePreprocessError, match=match):
        parse_md5_sidecar(content, "data.xml.gz")


@pytest.mark.parametrize(
    "sidecar_template",
    [
        pytest.param("MD5(sample.xml.gz)= {digest}\n", id="bsd-format"),
        pytest.param("{digest}  sample.xml.gz\n", id="gnu-format"),
        pytest.param("{digest}\n", id="bare-format"),
    ],
)
def test_validate_md5_sidecar_pass_any_backend(
    fs_backend: FsBackend,
    sidecar_template: str,
) -> None:
    """Hash raw compressed .xml.gz bytes and validate against the .md5 sidecar across backends."""
    compressed_bytes = gzip.compress(b"<PubmedArticleSet><PubmedArticle/></PubmedArticleSet>", mtime=0)
    digest = _md5_hex(compressed_bytes)
    fs_backend.put("sub/sample.xml.gz", compressed_bytes)
    fs_backend.put("sub/sample.xml.gz.md5", sidecar_template.format(digest=digest).encode("utf-8"))

    items = fs_backend.file_items(file_glob="**/*.xml.gz")
    assert len(items) == 1
    validated = validate_md5_sidecar(items[0])
    assert validated is items[0]
    assert list(FILE_PREPROCESSORS) == [validate_md5_sidecar]


def test_validate_md5_sidecar_fail_missing_sidecar(fs_backend: FsBackend) -> None:
    """Raise FilePreprocessError when the accompanying .md5 file does not exist."""
    fs_backend.put("sample.xml.gz", gzip.compress(b"<root/>", mtime=0))
    items = fs_backend.file_items(file_glob="*.xml.gz")
    assert len(items) == 1

    with pytest.raises(FilePreprocessError, match=r"Missing MD5 sidecar file for sample\.xml\.gz"):
        validate_md5_sidecar(items[0])


def test_validate_md5_sidecar_fail_mismatch(fs_backend: FsBackend) -> None:
    """Raise FilePreprocessError when the computed MD5 does not match the sidecar digest."""
    compressed_bytes = gzip.compress(b"<root>actual</root>", mtime=0)
    actual_digest = _md5_hex(compressed_bytes)
    wrong_digest = "0" * 32
    fs_backend.put("sample.xml.gz", compressed_bytes)
    fs_backend.put("sample.xml.gz.md5", f"MD5(sample.xml.gz)= {wrong_digest}\n".encode())
    items = fs_backend.file_items(file_glob="*.xml.gz")

    with pytest.raises(
        FilePreprocessError,
        match=rf"MD5 mismatch for sample\.xml\.gz: expected {wrong_digest}, got {actual_digest}",
    ):
        validate_md5_sidecar(items[0])


def test_validate_md5_sidecar_fail_oserror_reading_sidecar(fs_backend: FsBackend) -> None:
    """Wrap non-FileNotFoundError OSErrors in FilePreprocessError."""
    fs_backend.put("sample.xml.gz", gzip.compress(b"<root/>", mtime=0))
    file_item = fs_backend.file_items(file_glob="*.xml.gz")[0]

    with (
        patch.object(file_item.fsspec, "open", side_effect=PermissionError("permission denied")),
        pytest.raises(
            FilePreprocessError,
            match=r"Failed to read MD5 sidecar file for sample\.xml\.gz: permission denied",
        ),
    ):
        validate_md5_sidecar(file_item)
