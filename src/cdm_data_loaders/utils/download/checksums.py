"""Algorithm-agnostic checksum helpers shared between local and S3 download paths.

These helpers are intentionally free of any network/S3 dependencies so that
they can be unit tested in isolation from `link_scraper.py` and `s3_upload.py`.
"""

import hashlib
from dataclasses import dataclass
from logging import Logger, getLogger
from urllib.parse import urljoin

logger: Logger = getLogger(__name__)


def validate_checksum_fn(checksum_fn: str) -> str:
    """Validate a checksum/hash algorithm name against what hashlib supports.

    Shake algorithms are rejected because their `hexdigest()` requires a
    length argument, which doesn't fit the fixed-digest comparisons used here.

    :param checksum_fn: algorithm name, e.g. "md5", "sha256"
    :type checksum_fn: str
    :raises ValueError: if the algorithm is unsupported or a shake variant
    :return: the lowercased, validated algorithm name
    :rtype: str
    """
    checksum_fn = checksum_fn.lower()
    if checksum_fn not in hashlib.algorithms_available or checksum_fn.startswith("shake"):
        msg = f"Hashing algorithm {checksum_fn} not supported."
        logger.error(msg)
        raise ValueError(msg)
    return checksum_fn


def resolve_checksum_fn(
    expected_checksum: str | None,
    checksum_fn: str | None,
    default_checksum_fn: str,
) -> str | None:
    """Resolve and validate which algorithm to use for a checksum comparison.

    Centralises the "default the algorithm if a checksum was given but no
    algorithm was specified" rule used by both `stream_to_s3` and
    `S3StreamUploader.upload`.

    :param expected_checksum: expected digest to compare against, or None if
        no verification is required
    :type expected_checksum: str | None
    :param checksum_fn: algorithm requested by the caller, or None to fall
        back to `default_checksum_fn`
    :type checksum_fn: str | None
    :param default_checksum_fn: algorithm to use when `checksum_fn` is not
        supplied but `expected_checksum` is
    :type default_checksum_fn: str
    :raises ValueError: if the resolved algorithm is not a supported hashlib algorithm
    :return: the validated algorithm name, or None if no verification is required
    :rtype: str | None
    """
    if not expected_checksum:
        return None
    return validate_checksum_fn(checksum_fn or default_checksum_fn)


@dataclass(frozen=True, slots=True)
class ChecksumEntry:
    """A single expected checksum, tied to a specific algorithm.

    :param algorithm: hashlib algorithm name the checksum was generated with
    :type algorithm: str
    :param value: expected digest, as lowercase hex
    :type value: str
    """

    algorithm: str
    value: str


def parse_checksum_file(text: str, base_url: str, algorithm: str) -> dict[str, ChecksumEntry]:
    """Parse a `*SUM.txt`-style checksum manifest (md5sum/sha256sum -c format).

    Expected line format: "<hex digest>  ./relative/path", one entry per
    line (matches the output of GNU coreutils' md5sum/sha256sum/etc).

    :param text: raw contents of the checksum manifest file
    :type text: str
    :param base_url: URL of the directory the manifest was fetched from;
        relative paths inside the file are resolved against this
    :type base_url: str
    :param algorithm: hashlib algorithm name the digests in this file were
        generated with, e.g. "md5", "sha256"
    :type algorithm: str
    :raises ValueError: if `algorithm` is not a supported hashlib algorithm
    :return: mapping of absolute file URL to its expected ChecksumEntry
    :rtype: dict[str, ChecksumEntry]
    """
    algorithm = validate_checksum_fn(algorithm)
    checksums: dict[str, ChecksumEntry] = {}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split(None, 1)
        if len(parts) != 2:  # noqa: PLR2004
            continue

        digest, rel_path = parts
        rel_path = rel_path.strip().removeprefix("./")
        url = urljoin(base_url, rel_path)
        checksums[url] = ChecksumEntry(algorithm=algorithm, value=digest.lower())

    return checksums


class HashingReader:
    """
    Wraps a file-like object, computing a hash of everything read through it.

    Transparently proxies attribute access (e.g. `close`) to the wrapped
    object so it remains compatible with s3transfer's fileobj introspection.
    Deliberately does not implement `seek`/`tell` itself, so non-seekable
    streams (such as `requests`' `response.raw`) are still treated as
    non-seekable by anything inspecting the wrapper.
    """

    def __init__(self, raw, algorithm: str) -> None:
        """Initialise a hashing wrapper around a readable, file-like object.

        :param raw: the underlying file-like object to wrap; must implement `read`
        :type raw: Any
        :param algorithm: hashlib algorithm name to use for hashing, e.g. "md5"
        :type algorithm: str
        :raises ValueError: if `algorithm` is not a supported hashlib algorithm
        """
        self._raw = raw
        self._hash = hashlib.new(validate_checksum_fn(algorithm))

    def read(self, amt: int = -1) -> bytes:
        """Read up to `amt` bytes from the wrapped object, updating the running hash.

        :param amt: number of bytes to read; -1 reads all remaining data, defaults to -1
        :type amt: int, optional
        :return: the bytes read
        :rtype: bytes
        """
        chunk = self._raw.read(amt)
        if chunk:
            self._hash.update(chunk)
        return chunk

    def __getattr__(self, item: str):
        """Proxy any attribute not defined on this wrapper to the wrapped object.

        :param item: attribute name being accessed
        :type item: str
        :return: the attribute value from the wrapped object
        :rtype: Any
        """
        return getattr(self._raw, item)

    def hexdigest(self) -> str:
        """Return the hex digest of all bytes read so far.

        :return: hex digest of the accumulated hash
        :rtype: str
        """
        return self._hash.hexdigest()
