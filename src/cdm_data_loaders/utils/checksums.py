"""Algorithm-agnostic checksum helpers."""

import base64
import hashlib
import re
from dataclasses import dataclass
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import IO, Any, Final, Protocol, runtime_checkable

from awscrt.checksums import crc64nvme as _crc64nvme

HASH_CHUNK_BYTES: Final[int] = 1 << 20

MD5_BSD_RE: Final[re.Pattern[str]] = re.compile(r"^MD5\s*\(.+\)\s*=\s*(?P<digest>[0-9a-fA-F]{32})$")
MD5_GNU_OR_BARE_RE: Final[re.Pattern[str]] = re.compile(r"^(?P<digest>[0-9a-fA-F]{32})(?:\s+\*?.+)?$")

# Algorithms handled outside hashlib but exposed through the same interface.
EXTRA_ALGORITHMS = frozenset({"crc64nvme"})

logger: Logger = getLogger(__name__)


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


@runtime_checkable
class Hasher(Protocol):
    """Minimal interface satisfied by both hashlib hash objects and Crc64NvmeHasher."""

    def update(self, data: bytes) -> object:
        """Update the hash object with `data`, a bytes-like object.

        Repeated calls are equivalent to a single call with the concatenation of all the arguments: m.update(a); m.update(b) is equivalent to m.update(a+b).
        """
        ...

    def hexdigest(self) -> str:
        """Return the digest of the data passed to the update() method so far.

        The digest is returned as a string object of double length, containing only hexadecimal digits.

        :return: digest of data so far, as a string containing only hexadecimal digits
        :rtype: str
        """
        ...


class Crc64NvmeHasher:
    """Adapts awscrt's stateless crc64nvme(data, prev) function to a hashlib-like interface."""

    def __init__(self) -> None:
        """Create a new Crc64NvmeHasher with an initial CRC of 0."""
        self._crc = 0

    def update(self, data: bytes) -> None:
        """Update the hash object with `data`, a bytes-like object."""
        self._crc = _crc64nvme(data, self._crc)

    def digest(self) -> bytes:
        """Return the digest of the data passed to the update() method so far."""
        return self._crc.to_bytes(8, byteorder="big")

    def hexdigest(self) -> str:
        """Return the digest of the data passed to the update() method so far as a string of hexadecimal digits."""
        return self.digest().hex()

    def b64digest(self) -> str:
        """Base64 form, matching S3's ChecksumCRC64NVME representation."""
        return base64.b64encode(self.digest()).decode()


def finalize_digest(hasher: Hasher) -> str:
    """Return the canonical digest string for a completed hasher.

    Uses the base64 form for ``crc64nvme`` (matching S3's ``ChecksumCRC64NVME``
    representation) and lowercase hex for all other algorithms.

    :param hasher: hasher that has consumed all input bytes
    :type hasher: Hasher
    :return: hex digest, or base64 digest for ``crc64nvme``
    :rtype: str
    """
    if isinstance(hasher, Crc64NvmeHasher):
        return hasher.b64digest()
    return hasher.hexdigest()


class HashingReader:
    """Wraps a file-like object, computing a hash of everything read through it.

    Transparently proxies attribute access (e.g. `close`) to the wrapped
    object so it remains compatible with s3transfer's fileobj introspection.
    Deliberately does not implement `seek`/`tell` itself, so non-seekable
    streams (such as `requests`' `response.raw`) are still treated as
    non-seekable by anything inspecting the wrapper.
    """

    def __init__(self, raw: Any, algorithm: str) -> None:
        """Initialise a hashing wrapper around a readable, file-like object.

        :param raw: the underlying file-like object to wrap; must implement `read`
        :type raw: Any
        :param algorithm: hashlib algorithm name to use for hashing, e.g. "md5"
        :type algorithm: str
        :raises ValueError: if `algorithm` is not a supported hashlib algorithm
        """
        self._raw = raw
        self._hash = new_hasher(algorithm)

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

    def __getattr__(self, item: str) -> Any:
        """Proxy any attribute not defined on this wrapper to the wrapped object.

        :param item: attribute name being accessed
        :type item: str
        :return: the attribute value from the wrapped object
        :rtype: Any
        :raises AttributeError: if `item` is one of this wrapper's own internal
            attributes (not yet set) or is missing from the wrapped object too
        """
        # __getattr__ is only invoked when normal attribute lookup fails, which
        # includes the case where `_raw` itself hasn't been assigned yet (e.g.
        # during copy/pickle/deepcopy, or attribute access before __init__ runs).
        # Without this guard, looking up `_raw` would recurse into this method
        # forever trying to resolve `self._raw`.
        if item in ("_raw", "_hash"):
            msg = f"{type(self).__name__!r} object has no attribute {item!r}"
            raise AttributeError(msg)
        return getattr(self._raw, item)

    def hexdigest(self) -> str:
        """Return the hex digest of all bytes read so far.

        :return: hex digest of the accumulated hash
        :rtype: str
        """
        return self._hash.hexdigest()

    def b64digest(self) -> str:
        """Return the base64 digest of all bytes read so far.

        Only supported for algorithms whose hasher implements `b64digest`
        (currently crc64nvme), matching S3's ChecksumCRC64NVME representation.

        :return: base64-encoded digest of the accumulated hash
        :rtype: str
        :raises AttributeError: if the underlying algorithm's hasher does not
            support base64 digests
        """
        return self._hash.b64digest()


def compute_stream_checksum(
    stream: IO[bytes],
    algorithm: str,
    chunk_size: int = HASH_CHUNK_BYTES,
) -> str:
    """Compute a checksum from an open binary stream using any algorithm ``new_hasher`` supports.

    :param stream: readable binary stream
    :type stream: IO[bytes]
    :param algorithm: hashlib algorithm name, or "crc64nvme"
    :type algorithm: str
    :param chunk_size: read chunk size in bytes
    :type chunk_size: int
    :return: hex digest, or base64 digest for crc64nvme (matching S3's format)
    :rtype: str
    """
    hasher: Hasher = new_hasher(algorithm)
    for chunk in iter(lambda: stream.read(chunk_size), b""):
        hasher.update(chunk)
    return finalize_digest(hasher)


def compute_file_checksum(file_path: str | Path, algorithm: str) -> str:
    """Compute a checksum for a local file using any algorithm new_hasher supports.

    :param file_path: path to the file
    :type file_path: str | Path
    :param algorithm: hashlib algorithm name, or "crc64nvme"
    :type algorithm: str
    :return: hex digest, or base64 digest for crc64nvme (matching S3's format)
    """
    with Path(file_path).open("rb") as f:
        return compute_stream_checksum(f, algorithm)


def validate_checksum_fn(checksum_fn: str, include_extras: bool = True) -> str:  # noqa: FBT001, FBT002
    """Validate a checksum/hash algorithm name against hashlib + extra algorithms.

    :param checksum_fn: name of the algorithm to validate, e.g. "md5" or "sha256"
    :type checksum_fn: str
    :param include_extras: whether to include the non-hashlib algorithms, defaults to True
    :type include_extras: bool, optional
    :raises ValueError: if the algorithm is not supported
    :return: the validated algorithm name, lowercased
    :rtype: str
    """
    checksum_fn = checksum_fn.lower()
    valid_algorithms = {algo for algo in hashlib.algorithms_available if not algo.startswith("shake")}
    if include_extras:
        valid_algorithms |= EXTRA_ALGORITHMS

    if checksum_fn not in valid_algorithms:
        msg = f"Hashing algorithm {checksum_fn} not supported."
        logger.error(msg)
        raise ValueError(msg)
    return checksum_fn


def new_hasher(algorithm: str, include_extras: bool = True) -> Hasher:  # noqa: FBT001, FBT002
    """Construct a Hasher for any supported algorithm (hashlib or crc64nvme).

    :param algorithm: algorithm name
    :type algorithm: str
    :param include_extras: whether to include the non-hashlib algorithms, defaults to True
    :type include_extras: bool, optional
    :return: initialized hasher instance
    :rtype: Hasher
    """
    algorithm = validate_checksum_fn(algorithm, include_extras=include_extras)
    if algorithm == "crc64nvme":
        return Crc64NvmeHasher()
    return hashlib.new(algorithm)


def parse_md5_sidecar(
    content: str,
    source_file: str,
    *,
    error_cls: type[Exception] = ValueError,
) -> str:
    """Extract the 32-character hex MD5 digest from sidecar file text.

    Supports BSD/NCBI format (``MD5(file.xml.gz)= <hex>``), GNU ``md5sum`` format
    (``<hex>  file.xml.gz`` or ``<hex> *file.xml.gz``), and bare 32-character hex digests.

    :param content: raw text content of the ``.md5`` sidecar file
    :type content: str
    :param source_file: relative path of the data file being validated
    :type source_file: str
    :param error_cls: exception class to raise when ``content`` is empty or malformed
    :type error_cls: type[Exception]
    :return: lowercase 32-character hexadecimal MD5 digest
    :rtype: str
    :raises Exception: instance of ``error_cls`` if ``content`` is empty or invalid
    """
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = MD5_BSD_RE.match(line) or MD5_GNU_OR_BARE_RE.match(line)
        if match is not None:
            return match.group("digest").lower()
        msg = f"Invalid MD5 sidecar content for {source_file}: {line!r}"
        raise error_cls(msg)

    msg = f"Empty MD5 sidecar file for {source_file}"
    raise error_cls(msg)


def parse_md5_checksums_file(text: str) -> dict[PurePosixPath, str]:
    """Parse an ``md5checksums.txt`` file into a relative-path-to-MD5 mapping.

    Each entry line uses the two-space GNU format ``<md5>  ./<filename>`` or ``<md5>  <filename>``.

    :param text: raw text of the ``md5checksums.txt`` file
    :type text: str
    :return: dictionary mapping ``PurePosixPath`` relative file path to MD5 hex digest
    :rtype: dict[PurePosixPath, str]
    """
    expected_columns = 2
    checksums: dict[PurePosixPath, str] = {}
    for raw_line in text.strip().splitlines():
        parts = raw_line.strip().split("  ", maxsplit=1)
        if len(parts) == expected_columns:
            md5_hash, filename = parts
            checksums[PurePosixPath(filename.removeprefix("./"))] = md5_hash.strip()
    return checksums
