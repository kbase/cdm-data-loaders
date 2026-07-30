"""Algorithm-agnostic checksum helpers."""

import base64
import hashlib
from dataclasses import dataclass
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from awscrt.checksums import crc64nvme as _crc64nvme

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

    def update(self, data: bytes) -> object: ...
    def hexdigest(self) -> str: ...


class Crc64NvmeHasher:
    """Adapts awscrt's stateless crc64nvme(data, prev) function to a hashlib-like interface."""

    def __init__(self) -> None:
        self._crc = 0

    def update(self, data: bytes) -> None:
        self._crc = _crc64nvme(data, self._crc)

    def digest(self) -> bytes:
        return self._crc.to_bytes(8, byteorder="big")

    def hexdigest(self) -> str:
        return self.digest().hex()

    def b64digest(self) -> str:
        """Base64 form, matching S3's ChecksumCRC64NVME representation."""
        return base64.b64encode(self.digest()).decode()


def _final_digest(hasher: Hasher) -> str:
    """Return the canonical digest representation for a completed hasher.

    Uses the base64 form for crc64nvme (matching S3's ChecksumCRC64NVME
    representation) and the hex form for everything else.

    :param hasher: a hasher that has already consumed all its input data
    :type hasher: Hasher
    :return: hex digest, or base64 digest for crc64nvme
    :rtype: str
    """
    if isinstance(hasher, Crc64NvmeHasher):
        return hasher.b64digest()
    return hasher.hexdigest()


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


def compute_file_checksum(file_path: str | Path, algorithm: str) -> str:
    """Compute a checksum for a local file using any algorithm new_hasher supports.

    :param file_path: path to the file
    :param algorithm: hashlib algorithm name, or "crc64nvme"
    :return: hex digest, or base64 digest for crc64nvme (matching S3's format)
    """
    hasher: Hasher = new_hasher(algorithm)
    with Path(file_path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            hasher.update(chunk)
    return _final_digest(hasher)


def validate_checksum_fn(checksum_fn: str) -> str:
    """Validate a checksum/hash algorithm name against hashlib + our extra algorithms."""
    checksum_fn = checksum_fn.lower()
    if checksum_fn in EXTRA_ALGORITHMS:
        return checksum_fn
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


def new_hasher(algorithm: str) -> Hasher:
    """Construct a Hasher for any supported algorithm (hashlib or crc64nvme)."""
    algorithm = validate_checksum_fn(algorithm)
    if algorithm == "crc64nvme":
        return Crc64NvmeHasher()
    return hashlib.new(algorithm)
