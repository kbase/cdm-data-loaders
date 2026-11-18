"""General-purpose FTP client utilities.

Provides resilient FTP connections with TCP keepalive, NOOP pings, retry
on transient errors, and thread-local connection management for parallel
downloads.  Protocol-agnostic — callers supply the FTP hostname.
"""

import contextlib
import logging
import socket
import threading
import time
from collections.abc import Callable, Iterator
from ftplib import FTP, error_temp
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any, Final

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from cdm_data_loaders.utils.checksums import finalize_digest, new_hasher, validate_checksum_fn

logger: Logger = getLogger(__name__)

DEFAULT_TIMEOUT: Final[int] = 60
DEFAULT_RETRIES: Final[int] = 3
RETRY_WAIT_SECONDS: Final[int] = 2
TRANSIENT_FTP_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    error_temp,
    BrokenPipeError,
    ConnectionResetError,
    EOFError,
)


def connect_ftp(host: str, timeout: int = DEFAULT_TIMEOUT) -> FTP:
    """Connect and log in to an FTP server with TCP keepalive enabled.

    :param host: FTP hostname
    :type host: str
    :param timeout: connection timeout in seconds
    :type timeout: int
    :return: logged-in FTP connection
    """
    ftp = FTP(host, timeout=timeout)  # noqa: S321
    ftp.login()
    _set_keepalive(ftp)
    return ftp


@contextlib.contextmanager
def ftp_session(host: str, ftp: FTP | None = None, timeout: int = DEFAULT_TIMEOUT) -> Iterator[FTP]:
    """Context manager yielding an active FTP connection and closing it on exit if created here.

    :param host: FTP hostname
    :type host: str
    :param ftp: optional existing FTP connection (caller manages lifecycle when provided)
    :type ftp: FTP | None
    :param timeout: connection timeout in seconds
    :type timeout: int
    """
    owns_ftp = ftp is None
    active_ftp = connect_ftp(host, timeout=timeout) if ftp is None else ftp
    try:
        yield active_ftp
    finally:
        if owns_ftp:
            with contextlib.suppress(Exception):
                active_ftp.quit()


def _set_keepalive(ftp: FTP, idle: int = 30, interval: int = 10, count: int = 3) -> None:
    """Enable TCP keepalive on the FTP control socket.

    Prevents idle-timeout disconnects (e.g. '421 No transfer timeout') when
    the control connection sits idle during data transfers or checksum
    verification.
    """
    sock = ftp.sock
    if sock is None:
        return
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)


def ftp_noop_keepalive(ftp: FTP, last_activity: float, interval: int = 25) -> float:
    """Send NOOP if the connection has been idle longer than *interval* seconds.

    :param ftp: active FTP connection
    :type ftp: FTP
    :param last_activity: monotonic timestamp of last FTP activity
    :type last_activity: float
    :param interval: seconds of idle time before sending NOOP
    :type interval: int
    :return: updated last-activity timestamp
    """
    if time.monotonic() - last_activity > interval:
        with contextlib.suppress(Exception):
            ftp.sendcmd("NOOP")
        return time.monotonic()
    return last_activity


def _ftp_retry(retries: int) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Build a tenacity retry decorator for transient FTP and socket errors.

    :param retries: total attempt count before re-raising
    :type retries: int
    :return: tenacity retry decorator
    :rtype: Callable[[Callable[..., Any]], Callable[..., Any]]
    """
    return retry(
        retry=retry_if_exception_type(TRANSIENT_FTP_EXCEPTIONS),
        stop=stop_after_attempt(retries),
        wait=wait_fixed(RETRY_WAIT_SECONDS),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )


def ftp_list_dir(ftp: FTP, path: PurePosixPath, retries: int = DEFAULT_RETRIES) -> list[PurePosixPath]:
    """List files in an FTP directory with retry on transient errors.

    :param ftp: active FTP connection
    :type ftp: FTP
    :param path: remote directory path
    :type path: PurePosixPath
    :param retries: number of retry attempts
    :type retries: int
    :return: list of filenames
    :rtype: list[PurePosixPath]
    """
    ftp.cwd(str(path))

    @_ftp_retry(retries)
    def _list() -> list[PurePosixPath]:
        file_strings: list[str] = []
        ftp.retrlines("NLST", file_strings.append)
        return [PurePosixPath(f) for f in file_strings]

    return _list()


def ftp_download_file(
    ftp: FTP,
    remote_path: PurePosixPath,
    local_path: Path,
    retries: int = DEFAULT_RETRIES,
    *,
    checksum_fn: str | None = None,
) -> str | None:
    """Download a single file from FTP with retry on transient errors and optional in-stream hashing.

    :param ftp: active FTP connection
    :type ftp: FTP
    :param remote_path: remote file path
    :type remote_path: PurePosixPath
    :param local_path: local destination path
    :type local_path: Path
    :param retries: number of retry attempts
    :type retries: int
    :param checksum_fn: optional checksum algorithm name (for example, ``"md5"``)
    :type checksum_fn: str | None
    :return: computed checksum digest when ``checksum_fn`` is set, otherwise ``None``
    :rtype: str | None
    """
    validated_algo = validate_checksum_fn(checksum_fn) if checksum_fn is not None else None

    @_ftp_retry(retries)
    def _download() -> str | None:
        hasher = new_hasher(validated_algo) if validated_algo is not None else None
        with local_path.open("wb") as f:

            def _write_chunk(chunk: bytes) -> None:
                f.write(chunk)
                if hasher is not None:
                    hasher.update(chunk)

            ftp.retrbinary(f"RETR {remote_path}", _write_chunk)
        return finalize_digest(hasher) if hasher is not None else None

    return _download()


def ftp_retrieve_text(ftp: FTP, remote_path: PurePosixPath, retries: int = DEFAULT_RETRIES) -> str:
    """Retrieve a text file from FTP with retry on transient errors.

    :param ftp: active FTP connection
    :type ftp: FTP
    :param remote_path: remote file path
    :type remote_path: PurePosixPath
    :param retries: number of retry attempts
    :type retries: int
    :return: file content joined with newlines
    :rtype: str
    """

    @_ftp_retry(retries)
    def _retrieve() -> str:
        lines: list[str] = []
        ftp.retrlines(f"RETR {remote_path}", lines.append)
        return "\n".join(lines)

    return _retrieve()


class ThreadLocalFTP:
    """Manage thread-local FTP connections for parallel downloads.

    Each thread gets its own FTP connection, created on first access.
    Call :meth:`close_all` when done to cleanly shut down all connections.
    """

    def __init__(self, host: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        """Initialise with FTP host and timeout.

        :param host: FTP hostname (required — no default)
        :type host: str
        :param timeout: connection timeout in seconds
        :type timeout: int
        """
        self._host = host
        self._timeout = timeout
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connections: list[FTP] = []

    def get(self) -> FTP:
        """Return the FTP connection for the current thread, reconnecting if stale.

        Sends a NOOP to verify the connection is still alive before returning it.
        If the server has closed the connection (e.g. after an idle timeout or
        session limit), the dead socket is discarded and a fresh connection is
        established transparently.
        """
        ftp = getattr(self._local, "ftp", None)
        if ftp is not None:
            try:
                ftp.voidcmd("NOOP")
            except Exception:  # noqa: BLE001
                # Connection is stale — discard it and reconnect below
                with contextlib.suppress(Exception):
                    ftp.quit()
                with self._lock, contextlib.suppress(ValueError):
                    self._connections.remove(ftp)
                ftp = None
                self._local.ftp = None
        if ftp is None:
            ftp = connect_ftp(self._host, self._timeout)
            self._local.ftp = ftp
            with self._lock:
                self._connections.append(ftp)
        return ftp

    def close_all(self) -> None:
        """Close all thread-local FTP connections."""
        with self._lock:
            for ftp in self._connections:
                with contextlib.suppress(Exception):
                    ftp.quit()
            self._connections.clear()
