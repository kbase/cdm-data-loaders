"""Filesystem backends for testing readers against local and remote storage."""

import os
import posixpath
import uuid
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from operator import itemgetter
from pathlib import Path
from typing import Any, Final

import fsspec
import pytest
import s3fs
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.sources.filesystem import filesystem
from fsspec import AbstractFileSystem
from moto.server import ThreadedMotoServer

DRIVERS_ENV_VAR: Final[str] = "CDM_TEST_FS_DRIVERS"
LOCAL_PATH: Final[str] = "local_path"
LOCAL_FILE_URL: Final[str] = "local_file_url"
MEMORY: Final[str] = "memory"
S3_MOTO: Final[str] = "s3_moto"
S3_CEPH: Final[str] = "s3_ceph"

ALL_DRIVERS: Final[tuple[str, ...]] = (LOCAL_PATH, LOCAL_FILE_URL, MEMORY, S3_MOTO, S3_CEPH)
DEFAULT_DRIVERS: Final[tuple[str, ...]] = (LOCAL_PATH, LOCAL_FILE_URL, MEMORY, S3_MOTO)
LOCAL_DRIVERS: Final[frozenset[str]] = frozenset({LOCAL_PATH, LOCAL_FILE_URL})
S3_DRIVERS: Final[frozenset[str]] = frozenset({S3_MOTO, S3_CEPH})

MOTO_AWS_CREDENTIALS: Final[dict[str, str]] = {
    "aws_access_key_id": "test_access_key",
    "aws_secret_access_key": "test_access_val",
}
CEPH_BUCKET: Final[str] = "cdm-test-fs-backends"

DRIVER_MARKS: Final[dict[str, tuple[pytest.MarkDecorator, ...]]] = {S3_CEPH: (pytest.mark.requires_ceph,)}


def selected_drivers() -> tuple[str, ...]:
    """Return the backends named in ``CDM_TEST_FS_DRIVERS``, or ``DEFAULT_DRIVERS`` when unset.

    :return: Selected backend names in order.
    :rtype: tuple[str, ...]
    :raises ValueError: If an unknown backend is named.
    """
    raw = os.environ.get(DRIVERS_ENV_VAR, "").strip()
    if not raw:
        return DEFAULT_DRIVERS
    drivers = tuple(name.strip() for name in raw.split(",") if name.strip())
    unknown = sorted(set(drivers) - set(ALL_DRIVERS))
    if unknown:
        msg = f"Unknown filesystem backend(s) in {DRIVERS_ENV_VAR}: {unknown}; choose from {list(ALL_DRIVERS)}"
        raise ValueError(msg)
    return drivers


@dataclass(frozen=True, slots=True)
class FsBackend:
    """A location on a filesystem backend that tests can write files to and list with dlt."""

    name: str
    bucket_url: str
    root: str
    fs: AbstractFileSystem
    has_directories: bool

    @property
    def is_local(self) -> bool:
        """Return whether the backend uses the local filesystem.

        :return: ``True`` for local path or ``file://`` backends.
        :rtype: bool
        """
        return self.name in LOCAL_DRIVERS

    def path(self, relative_path: str) -> str:
        """Return the path of ``relative_path`` on ``fs``.

        :param relative_path: POSIX path relative to ``root``.
        :type relative_path: str
        :return: Full path on ``fs``.
        :rtype: str
        """
        return posixpath.join(self.root, relative_path)

    def put(self, relative_path: str, data: bytes, **kwargs: str) -> None:
        """Write ``data`` to ``relative_path``, creating parent directories when needed.

        :param relative_path: POSIX path relative to the backend root.
        :type relative_path: str
        :param data: Bytes to write.
        :type data: bytes
        :param kwargs: Extra keyword arguments forwarded to ``fs.pipe``.
        :type kwargs: str
        """
        self.fs.pipe(self.path(relative_path), data, **kwargs)

    def put_all(self, files: dict[str, bytes]) -> None:
        """Write every ``relative_path: data`` pair in ``files``.

        :param files: Mapping of relative POSIX paths to file contents.
        :type files: dict[str, bytes]
        """
        for relative_path, data in files.items():
            self.put(relative_path, data)

    def file_items(self, file_glob: str = "**/*") -> list[FileItemDict]:
        """List files matching ``file_glob`` via the dlt ``filesystem`` resource, sorted by relative path.

        :param file_glob: Glob pattern passed to ``filesystem``.
        :type file_glob: str
        :return: Sorted ``FileItemDict`` entries.
        :rtype: list[FileItemDict]
        """
        items = list(filesystem(bucket_url=self.bucket_url, credentials=self.fs, file_glob=file_glob))
        return sorted(items, key=itemgetter("relative_path"))

    def file_item(self, relative_path: str) -> FileItemDict:
        """Return the single file item matching ``relative_path``.

        :param relative_path: Expected ``relative_path`` entry.
        :type relative_path: str
        :return: Matching ``FileItemDict``.
        :rtype: FileItemDict
        """
        items = [item for item in self.file_items() if item["relative_path"] == relative_path]
        assert len(items) == 1, f"expected one file item for {relative_path!r}, found {len(items)}"
        return items[0]


@pytest.fixture(scope="session")
def s3_moto_endpoint() -> Iterator[str]:
    """Start a moto S3 server on a free local port for the test session and yield its URL.

    :return: Iterator yielding ``http://host:port``.
    :rtype: Iterator[str]
    """
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


def _s3_backend(name: str, endpoint_url: str, key: str, secret: str, bucket: str, *, create: bool) -> FsBackend:
    """Build an S3 backend rooted at a unique prefix in ``bucket``.

    :param name: Driver name.
    :type name: str
    :param endpoint_url: S3 HTTP endpoint URL.
    :type endpoint_url: str
    :param key: AWS access key ID.
    :type key: str
    :param secret: AWS secret access key.
    :type secret: str
    :param bucket: Bucket name.
    :type bucket: str
    :param create: Whether to create ``bucket`` when missing.
    :type create: bool
    :return: Configured ``FsBackend``.
    :rtype: FsBackend
    """
    # omit region_name so s3fs does not send LocationConstraint on us-east-1
    fs = s3fs.S3FileSystem(
        key=key,
        secret=secret,
        client_kwargs={"endpoint_url": endpoint_url},
        skip_instance_cache=True,
        use_listings_cache=False,
    )
    if create and not fs.exists(bucket):
        fs.mkdir(bucket)
    root = f"{bucket}/{uuid.uuid4().hex}"
    return FsBackend(name=name, bucket_url=f"s3://{root}", root=root, fs=fs, has_directories=False)


def _make_backend(driver: str, tmp_path: Path, request: pytest.FixtureRequest) -> FsBackend:
    """Create a fresh, empty backend location for ``driver``.

    :param driver: Backend driver name.
    :type driver: str
    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Configured ``FsBackend``.
    :rtype: FsBackend
    :raises ValueError: If ``driver`` is unknown.
    """
    if driver in LOCAL_DRIVERS:
        root_path = tmp_path / "fs_backend"
        root_path.mkdir()
        bucket_url = str(root_path) if driver == LOCAL_PATH else root_path.as_uri()
        local_fs = fsspec.filesystem("file", auto_mkdir=True, skip_instance_cache=True)
        return FsBackend(
            name=driver, bucket_url=bucket_url, root=root_path.as_posix(), fs=local_fs, has_directories=True
        )

    if driver == MEMORY:
        memory_fs = fsspec.filesystem("memory")
        root = f"/{uuid.uuid4().hex}"
        return FsBackend(name=driver, bucket_url=f"memory://{root}", root=root, fs=memory_fs, has_directories=False)

    if driver == S3_MOTO:
        endpoint_url = request.getfixturevalue("s3_moto_endpoint")
        return _s3_backend(
            driver,
            endpoint_url,
            MOTO_AWS_CREDENTIALS["aws_access_key_id"],
            MOTO_AWS_CREDENTIALS["aws_secret_access_key"],
            f"test-{uuid.uuid4().hex[:20]}",
            create=True,
        )

    if driver == S3_CEPH:
        return _s3_backend(
            driver,
            os.environ["AWS_ENDPOINT_URL"],
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"],
            CEPH_BUCKET,
            create=True,
        )

    msg = f"Unknown filesystem backend: {driver}"
    raise ValueError(msg)


def _remove_backend(backend: FsBackend) -> None:
    """Delete everything under the backend root for non-local drivers.

    :param backend: Active ``FsBackend`` to clean up.
    :type backend: FsBackend
    """
    if backend.is_local:
        return
    if backend.fs.exists(backend.root):
        backend.fs.rm(backend.root, recursive=True)
    if backend.name == S3_MOTO:
        bucket = backend.root.split("/", 1)[0]
        backend.fs.rmdir(bucket)


def _driver_params(*, include_local: bool) -> list[Any]:
    """Return ``pytest.param`` entries for selected backends with marks attached.

    :param include_local: Whether to include local drivers.
    :type include_local: bool
    :return: Parameterized pytest entries.
    :rtype: list[Any]
    """
    return [
        pytest.param(driver, id=driver, marks=DRIVER_MARKS.get(driver, ()))
        for driver in selected_drivers()
        if include_local or driver not in LOCAL_DRIVERS
    ]


@pytest.fixture(params=_driver_params(include_local=True))
def fs_backend(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[FsBackend]:
    """Yield an empty location on each selected filesystem backend.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Generator yielding ``FsBackend``.
    :rtype: Generator[FsBackend]
    """
    backend = _make_backend(request.param, tmp_path, request)
    try:
        yield backend
    finally:
        _remove_backend(backend)


@pytest.fixture(params=_driver_params(include_local=False))
def remote_fs_backend(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[FsBackend]:
    """Yield an empty location on each selected non-local filesystem backend.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Generator yielding ``FsBackend``.
    :rtype: Generator[FsBackend]
    """
    backend = _make_backend(request.param, tmp_path, request)
    try:
        yield backend
    finally:
        _remove_backend(backend)
