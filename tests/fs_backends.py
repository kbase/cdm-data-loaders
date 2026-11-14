"""Filesystem backends for testing readers against local and non-local storage.

Each backend exposes the same small interface (``FsBackend``), so a reader test can be written once and run on
every backend via the parametrized ``fs_backend`` fixture. The pattern follows fsspec's ``AbstractFixtures``
(one fixture class per filesystem) and dlt's ``ALL_FILESYSTEM_DRIVERS`` (the active backends are chosen by an
environment variable).

Backends:

- ``local_path``: a local directory given as a plain path
- ``local_file_url``: the same, given as a ``file://`` URL
- ``memory``: fsspec's in-memory filesystem. It is not ``file``, so it catches code that assumes a local path.
- ``s3_moto``: S3 via a moto server in a background thread (``moto.server.ThreadedMotoServer``). In-process
  ``mock_aws`` does not intercept the aiobotocore client used by s3fs, so a real HTTP endpoint is needed.
- ``s3_ceph``: S3 on the Ceph RGW container from ``docker-compose.yml``. Marked ``requires_ceph``.

Set ``CDM_TEST_FS_DRIVERS`` to a comma-separated list of backend names to choose which backends run; the default
is every backend except ``s3_ceph``.
"""

import os
import posixpath
import uuid
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import fsspec
import pytest
import s3fs
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.sources.filesystem import filesystem
from fsspec import AbstractFileSystem

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

MOTO_ACCESS_KEY: Final[str] = "test_access_key"
MOTO_SECRET_KEY: Final[str] = "test_access_secret"  # noqa: S105
CEPH_BUCKET: Final[str] = "cdm-test-fs-backends"

DRIVER_MARKS: Final[dict[str, tuple[pytest.MarkDecorator, ...]]] = {S3_CEPH: (pytest.mark.requires_ceph,)}


def selected_drivers() -> tuple[str, ...]:
    """Return the backends named in ``CDM_TEST_FS_DRIVERS``, or the default backends if it is unset.

    :raises ValueError: if an unknown backend is named
    :return: backend names, in the order given
    :rtype: tuple[str, ...]
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
    """A location on a filesystem backend that tests can write files to and list with dlt.

    :param name: backend name, e.g. ``memory``
    :param bucket_url: URL or path passed to the dlt ``filesystem`` resource as ``bucket_url``
    :param root: ``bucket_url`` as a path on ``fs`` (no protocol)
    :param fs: authenticated fsspec filesystem, also passed to dlt as ``credentials``
    :param has_directories: whether empty directories exist on this filesystem (False for object stores)
    """

    name: str
    bucket_url: str
    root: str
    fs: AbstractFileSystem
    has_directories: bool

    @property
    def is_local(self) -> bool:
        """Whether the backend is the local filesystem."""
        return self.name in LOCAL_DRIVERS

    def path(self, relative_path: str) -> str:
        """Return the path of ``relative_path`` on ``fs``."""
        return posixpath.join(self.root, relative_path)

    def put(self, relative_path: str, data: bytes, **kwargs: Any) -> None:  # noqa: ANN401
        """Write ``data`` to ``relative_path``, creating parent directories as needed.

        :param relative_path: POSIX path relative to the backend root
        :param data: bytes to write
        :param kwargs: extra arguments for ``fs.pipe``, e.g. ``ContentEncoding`` for S3
        """
        self.fs.pipe(self.path(relative_path), data, **kwargs)

    def put_all(self, files: dict[str, bytes]) -> None:
        """Write every ``relative_path: data`` pair in ``files``."""
        for relative_path, data in files.items():
            self.put(relative_path, data)

    def file_items(self, file_glob: str = "**/*") -> list[FileItemDict]:
        """List files matching ``file_glob`` with the dlt ``filesystem`` resource, sorted by relative path."""
        items = list(filesystem(bucket_url=self.bucket_url, credentials=self.fs, file_glob=file_glob))
        return sorted(items, key=lambda item: item["relative_path"])

    def file_item(self, relative_path: str) -> FileItemDict:
        """Return the file item for ``relative_path``, listed with the dlt ``filesystem`` resource."""
        items = [item for item in self.file_items() if item["relative_path"] == relative_path]
        assert len(items) == 1, f"expected one file item for {relative_path!r}, found {len(items)}"
        return items[0]


@pytest.fixture(scope="session")
def s3_moto_endpoint() -> Iterator[str]:
    """Start a moto S3 server on a free local port for the test session and return its endpoint URL."""
    from moto.server import ThreadedMotoServer  # noqa: PLC0415 - optional dependency, only needed for S3 tests

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


def _s3_backend(name: str, endpoint_url: str, key: str, secret: str, bucket: str, *, create: bool) -> FsBackend:
    """Build an S3 backend rooted at a unique prefix in ``bucket``."""
    fs = s3fs.S3FileSystem(
        key=key,
        secret=secret,
        # no region_name: s3fs would send it as a bucket LocationConstraint, which S3 rejects for us-east-1
        client_kwargs={"endpoint_url": endpoint_url},
        skip_instance_cache=True,
        use_listings_cache=False,
    )
    if create and not fs.exists(bucket):
        fs.mkdir(bucket)
    root = f"{bucket}/{uuid.uuid4().hex}"
    return FsBackend(name=name, bucket_url=f"s3://{root}", root=root, fs=fs, has_directories=False)


def _make_backend(driver: str, tmp_path: Path, request: pytest.FixtureRequest) -> FsBackend:
    """Create a fresh, empty backend location for ``driver``."""
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
        # a bucket per test keeps tests independent on the shared server
        return _s3_backend(
            driver, endpoint_url, MOTO_ACCESS_KEY, MOTO_SECRET_KEY, f"test-{uuid.uuid4().hex[:20]}", create=True
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
    """Delete everything under the backend root (local roots are removed with ``tmp_path``)."""
    if backend.is_local:
        return
    if backend.fs.exists(backend.root):
        backend.fs.rm(backend.root, recursive=True)
    if backend.name == S3_MOTO:
        bucket = backend.root.split("/", 1)[0]
        backend.fs.rmdir(bucket)


def _driver_params(*, include_local: bool) -> list[Any]:
    """Return ``pytest.param`` entries for the selected backends, with their marks."""
    return [
        pytest.param(driver, id=driver, marks=DRIVER_MARKS.get(driver, ()))
        for driver in selected_drivers()
        if include_local or driver not in LOCAL_DRIVERS
    ]


@pytest.fixture(params=_driver_params(include_local=True))
def fs_backend(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[FsBackend]:
    """Yield an empty location on each selected filesystem backend."""
    backend = _make_backend(request.param, tmp_path, request)
    try:
        yield backend
    finally:
        _remove_backend(backend)


@pytest.fixture(params=_driver_params(include_local=False))
def remote_fs_backend(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[FsBackend]:
    """Yield an empty location on each selected backend that is not the local filesystem."""
    backend = _make_backend(request.param, tmp_path, request)
    try:
        yield backend
    finally:
        _remove_backend(backend)
