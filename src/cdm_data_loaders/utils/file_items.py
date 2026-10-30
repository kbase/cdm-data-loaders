"""Helpers for reading input files that are either local paths or dlt filesystem file items.

dlt's ``filesystem`` resource yields ``FileItemDict`` objects, which may refer to local files or to
remote objects (e.g. in S3). These helpers let readers stream either kind of input without
downloading it first; ``local_copy`` is provided for consumers, such as external binaries, that
need a real local file.
"""

import shutil
from collections.abc import Generator
from contextlib import contextmanager
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import IO, Any, Literal

from dlt.common.storages.fsspec_filesystem import FileItemDict

from cdm_data_loaders.core.fields import GZIP_SUFFIX
from cdm_data_loaders.utils.gz import open_maybe_gzip

logger: Logger = getLogger(__name__)

type InputFile = str | Path | FileItemDict
"""An input file: a local path, or a file item from dlt's filesystem resource."""

LOCAL_FILE_PROTOCOL = "file"
COPY_CHUNK_SIZE = 16 * 1024 * 1024


def compression_mode(file_name: str) -> Literal["enable", "disable"]:
    """Return the dlt compression mode for a file name.

    Returns "enable" for names ending in .gz and "disable" otherwise.
    Auto-detection is not used, since it depends on remote file metadata
    that local files do not carry.
    """
    return "enable" if file_name.endswith(GZIP_SUFFIX) else "disable"


def is_file_item(source: InputFile) -> bool:
    """Whether ``source`` is a dlt file item rather than a local path."""
    return isinstance(source, FileItemDict)


@contextmanager
def open_input(source: InputFile) -> Generator[IO[bytes] | Any]:
    """Open an input file for binary reading, decompressing it if its name ends in .gz.

    Local paths are opened with ``open_maybe_gzip``; file items are streamed via their fsspec
    filesystem, so remote files are not downloaded in full before reading.

    :param source: local path or dlt file item
    :type source: InputFile
    :yield: an open binary file handle
    :rtype: Generator[IO[bytes] | Any]
    """
    if isinstance(source, FileItemDict):
        with source.open(mode="rb", compression=compression_mode(source["file_name"])) as fh:
            yield fh
        return
    with open_maybe_gzip(source) as fh:
        yield fh


def source_name(source: InputFile) -> str:
    """Return the base name of an input file, e.g. ``uniref50.xml.gz``."""
    if isinstance(source, FileItemDict):
        return PurePosixPath(source["file_name"]).name
    return Path(source).name


def source_label(source: InputFile) -> str:
    """Return the identifier recorded as an input file's provenance.

    For file items, this is the path relative to the input location (``relative_path``), which does
    not depend on where the input location is mounted or stored. Local paths are returned as given.
    """
    if isinstance(source, FileItemDict):
        return str(source["relative_path"])
    return str(source)


@contextmanager
def local_copy(file_item: FileItemDict) -> Generator[Path]:
    """Yield a local path for a file item, copying remote files to a temporary directory.

    Local (``file://``) items are not copied. Remote items are copied with their original file name,
    so suffix-based handling (e.g. ``.gz`` decompression) still works; the copy is deleted on exit.

    :param file_item: dlt file item
    :type file_item: FileItemDict
    :yield: path to a local file with the item's contents
    :rtype: Generator[Path]
    """
    protocols = file_item.fsspec.protocol
    protocols = (protocols,) if isinstance(protocols, str) else tuple(protocols)
    if LOCAL_FILE_PROTOCOL in protocols:
        yield Path(file_item.local_file_path)
        return

    with TemporaryDirectory(prefix="cdl_input_") as tmp_dir:
        local_path = Path(tmp_dir) / source_name(file_item)
        logger.debug("Copying %s to %s", file_item["file_url"], local_path)
        with file_item.open(mode="rb", compression="disable") as src, local_path.open("wb") as dest:
            shutil.copyfileobj(src, dest, COPY_CHUNK_SIZE)
        yield local_path
