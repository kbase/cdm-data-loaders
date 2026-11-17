"""Pure gzip file compression and decompression helpers."""

import gzip
import shutil
from logging import Logger, getLogger
from pathlib import Path
from typing import Final

logger: Logger = getLogger(__name__)

GZIP_SUFFIX: Final[str] = ".gz"


def decompress_file(file: str | Path) -> None:
    """Decompress a gzip file in place unless the uncompressed target exists.

    :param file: file path to decompress
    :type file: str | Path
    """
    if not isinstance(file, Path):
        file = Path(file)

    if file.suffix != GZIP_SUFFIX:
        logger.info("File %s does not end with .gz: skipping decompression", str(file))
        return

    if not file.exists():
        logger.warning("File %s does not exist: skipping decompression", str(file))
        return

    if not file.is_file():
        logger.warning("%s is not a file: skipping decompression", str(file))
        return

    # remove .gz suffix
    output_file = file.with_suffix("")
    if output_file.exists():
        logger.info("Found existing file %s: skipping decompression", output_file)
        return

    with gzip.open(file, "rb") as f_in, output_file.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    logger.info("Created output file %s", output_file)


def compress_files(directory: Path | str, file_glob: str) -> None:
    """Compress all files matching ``file_glob`` inside ``directory``.

    :param directory: directory to scan
    :type directory: Path | str
    :param file_glob: glob pattern to match
    :type file_glob: str
    :raises FileNotFoundError: if ``directory`` does not exist or is not a directory
    """
    if not isinstance(directory, Path):
        directory = Path(directory)

    if not directory.exists() or not directory.is_dir():
        msg = f"Directory {directory!s} not found: check the path is correct"
        raise FileNotFoundError(msg)

    to_compress = list(directory.glob(file_glob))
    logger.info("Found %d file(s) to compress", len(to_compress))
    if not to_compress:
        return

    for f in to_compress:
        compress_file(f)
    logger.info("Work complete!")


def compress_file(file: Path) -> None:
    """Compress ``file`` using gzip unless ``<file>.gz`` exists.

    :param file: file to compress
    :type file: Path
    """
    if Path(f"{file!s}.gz").exists():
        logger.info("Found existing file %s: skipping gz operation", str(file) + GZIP_SUFFIX)
        return

    with file.open("rb") as f_in, gzip.open(str(file) + GZIP_SUFFIX, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    logger.info("Created output file %s.gz", str(file))
