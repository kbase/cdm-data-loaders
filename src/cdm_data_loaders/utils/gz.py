"""File compression utils."""

import gzip
import shutil
from logging import Logger, getLogger
from pathlib import Path
from typing import Annotated, Final

from frozendict import frozendict
from pydantic import BeforeValidator, Field
from pydantic_settings import CliApp, SettingsConfigDict

from cdm_data_loaders.core.fields import FILE_GLOB, GZIP_SUFFIX, FileGlob
from cdm_data_loaders.core.path_fields import check_local_path
from cdm_data_loaders.core.settings import CdmDataLoadersBase, default_settings_with_shortcuts

logger: Logger = getLogger(__name__)

GZ_CLI_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    {
        "source": "i",
        FILE_GLOB: "f",
    }
)


class GzSettings(CdmDataLoadersBase):
    """Settings for compressing a file or directory from the command line."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(cli_shortcuts=GZ_CLI_SHORTCUTS)

    source: Annotated[
        Path,
        Field(description="Input file(s) to process"),
        BeforeValidator(check_local_path),
    ]
    file_glob: FileGlob


def decompress_file(file: str | Path) -> None:
    """Decompress a gzip file.

    :param file: file to decompress
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
    """Compress all files matching a certain pattern in a directory.

    :param directory: directory to look in
    :type directory: Path | str
    :param file_glob: pattern to match
    :type file_glob: str
    :raises ValueError: if the directory is not found
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
    """Compress a file using gzip.

    :param file: file to compress
    :type file: Path
    """
    if Path(f"{file!s}.gz").exists():
        logger.info("Found existing file %s: skipping gz operation", str(file) + GZIP_SUFFIX)
        return

    with file.open("rb") as f_in, gzip.open(str(file) + GZIP_SUFFIX, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    logger.info("Created output file %s.gz", str(file))


def main(cli_args: list[str] | None = None) -> None:
    """Compress a file or directory from the command line.

    :param cli_args: command-line arguments; defaults to ``sys.argv[1:]`` when None
    :type cli_args: list[str] | None
    :raises RuntimeError: if the source path does not exist
    """
    settings = CliApp.run(GzSettings, cli_args=cli_args)
    if not settings.source.exists():
        msg = f"Path {settings.source} does not exist"
        raise RuntimeError(msg)

    if settings.source.is_dir():
        compress_files(settings.source, settings.file_glob or "*")
    else:
        compress_file(settings.source)


if __name__ == "__main__":
    main()
