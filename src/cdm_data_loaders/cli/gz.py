"""CLI entry point and settings for gzip file and directory compression."""

from pathlib import Path
from typing import Annotated, Final

from frozendict import frozendict
from pydantic import BeforeValidator, Field
from pydantic_settings import CliApp, SettingsConfigDict

from cdm_data_loaders.core.fields import FILE_GLOB, FileGlob
from cdm_data_loaders.core.path_fields import check_local_path
from cdm_data_loaders.core.settings import CdmDataLoadersBase, default_settings_with_shortcuts
from cdm_data_loaders.utils.gz import compress_file, compress_files

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


def main(cli_args: list[str] | None = None) -> None:
    """Compress a file or directory from the command line.

    :param cli_args: command-line arguments; defaults to ``sys.argv[1:]`` when ``None``
    :type cli_args: list[str] | None
    :raises RuntimeError: if ``source`` does not exist
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
