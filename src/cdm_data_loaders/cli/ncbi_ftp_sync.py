"""CLI interface for downloading NCBI FTP assemblies listed in a transfer manifest (Phase 2)."""

from logging import Logger, getLogger
from pathlib import Path
from typing import Final

from pydantic import Field, PositiveInt
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import INPUT_MOUNT, OUTPUT_MOUNT
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.datasets.ncbi_ftp.constants import FTP_HOST
from cdm_data_loaders.datasets.ncbi_ftp.download import download_batch

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "ncbi_ftp_sync"
NCBI_FTP_SYNC_CLI_SHORTCUTS: Final[dict[str, str]] = {
    "manifest": "m",
    "threads": "t",
    "limit": "l",
}


class DownloadSettings(LoggerSettings):
    """Configuration for the NCBI FTP assembly download CLI."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=NCBI_FTP_SYNC_CLI_SHORTCUTS,
    )

    manifest: Path = Field(
        default=Path(INPUT_MOUNT) / "transfer_manifest.txt",
        description="Path to the transfer manifest file listing FTP paths to download",
    )
    output_dir: Path = Field(
        default=Path(OUTPUT_MOUNT),
        description="Output directory for downloaded assembly files",
    )
    threads: PositiveInt = Field(
        default=4,
        le=32,
        description="Number of parallel download threads",
    )
    ftp_host: str = Field(
        default=FTP_HOST,
        description="NCBI FTP hostname",
    )
    limit: PositiveInt | None = Field(
        default=None,
        description="Limit to first N assemblies (for testing)",
    )


def run_download(config: DownloadSettings) -> None:
    """Execute Phase 2 NCBI FTP batch download and log any failed assemblies.

    Returns exit code 0 after writing ``download_report.json`` so KBase CTS containers
    preserve output directory artefacts when individual assembly downloads fail.

    :param config: validated download settings
    :type config: DownloadSettings
    """
    report = download_batch(
        manifest_path=config.manifest,
        output_dir=config.output_dir,
        threads=config.threads,
        ftp_host=config.ftp_host,
        limit=config.limit,
    )
    if report["failed"] > 0:
        logger.error("Download completed with %d failures", report["failed"])


cli = CliRunner(DownloadSettings, run_download)


if __name__ == "__main__":
    cli()
