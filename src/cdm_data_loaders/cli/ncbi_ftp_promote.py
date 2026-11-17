"""CLI interface for promoting staged NCBI FTP files to the Lakehouse (Phase 3)."""

from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any, Final

from pydantic import Field
from pydantic_settings import CliImplicitFlag, SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.datasets.ncbi_ftp.constants import (
    DEFAULT_DESTINATION_BUCKET,
    DEFAULT_DESTINATION_PREFIX,
    DEFAULT_STAGING_BUCKET,
    DEFAULT_TRANSFER_MANIFEST_FILE,
)
from cdm_data_loaders.datasets.ncbi_ftp.promote import promote_from_s3

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "ncbi_ftp_promote"
NCBI_FTP_PROMOTE_CLI_SHORTCUTS: Final[dict[str, str]] = {
    "staging_path": "s",
    "removed_manifest_path": "r",
    "updated_manifest_path": "u",
    "transfer_manifest_path": "t",
}


def _default_transfer_manifest_path(validated_data: dict[str, Any]) -> PurePosixPath | None:
    """Derive the default S3 transfer manifest path from ``staging_path``.

    :param validated_data: dictionary of already-validated sibling field values
    :type validated_data: dict[str, Any]
    :return: ``staging_path / DEFAULT_TRANSFER_MANIFEST_FILE`` when ``staging_path`` is present, else ``None``
    :rtype: PurePosixPath | None
    """
    staging_path = validated_data.get("staging_path")
    if isinstance(staging_path, PurePosixPath):
        return staging_path / DEFAULT_TRANSFER_MANIFEST_FILE
    return None


class PromoteSettings(LoggerSettings):
    """Configuration for the NCBI FTP file promotion CLI."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=NCBI_FTP_PROMOTE_CLI_SHORTCUTS,
    )

    staging_bucket: PurePosixPath = Field(
        default=DEFAULT_STAGING_BUCKET,
        description="Bucket where staged files reside after download",
    )
    destination_bucket: PurePosixPath = Field(
        default=DEFAULT_DESTINATION_BUCKET,
        description="Bucket to which staged files will be promoted",
    )
    staging_path: PurePosixPath = Field(
        description="Path to folder in the staging bucket containing `raw_data/`",
    )
    destination_path: PurePosixPath = Field(
        default=DEFAULT_DESTINATION_PREFIX,
        description="Path to folder in the destination bucket that will hold `raw_data/`",
    )
    removed_manifest_path: Path | None = Field(
        default=None,
        description="Local path to the Phase 1 removed files manifest, or None to skip archiving removed records",
    )
    updated_manifest_path: Path | None = Field(
        default=None,
        description="Local path to the Phase 1 updated files manifest, or None to skip archiving updated records",
    )
    transfer_manifest_path: PurePosixPath | None = Field(
        default_factory=_default_transfer_manifest_path,
        description="S3 object key of the transfer manifest to trim after promotion, or None to skip pruning",
    )
    dry_run: CliImplicitFlag[bool] = Field(
        default=False,
        description="Log actions without making changes",
    )


def run_promote(config: PromoteSettings) -> None:
    """Execute Phase 3 promotion from S3 staging to the Lakehouse and log any failures.

    Returns exit code 0 after writing the promotion report so KBase CTS containers
    preserve output artefacts when individual file promotions fail.

    :param config: validated promote settings
    :type config: PromoteSettings
    """
    report = promote_from_s3(
        staging_bucket=config.staging_bucket,
        staging_key_prefix=config.staging_path,
        lakehouse_bucket=config.destination_bucket,
        lakehouse_key_prefix=config.destination_path,
        removed_manifest_path=config.removed_manifest_path,
        updated_manifest_path=config.updated_manifest_path,
        manifest_s3_key=config.transfer_manifest_path,
        dry_run=config.dry_run,
    )
    if report["failed"] > 0:
        logger.error("Promote completed with %d failures", report["failed"])


cli = CliRunner(PromoteSettings, run_promote)


if __name__ == "__main__":
    cli()
