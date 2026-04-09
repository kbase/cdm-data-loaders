"""Common defaults for running pipelines on the KBase CTS."""

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

INPUT_MOUNT = "/input_dir"
OUTPUT_MOUNT = "/output_dir"

VALID_DESTINATIONS = ["local_fs", "s3"]
DEFAULT_BATCH_SIZE = 50


class CtsDefaultSettings(BaseSettings):
    """Configuration for running a basic import pipeline."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_exit_on_error=False,
        cli_ignore_unknown_args=True,
    )
    input_dir: str = Field(
        default=INPUT_MOUNT,
        description="Location of directory containing UniRef XML files to import",
        # explicitly allow both kebab case and snake case
        validation_alias=AliasChoices("i", "input_dir", "input-dir", "input_dir"),
    )
    destination: str = Field(
        default="local_fs",
        description=f"Destination configuration to use for data output. Choices: {VALID_DESTINATIONS}",
        validation_alias=AliasChoices("d", "destination"),
    )
    output: str | None = Field(
        default=None,
        description="Location to save imported data to, if different from the default supplied by the destination config",
        validation_alias=AliasChoices("o", "output"),
    )

    @field_validator("destination")
    @classmethod
    def validate_destination(cls, v: str) -> str:
        """Validate the destination against valid choices.

        :param v: destination specified
        :type v: str
        :raises ValueError: if the destination is not valid
        :return: valid destination
        :rtype: str
        """
        if v not in VALID_DESTINATIONS:
            err_msg = f"destination must be one of {VALID_DESTINATIONS}, got '{v}'"
            raise ValueError(err_msg)
        return v


class BatchedFileInputSettings(CtsDefaultSettings):
    """Settings object for an importer that deals with batches of files."""

    start_at: int = Field(
        default=0,
        description="File to start import at",
        validation_alias=AliasChoices("s", "start", "start-at", "start_at"),
    )
