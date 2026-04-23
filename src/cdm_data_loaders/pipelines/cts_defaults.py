"""Common defaults for running pipelines on the KBase CTS."""

from pathlib import Path
from typing import Any, Self

import dlt.common.configuration.accessors
from frozendict import frozendict
from pydantic import AliasChoices, Field, ValidationError, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, CliSuppress, SettingsConfigDict

INPUT_MOUNT = "/input_dir"
OUTPUT_MOUNT = "/output_dir"

VALID_DESTINATIONS = ["local_fs", "s3"]
DEFAULT_BATCH_SIZE = 50
DEFAULT_START_AT = 0

# TODO: frozendict can be moved to the stdlib implementation when py 3.15 is released.


DEFAULT_CTS_SETTINGS = frozendict(
    {
        "dev_mode": False,
        "input_dir": INPUT_MOUNT,
        # N.b. this gets replaced by destination.local_fs.bucket_url
        "output": "",
        "use_destination": "local_fs",
        "use_output_dir_for_pipeline_metadata": False,
    }
)

DEFAULT_BATCH_FILE_SETTINGS = frozendict(
    {
        **DEFAULT_CTS_SETTINGS,
        "start_at": DEFAULT_START_AT,
    }
)


ARG_ALIASES = frozendict(
    {
        "batch_size": ["-b", "--batch_size", "--batch-size"],
        "dev_mode": ["--dev_mode", "--dev-mode", "--dev"],
        "input_dir": ["-i", "--input_dir", "--input-dir"],
        "output": ["-o", "--output"],
        "start_at": ["-s", "--start_at", "--start-at"],
        "use_destination": ["-d", "--use_destination", "--use-destination"],
        "use_output_dir_for_pipeline_metadata": [
            "-p",
            "--use_output_dir_for_pipeline_metadata",
            "--use-output-dir-for-pipeline-metadata",
        ],
    }
)

DEFAULT_SETTINGS_CONFIG_DICT = frozendict(
    {
        "cli_parse_args": True,
        "cli_exit_on_error": False,
        "cli_ignore_unknown_args": True,
        "str_strip_whitespace": True,
    }
)


class CtsSettings(BaseSettings):
    """Configuration for running a basic import pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT)

    dev_mode: bool = Field(
        default=DEFAULT_CTS_SETTINGS["dev_mode"],
        description="Whether to run the pipeline in dev mode, which saves raw API responses to disk and disables compression for easier debugging.",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["dev_mode"]]),
    )
    input_dir: str = Field(
        default=DEFAULT_CTS_SETTINGS["input_dir"],
        description="Location of directory containing file(s) to import",
        # explicitly allow both kebab case and snake case
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["input_dir"]]),
    )
    output: str = Field(
        default=DEFAULT_CTS_SETTINGS["output"],
        description="Location to save imported data to, if different from the default supplied by the destination config",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["output"]]),
    )
    use_destination: str = Field(
        default=DEFAULT_CTS_SETTINGS["use_destination"],
        description=f"DLT destination configuration to use for data output. Choices: {VALID_DESTINATIONS}",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["use_destination"]]),
    )
    use_output_dir_for_pipeline_metadata: bool = Field(
        default=DEFAULT_CTS_SETTINGS["use_output_dir_for_pipeline_metadata"],
        description="If true, use the output directory for pipeline metadata.",
        validation_alias=AliasChoices(
            *[alias.strip("-") for alias in ARG_ALIASES["use_output_dir_for_pipeline_metadata"]]
        ),
    )
    # this should really just be _Accessor but leaving it as-is for ease of testing
    dlt_config: CliSuppress[dlt.common.configuration.accessors._Accessor | dict[str, Any]] = Field(
        description="DLT configuration for the pipeline.",
    )

    @model_validator(mode="after")
    def reconcile_with_dlt_config(self) -> Self:
        """Update dlt.config based on the current state of the settings."""
        # validate destination
        all_destinations = self.dlt_config.get("destination") or {}  # type: ignore[reportArgumentType]
        if not all_destinations:
            err_msg = "No valid destinations found in dlt configuration."
            raise ValueError(err_msg)

        if self.use_destination not in all_destinations:
            err_msg = f"use_destination must be one of {sorted(all_destinations)}, got '{self.use_destination}'"
            raise ValueError(err_msg)

        if not self.output:
            if not self.dlt_config.get(f"destination.{self.use_destination}.bucket_url"):  # type: ignore[reportArgumentType]
                err_msg = f"No bucket_url specified for destination {self.use_destination}"
                raise ValueError(err_msg)

            self.output = self.dlt_config[f"destination.{self.use_destination}.bucket_url"]
            if self.output != "/":
                self.output.rstrip("/")

        if not self.output:
            raise ValueError("No output specified")

        # ensure that the use_destination value does not conflict with whether or not pipeline data should be saved
        destination_is_s3 = False
        if self.output.startswith("s3://") or self.output.startswith("s3a://"):
            destination_is_s3 = True

        if self.use_output_dir_for_pipeline_metadata and destination_is_s3:
            err_msg = "It is not currently possible to have the pipeline directory on s3."
            raise ValueError(err_msg)

        return self

    @field_validator("dlt_config", mode="before")
    @classmethod
    def validate_dlt_config(cls, dlt_config: Any) -> Any:
        """Perform some rudimentary validation on the incoming dlt config."""
        if dlt_config is None:
            err_msg = "dlt_config must be defined"
            raise ValueError(err_msg)

        return dlt_config

    @field_validator("input_dir", "output", mode="after")
    @classmethod
    def validate_dir_path(cls, value: str) -> str:
        """Remove any trailing slashes from directory paths."""
        if value == "/":
            return value
        return value.rstrip("/")

    @computed_field
    @property
    def raw_data_dir(self) -> str:
        """Directory in which to save the raw data files that are downloaded.

        If not set, defaults to a 'raw_data' directory within the output directory after reconciling with dlt config.
        """
        return f"{self.output}{'' if self.output in ('', '/') else '/'}raw_data"

    @computed_field
    @property
    def pipeline_dir(self) -> str | None:
        """Custom directory to save pipeline metadata to.

        If use_output_dir_for_pipeline_metadata is true, this defaults to a `.dlt_conf` directory within the output directory.
        """
        if self.use_output_dir_for_pipeline_metadata:
            return f"{self.output}{'' if self.output in ('', '/') else '/'}.dlt_conf"
        return None


class BatchedFileInputSettings(CtsSettings):
    """Settings object for an importer that deals with batches of files."""

    start_at: int = Field(
        default=DEFAULT_BATCH_FILE_SETTINGS["start_at"],
        description="File to start import at",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["start_at"]]),
    )
