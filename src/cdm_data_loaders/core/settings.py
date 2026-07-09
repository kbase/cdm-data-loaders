"""Common defaults for running pipelines on the KBase CTS."""

from typing import Any, Self

from frozendict import frozendict
from pydantic import computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from cdm_data_loaders.core.fields import (
    DEFAULTS,
    DEV_MODE,
    INPUT_DIR,
    LOG_CONFIG_FILE,
    OUTPUT,
    START_AT,
    USE_DESTINATION,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
    DevMode,
    DltConfig,
    InputDir,
    LogConfigFile,
    Output,
    StartAt,
    UseDestination,
    UseOutputDirForPipelineMetadata,
)

DEFAULT_SETTINGS_CONFIG_DICT = frozendict(
    {
        "cli_parse_args": True,
        "cli_exit_on_error": False,
        "cli_ignore_unknown_args": True,
        "str_strip_whitespace": True,
    }
)


DEFAULT_CTS_SETTINGS = frozendict(
    {
        k: DEFAULTS[k]
        for k in [
            DEV_MODE,
            INPUT_DIR,
            LOG_CONFIG_FILE,
            OUTPUT,
            USE_DESTINATION,
            USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
        ]
    }
)

DEFAULT_BATCH_FILE_SETTINGS = frozendict(
    {
        **DEFAULT_CTS_SETTINGS,
        START_AT: DEFAULTS[START_AT],
    }
)


class LoggerSettings(BaseSettings):
    """Configuration for a class with a logger config."""

    log_config_file: LogConfigFile


class CtsSettings(LoggerSettings):
    """Configuration for running a basic import pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT)

    dev_mode: DevMode
    input_dir: InputDir
    output: Output
    use_destination: UseDestination
    use_output_dir_for_pipeline_metadata: UseOutputDirForPipelineMetadata
    dlt_config: DltConfig

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

        # N.b. this should never happen
        if not self.output:
            err_msg = "No output specified!"
            raise ValueError(err_msg)

        # ensure that the use_destination value does not conflict with whether or not pipeline data should be saved
        destination_is_s3 = False
        if self.output.startswith("s3://") or self.output.startswith("s3a://"):
            destination_is_s3 = True

        # self.use_destination should be "s3" if the output is an s3 url and vice versa
        if bool(self.use_destination == "s3") != destination_is_s3:
            err_msg = "Mismatch between output location and use_destination. To ensure internal settings functions work correctly, set use_destination to 's3' for writing files to s3, and 'local_fs' for writing files locally. The output directory can be configured using the 'output' parameter."
            raise ValueError(err_msg)

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

    start_at: StartAt
