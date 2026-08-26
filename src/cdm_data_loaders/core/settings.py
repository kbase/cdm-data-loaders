"""Common defaults for running pipelines on the KBase CTS."""

import logging
from collections.abc import Hashable
from typing import Any, Self

import dlt
from frozendict import frozendict
from pydantic import AliasChoices, AliasPath, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from cdm_data_loaders.core.fields import (
    BUFFER_SIZE,
    DEFAULTS,
    DEV_MODE,
    INPUT_DIR,
    LOG_CONFIG_FILE,
    LOG_INTERVAL,
    OUTPUT,
    START_AT,
    USE_DESTINATION,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
    BufferSize,
    DevMode,
    DltConfig,
    InputDir,
    LogConfigFile,
    LogInterval,
    Output,
    StartAt,
    UseDestination,
    UseOutputDirForPipelineMetadata,
)

logger = logging.getLogger(__name__)


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
    {**DEFAULT_CTS_SETTINGS, **{k: DEFAULTS[k] for k in [BUFFER_SIZE, LOG_INTERVAL, START_AT]}}
)


DEFAULT_SETTINGS_CONFIG_DICT = frozendict(
    {
        "cli_parse_args": True,
        "cli_exit_on_error": False,
        "cli_ignore_unknown_args": True,
        "str_strip_whitespace": True,
    }
)


def _alias_key(alias: str | AliasPath) -> Hashable:
    """Convert a validation alias into a hashable, structurally comparable key.

    A plain string alias is returned unchanged. An AliasPath is converted to a
    tuple of its `path` elements, which is hashable and equal to the tuple
    produced by any other AliasPath with an identical `path`. A tuple key is
    never equal to a string key, so string aliases and AliasPath aliases cannot
    be mistaken for one another.
    """
    if isinstance(alias, AliasPath):
        return tuple(alias.path)
    return alias


def _format_alias_key(key: Hashable) -> str:
    """Render an alias key for inclusion in the collision error message."""
    if isinstance(key, tuple):
        return f"AliasPath{list(key)}"
    return str(key)


class CdmDataLoadersBase(BaseSettings):
    """Base for all CDM Data Loaders settings classes.

    - Sets up some basic CLI parsing defaults
    - Adds a class method to check for conflicting aliases
    """

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT)

    @model_validator(mode="before")
    @classmethod
    def check_aliases(cls, data: Any) -> Any:  # noqa: ANN401
        """Ensure there is no overlap in the aliases used, including AliasPath aliases.

        AliasPath aliases are compared by their `path` attribute. Two AliasPath
        instances are treated as the same alias if and only if their `path` values
        are equal. A plain string alias is never treated as equal to an AliasPath
        alias.
        """
        all_aliases: dict[Hashable, list[str]] = {}
        for field_name, field_info in cls.model_fields.items():
            name_alias_set: set[Hashable] = {field_name}
            validation_alias = field_info.validation_alias
            if validation_alias is not None:
                if isinstance(validation_alias, AliasChoices):
                    name_alias_set.update(_alias_key(va) for va in validation_alias.choices)
                else:
                    name_alias_set.add(_alias_key(validation_alias))

            for alias in name_alias_set:
                all_aliases.setdefault(alias, []).append(field_name)

        field_collisions = {k: v for k, v in all_aliases.items() if len(v) > 1}
        if field_collisions:
            err_msg = "The following aliases are used more than once:\n" + "\n".join(
                sorted([f"{_format_alias_key(k)}: {', '.join(sorted(v))}" for k, v in field_collisions.items()])
            )
            raise ValueError(err_msg)
        return data


class LoggerSettings(CdmDataLoadersBase):
    """Configuration for a class with a logger config."""

    log_config_file: LogConfigFile


class InputOutputSettings(LoggerSettings):
    """Configuration with basic input and output settings."""

    input_dir: InputDir
    output: Output

    @field_validator("input_dir", "output", mode="after")
    @classmethod
    def validate_dir_path(cls, value: str) -> str:
        """Remove any trailing slashes from directory paths."""
        if value == "/":
            return value
        return value.rstrip("/")


class CtsSettings(InputOutputSettings):
    """Configuration for running a basic DLT pipeline."""

    dev_mode: DevMode
    use_destination: UseDestination
    use_output_dir_for_pipeline_metadata: UseOutputDirForPipelineMetadata

    _dlt_config: DltConfig

    @model_validator(mode="after")
    def init_dlt_config(self) -> Self:
        """Initialise the _dlt_config private attribute."""
        self._dlt_config = dlt.config
        return self

    @model_validator(mode="after")
    def reconcile_with_dlt_config(self) -> Self:
        """Update dlt.config based on the current state of the settings."""
        if self._dlt_config is None:
            err_msg = "dlt_config must be defined"
            raise ValueError(err_msg)

        # validate destination
        all_destinations = self._dlt_config.get("destination") or {}  # type: ignore[reportArgumentType]
        if not all_destinations:
            err_msg = "No valid destinations found in dlt configuration."
            raise ValueError(err_msg)

        if self.use_destination not in all_destinations:
            err_msg = f"use_destination must be one of {sorted(all_destinations)}, got '{self.use_destination}'"
            raise ValueError(err_msg)

        if not self.output:
            if not self._dlt_config.get(f"destination.{self.use_destination}.bucket_url"):  # type: ignore[reportArgumentType]
                err_msg = f"No bucket_url specified for destination {self.use_destination}"
                raise ValueError(err_msg)

            self.output = self._dlt_config[f"destination.{self.use_destination}.bucket_url"]
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
    buffer_size: BufferSize
    log_interval: LogInterval
