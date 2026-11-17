"""Settings for the AllTheBacteria bulk download pipeline."""

import re
from functools import cached_property
from pathlib import Path
from typing import ClassVar, Final, Self

from pydantic import Field, FilePath, computed_field, model_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import DestinationFileFormat, NonEmptyStr
from cdm_data_loaders.core.settings import (
    FILE_FORMAT_CLI_SHORTCUTS,
    FileFormatIngestSettings,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.datasets.all_the_bacteria.osf import (
    ATB_VERSION,
    DATASET_NAME,
    PROJECT_PART_REGEX,
    load_patterns,
)

VERSION: Final[str] = "version"


class AtbSettings(FileFormatIngestSettings):
    """Configuration for running the AllTheBacteria import pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=DATASET_NAME,
        cli_shortcuts={VERSION: "v", **FILE_FORMAT_CLI_SHORTCUTS},
    )

    dataset_name: ClassVar[str] = DATASET_NAME

    loader_file_format: DestinationFileFormat  # type: ignore[assignment]

    version: NonEmptyStr = Field(
        default=ATB_VERSION,
        description="Name of the AllTheBacteria version to download",
    )

    pattern_file: FilePath | None = Field(
        default=None,
        description="Path to a file containing patterns to match when downloading ATB files",
    )

    @model_validator(mode="after")
    def post_init_checks(self) -> Self:
        """Validate ``pattern_file`` regex compilation during model initialization.

        :return: validated settings instance
        :rtype: Self
        """
        _ = self.pattern_matches
        return self

    @computed_field
    @cached_property
    def raw_data_dir(self) -> Path:
        """Directory to save downloaded raw data files to: ``<output_dir>/raw_data/<version>``.

        :return: versioned raw data output directory
        :rtype: Path
        """
        return self.output_dir / "raw_data" / self.version

    @computed_field
    @cached_property
    def pattern_matches(self) -> re.Pattern[str]:
        """Compiled regular expression used to select ATB files for download.

        :return: compiled regular expression pattern
        :rtype: re.Pattern[str]
        :raises ValueError: if ``pattern_file`` is supplied but yields no valid patterns
        """
        if self.pattern_file:
            regex = load_patterns(self.pattern_file)
            if regex is None:
                err_msg = f"No patterns found in {self.pattern_file!s}"
                raise ValueError(err_msg)
            return regex
        return PROJECT_PART_REGEX
