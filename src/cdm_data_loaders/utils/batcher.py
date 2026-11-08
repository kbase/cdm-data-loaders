"""Utilities for splitting a selection into batches."""

import re
from logging import Logger, getLogger
from typing import Annotated, Final, Self

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.storages.fsspec_filesystem import FileItemDict, fsspec_filesystem, glob_files
from dlt.sources.credentials import FileSystemCredentials
from dlt.sources.filesystem.helpers import FilesystemConfigurationResource
from fsspec import AbstractFileSystem
from pydantic import BaseModel, Field, model_validator

from cdm_data_loaders.core.fields import MIN_START_AT
from cdm_data_loaders.core.path_fields import InputDir

# Matches files like: name_00001.ext or name_00001.ext.gz
FILE_NAME_REGEX: re.Pattern[str] = re.compile(r"^\w+_(\d+)(\.\w+)+$")

MIN_BATCH_SIZE: Final[int] = 1
MIN_END_AT: Final[int] = 0
DEFAULT_PIPELINE_BATCH_SIZE: Final[int] = 50

logger: Logger = getLogger(__name__)


@with_config(
    spec=FilesystemConfigurationResource,
    sections=(known_sections.SOURCES, "filesystem"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
)
def _get_fs_client(
    bucket_url: str,
    credentials: FileSystemCredentials | AbstractFileSystem = dlt.secrets.value,
) -> AbstractFileSystem:
    """Instantiate an fsspec filesystem client for ``bucket_url`` using dlt filesystem config."""
    if isinstance(credentials, AbstractFileSystem):
        return credentials
    return fsspec_filesystem(bucket_url, credentials)[0]


class NumericFileSequenceBatcher(BaseModel):
    """A batcher that can be used to retrieve batches of files from a directory or fsspec URL.

    Searches the directory for files with names ending in `_[0-9]+`
    (underscore followed by one or more digits) with one or more extensions (e.g. `.gz`, `.txt`, `.tar.gz`).

    Assumes that file names only contain alphanumeric characters, underscore, and extension(s).

    Re-scans the directory each time the next batch is requested.

    :param directory:   local directory or fsspec URL to retrieve files from
    :type  directory:   InputDir, required
    :param batch_size:  number of files to return per invocation, defaults to 1
    :type  batch_size:  int, optional
    :param start_at:    file number to start at, defaults to 1.
                        Must be greater than or equal to 1.
    :type  start_at:    int, optional
    :param end_at:      file number to end at, inclusive (i.e. if set to 15, file_0015.txt will be the last file)
                        If set to 0, the end_at parameter is ignored -- i.e. there is no maximum value.
                        Must be greater than or equal to 0
                        Defaults to 0
    :type  end_at:      int, optional
    :param file_regex:  pattern that the file names should match. Capture group 1 is expected to be numeric.
    :type  file_regex:  re.Pattern, optional
    """

    directory: InputDir
    batch_size: Annotated[int, Field(ge=MIN_BATCH_SIZE, strict=True)] = MIN_BATCH_SIZE
    start_at: Annotated[int, Field(ge=MIN_START_AT, strict=True)] = MIN_START_AT
    end_at: Annotated[int, Field(ge=MIN_END_AT, strict=True)] = MIN_END_AT
    file_regex: re.Pattern[str] = Field(default=FILE_NAME_REGEX)

    @model_validator(mode="after")
    def check_start_at_end_at(self) -> Self:
        """Ensure that the end_at value, if set and non-zero, is greater than the start_at value.

        :raises ValueError: if end_at is set and start_at exceeds end_at
        :return: self
        :rtype: Self
        """
        if self.end_at != 0 and self.end_at < self.start_at:
            err_msg = "end_at must be greater than start_at"
            raise ValueError(err_msg)
        return self

    def _get_sequence_number(self, item: FileItemDict) -> int:
        match = self.file_regex.match(item["file_name"])
        return int(match.group(1))  # pyright: ignore[reportOptionalMemberAccess]

    def get_batch(self) -> list[FileItemDict]:
        """Return the next `batch_size` files whose sequence number >= start_at.

        Re-scans the directory on every call to pick up newly added files and
        updates `start_at` to the next file in the directory list.
        """
        if self.end_at and self.start_at > self.end_at:
            return []

        fs_client = _get_fs_client(self.directory)
        fs_client.invalidate_cache()
        matched = sorted(
            (
                FileItemDict(item, fs_client)
                for item in glob_files(fs_client, self.directory, file_glob="*")
                if self.file_regex.match(item["file_name"])
            ),
            key=lambda item: item["file_name"],
        )
        if not matched:
            logger.warning("No matching files found in %s", str(self.directory))
            return []

        eligible = [
            item
            for item in matched
            if self._get_sequence_number(item) >= self.start_at
            and (not self.end_at or self._get_sequence_number(item) <= self.end_at)
        ]

        batch = eligible[: self.batch_size]
        if batch:
            self.start_at = self._get_sequence_number(batch[-1]) + 1

        return batch
