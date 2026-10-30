"""Resolve where a pipeline reads its input files from: the local input_dir or a dlt filesystem source.

A dlt filesystem source is a ``[sources.<name>]`` block in the dlt config, selected with the
``use_resource`` setting::

    [sources.my_source]
    resource_type = "filesystem"
    bucket_url = "s3://my-bucket/some/prefix"

    # secrets.toml
    [sources.my_source.credentials]
    aws_access_key_id = "..."
    aws_secret_access_key = "..."
    endpoint_url = "https://..."
"""

from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Any, Final

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.storages.fsspec_filesystem import fsspec_filesystem
from dlt.extract import DltResource
from dlt.sources.filesystem import filesystem
from dlt.sources.filesystem.helpers import FilesystemConfigurationResource
from fsspec.core import split_protocol

from cdm_data_loaders.core.settings import CtsSettings

logger: Logger = getLogger(__name__)

SOURCES_SECTION: Final[str] = "sources"
RESOURCE_TYPE_KEY: Final[str] = "resource_type"
FILESYSTEM_RESOURCE_TYPE: Final[str] = "filesystem"
SOURCE_NOT_CONFIGURED: Final[str] = (
    "Source {name!r} is not configured: set sources.{name}.resource_type and sources.{name}.bucket_url in the dlt config."
)
SOURCE_TYPE_UNSUPPORTED: Final[str] = (
    "Source {name!r} has unsupported resource_type {resource_type!r}; only 'filesystem' is supported."
)
SOURCE_INVALID: Final[str] = "Invalid source configuration for {name!r}"


@dataclass(frozen=True, slots=True)
class InputSource:
    """Location and credentials of the input files for a pipeline.

    :param bucket_url: fsspec URL or local path of the directory holding the input files
    :param credentials: filesystem credentials, if any
    :param kwargs: extra arguments for the fsspec filesystem constructor
    :param client_kwargs: extra arguments for the fsspec filesystem's native client
    """

    bucket_url: str
    credentials: Any = None
    kwargs: dict[str, Any] | None = None
    client_kwargs: dict[str, Any] | None = None

    @property
    def protocol(self) -> str:
        """Lower-case protocol of ``bucket_url``; plain paths count as 'file'."""
        protocol, _ = split_protocol(self.bucket_url)
        return (protocol or "file").lower()

    def join(self, subdir: str | None) -> str:
        """Return ``bucket_url`` with ``subdir`` appended, using '/' as the separator."""
        if not subdir:
            return self.bucket_url
        return f"{self.bucket_url.rstrip('/')}/{subdir.strip('/')}"


def resolve_input_source(settings: CtsSettings) -> InputSource:
    """Resolve the input source for a pipeline.

    If ``settings.use_resource`` is set, the ``[sources.<use_resource>]`` block in the dlt config is
    resolved (including protocol-specific credentials from the dlt secrets); otherwise, the local
    ``settings.input_dir`` is used.

    :param settings: pipeline settings
    :type  settings: CtsSettings
    :raises ValueError: if the named source is not configured, not a filesystem source, or invalid
    :return: the input source
    :rtype: InputSource
    """
    name = settings.use_resource
    if name is None:
        return InputSource(bucket_url=str(settings.input_dir))

    resource_type = dlt.config.get(f"{SOURCES_SECTION}.{name}.{RESOURCE_TYPE_KEY}")
    if resource_type is None:
        raise ValueError(SOURCE_NOT_CONFIGURED.format(name=name))
    if str(resource_type) != FILESYSTEM_RESOURCE_TYPE:
        raise ValueError(SOURCE_TYPE_UNSUPPORTED.format(name=name, resource_type=resource_type))

    try:
        config = resolve_configuration(FilesystemConfigurationResource(), sections=(SOURCES_SECTION, name))
    except ConfigurationException as e:
        err_msg = SOURCE_INVALID.format(name=name)
        logger.exception(err_msg)
        raise ValueError(err_msg) from e

    return InputSource(
        bucket_url=config.bucket_url,
        credentials=config.credentials,
        kwargs=config.kwargs,
        client_kwargs=config.client_kwargs,
    )


def build_input_files(settings: CtsSettings, *, file_glob: str, subdir: str | None = None) -> DltResource:
    """Build a dlt ``filesystem`` resource listing the input files that match ``file_glob``.

    :param settings: pipeline settings
    :type  settings: CtsSettings
    :param file_glob: glob pattern for files, relative to the input location (and ``subdir``)
    :type  file_glob: str
    :param subdir: optional subdirectory of the input location to list files from
    :type  subdir: str | None
    :return: dlt filesystem resource yielding pages of file items
    :rtype: DltResource
    """
    source = resolve_input_source(settings)
    fs_args: dict[str, Any] = {"bucket_url": source.join(subdir), "file_glob": file_glob}
    if source.credentials is not None:
        fs_args["credentials"] = source.credentials
    if source.kwargs is not None:
        fs_args["kwargs"] = source.kwargs
    if source.client_kwargs is not None:
        fs_args["client_kwargs"] = source.client_kwargs
    return filesystem(**fs_args)


def input_subdir_exists(settings: CtsSettings, subdir: str) -> bool:
    """Whether ``subdir`` exists as a directory (or, for object stores, a key prefix) in the input location.

    :param settings: pipeline settings
    :type  settings: CtsSettings
    :param subdir: subdirectory of the input location
    :type  subdir: str
    :return: True if the subdirectory exists
    :rtype: bool
    """
    source = resolve_input_source(settings)
    # despite its parameter name, fsspec_filesystem expects the full bucket URL, from which it derives the protocol
    fs_client, _ = fsspec_filesystem(
        source.bucket_url, source.credentials, kwargs=source.kwargs, client_kwargs=source.client_kwargs
    )
    return bool(fs_client.isdir(source.join(subdir)))
