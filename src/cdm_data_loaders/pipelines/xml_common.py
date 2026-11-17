"""Shared XML CLI shortcuts, globs, field types, and base settings for XML ingestion pipelines."""

from typing import Annotated, Final

from frozendict import frozendict
from pydantic import Field, FilePath
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    FileGlob,
    NonEmptyStr,
    TableName,
    file_glob_description,
)
from cdm_data_loaders.core.settings import (
    FILE_FORMAT_CLI_SHORTCUTS,
    FileFormatIngestSettings,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.core.transforms.settings_mixin import TransformSettingsMixin

DEFAULT_XML_FILE_GLOB: Final[str] = "*.xml*"
XML_GZ_FILE_GLOB: Final[str] = "*.xml.gz"

XML_CLI_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    {
        **FILE_FORMAT_CLI_SHORTCUTS,
        FILE_GLOB: "g",
    }
)

XmlTag = Annotated[
    NonEmptyStr,
    Field(
        description="XML tag to capture the contents of",
    ),
]
XsdFile = Annotated[
    FilePath,
    Field(
        description="Path to the XSD file describing the XML documents to be imported.",
    ),
]


class XmlIngestSettings(FileFormatIngestSettings):
    """Configuration for DLT pipelines that import XML files."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(cli_shortcuts=XML_CLI_SHORTCUTS)

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_XML_FILE_GLOB,
            description=file_glob_description("Glob pattern for XML files"),
        ),
    ]


class SingleTableXmlIngestSettings(XmlIngestSettings, TransformSettingsMixin):
    """Configuration for DLT pipelines that import XML elements into a single target table."""

    table_name: TableName
    xml_tag: XmlTag
