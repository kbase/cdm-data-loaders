"""CLI interface for generating static Python PyArrow schema modules from XSD schemas."""

from logging import Logger, getLogger
from pathlib import Path
from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, Field, FilePath, PositiveInt
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters.cli.iceberg_base import write_schema_text
from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import XsdToPyArrow
from cdm_data_loaders.core.fields import NonEmptyStr
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.pipelines.core import PipelineCli

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "xsd_to_pyarrow"

XSD_TO_PYARROW_CLI_SHORTCUTS: Final[dict[str, str]] = {
    "xsd_file": "x",
    "output": "o",
    "xml_level": "v",
    "xml_tag": "m",
    "root_tag": "r",
}


class SchemaCodegenConfig(BaseModel):
    """Configuration for generating a component-based PyArrow ``schema.py`` module from an XSD file."""

    model_config = ConfigDict(frozen=True)

    xsd_file: Path
    output: Path
    docstring: str
    xml_level: int | None = 1
    xml_tag: str | None = None
    root_tag: str | None = None
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH


class XsdToPyArrowCliSettings(LoggerSettings):
    """Settings for generating a static Python PyArrow schema module from an XSD file."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=XSD_TO_PYARROW_CLI_SHORTCUTS,
    )

    xsd_file: Annotated[
        FilePath,
        Field(description="Path to the input XSD file"),
    ]
    output: Annotated[
        Path,
        Field(description="Destination Python file path for the generated schema module"),
    ]
    xml_level: Annotated[
        PositiveInt,
        Field(
            default=1,
            description="Element depth relative to root (default: 1)",
        ),
    ]
    xml_tag: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Single target XML tag (overrides --xml-level)",
        ),
    ]
    root_tag: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Explicit root XML tag for level discovery",
        ),
    ]
    max_recursion_depth: Annotated[
        PositiveInt,
        Field(
            default=DEFAULT_MAX_RECURSION_DEPTH,
            description="Maximum recursion depth before JSON string truncation",
        ),
    ]


def render_schema_module(config: SchemaCodegenConfig) -> str:
    """Render a complete, formatted Python schema module from ``config``.

    :param config: Code generation configuration.
    :type config: SchemaCodegenConfig
    :return: Formatted Python module source code.
    :rtype: str
    """
    converter = XsdToPyArrow(
        xml_tag=config.xml_tag,
        max_recursion_depth=config.max_recursion_depth,
    )
    if config.xml_tag is not None:
        return converter.convert_to_python(config.xsd_file, docstring=config.docstring)
    level = config.xml_level if config.xml_level is not None else 1
    return converter.convert_level_to_python(
        config.xsd_file,
        level,
        root_tag=config.root_tag,
        docstring=config.docstring,
    )


def generate_python_schema(settings: XsdToPyArrowCliSettings) -> None:
    """Generate and write a static Python PyArrow schema module from an XSD file.

    :param settings: XSD-to-PyArrow CLI settings.
    :type settings: XsdToPyArrowCliSettings
    """
    config = SchemaCodegenConfig(
        xsd_file=settings.xsd_file,
        output=settings.output,
        docstring=f"PyArrow schemas and reusable struct components generated from {settings.xsd_file.name}.",
        xml_level=None if settings.xml_tag else settings.xml_level,
        xml_tag=settings.xml_tag,
        root_tag=settings.root_tag,
        max_recursion_depth=settings.max_recursion_depth,
    )
    text = render_schema_module(config)
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    write_schema_text(text, settings.output, logger)


cli = PipelineCli(XsdToPyArrowCliSettings, generate_python_schema)


if __name__ == "__main__":
    cli()
