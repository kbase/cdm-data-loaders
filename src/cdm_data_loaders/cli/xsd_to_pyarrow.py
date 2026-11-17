"""CLI interface for generating static Python PyArrow schema modules from XSD schemas."""

import os
import sys
from collections.abc import Iterable
from logging import Logger, getLogger
from pathlib import Path
from typing import Annotated, Final, Self

from pydantic import BaseModel, ConfigDict, Field, FilePath, PositiveInt, model_validator
from pydantic_settings import CliImplicitFlag, SettingsConfigDict

from cdm_data_loaders.converters.core.io import write_schema_text
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import XsdToPyArrow
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import NonEmptyStr
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.utils.recursion import DEFAULT_MAX_RECURSION_DEPTH

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "xsd_to_pyarrow"
REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
SCHEMAS_DIR: Final[Path] = Path(__file__).resolve().parent.parent / "schemas"

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


UNIREF_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "uniref" / "uniref.xsd",
    output=SCHEMAS_DIR / "uniref" / "schema.py",
    docstring="PyArrow schema and reusable struct components for UniRef (xml_tag='entry').",
    xml_level=1,
)

UNIPROT_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "uniprot" / "uniprot.xsd",
    output=SCHEMAS_DIR / "uniprot" / "schema.py",
    docstring="PyArrow schemas and reusable struct components for UniProt XML.",
    xml_level=1,
)

PUBMED_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "pubmed" / "pubmed.xsd",
    output=SCHEMAS_DIR / "pubmed" / "schema.py",
    docstring="PyArrow schemas and reusable struct components for PubMed XML.",
    xml_level=1,
    root_tag="PubmedArticleSet",
)

CONFIGS: Final[tuple[SchemaCodegenConfig, ...]] = (
    UNIREF_SCHEMA_CONFIG,
    UNIPROT_SCHEMA_CONFIG,
    PUBMED_SCHEMA_CONFIG,
)


class XsdToPyArrowCliSettings(LoggerSettings):
    """Settings for generating static Python PyArrow schema modules from XSD files."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=XSD_TO_PYARROW_CLI_SHORTCUTS,
    )

    xsd_file: Annotated[
        FilePath | None,
        Field(
            default=None,
            description="Path to the input XSD file (required unless --bundled is set)",
        ),
    ]
    output: Annotated[
        Path | None,
        Field(
            default=None,
            description="Destination Python file path for the generated schema module (required unless --bundled is set)",
        ),
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
            description="Root XML tag for level discovery",
        ),
    ]
    max_recursion_depth: Annotated[
        PositiveInt,
        Field(
            default=DEFAULT_MAX_RECURSION_DEPTH,
            description="Maximum recursion depth before JSON string truncation",
        ),
    ]
    bundled: CliImplicitFlag[bool] = Field(
        default=False,
        description="Regenerate or check all committed bundled PyArrow schema modules",
    )
    check: CliImplicitFlag[bool] = Field(
        default=False,
        description="Exit with code 1 without writing when generated output differs from disk",
    )

    @model_validator(mode="after")
    def _validate_target_mode(self) -> Self:
        """Require ``xsd_file`` and ``output`` unless ``bundled`` mode is active."""
        if self.bundled:
            if self.xsd_file is not None or self.output is not None:
                msg = "--xsd-file and --output cannot be combined with --bundled"
                raise ValueError(msg)
            return self
        if self.xsd_file is None or self.output is None:
            msg = "--xsd-file and --output are required unless --bundled is set"
            raise ValueError(msg)
        return self


def render_schema_module(config: SchemaCodegenConfig) -> str:
    """Render a formatted Python schema module from ``config``.

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


def check_or_write_files(
    outputs: Iterable[tuple[Path, str]],
    *,
    check: bool,
    repo_root: Path = REPO_ROOT,
) -> int:
    """Compare each ``(output_path, rendered_text)`` pair against disk and report stale files or write updates.

    :param outputs: Iterable of ``(output_path, rendered_text)`` pairs.
    :type outputs: Iterable[tuple[Path, str]]
    :param check: When ``True``, report stale files to ``sys.stderr`` and return ``1`` without writing.
    :type check: bool
    :param repo_root: Repository root used to format relative paths on ``stderr``.
    :type repo_root: Path
    :return: ``0`` if all files match (or are written), ``1`` if ``check`` is ``True`` and any file is stale.
    :rtype: int
    """
    stale: list[Path] = []
    for output_path, text in outputs:
        current = output_path.read_text(encoding="utf-8") if output_path.exists() else None
        if current == text:
            continue
        if check:
            stale.append(output_path)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(text, encoding="utf-8")
    for path in stale:
        rel_out = (
            path.relative_to(repo_root).as_posix()
            if path.is_relative_to(repo_root)
            else Path(os.path.relpath(path.resolve(), repo_root.resolve())).as_posix()
        )
        sys.stderr.write(f"stale: {rel_out} is out of date\n")
    return 1 if stale else 0


def run_bundled_codegen(*, check: bool = False) -> int:
    """Write or check every configured bundled PyArrow schema module in ``CONFIGS``.

    :param check: When ``True``, check committed modules without writing.
    :type check: bool
    :return: ``0`` on match or write, ``1`` if ``check`` detects stale files.
    :rtype: int
    """
    return check_or_write_files(
        ((config.output, render_schema_module(config)) for config in CONFIGS),
        check=check,
        repo_root=REPO_ROOT,
    )


def generate_python_schema(settings: XsdToPyArrowCliSettings) -> None:
    """Generate and write or check static Python PyArrow schema module(s) from XSD file(s).

    :param settings: XSD-to-PyArrow CLI settings.
    :type settings: XsdToPyArrowCliSettings
    :raises SystemExit: If ``settings.check`` is ``True`` and one or more target files are stale.
    :raises ValueError: If ``xsd_file`` or ``output`` is ``None`` outside ``bundled`` mode.
    """
    if settings.bundled:
        exit_code = run_bundled_codegen(check=settings.check)
        if exit_code != 0:
            raise SystemExit(exit_code)
        return

    if settings.xsd_file is None or settings.output is None:
        msg = "xsd_file and output must be set when bundled is False"
        raise ValueError(msg)

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
    if settings.check:
        exit_code = check_or_write_files([(settings.output, text)], check=True, repo_root=REPO_ROOT)
        if exit_code != 0:
            raise SystemExit(exit_code)
        return
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    write_schema_text(text, settings.output, logger)


cli = CliRunner(XsdToPyArrowCliSettings, generate_python_schema)


if __name__ == "__main__":
    cli()
