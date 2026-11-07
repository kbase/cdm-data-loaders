"""Settings class for the XML to PyArrow pipeline for the KBase CTS."""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Annotated, Any, ClassVar, Final

import pyarrow as pa
import xmlschema
from lxml.etree import Element
from pydantic import Field, FilePath, PositiveInt, PrivateAttr
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters.xsd_to_pyarrow import (
    DEFAULT_MAX_RECURSION_DEPTH,
    generate_pyarrow_schema,
    generate_pyarrow_schemas_at_level,
    load_schema,
    make_xml_element_converter,
    make_xml_level_converters,
)
from cdm_data_loaders.core.fields import NonEmptyStr, XsdFile
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.core.settings import (
    XML_CLI_SHORTCUTS,
    XmlIngestSettings,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.core.transforms import TransformSettingsMixin, validate_table_transforms
from cdm_data_loaders.schemas.pubmed.schema import SCHEMAS as PUBMED_SCHEMAS
from cdm_data_loaders.schemas.uniprot.schema import SCHEMA as UNIPROT_SCHEMA
from cdm_data_loaders.schemas.uniref.schema import SCHEMA as UNIREF_SCHEMA

PIPELINE_NAME: Final[str] = "xmltopyarrow"
PUBMED_PIPELINE_NAME: Final[str] = "pubmed"
UNIPROT_PIPELINE_NAME: Final[str] = "uniprot"
UNIREF_PIPELINE_NAME: Final[str] = "uniref"

ENTRY_XML_TAG: Final[str] = "entry"
ENTRY_TABLE_NAME: Final[str] = "entry"
PUBMED_ROOT_TAG: Final[str] = "PubmedArticleSet"

SCHEMAS_DIR: Final[Path] = Path(__file__).resolve().parents[2] / "schemas"
PUBMED_XSD_FILE: Final[Path] = SCHEMAS_DIR / "pubmed" / "pubmed.xsd"
UNIPROT_XSD_FILE: Final[Path] = SCHEMAS_DIR / "uniprot" / "uniprot.xsd"
UNIREF_XSD_FILE: Final[Path] = SCHEMAS_DIR / "uniref" / "uniref.xsd"


UNIREF_FIFTY: Final[str] = "uniref50"
UNIREF_NINETY: Final[str] = "uniref90"
UNIREF_HUNDRED: Final[str] = "uniref100"


class XmlToPyArrowSettings(XmlIngestSettings, TransformSettingsMixin):
    """Settings for the XML to PyArrow ingestion pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    _xsd_schema: xmlschema.XMLSchema = PrivateAttr()
    _xml_element_converter: Callable[[Element], dict[str, Any]] | None = PrivateAttr(default=None)
    _xml_level_converters: dict[str, Callable[[Element], dict[str, Any]]] = PrivateAttr(default_factory=dict)
    _pyarrow_schema: pa.Schema | None = PrivateAttr(default=None)
    _pyarrow_schemas: dict[str, pa.Schema] = PrivateAttr(default_factory=dict)

    table_name: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="The name of the table to export parsed data to (required when xml_level is not set).",
        ),
    ]
    xml_tag: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="XML tag to capture the contents of (required when xml_level is not set).",
        ),
    ]
    xml_level: Annotated[
        PositiveInt | None,
        Field(
            default=None,
            description="XML depth level relative to the root element to extract all elements from and assign to tables by tag name.",
        ),
    ]
    root_tag: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Optional root XML element tag name used to anchor level-based XSD element resolution.",
        ),
    ]
    xsd_file: XsdFile
    max_recursion_depth: Annotated[
        PositiveInt,
        Field(
            default=DEFAULT_MAX_RECURSION_DEPTH,
            description="Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON.",
        ),
    ]

    def _validate_target_selection(self) -> None:
        """Validate mutually exclusive single-tag and level-based target selection fields."""
        if self.xml_level is not None:
            if self.xml_tag is not None or self.table_name is not None:
                err_msg = "xml_level cannot be combined with xml_tag or table_name"
                raise ValueError(err_msg)
            return

        if self.table_name is None or self.xml_tag is None:
            missing = [name for name in ("table_name", "xml_tag") if getattr(self, name) is None]
            err_msg = f"Missing required field(s) when xml_level is not set: {', '.join(missing)}"
            raise ValueError(err_msg)
        if self.root_tag is not None:
            err_msg = "root_tag can only be specified when xml_level is set"
            raise ValueError(err_msg)

    def _resolve_level_schema_map(self) -> None:
        """Validate `table_transforms` and populate `_schema_map` for level-based mode."""
        validate_table_transforms(self, self._pyarrow_schemas)
        resolved_schemas: dict[str, pa.Schema] = (
            dict(self._unreconciled_output_schemas.registry) if self._unreconciled_output_schemas is not None else {}
        )
        for table_name, table_schema in self._pyarrow_schemas.items():
            if self._needs_default_pyarrow_schema(table_name):
                resolved_schemas[table_name] = table_schema
        self._schema_map = ArrowSchemaRegistry(registry=resolved_schemas) if resolved_schemas else None

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Validate target selection mode, load the XSD, and resolve `_schema_map`."""
        self._validate_target_selection()

        pyarrow_schema: pa.Schema | None = None
        try:
            self._xsd_schema = load_schema(self.xsd_file)
            if self.xml_level is not None:
                self._xml_level_converters = make_xml_level_converters(
                    self._xsd_schema,
                    self.xml_level,
                    root_tag=self.root_tag,
                    max_recursion_depth=self.max_recursion_depth,
                )
                self._pyarrow_schemas = self._resolve_pyarrow_schemas()
                self._pyarrow_schema = None
            else:
                table_name = self.table_name or ""
                xml_tag = self.xml_tag or ""
                self._xml_element_converter = make_xml_element_converter(
                    self._xsd_schema,
                    xml_tag,
                    max_recursion_depth=self.max_recursion_depth,
                )
                pyarrow_schema = (
                    default_pyarrow_schema
                    if default_pyarrow_schema is not None or not self._needs_default_pyarrow_schema(table_name)
                    else self._resolve_pyarrow_schema()
                )
                self._pyarrow_schema = pyarrow_schema
                self._pyarrow_schemas = {table_name: pyarrow_schema} if pyarrow_schema is not None else {}
        except Exception as e:
            err_msg = f"Could not generate PyArrow schema from XSD: {e!s}"
            raise ValueError(err_msg) from e

        if self.xml_level is not None:
            self._resolve_level_schema_map()
        else:
            super()._resolve_settings_schema_map(default_pyarrow_schema=pyarrow_schema)

    def _resolve_pyarrow_schema(self) -> pa.Schema:
        """Return the PyArrow schema for single-tag mode.

        :return: PyArrow schema for xml_tag
        :rtype : pa.Schema
        """
        return generate_pyarrow_schema(
            self._xsd_schema,
            self.xml_tag or "",
            max_recursion_depth=self.max_recursion_depth,
        )

    def _resolve_pyarrow_schemas(self) -> dict[str, pa.Schema]:
        """Return the mapping of table names to PyArrow schemas for level-based mode.

        :return: mapping of element tag names to PyArrow schemas
        :rtype : dict[str, pa.Schema]
        """
        return generate_pyarrow_schemas_at_level(
            self._xsd_schema,
            self.xml_level or 0,
            root_tag=self.root_tag,
            max_recursion_depth=self.max_recursion_depth,
        )

    @property
    def xsd_schema(self) -> xmlschema.XMLSchema:
        """Return the loaded XMLSchema instance.

        :return: loaded XMLSchema instance
        :rtype : xmlschema.XMLSchema
        """
        return self._xsd_schema

    @property
    def xml_element_converter(self) -> Callable[[Element], dict[str, Any]] | None:
        """Return the precomputed XML-element-to-dict converter for `xml_tag`."""
        return self._xml_element_converter

    @property
    def xml_level_converters(self) -> dict[str, Callable[[Element], dict[str, Any]]]:
        """Return the precomputed XML-element-to-dict converters keyed by tag name at `xml_level`."""
        return self._xml_level_converters

    @property
    def pyarrow_schema(self) -> pa.Schema | None:
        """Return the generated PyArrow schema for xml_tag (or None in level mode).

        :return: PyArrow schema or None
        :rtype : pa.Schema | None
        """
        return self._pyarrow_schema

    @property
    def pyarrow_schemas(self) -> dict[str, pa.Schema]:
        """Return the mapping of target table names to generated PyArrow schemas.

        :return: mapping of table names to PyArrow schemas
        :rtype : dict[str, pa.Schema]
        """
        return self._pyarrow_schemas


class PresetXmlToPyArrowSettings(XmlToPyArrowSettings):
    """Base settings for XML-to-PyArrow pipelines backed by a bundled XSD and precomputed PyArrow schema."""

    _PRECOMPUTED_SCHEMA: ClassVar[pa.Schema]

    table_name: ClassVar[str] = ENTRY_TABLE_NAME
    xml_tag: ClassVar[str] = ENTRY_XML_TAG
    xml_level: ClassVar[int | None] = None
    root_tag: ClassVar[str | None] = None

    def _resolve_pyarrow_schema(self) -> pa.Schema:
        """Return the precomputed PyArrow schema for this preset pipeline.

        :return: precomputed PyArrow schema
        :rtype : pa.Schema
        """
        return self._PRECOMPUTED_SCHEMA


class LevelPresetXmlToPyArrowSettings(XmlToPyArrowSettings):
    """Base settings for level-based XML-to-PyArrow pipelines backed by a bundled XSD and precomputed schemas."""

    _PRECOMPUTED_SCHEMAS: ClassVar[Mapping[str, pa.Schema]]

    table_name: ClassVar[str | None] = None
    xml_tag: ClassVar[str | None] = None
    xml_level: ClassVar[int] = 1

    def _resolve_pyarrow_schemas(self) -> dict[str, pa.Schema]:
        """Return the precomputed PyArrow table schemas for this preset pipeline.

        :return: mapping of table names to precomputed PyArrow schemas
        :rtype : dict[str, pa.Schema]
        """
        return dict(self._PRECOMPUTED_SCHEMAS)


class PubMedXmlToPyArrowSettings(LevelPresetXmlToPyArrowSettings):
    """Settings for the PubMed XML to PyArrow ingestion pipeline."""

    _PRECOMPUTED_SCHEMAS: ClassVar[Mapping[str, pa.Schema]] = PUBMED_SCHEMAS

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PUBMED_PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = PUBMED_PIPELINE_NAME
    root_tag: ClassVar[str | None] = PUBMED_ROOT_TAG

    xsd_file: Annotated[
        FilePath,
        Field(
            default=PUBMED_XSD_FILE,
            description="Path to the PubMed XSD file describing the XML documents to be imported.",
        ),
    ]


class UniProtXmlToPyArrowSettings(PresetXmlToPyArrowSettings):
    """Settings for the UniProt XML to PyArrow ingestion pipeline."""

    _PRECOMPUTED_SCHEMA: ClassVar[pa.Schema] = UNIPROT_SCHEMA

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIPROT_PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIPROT_PIPELINE_NAME

    xsd_file: Annotated[
        FilePath,
        Field(
            default=UNIPROT_XSD_FILE,
            description="Path to the UniProt XSD file describing the XML documents to be imported.",
        ),
    ]


class UniRefXmlToPyArrowSettings(PresetXmlToPyArrowSettings):
    """Settings for the UniRef XML to PyArrow ingestion pipeline."""

    _PRECOMPUTED_SCHEMA: ClassVar[pa.Schema] = UNIREF_SCHEMA

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF_PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    xsd_file: Annotated[
        FilePath,
        Field(
            default=UNIREF_XSD_FILE,
            description="Path to the UniRef XSD file describing the XML documents to be imported.",
        ),
    ]


class UniRef50XmlToPyArrowSettings(UniRefXmlToPyArrowSettings):
    """UniRef 50 XML to PyArrow ingest settings."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF_FIFTY,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF_FIFTY


class UniRef90XmlToPyArrowSettings(UniRefXmlToPyArrowSettings):
    """UniRef 90 XML to PyArrow ingest settings."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF_NINETY,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )
    dataset_name: ClassVar[str] = UNIREF_NINETY


class UniRef100XmlToPyArrowSettings(UniRefXmlToPyArrowSettings):
    """UniRef 100 XML to PyArrow ingest settings."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF_HUNDRED,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )
    dataset_name: ClassVar[str] = UNIREF_HUNDRED
