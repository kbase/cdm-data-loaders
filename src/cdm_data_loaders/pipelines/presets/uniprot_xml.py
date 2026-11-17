"""Preset XML-to-PyArrow ingestion pipelines for raw UniProtKB and UniRef XML files."""

from pathlib import Path
from typing import Annotated, ClassVar, Final

import pyarrow as pa
from pydantic import Field, FilePath

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    SCHEMAS_DIR,
    PresetXmlToPyArrowSettings,
)
from cdm_data_loaders.schemas.uniprot.schema import SCHEMA as UNIPROT_SCHEMA
from cdm_data_loaders.schemas.uniref.schema import SCHEMA as UNIREF_SCHEMA

UNIPROT_PIPELINE_NAME: Final[str] = "uniprot"
UNIREF_PIPELINE_NAME: Final[str] = "uniref"
UNIREF_FIFTY: Final[str] = "uniref50"
UNIREF_NINETY: Final[str] = "uniref90"
UNIREF_HUNDRED: Final[str] = "uniref100"

UNIPROT_XSD_FILE: Final[Path] = SCHEMAS_DIR / "uniprot" / "uniprot.xsd"
UNIREF_XSD_FILE: Final[Path] = SCHEMAS_DIR / "uniref" / "uniref.xsd"


class UniProtXmlToPyArrowSettings(PresetXmlToPyArrowSettings, cli_prog_name=UNIPROT_PIPELINE_NAME):
    """Settings for the UniProt XML-to-PyArrow ingestion pipeline."""

    PRECOMPUTED_SCHEMA: ClassVar[pa.Schema] = UNIPROT_SCHEMA
    dataset_name: ClassVar[str] = UNIPROT_PIPELINE_NAME

    xsd_file: Annotated[
        FilePath,
        Field(
            default=UNIPROT_XSD_FILE,
            description="Path to the UniProt XSD file describing the XML documents to be imported.",
        ),
    ]


class UniRefXmlToPyArrowSettings(PresetXmlToPyArrowSettings, cli_prog_name=UNIREF_PIPELINE_NAME):
    """Settings for the UniRef XML-to-PyArrow ingestion pipeline."""

    PRECOMPUTED_SCHEMA: ClassVar[pa.Schema] = UNIREF_SCHEMA

    xsd_file: Annotated[
        FilePath,
        Field(
            default=UNIREF_XSD_FILE,
            description="Path to the UniRef XSD file describing the XML documents to be imported.",
        ),
    ]


class UniRef50XmlToPyArrowSettings(UniRefXmlToPyArrowSettings, cli_prog_name=UNIREF_FIFTY):
    """UniRef 50 XML-to-PyArrow ingest settings."""

    dataset_name: ClassVar[str] = UNIREF_FIFTY


class UniRef90XmlToPyArrowSettings(UniRefXmlToPyArrowSettings, cli_prog_name=UNIREF_NINETY):
    """UniRef 90 XML-to-PyArrow ingest settings."""

    dataset_name: ClassVar[str] = UNIREF_NINETY


class UniRef100XmlToPyArrowSettings(UniRefXmlToPyArrowSettings, cli_prog_name=UNIREF_HUNDRED):
    """UniRef 100 XML-to-PyArrow ingest settings."""

    dataset_name: ClassVar[str] = UNIREF_HUNDRED


uniprot_cli = CliRunner(UniProtXmlToPyArrowSettings, run_xmltopyarrow_pipeline)
uniref50_cli = CliRunner(UniRef50XmlToPyArrowSettings, run_xmltopyarrow_pipeline)
uniref90_cli = CliRunner(UniRef90XmlToPyArrowSettings, run_xmltopyarrow_pipeline)
uniref100_cli = CliRunner(UniRef100XmlToPyArrowSettings, run_xmltopyarrow_pipeline)
