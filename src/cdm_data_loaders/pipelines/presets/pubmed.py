"""Preset XML-to-PyArrow ingestion pipeline for PubMed XML archives."""

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, ClassVar, Final

import pyarrow as pa
from pydantic import Field, FilePath

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import FileGlob, file_glob_description
from cdm_data_loaders.core.md5_preprocessor import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn
from cdm_data_loaders.pipelines.xml_common import XML_GZ_FILE_GLOB
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    SCHEMAS_DIR,
    LevelPresetXmlToPyArrowSettings,
)
from cdm_data_loaders.schemas.pubmed.schema import SCHEMAS as PUBMED_SCHEMAS

PUBMED_PIPELINE_NAME: Final[str] = "pubmed"
PUBMED_ROOT_TAG: Final[str] = "PubmedArticleSet"
PUBMED_XSD_FILE: Final[Path] = SCHEMAS_DIR / "pubmed" / "pubmed.xsd"


class PubMedXmlToPyArrowSettings(LevelPresetXmlToPyArrowSettings, cli_prog_name=PUBMED_PIPELINE_NAME):
    """Settings for the PubMed XML-to-PyArrow ingestion pipeline."""

    PRECOMPUTED_SCHEMAS: ClassVar[Mapping[str, pa.Schema]] = PUBMED_SCHEMAS
    DEFAULT_FILE_PREPROCESSORS: ClassVar[tuple[FilePreprocessFn, ...]] = (validate_md5_sidecar,)
    dataset_name: ClassVar[str] = PUBMED_PIPELINE_NAME

    file_glob: Annotated[
        FileGlob,
        Field(
            default=XML_GZ_FILE_GLOB,
            description=file_glob_description("Glob pattern for PubMed XML files"),
        ),
    ]
    xsd_file: Annotated[
        FilePath,
        Field(
            default=PUBMED_XSD_FILE,
            description="Path to the PubMed XSD file describing the XML documents to be imported.",
        ),
    ]


cli = CliRunner(PubMedXmlToPyArrowSettings, run_xmltopyarrow_pipeline)


if __name__ == "__main__":
    cli()
