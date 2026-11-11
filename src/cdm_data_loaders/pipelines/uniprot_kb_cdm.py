"""DLT pipeline to import UniProt data into KBase CDM tables via XML-to-PyArrow."""

from typing import Annotated, ClassVar, Final

import pyarrow as pa
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import LogInterval
from cdm_data_loaders.core.settings import (
    XML_CLI_SHORTCUTS,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.parsers.uniprot.uniprot_kb import UNIPROT_CDM_SCHEMA_MAP, make_uniprot_cdm_transform
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import UniProtXmlToPyArrowSettings

PIPELINE_NAME: Final[str] = "uniprot_cdm"
UNIPROT_LOG_INTERVAL: Final[int] = 1000


class UniProtSettings(UniProtXmlToPyArrowSettings):
    """Configuration for running the UniProt KB CDM XML-to-PyArrow import pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = PIPELINE_NAME
    transforms_module: ClassVar[str | None] = None
    output_schemas_module: ClassVar[str | None] = None

    log_interval: Annotated[
        LogInterval,
        Field(
            default=UNIPROT_LOG_INTERVAL,
        ),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Bind the UniProt CDM transform and schemas to the entry table."""
        self._bind_cdm_transform_and_schemas(
            make_uniprot_cdm_transform(),
            UNIPROT_CDM_SCHEMA_MAP,
            default_pyarrow_schema=default_pyarrow_schema,
        )


cli = PipelineCli(UniProtSettings, run_xmltopyarrow_pipeline)


if __name__ == "__main__":
    cli()
