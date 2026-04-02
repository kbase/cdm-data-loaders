"""DLT pipeline to import UniRef data."""

from collections.abc import Generator
from typing import Any

import dlt
from dlt.extract.items import DataItemWithMeta
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.parsers.uniprot.uniref import ENTRY_XML_TAG, UNIREF_VARIANTS, parse_uniref_entry
from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
    stream_xml_file_resource,
)
from cdm_data_loaders.pipelines.cts_defaults import (
    BatchedFileInputSettings,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger

logger = get_cdm_logger()

APP_NAME = "uniref_importer"
UNIREF_LOG_INTERVAL = 10000


class Settings(BatchedFileInputSettings):
    """Configuration for running the UniRef import pipeline."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_prog_name="uniref",
        cli_exit_on_error=False,
        cli_ignore_unknown_args=True,
    )
    uniref_variant: str = Field(
        description=f"Which UniRef variant to import. Choices: {UNIREF_VARIANTS}",
        validation_alias=AliasChoices("u", "uniref", "uniref-variant", "uniref_variant"),
    )

    @field_validator("uniref_variant")
    @classmethod
    def validate_uniref_variant(cls, v: str) -> str:
        """Validate the uniref variant against valid choices.

        :param v: uniref variant specified
        :type v: str
        :raises ValueError: if the uniref variant is not valid
        :return: valid uniref variant
        :rtype: str
        """
        if v not in UNIREF_VARIANTS:
            err_msg = f"uniref_variant must be one of {UNIREF_VARIANTS}, got '{v}'"
            raise ValueError(err_msg)
        return v


@dlt.resource(name="parse_uniref", parallelized=True)
def parse_uniref(config: Settings) -> Generator[DataItemWithMeta, Any]:
    """Parse the information from UniRef files, batch by batch.

    :param config: config for running the pipeline.
    :type config: Settings
    """
    yield from stream_xml_file_resource(
        config=config,
        xml_tag=ENTRY_XML_TAG,
        parse_fn=lambda entry, timestamp, file_path: parse_uniref_entry(
            entry=entry, timestamp=timestamp, file_path=file_path, uniref_variant=f"UniRef {config.uniref_variant}"
        ),
        log_interval=UNIREF_LOG_INTERVAL,
    )


def run_uniref_pipeline(config: Settings) -> None:
    """Execute the Uniref pipeline.

    :param config: config for running the pipeline.
    :type config: Settings
    """
    run_pipeline(
        config=config,
        resource=parse_uniref(config),
        pipeline_kwargs={
            "pipeline_name": f"uniref_{config.uniref_variant}",
            "dataset_name": "uniprot_kb",
        },
        pipeline_run_kwargs={"table_format": "delta"},
    )


def cli() -> None:
    """CLI interface for the UniRef importer pipeline."""
    run_cli(Settings, run_uniref_pipeline)


if __name__ == "__main__":
    cli()
