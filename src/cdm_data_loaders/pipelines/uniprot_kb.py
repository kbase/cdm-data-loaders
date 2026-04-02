"""DLT pipeline to import UniProt data."""

from collections.abc import Generator
from typing import Any

import dlt
from dlt.extract.items import DataItemWithMeta
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.parsers.uniprot.uniprot_kb import ENTRY_XML_TAG, parse_uniprot_entry
from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
    stream_xml_file_resource,
)
from cdm_data_loaders.pipelines.cts_defaults import BatchedFileInputSettings

APP_NAME = "uniprot_kb_importer"
UNIPROT_LOG_INTERVAL = 1000


class Settings(BatchedFileInputSettings):
    """Configuration for running the UniProt KB import pipeline."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_prog_name="uniprot",
        cli_exit_on_error=False,
        cli_ignore_unknown_args=True,
    )


@dlt.resource(name="parse_uniprot", parallelized=True)
def parse_uniprot(config: Settings) -> Generator[DataItemWithMeta, Any]:
    """Parse the information from UniProt files, batch by batch."""
    yield from stream_xml_file_resource(
        config=config,
        xml_tag=ENTRY_XML_TAG,
        parse_fn=parse_uniprot_entry,
        log_interval=UNIPROT_LOG_INTERVAL,
    )


def run_uniprot_pipeline(config: Settings) -> None:
    """Execute the UniProt KB pipeline."""
    run_pipeline(
        config=config,
        resource=parse_uniprot(config),
        pipeline_kwargs={
            "pipeline_name": "uniprot_kb",
            "dataset_name": "uniprot_kb",
        },
        pipeline_run_kwargs={"table_format": "delta"},
    )


def cli() -> None:
    """CLI interface for the UniProt KB importer pipeline."""
    run_cli(Settings, run_uniprot_pipeline)


if __name__ == "__main__":
    cli()
