"""DLT pipeline to import UniProt data."""

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Annotated, Any, Final

import dlt
from dlt.extract.items import DataItemWithMeta
from pydantic import Field, PositiveInt
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.settings import DEFAULT_SETTINGS_CONFIG_DICT, BatchedFileInputSettings
from cdm_data_loaders.parsers.uniprot.uniprot_kb import ENTRY_XML_TAG, parse_uniprot_entry
from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
)
from cdm_data_loaders.readers.xml import process_xml_file_batches

APP_NAME: Final[str] = "uniprot_kb_importer"
UNIPROT_LOG_INTERVAL: Final[int] = 1000


class UniProtSettings(BatchedFileInputSettings):
    """Configuration for running the UniProt KB import pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name="uniprot",
    )

    log_interval: Annotated[
        PositiveInt,
        Field(
            default=UNIPROT_LOG_INTERVAL,
            description="How often (in number of processed entries) to emit a progress log message. Must be a positive integer.",
        ),
    ]


@dlt.resource(name="parse_uniprot", file_format="parquet", parallelized=True)
def parse_uniprot(settings: UniProtSettings) -> Generator[DataItemWithMeta, Any]:
    """Parse the information from UniProt files, batch by batch.

    :param settings: config for running the pipeline.
    :type settings: UniProtSettings
    """
    # a single timestamp is used to mark every entity parsed in this run
    timestamp = datetime.now(UTC)
    yield from process_xml_file_batches(
        settings=settings,
        xml_tag=ENTRY_XML_TAG,
        parse_fn=lambda entry, file_path: parse_uniprot_entry(
            entry=entry, timestamp=timestamp, file_path=file_path
        ),
    )


def run_uniprot_pipeline(settings: UniProtSettings) -> None:
    """Execute the UniProt KB pipeline."""
    run_pipeline(
        settings=settings,
        resource=parse_uniprot(settings),
        pipeline_kwargs={
            "pipeline_name": "uniprot_kb",
            "dataset_name": "uniprot_kb",
        },
    )


def cli() -> None:
    """CLI interface for the UniProt KB importer pipeline."""
    run_cli(
        UniProtSettings,
        run_uniprot_pipeline,
    )


if __name__ == "__main__":
    cli()
