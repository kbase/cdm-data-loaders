"""DLT pipeline to import UniRef data."""

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Annotated, Any, Final

import dlt
from dlt.extract.items import DataItemWithMeta
from pydantic import Field, PositiveInt, field_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.settings import (
    DEFAULT_SETTINGS_CONFIG_DICT,
    BatchedFileInputSettings,
)
from cdm_data_loaders.parsers.uniprot.uniref import (
    ENTRY_XML_TAG,
    UNIREF_VARIANTS,
    parse_uniref_entry,
)
from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
)
from cdm_data_loaders.readers.xml import process_xml_file_batches

APP_NAME: Final[str] = "uniref_importer"
UNIREF_LOG_INTERVAL: Final[int] = 10000
VARIANT: Final[str] = "variant"


class UnirefSettings(BatchedFileInputSettings):
    """Configuration for running the UniRef import pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name="uniref",
    )

    variant: Annotated[
        str,
        Field(
            description=f"Which UniRef variant to import. Choices: {UNIREF_VARIANTS}",
        ),
    ]

    log_interval: Annotated[
        PositiveInt,
        Field(
            default=UNIREF_LOG_INTERVAL,
            description="How often (in number of processed entries) to emit a progress log message. Must be a positive integer.",
        ),
    ]

    @field_validator("variant")
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
            err_msg = f"UniRef variant must be one of {UNIREF_VARIANTS}, got '{v}'"
            raise ValueError(err_msg)
        return v


@dlt.resource(name="parse_uniref", file_format="parquet", parallelized=True)
def parse_uniref(settings: UnirefSettings) -> Generator[DataItemWithMeta, Any]:
    """Parse the information from UniRef files, batch by batch.

    :param settings: config for running the pipeline.
    :type settings: UnirefSettings
    """
    # a single timestamp is used to mark every entity parsed in this run
    timestamp = datetime.now(UTC)
    yield from process_xml_file_batches(
        settings=settings,
        xml_tag=ENTRY_XML_TAG,
        parse_fn=lambda entry, file_path: parse_uniref_entry(
            entry=entry,
            timestamp=timestamp,
            file_path=file_path,
            uniref_variant=f"UniRef {settings.variant}",
        ),
    )


def run_uniref_pipeline(settings: UnirefSettings) -> None:
    """Execute the Uniref pipeline.

    :param settings: config for running the pipeline.
    :type settings: UnirefSettings
    """
    run_pipeline(
        settings=settings,
        resource=parse_uniref(settings),
        pipeline_kwargs={
            "pipeline_name": f"uniref_{settings.variant}",
            "dataset_name": "uniprot_kb",
        },
    )


def cli() -> None:
    """CLI interface for the UniRef importer pipeline."""
    run_cli(
        UnirefSettings,
        run_uniref_pipeline,
    )


if __name__ == "__main__":
    cli()
