"""DLT pipeline to import UniRef data."""

from collections.abc import Generator
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Final

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.extract.items import DataItemWithMeta
from pydantic import Field, field_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import START_AT, LogInterval
from cdm_data_loaders.core.settings import (
    CLI_SHORTCUTS,
    DEFAULT_SETTINGS_CONFIG_DICT,
    BatchedFileInputSettings,
)
from cdm_data_loaders.parsers.uniprot.uniref import (
    ENTRY_XML_TAG,
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
FIFTY: Final[str] = "50"
NINETY: Final[str] = "90"
HUNDRED: Final[str] = "100"


class UnirefVariantEnum(StrEnum):
    """Valid values for the Uniref variant."""

    FIFTY = FIFTY
    NINETY = NINETY
    HUNDRED = HUNDRED


UNIREF_VARIANTS: Final[list[str]] = [member.value for member in UnirefVariantEnum.__members__.values()]


class UnirefSettings(BatchedFileInputSettings):
    """Configuration for running the UniRef import pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name="uniref",
        cli_shortcuts={**CLI_SHORTCUTS, START_AT: "s", "variant": "v"},
    )

    variant: Annotated[
        UnirefVariantEnum,
        Field(
            description=f"Which UniRef variant to import. Choices: {UNIREF_VARIANTS}",
        ),
    ]

    log_interval: Annotated[
        LogInterval,
        Field(
            default=UNIREF_LOG_INTERVAL,
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


def run_uniref_pipeline(settings: UnirefSettings) -> LoadInfo | None:
    """Execute the Uniref pipeline.

    :param settings: config for running the pipeline.
    :type settings: UnirefSettings
    """
    return run_pipeline(
        settings=settings,
        resource=parse_uniref(settings),
        pipeline_kwargs={
            "pipeline_name": f"uniref_{settings.variant}",
            "dataset_name": "uniprot_kb",
        },
    )


def cli() -> LoadInfo | None:
    """CLI interface for the UniRef importer pipeline."""
    return run_cli(
        UnirefSettings,
        run_uniref_pipeline,
    )


if __name__ == "__main__":
    cli()
