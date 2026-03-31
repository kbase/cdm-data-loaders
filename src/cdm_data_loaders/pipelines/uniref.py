"""DLT pipeline to import UniRef data."""

import datetime
from collections.abc import Generator
from typing import Any

import dlt
from dlt.extract.items import DataItemWithMeta
from pydantic import AliasChoices, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, SettingsError

from cdm_data_loaders.parsers.uniprot.uniref import UNIREF_URL, UNIREF_VARIANTS, parse_uniref_entry
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.file_system import BatchCursor
from cdm_data_loaders.utils.xml_utils import stream_xml_file

logger = get_cdm_logger()

APP_NAME = "uniref_importer"

VALID_DESTINATIONS = ["local_fs", "minio"]
DEFAULT_BATCH_SIZE = 50


class Settings(BaseSettings):
    """Configuration for running the UniRef import pipeline."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_prog_name="uniref",
        cli_exit_on_error=False,
        cli_ignore_unknown_args=True,
    )
    input_dir: str = Field(
        default=INPUT_MOUNT,
        description="Location of directory containing UniRef XML files to import",
        # explicitly allow both kebab case and snake case
        validation_alias=AliasChoices("i", "input_dir", "input-dir", "input_dir"),
    )
    destination: str = Field(
        default="local_fs",
        description=f"Destination configuration to use for data output. Choices: {VALID_DESTINATIONS}",
        validation_alias=AliasChoices("d", "destination"),
    )
    uniref_variant: str = Field(
        description=f"Which UniRef variant to import. Choices: {UNIREF_VARIANTS}",
        validation_alias=AliasChoices("u", "uniref", "uniref-variant", "uniref_variant"),
    )
    start_at: int = Field(
        default=0,
        description="File to start import at",
        validation_alias=AliasChoices("s", "start", "start-at", "start_at"),
    )
    output: str | None = Field(
        default=None,
        description="Location to save imported data to, if different from the default supplied by the destination config",
        validation_alias=AliasChoices("o", "output"),
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

    @field_validator("destination")
    @classmethod
    def validate_destination(cls, v: str) -> str:
        """Validate the destination against valid choices.

        :param v: destination specified
        :type v: str
        :raises ValueError: if the destination is not valid
        :return: valid destination
        :rtype: str
        """
        if v not in VALID_DESTINATIONS:
            err_msg = f"destination must be one of {VALID_DESTINATIONS}, got '{v}'"
            raise ValueError(err_msg)
        return v


@dlt.resource(name="parse_uniref", parallelized=True)
def parse_uniref(config: Settings) -> Generator[DataItemWithMeta, Any]:
    """Parse the information from UniRef files, batch by batch.

    :param config: config for running the pipeline.
    :type config: Settings
    """
    timestamp = datetime.datetime.now(tz=datetime.UTC)
    batch_params: dict[str, Any] = {}
    if config.start_at:
        batch_params["start_at"] = config.start_at

    batcher = BatchCursor(config.input_dir, batch_size=DEFAULT_BATCH_SIZE, **batch_params)
    while uniref_files := batcher.get_batch():
        for file_path in uniref_files:
            logger.info("Reading from %s", str(file_path))
            for n_entries, entry in enumerate(stream_xml_file(file_path, f"{{{UNIREF_URL}}}entry")):
                parsed_entry = parse_uniref_entry(entry, timestamp, f"UniRef {config.uniref_variant}", file_path)
                for table, rows in parsed_entry.items():
                    yield dlt.mark.with_table_name(rows, table)
                if n_entries + 1 % 10000 == 0:
                    logger.info("Processed %d entries", n_entries + 1)


def run_pipeline(config: Settings) -> None:
    """Execute the pipeline.

    :param config: config for running the pipeline.
    :type config: Settings
    """
    # check whether there is a custom output location; if so, set it in the config
    if config.output:
        dlt.config[f"destination.{config.destination}.bucket_url"] = config.output

    pipeline = dlt.pipeline(
        pipeline_name=f"uniref_{config.uniref_variant}",
        destination=dlt.destination(config.destination, max_table_nesting=0),
        dataset_name="uniprot_kb",
    )
    load_info = pipeline.run(parse_uniref(config), table_format="delta")
    logger.info(load_info)
    logger.info("Work complete!")


def cli() -> None:
    """CLI interface for the UniRef importer pipeline.

    See the ``Settings`` object for parameters.
    """
    try:
        config = Settings()  # pyright: ignore[reportCallIssue]
    except (Exception, SettingsError, ValidationError) as e:
        print(f"Error initialising config: {e}")
        raise

    run_pipeline(config)


if __name__ == "__main__":
    cli()
