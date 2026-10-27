"""Generic reference pipeline for testing pipeline configurations and processing elements."""

from collections.abc import Generator
from typing import TYPE_CHECKING, Annotated, Any, Final

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    TABLE_NAME,
    FileGlob,
    PreserveTableNesting,
    TableName,
)
from cdm_data_loaders.core.settings import (
    FILE_FORMAT_CLI_SHORTCUTS,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.pipelines.core import (
    DEFAULT_SCHEMA_CONTRACT,
    PipelineCli,
    build_file_ingest_kwargs,
    run_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.readers.jsonlines import stream_jsonl_lines
from cdm_data_loaders.utils.buffer import DictBuffer, ListBuffer

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource

PIPELINE_NAME: Final[str] = "reference_pipeline"
DEFAULT_REFERENCE_FILE_GLOB: Final[str] = "**/*.jsonl*"
DEFAULT_REFERENCE_TABLE_NAME: Final[str] = "dataset"


class ReferencePipelineSettings(JsonlExtractSettings):
    """Settings for the generic reference test pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **FILE_FORMAT_CLI_SHORTCUTS,
            FILE_GLOB: "g",
            TABLE_NAME: "t",
        },
    )

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_REFERENCE_FILE_GLOB,
            description="Glob pattern for JSONL files",
        ),
    ]
    table_name: Annotated[
        TableName,
        Field(
            default=DEFAULT_REFERENCE_TABLE_NAME,
            description="The name of the table to export parsed data to",
        ),
    ]
    preserve_table_nesting: PreserveTableNesting
    parallel_step_1: Annotated[
        bool,
        Field(
            default=True,
            description="Whether transformer step 1 (reading JSONL files) is parallelized",
        ),
    ]
    parallel_step_2: Annotated[
        bool,
        Field(
            default=True,
            description="Whether transformer step 2 (ListBuffer) is parallelized",
        ),
    ]
    parallel_step_3: Annotated[
        bool,
        Field(
            default=True,
            description="Whether transformer step 3 (DictBuffer) is parallelized",
        ),
    ]


def list_buffer_step(
    page: list[dict[str, Any]],
    settings: ReferencePipelineSettings,
) -> Generator[TDataItems, Any, Any]:
    """Buffer parsed records from a page using ListBuffer and forward batches unchanged."""
    buf = ListBuffer(table_name=settings.table_name, max_items=settings.buffer_size)
    for item in page:
        if item["parse_error"] is None:
            yield from buf.add_item(item["record"])
    yield from buf.flush()


def dict_buffer_step(
    page: list[dict[str, Any]],
    settings: ReferencePipelineSettings,
) -> Generator[TDataItems, Any, Any]:
    """Buffer records from a page using DictBuffer and yield batches to the target table unchanged."""
    buf = DictBuffer(max_items=settings.buffer_size)
    for record in page:
        yield from buf.add_item(settings.table_name, record)
    yield from buf.flush()


def build_reference_resource(settings: ReferencePipelineSettings) -> "DltResource":
    """Build the three-step reference pipeline resource over a filesystem source."""
    files = filesystem(bucket_url=str(settings.input_dir), file_glob=settings.file_glob)

    step_1 = dlt.transformer(
        stream_jsonl_lines,
        data_from=files,
        name="step_1_read",
        parallelized=settings.parallel_step_1,
    )
    step_1.bind(settings.buffer_size)

    step_2 = dlt.transformer(
        list_buffer_step,
        data_from=step_1,
        name="step_2_list_buffer",
        parallelized=settings.parallel_step_2,
    )
    step_2.bind(settings)

    step_3_kwargs: dict[str, Any] = {"parallelized": settings.parallel_step_3}
    if settings.preserve_table_nesting:
        step_3_kwargs["max_table_nesting"] = 0

    step_3 = dlt.transformer(
        dict_buffer_step,
        data_from=step_2,
        name="step_3_dict_buffer",
        **step_3_kwargs,
    )
    step_3.bind(settings)
    step_3.apply_hints(schema_contract=TSchemaContractDict(**DEFAULT_SCHEMA_CONTRACT))  # pyright: ignore[reportCallIssue]
    return step_3


def run_reference_pipeline(settings: ReferencePipelineSettings) -> LoadInfo | None:
    """Run the generic reference pipeline."""
    return run_pipeline(
        settings=settings,
        resource=build_reference_resource(settings),
        **build_file_ingest_kwargs(settings, PIPELINE_NAME, fallback_dataset_name=True),
    )


cli = PipelineCli(ReferencePipelineSettings, run_reference_pipeline)

if __name__ == "__main__":
    cli()
