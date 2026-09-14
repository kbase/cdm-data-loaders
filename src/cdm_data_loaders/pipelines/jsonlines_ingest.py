"""JSONL validate-and-load pipeline for the KBase CTS.

Reads JSONL files. Validates each record against a Pydantic model.
Writes valid and invalid records to separate parquet tables.

The entity models come from an external module set by
`entity_models_module`. That module must define:

    ENTITY_MODELS: dict[str, type[BaseModel]]

Each key is a table name. Each value is the Pydantic model for that
table. Example:

    from datetime import date
    from pydantic import BaseModel

    class Sample(BaseModel):
        id: str
        collection_date: date

    ENTITY_MODELS: dict[str, type[BaseModel]] = {"sample": Sample}

`input_dir` must contain one subdirectory per table name in
`ENTITY_MODELS`. Each subdirectory holds that table's JSONL files.
Files may be plain text or gzip-compressed (`.jsonl` or `.jsonl.gz`).

Valid records go to a table named after the entity. Records that fail
JSON parsing or model validation go to a `<table>_rejected` table. Each
rejected record includes source file, line number, and error detail.
Write disposition is `append` or `replace` only. No merge is performed.
"""

import json
from collections.abc import Callable, Generator, Iterator
from importlib import import_module
from logging import Logger, getLogger
from pathlib import Path
from typing import Annotated, Any, Final

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem
from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import FILE_GLOB, BufferSize, DatasetName, FileGlob
from cdm_data_loaders.core.settings import CLI_SHORTCUTS, DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings
from cdm_data_loaders.pipelines.core import run_cli, run_pipeline
from cdm_data_loaders.utils.buffer import ListBuffer

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "jsonlines_ingest"

ENTITY_MODELS_MODULE: Final[str] = "entity_models_module"
TABLE_NAMES: Final[str] = "table_names"
DEFAULT_FILE_GLOB: Final[str] = "*.jsonl*"
GZIP_SUFFIX: Final[str] = ".gz"

SCHEMA_CONTRACT: Final[dict[str, str]] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}


class JsonlIngestSettings(CtsSettings):
    """Settings for the JSONL validate-and-load pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            ENTITY_MODELS_MODULE.replace("_", "-"): "m",
            TABLE_NAMES.replace("_", "-"): "t",
            FILE_GLOB.replace("_", "-"): "g",
        },
    )
    buffer_size: BufferSize
    dataset_name: DatasetName
    entity_models_module: Annotated[
        str,
        Field(
            description=(
                "Dotted import path to a module that defines "
                "`ENTITY_MODELS: dict[str, type[BaseModel]]`. Each key is a table name and a "
                "subdirectory of `input_dir`. Each value is the Pydantic model for that table."
            ),
        ),
    ]
    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_FILE_GLOB,
            description="Glob pattern for JSONL files inside each entity's input subdirectory.",
        ),
    ]
    table_names: list[str] | None = Field(
        default=None,
        description="Table names to process. Defaults to every table in entity_models_module.",
    )

    @field_validator(TABLE_NAMES, mode="before")
    @classmethod
    def split_table_names(cls, v: str | list[str] | None) -> list[str] | None:
        """Split a comma-separated string into a list. Pass lists and None through unchanged."""
        if v is None or isinstance(v, list):
            return v
        return [name.strip() for name in v.split(",") if name.strip()] or None


def load_entity_models(module_path: str) -> dict[str, type[BaseModel]]:
    """Import module_path and return its ENTITY_MODELS dict.

    :param module_path: dotted import path of the module defining ENTITY_MODELS
    :type module_path: str
    :return: mapping of table name to Pydantic model
    :rtype: dict[str, type[BaseModel]]
    :raises ValueError: if ENTITY_MODELS is missing, empty, or not a dict
    """
    module = import_module(module_path)
    registry = getattr(module, "ENTITY_MODELS", None)
    if not isinstance(registry, dict) or not registry:
        err_msg = f"{module_path} does not define a non-empty ENTITY_MODELS: dict[str, type[BaseModel]]"
        raise ValueError(err_msg)
    return registry


def _read_jsonl_lines(items: Iterator[FileItemDict], buffer_size: int) -> Generator[TDataItems, Any, Any]:
    """Read each file in items. Yield pages of line records.

    Opens each file in text mode. Sets compression to "enable" for file
    names ending in .gz, and "disable" otherwise. Auto-detection is not
    used, since it depends on remote file metadata that local files do
    not carry.

    Each record has: record (parsed JSON, or None on parse failure),
    raw_line, parse_error (None on success), source_file, line_no.
    Records are accumulated into pages of `buffer_size` before being
    yielded, so downstream transformers receive pages rather than
    individual rows.

    :param items: file items to read
    :type items: Iterator[FileItemDict]
    :param buffer_size: number of line records per yielded page
    :type buffer_size: int
    :yield: pages of line records
    :rtype: Generator[TDataItems, Any, Any]
    """
    page: list[dict[str, Any]] = []
    for file_item in items:
        compression = "enable" if file_item["file_name"].endswith(GZIP_SUFFIX) else "disable"
        with file_item.open(mode="rt", compression=compression, encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                parse_error: str | None = None
                record: dict[str, Any] | None = None
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError as e:
                    parse_error = str(e)
                page.append(
                    {
                        "record": record,
                        "raw_line": stripped,
                        "parse_error": parse_error,
                        "source_file": file_item["relative_path"],
                        "line_no": line_no,
                    }
                )
                if len(page) >= buffer_size:
                    yield page
                    page = []
    if page:
        yield page


def _make_validator(
    table_name: str,
    model: type[BaseModel],
    buffer_size: int,
) -> Callable[[list[dict[str, Any]]], Generator[Any, Any, Any]]:
    """Build a function that validates pages of records and routes them to tables.

    A parse_error sends the record to `<table_name>_rejected`. The model
    is not called in this case. A record that fails
    `model.model_validate` also goes to `<table_name>_rejected`. Its
    `error_detail` field holds the Pydantic error list as JSON. A record
    that passes validation goes to `table_name`, dumped from the
    validated model.

    Records are accumulated per table in a ListBuffer and yielded as
    pages of at most `buffer_size` rows.

    :param table_name: table name for valid records
    :type table_name: str
    :param model: Pydantic model used to validate records
    :type model: type[BaseModel]
    :param buffer_size: number of rows to buffer per table before yielding a page
    :type buffer_size: int
    :return: function taking one page of record dicts and yielding routed pages
    :rtype: Callable[[list[dict[str, Any]]], Generator[Any, Any, Any]]
    """
    rejected_table = f"{table_name}_rejected"

    def _validate(page: list[dict[str, Any]]) -> Generator[Any, Any, Any]:
        valid_buffer = ListBuffer(table_name=table_name, max_items=buffer_size)
        rejected_buffer = ListBuffer(table_name=rejected_table, max_items=buffer_size)

        for item in page:
            source_file = item["source_file"]
            line_no = item["line_no"]

            if item["parse_error"] is not None:
                rejected = {
                    "source_file": source_file,
                    "line_no": line_no,
                    "error_detail": item["parse_error"],
                    "raw_record": item["raw_line"],
                }
                yield from rejected_buffer.add_item(rejected)
                continue

            try:
                validated = model.model_validate(item["record"])
            except ValidationError as e:
                rejected = {
                    "source_file": source_file,
                    "line_no": line_no,
                    "error_detail": json.dumps(e.errors(), default=str),
                    "raw_record": json.dumps(item["record"], default=str),
                }
                yield from rejected_buffer.add_item(rejected)
                continue

            yield from valid_buffer.add_item(validated.model_dump(mode="python"))

        yield from valid_buffer.flush()
        yield from rejected_buffer.flush()

    return _validate


def build_entity_resource(table_name: str, model: type[BaseModel], settings: JsonlIngestSettings) -> Any:  # noqa: ANN401
    """Build the resource that reads, validates, and routes one entity's records.

    Reads files matching `settings.file_glob` from
    `settings.input_dir/table_name`. If that directory does not exist,
    uses an empty file list instead. A missing entity then produces zero
    rows, not an error.

    :param table_name: entity name; also the input_dir subdirectory name
    :type table_name: str
    :param model: Pydantic model used to validate this entity's records
    :type model: type[BaseModel]
    :param settings: pipeline settings
    :type settings: JsonlIngestSettings
    :return: resource yielding validated and rejected records for this entity
    :rtype: Any
    """
    entity_dir = Path(settings.input_dir) / table_name

    if entity_dir.is_dir():
        files = filesystem(bucket_url=str(entity_dir), file_glob=settings.file_glob)
    else:
        logger.warning("Entity directory does not exist, skipping: %s", entity_dir)
        files = dlt.resource([], name=f"{table_name}_files")

    raw = dlt.transformer(
        _read_jsonl_lines,
        data_from=files,
        name=f"{table_name}_raw",
        max_table_nesting=0,
    )
    raw.bind(settings.buffer_size)

    validated = dlt.transformer(
        _make_validator(table_name, model, settings.buffer_size),
        data_from=raw,
        name=f"{table_name}_validated",
        max_table_nesting=0,
        parallelized=True,
    )
    validated.apply_hints(schema_contract=SCHEMA_CONTRACT)  # pyright: ignore[reportArgumentType]
    return validated


def run_jsonlines_ingest_pipeline(settings: JsonlIngestSettings) -> None:
    """Run the JSONL validate-and-load pipeline for every requested table.

    :param settings: pipeline configuration
    :type settings: JsonlIngestSettings
    :raises ValueError: if settings.table_names includes a name not in ENTITY_MODELS
    """
    entity_models = load_entity_models(settings.entity_models_module)

    table_names = settings.table_names or sorted(entity_models)
    unknown = sorted(set(table_names) - set(entity_models))
    if unknown:
        err_msg = f"Unknown table name(s) requested: {unknown}; available: {sorted(entity_models)}"
        raise ValueError(err_msg)

    resources = [build_entity_resource(table_name, entity_models[table_name], settings) for table_name in table_names]

    pipeline_kwargs = {
        "pipeline_name": PIPELINE_NAME,
        "dataset_name": settings.dataset_name or PIPELINE_NAME,
    }
    run_pipeline(
        settings=settings,
        resource=resources,
        destination_kwargs={"max_table_nesting": 0},
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs={
            "loader_file_format": "parquet",
        },
    )


def cli() -> None:
    """Command-line entry point for the JSONL validate-and-load pipeline."""
    run_cli(JsonlIngestSettings, run_jsonlines_ingest_pipeline)


if __name__ == "__main__":
    cli()
