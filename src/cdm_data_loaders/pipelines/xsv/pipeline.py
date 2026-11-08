"""XSV (CSV/TSV) ingestion pipeline for the KBase CTS.

Cleans and validates each XSV file against `settings.schema_file` using qsv (see
`readers.jsonschema_xsv.xsv_validator`), then loads each valid row, cast to the types declared
in the schema, into `table_name`. Files that fail cleaning/validation are routed to
`_rejected`, one row per recorded error; qsv's own per-row validation diagnostics are
left in its scratch output directory and are not currently surfaced as rows.
"""

from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from typing import Any

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.transforms import route_and_coerce_record
from cdm_data_loaders.pipelines.core import (
    DEFAULT_SCHEMA_CONTRACT,
    PipelineCli,
    build_file_ingest_kwargs,
    build_per_file_filesystem_resource,
    run_pipeline,
)
from cdm_data_loaders.pipelines.xsv.settings import PIPELINE_NAME, XsvIngestSettings
from cdm_data_loaders.readers.jsonschema_xsv.xsv_reader import read_validated_rows
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.helpers import CleanerValidatorArgs, ErrorRecord
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.qsv import clean_validate_file, qsv_check
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import generate_header
from cdm_data_loaders.utils.buffer import DictBuffer
from cdm_data_loaders.utils.file_items import local_copy, source_id


@dataclass
class XsvWorkPaths:
    """Scratch resources shared across all files processed in one pipeline run.

    :param qsv_cmd: path to the qsv binary
    :type qsv_cmd: str
    :param header_file_path: header file used when an input file is missing a header row
    :type header_file_path: Path
    :param tmp_dir: scratch directory for intermediate cleaning/validation files
    :type tmp_dir: Path
    :param qsv_output_dir: directory qsv writes failure diagnostics to
    :type qsv_output_dir: Path
    :param validated_dir: directory the final cleaned, validated file is written to
    :type validated_dir: Path
    :param download_dir: directory non-local input files are downloaded to before validation (qsv needs local files)
    :type download_dir: Path
    """

    qsv_cmd: str
    header_file_path: Path
    tmp_dir: Path
    qsv_output_dir: Path
    validated_dir: Path
    download_dir: Path


def _process_xsv_file(
    file_item: FileItemDict,
    settings: XsvIngestSettings,
    work_paths: XsvWorkPaths,
) -> Generator[TDataItems, Any, Any]:
    """Clean, validate, and load one XSV file. Yield rows for table_name and its rejected table.

    qsv works on local files, so a file that is not on the local filesystem (e.g. on S3) is first
    downloaded to ``work_paths.download_dir`` and deleted once it has been processed. Local files are
    used in place. Rejected rows record the file's path relative to the input location.

    :param file_item: raw XSV file as a FileItemDict
    :type  file_item: FileItemDict
    :param settings: pipeline configuration
    :type  settings: XsvIngestSettings
    :param work_paths: scratch resources shared across the pipeline run
    :type  work_paths: XsvWorkPaths
    :yield: rows routed to table_name and/or ``_rejected``
    :rtype: Generator[TDataItems, Any, Any]
    """
    with (
        TemporaryDirectory(dir=work_paths.download_dir) as file_download_dir,
        local_copy(file_item, Path(file_download_dir)) as file_path,
    ):
        yield from _process_local_xsv_file(file_path, source_id(file_item), settings, work_paths)


def _process_local_xsv_file(
    file_path: Path,
    file_source_id: str,
    settings: XsvIngestSettings,
    work_paths: XsvWorkPaths,
) -> Generator[TDataItems, Any, Any]:
    """Clean, validate, and load one local XSV file. Yield rows for table_name and its rejected table.

    :param file_path: local path to the XSV file
    :type  file_path: Path
    :param file_source_id: identifier recorded for the file in rejected rows
    :type  file_source_id: str
    :param settings: pipeline configuration
    :type  settings: XsvIngestSettings
    :param work_paths: scratch resources shared across the pipeline run
    :type  work_paths: XsvWorkPaths
    :yield: rows routed to table_name and/or ``_rejected``
    :rtype: Generator[TDataItems, Any, Any]
    """
    file_validated_dir = Path(mkdtemp(prefix=f"{file_path.stem}_", dir=work_paths.validated_dir))
    file_qsv_output_dir = Path(mkdtemp(prefix=f"{file_path.stem}_", dir=work_paths.qsv_output_dir))
    with TemporaryDirectory(prefix=f"{file_path.stem}_", dir=work_paths.tmp_dir) as file_tmp_dir:
        args = CleanerValidatorArgs(
            source_id=file_source_id,
            qsv_cmd=work_paths.qsv_cmd,
            xsv_file_path=file_path,
            header_file_path=work_paths.header_file_path,
            first_pass_schema=settings.first_pass_schema,
            post_norm_schema=settings.validated_schema,
            tmp_dir_path=Path(file_tmp_dir),
            qsv_output_dir_path=file_qsv_output_dir,
            validated_file_dir_path=file_validated_dir,
            **settings.parsing_config,
        )

        validated_file_name = clean_validate_file(args)

    buffer = DictBuffer(
        max_items=settings.buffer_size,
        schema_map=settings.schema_map,
        use_pyarrow=settings.use_pyarrow,
    )

    if validated_file_name:
        properties = settings.validated_schema.jsonschema.get("properties", {})
        transform = settings.get_table_transform(settings.table_name)
        pa_schema = settings.schema_map.registry.get(settings.table_name) if settings.schema_map else None
        use_pyarrow = settings.use_pyarrow and transform is None
        items = read_validated_rows(
            file_validated_dir / validated_file_name,
            properties,
            delimiter=args.delimiter,
            quote=args.quote,
            escape=args.escape,
            chunk_size=settings.buffer_size,
            use_pyarrow=use_pyarrow,
            pa_schema=pa_schema,
        )
        if use_pyarrow:
            for table in items:
                yield dlt.mark.with_table_name(table, settings.table_name)
        else:
            for row in items:
                yield from buffer.add_items(
                    route_and_coerce_record(
                        row,  # type: ignore[arg-type]
                        settings.table_name,
                        transform=transform,
                        schema_map=settings.schema_map,
                    )
                )
    elif not args.errors:
        args.errors.append(
            ErrorRecord(
                file=args.file_name,
                message="File failed cleaning/validation with no rows surviving; see qsv output directory for details.",
            )
        )

    for error in args.errors:
        yield from buffer.add_item(REJECTED_TABLE_NAME, error.to_rejected_row(settings.table_name))

    yield from buffer.flush()


def run_xsv_pipeline(settings: XsvIngestSettings) -> LoadInfo | None:
    """Run the XSV ingestion pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: XsvIngestSettings
    :return: load information for the pipeline
    :rtype: LoadInfo | None
    """
    qsv_cmd = qsv_check()

    with (
        TemporaryDirectory(prefix="xsv_tmp_") as tmp_dir,
        TemporaryDirectory(prefix="xsv_qsv_output_") as qsv_output_dir,
        TemporaryDirectory(prefix="xsv_validated_") as validated_dir,
        TemporaryDirectory(prefix="xsv_download_") as download_dir,
    ):
        work_paths = XsvWorkPaths(
            qsv_cmd=qsv_cmd,
            header_file_path=generate_header(settings.validated_schema, Path(tmp_dir)),
            tmp_dir=Path(tmp_dir),
            qsv_output_dir=Path(qsv_output_dir),
            validated_dir=Path(validated_dir),
            download_dir=Path(download_dir),
        )

        xsv_resource = build_per_file_filesystem_resource(
            settings,
            _process_xsv_file,
            work_paths,
            name="xsv_reader",
            schema_contract=DEFAULT_SCHEMA_CONTRACT,
        )

        return run_pipeline(
            settings=settings,
            resource=xsv_resource,
            **build_file_ingest_kwargs(settings, PIPELINE_NAME),
        )


cli = PipelineCli(XsvIngestSettings, run_xsv_pipeline)


if __name__ == "__main__":
    cli()
