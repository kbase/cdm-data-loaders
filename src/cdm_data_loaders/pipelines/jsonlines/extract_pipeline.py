"""JSONL load pipeline for the KBase CTS.

Reads JSONL files. No validation occurs other than that records are valid JSON.

`table_name` names the destination table for all valid records; files
may live anywhere under `input_dir`. Records that fail JSON parsing go
to the `_rejected` table. Each rejected record includes the target table
name, source file, line number, raw text, and error detail.
"""

from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines.core import (
    PipelineCli,
    build_file_ingest_kwargs,
    build_filesystem_resource,
    run_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import (
    EXTRACT_PIPELINE_NAME,
    JsonlExtractSettings,
)
from cdm_data_loaders.readers.jsonlines import route_jsonl_lines


def run_jsonlines_pipeline(settings: JsonlExtractSettings) -> LoadInfo | None:
    """Run the JSONL pipeline.

    :param settings: pipeline configuration
    :type settings: JsonlExtractSettings
    :return: load information for the pipeline
    :rtype: LoadInfo | None
    """
    jsonl_resource = build_filesystem_resource(settings, route_jsonl_lines, name="jsonl_reader")

    return run_pipeline(
        settings=settings,
        resource=jsonl_resource,
        **build_file_ingest_kwargs(settings, EXTRACT_PIPELINE_NAME, fallback_dataset_name=True),
    )


cli = PipelineCli(JsonlExtractSettings, run_jsonlines_pipeline)


if __name__ == "__main__":
    cli()
