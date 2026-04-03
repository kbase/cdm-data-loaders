"""Pipeline to import data from the NCBI API."""

import logging
import os
from collections.abc import Generator
from functools import partial
from itertools import islice
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import dlt
from dlt.extract.items import DataItemWithMeta
from dlt.sources.helpers.requests import Response
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import (
    JSONResponseCursorPaginator,
)
from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import CliSuppress, SettingsConfigDict

from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
)
from cdm_data_loaders.pipelines.cts_defaults import (
    BatchedFileInputSettings,
)
from cdm_data_loaders.utils.file_system import BatchCursor

DATASET_NAME = "ncbi_rest_api"

NCBI_API_KEY = os.environ.get("NCBI_API_KEY") or "DEMO_KEY"

# Max number of items to request per page from the NCBI REST API (max allowed is 1000).
MAX_RESULTS_PER_PAGE = 1000

DATASET = "dataset"
ANNOTATION = "annotation"

dlt_logger = logging.getLogger("dlt")

REST_CLIENT_HOOKS = {}


class Settings(BatchedFileInputSettings):
    """Configuration for running the NCBI REST API import pipeline."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_prog_name="ncbi_rest_api",
        cli_exit_on_error=False,
        cli_ignore_unknown_args=True,
    )

    batch_size: int = Field(
        default=MAX_RESULTS_PER_PAGE,
        description="Number of IDs to send in each request to the NCBI REST API.",
        validation_alias=AliasChoices("b", "batch-size", "batch_size"),
        ge=1,
        le=MAX_RESULTS_PER_PAGE,
    )

    pipeline_dir: CliSuppress[Path | None] = Field(
        default=None, description="Custom directory to save pipeline metadata to."
    )

    use_output_dir_for_pipeline_metadata: bool = Field(
        default=False,
        description="If true, use the output directory for pipeline metadata.",
        validation_alias=AliasChoices("p", "pipeline-dir", "pipeline_dir", "use_output_dir_for_pipeline_metadata"),
    )

    dev_mode: bool = Field(
        default=False,
        description="Whether to run the pipeline in dev mode, which saves raw API responses to disk and disables compression for easier debugging.",
        validation_alias=AliasChoices("dev", "dev_mode", "dev-mode"),
    )

    @model_validator(mode="after")
    def set_output_dir(self) -> "Settings":
        """Set a default for the output directory if not set."""
        if self.output is None:
            self.output = dlt.config[f"destination.{self.destination}.bucket_url"]
        else:
            dlt.config[f"destination.{self.destination}.bucket_url"] = self.output
        return self

    @model_validator(mode="after")
    def set_pipeline_dir_default(self) -> "Settings":
        """Set a default for the pipeline directory if not set."""
        if self.use_output_dir_for_pipeline_metadata:
            p_dir = self.output
            self.pipeline_dir = Path(p_dir) / ".dlt_conf"  # type: ignore[reportArgumentType]
        return self


def generate_file_path_name_from_url(config: Settings, url_string: str) -> Path:
    """Given an NCBI URL, generate a save directory and file name for the raw HTTP responses.

    :param url_string: the URL
    :type url_string: str
    :return: path for the save dir
    :rtype: Path
    """
    raw_data_dir = Path(config.output) / "raw_responses"  # type: ignore[reportArgumentType]
    url = urlparse(url_string)
    # retrieve the page token
    page_token = "".join(parse_qs(url.query).get("page_token", []))
    # save_parts 0 is the broad type, e.g. gene, protein, genome
    # save_parts 1 is the type of PID
    # save_parts 2 is the value of the PID
    # any subsequent parts are the specific data type being accessed
    save_parts = url.path.replace("/datasets/v2/", "").split("/")
    # make a directory for this ID
    save_dir = raw_data_dir / save_parts[3] / save_parts[2]
    save_dir.mkdir(parents=True, exist_ok=True)
    # file name is the PID plus optional page token if this was a multi-request PID
    file_name = f"{save_parts[2]}__{page_token}.json" if page_token else f"{save_parts[2]}.json"
    return save_dir / file_name


def save_raw_response(config: Settings, response: Response, *_: Any, **__: Any) -> Response:  # noqa: ANN401
    """Save the response content to disk.

    :param response: HTTP Response object
    :type response: Response
    :return: the response
    :rtype: Response
    """
    file_path = generate_file_path_name_from_url(config, response.url)
    file_path.write_bytes(response.content)
    return response


ncbi_genome_client = RESTClient(
    base_url="https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/",
    headers={"accept": "application/json"},
    auth=APIKeyAuth(name="api_key", api_key=NCBI_API_KEY),
    paginator=JSONResponseCursorPaginator(cursor_path="next_page_token", cursor_param="page_token"),
    data_selector="reports",
)


def get_assembly_reports(assembly_id_list: list[str]) -> dict[str, Any]:
    """Retrieve dataset and annotation reports for a list of IDs from the NCBI datasets API.

    :param assembly_id_list: list of IDs to retrieve data for
    :type assembly_id_list: list[str]
    :return: dictionary with keys DATASET and ANNOTATION containing dataset and annotation data respectively.
    :rtype: dict[str, Any]
    """
    if not assembly_id_list:
        return {}

    # N.b. invalid IDs will not be present in dataset_reports
    dataset_reports = get_dataset_reports(assembly_id_list)
    annotation_reports = {assembly_id: get_annotation_report(assembly_id) for assembly_id in assembly_id_list}

    # ensure every assembly_id in the list has either the downloaded dataset_report or None
    return {
        DATASET: {assembly_id: dataset_reports.get(assembly_id) for assembly_id in assembly_id_list},
        ANNOTATION: {assembly_id: annotation_reports.get(assembly_id) for assembly_id in assembly_id_list},
    }


def get_dataset_reports(assembly_id_list: list[str]) -> dict[str, None | dict[str, Any]]:
    """Fetch the dataset report for a list of assemblies from the NCBI datasets REST API."""
    if not assembly_id_list:
        return {}

    dlt_logger.info("fetching dataset reports for:\n%s", ", ".join(sorted(assembly_id_list)))
    assembly_dataset_reports = []
    for page in ncbi_genome_client.paginate(
        f"{'%2C'.join(assembly_id_list)}/dataset_report",
        params={
            "page_size": MAX_RESULTS_PER_PAGE,
        },
        hooks=REST_CLIENT_HOOKS,  # type: ignore[reportArgumentType]
    ):
        assembly_dataset_reports.extend(page)

    # return dataset reports, indexed by assembly_id
    # invalid IDs are silently dropped by the API
    datasets = {report.get("accession"): report for report in assembly_dataset_reports}
    # fill in the missing gaps in assembly_id_list with None
    return {assembly_id: datasets.get(assembly_id) for assembly_id in assembly_id_list}


def get_annotation_report(assembly_id: str) -> list[dict[str, Any]] | None:
    """Fetch the annotation report for an assembly from the NCBI datasets REST API."""
    dlt_logger.info("fetching annotation report for %s", assembly_id)
    page_data = []
    for page in ncbi_genome_client.paginate(
        f"{assembly_id}/annotation_report",
        params={
            "page_size": MAX_RESULTS_PER_PAGE,
        },
        hooks=REST_CLIENT_HOOKS,  # type: ignore[reportArgumentType]
    ):
        page_data.extend(page)
    # page_data is empty if the ID is invalid
    return page_data or None


@dlt.resource(name="assembly_list")
def assembly_list(config: Settings) -> Generator[list[str], Any, Any]:
    """List of assemblies to fetch."""
    batcher = BatchCursor(config.input_dir, batch_size=1)
    while files := batcher.get_batch():
        for file_path in files:
            with file_path.open() as f:
                while next_chunk := list(islice(f, config.batch_size)):
                    yield [line.strip() for line in next_chunk if line.strip()]


@dlt.transformer(name="assembly_reports", data_from=assembly_list, max_table_nesting=0)
def assembly_reports(
    assembly_id_list: list[str],
) -> Generator[dict[str, dict[str, dict[str, Any]] | list[dict[str, Any]]], Any]:
    """Fetch reports for a list of assemblies from the NCBI datasets REST API."""
    yield get_assembly_reports(assembly_id_list)


@dlt.transformer(name="assembly_report_parser", data_from=assembly_reports, max_table_nesting=0)
def assembly_report_parser(
    assembly_reports: dict[str, dict[str, Any]],
) -> Generator[DataItemWithMeta, Any]:
    """Parse and export assembly data from the REST API.

    :param assembly_reports: output from the dataset_report and annotation_report API endpoint, indexed by assembly_id
    :type assembly_reports: dict[str, dict[str, None | dict[str, Any]]]
    :yield: table rows for assemblies
    :rtype: Generator[DataItemWithMeta, Any]
    """
    if not assembly_reports:
        return

    dataset_reports: dict[str, dict[str, Any]] = assembly_reports.get(DATASET)  # type: ignore[reportAssignmentType]
    annotation_reports: dict[str, list[dict[str, Any]]] = assembly_reports.get(ANNOTATION)  # type: ignore[reportAssignmentType]
    # yield the raw data to save as tables
    yield dlt.mark.with_table_name(
        [{"assembly_id": assembly_id, **(report or {})} for assembly_id, report in dataset_reports.items()],
        f"{DATASET}_report",
    )
    tr_annotation_rows = [
        {"assembly_id": aid, **report}
        for aid, report_list in annotation_reports.items()
        for report in (report_list or [])
    ]
    # save the rows from the annotation reports
    yield dlt.mark.with_table_name(tr_annotation_rows, f"{ANNOTATION}_report")


def run_ncbi_pipeline(config: Settings) -> None:
    """Run the NCBI datasets API pipeline.

    :param config: configuration for the pipeline
    :type config: Settings
    """
    if config.dev_mode:
        REST_CLIENT_HOOKS["response"] = [partial(save_raw_response, config)]
    # update config according to the value of dev_mode
    dlt.config["normalize.data_writer.disable_compression"] = config.dev_mode

    # ensure that assembly_list has the correct config bound before running the pipeline
    assembly_list.bind(config)

    pipeline_kwargs = {
        "pipeline_name": DATASET_NAME,
        "dataset_name": DATASET_NAME,
        "dev_mode": config.dev_mode,
    }
    # set the output directory for all the pipeline gubbins
    if config.use_output_dir_for_pipeline_metadata:
        pipeline_kwargs["pipelines_dir"] = config.pipeline_dir

    run_pipeline(
        config=config,
        resource=[assembly_report_parser],
        destination_kwargs={"max_table_nesting": 0},
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs=None,
    )


def cli() -> None:
    """CLI interface for the UniProt KB importer pipeline."""
    run_cli(Settings, run_ncbi_pipeline)


if __name__ == "__main__":
    cli()
