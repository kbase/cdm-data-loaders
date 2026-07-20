"""Pipeline to import assembly summary files from NCBI.

Downloads the most recent assembly summary files from NCBI, validates them against a JSON schema,
and loads them into a lakehouse as Iceberg tables.


"""

import csv
import json
import os
import re
import subprocess
from collections.abc import Generator
from logging import Logger, getLogger
from pathlib import Path
from typing import Annotated, Any, Final
from tempfile import TemporaryDirectory
from cdm_data_loaders.validation.xsv import validate
import dlt
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta
from dlt.sources.filesystem import filesystem, read_csv, read_csv_duckdb
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client.client import RESTClient
from frictionless.schemes import S3Loader
from frozendict import frozendict
from jsonschema import Draft202012Validator
from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import SettingsConfigDict
import datetime as dt
from cdm_data_loaders.core.fields import DevMode, DltConfig, Output, UseDestination, generate_aliases
from cdm_data_loaders.core.settings import DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings, LoggerSettings
from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
)
from cdm_data_loaders.utils.download.sync_client import FileDownloader
from cdm_data_loaders.utils.s3 import stream_to_s3, upload_file
from cdm_data_loaders.validation.xsv import validate as xsv_validate

logger: Logger = getLogger(__name__)

DATASET_NAME: Final[str] = "ncbi_assembly_summary"

REFSEQ: Final[str] = "refseq"
GENBANK: Final[str] = "genbank"

S3_BASE_URL = "s3://cdm-lake/tenant-general-warehouse/refdata/datasets/ncbi/"

NCBI_BASE_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/"

ASSEMBLY_SUMMARY_SCHEMA_PATH: Final[Path] = Path(
    "src/cdm_data_loaders/parsers/ncbi/assembly_summary/assembly_summary-original.schema.json"
)

INVALID_ROWS_THRESHOLD: Final[int] = 20

S3_REGEX: re.Pattern[str] = re.compile("^s3a?://")

NCBI_RELEASE_URLS: Final[dict[str, str]] = {
    REFSEQ: "https://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE_NUMBER",
    GENBANK: "https://ftp.ncbi.nlm.nih.gov/genbank/GB_Release_Number",
    "INVALID_URL": "https://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE",
}


# DLT_ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG__URI
iceberg_catalog_config = {
    "uri": os.environ.get("POLARIS_CATALOG_URI"),
    "type": "rest",
    "warehouse": "tenant_refdata",
    "credential": os.environ.get("POLARIS_CREDENTIAL"),
    "header.X-Iceberg-Access-Delegation": "vended-credentials",
    "scope": "PRINCIPAL_ROLE:ALL",
    "token-refresh-enabled": True,
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
}


TENANT_DB_REGEX: re.Pattern[str] = re.compile(r"^\w+\.\w+$")

ALIASES = frozendict(
    {
        "tenant_db": generate_aliases("tenant_db"),
    }
)


Tenant = Annotated[
    str | None,
    Field(
        default=None,
        description="The tenant in which files and database tables should be stored. If empty, defaults to the user namespace",
    ),
]


Database = Annotated[
    str,
    Field(
        default="ncbi_assembly_summary",
        description="The namespace or database to use for tabular output from the pipeline.",
        pattern=TENANT_DB_REGEX,
    ),
]


class NcbiAssemblySummarySettings(CtsSettings):
    """Configuration for running the NCBI Assembly Summary import pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT, cli_prog_name="ncbi_assembly_summary")

    dev_mode: DevMode
    dlt_config: DltConfig
    output_dir: Output
    tenant: Tenant
    database: Database
    use_destination: UseDestination
    working_dir: Path

    @computed_field
    @property
    def downloads_dir(self) -> Path:
        """Directory to store raw assembly summary files downloaded from NCBI."""
        return self.working_dir / "downloads"

    @computed_field
    @property
    def validated_data_dir(self) -> Path:
        """Directory to store validated assembly summary files."""
        return self.working_dir / "validated"

    @computed_field
    @property
    def year_month(self) -> str:
        """The current year and month, hyphen-separated."""
        return dt.datetime.now(tz=dt.UTC).strftime("%Y-%m")

    @computed_field
    @property
    def tenant_db(self) -> str:
        return f"{self.tenant}.{self.database}"


def get_release(release_type: str) -> str:
    """Get the current release number for the given release type.

    :param release_type: the release type to get, either "genbank" or "refseq"
    :type release_type: str
    :return: the release number for the current release
    :rtype: str
    """
    if release_type not in NCBI_RELEASE_URLS:
        err_msg = f"Invalid release type: {release_type}. Must be {' or '.join(NCBI_RELEASE_URLS.keys())}."
        raise ValueError(err_msg)

    resp = requests.get(NCBI_RELEASE_URLS[release_type], timeout=10)
    resp.raise_for_status()
    if release := resp.text.strip():
        return release

    err_msg = f"Could not get release number for {release_type} from {NCBI_RELEASE_URLS[release_type]}"
    raise ValueError(err_msg)


def get_release_type_for_file(file_name: str) -> str | None:
    """Determine which NCBI release type (genbank/refseq) a file belongs to.

    :param file_name: the file name to inspect
    :type file_name: str
    :return: "genbank", "refseq", or None (e.g. for the README)
    :rtype: str | None
    """
    for rls_type in [GENBANK, REFSEQ]:
        if rls_type in file_name:
            return rls_type
    return None


def get_s3_destination_dir(subdir: str, file_name: str, year_month: str, releases: dict[str, str]) -> str:
    """Build the destination S3 path for a given file.

    Layout:
        {S3_BASE_URL}/{subdir}/{year_month}/{release_type}-{release_number}/{file_name}
    or, for files with no associated release (e.g. the README):
        {S3_BASE_URL}/{subdir}/{year_month}/{file_name}

    :param subdir: either "raw_data" or "derived"
    :type subdir: str
    :param file_name: the name of the file being uploaded
    :type file_name: str
    :param year_month: the current year-month, e.g. "2024-06"
    :type year_month: str
    :param releases: mapping of release type -> release number
    :type releases: dict[str, str]
    :return: the fully qualified S3 destination directory
    :rtype: str
    """
    base = f"{S3_BASE_URL}{subdir}/{year_month}"
    release_type = get_release_type_for_file(file_name)
    if release_type is None:
        return f"{base}"
    return f"{base}/{release_type}-{releases[release_type]}"


def _table_name_for_file(file_name: str) -> str:
    """Derive an Iceberg table name from a cleaned assembly summary file name.

    :param file_name: the file name, e.g. "assembly_summary_genbank.tsv"
    :type file_name: str
    :return: the table name, e.g. "assembly_summary_genbank"
    :rtype: str
    """
    return Path(file_name).stem


def ncbi_file_downloader(
    settings: NcbiAssemblySummarySettings, ncbi_file_list: list[str]
) -> Generator[DataItemWithMeta, Any]:
    """Download assembly summary files from the NCBI website.

    :param settings: pipeline config
    :type settings: Settings
    :param ncbi_file_list: info about files to transfer, as a list of dictionaries
    :type ncbi_file_list: list[dict[str, Any]]
    """
    client = FileDownloader()
    downloads_dir = settings.downloads_dir
    downloads_dir.mkdir(parents=True, exist_ok=True)

    releases = {release_type: get_release(release_type) for release_type in (GENBANK, REFSEQ)}

    successful_downloads: list[dict[str, Any]] = []
    for file_name in ncbi_file_list:
        url = f"{NCBI_BASE_URL}{file_name}"
        save_path = downloads_dir / file_name
        try:
            client.download(url=url, destination=save_path)
            successful_downloads.append({"source_url": url, "file_name": file_name, "path": save_path})
        except Exception as e:
            err_msg = f"Could not download file from {url}: {e!s}"
            logger.exception(err_msg)
            continue

    # once all files are downloaded, clean and validate the assembly_summary_* files
    cleaned_dir = settings.working_dir / "cleaned"
    ok_files, errors = validate_assembly_summary(downloads_dir, cleaned_dir)
    for error in errors:
        logger.error("Validation failed with %s invalid rows: %s", error.invalid_rows, error)

    yield dlt.mark.with_table_name(successful_downloads + ok_files, "downloaded_files")

    # copy the raw downloads to S3_BASE_DIR / raw_data
    for file_info in successful_downloads:
        dest = get_s3_destination_dir("raw_data", file_info["file_name"], settings.year_month, releases)
        upload_file(file_info["path"], dest, file_info["file_name"], show_progress=False)

    # copy the cleaned/validated files to S3_BASE_DIR / derived
    for file_info in ok_files:
        dest = get_s3_destination_dir("derived", file_info["file_name"], settings.year_month, releases)
        upload_file(file_info["path"], dest, file_info["file_name"], show_progress=False)


@dlt.resource(name="ncbi_file_list")
def ncbi_file_list() -> Generator[list[str], Any, Any]:
    """Assembly summary files to be downloaded."""
    yield [
        "README_assembly_summary.txt",
        "assembly_summary_genbank.txt",
        "assembly_summary_genbank_historical.txt",
        "assembly_summary_refseq.txt",
        "assembly_summary_refseq_historical.txt",
    ]


@dlt.transformer(name="file_downloader", data_from=ncbi_file_list, parallelized=True)
def file_downloader(
    ncbi_file_list: list[str],
    settings: NcbiAssemblySummarySettings,
) -> Generator[DataItemWithMeta, Any]:
    """Download NCBI assembly summary files to disk.

    :param settings: pipeline config
    :type settings: Settings
    :param ncbi_file_list: list of files to download
    :type ncbi_file_list: list[dict[str, Any]]
    :return: output of the osf_file_downloader
    :rtype: Generator[DataItemWithMeta]
    """
    return ncbi_file_downloader(settings, ncbi_file_list)


def validate_assembly_summary(save_dir: Path, output_dir: Path) -> tuple[list[dict[str, Any]], list[Any]]:
    """Clean and validate downloaded assembly summary files.

    Runs the assembly_summary_cleaner.sh script over ``save_dir``, which produces
    ``assembly_summary_*-valid.tsv`` and ``assembly_summary_*-errors.tsv`` files for each
    downloaded ``assembly_summary_*`` file. Each ``*-valid.tsv`` file is then further validated
    against the JSON schema.

    :param save_dir: directory containing the downloaded assembly summary files
    :type save_dir: Path
    :param output_dir: directory to write cleaned/validated output files to
    :type output_dir: Path
    :return: a tuple of (ok_files, errors), where ok_files is a list of dicts describing files
        that passed validation, and errors is a list of validation results for files that did not
    :rtype: tuple[list[dict[str, Any]], list[Any]]
    """
    # run the cleaner script
    subprocess.run(
        [
            "/bin/sh",
            "./scripts/assembly_summary_cleaner.sh",
            "-d",
            str(save_dir),
            "-p",
            "assembly_summary_",
            "-n",
            "38",
        ],
        check=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    errors: list[Any] = []
    ok_files: list[dict[str, Any]] = []
    for file_path in save_dir.glob("*-valid.tsv"):
        output = xsv_validate(
            file_path,
            schema=ASSEMBLY_SUMMARY_SCHEMA_PATH,
            output_path=output_dir,
            delimiter="\t",
            null_strings=set(),
            summary=True,
        )
        if output.invalid_rows and output.invalid_rows > INVALID_ROWS_THRESHOLD:
            errors.append(output)
            continue

        base_name = file_path.stem.removesuffix("-valid")
        ok_files.append(
            {
                "source_url": str(file_path),
                "file_name": f"{base_name}.tsv",
                "path": output.valid_records_file,
            }
        )
    return ok_files, errors


def run_assembly_summary_pipeline(settings: NcbiAssemblySummarySettings) -> None:
    """Run the NCBI assembly summary pipeline.

    :param settings: configuration for the pipeline
    :type settings: NcbiAssemblySummarySettings
    """
    # ensure that assembly_list has the correct settings bound before running the pipeline
    file_downloader.bind(settings)

    pipeline_kwargs = {
        "pipeline_name": DATASET_NAME,
        "dataset_name": DATASET_NAME,
    }
    # first stage: download files, validate, move validated files to validated data dir
    # save assembly summary files to appropriate raw data dir

    # first stage: download files, validate, and stage the raw/cleaned files in S3
    run_pipeline(
        settings=settings,
        resource=file_downloader,
        destination_kwargs=None,
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs={"table_format": "iceberg"},
    )

    # # second stage: parse the validated files and load them into the destination
    # run_pipeline(
    #     settings=settings,
    #     resource=file_downloader,
    #     destination_kwargs=None,
    #     pipeline_kwargs=pipeline_kwargs,
    #     pipeline_run_kwargs={"table_format": "iceberg"},
    # )


def cli() -> None:
    """CLI interface for the NCBI Assembly Summary importer pipeline."""
    with TemporaryDirectory() as temp_dir:
        run_cli(
            NcbiAssemblySummarySettings,
            settings_kwargs={"working_dir": temp_dir},
            pipeline_fn=run_assembly_summary_pipeline,
        )


if __name__ == "__main__":
    cli()
