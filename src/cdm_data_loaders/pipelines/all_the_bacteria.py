"""All The Bacteria bulk data downloader.

Project documentation: https://allthebacteria.org/

Batch downloading from OSF: https://allthebacteria.org/docs/osf_downloads/

all_atb_files.tsv: https://osf.io/xv7q9/files/r6gcp (or Rg6cp, casing varies)

"""

import csv
import logging
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any

import dlt
from dlt.extract.items import DataItemWithMeta
from dlt.sources.helpers.rest_client.client import RESTClient
from frozendict import frozendict
from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.pipelines.core import (
    run_cli,
    run_pipeline,
)
from cdm_data_loaders.pipelines.cts_defaults import DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings
from cdm_data_loaders.utils.download.sync_client import FileDownloader

logger = logging.getLogger("dlt")


DATASET_NAME = "all_the_bacteria"
ALL_FILES_TSV_FILE_ID = "R6gcp"
ALL_ATB_FILE_NAME = "all_atb_files.tsv"
REGEX_FILE = "filters.txt"
ATB_VERSION = "2025-05"

ARG_ALIASES = frozendict(
    {
        "version": ["-v", "--version"],
        "pattern_file": ["-p", "--pattern-file", "--pattern_file"],
    },
)

# project parts needed:
PROJECT_PARTS = ["Annotation/Bakta", "Assembly", "Metadata"]
PROJECT_PART_REGEX = re.compile(f"^AllTheBacteria/({'|'.join(PROJECT_PARTS)})")

EXPECTED_ATB_FIELDNAMES = ["project", "project_id", "filename", "url", "md5", "size(MB)"]
REQUIRED_ATB_FIELDNAMES = {"project", "filename", "url", "md5"}


class AtbSettings(CtsSettings):
    """Configuration for running the AllTheBacteria import pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT, cli_prog_name="all_the_bacteria")

    version: str = Field(
        default=ATB_VERSION,
        description="Name of the current AllTheBacteria version",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["version"]]),
    )

    pattern_file: str | None = Field(
        default=None,
        description="Path, relative to the input dir, of a file containing patterns to match when downloading ATB files",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES["pattern_file"]]),
    )

    @computed_field
    @property
    def raw_data_dir(self) -> str:
        """Directory in which to save the raw data files that are downloaded.

        Set to the output directory / "raw_data" / version.
        """
        return str(Path(self.output) / "raw_data" / self.version)

    @computed_field
    @property
    def pattern_matches(self) -> re.Pattern:
        """The regular expression pattern to be used to select files for download.

        If a pattern_file is supplied, it will read in the file at {input_dir}/{pattern_file}
        and convert the contents into a regular expression. If no file is supplied or the file is empty
        or does not contain any content, the default PROJECT_PART_REGEX will be used instead.
        """
        if self.pattern_file:
            pattern_file = Path(self.input_dir) / self.pattern_file
            regex = load_patterns(pattern_file)
            if regex is not None:
                return regex
        # return the default
        return PROJECT_PART_REGEX


def load_patterns(pattern_file: Path) -> re.Pattern | None:
    """Load the pattern file and convert it into a set of regexes."""
    patterns = []
    try:
        for line in pattern_file.read_text(encoding="utf-8").splitlines():
            trimmed_line = line.strip()
            # skip blank lines
            if not trimmed_line:
                continue
            patterns.append(
                re.escape(trimmed_line[:-1]) + ".*" if trimmed_line.endswith("*") else re.escape(trimmed_line)
            )

        if patterns:
            return re.compile("^(" + "|".join(patterns) + ")$")
    except Exception:
        logger.exception("Could not load patterns from %s", str(pattern_file))

    return None


def download_atb_index_tsv(settings: AtbSettings) -> Path:
    """Download the ATB file index TSV file from the OSF and save it to disk.

    :param settings: pipeline config
    :type settings: AtbSettings
    :raises RuntimeError: if the download URL cannot be found
    :return: path to the downloaded file
    :rtype: Path
    """
    # make sure that the directory structure to save the file in can be written to
    raw_data_dir = Path(settings.raw_data_dir)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    # get the all_atb_files.tsv file info from the OSF API and retrieve the download link
    osf_client = RESTClient(
        base_url="https://api.osf.io/v2/",
        headers={"accept": "application/json"},
        data_selector="data",
    )
    resp = osf_client.get(f"https://api.osf.io/v2/files/{ALL_FILES_TSV_FILE_ID}/")
    resp.raise_for_status()

    resp_json = resp.json()
    all_files_tsv_download = resp_json.get("data", {}).get("links", {}).get("download")
    if all_files_tsv_download is None:
        logger.error("Could not find download URL in OSF API response:")
        logger.error(resp_json)
        err_msg = f"Could not find download URL in response from 'https://api.osf.io/v2/files/{ALL_FILES_TSV_FILE_ID}/'"
        raise RuntimeError(err_msg)

    atb_files_tsv = raw_data_dir / "all_atb_files.tsv"
    # download the file listing and save it
    FileDownloader().download(all_files_tsv_download, atb_files_tsv)
    return atb_files_tsv


def get_file_download_links(settings: AtbSettings, atb_files_tsv: Path) -> Generator[list[dict[str, Any]], Any]:
    """Parse the ATB file index TSV and to yield a list of files to download.

    :param settings: pipeline config
    :type settings: AtbSettings
    :param atb_files_tsv: path to the ATB file index TSV file
    :type atb_files_tsv: Path
    :yield: list of fields to download
    :rtype: Generator[list[dict[str, Any]], Any]
    """
    pattern_to_match = settings.pattern_matches
    with atb_files_tsv.open() as index_file:
        reader = csv.DictReader(index_file, delimiter="\t")
        all_lines = list(reader)
        if not all_lines:
            err_msg = f"No valid TSV data found in {atb_files_tsv!s}"
            logger.error(err_msg)
            raise RuntimeError(err_msg)

        if reader.fieldnames != EXPECTED_ATB_FIELDNAMES:
            err_msg = f"ATB file index TSV headers have changed.\nExpected: {EXPECTED_ATB_FIELDNAMES}\nGot: {reader.fieldnames}"
            logger.warning(err_msg)
            # do we have the essentials? project, filename, url, md5
            missing_required_fields = [f for f in REQUIRED_ATB_FIELDNAMES if f not in (reader.fieldnames or [])]
            if missing_required_fields:
                err_msg = f"Missing required ATB file index TSV headers: {sorted(missing_required_fields)}"
                logger.error(err_msg)
                raise RuntimeError(err_msg)

        files_to_download = [row for row in all_lines if pattern_to_match.match(row["project"])]

        yield files_to_download


def osf_file_downloader(settings: AtbSettings, atb_file_list: list[dict[str, Any]]) -> Generator[DataItemWithMeta, Any]:
    """Download files from OSF to the local storage space.

    :param settings: pipeline config
    :type settings: Settings
    :param atb_file_list: list of dictionaries
    :type atb_file_list: list[dict[str, Any]]
    """
    client = FileDownloader()
    raw_data_dir = Path(settings.raw_data_dir)
    successful_downloads = []
    for f in atb_file_list:
        try:
            save_path = raw_data_dir / f["filename"]
            client.download(f["url"], save_path, expected_checksum=f["md5"], checksum_fn="md5")
            f["path"] = str(save_path)
            successful_downloads.append(f)
        except Exception as e:
            # do something!
            err_msg = f"Could not download file from {f['url']}: {e!s}"
            logger.exception(err_msg)
            continue

    yield dlt.mark.with_table_name(successful_downloads, "downloaded_files")


@dlt.resource(name="atb_file_list")
def atb_file_list(settings: AtbSettings) -> Generator[list[dict[str, Any]], Any, Any]:
    """Generate a list of files to download from the list of all ATB files."""
    atb_files_tsv = download_atb_index_tsv(settings)
    return get_file_download_links(settings, atb_files_tsv)


@dlt.transformer(name="file_downloader", data_from=atb_file_list, parallelized=True)
def file_downloader(
    atb_file_list: list[dict[str, Any]],
    settings: AtbSettings,
) -> Generator[DataItemWithMeta, Any]:
    """Download ATB files to disk.

    :param settings: pipeline config
    :type settings: Settings
    :param atb_file_list: list of files to download
    :type atb_file_list: list[dict[str, Any]]
    :return: output of the osf_file_downloader
    :rtype: Generator[DataItemWithMeta]
    """
    return osf_file_downloader(settings, atb_file_list)


def run_atb_pipeline(settings: AtbSettings) -> None:
    """Run the AllTheBacteria pipeline.

    :param settings: configuration for the pipeline
    :type settings: AtbSettings
    """
    atb_file_list.bind(settings)
    file_downloader.bind(settings)

    pipeline_kwargs: dict[str, Any] = {
        "pipeline_name": DATASET_NAME,
        "dataset_name": DATASET_NAME,
    }

    run_pipeline(
        settings=settings,
        resource=file_downloader,
        destination_kwargs={"max_table_nesting": 0},
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs=None,
    )


def cli() -> None:
    """CLI interface for the AllTheBacteria importer pipeline."""
    run_cli(AtbSettings, run_atb_pipeline)


if __name__ == "__main__":
    cli()
