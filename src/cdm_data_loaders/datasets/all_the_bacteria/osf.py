"""OSF index parsing, pattern matching, and file download helpers for AllTheBacteria."""

import csv
import re
from collections.abc import Generator
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

from cdm_data_loaders.io.sync_client import FileDownloader, fetch_json

logger: Logger = getLogger(__name__)

DATASET_NAME: Final[str] = "all_the_bacteria"
ALL_FILES_TSV_FILE_ID: Final[str] = "R6gcp"
ALL_ATB_FILE_NAME: Final[str] = "all_atb_files.tsv"
ATB_VERSION: Final[str] = "2025-05"
OSF_TIMEOUT_SECONDS: Final[int] = 60

PROJECT_PARTS: Final[tuple[str, ...]] = ("Annotation/Bakta", "Assembly", "Metadata")
PROJECT_PART_REGEX: Final[re.Pattern[str]] = re.compile(f"^AllTheBacteria/({'|'.join(PROJECT_PARTS)})")

EXPECTED_ATB_FIELDNAMES: Final[list[str]] = ["project", "project_id", "filename", "url", "md5", "size(MB)"]
REQUIRED_ATB_FIELDNAMES: Final[frozenset[str]] = frozenset({"project", "filename", "url", "md5"})


def load_patterns(pattern_file: Path) -> re.Pattern[str] | None:
    """Load a pattern file and compile non-empty lines into a combined regular expression.

    :param pattern_file: path to a text file with one literal or trailing-``*`` pattern per line
    :type pattern_file: Path
    :return: compiled regular expression, or ``None`` if the file cannot be read or has no non-blank lines
    :rtype: re.Pattern[str] | None
    """
    patterns: list[str] = []
    try:
        for line in pattern_file.read_text(encoding="utf-8").splitlines():
            trimmed_line = line.strip()
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


def download_osf_index_tsv(raw_data_dir: Path) -> Path:
    """Download the AllTheBacteria file index TSV from OSF into ``raw_data_dir``.

    :param raw_data_dir: local directory to save ``all_atb_files.tsv`` into
    :type raw_data_dir: Path
    :return: path to the downloaded TSV file
    :rtype: Path
    :raises RuntimeError: if the OSF API response omits the download link
    """
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    api_url = f"https://api.osf.io/v2/files/{ALL_FILES_TSV_FILE_ID}/"
    resp_json = fetch_json(
        api_url,
        timeout=OSF_TIMEOUT_SECONDS,
        extra_headers={"accept": "application/json"},
    )
    all_files_tsv_download = resp_json.get("data", {}).get("links", {}).get("download")
    if all_files_tsv_download is None:
        logger.error("Could not find download URL in OSF API response:")
        logger.error(resp_json)
        err_msg = f"Could not find download URL in response from '{api_url}'"
        raise RuntimeError(err_msg)

    atb_files_tsv = raw_data_dir / ALL_ATB_FILE_NAME
    FileDownloader().download(url=all_files_tsv_download, destination=atb_files_tsv)
    logger.info("Downloaded TSV index file.")
    return atb_files_tsv


def iter_atb_download_links(
    atb_files_tsv: Path,
    pattern_to_match: re.Pattern[str],
) -> Generator[list[dict[str, Any]]]:
    """Parse an ATB file index TSV and yield rows whose ``project`` matches ``pattern_to_match``.

    :param atb_files_tsv: path to the ATB file index TSV file
    :type atb_files_tsv: Path
    :param pattern_to_match: compiled regular expression matched against ``row["project"]``
    :type pattern_to_match: re.Pattern[str]
    :yield: list of matching TSV row dictionaries
    :rtype: Generator[list[dict[str, Any]]]
    :raises RuntimeError: if the TSV file is empty or missing required header columns
    """
    with atb_files_tsv.open(encoding="utf-8") as index_file:
        reader = csv.DictReader(index_file, delimiter="\t")
        all_lines = list(reader)
        if not all_lines:
            err_msg = f"No valid TSV data found in {atb_files_tsv!s}"
            logger.error(err_msg)
            raise RuntimeError(err_msg)

        if reader.fieldnames != EXPECTED_ATB_FIELDNAMES:
            warn_msg = (
                f"ATB file index TSV headers have changed.\n"
                f"Expected: {EXPECTED_ATB_FIELDNAMES}\nGot: {reader.fieldnames}"
            )
            logger.warning(warn_msg)
            missing_required_fields = [f for f in REQUIRED_ATB_FIELDNAMES if f not in (reader.fieldnames or [])]
            if missing_required_fields:
                err_msg = f"Missing required ATB file index TSV headers: {sorted(missing_required_fields)}"
                logger.error(err_msg)
                raise RuntimeError(err_msg)

        yield [row for row in all_lines if pattern_to_match.match(row["project"])]


def download_osf_files(
    raw_data_dir: Path,
    atb_file_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Download OSF files into ``raw_data_dir`` and return records that succeeded.

    :param raw_data_dir: base directory for downloaded raw files
    :type raw_data_dir: Path
    :param atb_file_list: list of file descriptor dictionaries with ``project``, ``filename``, ``url``, and ``md5``
    :type atb_file_list: list[dict[str, Any]]
    :return: list of successfully downloaded file records with ``path`` populated
    :rtype: list[dict[str, Any]]
    """
    client = FileDownloader()
    successful_downloads: list[dict[str, Any]] = []
    for file_info in atb_file_list:
        try:
            project_part = file_info["project"].removeprefix("AllTheBacteria").removeprefix("/").rstrip("/")
            save_path = raw_data_dir / project_part / file_info["filename"]
            client.download(
                url=file_info["url"],
                destination=save_path,
                expected_checksum=file_info["md5"],
                checksum_fn="md5",
            )
            file_info["path"] = str(save_path)
            successful_downloads.append(file_info)
        except Exception as e:
            err_msg = f"Could not download file from {file_info['url']}: {e!s}"
            logger.exception(err_msg)
            continue

    return successful_downloads
