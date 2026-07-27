"""Pipeline to import assembly summary files from NCBI.

Downloads the most recent assembly summary files from NCBI, validates them against a JSON schema,
and loads them into a lakehouse as Iceberg tables.


"""

import datetime as dt
import json
import os
import re
import subprocess
from collections.abc import Iterator
from logging import Logger, getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, Final

import pandas as pd
from dlt.sources import TDataItems
from dlt.sources.helpers import requests
from frictionless import Resource
from frozendict import frozendict
from pydantic import Field

from cdm_data_loaders.core.fields import generate_aliases
from cdm_data_loaders.pipelines.ncbi.config import (
    ASS_SUM_GENBANK,
    ASS_SUM_GENBANK_HIST,
    ASS_SUM_README,
    ASS_SUM_REFSEQ,
    ASS_SUM_REFSEQ_HIST,
    NCBI_FILES,
    NcbiReleaseMetadata,
)
from cdm_data_loaders.readers.jsonschema_xsv.transforms import apply_date_parse, apply_prefix, apply_split_prefix
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator import (
    CleanerValidatorArgs,
    clean_validate_file,
    generate_header,
)
from cdm_data_loaders.utils.download.sync_client import FileDownloader
from cdm_data_loaders.utils.s3 import upload_file
from cdm_data_loaders.validation.xsv import validate as xsv_validate

logger: Logger = getLogger(__name__)

DATASET_NAME: Final[str] = "ncbi_assembly_summary"

REFSEQ: Final[str] = "refseq"
GENBANK: Final[str] = "genbank"

S3_BASE_URL = "s3://cdm-lake/tenant-general-warehouse/refdata/datasets/ncbi/"

NCBI_BASE_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/"

CONFIG_SCHEMA_PATH: Final[Path] = Path("src/cdm_data_loaders/pipelines/ncbi/ncbi_release_metadata.json")

VALIDATION_ERRORS = ".validation-errors.tsv"
FIRST_PASS_SCHEMA = Path("src/cdm_data_loaders/parsers/ncbi/assembly_summary/assembly_summary.first-pass.schema.json")
POST_NORM_SCHEMA = Path("src/cdm_data_loaders/parsers/ncbi/assembly_summary/assembly_summary.original.schema.json")


ASSEMBLY_SUMMARY_SCHEMA_PATH: Final[Path] = Path(
    "src/cdm_data_loaders/parsers/ncbi/assembly_summary/assembly_summary-original.schema.json"
)

INVALID_ROWS_THRESHOLD: Final[int] = 20

S3_REGEX: re.Pattern[str] = re.compile("^s3a?://")

NCBI_RELEASE_URLS: Final[dict[str, str]] = {
    REFSEQ: "https://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE_NUMBER",
    GENBANK: "https://ftp.ncbi.nlm.nih.gov/genbank/GB_Release_Number",
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

ASS_SUM_SUBSTR: Final[str] = "assembly_summary_"

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
        description="The tenant in which files and database tables should be stored.",
    ),
]

TenantType = Annotated[str, Field(default="user", description="Whether or not the tenant is a group tenant or a user.")]

Database = Annotated[
    str,
    Field(
        default="ncbi_assembly_summary",
        description="The namespace or database to use for tabular output from the pipeline.",
        pattern=TENANT_DB_REGEX,
    ),
]

Releases = Annotated[
    dict[str, str],
    Field(
        default_factory=dict,
        description="The current genbank and refseq release IDs",
    ),
]


def generate_ncbi_release_metadata(raw_data_dir: Path, validated_data_dir: Path, date: dt.date) -> NcbiReleaseMetadata:

    # get the release #s
    releases = {release_type: get_release(release_type) for release_type in NCBI_RELEASE_URLS}

    config = {
        "date": date.isoformat(),
        "date_yyyy_mm": date.strftime("%Y-%m"),
        "release": releases,
        "local_raw_data_dir": raw_data_dir,
        "local_validated_data_dir": validated_data_dir,
        "files": {},
    }

    for file_name in NCBI_FILES:
        # remove the suffix
        file_base_name = file_name.replace(".txt", "")
        config["files"][file_base_name] = {
            "url": f"{NCBI_BASE_URL}/{file_name}",
            "s3_raw_data_dir": S3_BASE_URL
            + get_s3_destination_dir("raw_data", file_name, config["date_yyyy_mm"], releases),
        }
        if ASS_SUM_SUBSTR in file_name:
            config["files"][file_base_name]["s3_derived_dir"] = S3_BASE_URL + get_s3_destination_dir(
                "derived", file_name, config["date_yyyy_mm"], releases
            )
    return NcbiReleaseMetadata.model_validate(config)


def get_release(release_type: str) -> str:
    """Get the current release number for the given release type.

    Hits the NCBI website to retrieve the release number so requires internet access.

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
    base = f"{subdir}/{year_month}"
    release_type = get_release_type_for_file(file_name)
    if release_type is None:
        return f"{base}"
    return f"{base}/{release_type}-{releases[release_type]}"


def get_description(file_name: str, date: str) -> str:
    file_info: str = ""
    descriptions = {
        ASS_SUM_GENBANK: "current GenBank",
        ASS_SUM_GENBANK_HIST: "replaced and suppressed GenBank",
        ASS_SUM_REFSEQ: "current RefSeq",
        ASS_SUM_REFSEQ_HIST: "replaced and suppressed RefSeq",
    }

    if file_name in descriptions:
        file_info = (
            f"Metadata for {descriptions[file_name]} genome assemblies, available from the NCBI genomes FTP site. "
        )
    elif file_name == ASS_SUM_README:
        file_info = "README for NCBI assembly summary files. "

    return f"{file_info}Downloaded {date}"


def generate_frictionless_resource(file_path: str | Path, file_name: str, date: str) -> Resource:
    """Generate a Frictionless Resource object for a file.

    Note that files on s3 will take significantly longer to generate resources for as the whole file
    is downloaded to generate the stats.

    :param file_path: path to the file (including file name). Can be on s3 or local.
    :type file_path: str | Path
    :param file_name: name of the file
    :type file_name: str
    :param date: date of the file download
    :type date: str
    :return: Frictionless Resource for the file
    :rtype: Resource
    """
    resource = Resource(str(file_path))
    resource.infer(stats=True)
    resource.description = get_description(file_name, date)
    resource.sources = [{"title": "NCBI genomes FTP site", "url": f"{NCBI_BASE_URL}{file_name}"}]
    return resource


def ncbi_file_downloader(
    config: NcbiReleaseMetadata, downloads_dir: Path, ncbi_file_list: list[str]
) -> dict[str, Resource]:
    """Download assembly summary files from the NCBI website.

    :param settings: pipeline config
    :type settings: Settings
    :param ncbi_file_list: info about files to transfer, as a list of dictionaries
    :type ncbi_file_list: list[dict[str, Any]]
    """
    successful_downloads: dict[str, Resource] = {}
    to_download = []
    # make sure the directory exists

    if downloads_dir.exists() and downloads_dir.is_dir():
        # check whether we already have the files in the output directory
        for file_name in ncbi_file_list:
            file_path = downloads_dir / file_name
            if file_path.is_file():
                # generate frictionless resource for the file
                resource = generate_frictionless_resource(file_path, file_name, config.date)
                successful_downloads[file_name] = resource
            else:
                to_download.append(file_name)
    else:
        to_download = ncbi_file_list

    if to_download:
        downloads_dir.mkdir(parents=True, exist_ok=True)
        client = FileDownloader()
        for file_name in to_download:
            file_info = config.files[file_name.removesuffix(".txt")]
            file_path = downloads_dir / file_name
            try:
                client.download(url=file_info.url, destination=file_path)
                # generate frictionless resource for the file
                resource = generate_frictionless_resource(file_path, file_name, config.date)
                successful_downloads[file_name] = resource
            except Exception as e:
                err_msg = f"Could not download file from {file_info.url}: {e!s}"
                logger.exception(err_msg)
                continue

    # upload to s3 raw_data_dir
    return successful_downloads


def validate_files(
    config: NcbiReleaseMetadata, cleaned_dir: Path, successful_downloads: dict[str, Any]
) -> dict[str, Resource]:
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
    files_to_validate = {k: v for k, v in successful_downloads.items() if ASS_SUM_SUBSTR in k}
    files_to_validate = successful_downloads

    if not files_to_validate:
        return {}

    # once all files are downloaded, clean and validate the assembly_summary_* files
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    # check for qsv
    result = subprocess.run(["which", "qsv"], text=True, capture_output=True)
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError("wtf")
    qsv_cmd = result.stdout.strip()

    summary = {"error": [], "valid": {}, "invalid": []}
    with TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        header_file_path: Path = generate_header(tmp_dir_path, FIRST_PASS_SCHEMA)

        # for file_name, resource in assembly_summary_files.items():
        for resource in files_to_validate.values():
            args = CleanerValidatorArgs.model_validate(
                {
                    "summary": summary,
                    "qsv_cmd": qsv_cmd,
                    "xsv_file_path": Path(resource.path),
                    "header_file_path": header_file_path,
                    "first_pass_schema": FIRST_PASS_SCHEMA,
                    "post_norm_schema": POST_NORM_SCHEMA,
                    "tmp_dir_path": tmp_dir_path,
                    "output_dir_path": cleaned_dir,
                    "delimiter": "\t",
                    "comment_char": "#",
                    "null_regex": "na",
                    "missing_header": True,
                }
            )
            clean_validate_file(args)

    if summary["errors"]:
        error_file = cleaned_dir / "processing_errors.txt"
        error_file.write_text("\n".join(summary["errors"]))

    if summary["errors"] or summary["invalid"] or len(summary["valid"]) != len(files_to_validate):
        err_msg = "Errors found during file validation and normalisation"
        raise RuntimeError(err_msg)

    # upload to s3 derived dir with frictionless manifest
    return summary["valid"]


def upload_to_s3(
    config: NcbiReleaseMetadata,
    frictionless_resource_dict: dict[str, Resource],
) -> None:

    for f, resource in frictionless_resource_dict.items():
        file_path = Path(resource.path)
        # compile the metadata into a package, upload as json/yaml
        upload_file(file_info["path"], dest, file_path.name, show_progress=False)


# Simple string-prefix transforms: column -> prefix
PREFIX_MAP = {
    "assembly_accession": "ncbi.assembly",
    "bioproject": "bioproject",
    "biosample": "biosample",
    "gbrs_paired_asm": "ncbi.assembly",
    "species_taxid": "NCBItaxon",
    "taxid": "NCBItaxon",
}

DATE_COLS = ["asm_not_live_date", "annotation_date", "seq_rel_date"]


def tsv_to_table(
    config: NcbiReleaseMetadata,
    frictionless_resource_list: list[Resource],
    chunksize: int = 10000,
    **pandas_kwargs: Any,
) -> Iterator[TDataItems]:

    kwargs = {
        "header": "infer",
        "chunksize": chunksize,
        "sep": "\t",
        "dtype": str,
        "keep_default_na": True,
        **pandas_kwargs,
    }

    for resource in frictionless_resource_list:
        # resource.path
        with Path(resource.path).open() as file:
            for df in pd.read_csv(file, **kwargs):
                if "pubmed_id" in df.columns:
                    df["pubmed_id"] = df["pubmed_id"].astype("object")
                # simple prefix transforms
                for col, prefix in PREFIX_MAP.items():
                    apply_prefix(df, col, prefix)

                # pubmed_id: split on ";" then prefix each piece
                apply_split_prefix(df, col="pubmed_id", delim=";", prefix="pubmed:")

                df["source_file"] = resource["file_name"]
                yield df.to_dict(orient="records")


if __name__ == "__main__":
    downloads_dir = Path("assembly_summary/")
    validated_dir = Path("assembly_summary/validated")
    config_file = Path("ncbi_config.json")
    if not config_file.exists():
        md = generate_ncbi_release_metadata(
            raw_data_dir=downloads_dir,
            validated_data_dir=validated_dir,
            date=dt.date.today(),
        )
        Path("ncbi_config.json").write_text(md.model_dump_json())
    else:
        md = NcbiReleaseMetadata.model_validate(json.loads(config_file.read_bytes()))

    downloaded_files = ncbi_file_downloader(md, downloads_dir, NCBI_FILES)
    validated_file_names = validate_files(md, validated_dir, downloaded_files)
