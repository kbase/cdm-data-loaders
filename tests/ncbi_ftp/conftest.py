"""Shared fixtures for ncbi_ftp tests."""

import functools
from collections.abc import Callable, Generator
from unittest.mock import patch

import boto3
import botocore.client
import pytest
from moto import mock_aws

import cdm_data_loaders.ncbi_ftp.promote as promote_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.utils.s3 import CDM_LAKE_BUCKET, reset_s3_client

AWS_REGION = "us-east-1"
TEST_BUCKET = CDM_LAKE_BUCKET


# Minimal assembly_summary_refseq.txt content (tab-separated, 20+ columns)
SAMPLE_SUMMARY = (
    "# assembly_accession\tbioproject\tbiosample\twgs_master\trefseq_category\t"
    "taxid\tspecies_taxid\torganism_name\tinfraspecific_name\tisolate\t"
    "version_status\tassembly_level\trelease_type\tgenome_rep\tseq_rel_date\t"
    "asm_name\t16\t17\t18\tftp_path\n"
    "GCF_000001215.4\tPRJNA13812\tSAMN02803731\t\treference genome\t7227\t7227\t"
    "Drosophila melanogaster\t\t\tlatest\tChromosome\tMajor\tFull\t2014/10/21\t"
    "Release_6_plus_ISO1_MT\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT\n"
    "GCF_000001405.40\tPRJNA168\tna\t\treference genome\t9606\t9606\t"
    "Homo sapiens\t\t\tlatest\tChromosome\tPatch\tFull\t2022/02/03\t"
    "GRCh38.p14\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14\n"
    "GCF_000005845.2\tPRJNA57779\tSAMN02604091\t\trepresentative genome\t511145\t562\t"
    "Escherichia coli\t\t\treplaced\tComplete Genome\tMajor\tFull\t2013/09/26\t"
    "ASM584v2\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/005/845/GCF_000005845.2_ASM584v2\n"
    "GCF_000009999.1\tPRJNA999\tSAMN999\t\tna\t0\t0\t"
    "Test organism\t\t\tsuppressed\tScaffold\tMajor\tFull\t2010/01/01\t"
    "ASM999v1\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/999/GCF_000009999.1_ASM999v1\n"
    "GCF_000099999.1\tPRJNA888\tSAMN888\t\tna\t0\t0\t"
    "Test organism 2\t\t\tlatest\tContig\tMajor\tFull\t2023/06/15\t"
    "ASM9999v1\t\t\t\tna\n"
)


def strip_checksum_algorithm(method: Callable) -> Callable:
    """Wrap a boto3 S3 method to remove ChecksumAlgorithm (moto CRC64NVME workaround)."""

    @functools.wraps(method)
    def wrapper(*args: object, **kwargs: object) -> object:
        kwargs.pop("ChecksumAlgorithm", None)  # type: ignore[arg-type]
        return method(*args, **kwargs)

    return wrapper


@pytest.fixture
def mock_s3_client() -> Generator[botocore.client.BaseClient]:
    """Yield a mocked S3 client with the CDM Lake bucket created."""
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)

        reset_s3_client()
        with (
            patch.object(s3_utils, "get_s3_client", return_value=client),
            patch.object(promote_mod, "get_s3_client", return_value=client),
        ):
            yield client
        reset_s3_client()


@pytest.fixture
def mock_s3_client_no_checksum(mock_s3_client: botocore.client.BaseClient) -> botocore.client.BaseClient:
    """Mocked S3 client with copy_object and upload_file patched to strip ChecksumAlgorithm."""
    mock_s3_client.copy_object = strip_checksum_algorithm(mock_s3_client.copy_object)  # type: ignore[method-assign]
    mock_s3_client.upload_file = strip_checksum_algorithm(mock_s3_client.upload_file)  # type: ignore[method-assign]
    return mock_s3_client
