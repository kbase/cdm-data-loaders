"""Shared fixtures for PDB tests."""

import functools
from collections.abc import Callable, Generator
from unittest.mock import patch

import boto3
import botocore.client
import pytest
from moto import mock_aws

import cdm_data_loaders.pdb.promote as promote_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.utils.s3 import CDM_LAKE_BUCKET, reset_s3_client

AWS_REGION = "us-east-1"
TEST_BUCKET = CDM_LAKE_BUCKET

# A handful of synthetic PDB holdings records for use across tests.
SAMPLE_CURRENT_HOLDINGS = {
    "pdb_00001abc": {"content_type": ["coordinates_pdbx", "validation_report"]},
    "pdb_00002def": {"content_type": ["coordinates_pdbx", "structure_factors"]},
    "pdb_00003ghi": {"content_type": ["coordinates_pdbx"]},
    "pdb_00004jkl": {"content_type": ["coordinates_pdbx", "assemblies"]},
}

SAMPLE_DATES = {
    "pdb_00001abc": "2024-01-10",
    "pdb_00002def": "2024-02-15",
    "pdb_00003ghi": "2024-03-01",
    "pdb_00004jkl": "2024-01-20",
}

SAMPLE_REMOVED = {
    "pdb_00009zzz": {"obsolete_date": "2023-12-01", "superseded_by": "pdb_00001abc"},
}


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
            # Workaround: moto does not support CRC64NVME; strip it from put_object calls
            client.put_object = strip_checksum_algorithm(client.put_object)  # type: ignore[method-assign]
            yield client
        reset_s3_client()
