"""Shared fixtures for RCSB metadata tests."""

from collections.abc import Generator
from unittest.mock import patch

import boto3
import botocore.client
import pytest
from moto import mock_aws

import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.utils.s3 import reset_s3_client

AWS_REGION = "us-east-1"
TEST_BUCKET = "test-lake"


def strip_checksum_algorithm(method):
    """Wrap a boto3 S3 method to strip ChecksumAlgorithm (moto CRC64NVME workaround)."""
    import functools

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        kwargs.pop("ChecksumAlgorithm", None)
        return method(*args, **kwargs)

    return wrapper


@pytest.fixture
def mock_s3_client() -> Generator[botocore.client.BaseClient, None, None]:
    """Yield a moto-backed S3 client with the test bucket pre-created."""
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        client.put_object = strip_checksum_algorithm(client.put_object)
        reset_s3_client()
        with (
            patch.object(s3_utils, "get_s3_client", return_value=client),
            patch.object(s3_utils, "_s3_client", client),
        ):
            yield client
        reset_s3_client()
