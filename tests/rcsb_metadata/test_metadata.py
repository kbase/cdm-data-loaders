"""Unit tests for cdm_data_loaders.rcsb_metadata.metadata."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

if TYPE_CHECKING:
    from collections.abc import Generator

    import botocore.client

import cdm_data_loaders.rcsb_metadata.metadata as metadata_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.rcsb_metadata.metadata import (
    DescriptorResource,
    archive_descriptor,
    build_archive_descriptor_key,
    build_descriptor_key,
    create_descriptor,
    upload_descriptor,
    validate_descriptor,
)
from cdm_data_loaders.utils.s3 import reset_s3_client

AWS_REGION = "us-east-1"
TEST_BUCKET = "test-lake"
_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
_TIMESTAMP = 1_700_000_000
_DATE_TAG = "2026-05-01"

_ENTITY_TYPES = ["entries", "validation", "taxonomy"]

_SAMPLE_RESOURCES: list[DescriptorResource] = [
    {
        "name": "entries.ndjson",
        "path": f"{_KEY_PREFIX}/metadata/rcsb/raw_data/entries.ndjson",
        "format": "ndjson",
        "bytes": 2048,
        "hash": "md5:abc123",
    },
    {
        "name": "validation.ndjson",
        "path": f"{_KEY_PREFIX}/metadata/rcsb/raw_data/validation.ndjson",
        "format": "ndjson",
        "bytes": 512,
        "hash": None,
    },
]


# ── build_descriptor_key / build_archive_descriptor_key ─────────────────


@pytest.mark.parametrize("prefix", [_KEY_PREFIX, _KEY_PREFIX + "/"])
def test_build_descriptor_key(prefix: str) -> None:
    """Key is under metadata/rcsb/metadata/, ends with _datapackage.json; trailing slash normalized."""
    key = build_descriptor_key(prefix)
    assert key == f"{_KEY_PREFIX}/metadata/rcsb/metadata/rcsb_metadata_datapackage.json"
    assert "//" not in key


@pytest.mark.parametrize("prefix", [_KEY_PREFIX, _KEY_PREFIX + "/"])
def test_build_archive_descriptor_key(prefix: str) -> None:
    """Archive key includes date_tag and is under metadata/archive/; no double slash."""
    key = build_archive_descriptor_key(prefix, _DATE_TAG)
    assert key == f"{_KEY_PREFIX}/metadata/archive/{_DATE_TAG}/rcsb/metadata/rcsb_metadata_datapackage.json"
    assert "//" not in key


# ── create_descriptor ────────────────────────────────────────────────────


def test_create_descriptor() -> None:
    """create_descriptor produces a fully populated descriptor."""
    d = create_descriptor(_ENTITY_TYPES, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)

    assert d["identifier"] == "RCSB:annotations"
    assert d["resource_type"] == "dataset"
    assert d["url"] == "https://data.rcsb.org"
    assert d["license"] == {}
    assert d["contributors"][0]["name"] == "Research Collaboratory for Structural Bioinformatics"
    assert d["publisher"]["organization_id"] == "ROR:02e8wq794"

    # Version is derived from timestamp as ISO date
    assert d["version"] == "2023-11-14"

    # Description mentions all entity types
    desc = d["descriptions"][0]["description_text"]
    for entity in _ENTITY_TYPES:
        assert entity in desc

    # Meta fields
    assert d["meta"]["saved_by"] == "cdm-data-loaders-rcsb-metadata"
    assert d["meta"]["credit_metadata_schema_version"] == "1.0"
    assert d["meta"]["timestamp"] == _TIMESTAMP
    src = d["meta"]["credit_metadata_source"][0]
    assert src["source_name"] == "RCSB PDB GraphQL API"
    assert src["access_timestamp"] == _TIMESTAMP

    # Resources: hash=None key absent, bytes present
    r0 = d["resources"][0]
    assert r0["hash"] == "md5:abc123"
    assert r0["bytes"] == 2048
    assert r0["name"] == "entries.ndjson"

    r1 = d["resources"][1]
    assert "hash" not in r1
    assert r1["bytes"] == 512


def test_create_descriptor_default_timestamp_is_recent() -> None:
    """Default timestamp is close to current time when not specified."""
    before = int(time.time())
    d = create_descriptor(_ENTITY_TYPES, _SAMPLE_RESOURCES)
    after = int(time.time())
    assert before <= d["meta"]["timestamp"] <= after + 1


def test_create_descriptor_resource_name_lowercased() -> None:
    """Resource names are converted to lowercase."""
    resources: list[DescriptorResource] = [
        {"name": "ENTRIES.NDJSON", "path": "s3://b/a", "format": "ndjson", "bytes": 100, "hash": "md5:x"},
    ]
    d = create_descriptor(_ENTITY_TYPES, resources, timestamp=_TIMESTAMP)
    assert d["resources"][0]["name"] == "entries.ndjson"


def test_create_descriptor_null_bytes_omitted() -> None:
    """Resources with bytes=None have the 'bytes' key removed."""
    resources: list[DescriptorResource] = [
        {"name": "f.ndjson", "path": "s3://b/f", "format": "ndjson", "bytes": None, "hash": "md5:x"},
    ]
    d = create_descriptor(_ENTITY_TYPES, resources, timestamp=_TIMESTAMP)
    assert "bytes" not in d["resources"][0]


def test_create_descriptor_empty_resources() -> None:
    """Empty resources list produces a valid descriptor."""
    d = create_descriptor(_ENTITY_TYPES, [], timestamp=_TIMESTAMP)
    assert d["resources"] == []


# ── validate_descriptor ──────────────────────────────────────────────────


def test_validate_descriptor_valid() -> None:
    """Valid descriptor does not raise."""
    validate_descriptor(create_descriptor(_ENTITY_TYPES, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP))


def test_validate_descriptor_empty_raises() -> None:
    """Empty dict fails frictionless validation and raises."""
    with pytest.raises((ValueError, Exception)):
        validate_descriptor({})


# ── S3 upload / archive fixtures ─────────────────────────────────────────


@pytest.fixture
def mock_s3() -> Generator[botocore.client.BaseClient, None, None]:
    """Mocked S3 client with the test bucket pre-created."""
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        reset_s3_client()
        with (
            patch.object(s3_utils, "get_s3_client", return_value=client),
            patch.object(metadata_mod, "get_s3_client", return_value=client),
        ):
            yield client
        reset_s3_client()


@pytest.fixture
def mock_s3_with_descriptor(mock_s3: botocore.client.BaseClient):
    """mock_s3 with a live descriptor pre-uploaded and copy_object patched."""
    descriptor = create_descriptor(_ENTITY_TYPES, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    live_key = build_descriptor_key(_KEY_PREFIX)
    mock_s3.put_object(Bucket=TEST_BUCKET, Key=live_key, Body=json.dumps(descriptor).encode())
    with patch.object(metadata_mod, "copy_object") as mock_copy:
        yield mock_s3, mock_copy


# ── upload_descriptor ────────────────────────────────────────────────────


@pytest.mark.s3
def test_upload_descriptor(mock_s3: botocore.client.BaseClient) -> None:
    """Uploaded object is valid JSON at the expected key with the expected identifier."""
    descriptor = create_descriptor(_ENTITY_TYPES, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    key = upload_descriptor(descriptor, TEST_BUCKET, _KEY_PREFIX)
    expected_key = build_descriptor_key(_KEY_PREFIX)
    assert key == expected_key
    assert "rcsb/metadata" in key
    assert key.endswith("_datapackage.json")
    body = json.loads(mock_s3.get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
    assert body["identifier"] == "RCSB:annotations"


@pytest.mark.s3
def test_upload_descriptor_dry_run(mock_s3: botocore.client.BaseClient) -> None:
    """Dry-run returns the correct key but creates no S3 object."""
    descriptor = create_descriptor(_ENTITY_TYPES, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    key = upload_descriptor(descriptor, TEST_BUCKET, _KEY_PREFIX, dry_run=True)
    assert key == build_descriptor_key(_KEY_PREFIX)
    objs = mock_s3.list_objects_v2(Bucket=TEST_BUCKET).get("Contents", [])
    assert not any(o["Key"] == key for o in objs)


# ── archive_descriptor ───────────────────────────────────────────────────


@pytest.mark.s3
def test_archive_descriptor(mock_s3_with_descriptor) -> None:
    """archive_descriptor returns True and calls copy_object with the correct keys."""
    _, mock_copy = mock_s3_with_descriptor
    result = archive_descriptor(TEST_BUCKET, _KEY_PREFIX, _DATE_TAG)
    assert result is True
    mock_copy.assert_called_once()
    src_arg, dst_arg = mock_copy.call_args[0]
    assert build_descriptor_key(_KEY_PREFIX) in src_arg
    assert build_archive_descriptor_key(_KEY_PREFIX, _DATE_TAG) in dst_arg


@pytest.mark.s3
def test_archive_descriptor_dry_run(mock_s3_with_descriptor) -> None:
    """Dry-run returns True but does not call copy_object."""
    _, mock_copy = mock_s3_with_descriptor
    assert archive_descriptor(TEST_BUCKET, _KEY_PREFIX, _DATE_TAG, dry_run=True) is True
    mock_copy.assert_not_called()


@pytest.mark.s3
def test_archive_descriptor_missing_returns_false(mock_s3: botocore.client.BaseClient) -> None:
    """Returns False when no descriptor exists at the live key."""
    assert archive_descriptor(TEST_BUCKET, _KEY_PREFIX, _DATE_TAG) is False
