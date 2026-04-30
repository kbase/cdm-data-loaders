"""Unit tests for cdm_data_loaders.pdb.metadata."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import boto3
import pytest
from moto import mock_aws

if TYPE_CHECKING:
    from collections.abc import Generator

    import botocore.client

import cdm_data_loaders.pdb.metadata as metadata_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.pdb.metadata import (
    DescriptorResource,
    archive_descriptor,
    build_archive_descriptor_key,
    build_descriptor_key,
    create_descriptor,
    upload_descriptor,
    validate_descriptor,
)
from cdm_data_loaders.utils.s3 import reset_s3_client
from tests.pdb.conftest import TEST_BUCKET

AWS_REGION = "us-east-1"
_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb/"
_PDB_ID = "pdb_00001abc"
_RELEASE_TAG = "2024-04-01"
_TIMESTAMP = 1_700_000_000

_SAMPLE_RESOURCES: list[DescriptorResource] = [
    {
        "name": "pdb_00001abc.cif.gz",
        "path": f"{_KEY_PREFIX}raw_data/ab/pdb_00001abc/structures/pdb_00001abc.cif.gz",
        "format": "gz",
        "bytes": 2048,
        "hash": "abc123",
    },
    {
        "name": "pdb_00001abc_validation.pdf.gz",
        "path": f"{_KEY_PREFIX}raw_data/ab/pdb_00001abc/validation_reports/pdb_00001abc_validation.pdf.gz",
        "format": "gz",
        "bytes": None,
        "hash": None,
    },
]


# ── Key helpers ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("pdb_id", "key_prefix", "expected"),
    [
        pytest.param(_PDB_ID, _KEY_PREFIX, f"{_KEY_PREFIX}metadata/{_PDB_ID}_datapackage.json", id="standard"),
        pytest.param(_PDB_ID, _KEY_PREFIX.rstrip("/"), f"{_KEY_PREFIX}metadata/{_PDB_ID}_datapackage.json", id="no_trailing_slash"),
    ],
)
def test_build_descriptor_key(pdb_id: str, key_prefix: str, expected: str) -> None:
    """Verify metadata key is under metadata/ with _datapackage.json suffix and no double-slash."""
    key = build_descriptor_key(pdb_id, key_prefix)
    assert key == expected
    assert "//" not in key


@pytest.mark.parametrize(
    ("pdb_id", "release_tag", "key_prefix"),
    [
        pytest.param(_PDB_ID, _RELEASE_TAG, _KEY_PREFIX, id="standard"),
        pytest.param(_PDB_ID, _RELEASE_TAG, _KEY_PREFIX.rstrip("/"), id="no_trailing_slash"),
        pytest.param(_PDB_ID, "2025-01-15", _KEY_PREFIX, id="different_release_tag"),
    ],
)
def test_build_archive_descriptor_key(pdb_id: str, release_tag: str, key_prefix: str) -> None:
    """Verify archive key is under archive/{release_tag}/{archive_reason}/metadata/."""
    key = build_archive_descriptor_key(pdb_id, release_tag, key_prefix, "updated")
    assert f"archive/{release_tag}/updated/metadata/" in key
    assert key.endswith("_datapackage.json")
    assert "//" not in key


# ── create_descriptor ─────────────────────────────────────────────────────


def test_create_descriptor_structure() -> None:
    """Verify the main structural fields of the descriptor are correct."""
    d = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, last_modified="2024-01-10", timestamp=_TIMESTAMP)
    assert d["identifier"] == f"PDB:{_PDB_ID}"
    assert d["version"] == "2024-01-10"
    assert _PDB_ID in d["titles"][0]["title"]
    assert _PDB_ID in d["descriptions"][0]["description_text"]
    assert d["resource_type"] == "dataset"
    assert d["license"] == {}


def test_create_descriptor_url_points_to_rcsb() -> None:
    """URL references RCSB and contains the classic PDB ID."""
    d = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    parsed = urlparse(d["url"])
    assert parsed.hostname is not None and parsed.hostname.endswith("rcsb.org")
    assert "1ABC" in d["url"]  # pdb_00001abc → classic 1ABC


def test_create_descriptor_contributor() -> None:
    """Contributor is RCSB with the correct ROR ID."""
    d = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    assert d["contributors"][0]["name"] == "Research Collaboratory for Structural Bioinformatics"
    assert d["contributors"][0]["contributor_id"] == "ROR:02e8wq794"


def test_create_descriptor_meta() -> None:
    """meta.saved_by, meta.timestamp, schema_version are correct."""
    d = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    assert d["meta"]["saved_by"] == "cdm-data-loaders-pdb"
    assert d["meta"]["timestamp"] == _TIMESTAMP
    assert d["meta"]["credit_metadata_source"][0]["access_timestamp"] == _TIMESTAMP
    assert d["meta"]["credit_metadata_schema_version"] == "1.0"


def test_create_descriptor_default_version_is_today() -> None:
    """Version defaults to today's date when last_modified is omitted."""
    today = time.strftime("%Y-%m-%d")
    assert create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)["version"] == today


def test_create_descriptor_default_timestamp_is_recent() -> None:
    """Default timestamp is close to current time when not specified."""
    before = int(time.time())
    d = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES)
    assert before <= d["meta"]["timestamp"] <= int(time.time()) + 1


def test_create_descriptor_resources() -> None:
    """Resource list is transformed correctly (lowercase names, null bytes/hash omitted)."""
    resources: list[DescriptorResource] = [
        {"name": "FILE_UPPER.CIF.GZ", "path": "s3://bucket/a", "format": "gz", "bytes": 100, "hash": "x"},
        {"name": "no_hash.gz", "path": "s3://bucket/b", "format": "gz", "bytes": None, "hash": None},
    ]
    d = create_descriptor(_PDB_ID, resources, timestamp=_TIMESTAMP)
    assert d["resources"][0]["name"] == "file_upper.cif.gz"
    assert d["resources"][0]["hash"] == "x"
    assert d["resources"][0]["bytes"] == 100
    assert "hash" not in d["resources"][1]
    assert "bytes" not in d["resources"][1]


def test_create_descriptor_empty_resources() -> None:
    """Empty resources list produces a valid descriptor."""
    assert create_descriptor(_PDB_ID, [], timestamp=_TIMESTAMP)["resources"] == []


# ── validate_descriptor ───────────────────────────────────────────────────


def test_validate_descriptor_valid_passes() -> None:
    """Valid descriptor does not raise."""
    d = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    validate_descriptor(d, _PDB_ID)


def test_validate_descriptor_empty_raises() -> None:
    """Empty dict fails frictionless validation and raises."""
    with pytest.raises((ValueError, Exception)):
        validate_descriptor({}, _PDB_ID)


# ── upload_descriptor ─────────────────────────────────────────────────────


@pytest.fixture
def _mock_s3() -> Generator[botocore.client.BaseClient]:
    """Yield a mocked S3 client with the CDM Lake bucket pre-created."""
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


@pytest.mark.s3
def test_upload_descriptor_uploads_valid_json(_mock_s3: botocore.client.BaseClient) -> None:
    """Uploaded object is valid JSON with the expected identifier."""
    descriptor = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    key = upload_descriptor(descriptor, _PDB_ID, TEST_BUCKET, _KEY_PREFIX)
    assert key == build_descriptor_key(_PDB_ID, _KEY_PREFIX)
    body = json.loads(_mock_s3.get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
    assert body["identifier"] == f"PDB:{_PDB_ID}"


@pytest.mark.s3
def test_upload_descriptor_dry_run_skips_upload(_mock_s3: botocore.client.BaseClient) -> None:
    """Dry-run returns the key but does not create any S3 object."""
    descriptor = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    key = upload_descriptor(descriptor, _PDB_ID, TEST_BUCKET, _KEY_PREFIX, dry_run=True)
    assert key == build_descriptor_key(_PDB_ID, _KEY_PREFIX)
    objs = _mock_s3.list_objects_v2(Bucket=TEST_BUCKET).get("Contents", [])
    assert not any(o["Key"] == key for o in objs)


# ── archive_descriptor ────────────────────────────────────────────────────


@pytest.fixture
def _mock_s3_with_descriptor() -> Generator[tuple[botocore.client.BaseClient, MagicMock]]:
    """S3 with a live descriptor already uploaded."""
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        descriptor = create_descriptor(_PDB_ID, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        live_key = build_descriptor_key(_PDB_ID, _KEY_PREFIX)
        client.put_object(Bucket=TEST_BUCKET, Key=live_key, Body=json.dumps(descriptor).encode())
        reset_s3_client()
        with (
            patch.object(s3_utils, "get_s3_client", return_value=client),
            patch.object(metadata_mod, "get_s3_client", return_value=client),
            patch.object(metadata_mod, "copy_object") as mock_copy,
        ):
            yield client, mock_copy
        reset_s3_client()


@pytest.mark.s3
def test_archive_descriptor_returns_true_when_exists(
    _mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock],
) -> None:
    """Returns True when the live descriptor object exists in S3."""
    result = archive_descriptor(_PDB_ID, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
    assert result is True


@pytest.mark.s3
def test_archive_descriptor_calls_copy_with_correct_keys(
    _mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock],
) -> None:
    """copy_object_with_metadata is called with the live and archive keys."""
    _, mock_copy = _mock_s3_with_descriptor
    archive_descriptor(_PDB_ID, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
    live_key = build_descriptor_key(_PDB_ID, _KEY_PREFIX)
    archive_key = build_archive_descriptor_key(_PDB_ID, _RELEASE_TAG, _KEY_PREFIX, "unknown")
    mock_copy.assert_called_once()
    args = mock_copy.call_args[0]
    assert f"{TEST_BUCKET}/{live_key}" in args
    assert f"{TEST_BUCKET}/{archive_key}" in args


@pytest.mark.s3
def test_archive_descriptor_dry_run(
    _mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock],
) -> None:
    """Dry-run returns True but does not call copy_object_with_metadata."""
    _, mock_copy = _mock_s3_with_descriptor
    assert archive_descriptor(_PDB_ID, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG, dry_run=True) is True
    mock_copy.assert_not_called()


@pytest.mark.s3
def test_archive_descriptor_missing_returns_false() -> None:
    """Returns False when no descriptor exists at the live key."""
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        reset_s3_client()
        with (
            patch.object(s3_utils, "get_s3_client", return_value=client),
            patch.object(metadata_mod, "get_s3_client", return_value=client),
        ):
            result = archive_descriptor(_PDB_ID, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
        reset_s3_client()
    assert result is False

