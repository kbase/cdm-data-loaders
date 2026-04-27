"""Unit tests for cdm_data_loaders.ncbi_ftp.metadata."""

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

import cdm_data_loaders.ncbi_ftp.metadata as metadata_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.ncbi_ftp.metadata import (
    DescriptorResource,
    archive_descriptor,
    build_archive_descriptor_key,
    build_descriptor_key,
    create_descriptor,
    upload_descriptor,
    validate_descriptor,
)
from cdm_data_loaders.utils.s3 import reset_s3_client
from tests.ncbi_ftp.conftest import TEST_BUCKET

AWS_REGION = "us-east-1"

_ACCESSION = "GCF_000001215.4"
_ASSEMBLY_DIR = "GCF_000001215.4_Release_6_plus_ISO1_MT"
_RELEASE_TAG = "2024-01"
_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"
_TIMESTAMP = 1_700_000_000

_SAMPLE_RESOURCES: list[DescriptorResource] = [
    {
        "name": "GCF_000001215.4_genomic.fna.gz",
        "path": f"{_KEY_PREFIX}raw_data/GCF/000/001/215/{_ASSEMBLY_DIR}/GCF_000001215.4_genomic.fna.gz",
        "format": "gz",
        "bytes": 1024,
        "hash": "abc123",
    },
    {
        "name": "GCF_000001215.4_assembly_report.txt",
        "path": f"{_KEY_PREFIX}raw_data/GCF/000/001/215/{_ASSEMBLY_DIR}/GCF_000001215.4_assembly_report.txt",
        "format": "txt",
        "bytes": 512,
        "hash": None,  # no md5 sidecar for this one
    },
]


# ── build_descriptor_key / build_archive_descriptor_key ─────────────────


@pytest.mark.parametrize("prefix", [_KEY_PREFIX, _KEY_PREFIX.rstrip("/")])
def test_build_descriptor_key(prefix: str) -> None:
    """Key is under metadata/, ends with _datapackage.json, trailing slash on prefix is normalized."""
    key = build_descriptor_key(_ASSEMBLY_DIR, prefix)
    assert key == f"{_KEY_PREFIX}metadata/{_ASSEMBLY_DIR}_datapackage.json"
    assert "//" not in key


@pytest.mark.parametrize(
    ("prefix", "tag"),
    [
        pytest.param(_KEY_PREFIX, _RELEASE_TAG, id="trailing_slash"),
        pytest.param(_KEY_PREFIX.rstrip("/"), _RELEASE_TAG, id="no_trailing_slash"),
        pytest.param(_KEY_PREFIX, "2025-06", id="different_tag"),
    ],
)
def test_build_archive_descriptor_key(prefix: str, tag: str) -> None:
    """Archive key includes tag and has no double slash; trailing slash on prefix is normalized."""
    key = build_archive_descriptor_key(_ASSEMBLY_DIR, tag, prefix)
    assert key == f"{_KEY_PREFIX}archive/{tag}/metadata/{_ASSEMBLY_DIR}_datapackage.json"
    assert "//" not in key


# ── create_descriptor ────────────────────────────────────────────────────


def test_create_descriptor() -> None:
    """create_descriptor produces a fully populated descriptor matching the expected structure."""
    d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)

    # URL hostname is computed; can't express as equality
    host = urlparse(d["url"]).hostname
    assert host is not None and (host == "ncbi.nlm.nih.gov" or host.endswith(".ncbi.nlm.nih.gov"))

    # resource[1]: hash=None → key absent; bytes=512 → key present
    r1 = d["resources"][1]
    assert "hash" not in r1
    assert "bytes" in r1

    assert {k: d[k] for k in ("identifier", "resource_type", "version", "license", "contributors")} == {
        "identifier": f"NCBI:{_ACCESSION}",
        "resource_type": "dataset",
        "version": "4",
        "license": {},
        "contributors": [
            {
                "name": "National Center for Biotechnology Information",
                "contributor_id": "ROR:02meqm098",
                "contributor_type": "Organization",
                "contributor_roles": "DataCurator",
            }
        ],
    }
    assert {
        k: d["meta"][k] for k in ("saved_by", "credit_metadata_schema_version", "timestamp", "credit_metadata_source")
    } == {
        "saved_by": "cdm-data-loaders-ncbi-ftp",
        "credit_metadata_schema_version": "1.0",
        "timestamp": _TIMESTAMP,
        "credit_metadata_source": [
            {
                "access_timestamp": _TIMESTAMP,
                "source_name": "NCBI Genomes FTP",
                "source_url": "ftp.ncbi.nlm.nih.gov/genomes/all/",
            }
        ],
    }
    assert _ASSEMBLY_DIR in d["titles"][0]["title"]
    assert _ACCESSION in d["descriptions"][0]["description_text"]
    r0 = d["resources"][0]
    assert {k: r0[k] for k in ("hash", "bytes", "path")} == {
        "hash": "abc123",
        "bytes": 1024,
        "path": _SAMPLE_RESOURCES[0]["path"],
    }


def test_create_descriptor_default_timestamp_is_recent() -> None:
    """Default timestamp is close to current time when not specified."""
    before = int(time.time())
    d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES)
    after = int(time.time())
    assert before <= d["meta"]["timestamp"] <= after + 1


def test_create_descriptor_resource_name_lowercased() -> None:
    """Resource names are converted to lowercase."""
    resources: list[DescriptorResource] = [
        {"name": "FILE_UPPER.FNA.GZ", "path": "s3://bucket/a", "format": "gz", "bytes": 100, "hash": "x"},
    ]
    d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, resources, timestamp=_TIMESTAMP)
    assert d["resources"][0]["name"] == "file_upper.fna.gz"


def test_create_descriptor_null_bytes_omitted() -> None:
    """Resources with bytes=None have the 'bytes' key removed from the output."""
    resources: list[DescriptorResource] = [
        {"name": "f.txt", "path": "s3://b/f.txt", "format": "txt", "bytes": None, "hash": "x"},
    ]
    d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, resources, timestamp=_TIMESTAMP)
    assert "bytes" not in d["resources"][0]


def test_create_descriptor_empty_resources() -> None:
    """Empty resources list produces a valid descriptor."""
    d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, [], timestamp=_TIMESTAMP)
    assert d["resources"] == []


# ── validate_descriptor ──────────────────────────────────────────────────


def test_validate_descriptor_valid() -> None:
    """Valid descriptor does not raise."""
    validate_descriptor(
        create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP),
        _ACCESSION,
    )


def test_validate_descriptor_empty_raises() -> None:
    """Empty dict fails frictionless validation and raises."""
    with pytest.raises((ValueError, Exception)):
        validate_descriptor({}, _ACCESSION)


# ── upload_descriptor / archive_descriptor ───────────────────────────────


@pytest.fixture
def mock_s3() -> Generator[botocore.client.BaseClient]:
    """Mocked S3 client with the CDM Lake bucket pre-created."""
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
def mock_s3_with_descriptor(mock_s3: botocore.client.BaseClient) -> tuple[botocore.client.BaseClient, MagicMock]:
    """mock_s3 with a live descriptor pre-uploaded and copy_object patched."""
    descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    live_key = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
    mock_s3.put_object(Bucket=TEST_BUCKET, Key=live_key, Body=json.dumps(descriptor).encode())
    with patch.object(metadata_mod, "copy_object") as mock_copy:
        yield mock_s3, mock_copy


@pytest.mark.s3
def test_upload_descriptor(mock_s3: botocore.client.BaseClient) -> None:
    """Uploaded object is valid JSON at the expected key with the expected identifier."""
    descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    key = upload_descriptor(descriptor, _ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX)
    expected_key = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
    assert key == expected_key
    assert key.startswith(_KEY_PREFIX)
    assert key.endswith("_datapackage.json")
    body = json.loads(mock_s3.get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
    assert body["identifier"] == f"NCBI:{_ACCESSION}"


@pytest.mark.s3
def test_upload_descriptor_dry_run(mock_s3: botocore.client.BaseClient) -> None:
    """Dry-run returns the correct key but creates no S3 object."""
    descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
    key = upload_descriptor(descriptor, _ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, dry_run=True)
    assert key == build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
    objs = mock_s3.list_objects_v2(Bucket=TEST_BUCKET).get("Contents", [])
    assert not any(o["Key"] == key for o in objs)


@pytest.mark.s3
def test_archive_descriptor(mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock]) -> None:
    """archive_descriptor returns True and calls copy_object with the correct keys."""
    _, mock_copy = mock_s3_with_descriptor
    result = archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
    assert result is True
    mock_copy.assert_called_once()
    args = mock_copy.call_args[0]
    assert f"{TEST_BUCKET}/{build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)}" in args
    assert f"{TEST_BUCKET}/{build_archive_descriptor_key(_ASSEMBLY_DIR, _RELEASE_TAG, _KEY_PREFIX)}" in args


@pytest.mark.s3
def test_archive_descriptor_dry_run(mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock]) -> None:
    """Dry-run returns True but does not call copy_object."""
    _, mock_copy = mock_s3_with_descriptor
    assert archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG, dry_run=True) is True
    mock_copy.assert_not_called()


@pytest.mark.s3
def test_archive_descriptor_missing_returns_false(mock_s3: botocore.client.BaseClient) -> None:
    """Returns False when no descriptor exists at the live key."""
    assert archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG) is False
