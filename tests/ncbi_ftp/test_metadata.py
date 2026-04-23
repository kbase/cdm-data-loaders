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


# ── build_descriptor_key ─────────────────────────────────────────────────


class TestBuildDescriptorKey:
    """Tests for build_descriptor_key path helper."""

    def test_produces_metadata_path(self) -> None:
        """Key is located under metadata/ with _datapackage.json suffix."""
        key = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
        assert key == f"{_KEY_PREFIX}metadata/{_ASSEMBLY_DIR}_datapackage.json"

    def test_trailing_slash_normalised(self) -> None:
        """Key is the same whether key_prefix ends with a slash or not."""
        key_no_slash = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX.rstrip("/"))
        key_with_slash = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
        assert key_no_slash == key_with_slash

    def test_no_double_slash(self) -> None:
        """Key never contains a double slash."""
        key = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
        assert "//" not in key


# ── build_archive_descriptor_key ─────────────────────────────────────────


class TestBuildArchiveDescriptorKey:
    """Tests for build_archive_descriptor_key path helper."""

    def test_produces_archive_path(self) -> None:
        """Key is located under archive/{release_tag}/metadata/."""
        key = build_archive_descriptor_key(_ASSEMBLY_DIR, _RELEASE_TAG, _KEY_PREFIX)
        expected = f"{_KEY_PREFIX}archive/{_RELEASE_TAG}/metadata/{_ASSEMBLY_DIR}_datapackage.json"
        assert key == expected

    def test_trailing_slash_normalised(self) -> None:
        """Key is the same whether key_prefix ends with a slash or not."""
        a = build_archive_descriptor_key(_ASSEMBLY_DIR, _RELEASE_TAG, _KEY_PREFIX.rstrip("/"))
        b = build_archive_descriptor_key(_ASSEMBLY_DIR, _RELEASE_TAG, _KEY_PREFIX)
        assert a == b

    def test_release_tag_in_path(self) -> None:
        """Release tag appears in the archive key path."""
        key = build_archive_descriptor_key(_ASSEMBLY_DIR, "2025-06", _KEY_PREFIX)
        assert "2025-06" in key


# ── create_descriptor ────────────────────────────────────────────────────


class TestCreateDescriptor:
    """Tests for create_descriptor()."""

    def test_identifier(self) -> None:
        """Identifier field is prefixed with NCBI:."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["identifier"] == f"NCBI:{_ACCESSION}"

    def test_version_extracted_from_accession(self) -> None:
        """Version is the suffix after the last dot in the accession."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["version"] == "4"  # last segment of GCF_000001215.4

    def test_title_includes_assembly_dir(self) -> None:
        """Title includes the full assembly directory name."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert _ASSEMBLY_DIR in d["titles"][0]["title"]

    def test_description_includes_accession(self) -> None:
        """Description text includes the accession."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert _ACCESSION in d["descriptions"][0]["description_text"]

    def test_url_references_accession(self) -> None:
        """URL points to the NCBI genome page for the accession."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert _ACCESSION in d["url"]
        parsed = urlparse(d["url"])
        host = parsed.hostname
        assert host is not None
        assert host == "ncbi.nlm.nih.gov" or host.endswith(".ncbi.nlm.nih.gov")

    def test_ncbi_contributor(self) -> None:
        """Contributor is NCBI with the correct ROR ID."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["contributors"][0]["name"] == "National Center for Biotechnology Information"
        assert d["contributors"][0]["contributor_id"] == "ROR:02meqm098"

    def test_saved_by(self) -> None:
        """meta.saved_by is the cdm-data-loaders identifier."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["meta"]["saved_by"] == "cdm-data-loaders-ncbi-ftp"

    def test_timestamp_propagated(self) -> None:
        """Explicit timestamp is used for both meta.timestamp and access_timestamp."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["meta"]["timestamp"] == _TIMESTAMP
        assert d["meta"]["credit_metadata_source"][0]["access_timestamp"] == _TIMESTAMP

    def test_default_timestamp_is_recent(self) -> None:
        """Default timestamp is close to current time when not specified."""
        before = int(time.time())
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES)
        after = int(time.time())
        ts = d["meta"]["timestamp"]
        assert before <= ts <= after + 1

    def test_resource_names_lowercased(self) -> None:
        """Resource names are converted to lowercase."""
        resources: list[DescriptorResource] = [
            {"name": "FILE_UPPER.FNA.GZ", "path": "s3://bucket/a", "format": "gz", "bytes": 100, "hash": "x"},
        ]
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, resources, timestamp=_TIMESTAMP)
        assert d["resources"][0]["name"] == "file_upper.fna.gz"

    def test_null_hash_omitted(self) -> None:
        """Resources with hash=None must not include the 'hash' key."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        resources = d["resources"]
        # Second resource has hash=None → key absent
        assert "hash" not in resources[1]

    def test_non_null_hash_present(self) -> None:
        """Non-null hash is retained in the resource entry."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["resources"][0]["hash"] == _SAMPLE_RESOURCES[0]["hash"]

    def test_resource_count(self) -> None:
        """Resource list length matches the number of input resources."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert len(d["resources"]) == len(_SAMPLE_RESOURCES)

    def test_resource_bytes(self) -> None:
        """Resource bytes matches the input bytes value."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["resources"][0]["bytes"] == _SAMPLE_RESOURCES[0]["bytes"]

    def test_resource_path(self) -> None:
        """Resource path matches the input path value."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["resources"][0]["path"] == _SAMPLE_RESOURCES[0]["path"]

    def test_license_is_empty_dict(self) -> None:
        """License field is an empty dict."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["license"] == {}

    def test_resource_type_is_dataset(self) -> None:
        """resource_type is 'dataset'."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["resource_type"] == "dataset"

    def test_schema_version(self) -> None:
        """credit_metadata_schema_version is '1.0'."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        assert d["meta"]["credit_metadata_schema_version"] == "1.0"

    def test_empty_resources_allowed(self) -> None:
        """Empty resources list produces a valid descriptor."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, [], timestamp=_TIMESTAMP)
        assert d["resources"] == []

    def test_null_bytes_omitted(self) -> None:
        """Resources with bytes=None have the 'bytes' key removed from the output."""
        resources: list[DescriptorResource] = [
            {"name": "f.txt", "path": "s3://b/f.txt", "format": "txt", "bytes": None, "hash": "x"},
        ]
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, resources, timestamp=_TIMESTAMP)
        assert "bytes" not in d["resources"][0]


# ── validate_descriptor ──────────────────────────────────────────────────


class TestValidateDescriptor:
    """Tests for validate_descriptor()."""

    def test_valid_descriptor_passes(self) -> None:
        """Valid descriptor does not raise."""
        d = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        # Should not raise
        validate_descriptor(d, _ACCESSION)

    def test_empty_descriptor_raises(self) -> None:
        """Empty dict fails frictionless validation and raises."""
        with pytest.raises((ValueError, Exception)):
            validate_descriptor({}, _ACCESSION)


# ── upload_descriptor ────────────────────────────────────────────────────


@pytest.mark.s3
class TestUploadDescriptor:
    """Tests for upload_descriptor() using moto-mocked S3."""

    @pytest.fixture
    def mock_s3(self) -> Generator[botocore.client.BaseClient]:
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

    def test_uploads_json(self, mock_s3: botocore.client.BaseClient) -> None:
        """Uploaded object is valid JSON with the expected identifier."""
        descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        key = upload_descriptor(descriptor, _ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX)
        assert key == build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
        obj = mock_s3.get_object(Bucket=TEST_BUCKET, Key=key)
        body = json.loads(obj["Body"].read())
        assert body["identifier"] == f"NCBI:{_ACCESSION}"

    def test_returns_expected_key(self, mock_s3: botocore.client.BaseClient) -> None:
        """Return value is the metadata/ S3 key for the assembly."""
        descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        key = upload_descriptor(descriptor, _ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX)
        assert key.startswith(_KEY_PREFIX)
        assert key.endswith("_datapackage.json")

    def test_dry_run_skips_upload(self, mock_s3: botocore.client.BaseClient) -> None:
        """Dry-run returns the key but does not create any S3 object."""
        descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        key = upload_descriptor(descriptor, _ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, dry_run=True)
        # No object in S3
        objs = mock_s3.list_objects_v2(Bucket=TEST_BUCKET).get("Contents", [])
        assert not any(o["Key"] == key for o in objs)

    def test_dry_run_returns_key(self, mock_s3: botocore.client.BaseClient) -> None:
        """Dry-run returns the same key as a real upload would."""
        descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
        key = upload_descriptor(descriptor, _ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, dry_run=True)
        assert key == build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)


# ── archive_descriptor ───────────────────────────────────────────────────


@pytest.mark.s3
class TestArchiveDescriptor:
    """Tests for archive_descriptor() using moto-mocked S3."""

    @pytest.fixture
    def mock_s3_with_descriptor(self) -> Generator[tuple[botocore.client.BaseClient, MagicMock]]:
        """S3 with a live descriptor already uploaded."""
        with mock_aws():
            client = boto3.client("s3", region_name=AWS_REGION)
            client.create_bucket(Bucket=TEST_BUCKET)
            # Pre-upload a descriptor
            descriptor = create_descriptor(_ASSEMBLY_DIR, _ACCESSION, _SAMPLE_RESOURCES, timestamp=_TIMESTAMP)
            live_key = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
            client.put_object(
                Bucket=TEST_BUCKET,
                Key=live_key,
                Body=json.dumps(descriptor).encode(),
            )
            reset_s3_client()
            with (
                patch.object(s3_utils, "get_s3_client", return_value=client),
                patch.object(metadata_mod, "get_s3_client", return_value=client),
                patch.object(metadata_mod, "copy_object") as mock_copy,
            ):
                yield client, mock_copy
            reset_s3_client()

    def test_returns_true_when_descriptor_exists(
        self, mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock]
    ) -> None:
        """Returns True when the live descriptor object exists in S3."""
        _, _ = mock_s3_with_descriptor
        result = archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
        assert result is True

    def test_calls_copy_with_correct_keys(
        self, mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock]
    ) -> None:
        """copy_object is called with the live and archive keys."""
        _, mock_copy = mock_s3_with_descriptor
        archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
        live_key = build_descriptor_key(_ASSEMBLY_DIR, _KEY_PREFIX)
        archive_key = build_archive_descriptor_key(_ASSEMBLY_DIR, _RELEASE_TAG, _KEY_PREFIX)
        mock_copy.assert_called_once()
        args = mock_copy.call_args
        assert f"{TEST_BUCKET}/{live_key}" in args[0]
        assert f"{TEST_BUCKET}/{archive_key}" in args[0]

    def test_dry_run_returns_true_without_copy(
        self, mock_s3_with_descriptor: tuple[botocore.client.BaseClient, MagicMock]
    ) -> None:
        """Dry-run returns True but does not call copy_object."""
        _, mock_copy = mock_s3_with_descriptor
        result = archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG, dry_run=True)
        assert result is True
        mock_copy.assert_not_called()

    def test_missing_descriptor_returns_false(self) -> None:
        """Returns False when no descriptor exists at the live key."""
        with mock_aws():
            client = boto3.client("s3", region_name=AWS_REGION)
            client.create_bucket(Bucket=TEST_BUCKET)
            reset_s3_client()
            with (
                patch.object(s3_utils, "get_s3_client", return_value=client),
                patch.object(metadata_mod, "get_s3_client", return_value=client),
            ):
                result = archive_descriptor(_ASSEMBLY_DIR, TEST_BUCKET, _KEY_PREFIX, _RELEASE_TAG)
            reset_s3_client()
        assert result is False
