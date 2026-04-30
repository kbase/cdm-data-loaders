"""Unit tests for utils.s3_versioned_upload."""

import hashlib
from datetime import date
from pathlib import Path
from unittest.mock import patch

import boto3
import botocore.client
import pytest
from moto import mock_aws

import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.utils.s3 import reset_s3_client
from cdm_data_loaders.utils.s3_versioned_upload import versioned_upload

AWS_REGION = "us-east-1"
TEST_BUCKET = "test-lake"
DEST_KEY = "tenant-general-warehouse/kbase/datasets/pdb/derived_data/rcsb/pdb_entries.ndjson"
DEST_PATH = f"{TEST_BUCKET}/{DEST_KEY}"
ARCHIVE_BASE = f"{TEST_BUCKET}/tenant-general-warehouse/kbase/datasets/pdb/derived_data/archive"
SUB_PATH = "rcsb/pdb_entries.ndjson"
FIXED_DATE = date(2026, 4, 30)


@pytest.fixture
def mock_s3_client(tmp_path: Path):
    """Mocked S3 client with test bucket pre-created."""
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        reset_s3_client()
        with (
            patch.object(s3_utils, "get_s3_client", return_value=client),
            patch.object(s3_utils, "_s3_client", client),
        ):
            yield client
        reset_s3_client()


class TestVersionedUploadNew:
    def test_first_upload_returns_new_status(self, mock_s3_client, tmp_path: Path):
        content = b"line1\nline2\n"
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(content)

        result = versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        assert result.status == "new"
        assert result.archive_key is None
        assert result.dest_path == DEST_PATH

    def test_first_upload_object_exists_in_s3(self, mock_s3_client, tmp_path: Path):
        content = b"line1\nline2\n"
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(content)

        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        resp = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key=DEST_KEY)
        assert resp["Body"].read() == content


class TestVersionedUploadUnchanged:
    def test_unchanged_content_returns_unchanged_status(self, mock_s3_client, tmp_path: Path):
        content = b"same content\n"
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(content)

        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)
        result = versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        assert result.status == "unchanged"
        assert result.archive_key is None

    def test_unchanged_does_not_create_archive(self, mock_s3_client, tmp_path: Path):
        content = b"same content\n"
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(content)

        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)
        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        archive_prefix = "tenant-general-warehouse/kbase/datasets/pdb/derived_data/archive/"
        paginator = mock_s3_client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=TEST_BUCKET, Prefix=archive_prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        assert keys == []


class TestVersionedUploadArchivedAndReplaced:
    def test_changed_content_returns_archived_and_replaced(self, mock_s3_client, tmp_path: Path):
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(b"version 1\n")
        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        local.write_bytes(b"version 2\n")
        result = versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        assert result.status == "archived_and_replaced"
        assert result.archive_key == f"{ARCHIVE_BASE}/2026-04-30/{SUB_PATH}"

    def test_changed_content_archive_has_old_content(self, mock_s3_client, tmp_path: Path):
        old_content = b"version 1\n"
        new_content = b"version 2\n"
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(old_content)
        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        local.write_bytes(new_content)
        result = versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        archive_bucket, archive_key = result.archive_key.split("/", 1)
        resp = mock_s3_client.get_object(Bucket=archive_bucket, Key=archive_key)
        assert resp["Body"].read() == old_content

    def test_changed_content_dest_has_new_content(self, mock_s3_client, tmp_path: Path):
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(b"version 1\n")
        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        new_content = b"version 2\n"
        local.write_bytes(new_content)
        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        resp = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key=DEST_KEY)
        assert resp["Body"].read() == new_content

    def test_archive_key_uses_supplied_date(self, mock_s3_client, tmp_path: Path):
        local = tmp_path / "pdb_entries.ndjson"
        local.write_bytes(b"v1\n")
        versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=FIXED_DATE)

        local.write_bytes(b"v2\n")
        result = versioned_upload(local, DEST_PATH, ARCHIVE_BASE, SUB_PATH, today=date(2026, 5, 7))

        assert "/2026-05-07/" in result.archive_key
