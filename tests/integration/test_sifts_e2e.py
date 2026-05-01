"""MinIO integration tests for the SIFTS pipeline.

Requires a running MinIO instance (see tests/integration/conftest.py for
connection details).  Tests are automatically skipped when MinIO is not
reachable.

Two test classes:

* :class:`TestSiftsIntegration` — mocks FTP download, uses real MinIO
* :class:`TestSiftsExternalRequest` — hits real EBI FTP (marked ``external_request`` + ``slow_test``)
"""

from pathlib import Path
from unittest.mock import patch

import boto3
import botocore.config
import pytest

import cdm_data_loaders.sifts.run as sifts_run_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.sifts.run import run_sifts
from cdm_data_loaders.sifts.settings import SIFTS_ARCHIVE_PREFIX, SIFTS_DERIVED_DATA_PREFIX, SiftsSettings
from cdm_data_loaders.utils.s3 import reset_s3_client

FAKE_CONTENT_V1 = b"pdb_chain\tuniprot_acc\n4HHB_A\tP69905\n"
FAKE_CONTENT_V2 = b"pdb_chain\tuniprot_acc\n4HHB_A\tP69905\n1CBS_A\tP18065\n"

pytestmark = pytest.mark.integration

_UNIPROT_FILE = "pdb_chain_uniprot.tsv.gz"


def _fake_download_files_fn(content_map: dict[str, bytes]):
    """Return a fake for download_sifts_files that writes content from *content_map*."""

    def fake_download_files(filenames, dest_dir, ftp_host):
        paths = []
        for filename in filenames:
            path = dest_dir / filename
            path.write_bytes(content_map.get(filename, b""))
            paths.append(path)
        return paths

    return fake_download_files


def _sifts_settings(bucket: str, dry_run: bool = False) -> SiftsSettings:
    return SiftsSettings(
        lakehouse_bucket=bucket,
        sifts_files=[_UNIPROT_FILE],
        dry_run=dry_run,
    )


def _dest_key(settings: SiftsSettings) -> str:
    prefix = settings.lakehouse_key_prefix.strip("/")
    return f"{prefix}/{SIFTS_DERIVED_DATA_PREFIX}/pdb_chain_uniprot.tsv.gz"


class TestSiftsIntegration:
    def test_first_run_uploads_file(self, minio_s3_client, test_bucket):
        settings = _sifts_settings(test_bucket)
        with patch.object(
            sifts_run_mod, "download_sifts_files", side_effect=_fake_download_files_fn({_UNIPROT_FILE: FAKE_CONTENT_V1})
        ):
            result = run_sifts(settings)

        fr = result.file_results[_UNIPROT_FILE]
        assert fr.upload_status == "new"
        assert fr.archive_key is None

        # Verify file exists in MinIO
        key = _dest_key(settings)
        body = minio_s3_client.get_object(Bucket=test_bucket, Key=key)["Body"].read()
        assert body == FAKE_CONTENT_V1

    def test_identical_rerun_is_unchanged(self, minio_s3_client, test_bucket):
        settings = _sifts_settings(test_bucket)
        with patch.object(
            sifts_run_mod, "download_sifts_files", side_effect=_fake_download_files_fn({_UNIPROT_FILE: FAKE_CONTENT_V1})
        ):
            run_sifts(settings)
            result = run_sifts(settings)

        fr = result.file_results[_UNIPROT_FILE]
        assert fr.upload_status == "unchanged"
        assert fr.archive_key is None

    def test_changed_content_archives_old_version(self, minio_s3_client, test_bucket):
        settings = _sifts_settings(test_bucket)
        with patch.object(
            sifts_run_mod, "download_sifts_files", side_effect=_fake_download_files_fn({_UNIPROT_FILE: FAKE_CONTENT_V1})
        ):
            run_sifts(settings)
        with patch.object(
            sifts_run_mod, "download_sifts_files", side_effect=_fake_download_files_fn({_UNIPROT_FILE: FAKE_CONTENT_V2})
        ):
            result = run_sifts(settings)

        fr = result.file_results[_UNIPROT_FILE]
        assert fr.upload_status == "archived_and_replaced"
        assert fr.archive_key is not None
        assert SIFTS_ARCHIVE_PREFIX in fr.archive_key

        # New content is live
        key = _dest_key(settings)
        body = minio_s3_client.get_object(Bucket=test_bucket, Key=key)["Body"].read()
        assert body == FAKE_CONTENT_V2

        # Archived content is present
        # archive_key includes bucket prefix (e.g. "bucket/key/path") — strip it
        archive_s3_key = fr.archive_key.split("/", 1)[1]
        archived = minio_s3_client.get_object(Bucket=test_bucket, Key=archive_s3_key)["Body"].read()
        assert archived == FAKE_CONTENT_V1


@pytest.mark.external_request
@pytest.mark.slow_test
class TestSiftsRealApi:
    """Hit the real EBI FTP server; upload results to MinIO.

    Part of the normal ``integration`` tier — must not be excluded by default.
    Use ``-m "integration and not slow_test"`` to skip in fast CI runs.
    """

    def test_downloads_real_sifts_file(self, minio_s3_client, test_bucket):
        """Download the real SIFTS file from EBI FTP and upload to MinIO."""
        settings = SiftsSettings(
            lakehouse_bucket=test_bucket,
            sifts_files=[_UNIPROT_FILE],
        )
        result = run_sifts(settings)

        fr = result.file_results[_UNIPROT_FILE]
        assert fr.upload_status in ("new", "archived_and_replaced", "unchanged")
        key = _dest_key(settings)
        head = minio_s3_client.head_object(Bucket=test_bucket, Key=key)
        assert head["ContentLength"] > 0
