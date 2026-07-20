"""Integration tests for the PDB manifest-generating pipeline.

Tests in ``TestDownloadHoldingsFilesLive`` hit the real RCSB PDB holdings
service and are marked ``slow_test`` + ``external_request``.

Tests in ``TestSnapshotCeph`` and ``TestGenerateSnapshotFromS3State`` require
a running CEPH test store and are marked ``requires_ceph`` + ``slow_test``.
"""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from unittest.mock import patch

import botocore.client
import pytest

import cdm_data_loaders.pipelines.pdb_manifest as pdb_manifest_mod
from cdm_data_loaders.pdb.constants import (
    HoldingsFileTypes,
    PDBRecord,
)
from cdm_data_loaders.pipelines.pdb_manifest import (
    _download_holdings_files,
    _download_holdings_snapshot,
    _generate_snapshot_from_s3_state,
    _save_holdings_snapshot,
)

# A prefix under which we seed fake PDB objects in CEPH
_PDB_KEY_PREFIX = PurePosixPath("tenant-general-warehouse/refdata/datasets/pdb/raw_data")

# A handful of fake extended PDB IDs used across tests
_FAKE_IDS = [
    "pdb_00001abc",
    "pdb_00001def",
    "pdb_aaaaaaaa",
]


# ---------------------------------------------------------------------------
# Local fixture — patch pdb_manifest's get_s3_client to use CEPH client
# ---------------------------------------------------------------------------


@pytest.fixture
def pdb_ceph_client(
    ceph_s3_client: botocore.client.BaseClient,
) -> Generator[botocore.client.BaseClient]:
    """Extend ceph_s3_client by also patching get_s3_client inside pdb_manifest."""
    with patch.object(pdb_manifest_mod, "get_s3_client", return_value=ceph_s3_client):
        yield ceph_s3_client


def _seed_fake_pdb_objects(
    s3: botocore.client.BaseClient,
    bucket: PurePosixPath,
    pdb_ids: list[str],
    prefix: PurePosixPath = _PDB_KEY_PREFIX,
) -> None:
    """Upload a minimal placeholder object for each PDB ID into CEPH."""
    for pdb_id in pdb_ids:
        key = str(prefix / pdb_id / f"{pdb_id}_model.cif.gz")
        s3.put_object(Bucket=str(bucket), Key=key, Body=b"placeholder")


# ---------------------------------------------------------------------------
# Live PDB service tests
# ---------------------------------------------------------------------------

type HoldingsFiles = dict[HoldingsFileTypes, dict[str, PDBRecord]]


@pytest.fixture(scope="module")
def holdings_files() -> HoldingsFiles:
    """Download the holdings files."""
    return _download_holdings_files()


@pytest.mark.slow_test
@pytest.mark.external_request
class TestDownloadHoldingsFilesLive:
    """Download real PDB holdings files from the RCSB server."""

    def test_returns_all_three_holding_types(self, holdings_files: HoldingsFiles) -> None:
        """Ensures all three holdings files are downloaded."""
        assert HoldingsFileTypes.CURRENT in holdings_files
        assert HoldingsFileTypes.LAST_MODIFIED in holdings_files
        assert HoldingsFileTypes.REMOVED in holdings_files

    def test_current_holdings_are_non_empty(self, holdings_files: HoldingsFiles) -> None:
        """Ensures records exist is each holdings file."""
        assert len(holdings_files[HoldingsFileTypes.CURRENT]) > 0
        assert len(holdings_files[HoldingsFileTypes.LAST_MODIFIED]) > 0
        assert len(holdings_files[HoldingsFileTypes.REMOVED]) > 0

    def test_all_records_have_lowercase_ids(self, holdings_files: HoldingsFiles) -> None:
        """Ensures PDB IDs are properly lower-cased."""
        for file_type, records in holdings_files.items():
            for pdb_id, rec in records.items():
                assert pdb_id == pdb_id.lower(), f"{file_type}: key '{pdb_id}' is not lowercase"
                assert rec.id == rec.id.lower(), f"{file_type}: record.id '{rec.id}' is not lowercase"

    def test_last_modified_records_have_dates(self, holdings_files: HoldingsFiles) -> None:
        """Ensure last-modified holdings file has dates included."""
        dates = holdings_files[HoldingsFileTypes.LAST_MODIFIED]
        assert len(dates) > 0
        sample = next(iter(dates.values()))
        assert sample.last_modified != ""


# ---------------------------------------------------------------------------
# CEPH snapshot round-trip tests
# ---------------------------------------------------------------------------


@pytest.mark.requires_ceph
@pytest.mark.slow_test
class TestSnapshotCeph:
    """Save a holdings snapshot to CEPH and retrieve it via _download_holdings_snapshot."""

    def test_save_and_download_snapshot(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
        tmp_path: Path,
    ) -> None:
        """Tests round-trip saving/loading of snapshot to S3 store."""
        records = {
            "pdb_00001abc": PDBRecord(id="pdb_00001abc", last_modified="2024-01-15"),
            "pdb_00001def": PDBRecord(id="pdb_00001def", last_modified="2023-06-30"),
        }
        snapshot_key = PurePosixPath("snapshots/current_holdings_snapshot.json.gz")
        local_file = tmp_path / "snapshot.json.gz"

        # Save locally then upload to CEPH
        _save_holdings_snapshot(records, local_file)
        pdb_ceph_client.upload_file(
            Filename=str(local_file),
            Bucket=str(test_bucket),
            Key=str(snapshot_key),
        )

        # Download and verify round-trip
        downloaded = _download_holdings_snapshot(
            bucket=test_bucket,
            key=snapshot_key,
        )
        assert downloaded == records

    def test_empty_snapshot_round_trip(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
        tmp_path: Path,
    ) -> None:
        """Tests round-trip saving/loading of empty snapshot to S3 store."""
        snapshot_key = PurePosixPath("snapshots/empty_snapshot.json.gz")
        local_file = tmp_path / "empty.json.gz"
        _save_holdings_snapshot({}, local_file)
        pdb_ceph_client.upload_file(
            Filename=str(local_file),
            Bucket=str(test_bucket),
            Key=str(snapshot_key),
        )
        downloaded = _download_holdings_snapshot(bucket=test_bucket, key=snapshot_key)
        assert downloaded == {}


# ---------------------------------------------------------------------------
# CEPH snapshot-from-S3-state tests
# ---------------------------------------------------------------------------


@pytest.mark.requires_ceph
@pytest.mark.slow_test
class TestGenerateSnapshotFromS3State:
    """Bootstrap a holdings snapshot by scanning objects in CEPH."""

    def test_finds_all_seeded_ids(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
    ) -> None:
        """Tests bootstrapped snapshot includes in-store ids."""
        _seed_fake_pdb_objects(pdb_ceph_client, test_bucket, _FAKE_IDS)
        bootstrap_date = datetime(2024, 1, 1, tzinfo=UTC)

        result = _generate_snapshot_from_s3_state(
            bucket=test_bucket,
            key_prefix=_PDB_KEY_PREFIX,
            date=bootstrap_date,
        )

        assert set(result.keys()) == set(_FAKE_IDS)

    def test_all_records_use_bootstrap_date(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
    ) -> None:
        """Ensures the provided snapshot date is properly set."""
        _seed_fake_pdb_objects(pdb_ceph_client, test_bucket, _FAKE_IDS)
        bootstrap_date = datetime(2024, 1, 1, tzinfo=UTC)

        result = _generate_snapshot_from_s3_state(
            bucket=test_bucket,
            key_prefix=_PDB_KEY_PREFIX,
            date=bootstrap_date,
        )

        expected_date = bootstrap_date.isoformat()
        for pdb_id, rec in result.items():
            assert rec.last_modified == expected_date, f"{pdb_id} has unexpected date {rec.last_modified}"

    def test_deduplicates_multiple_objects_per_id(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
    ) -> None:
        """Multiple S3 objects sharing the same PDB ID produce a single snapshot entry."""
        pdb_id = "pdb_00001abc"
        for suffix in ("model.cif.gz", "data.cif.gz", "info.json"):
            key = str(_PDB_KEY_PREFIX / pdb_id / f"{pdb_id}_{suffix}")
            pdb_ceph_client.put_object(Bucket=str(test_bucket), Key=key, Body=b"x")

        result = _generate_snapshot_from_s3_state(
            bucket=test_bucket,
            key_prefix=_PDB_KEY_PREFIX,
            date=datetime(2024, 6, 1, tzinfo=UTC),
        )

        assert list(result.keys()).count(pdb_id) == 1
