"""Integration tests for the PDB manifest-generating pipeline.

Tests in ``TestDownloadHoldingsFilesLive`` hit the real RCSB PDB holdings
service and are marked ``slow_test`` + ``external_request``.

Tests in ``TestSnapshotCeph`` and ``TestGenerateSnapshotFromS3State`` require
a running CEPH test store and are marked ``requires_ceph`` + ``slow_test``.
"""

from collections.abc import Generator
from datetime import date
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import botocore.client
import pytest

import cdm_data_loaders.pipelines.pdb_manifest as pdb_manifest_mod
from cdm_data_loaders.pdb.constants import (
    HoldingsFileTypes,
    PDBRecord,
)
from cdm_data_loaders.pipelines.pdb_manifest import (
    PdbManfestSettings,
    _download_holdings_files,
    _download_holdings_snapshot,
    _generate_snapshot_from_s3_state,
    _save_holdings_snapshot,
    run_manifest_generation,
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
# Local fixture - patch pdb_manifest's get_s3_client to use CEPH client
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
        bootstrap_date = date(2024, 1, 1)

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
        bootstrap_date = date(2024, 1, 1)

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
            date=date(2024, 6, 1),
        )

        assert list(result.keys()).count(pdb_id) == 1


# ---------------------------------------------------------------------------
# Full pipeline end-to-end tests
# ---------------------------------------------------------------------------

# Holdings data shared across E2E tests.
# pdb_aaaaaaaa is intentionally absent from LAST_MODIFIED; should show up in missing_dates.
_E2E_CURRENT: dict[str, PDBRecord] = {
    "pdb_00001abc": PDBRecord(id="pdb_00001abc"),
    "pdb_00001def": PDBRecord(id="pdb_00001def"),
    "pdb_aaaaaaaa": PDBRecord(id="pdb_aaaaaaaa"),
}
_E2E_LAST_MODIFIED: dict[str, PDBRecord] = {
    "pdb_00001abc": PDBRecord(id="pdb_00001abc", last_modified="2024-01-15"),
    "pdb_00001def": PDBRecord(id="pdb_00001def", last_modified="2024-02-20"),
}
_E2E_REMOVED: dict[str, PDBRecord] = {}

_SNAPSHOT_PREFIX = PurePosixPath("e2e-pdb-test")
_SNAPSHOT_FILENAME = PurePosixPath("current_snapshot.json.gz")


@pytest.mark.requires_ceph
@pytest.mark.slow_test
class TestRunManifestGenerationE2E:
    """End-to-end tests for run_manifest_generation.

    PDB HTTP downloads are mocked; S3 interactions use the real CEPH store.
    """

    @pytest.fixture(autouse=True)
    def _mock_holdings_download(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replace live PDB downloads with controlled in-memory data."""
        mock_raw = {
            HoldingsFileTypes.CURRENT: dict(_E2E_CURRENT),
            HoldingsFileTypes.LAST_MODIFIED: dict(_E2E_LAST_MODIFIED),
            HoldingsFileTypes.REMOVED: dict(_E2E_REMOVED),
        }
        monkeypatch.setattr(pdb_manifest_mod, "_download_holdings_files", lambda: mock_raw)

    def _make_config(
        self,
        test_bucket: PurePosixPath,
        tmp_path: Path,
        **kwargs: object,
    ) -> PdbManfestSettings:
        defaults: dict = {
            "bootstrap_date": None,
            "skip_diff": False,
            "regex_filter": None,
            "destination_bucket": test_bucket,
            "destination_prefix": _PDB_KEY_PREFIX,
            "holdings_snapshot_path": _SNAPSHOT_FILENAME,
            "output_path": tmp_path,
        }
        defaults.update(kwargs)
        return cast("PdbManfestSettings", SimpleNamespace(**defaults))

    def _read_manifest(self, tmp_path: Path, filename: str) -> list[str]:
        return (tmp_path / filename).read_text().splitlines()

    @pytest.mark.usefixtures("pdb_ceph_client")
    def test_skip_diff_all_current_records_are_new(
        self,
        test_bucket: PurePosixPath,
        tmp_path: Path,
    ) -> None:
        """With skip_diff=True, every current ID appears as new in the transfer manifest."""
        config = self._make_config(test_bucket, tmp_path, skip_diff=True)
        run_manifest_generation(config)

        transfer = sorted(self._read_manifest(tmp_path, "transfer_manifest.txt"))
        assert transfer == sorted(_E2E_CURRENT.keys())
        assert self._read_manifest(tmp_path, "updated_manifest.txt") == []
        assert self._read_manifest(tmp_path, "removed_manifest.txt") == []
        assert self._read_manifest(tmp_path, "missing_dates.txt") == ["pdb_aaaaaaaa"]

    def test_bootstrap_date_uses_s3_state_as_previous(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
        tmp_path: Path,
    ) -> None:
        """With bootstrap_date, the S3 store determines the previous snapshot."""
        # pdb_00001def: in S3, bootstrap date older than current: updated.
        # pdb_00009999: in S3 but not in current holdings: removed.
        _seed_fake_pdb_objects(pdb_ceph_client, test_bucket, ["pdb_00001def", "pdb_00009999"])
        bootstrap_date = date(2020, 1, 1)

        config = self._make_config(test_bucket, tmp_path, bootstrap_date=bootstrap_date)
        run_manifest_generation(config)

        # pdb_00001abc and pdb_aaaaaaaa are not in the store: new
        transfer = self._read_manifest(tmp_path, "transfer_manifest.txt")
        assert "pdb_00001abc" in transfer
        assert "pdb_00001def" in transfer  # also in transfer (updated IDs included)
        assert "pdb_aaaaaaaa" in transfer

        assert self._read_manifest(tmp_path, "updated_manifest.txt") == ["pdb_00001def"]
        assert self._read_manifest(tmp_path, "removed_manifest.txt") == ["pdb_00009999"]
        assert self._read_manifest(tmp_path, "missing_dates.txt") == ["pdb_aaaaaaaa"]

    def test_with_existing_snapshot_incremental_diff(
        self,
        pdb_ceph_client: botocore.client.BaseClient,
        test_bucket: PurePosixPath,
        tmp_path: Path,
    ) -> None:
        """An S3 snapshot drives incremental diffing; only changed records appear."""
        # pdb_00001def: in snapshot with older date: updated.
        # pdb_00009999: in snapshot but not in current holdings: removed.
        previous = {
            "pdb_00001def": PDBRecord(id="pdb_00001def", last_modified="2023-06-01"),
            "pdb_00009999": PDBRecord(id="pdb_00009999", last_modified="2022-01-01"),
        }
        local_snapshot = tmp_path / "input_snapshot.json.gz"
        _save_holdings_snapshot(previous, local_snapshot)
        snapshot_key = _SNAPSHOT_PREFIX / _SNAPSHOT_FILENAME
        pdb_ceph_client.upload_file(
            Filename=str(local_snapshot),
            Bucket=str(test_bucket),
            Key=str(snapshot_key),
        )

        config = self._make_config(
            test_bucket,
            tmp_path,
            destination_prefix=_SNAPSHOT_PREFIX,
        )
        run_manifest_generation(config)

        # pdb_00001abc and pdb_aaaaaaaa were not in snapshot: new
        transfer = self._read_manifest(tmp_path, "transfer_manifest.txt")
        assert "pdb_00001abc" in transfer
        assert "pdb_00001def" in transfer  # also in transfer (updated IDs included)
        assert "pdb_aaaaaaaa" in transfer

        assert self._read_manifest(tmp_path, "updated_manifest.txt") == ["pdb_00001def"]
        assert self._read_manifest(tmp_path, "removed_manifest.txt") == ["pdb_00009999"]
        assert self._read_manifest(tmp_path, "missing_dates.txt") == ["pdb_aaaaaaaa"]

    @pytest.mark.usefixtures("pdb_ceph_client")
    @pytest.mark.parametrize(
        ("regex_filter", "expected_transfer", "expected_missing"),
        [
            pytest.param(
                "pdb_00001",
                ["pdb_00001abc", "pdb_00001def"],
                [],
                id="prefix-filter",
            ),
            pytest.param(
                "pdb_aaa",
                ["pdb_aaaaaaaa"],
                ["pdb_aaaaaaaa"],
                id="single-match-still-missing-date",
            ),
        ],
    )
    def test_regex_filter(
        self,
        test_bucket: PurePosixPath,
        tmp_path: Path,
        regex_filter: str,
        expected_transfer: list[str],
        expected_missing: list[str],
    ) -> None:
        """Regex filter restricts which IDs appear across all output manifest files."""
        config = self._make_config(test_bucket, tmp_path, skip_diff=True, regex_filter=regex_filter)
        run_manifest_generation(config)

        transfer = sorted(self._read_manifest(tmp_path, "transfer_manifest.txt"))
        assert transfer == sorted(expected_transfer)
        assert self._read_manifest(tmp_path, "updated_manifest.txt") == []
        assert self._read_manifest(tmp_path, "removed_manifest.txt") == []
        assert sorted(self._read_manifest(tmp_path, "missing_dates.txt")) == sorted(expected_missing)
