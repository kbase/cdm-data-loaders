"""Tests for pdb.manifest module — holdings parsing, diff, scan, manifest writing."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from cdm_data_loaders.pdb.entry import PDBRecord
from cdm_data_loaders.pdb.manifest import (
    PDBDiffResult,
    build_current_records,
    compute_diff,
    filter_by_hash_range,
    load_holdings_snapshot,
    merge_holdings_dates,
    parse_current_holdings,
    parse_last_modified_dates,
    parse_removed_entries,
    save_holdings_snapshot,
    scan_store_to_previous_ids,
    write_diff_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)

from .conftest import SAMPLE_CURRENT_HOLDINGS, SAMPLE_DATES, SAMPLE_REMOVED


# ── parse_current_holdings ───────────────────────────────────────────────


@pytest.mark.parametrize(
    ("data", "expected_keys", "check"),
    [
        pytest.param(
            SAMPLE_CURRENT_HOLDINGS,
            {"pdb_00001abc", "pdb_00002def", "pdb_00003ghi", "pdb_00004jkl"},
            lambda r: "coordinates_pdbx" in r["pdb_00001abc"].file_types,
            id="dict_format",
        ),
        pytest.param(
            [
                {"entry_id": "pdb_00001abc", "content_type": ["coordinates_pdbx"]},
                {"entry_id": "pdb_00002def", "content_type": []},
            ],
            {"pdb_00001abc", "pdb_00002def"},
            None,
            id="list_format",
        ),
        pytest.param(
            {"PDB_00001ABC": {"content_type": []}},
            {"pdb_00001abc"},
            None,
            id="ids_lowercased",
        ),
        pytest.param({}, set(), None, id="empty_input"),
        pytest.param("bad_data", set(), None, id="unexpected_type_returns_empty"),
    ],
)
def test_parse_current_holdings(
    data: object, expected_keys: set[str], check: object
) -> None:
    """Verify dict/list/lowercase/empty holdings data is parsed correctly."""
    records = parse_current_holdings(data)  # type: ignore[arg-type]
    assert set(records.keys()) == expected_keys
    if check:
        assert check(records)


# ── parse_last_modified_dates ────────────────────────────────────────────


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        pytest.param(SAMPLE_DATES, {"pdb_00001abc": "2024-01-10"}, id="dict_format"),
        pytest.param({"PDB_00001ABC": "2024-01-01"}, {"pdb_00001abc": "2024-01-01"}, id="ids_lowercased"),
        pytest.param(
            [{"entry_id": "pdb_00001abc", "last_modified": "2024-01-10"}],
            {"pdb_00001abc": "2024-01-10"},
            id="list_format",
        ),
    ],
)
def test_parse_last_modified_dates(data: object, expected: dict[str, str]) -> None:
    """Verify dates are parsed from dict/list format and IDs are lowercased."""
    dates = parse_last_modified_dates(data)  # type: ignore[arg-type]
    assert {k: dates[k] for k in expected} == expected


# ── parse_removed_entries ────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("data", "expected_id"),
    [
        pytest.param(SAMPLE_REMOVED, "pdb_00009zzz", id="dict_format"),
        pytest.param([{"entry_id": "pdb_00001abc"}], "pdb_00001abc", id="list_of_dicts"),
        pytest.param(["pdb_00001abc"], "pdb_00001abc", id="list_of_strings"),
        pytest.param({"PDB_00001ABC": {}}, "pdb_00001abc", id="ids_lowercased"),
    ],
)
def test_parse_removed_entries(data: object, expected_id: str) -> None:
    """Verify removed entry IDs are extracted from dict/list/list-of-strings formats."""
    assert expected_id in parse_removed_entries(data)  # type: ignore[arg-type]


# ── merge_holdings_dates ─────────────────────────────────────────────────


def test_merge_holdings_dates_populates_last_modified() -> None:
    """Verify last_modified is populated from the dates dict."""
    records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
    merged = merge_holdings_dates(records, SAMPLE_DATES)
    assert merged["pdb_00001abc"].last_modified == "2024-01-10"


def test_merge_holdings_dates_missing_date_stays_empty() -> None:
    """Entries missing from dates keep empty last_modified."""
    records = {"pdb_new_xx": PDBRecord(pdb_id="pdb_new_xx", last_modified="")}
    merged = merge_holdings_dates(records, {})
    assert merged["pdb_new_xx"].last_modified == ""


def test_merge_holdings_dates_returns_same_dict() -> None:
    """merge_holdings_dates mutates in place and returns the same object."""
    records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
    assert merge_holdings_dates(records, SAMPLE_DATES) is records


# ── build_current_records ────────────────────────────────────────────────


def test_build_current_records() -> None:
    """Verify convenience wrapper populates last_modified from dates."""
    holdings_data = {"current": SAMPLE_CURRENT_HOLDINGS, "dates": SAMPLE_DATES, "removed": SAMPLE_REMOVED}
    records = build_current_records(holdings_data)
    assert records["pdb_00001abc"].last_modified == "2024-01-10"
    assert len(records) == 4  # noqa: PLR2004


# ── filter_by_hash_range ─────────────────────────────────────────────────


@pytest.fixture
def _sample_records() -> dict[str, PDBRecord]:
    """Parsed + dated records for filter/diff tests."""
    records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
    return merge_holdings_dates(records, SAMPLE_DATES)


@pytest.mark.parametrize(
    ("hash_from", "hash_to", "expected_in", "expected_out"),
    [
        pytest.param(None, None, {"pdb_00001abc", "pdb_00002def"}, set(), id="no_filter"),
        pytest.param("de", None, {"pdb_00002def"}, {"pdb_00001abc"}, id="from_only"),
        pytest.param(None, "ab", {"pdb_00001abc"}, {"pdb_00002def"}, id="to_only"),
        pytest.param("ab", "de", {"pdb_00001abc", "pdb_00002def"}, {"pdb_00003ghi"}, id="both_bounds"),
    ],
)
def test_filter_by_hash_range(
    _sample_records: dict[str, PDBRecord],
    hash_from: str | None,
    hash_to: str | None,
    expected_in: set[str],
    expected_out: set[str],
) -> None:
    """Verify hash-range filtering includes/excludes the correct entries."""
    kwargs = {k: v for k, v in {"hash_from": hash_from, "hash_to": hash_to}.items() if v is not None}
    result = filter_by_hash_range(_sample_records, **kwargs)
    for pdb_id in expected_in:
        assert pdb_id in result
    for pdb_id in expected_out:
        assert pdb_id not in result


def test_filter_by_hash_range_no_filter_returns_same_object(_sample_records: dict[str, PDBRecord]) -> None:
    """No bounds returns the exact same dict object."""
    assert filter_by_hash_range(_sample_records) is _sample_records


# ── compute_diff ─────────────────────────────────────────────────────────


def test_compute_diff_all_new_when_no_previous(_sample_records: dict[str, PDBRecord]) -> None:
    """All entries are new when previous is empty."""
    diff = compute_diff(_sample_records)
    assert diff == PDBDiffResult(new=sorted(_sample_records.keys()), updated=[], removed=[])


def test_compute_diff_updated_when_date_increased(_sample_records: dict[str, PDBRecord]) -> None:
    """An entry is marked updated when its last_modified date increases."""
    previous = {"pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2023-12-01")}
    diff = compute_diff({"pdb_00001abc": _sample_records["pdb_00001abc"]}, previous=previous)
    assert "pdb_00001abc" in diff.updated
    assert "pdb_00001abc" not in diff.new


def test_compute_diff_not_updated_when_date_unchanged(_sample_records: dict[str, PDBRecord]) -> None:
    """An entry is not marked updated when its date is the same."""
    previous = {"pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2024-01-10")}
    diff = compute_diff({"pdb_00001abc": _sample_records["pdb_00001abc"]}, previous=previous)
    assert "pdb_00001abc" not in diff.updated
    assert "pdb_00001abc" not in diff.new


def test_compute_diff_removed_from_removed_ids(_sample_records: dict[str, PDBRecord]) -> None:
    """Entries in removed_ids that were previously known are marked removed."""
    previous = {"pdb_00009zzz": PDBRecord("pdb_00009zzz", last_modified="2023-01-01")}
    diff = compute_diff(_sample_records, previous=previous, removed_ids={"pdb_00009zzz"})
    assert "pdb_00009zzz" in diff.removed


def test_compute_diff_removed_when_gone_from_current() -> None:
    """An entry absent from current is marked removed if it was previously known."""
    previous = {"pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2024-01-10")}
    diff = compute_diff({}, previous=previous)
    assert "pdb_00001abc" in diff.removed


def test_compute_diff_previous_ids_fallback(_sample_records: dict[str, PDBRecord]) -> None:
    """previous_ids fallback marks known entries as non-new."""
    diff = compute_diff(_sample_records, previous_ids={"pdb_00001abc"})
    assert "pdb_00001abc" not in diff.new
    assert "pdb_00002def" in diff.new


# ── scan_store_to_previous_ids ───────────────────────────────────────────


def test_scan_store_to_previous_ids_returns_set_of_ids(mock_s3_client: object) -> None:
    """IDs are extracted from S3 object keys under the prefix."""
    import cdm_data_loaders.pdb.manifest as manifest_mod  # noqa: PLC0415
    from tests.pdb.conftest import TEST_BUCKET  # noqa: PLC0415

    client = mock_s3_client
    prefix = "tenant-general-warehouse/kbase/datasets/pdb/"
    key = f"{prefix}raw_data/ab/pdb_00001abc/structures/pdb_00001abc.cif.gz"
    client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")  # type: ignore[attr-defined]

    with patch.object(manifest_mod, "get_s3_client", return_value=client):
        ids = scan_store_to_previous_ids(TEST_BUCKET, prefix)

    assert "pdb_00001abc" in ids


def test_scan_store_to_previous_ids_empty_store(mock_s3_client: object) -> None:
    """Empty S3 prefix returns empty set."""
    import cdm_data_loaders.pdb.manifest as manifest_mod  # noqa: PLC0415
    from tests.pdb.conftest import TEST_BUCKET  # noqa: PLC0415

    with patch.object(manifest_mod, "get_s3_client", return_value=mock_s3_client):
        ids = scan_store_to_previous_ids(TEST_BUCKET, "missing/prefix/")

    assert ids == set()


# ── Manifest writing ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("diff", "write_fn", "expected_ids"),
    [
        pytest.param(
            PDBDiffResult(new=["pdb_00002def"], updated=["pdb_00001abc"]),
            write_transfer_manifest,
            ["pdb_00001abc", "pdb_00002def"],
            id="transfer_manifest_new_and_updated",
        ),
        pytest.param(
            PDBDiffResult(),
            write_transfer_manifest,
            [],
            id="transfer_manifest_empty",
        ),
        pytest.param(
            PDBDiffResult(removed=["pdb_00009zzz"]),
            write_removed_manifest,
            ["pdb_00009zzz"],
            id="removed_manifest",
        ),
        pytest.param(
            PDBDiffResult(updated=["pdb_00001abc"]),
            write_updated_manifest,
            ["pdb_00001abc"],
            id="updated_manifest",
        ),
    ],
)
def test_write_manifest(
    tmp_path: Path,
    diff: PDBDiffResult,
    write_fn: object,
    expected_ids: list[str],
) -> None:
    """Verify each manifest writer produces the expected list of IDs."""
    out = tmp_path / "manifest.txt"
    result = write_fn(diff, out)  # type: ignore[operator]
    assert sorted(result) == sorted(expected_ids)
    if expected_ids:
        lines = out.read_text().splitlines()
        for pdb_id in expected_ids:
            assert pdb_id in lines
    else:
        assert out.read_text() == ""


def test_write_diff_summary(tmp_path: Path) -> None:
    """Diff summary JSON contains correct counts and range; file matches return value."""
    diff = PDBDiffResult(new=["pdb_00001abc"], updated=["pdb_00002def"], removed=["pdb_00003ghi"])
    out = tmp_path / "diff_summary.json"
    summary = write_diff_summary(diff, out, hash_from="00", hash_to="ff")
    assert json.loads(out.read_text()) == summary
    assert {k: summary[k] for k in ("new", "updated", "removed", "total_to_transfer")} == {
        "new": 1,
        "updated": 1,
        "removed": 1,
        "total_to_transfer": 2,
    }


# ── Holdings snapshot I/O ────────────────────────────────────────────────


def test_holdings_snapshot_round_trip(tmp_path: Path) -> None:
    """Records survive a save/load round-trip through gzip JSON."""
    records = {
        "pdb_00001abc": PDBRecord("pdb_00001abc", "2024-01-10", ["coordinates_pdbx"]),
        "pdb_00002def": PDBRecord("pdb_00002def", "2024-02-15", []),
    }
    snap_path = tmp_path / "snapshot.json.gz"
    save_holdings_snapshot(records, snap_path)
    assert load_holdings_snapshot(snap_path) == records


