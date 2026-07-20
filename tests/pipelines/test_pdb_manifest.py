"""Unit tests for the PDB manifest-generating pipeline."""

import gzip
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cdm_data_loaders.pdb.constants import (
    HoldingsFile,
    HoldingsFileSchemas,
    HoldingsFileTypes,
    ManifestData,
    PDBRecord,
)
from cdm_data_loaders.pipelines.pdb_manifest import (
    _download_holdings_files,
    _extract_id_from_s3_key,
    _generate_manifest_data,
    _load_holdings_snapshot,
    _parse_pdb_record,
    _save_holdings_snapshot,
    _save_manifest_files,
    _save_summary_file,
)

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_HOLDINGS_ID_ONLY = HoldingsFile(filename=Path("dummy.json.gz"), schema=HoldingsFileSchemas.ID_ONLY)
_HOLDINGS_ID_DATE = HoldingsFile(filename=Path("dummy.json.gz"), schema=HoldingsFileSchemas.ID_DATE)
_HOLDINGS_BAD = HoldingsFile(filename=Path("dummy.json.gz"), schema="bad_schema")  # type: ignore[arg-type]

_ID_A = "pdb_00001abc"
_ID_B = "pdb_00001def"
_ID_C = "pdb_aaaaaaaa"
_ID_D = "pdb_03942abd"

_DATE_OLD = "2023-01-01"
_DATE_NEW = "2024-06-15"

_MANIFEST_ONE_OF_EACH = ManifestData(new=[_ID_A], updated=[_ID_B], removed=[_ID_C], missing_dates=[_ID_D])
_MANIFEST_THREE_NEW = ManifestData(new=[_ID_A, _ID_B, _ID_C], updated=[], removed=[], missing_dates=[])


def _make_gzipped_response(data: dict | list[str]) -> MagicMock:
    """Return a mock context-manager response yielding gzipped JSON bytes."""
    compressed = gzip.compress(json.dumps(data).encode())
    resp = MagicMock()
    resp.read.return_value = compressed
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
# _parse_pdb_record
# ---------------------------------------------------------------------------


class TestParsePdbRecord:
    """Parameterized cases for testing _parse_pdb_record()."""

    @pytest.mark.parametrize(
        ("holdings", "raw", "expected"),
        [
            pytest.param(
                _HOLDINGS_ID_ONLY,
                {"PDB_00001ABC": {"status": "RELEASED"}, "PDB_00001DEF": {}},
                {"pdb_00001abc": PDBRecord(id="pdb_00001abc"), "pdb_00001def": PDBRecord(id="pdb_00001def")},
                id="id-only-extracts-keys",
            ),
            pytest.param(
                _HOLDINGS_ID_ONLY,
                {"PDB_AAAAAAAA": {}},
                {"pdb_aaaaaaaa": PDBRecord(id="pdb_aaaaaaaa")},
                id="id-only-normalizes-case",
            ),
            pytest.param(_HOLDINGS_ID_ONLY, {}, {}, id="id-only-empty"),
            pytest.param(
                _HOLDINGS_ID_DATE,
                {"PDB_00001ABC": "2024-01-15", "PDB_00001DEF": "2024-02-20"},
                {
                    "pdb_00001abc": PDBRecord(id="pdb_00001abc", last_modified="2024-01-15"),
                    "pdb_00001def": PDBRecord(id="pdb_00001def", last_modified="2024-02-20"),
                },
                id="id-date-extracts-key-and-date",
            ),
            pytest.param(
                _HOLDINGS_ID_DATE,
                {"PDB_AAAAAAAA": "2024-03-01"},
                {"pdb_aaaaaaaa": PDBRecord(id="pdb_aaaaaaaa", last_modified="2024-03-01")},
                id="id-date-normalizes-case",
            ),
            pytest.param(_HOLDINGS_ID_DATE, {}, {}, id="id-date-empty"),
        ],
    )
    def test_parse_pdb_record(self, holdings: HoldingsFile, raw: dict, expected: dict) -> None:
        """Tests parsed output is as expected."""
        assert _parse_pdb_record(holdings, raw) == expected

    def test_invalid_schema_raises_value_error(self) -> None:
        """Ensures ValueError is raised when an invalid schema is specified."""
        with pytest.raises(ValueError, match="Invalid holdings file schema"):
            _parse_pdb_record(_HOLDINGS_BAD, {"PDB_00001ABC": {}})


# ---------------------------------------------------------------------------
# _generate_manifest_data
# ---------------------------------------------------------------------------


class TestGenerateManifestData:
    """Test for _generate_manifest_data()."""

    @pytest.mark.parametrize(
        ("current", "previous", "expected_new"),
        [
            pytest.param(
                {
                    _ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW),
                    _ID_B: PDBRecord(id=_ID_B, last_modified=_DATE_NEW),
                },
                {},
                [_ID_A, _ID_B],
                id="all-new-no-previous",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW)},
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_OLD)},
                [],
                id="already-in-previous",
            ),
        ],
    )
    def test_new_field(
        self,
        current: dict[str, PDBRecord],
        previous: dict[str, PDBRecord],
        expected_new: list[str],
    ) -> None:
        """Tests new records are properly determined."""
        result = _generate_manifest_data(current, removed=set(), previous=previous)
        assert sorted(result.new) == sorted(expected_new)

    @pytest.mark.parametrize(
        ("current", "previous", "expect_in_updated"),
        [
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW)},
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_OLD)},
                True,
                id="newer-date",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW)},
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW)},
                False,
                id="same-date",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_OLD)},
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW)},
                False,
                id="older-date",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW)},
                {_ID_A: PDBRecord(id=_ID_A, last_modified="")},
                True,
                id="prev-no-date",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A, last_modified="")},
                {_ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_OLD)},
                False,
                id="current-no-date",
            ),
        ],
    )
    def test_updated_field(
        self,
        current: dict[str, PDBRecord],
        previous: dict[str, PDBRecord],
        expect_in_updated: bool,
    ) -> None:
        """Ensures various combinations of specified and unspecified dates are handled."""
        result = _generate_manifest_data(current, removed=set(), previous=previous)
        assert (_ID_A in result.updated) is expect_in_updated

    @pytest.mark.parametrize(
        ("current", "removed_set", "previous", "expected_removed"),
        [
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A)},
                set(),
                {_ID_A: PDBRecord(id=_ID_A), _ID_B: PDBRecord(id=_ID_B)},
                [_ID_B],
                id="vanished-from-current",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A)},
                {_ID_B},
                {_ID_A: PDBRecord(id=_ID_A), _ID_B: PDBRecord(id=_ID_B)},
                [_ID_B],
                id="explicit-removed-in-previous",
            ),
            pytest.param(
                {_ID_A: PDBRecord(id=_ID_A)},
                {_ID_C},
                {_ID_A: PDBRecord(id=_ID_A)},
                [],
                id="explicit-removed-not-in-previous",
            ),
            pytest.param(
                {},
                set(),
                {_ID_B: PDBRecord(id=_ID_B), _ID_A: PDBRecord(id=_ID_A)},
                sorted([_ID_A, _ID_B]),
                id="all-removed-sorted",
            ),
        ],
    )
    def test_removed_field(
        self,
        current: dict[str, PDBRecord],
        removed_set: set[str],
        previous: dict[str, PDBRecord],
        expected_removed: list[str],
    ) -> None:
        """Ensures removed records are correctly determined based on holdings files and previous state."""
        result = _generate_manifest_data(current, removed=removed_set, previous=previous)
        assert result.removed == expected_removed

    def test_missing_dates_propagated(self) -> None:
        """Ensures missing dates are properly set."""
        result = _generate_manifest_data({}, removed=set(), missing_dates=[_ID_A, _ID_B])
        assert result.missing_dates == [_ID_A, _ID_B]

    def test_new_and_updated_are_disjoint(self) -> None:
        """An entry cannot be both new and updated."""
        current = {
            _ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW),  # new (not in previous)
            _ID_B: PDBRecord(id=_ID_B, last_modified=_DATE_NEW),  # updated (in previous, newer date)
        }
        previous = {_ID_B: PDBRecord(id=_ID_B, last_modified=_DATE_OLD)}
        result = _generate_manifest_data(current, removed=set(), previous=previous)
        assert set(result.new) & set(result.updated) == set()


# ---------------------------------------------------------------------------
# _extract_id_from_s3_key
# ---------------------------------------------------------------------------


class TestExtractIdFromS3Key:
    """Tests for _extract_id_from_s3_key()."""

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            pytest.param(
                "tenant-general-warehouse/pdb/raw_data/pdb_00001abc/file.cif.gz",
                "pdb_00001abc",
                id="nested-path",
            ),
            pytest.param("some/unrelated/path.txt", None, id="no-match"),
            pytest.param("data/PDB_00001ABC/file.cif", "pdb_00001abc", id="uppercase-normalized"),
            pytest.param(
                "pdb_00001abc/subdir/pdb_00001def/file",
                "pdb_00001abc",
                id="first-match-returned",
            ),
        ],
    )
    def test_extract_id(self, key: str, expected: str | None) -> None:
        """Ensures ids are properly extracted when present."""
        assert _extract_id_from_s3_key(key) == expected


# ---------------------------------------------------------------------------
# _save_holdings_snapshot / _load_holdings_snapshot
# ---------------------------------------------------------------------------


class TestHoldingsSnapshotRoundTrip:
    """Tests for IO of snapshop data."""

    @pytest.mark.parametrize(
        "records",
        [
            pytest.param(
                {
                    _ID_A: PDBRecord(id=_ID_A, last_modified=_DATE_NEW),
                    _ID_B: PDBRecord(id=_ID_B, last_modified=_DATE_OLD),
                },
                id="two-records",
            ),
            pytest.param({}, id="empty"),
        ],
    )
    def test_round_trip(self, tmp_path: Path, records: dict[str, PDBRecord]) -> None:
        """Tests save/load of snapshot data."""
        path = tmp_path / "snapshot.json.gz"
        _save_holdings_snapshot(records, path)
        assert _load_holdings_snapshot(path) == records

    def test_missing_last_modified_defaults_to_empty_string(self, tmp_path: Path) -> None:
        """Snapshot entries with no last_modified field are loaded with empty string."""
        payload = {_ID_A: PDBRecord(id=_ID_A)}  # no "last_modified" key
        path = tmp_path / "partial.json.gz"
        _save_holdings_snapshot(payload, path)
        assert _load_holdings_snapshot(path)[_ID_A].last_modified == ""


# ---------------------------------------------------------------------------
# _download_holdings_files
# ---------------------------------------------------------------------------


class TestDownloadHoldingsFiles:
    """Tests for _download_holdings_files()."""

    def test_downloads_and_parses_all_three_files(self) -> None:
        """Tests that each holdings file type is properly loaded."""
        raw_current = {"PDB_00001ABC": {}, "PDB_00001DEF": {}}
        raw_dates = {"PDB_00001ABC": "2024-01-15"}
        raw_removed = {"PDB_00009999": {}}

        responses = [
            _make_gzipped_response(raw_current),
            _make_gzipped_response(raw_dates),
            _make_gzipped_response(raw_removed),
        ]

        with patch("cdm_data_loaders.pipelines.pdb_manifest.urlopen", side_effect=responses):
            result = _download_holdings_files()

        assert HoldingsFileTypes.CURRENT in result
        assert HoldingsFileTypes.LAST_MODIFIED in result
        assert HoldingsFileTypes.REMOVED in result
        assert "pdb_00001abc" in result[HoldingsFileTypes.CURRENT]
        assert "pdb_00001def" in result[HoldingsFileTypes.CURRENT]
        assert result[HoldingsFileTypes.LAST_MODIFIED]["pdb_00001abc"].last_modified == "2024-01-15"
        assert "pdb_00009999" in result[HoldingsFileTypes.REMOVED]

    def test_non_dict_response_raises_type_error(self) -> None:
        """Ensures invalid JSON data raises TypeError."""
        with (
            patch(
                "cdm_data_loaders.pipelines.pdb_manifest.urlopen",
                return_value=_make_gzipped_response(["not", "a", "dict"]),
            ),
            pytest.raises(TypeError, match="expected to contain a JSON dict"),
        ):
            _download_holdings_files()


# ---------------------------------------------------------------------------
# _save_manifest_files
# ---------------------------------------------------------------------------


class TestSaveManifestFiles:
    """Tests for _save_manifest_files()."""

    @pytest.mark.parametrize(
        ("data", "filename", "expected_lines"),
        [
            pytest.param(
                _MANIFEST_ONE_OF_EACH,
                "transfer_manifest.txt",
                [_ID_A, _ID_B],
                id="transfer-contains-new-and-updated",
            ),
            pytest.param(
                _MANIFEST_ONE_OF_EACH,
                "updated_manifest.txt",
                [_ID_B],
                id="updated-contains-only-updated",
            ),
            pytest.param(
                _MANIFEST_ONE_OF_EACH,
                "removed_manifest.txt",
                [_ID_C],
                id="removed-contains-only-removed",
            ),
            pytest.param(
                _MANIFEST_ONE_OF_EACH,
                "missing_dates.txt",
                [_ID_D],
                id="missing-dates-empty",
            ),
            pytest.param(
                _MANIFEST_THREE_NEW,
                "transfer_manifest.txt",
                [_ID_A, _ID_B, _ID_C],
                id="each-id-on-its-own-line",
            ),
        ],
    )
    def test_file_contents(self, tmp_path: Path, data: ManifestData, filename: str, expected_lines: list[str]) -> None:
        """Ensures manifest file contents are correct."""
        _save_manifest_files(data, tmp_path)
        lines = (tmp_path / filename).read_text().splitlines()
        assert sorted(lines) == sorted(expected_lines)


# ---------------------------------------------------------------------------
# _save_summary_file
# ---------------------------------------------------------------------------


class TestSaveSummaryFile:
    """Tests for _save_summary_file()."""

    @pytest.mark.parametrize(
        ("data", "regex_filter", "expected_values", "absent_keys"),
        [
            pytest.param(
                ManifestData(new=[_ID_A, _ID_B], updated=[_ID_C], removed=[], missing_dates=[_ID_A]),
                None,
                {"new": 2, "updated": 1, "removed": 0, "missing_dates": 1},
                ["regex_filter"],
                id="counts-correct-no-filter",
            ),
            pytest.param(
                ManifestData(new=[], updated=[], removed=[], missing_dates=[]),
                "pdb_0000",
                {"regex_filter": "pdb_0000"},
                [],
                id="regex-filter-included",
            ),
            pytest.param(
                ManifestData(new=[], updated=[], removed=[], missing_dates=[]),
                None,
                {},
                ["regex_filter"],
                id="regex-filter-omitted",
            ),
        ],
    )
    def test_summary_file(
        self,
        tmp_path: Path,
        data: ManifestData,
        regex_filter: str | None,
        expected_values: dict,
        absent_keys: list[str],
    ) -> None:
        """Ensure metrics are output in summary file."""
        _save_summary_file(data, regex_filter, tmp_path)
        summary = json.loads((tmp_path / "summary.json").read_text())
        for key, val in expected_values.items():
            assert summary[key] == val
        for key in absent_keys:
            assert key not in summary
        assert "generated_at" in summary
        assert isinstance(summary["saved_to"], str)
