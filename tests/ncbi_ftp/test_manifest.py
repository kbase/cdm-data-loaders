"""Tests for ncbi_ftp.manifest module — assembly summary parsing, diff, filtering, writing."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from cdm_data_loaders.ncbi_ftp.manifest import (
    DiffResult,
    _ftp_dir_from_url,
    accession_prefix,
    compute_diff,
    filter_by_prefix_range,
    get_latest_assembly_paths,
    parse_assembly_summary,
    verify_transfer_candidates,
    write_diff_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)

from .conftest import SAMPLE_SUMMARY

_EXPECTED_ENTRIES = 4
_EXPECTED_TWO = 2
_EXPECTED_TOTAL_TRANSFER = 2


# ── parse_assembly_summary ───────────────────────────────────────────────


class TestParseAssemblySummary:
    """Test assembly summary parsing."""

    def test_parse_basic(self) -> None:
        """Verify basic parsing returns expected number of assemblies."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        assert len(assemblies) == _EXPECTED_ENTRIES
        assert "GCF_000001215.4" in assemblies
        assert "GCF_000005845.2" in assemblies
        assert "GCF_000099999.1" not in assemblies  # ftp_path == "na"

    def test_parse_status(self) -> None:
        """Verify status field is parsed correctly."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        assert assemblies["GCF_000001215.4"].status == "latest"
        assert assemblies["GCF_000005845.2"].status == "replaced"
        assert assemblies["GCF_000009999.1"].status == "suppressed"

    def test_parse_seq_rel_date(self) -> None:
        """Verify seq_rel_date field is parsed correctly."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        assert assemblies["GCF_000001215.4"].seq_rel_date == "2014/10/21"

    def test_parse_assembly_dir(self) -> None:
        """Verify assembly_dir is extracted from the FTP path."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        assert assemblies["GCF_000001215.4"].assembly_dir == "GCF_000001215.4_Release_6_plus_ISO1_MT"

    def test_parse_ftp_path(self) -> None:
        """Verify full FTP path is stored."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        assert assemblies["GCF_000001215.4"].ftp_path == (
            "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT"
        )

    def test_parse_empty(self) -> None:
        """Verify empty or comment-only input returns empty dict."""
        assemblies = parse_assembly_summary("# comment only\n")
        assert len(assemblies) == 0

    def test_parse_skips_comments(self) -> None:
        """Verify comment lines are not included in results."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        for acc in assemblies:
            assert acc.startswith("GCF_")

    def test_parse_from_file(self, tmp_path: Path) -> None:
        """Verify parsing from a file path object."""
        f = tmp_path / "summary.tsv"
        f.write_text(SAMPLE_SUMMARY)
        assemblies = parse_assembly_summary(f)
        assert len(assemblies) == _EXPECTED_ENTRIES

    def test_parse_from_file_str(self, tmp_path: Path) -> None:
        """Verify parsing from a string file path."""
        f = tmp_path / "summary.tsv"
        f.write_text(SAMPLE_SUMMARY)
        assemblies = parse_assembly_summary(str(f))
        assert len(assemblies) == _EXPECTED_ENTRIES

    def test_parse_from_list_of_lines(self) -> None:
        """Verify parsing from a list of lines."""
        lines = SAMPLE_SUMMARY.splitlines(keepends=True)
        assemblies = parse_assembly_summary(lines)
        assert len(assemblies) == _EXPECTED_ENTRIES


# ── get_latest_assembly_paths ────────────────────────────────────────────


class TestGetLatestAssemblyPaths:
    """Test extraction of FTP paths for latest assemblies."""

    def test_only_latest(self) -> None:
        """Verify only assemblies with status 'latest' are returned."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        paths = get_latest_assembly_paths(assemblies)
        accessions = [acc for acc, _ in paths]
        assert "GCF_000001215.4" in accessions
        assert "GCF_000001405.40" in accessions
        assert "GCF_000005845.2" not in accessions  # replaced
        assert "GCF_000009999.1" not in accessions  # suppressed

    def test_path_conversion(self) -> None:
        """Verify HTTPS paths are converted to FTP-relative paths."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        paths = dict(get_latest_assembly_paths(assemblies))
        assert paths["GCF_000001215.4"] == "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/"

    def test_paths_end_with_slash(self) -> None:
        """Verify all returned paths end with a trailing slash."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        for _, path in get_latest_assembly_paths(assemblies):
            assert path.endswith("/")

    def test_empty(self) -> None:
        """Verify empty input returns empty list."""
        assemblies = parse_assembly_summary("# empty\n")
        assert get_latest_assembly_paths(assemblies) == []


# ── compute_diff ─────────────────────────────────────────────────────────


class TestComputeDiff:
    """Test diff computation between current and previous assembly state."""

    def test_all_new_no_previous(self) -> None:
        """Verify all latest assemblies are marked new when no previous state."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions=set())
        assert "GCF_000001215.4" in diff.new
        assert "GCF_000001405.40" in diff.new
        assert "GCF_000005845.2" not in diff.new  # replaced

    def test_nothing_new_when_all_known(self) -> None:
        """Verify no new assemblies when all are already known."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        known = {"GCF_000001215.4", "GCF_000001405.40"}
        diff = compute_diff(current, previous_accessions=known)
        assert len(diff.new) == 0

    def test_detects_updated_seq_rel_date(self) -> None:
        """Verify assemblies with changed seq_rel_date are marked updated."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        previous = parse_assembly_summary(SAMPLE_SUMMARY)
        previous["GCF_000001215.4"].seq_rel_date = "2010/01/01"
        diff = compute_diff(current, previous_assemblies=previous)
        assert "GCF_000001215.4" in diff.updated

    def test_detects_replaced(self) -> None:
        """Verify assemblies with status 'replaced' are detected."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions={"GCF_000005845.2"})
        assert "GCF_000005845.2" in diff.replaced

    def test_detects_suppressed(self) -> None:
        """Verify assemblies with status 'suppressed' are detected."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions={"GCF_000009999.1"})
        assert "GCF_000009999.1" in diff.suppressed

    def test_detects_withdrawn(self) -> None:
        """Accessions in previous but entirely absent from current."""
        current = parse_assembly_summary("# empty\n")
        diff = compute_diff(current, previous_accessions={"GCF_000001215.4"})
        assert "GCF_000001215.4" in diff.suppressed

    def test_scan_store_fallback(self) -> None:
        """Verify known accessions are not marked as new."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions={"GCF_000001215.4"})
        assert "GCF_000001215.4" not in diff.new
        assert "GCF_000001405.40" in diff.new

    def test_results_are_sorted(self) -> None:
        """Verify diff results are sorted alphabetically."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions=set())
        assert diff.new == sorted(diff.new)


# ── accession_prefix & filter_by_prefix_range ────────────────────────────


class TestPrefixFiltering:
    """Test prefix extraction and range filtering."""

    def test_accession_prefix(self) -> None:
        """Verify 3-digit prefix extraction from accessions."""
        assert accession_prefix("GCF_000001215.4") == "000"
        assert accession_prefix("GCF_123456789.1") == "123"
        assert accession_prefix("invalid") is None

    def test_filter_range_inclusive(self) -> None:
        """Verify prefix range filter is inclusive."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        filtered = filter_by_prefix_range(assemblies, "000", "000")
        assert len(filtered) == len(assemblies)

    def test_filter_excludes_out_of_range(self) -> None:
        """Verify assemblies outside the prefix range are excluded."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        filtered = filter_by_prefix_range(assemblies, "001", "999")
        assert len(filtered) == 0

    def test_no_filter_returns_all(self) -> None:
        """Verify no prefix range returns all assemblies."""
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        filtered = filter_by_prefix_range(assemblies)
        assert len(filtered) == len(assemblies)


# ── Manifest writing ────────────────────────────────────────────────────


class TestManifestWriting:
    """Test manifest file writing."""

    def test_write_transfer_manifest(self, tmp_path: Path) -> None:
        """Verify transfer manifest file is written correctly."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions=set())
        manifest_file = tmp_path / "transfer.txt"
        paths = write_transfer_manifest(diff, current, manifest_file)
        assert len(paths) > 0
        lines = [line.strip() for line in manifest_file.read_text().splitlines() if line.strip()]
        assert len(lines) == len(paths)
        for line in lines:
            assert line.startswith("/genomes/")
            assert line.endswith("/")

    def test_write_removed_manifest(self, tmp_path: Path) -> None:
        """Verify removed manifest lists replaced and suppressed accessions."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions={"GCF_000005845.2", "GCF_000009999.1"})
        removed_file = tmp_path / "removed.txt"
        removed = write_removed_manifest(diff, removed_file)
        assert len(removed) == _EXPECTED_TWO
        lines = [line.strip() for line in removed_file.read_text().splitlines() if line.strip()]
        assert len(lines) == _EXPECTED_TWO

    def test_write_updated_manifest(self, tmp_path: Path) -> None:
        """Verify updated manifest lists only updated accessions."""
        diff = DiffResult(new=["GCF_000001215.4"], updated=["GCF_000005845.2", "GCF_000001405.40"])
        updated_file = tmp_path / "updated.txt"
        updated = write_updated_manifest(diff, updated_file)
        assert len(updated) == _EXPECTED_TWO
        lines = [line.strip() for line in updated_file.read_text().splitlines() if line.strip()]
        assert len(lines) == _EXPECTED_TWO
        # Should be sorted
        assert lines[0] == "GCF_000001405.40"
        assert lines[1] == "GCF_000005845.2"

    def test_write_diff_summary(self, tmp_path: Path) -> None:
        """Verify diff summary JSON is written with correct counts."""
        diff = DiffResult(new=["a"], updated=["b"], replaced=["c"], suppressed=[])
        summary_file = tmp_path / "summary.json"
        summary = write_diff_summary(diff, summary_file, "refseq", "000", "003")
        assert summary["counts"]["new"] == 1
        assert summary["counts"]["total_to_transfer"] == _EXPECTED_TOTAL_TRANSFER
        assert summary["prefix_range"]["from"] == "000"

        loaded = json.loads(summary_file.read_text())
        assert loaded["database"] == "refseq"


# ── _ftp_dir_from_url ───────────────────────────────────────────────────


class TestFtpDirFromUrl:
    """Test FTP URL to directory path conversion."""

    def test_https_url(self) -> None:
        """Verify https:// URLs are converted to FTP paths."""
        url = "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6"
        assert _ftp_dir_from_url(url) == "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6"

    def test_ftp_url(self) -> None:
        """Verify ftp:// URLs are converted to FTP paths."""
        url = "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6"
        assert _ftp_dir_from_url(url) == "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6"

    def test_bare_path(self) -> None:
        """Verify bare paths are returned unchanged."""
        path = "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6"
        assert _ftp_dir_from_url(path) == path

    def test_custom_ftp_host(self) -> None:
        """Verify custom FTP host is stripped from ftp:// URLs."""
        url = "ftp://custom.host.example.com/genomes/all/GCF/000/001/215"
        assert _ftp_dir_from_url(url, ftp_host="custom.host.example.com") == "/genomes/all/GCF/000/001/215"


# ── verify_transfer_candidates ───────────────────────────────────────────


_MD5_CHECKSUMS_TXT = (
    "d41d8cd98f00b204e9800998ecf8427e  ./GCF_000001215.4_Release_6_plus_ISO1_MT_genomic.fna.gz\n"
    "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4  ./GCF_000001215.4_Release_6_plus_ISO1_MT_protein.faa.gz\n"
    "ffffffffffffffffffffffffffffffff  ./GCF_000001215.4_Release_6_plus_ISO1_MT_assembly_report.txt\n"
    "0000000000000000000000000000dead  ./GCF_000001215.4_Release_6_plus_ISO1_MT_README.txt\n"
)


def _mock_s3_with_objects() -> MagicMock:
    """Return a mock S3 client whose list_objects_v2 always reports objects exist."""
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 1}
    return client


def _mock_s3_empty() -> MagicMock:
    """Return a mock S3 client whose list_objects_v2 reports no objects."""
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 0}
    return client


class TestVerifyTransferCandidates:
    """Test S3 checksum verification to prune transfer candidates."""

    def _assemblies(self) -> dict:
        return parse_assembly_summary(SAMPLE_SUMMARY)

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.head_object")
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", return_value=_MD5_CHECKSUMS_TXT)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_prunes_when_all_match(
        self,
        mock_connect: MagicMock,
        mock_retrieve: MagicMock,
        mock_head: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Assemblies where every file matches S3 are pruned from the list."""
        mock_connect.return_value = MagicMock()

        def head_side_effect(s3_path: str) -> dict | None:
            if "_genomic.fna.gz" in s3_path:
                return {
                    "size": 100,
                    "metadata": {"md5": "d41d8cd98f00b204e9800998ecf8427e"},
                    "checksum_crc64nvme": None,
                }
            if "_protein.faa.gz" in s3_path:
                return {
                    "size": 100,
                    "metadata": {"md5": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"},
                    "checksum_crc64nvme": None,
                }
            if "_assembly_report.txt" in s3_path:
                return {
                    "size": 100,
                    "metadata": {"md5": "ffffffffffffffffffffffffffffffff"},
                    "checksum_crc64nvme": None,
                }
            return None

        mock_head.side_effect = head_side_effect
        result = verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == []

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.head_object")
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", return_value=_MD5_CHECKSUMS_TXT)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_keeps_when_md5_differs(
        self,
        mock_connect: MagicMock,
        mock_retrieve: MagicMock,
        mock_head: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Assembly is kept when at least one file has a different MD5."""
        mock_connect.return_value = MagicMock()
        mock_head.return_value = {"size": 100, "metadata": {"md5": "WRONG"}, "checksum_crc64nvme": None}

        result = verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == ["GCF_000001215.4"]

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.head_object", return_value=None)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", return_value=_MD5_CHECKSUMS_TXT)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_keeps_when_s3_object_missing(
        self,
        mock_connect: MagicMock,
        mock_retrieve: MagicMock,
        mock_head: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Assembly is kept when at least one file doesn't exist in S3."""
        mock_connect.return_value = MagicMock()

        result = verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == ["GCF_000001215.4"]

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.head_object")
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", return_value=_MD5_CHECKSUMS_TXT)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_keeps_when_s3_has_no_md5_metadata(
        self,
        mock_connect: MagicMock,
        mock_retrieve: MagicMock,
        mock_head: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Assembly is kept when S3 object exists but has no md5 metadata."""
        mock_connect.return_value = MagicMock()
        mock_head.return_value = {"size": 100, "metadata": {}, "checksum_crc64nvme": None}

        result = verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == ["GCF_000001215.4"]

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", side_effect=Exception("FTP error"))
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_keeps_when_ftp_fails(self, mock_connect: MagicMock, mock_retrieve: MagicMock, mock_s3: MagicMock) -> None:
        """Assembly is kept (conservative) when md5checksums.txt cannot be fetched."""
        mock_connect.return_value = MagicMock()

        result = verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == ["GCF_000001215.4"]

    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_empty_input(self, mock_connect: MagicMock) -> None:
        """Empty accession list returns empty result without connecting."""
        result = verify_transfer_candidates([], {}, "cdm-lake", "prefix/")
        assert result == []

    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_unknown_accession_kept(self, mock_connect: MagicMock) -> None:
        """Accessions not in assemblies dict are kept (conservative)."""
        mock_connect.return_value = MagicMock()
        result = verify_transfer_candidates(["GCF_999999999.1"], {}, "cdm-lake", "prefix/")
        assert result == ["GCF_999999999.1"]

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.head_object", return_value=None)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", return_value=_MD5_CHECKSUMS_TXT)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_short_circuits_on_first_mismatch(
        self,
        mock_connect: MagicMock,
        mock_retrieve: MagicMock,
        mock_head: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Verification stops checking after the first missing/mismatched file."""
        mock_connect.return_value = MagicMock()

        verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert mock_head.call_count == 1

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_with_objects())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.head_object")
    @patch("cdm_data_loaders.ncbi_ftp.manifest.ftp_retrieve_text", return_value=_MD5_CHECKSUMS_TXT)
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_mixed_candidates(
        self,
        mock_connect: MagicMock,
        mock_retrieve: MagicMock,
        mock_head: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Verify a mix of matching and non-matching assemblies."""
        mock_connect.return_value = MagicMock()

        def head_side_effect(s3_path: str) -> dict | None:
            # GCF_000001215.4 assembly dir → all match; GCF_000001405.40 → missing
            if "GCF_000001215.4_Release_6_plus_ISO1_MT/" in s3_path:
                if "_genomic.fna.gz" in s3_path:
                    return {
                        "size": 1,
                        "metadata": {"md5": "d41d8cd98f00b204e9800998ecf8427e"},
                        "checksum_crc64nvme": None,
                    }
                if "_protein.faa.gz" in s3_path:
                    return {
                        "size": 1,
                        "metadata": {"md5": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"},
                        "checksum_crc64nvme": None,
                    }
                if "_assembly_report.txt" in s3_path:
                    return {
                        "size": 1,
                        "metadata": {"md5": "ffffffffffffffffffffffffffffffff"},
                        "checksum_crc64nvme": None,
                    }
            return None

        mock_head.side_effect = head_side_effect
        result = verify_transfer_candidates(
            ["GCF_000001215.4", "GCF_000001405.40"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == ["GCF_000001405.40"]

    @patch("cdm_data_loaders.ncbi_ftp.manifest.get_s3_client", return_value=_mock_s3_empty())
    @patch("cdm_data_loaders.ncbi_ftp.manifest.connect_ftp")
    def test_skips_ftp_when_folder_missing_from_store(
        self,
        mock_connect: MagicMock,
        mock_s3: MagicMock,
    ) -> None:
        """Accessions with no objects in S3 are confirmed without FTP round-trip."""
        result = verify_transfer_candidates(
            ["GCF_000001215.4"],
            self._assemblies(),
            "cdm-lake",
            "tenant-general-warehouse/kbase/datasets/ncbi/",
        )
        assert result == ["GCF_000001215.4"]
        # FTP should never have been connected (lazy init)
        mock_connect.assert_not_called()
