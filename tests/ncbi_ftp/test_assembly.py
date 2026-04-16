"""Tests for ncbi_ftp.assembly module — path helpers, file filtering, checksum parsing."""

import pytest

from cdm_data_loaders.ncbi_ftp.assembly import (
    FILE_FILTERS,
    build_accession_path,
    parse_assembly_path,
    parse_md5_checksums_file,
)

_EXPECTED_TWO_ENTRIES = 2


# ── Path helpers ─────────────────────────────────────────────────────────


class TestBuildAccessionPath:
    """Test output directory path construction from assembly names."""

    def test_basic(self) -> None:
        """Verify standard GCF accession path construction."""
        result = build_accession_path("GCF_000001215.4_Release_6_plus_ISO1_MT")
        assert result == "raw_data/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/"

    def test_gca_prefix(self) -> None:
        """Verify GCA prefix path construction."""
        result = build_accession_path("GCA_012345678.1_ASM1234v1")
        assert result == "raw_data/GCA/012/345/678/GCA_012345678.1_ASM1234v1/"

    def test_invalid_raises(self) -> None:
        """Verify ValueError on invalid assembly name."""
        with pytest.raises(ValueError, match="Cannot parse"):
            build_accession_path("invalid_name")


class TestParseAssemblyPath:
    """Test FTP path parsing."""

    def test_basic(self) -> None:
        """Verify db, assembly_dir, and accession are parsed correctly."""
        path = "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/"
        _db, _assembly_dir, accession = parse_assembly_path(path)
        assert _db == "GCF"
        assert _assembly_dir == "GCF_000001215.4_Release_6_plus_ISO1_MT"
        assert accession == "GCF_000001215.4"

    def test_without_trailing_slash(self) -> None:
        """Verify parsing works without trailing slash."""
        path = "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT"
        _db, _assembly_dir, accession = parse_assembly_path(path)
        assert accession == "GCF_000001215.4"

    def test_invalid_raises(self) -> None:
        """Verify ValueError on invalid path."""
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_assembly_path("/random/path/")


# ── FILE_FILTERS sanity ─────────────────────────────────────────────────


class TestFileFilters:
    """Sanity checks for the file suffix filter list."""

    def test_not_empty(self) -> None:
        """Verify FILE_FILTERS is not empty."""
        assert len(FILE_FILTERS) > 0

    def test_all_start_with_underscore(self) -> None:
        """Verify all filter patterns start with an underscore."""
        for f in FILE_FILTERS:
            assert f.startswith("_"), f"Filter should start with underscore: {f}"

    def test_genomic_fna_included(self) -> None:
        """Verify _genomic.fna.gz is in the filter list."""
        assert "_genomic.fna.gz" in FILE_FILTERS

    def test_assembly_report_included(self) -> None:
        """Verify _assembly_report.txt is in the filter list."""
        assert "_assembly_report.txt" in FILE_FILTERS


# ── parse_md5_checksums_file ─────────────────────────────────────────────


class TestParseMd5ChecksumsFile:
    """Test NCBI md5checksums.txt parsing."""

    def test_basic(self) -> None:
        """Verify parsing of standard md5checksums.txt format."""
        text = "abc123  ./GCF_000001215.4_genomic.fna.gz\ndef456  ./GCF_000001215.4_genomic.gff.gz\n"
        result = parse_md5_checksums_file(text)
        assert result == {
            "GCF_000001215.4_genomic.fna.gz": "abc123",
            "GCF_000001215.4_genomic.gff.gz": "def456",
        }

    def test_no_leading_dot_slash(self) -> None:
        """Verify parsing works without leading ./ prefix."""
        text = "abc123  GCF_000001215.4_genomic.fna.gz\n"
        result = parse_md5_checksums_file(text)
        assert result == {"GCF_000001215.4_genomic.fna.gz": "abc123"}

    def test_empty(self) -> None:
        """Verify empty or whitespace-only input returns empty dict."""
        assert parse_md5_checksums_file("") == {}
        assert parse_md5_checksums_file("   \n  \n") == {}

    def test_blank_lines_ignored(self) -> None:
        """Verify blank lines between entries are skipped."""
        text = "abc123  file1.txt\n\n\ndef456  file2.txt\n"
        result = parse_md5_checksums_file(text)
        assert len(result) == _EXPECTED_TWO_ENTRIES
