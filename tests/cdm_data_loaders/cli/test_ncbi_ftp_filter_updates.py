"""Unit tests for ``cdm_data_loaders.cli.ncbi_ftp_filter_updates``."""

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.cli.ncbi_ftp_filter_updates import (
    FilterNcbiUpdatesSettings,
    cli,
    run_filter_ncbi_updates,
)
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.datasets.ncbi_ftp.constants import Source


@pytest.fixture
def input_files(tmp_path: Path) -> tuple[Path, Path]:
    """Write sample modification-times and assemblies-of-interest files under tmp_path."""
    mod_times = tmp_path / "ftp_mod_times.txt"
    mod_times.write_text(
        "GCF_000000001.1_RefBefore\t2024/01/10\n"
        "GCF_000000002.1_RefAfterUnlisted\t2024/06/15\n"
        "GCA_000000003.1_GenAfterListed\t2024/07/01\n"
        "GCA_000000004.1_GenAfterUnlisted\t2024/08/01\n",
        encoding="utf-8",
    )
    interesting = tmp_path / "assemblies.txt"
    interesting.write_text("GCF_000000001.1\nGCA_000000003.1\n", encoding="utf-8")
    return mod_times, interesting


def test_cli_runner_wiring() -> None:
    """Verify cli binds FilterNcbiUpdatesSettings to run_filter_ncbi_updates."""
    assert isinstance(cli, CliRunner)
    assert cli.settings_cls is FilterNcbiUpdatesSettings
    assert cli.pipeline_fn is run_filter_ncbi_updates


@pytest.mark.parametrize(
    ("raw_date", "expected_date"),
    [
        pytest.param("2024/05/01", date(2024, 5, 1), id="slash-separated"),
        pytest.param("2024-05-01", date(2024, 5, 1), id="iso-hyphenated"),
        pytest.param(date(2024, 5, 1), date(2024, 5, 1), id="date-instance"),
    ],
)
def test_settings_date_formats_and_paths(
    input_files: tuple[Path, Path],
    tmp_path: Path,
    raw_date: date | str,
    expected_date: date,
) -> None:
    """Accept YYYY/MM/DD, YYYY-MM-DD, and date objects and expose derived directories."""
    mod_times, interesting = input_files
    out_dir = tmp_path / "out"
    settings = FilterNcbiUpdatesSettings(
        ftp_mod_times_path=mod_times,
        assemblies_of_interest_path=interesting,
        mod_date=raw_date,  # type: ignore[arg-type]
        output_dir=out_dir,
    )
    assert settings.mod_date == expected_date
    assert settings.mod_date_tag == "20240501"
    assert settings.derived_dir == out_dir / "derived"
    assert settings.source_dir(Source.REFSEQ) == out_dir / "refseq"
    assert settings.source_dir(Source.GENBANK) == out_dir / "genbank"


@pytest.mark.parametrize(
    ("field_overrides", "match"),
    [
        pytest.param({"mod_date": "not-a-date"}, "Invalid isoformat string", id="invalid-date"),
        pytest.param({"batch_size": 0}, "greater than 0", id="zero-batch-size"),
    ],
)
def test_settings_validation_errors(
    input_files: tuple[Path, Path],
    tmp_path: Path,
    field_overrides: dict[str, object],
    match: str,
) -> None:
    """Reject malformed modification dates and non-positive batch sizes."""
    mod_times, interesting = input_files
    kwargs: dict[str, object] = {
        "ftp_mod_times_path": mod_times,
        "assemblies_of_interest_path": interesting,
        "mod_date": "2024/05/01",
        "output_dir": tmp_path / "out",
        **field_overrides,
    }
    with pytest.raises(ValidationError, match=match):
        FilterNcbiUpdatesSettings(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("filter_refseq", "expected_refseq_after"),
    [
        pytest.param(False, ["GCF_000000002.1"], id="default-retains-all-refseq"),
        pytest.param(True, [], id="flag-filters-refseq"),
    ],
)
def test_run_filter_ncbi_updates_writes_tables_and_batches(
    input_files: tuple[Path, Path],
    tmp_path: Path,
    filter_refseq: bool,
    expected_refseq_after: list[str],
) -> None:
    """Write derived TSV tables and batched ID files for each (Source, Period) split."""
    mod_times, interesting = input_files
    out_dir = tmp_path / "output"
    cli_args = [
        "--ftp-mod-times-path",
        str(mod_times),
        "--assemblies-of-interest-path",
        str(interesting),
        "--mod-date",
        "2024/05/01",
        "--output-dir",
        str(out_dir),
        "--batch-size",
        "1",
    ]
    if filter_refseq:
        cli_args.append("--filter-refseq")

    settings = CliApp.run(FilterNcbiUpdatesSettings, cli_args=cli_args)
    run_filter_ncbi_updates(settings)

    assert (out_dir / "derived" / "ftp_mod_times_parsed.tsv").is_file()
    assert (out_dir / "derived" / "interesting_assemblies.tsv").is_file()
    assert (out_dir / "derived" / "refseq_before_20240501.tsv").is_file()
    assert (out_dir / "derived" / "genbank_after_20240501.tsv").is_file()

    assert (out_dir / "refseq" / "before_20240501-1.txt").read_text(encoding="utf-8") == "GCF_000000001.1\n"
    assert (out_dir / "genbank" / "after_20240501-1.txt").read_text(encoding="utf-8") == "GCA_000000003.1\n"
    assert not (out_dir / "genbank" / "before_20240501-1.txt").exists()

    refseq_after_batch = out_dir / "refseq" / "after_20240501-1.txt"
    if expected_refseq_after:
        assert refseq_after_batch.read_text(encoding="utf-8").splitlines() == expected_refseq_after
    else:
        assert not refseq_after_batch.exists()
