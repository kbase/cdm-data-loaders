"""Unit tests for ``cdm_data_loaders.datasets.ncbi_ftp.updates``."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from cdm_data_loaders.datasets.ncbi_ftp.constants import Source
from cdm_data_loaders.datasets.ncbi_ftp.updates import (
    Period,
    Splits,
    batch_count,
    build_interesting_membership_mask,
    build_splits,
    compute_batch_width,
    filter_to_interesting,
    infer_source,
    load_ftp_mod_times,
    load_interesting_assembly_ids,
    parse_assembly_dir,
    save_tsv,
    split_by_period,
    split_by_source,
    write_id_batches,
)


@pytest.mark.parametrize(
    ("assembly_dir", "expected"),
    [
        pytest.param(
            "GCF_000001215.4_Release_6_plus_ISO1_MT",
            ("GCF_000001215.4", "Release_6_plus_ISO1_MT"),
            id="gcf-multi-underscore-name",
        ),
        pytest.param(
            "GCA_012345678.10_ASM123v10",
            ("GCA_012345678.10", "ASM123v10"),
            id="gca-two-digit-version",
        ),
    ],
)
def test_parse_assembly_dir_valid(assembly_dir: str, expected: tuple[str, str]) -> None:
    """Split valid NCBI assembly directory names into accession and assembly name."""
    assert parse_assembly_dir(assembly_dir) == expected


@pytest.mark.parametrize(
    "assembly_dir",
    [
        pytest.param("GCF_000001215.4", id="missing-name-suffix"),
        pytest.param("GCF_000001215.4_", id="empty-name-suffix"),
        pytest.param("XYZ_000001215.4_Name", id="invalid-db-prefix"),
        pytest.param("GCF_12345.1_ShortDigits", id="too-few-digits"),
    ],
)
def test_parse_assembly_dir_invalid(assembly_dir: str) -> None:
    """Raise ValueError when assembly_dir does not match ASSEMBLY_DIR_REGEX."""
    with pytest.raises(ValueError, match="does not match expected pattern"):
        parse_assembly_dir(assembly_dir)


@pytest.mark.parametrize(
    ("assembly_id", "expected"),
    [
        pytest.param("GCF_000001215.4", Source.REFSEQ, id="refseq"),
        pytest.param("GCA_012345678.1", Source.GENBANK, id="genbank"),
    ],
)
def test_infer_source_valid(assembly_id: str, expected: Source) -> None:
    """Map GCF_ and GCA_ prefixes to Source enum members."""
    assert infer_source(assembly_id) is expected


def test_infer_source_invalid() -> None:
    """Raise ValueError when assembly_id lacks a GCF_ or GCA_ prefix."""
    with pytest.raises(ValueError, match="Unrecognized assembly_id prefix"):
        infer_source("GBK_000001215.4")


def test_load_ftp_mod_times_and_interesting_ids(tmp_path: Path) -> None:
    """Parse headerless modification-time TSV and interesting assembly ID list."""
    mod_file = tmp_path / "ftp_mod_times.txt"
    mod_file.write_text(
        "GCF_000001215.4_Rel6\t2024/01/15\nGCA_000005845.2_ASM584v2\t2024/06/01\n",
        encoding="utf-8",
    )
    ids_file = tmp_path / "assemblies.txt"
    ids_file.write_text("GCA_000005845.2\nGCF_999999999.1\n", encoding="utf-8")

    df = load_ftp_mod_times(mod_file)
    assert list(df.columns) == ["assembly_dir", "assembly_id", "assembly_name", "source", "modification_date"]
    assert df.to_dict(orient="records") == [
        {
            "assembly_dir": "GCF_000001215.4_Rel6",
            "assembly_id": "GCF_000001215.4",
            "assembly_name": "Rel6",
            "source": Source.REFSEQ,
            "modification_date": date(2024, 1, 15),
        },
        {
            "assembly_dir": "GCA_000005845.2_ASM584v2",
            "assembly_id": "GCA_000005845.2",
            "assembly_name": "ASM584v2",
            "source": Source.GENBANK,
            "modification_date": date(2024, 6, 1),
        },
    ]
    assert load_interesting_assembly_ids(ids_file) == {"GCA_000005845.2", "GCF_999999999.1"}


@pytest.mark.parametrize(
    ("filter_refseq", "expected_ids"),
    [
        pytest.param(
            False,
            ["GCA_000005845.2", "GCF_000001215.4", "GCF_000001405.40"],
            id="keep-all-refseq",
        ),
        pytest.param(
            True,
            ["GCA_000005845.2", "GCF_000001215.4"],
            id="filter-refseq-and-genbank",
        ),
    ],
)
def test_filter_to_interesting_deduplicates_latest_date(
    filter_refseq: bool,
    expected_ids: list[str],
) -> None:
    """Filter rows according to filter_refseq and retain the latest modification date per assembly."""
    parsed_df = pd.DataFrame(
        [
            {
                "assembly_dir": "GCF_000001215.4_Old",
                "assembly_id": "GCF_000001215.4",
                "assembly_name": "Old",
                "source": Source.REFSEQ,
                "modification_date": date(2023, 1, 1),
            },
            {
                "assembly_dir": "GCF_000001215.4_New",
                "assembly_id": "GCF_000001215.4",
                "assembly_name": "New",
                "source": Source.REFSEQ,
                "modification_date": date(2024, 5, 10),
            },
            {
                "assembly_dir": "GCF_000001405.40_GRCh38",
                "assembly_id": "GCF_000001405.40",
                "assembly_name": "GRCh38",
                "source": Source.REFSEQ,
                "modification_date": date(2024, 2, 1),
            },
            {
                "assembly_dir": "GCA_000005845.2_ASM584v2",
                "assembly_id": "GCA_000005845.2",
                "assembly_name": "ASM584v2",
                "source": Source.GENBANK,
                "modification_date": date(2024, 3, 1),
            },
            {
                "assembly_dir": "GCA_000009999.1_DropMe",
                "assembly_id": "GCA_000009999.1",
                "assembly_name": "DropMe",
                "source": Source.GENBANK,
                "modification_date": date(2024, 4, 1),
            },
        ]
    )
    interesting_ids = {"GCF_000001215.4", "GCA_000005845.2"}

    mask = build_interesting_membership_mask(parsed_df, interesting_ids, filter_refseq=filter_refseq)
    assert isinstance(mask, pd.Series)

    result = filter_to_interesting(parsed_df, interesting_ids, filter_refseq=filter_refseq)
    assert result["assembly_id"].tolist() == expected_ids
    rel_row = result.loc[result["assembly_id"] == "GCF_000001215.4"].iloc[0]
    assert rel_row["assembly_name"] == "New"
    assert rel_row["modification_date"] == date(2024, 5, 10)


def test_split_by_period_source_and_build_splits(tmp_path: Path) -> None:
    """Partition assemblies across (Source, Period) with inclusive threshold date and save TSV."""
    interesting_df = pd.DataFrame(
        [
            {"assembly_id": "GCF_000000001.1", "source": Source.REFSEQ, "modification_date": date(2024, 4, 30)},
            {"assembly_id": "GCF_000000002.1", "source": Source.REFSEQ, "modification_date": date(2024, 5, 1)},
            {"assembly_id": "GCA_000000003.1", "source": Source.GENBANK, "modification_date": date(2024, 5, 2)},
        ]
    )
    threshold = date(2024, 5, 1)

    by_period = split_by_period(interesting_df, threshold)
    assert by_period[Period.BEFORE]["assembly_id"].tolist() == ["GCF_000000001.1"]
    assert by_period[Period.AFTER]["assembly_id"].tolist() == ["GCA_000000003.1", "GCF_000000002.1"]

    by_source = split_by_source(interesting_df)
    assert by_source[Source.REFSEQ]["assembly_id"].tolist() == ["GCF_000000001.1", "GCF_000000002.1"]
    assert by_source[Source.GENBANK]["assembly_id"].tolist() == ["GCA_000000003.1"]

    splits = build_splits(interesting_df, threshold)
    assert {k: v["assembly_id"].tolist() for k, v in splits.items()} == {
        (Source.REFSEQ, Period.BEFORE): ["GCF_000000001.1"],
        (Source.REFSEQ, Period.AFTER): ["GCF_000000002.1"],
        (Source.GENBANK, Period.BEFORE): [],
        (Source.GENBANK, Period.AFTER): ["GCA_000000003.1"],
    }

    tsv_path = tmp_path / "split.tsv"
    save_tsv(splits[(Source.REFSEQ, Period.AFTER)], tsv_path)
    assert tsv_path.read_text(encoding="utf-8") == (
        "assembly_id\tsource\tmodification_date\nGCF_000000002.1\trefseq\t2024-05-01\n"
    )


@pytest.mark.parametrize(
    ("n_items", "batch_size", "expected"),
    [
        pytest.param(0, 10, 0, id="empty"),
        pytest.param(10, 10, 1, id="exact-batch"),
        pytest.param(11, 10, 2, id="partial-remainder"),
    ],
)
def test_batch_count(n_items: int, batch_size: int, expected: int) -> None:
    """Compute ceiling batch counts with zero items returning zero batches."""
    assert batch_count(n_items, batch_size) == expected


@pytest.mark.parametrize(
    ("row_counts", "batch_size", "expected_width"),
    [
        pytest.param((0, 0), 5, 1, id="all-empty-splits"),
        pytest.param((9, 2), 1, 1, id="single-digit-batches"),
        pytest.param((25, 3), 2, 2, id="two-digit-batches"),
    ],
)
def test_compute_batch_width(row_counts: tuple[int, int], batch_size: int, expected_width: int) -> None:
    """Compute zero-padding digit width from the largest split batch count."""
    splits: Splits = {
        (Source.REFSEQ, Period.AFTER): pd.DataFrame({"assembly_id": [f"id_{i}" for i in range(row_counts[0])]}),
        (Source.GENBANK, Period.BEFORE): pd.DataFrame({"assembly_id": [f"id_{i}" for i in range(row_counts[1])]}),
    }
    assert compute_batch_width(splits, batch_size) == expected_width


def test_write_id_batches_writes_zero_padded_files(tmp_path: Path) -> None:
    """Write numbered batch files with zero-padded suffixes and skip writing when ids is empty."""
    assert write_id_batches([], tmp_path, "empty_prefix", batch_size=2, width=2) == 0
    assert list(tmp_path.iterdir()) == []

    ids = ["GCF_000000001.1", "GCF_000000002.1", "GCF_000000003.1"]
    written = write_id_batches(ids, tmp_path, "after_20240501", batch_size=2, width=2)
    expected_batches = 2
    assert written == expected_batches
    assert (tmp_path / "after_20240501-01.txt").read_text(encoding="utf-8") == "GCF_000000001.1\nGCF_000000002.1\n"
    assert (tmp_path / "after_20240501-02.txt").read_text(encoding="utf-8") == "GCF_000000003.1\n"
