"""Tests of file system-related utilities."""

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from cdm_data_loaders.utils.file_system import FILE_NAME_REGEX, BatchCursor

# the maximum file number in the directory
MAX_FILE_NUMBER = 15
# what the range should be set to (max range is exclusive)
MAX_RANGE_VALUE = 16

EXPECTED: dict[int | None, dict[int | None, Any]] = {
    # batch_size
    1: {
        # start_at
        1: [[r] for r in range(1, MAX_RANGE_VALUE)],
        6: [[r] for r in range(6, MAX_RANGE_VALUE)],
        8: [[r] for r in range(8, MAX_RANGE_VALUE)],
        11: [[r] for r in range(11, MAX_RANGE_VALUE)],
    },
    5: {
        # start_at
        1: [range(1, 6), range(6, 11), range(11, MAX_RANGE_VALUE)],
        6: [range(6, 11), range(11, MAX_RANGE_VALUE)],
        8: [range(8, 13), range(13, MAX_RANGE_VALUE)],
        11: [range(11, MAX_RANGE_VALUE)],
    },
    8: {
        1: [range(1, 9), range(9, MAX_RANGE_VALUE)],
        6: [range(6, 14), range(14, MAX_RANGE_VALUE)],
        8: [range(8, MAX_RANGE_VALUE)],
        11: [range(11, MAX_RANGE_VALUE)],
    },
    15: {
        1: [range(1, MAX_RANGE_VALUE)],
        6: [range(6, MAX_RANGE_VALUE)],
        8: [range(8, MAX_RANGE_VALUE)],
        11: [range(11, MAX_RANGE_VALUE)],
    },
}
# batch_size is not specified
EXPECTED[None] = EXPECTED[1]
# batch_size greater than # of records
EXPECTED[20] = EXPECTED[15]
# add in results for start_at == 0 or not specified
for ix, vals in EXPECTED.items():
    if ix is not None:
        EXPECTED[ix][None] = vals[1]
        EXPECTED[ix][0] = vals[1]

EXPECTED_END_AT = deepcopy(EXPECTED)


def make_files(directory: Path, names: list[str]) -> list[Path]:
    """Touch each filename in *directory* and return the sorted Path list."""
    paths = []
    for name in names:
        p = directory / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
        paths.append(p)
    return sorted(paths)


def make_file_names(prefix: str, ext: str, numbers: list[int] | range) -> list[str]:
    """Create the file names for a given set of numbers."""
    if not isinstance(numbers, list):
        # convert the range into a list
        numbers = list(numbers)
    return [f"{prefix}_{n:05}.{ext}" for n in numbers]


def make_sequence(directory: Path, prefix: str, ext: str, numbers: list[int] | range) -> list[Path]:
    """Create files for the given sequence numbers and return sorted Paths."""
    names = make_file_names(prefix, ext, numbers)
    return make_files(directory, names)


@pytest.fixture
def file_dir(tmp_path: Path) -> Path:
    """Directory pre-populated with files numbered 00001 - 00015."""
    make_sequence(tmp_path, "report", "csv", range(1, MAX_RANGE_VALUE))
    return tmp_path


def test_batcher_defaults() -> None:
    """Ensure defaults are set correctly."""
    bc = BatchCursor(".")
    assert bc.start_at == 1
    assert bc.batch_size == 1
    assert bc.end_at is None
    assert bc.file_regex == FILE_NAME_REGEX


def test_batcher_default_end_at() -> None:
    """Ensure default end_at is set correctly."""
    bc = BatchCursor(".", end_at=0)
    assert bc.end_at is None


@pytest.mark.parametrize("batch_size", [None, 0, -1, 1.0, 1.2345678, -15, -1234567890, "something", "50", "None"])
def test_invalid_batch_size(batch_size: float | str | None) -> None:
    """Test invalid batch_size, start_at, end_at, and file_regex parameters."""
    with pytest.raises(ValueError, match="batch_size must be an integer, 1 or greater"):
        BatchCursor(".", batch_size)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("start_at", [None, -1, 1.0, 1.2345678, -15, -1234567890, "something", "50", "None"])
def test_invalid_start_at_params(start_at: float | None) -> None:
    """Test invalid start_at parameters."""
    with pytest.raises(ValueError, match="start_at must be an integer, 0 or greater"):
        BatchCursor(".", start_at=start_at)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("end_at", [None, -1, 1.0, 1.2345678, -15, -1234567890, "something", "50", "None"])
def test_invalid_end_at_params(end_at: float | None) -> None:
    """Test invalid end_at parameters."""
    with pytest.raises(ValueError, match="end_at must be an integer, 1 or greater"):
        BatchCursor(".", end_at=end_at)  # pyright: ignore[reportArgumentType]


def test_invalid_start_vs_end_at_params() -> None:
    """Ensure that an error is thrown if start_at and end_at are not compatible."""
    with pytest.raises(ValueError, match="end_at must be greater than start_at"):
        BatchCursor(".", start_at=2, end_at=1)


def test_ok_start_end_at_params() -> None:
    """Ensure that 0 is a valid end_at parameter, regardless of start_at value."""
    bc = BatchCursor(".", start_at=5, end_at=0)
    assert bc.end_at is None
    assert bc.start_at == 5  # noqa: PLR2004


def test_end_at_greater_than_start_at_during_iteration() -> None:
    """Ensure that if end_at is smaller than start_at during iteration, an empty list is returned."""
    bc = BatchCursor(".", start_at=0, end_at=5)
    assert bc.end_at == 5  # noqa: PLR2004
    bc.start_at = 10
    assert bc.get_batch() == []


CUTOFF_VALUE = 12


# your basic batch
@pytest.mark.parametrize("end_at", [None, 0, CUTOFF_VALUE])
@pytest.mark.parametrize("start_at", [None, 0, 1, 6, 8, 11])
@pytest.mark.parametrize("batch_size", EXPECTED.keys())
def test_get_batch_parametrized(
    file_dir: Path,
    batch_size: int | None,
    start_at: int | None,
    end_at: int | None,
) -> None:
    """Test retrieval of batches of files."""
    cursor_params = {}
    if batch_size is not None:
        cursor_params["batch_size"] = batch_size
    if start_at is not None:
        cursor_params["start_at"] = start_at
    if end_at is not None:
        cursor_params["end_at"] = end_at

    cursor = BatchCursor(file_dir, **cursor_params)

    # generate the expected files
    expected_files: list[list[Path]] = [
        [file_dir / fn for fn in make_file_names("report", "csv", numbers)]
        for numbers in EXPECTED[batch_size][start_at]
    ]
    if end_at:
        expected_files = []
        for numbers in EXPECTED[batch_size][start_at]:
            if cutoffless := [n for n in numbers if n <= end_at]:
                expected_files.append([file_dir / fn for fn in make_file_names("report", "csv", cutoffless)])

    output: list[list[Path]] = []
    while batch := cursor.get_batch():
        output.append(batch)
        if cursor.start_at >= MAX_RANGE_VALUE:
            break

    # check the number of batches is correct
    assert len(output) == len(expected_files)

    # results are sorted
    for batch in output:
        # results are all file paths
        assert all(isinstance(p, Path) for p in batch)
        assert sorted(batch) == batch
    assert output == expected_files

    # if end_at is defined, the start_at value will be one greater than the end_at value
    if end_at:
        assert cursor.start_at == CUTOFF_VALUE + 1


def test_get_batch_default_start_at_is_zero(file_dir: Path) -> None:
    """Ensure that the default start_at is 0."""
    cursor_default = BatchCursor(file_dir, batch_size=3)
    cursor_explicit = BatchCursor(file_dir, batch_size=3, start_at=0)
    assert cursor_default.get_batch() == cursor_explicit.get_batch()


def test_get_batch_start_at_matches_sequence_number(file_dir: Path) -> None:
    """Ensure start_at value matches sequence number."""
    cursor = BatchCursor(file_dir, batch_size=5, start_at=15)
    result = cursor.get_batch()
    assert len(result) == 1
    assert result[0].name == "report_00015.csv"


# advancing the cursor
def test_get_batch_start_at_advances_after_get_batch(file_dir: Path) -> None:
    """Ensure that the start_at value changes after each successful get_batch operation."""
    batch_size = 5
    cursor = BatchCursor(file_dir, batch_size=batch_size, start_at=0)
    assert cursor.start_at == 0
    batch_1 = cursor.get_batch()
    assert cursor.start_at == batch_size + 1  # next file is report_00006.csv
    batch_2 = cursor.get_batch()
    assert cursor.start_at == batch_size * 2 + 1  # report_00011.csv
    batch_3 = cursor.get_batch()
    assert cursor.start_at == batch_size * 3 + 1  # report_00016.csv (does not exist)
    # next call returns nothing
    assert cursor.get_batch() == []

    # all files should be the sequential list of existing files
    all_files = batch_1 + batch_2 + batch_3
    assert all_files == [file_dir / f"report_{n:05}.csv" for n in range(1, MAX_RANGE_VALUE)]


def test_get_batch_cursor_does_not_advance_on_empty_result(file_dir: Path) -> None:
    """Ensure that the cursor does not advance if the batch is empty."""
    start_at = 999
    cursor = BatchCursor(file_dir, batch_size=5, start_at=start_at)
    cursor.get_batch()
    assert cursor.start_at == start_at


def test_get_batch_partial_batch_advances_correctly(file_dir: Path) -> None:
    """Ensure that the cursor only advances as far as the last file in the batch."""
    # Only 3 files remain from 13 onward
    cursor = BatchCursor(file_dir, batch_size=5, start_at=13)
    result = cursor.get_batch()
    assert result == [file_dir / f"report_{n:05}.csv" for n in [13, 14, 15]]
    assert cursor.start_at == 16  # noqa: PLR2004


def test_get_batch_cursor_can_be_reset(file_dir: Path) -> None:
    """Ensure that the cursor can be reset."""
    batch_size = 5
    cursor = BatchCursor(file_dir, batch_size=batch_size)
    original_result = cursor.get_batch()
    assert cursor.start_at == batch_size + 1
    # set cursor to 0
    cursor.start_at = 0
    reset_result = cursor.get_batch()
    assert cursor.start_at == batch_size + 1
    assert original_result == reset_result
    assert reset_result[0].name == "report_00001.csv"


# Edge cases -- boundaries
def test_get_batch_start_at_beyond_end_returns_empty_list(file_dir: Path) -> None:
    """Ensure that nothing is returned if start_at is too high."""
    cursor = BatchCursor(file_dir, batch_size=5, start_at=999)
    assert cursor.get_batch() == []


def test_get_batch_empty_directory_returns_empty_list(tmp_path: Path) -> None:
    """Ensure that an empty dir returns nothing."""
    cursor = BatchCursor(tmp_path, batch_size=5)
    assert cursor.get_batch() == []


def test_get_batch_batch_size_larger_than_remaining_files(file_dir: Path) -> None:
    """Ensure that batches are sized correctly for partial batches."""
    cursor = BatchCursor(file_dir, batch_size=10, start_at=10)
    result = cursor.get_batch()
    # should have 00010 - 00015
    assert result[-1].name == "report_00015.csv"
    assert result == [file_dir / f"report_{n:05}.csv" for n in range(10, MAX_RANGE_VALUE)]
    assert cursor.start_at == 16  # noqa: PLR2004


# gaps in the sequence
def test_get_batch_start_at_skips_to_next_available_when_gap(tmp_path: Path) -> None:
    """Ensure that gaps in the sequence are dealt with correctly."""
    # Files exist for 1,2,3 then jump to 10,11,12 — no 4-9
    make_sequence(tmp_path, "data", "csv.gz", [1, 2, 3, 10, 11, 12])
    cursor = BatchCursor(tmp_path, batch_size=5, start_at=5)
    # retrieve 5 files, starting at 00005
    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [10, 11, 12]]
    assert cursor.start_at == 13  # noqa: PLR2004
    assert cursor.get_batch() == []


def test__get_batchsequential_calls_across_gap(tmp_path: Path) -> None:
    """Ensure that files are correctly retrieved across gaps in the sequence."""
    make_sequence(tmp_path, "data", "csv.gz", [1, 2, 3, 10, 11, 12])
    cursor = BatchCursor(tmp_path, batch_size=2)
    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [1, 2]]
    assert cursor.start_at == 3  # noqa: PLR2004

    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [3, 10]]
    assert cursor.start_at == 11  # noqa: PLR2004

    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [11, 12]]
    assert cursor.start_at == 13  # noqa: PLR2004

    assert cursor.get_batch() == []
    assert cursor.start_at == 13  # noqa: PLR2004


def test_get_batch_sequential_calls_across_gap_with_end_at(tmp_path: Path) -> None:
    """Ensure that files are correctly retrieved across gaps in the sequence when end_at is specified."""
    make_sequence(tmp_path, "data", "csv.gz", [1, 2, 3, 5, 8, 11, 15])
    cursor = BatchCursor(tmp_path, batch_size=2, end_at=10)
    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [1, 2]]
    assert cursor.start_at == 3  # noqa: PLR2004

    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [3, 5]]
    assert cursor.start_at == 6  # noqa: PLR2004

    assert cursor.get_batch() == [tmp_path / f"data_{n:05}.csv.gz" for n in [8]]
    assert cursor.start_at == 9  # noqa: PLR2004

    assert cursor.get_batch() == []
    assert cursor.start_at == 9  # noqa: PLR2004


@pytest.fixture
def mixed_dir(tmp_path: Path) -> Path:
    """Directory containing valid files alongside files that should be ignored."""
    make_sequence(tmp_path, "data", "txt", range(1, 6))
    more_files = [
        "data_123.txt",
        "data_000001.txt",
        "data_000100.txt.gz",
        "data_000200.txt.tar.gz",
        "data_000400.csv.gz",
        "file_000300.txt.tar.gz",
        # no numbers
        "README.md",
        # contains non-\w character
        ".hidden_00001.txt",
        # no extension
        "data_00001",
        # files in nested dirs -- will not be found
        "nested/data_00010.txt",
        "nested/dir1/data_00020.txt",
    ]
    make_files(tmp_path, more_files)
    return tmp_path


# File-name pattern filtering
def test_get_batch_ignores_invalid_filenames(mixed_dir: Path) -> None:
    """Ensure that filenames are matched correctly."""
    cursor = BatchCursor(mixed_dir, batch_size=20)
    generated_file_names = [f"data_{n:05}.txt" for n in range(1, 6)]
    file_names = sorted(
        [
            *generated_file_names,
            "data_123.txt",
            "data_000001.txt",
            "data_000100.txt.gz",
            "data_000200.txt.tar.gz",
            "data_000400.csv.gz",
            "file_000300.txt.tar.gz",
        ]
    )
    assert cursor.get_batch() == [mixed_dir / fn for fn in file_names]


def test_get_batch_mixed_extensions_sorted_correctly(tmp_path: Path) -> None:
    """Ensure that files with a mix of extensions are sorted numerially."""
    names = ["data_00001.csv", "data_00001.tar.gz", "data_00002.tar.gz", "data_00003.txt"]
    make_files(tmp_path, names)
    cursor = BatchCursor(tmp_path, batch_size=10)
    assert [p.name for p in cursor.get_batch()] == names


# Dynamic / live-directory behaviour
def test_get_batch_picks_up_newly_added_files(tmp_path: Path) -> None:
    """Ensure that adding files to a dir during batching picks up new files correctly."""
    # dir contains log_00001.txt -> log_00003.txt
    make_sequence(tmp_path, "log", "txt", range(1, 4))
    cursor = BatchCursor(tmp_path, batch_size=10)
    assert cursor.get_batch() == [tmp_path / f"log_{n:05}.txt" for n in [1, 2, 3]]

    (tmp_path / "log_00004.txt").touch()
    # Reset cursor to re-scan from the start
    cursor.start_at = 0
    assert cursor.get_batch() == [tmp_path / f"log_{n:05}.txt" for n in [1, 2, 3, 4]]


def test_get_batch_new_files_within_current_window_are_included(tmp_path: Path) -> None:
    """Ensure that all new files within the current batching params are included, regardless of sequence position."""
    # dir contains log_00001.txt -> log_00005.txt
    make_sequence(tmp_path, "log", "txt", range(1, 6))
    cursor = BatchCursor(tmp_path, batch_size=3, end_at=13)

    assert cursor.get_batch() == [tmp_path / f"log_{n:05}.txt" for n in [1, 2, 3]]
    assert cursor.start_at == 4  # noqa: PLR2004

    # New files added within the next window before the next call
    (tmp_path / "log_00006.txt").touch()
    (tmp_path / "log_00007.txt").touch()
    assert cursor.get_batch() == [tmp_path / f"log_{n:05}.txt" for n in [4, 5, 6]]
    assert cursor.start_at == 7  # noqa: PLR2004

    # New files added within the next window before the next call
    # Note: we are missing 00008 and 00009
    (tmp_path / "log_00010.txt").touch()
    (tmp_path / "log_00011.txt").touch()
    assert cursor.get_batch() == [tmp_path / f"log_{n:05}.txt" for n in [7, 10, 11]]
    assert cursor.start_at == 12  # noqa: PLR2004

    # add in missing files -- nothing is returned as start_at is at 12
    (tmp_path / "log_00008.txt").touch()
    (tmp_path / "log_00009.txt").touch()
    assert cursor.get_batch() == []
    assert cursor.start_at == 12  # noqa: PLR2004

    # add more files
    (tmp_path / "log_00012.txt").touch()
    (tmp_path / "log_00013.txt").touch()
    assert cursor.get_batch() == [tmp_path / f"log_{n:05}.txt" for n in [12, 13]]
    assert cursor.start_at == 14  # noqa: PLR2004

    # add more files beyond the end_at value
    (tmp_path / "log_00014.txt").touch()
    (tmp_path / "log_00015.txt").touch()
    assert cursor.get_batch() == []
    assert cursor.start_at == 14  # noqa: PLR2004
