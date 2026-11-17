"""Unit tests for ``cdm_data_loaders.cli.gz``."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from cdm_data_loaders.cli.gz import GzSettings, main
from tests._support.cli_assertions import assert_cli_field_roundtrips, assert_no_cli_clashes


def test_gz_settings_pass_no_cli_clashes() -> None:
    """Verify ``GzSettings`` has no CLI argument or shortcut collisions."""
    assert_no_cli_clashes(GzSettings)


@pytest.mark.parametrize(
    ("field_name", "raw_value", "expected_value"),
    [
        pytest.param("source", "some/file.txt", Path("some/file.txt"), id="source"),
        pytest.param("file_glob", "*.txt", "*.txt", id="file-glob"),
    ],
)
def test_gz_settings_pass_cli_field_roundtrips(
    field_name: str,
    raw_value: str,
    expected_value: Path | str,
) -> None:
    """Verify CLI options and shortcuts on ``GzSettings`` roundtrip to expected values."""
    assert_cli_field_roundtrips(
        GzSettings,
        field_name,
        raw_value,
        expected_value,
        required={"source": "default.txt"},
    )


@pytest.mark.parametrize(
    "source_flag",
    [
        pytest.param("--source", id="long-source"),
        pytest.param("-i", id="short-source"),
    ],
)
def test_cli_single_file(tmp_path: Path, source_flag: str) -> None:
    """Compress a single file passed via ``--source`` or ``-i``."""
    file_path = tmp_path / "single.txt"
    file_path.write_text("single file content", encoding="utf-8")
    main([source_flag, str(file_path)])
    assert (file_path.with_suffix(file_path.suffix + ".gz")).exists()


@pytest.mark.parametrize(
    ("source_flag", "glob_flag"),
    [
        pytest.param("--source", "--file-glob", id="long-flags"),
        pytest.param("-i", "-f", id="short-flags"),
    ],
)
def test_cli_directory_with_glob(tmp_path: Path, source_flag: str, glob_flag: str) -> None:
    """Compress matching files in a directory when ``--file-glob`` / ``-f`` is supplied."""
    sub_dir = tmp_path / "sub"
    sub_dir.mkdir()
    for name in ("a.txt", "b.log"):
        (sub_dir / name).write_text(name, encoding="utf-8")
    main([source_flag, str(sub_dir), glob_flag, "*.txt"])
    assert (sub_dir / "a.txt.gz").exists()
    assert not (sub_dir / "b.log.gz").exists()


def test_cli_main_nonexistent_source(tmp_path: Path) -> None:
    """Raise ``RuntimeError`` when ``--source`` points to a missing path."""
    missing = tmp_path / "doesnotexist"
    with pytest.raises(RuntimeError, match=f"Path {missing} does not exist"):
        main(["--source", str(missing)])


def test_cli_main_missing_source_arg() -> None:
    """Raise ``ValidationError`` when ``--source`` is omitted."""
    with pytest.raises(ValidationError):
        main([])
