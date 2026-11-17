"""Tests for cli.ncbi_ftp_sync — settings and CLI shortcuts."""

from pathlib import Path

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.cli.ncbi_ftp_sync import (
    DownloadSettings,
)
from cdm_data_loaders.core.fields import INPUT_MOUNT, OUTPUT_MOUNT
from cdm_data_loaders.datasets.ncbi_ftp.constants import FTP_HOST

DEFAULT_THREADS = 4
CUSTOM_THREADS = 8
ALIAS_THREADS = 16
BOUNDARY_MIN = 1
BOUNDARY_MAX = 32
OVER_MAX = 64
CUSTOM_LIMIT = 100
ALIAS_LIMIT = 50

# Settings defaults / all params / aliases

DEFAULT_SETTINGS = {
    "manifest": Path(INPUT_MOUNT) / "transfer_manifest.txt",
    "output_dir": Path(OUTPUT_MOUNT),
    "threads": DEFAULT_THREADS,
    "ftp_host": FTP_HOST,
    "limit": None,
}

ALL_PARAMS_SETTINGS = {
    "manifest": Path("/") / "data" / "my_manifest.txt",
    "output_dir": Path("/") / "data" / "output",
    "threads": CUSTOM_THREADS,
    "ftp_host": "ftp.example.com",
    "limit": CUSTOM_LIMIT,
}

ALIAS_SETTINGS = {
    "manifest": Path("/") / "data" / "m.txt",
    "output_dir": Path("/") / "data" / "o",
    "threads": ALIAS_THREADS,
    "limit": ALIAS_LIMIT,
}

# (kwargs, expected resolved values) — human-readable id per case
SETTINGS_CASES = [
    pytest.param(({}, DEFAULT_SETTINGS), id="defaults"),
    pytest.param((dict(ALL_PARAMS_SETTINGS), ALL_PARAMS_SETTINGS), id="all_params"),
]

invalid_settings = [
    pytest.param({"threads": 0}, id="threads_too_low"),
    pytest.param({"threads": OVER_MAX}, id="threads_too_high"),
    pytest.param({"limit": 0}, id="limit_must_be_positive"),
]


@pytest.mark.parametrize("case", SETTINGS_CASES)
def test_settings_resolve_expected_values(case: tuple) -> None:
    """Defaults and explicit params resolve to the expected settings values."""
    kwargs, expected = case
    s = DownloadSettings(**kwargs)
    for field, expected_value in expected.items():
        assert getattr(s, field) == expected_value


def test_settings_cli_shortcuts_resolve_expected_values() -> None:
    """CLI short flags (-m, -t, -l) resolve to the expected DownloadSettings fields."""
    s = CliApp.run(
        DownloadSettings,
        cli_args=[
            "-m",
            str(ALIAS_SETTINGS["manifest"]),
            "--output-dir",
            str(ALIAS_SETTINGS["output_dir"]),
            "-t",
            str(ALIAS_SETTINGS["threads"]),
            "-l",
            str(ALIAS_SETTINGS["limit"]),
        ],
    )
    for field, expected_value in ALIAS_SETTINGS.items():
        assert getattr(s, field) == expected_value


@pytest.mark.parametrize("kwargs", invalid_settings)
def test_settings_invalid_constraints_raise(kwargs: dict) -> None:
    """Out-of-range threads/limit values raise ValidationError."""
    with pytest.raises(ValidationError):
        DownloadSettings(**kwargs)


@pytest.mark.parametrize(
    "threads",
    [
        pytest.param(BOUNDARY_MIN, id="threads_boundary_1"),
        pytest.param(BOUNDARY_MAX, id="threads_boundary_32"),
    ],
)
def test_settings_threads_boundaries_accepted(threads: int) -> None:
    """Boundary thread counts (1 and 32) are accepted."""
    s = DownloadSettings(threads=threads)  # pyright: ignore[reportCallIssue]
    assert s.threads == threads
