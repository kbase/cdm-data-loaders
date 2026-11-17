"""Tests for cli.ncbi_ftp_promote — settings and CLI shortcuts."""

from pathlib import Path, PurePosixPath
from typing import Any, cast

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.cli.ncbi_ftp_promote import (
    PromoteSettings,
)
from cdm_data_loaders.datasets.ncbi_ftp.constants import (
    DEFAULT_DESTINATION_BUCKET,
    DEFAULT_DESTINATION_PREFIX,
    DEFAULT_STAGING_BUCKET,
    DEFAULT_TRANSFER_MANIFEST_FILE,
)
from tests.conftest import _generate_dlt_config

DEFAULT_STAGING_PATH: PurePosixPath = PurePosixPath("staging") / "run1"


def make_settings(**kwargs: str | int | bool | Path | PurePosixPath | None) -> PromoteSettings:
    """Generate a validated PromoteSettings object with a required staging_path default."""
    kwargs.setdefault("staging_path", DEFAULT_STAGING_PATH)
    return PromoteSettings(**kwargs)  # pyright: ignore[reportArgumentType]


# Settings defaults / all params — (id, kwargs, expected resolved values)
DEFAULT_RESOLVED = {
    "staging_bucket": DEFAULT_STAGING_BUCKET,
    "destination_bucket": DEFAULT_DESTINATION_BUCKET,
    "destination_path": DEFAULT_DESTINATION_PREFIX,
    "removed_manifest_path": None,
    "updated_manifest_path": None,
    "transfer_manifest_path": DEFAULT_STAGING_PATH / DEFAULT_TRANSFER_MANIFEST_FILE,
    "dry_run": False,
}

ALL_PARAMS_RESOLVED: dict[str, Any] = {
    "staging_bucket": PurePosixPath("my-staging-bucket"),
    "destination_bucket": PurePosixPath("my-dest-bucket"),
    "staging_path": PurePosixPath("staging") / "run42",
    "destination_path": PurePosixPath("warehouse") / "ncbi",
    "dry_run": True,
}


@pytest.mark.parametrize("case", [pytest.param(DEFAULT_RESOLVED, id="defaults")])
def test_promote_settings_resolve_expected_values(case: dict) -> None:
    """Default PromoteSettings fields resolve to the expected values."""
    s = make_settings()
    for field, expected_value in case.items():
        assert getattr(s, field) == expected_value


def test_promote_settings_all_params_resolve_expected_values(tmp_path: Path) -> None:
    """PromoteSettings fields are correctly set when all params are provided."""
    removed = tmp_path / "removed.txt"
    updated = tmp_path / "updated.txt"
    transfer = PurePosixPath("staging") / "run42" / "transfer_manifest.txt"
    kwargs = {
        **ALL_PARAMS_RESOLVED,
        "removed_manifest_path": removed,
        "updated_manifest_path": updated,
        "transfer_manifest_path": transfer,
    }

    s = make_settings(**kwargs)

    expected = {
        **ALL_PARAMS_RESOLVED,
        "removed_manifest_path": removed,
        "updated_manifest_path": updated,
        "transfer_manifest_path": transfer,
    }
    for field, expected_value in expected.items():
        assert getattr(s, field) == expected_value


CLI_SHORTCUT_CASES = [
    pytest.param(
        (["-s", "staging/runX"], "staging_path", PurePosixPath("staging") / "runX"),
        id="staging_path_shortcut_s",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "--destination-path", "warehouse/custom"],
            "destination_path",
            PurePosixPath("warehouse") / "custom",
        ),
        id="destination_path_flag",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "-r", "removed.txt"],
            "removed_manifest_path",
            Path("removed.txt"),
        ),
        id="removed_manifest_shortcut_r",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "-u", "updated.txt"],
            "updated_manifest_path",
            Path("updated.txt"),
        ),
        id="updated_manifest_shortcut_u",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "-t", "staging/run1/manifest.txt"],
            "transfer_manifest_path",
            PurePosixPath("staging") / "run1" / "manifest.txt",
        ),
        id="transfer_manifest_shortcut_t",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "--staging-bucket", "alt-staging"],
            "staging_bucket",
            PurePosixPath("alt-staging"),
        ),
        id="staging_bucket_flag",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "--destination-bucket", "alt-dest"],
            "destination_bucket",
            PurePosixPath("alt-dest"),
        ),
        id="destination_bucket_flag",
    ),
]


@pytest.mark.parametrize("case", CLI_SHORTCUT_CASES)
def test_promote_settings_alias_resolves_to_expected_field(case: tuple) -> None:
    """Each CLI shortcut or flag resolves to the expected PromoteSettings field."""
    cli_args, field, expected_value = case
    s = CliApp.run(PromoteSettings, cli_args=cli_args)
    assert getattr(s, field) == expected_value


# Settings validation
def test_promote_settings_staging_path_required() -> None:
    """Omitting staging_path raises ValidationError."""
    settings_ctor = cast("Any", PromoteSettings)
    with pytest.raises((ValidationError, Exception)):
        settings_ctor(_cli_parse_args=[], dlt_config=_generate_dlt_config())


def test_promote_settings_transfer_manifest_path_can_be_none() -> None:
    """transfer_manifest_path can be explicitly set to None."""
    s = make_settings(transfer_manifest_path=None)
    assert s.transfer_manifest_path is None
