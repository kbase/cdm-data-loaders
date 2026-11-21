"""Unit tests for ``cdm_data_loaders.cli.ncbi_ftp_promote`` settings, CLI shortcuts, and ``run_promote``."""

import logging
from pathlib import Path, PurePosixPath
from typing import Any, Final, cast
from unittest.mock import patch

import pytest
from _pytest.mark.structures import ParameterSet
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.cli.ncbi_ftp_promote import PromoteSettings, cli, run_promote
from cdm_data_loaders.datasets.ncbi_ftp.constants import (
    DEFAULT_DESTINATION_BUCKET,
    DEFAULT_DESTINATION_PREFIX,
    DEFAULT_STAGING_BUCKET,
    DEFAULT_TRANSFER_MANIFEST_FILE,
)
from tests.helpers.paths_and_constants import generate_dlt_config

DEFAULT_STAGING_PATH: Final[PurePosixPath] = PurePosixPath("staging") / "run1"

DEFAULT_RESOLVED: Final[dict[str, Any]] = {
    "staging_bucket": DEFAULT_STAGING_BUCKET,
    "destination_bucket": DEFAULT_DESTINATION_BUCKET,
    "destination_path": DEFAULT_DESTINATION_PREFIX,
    "removed_manifest_path": None,
    "updated_manifest_path": None,
    "transfer_manifest_path": DEFAULT_STAGING_PATH / DEFAULT_TRANSFER_MANIFEST_FILE,
    "dry_run": False,
}

ALL_PARAMS_RESOLVED: Final[dict[str, Any]] = {
    "staging_bucket": PurePosixPath("my-staging-bucket"),
    "destination_bucket": PurePosixPath("my-dest-bucket"),
    "staging_path": PurePosixPath("staging") / "run42",
    "destination_path": PurePosixPath("warehouse") / "ncbi",
    "dry_run": True,
}

CLI_SHORTCUT_CASES: Final[tuple[ParameterSet, ...]] = (
    pytest.param(
        (["-s", "staging/runX"], "staging_path", PurePosixPath("staging") / "runX"),
        id="staging-path-shortcut-s",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "--destination-path", "warehouse/custom"],
            "destination_path",
            PurePosixPath("warehouse") / "custom",
        ),
        id="destination-path-flag",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "-r", "removed.txt"],
            "removed_manifest_path",
            Path("removed.txt"),
        ),
        id="removed-manifest-shortcut-r",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "-u", "updated.txt"],
            "updated_manifest_path",
            Path("updated.txt"),
        ),
        id="updated-manifest-shortcut-u",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "-t", "staging/run1/manifest.txt"],
            "transfer_manifest_path",
            PurePosixPath("staging") / "run1" / "manifest.txt",
        ),
        id="transfer-manifest-shortcut-t",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "--staging-bucket", "alt-staging"],
            "staging_bucket",
            PurePosixPath("alt-staging"),
        ),
        id="staging-bucket-flag",
    ),
    pytest.param(
        (
            ["-s", str(DEFAULT_STAGING_PATH), "--destination-bucket", "alt-dest"],
            "destination_bucket",
            PurePosixPath("alt-dest"),
        ),
        id="destination-bucket-flag",
    ),
)

MOCK_REPORT_SUCCESS: Final[dict[str, Any]] = {
    "timestamp": "2026-01-01T00:00:00+00:00",
    "promoted": 5,
    "archived": 0,
    "failed": 0,
    "dry_run": False,
}


def make_settings(**kwargs: str | int | bool | Path | PurePosixPath | None) -> PromoteSettings:
    """Build a validated ``PromoteSettings`` instance with a default ``staging_path``.

    :param kwargs: Field overrides for ``PromoteSettings``.
    :type kwargs: str | int | bool | Path | PurePosixPath | None
    :return: Instantiated ``PromoteSettings``.
    :rtype: PromoteSettings
    """
    kwargs.setdefault("staging_path", DEFAULT_STAGING_PATH)
    return PromoteSettings(**kwargs)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("case", [pytest.param(DEFAULT_RESOLVED, id="defaults")])
def test_promote_settings_resolve_expected_values(case: dict[str, Any]) -> None:
    """Default PromoteSettings fields resolve to the expected values."""
    settings = make_settings()
    for field, expected_value in case.items():
        assert getattr(settings, field) == expected_value


def test_promote_settings_all_params_resolve_expected_values(tmp_path: Path) -> None:
    """PromoteSettings fields resolve all explicit parameters."""
    removed = tmp_path / "removed.txt"
    updated = tmp_path / "updated.txt"
    transfer = PurePosixPath("staging") / "run42" / "transfer_manifest.txt"
    kwargs = {
        **ALL_PARAMS_RESOLVED,
        "removed_manifest_path": removed,
        "updated_manifest_path": updated,
        "transfer_manifest_path": transfer,
    }

    settings = make_settings(**kwargs)

    expected = {
        **ALL_PARAMS_RESOLVED,
        "removed_manifest_path": removed,
        "updated_manifest_path": updated,
        "transfer_manifest_path": transfer,
    }
    for field, expected_value in expected.items():
        assert getattr(settings, field) == expected_value


@pytest.mark.parametrize("case", CLI_SHORTCUT_CASES)
def test_promote_settings_alias_resolves_to_expected_field(
    case: tuple[list[str], str, Path | PurePosixPath],
) -> None:
    """Each CLI shortcut or flag resolves to the expected PromoteSettings field."""
    cli_args, field, expected_value = case
    settings = CliApp.run(PromoteSettings, cli_args=cli_args)
    assert getattr(settings, field) == expected_value


def test_promote_settings_staging_path_required() -> None:
    """Omitting staging_path raises ValidationError."""
    settings_ctor = cast("Any", PromoteSettings)
    with pytest.raises((ValidationError, Exception)):
        settings_ctor(_cli_parse_args=[], dlt_config=generate_dlt_config())


def test_promote_settings_transfer_manifest_path_can_be_none() -> None:
    """transfer_manifest_path can be set to None."""
    settings = make_settings(transfer_manifest_path=None)
    assert settings.transfer_manifest_path is None


def test_run_promote_calls_promote_from_s3_with_correct_args(tmp_path: Path) -> None:
    """run_promote passes all PromoteSettings fields to promote_from_s3."""
    staging_path = PurePosixPath("staging") / "run1"
    dest_path = PurePosixPath("warehouse") / "ncbi"
    removed = tmp_path / "removed.txt"
    updated = tmp_path / "updated.txt"
    transfer = PurePosixPath("staging") / "run1" / "transfer_manifest.txt"
    config = make_settings(
        staging_bucket=PurePosixPath("my-staging"),
        destination_bucket=PurePosixPath("my-dest"),
        staging_path=staging_path,
        destination_path=dest_path,
        removed_manifest_path=removed,
        updated_manifest_path=updated,
        transfer_manifest_path=transfer,
        dry_run=True,
    )

    with patch(
        "cdm_data_loaders.cli.ncbi_ftp_promote.promote_from_s3",
        return_value=MOCK_REPORT_SUCCESS,
    ) as mock_promote:
        run_promote(config)

    mock_promote.assert_called_once_with(
        staging_bucket=PurePosixPath("my-staging"),
        staging_key_prefix=staging_path,
        lakehouse_bucket=PurePosixPath("my-dest"),
        lakehouse_key_prefix=dest_path,
        removed_manifest_path=removed,
        updated_manifest_path=updated,
        manifest_s3_key=transfer,
        dry_run=True,
    )


@pytest.mark.parametrize(
    ("failed_count", "expected_messages"),
    [
        pytest.param(0, [], id="zero-failures"),
        pytest.param(1, ["Promote completed with 1 failures"], id="single-failure"),
        pytest.param(2, ["Promote completed with 2 failures"], id="multiple-failures"),
    ],
)
def test_run_promote_logs_error_only_when_failures_positive(
    caplog: pytest.LogCaptureFixture,
    failed_count: int,
    expected_messages: list[str],
) -> None:
    """run_promote logs an error without raising if and only if promote_from_s3 reports failures > 0."""
    config = make_settings()
    report = {**MOCK_REPORT_SUCCESS, "failed": failed_count}
    with (
        patch(
            "cdm_data_loaders.cli.ncbi_ftp_promote.promote_from_s3",
            return_value=report,
        ),
        caplog.at_level(logging.ERROR, logger="cdm_data_loaders.cli.ncbi_ftp_promote"),
    ):
        run_promote(config)
    assert caplog.messages == expected_messages


def test_run_promote_dry_run_forwarded() -> None:
    """dry_run=True is forwarded to promote_from_s3."""
    config = make_settings(dry_run=True)
    with patch(
        "cdm_data_loaders.cli.ncbi_ftp_promote.promote_from_s3",
        return_value=MOCK_REPORT_SUCCESS,
    ) as mock_promote:
        run_promote(config)
    assert mock_promote.call_args.kwargs["dry_run"] is True


def test_cli_invokes_cli_app_and_run_promote(monkeypatch: pytest.MonkeyPatch) -> None:
    """CliRunner parses PromoteSettings via CliApp.run and invokes promote_from_s3."""
    fake_settings = make_settings(dry_run=True)

    def _fake_cli_app_run(_cls: object, **_kwargs: object) -> PromoteSettings:
        return fake_settings

    monkeypatch.setattr(CliApp, "run", _fake_cli_app_run)
    with patch(
        "cdm_data_loaders.cli.ncbi_ftp_promote.promote_from_s3",
        return_value=MOCK_REPORT_SUCCESS,
    ) as mock_promote:
        cli()
    mock_promote.assert_called_once()
    assert mock_promote.call_args.kwargs["dry_run"] is True
