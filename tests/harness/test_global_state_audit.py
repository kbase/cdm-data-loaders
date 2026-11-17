"""Opt-in global state mutation audit across the repository test suite."""

from typing import Final

import pytest

from tests.helpers.state_audit import run_pytest_subprocess

pytestmark = pytest.mark.state_audit

AUDIT_SUBPROCESS_MARKEXPR: Final[str] = (
    "not requires_spark and not requires_ceph and not external_request and not docker and not docs and not state_audit"
)


@pytest.mark.parametrize(
    "suite_args",
    [
        pytest.param(
            (
                "--ignore=tests/e2e",
                "--ignore=tests/cdm_data_loaders/pipelines",
                "--ignore=tests/docs",
                "--ignore=tests/harness/test_global_state_audit.py",
            ),
            id="core-and-converters",
        ),
        pytest.param(
            ("tests/cdm_data_loaders/pipelines/",),
            id="pipelines",
        ),
        pytest.param(
            ("tests/e2e/",),
            id="e2e",
        ),
    ],
)
def test_state_audit_suite_slice_has_no_unreverted_mutations(suite_args: tuple[str, ...]) -> None:
    """Run a clean pytest subprocess with ``--state-audit`` and verify zero unreverted global state mutations.

    :param suite_args: Positional or ignore arguments selecting a non-overlapping slice of ``tests/``.
    :type suite_args: tuple[str, ...]
    """
    proc = run_pytest_subprocess(
        [
            "--state-audit",
            "-qq",
            "--tb=short",
            "-p",
            "no:randomly",
            "-p",
            "no:cov",
            "-m",
            AUDIT_SUBPROCESS_MARKEXPR,
            *suite_args,
        ]
    )
    assert proc.returncode == 0, (
        f"Global state audit failed with exit code {proc.returncode} for args {suite_args}.\n"
        f"--- STDOUT ---\n{proc.stdout}\n"
        f"--- STDERR ---\n{proc.stderr}"
    )
    assert "[state-audit] Passed: 0 unreverted global state mutations" in proc.stdout
