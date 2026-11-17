"""Pytest fixtures for XSV reader and ``qsv`` validator tests."""

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Final
from unittest.mock import MagicMock

import pytest

from cdm_data_loaders.readers.xsv import qsv
from cdm_data_loaders.readers.xsv.models import CleanerValidatorArgs
from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema
from tests._support.fixtures_xsv import WriteFile, make_xsv_writer, resolve_qsv_cmd
from tests.cdm_data_loaders.readers.xsv._helpers import (
    FAKE_QSV_CMD,
    SCHEMAS_DIR,
    make_args_factory,
)

MIN_STEP_CMD_LEN: Final[int] = 3


@pytest.fixture(scope="session")
def qsv_cmd() -> str:
    """Resolve the real ``qsv`` binary path for integration-style validator steps.

    :return: Path to ``qsv``.
    :rtype: str
    """
    return resolve_qsv_cmd()


@pytest.fixture(scope="session")
def first_pass_schema_path() -> Path:
    """Return the path to the first-pass JSON Schema fixture.

    :return: Fixture JSON Schema path.
    :rtype: Path
    """
    return SCHEMAS_DIR / "first_pass_schema.json"


@pytest.fixture(scope="session")
def first_pass_validated_schema(first_pass_schema_path: Path) -> ValidatedXsvSchema:
    """Return a ``ValidatedXsvSchema`` for the first-pass schema fixture.

    :param first_pass_schema_path: First-pass schema file path.
    :type first_pass_schema_path: Path
    :return: Validated XSV schema.
    :rtype: ValidatedXsvSchema
    """
    return ValidatedXsvSchema(jsonschema=json.loads(first_pass_schema_path.read_bytes()))


@pytest.fixture(scope="session")
def post_norm_schema_path() -> Path:
    """Return the path to the post-normalisation JSON Schema fixture.

    :return: Fixture JSON Schema path.
    :rtype: Path
    """
    return SCHEMAS_DIR / "post_norm_schema.json"


@pytest.fixture(scope="session")
def post_norm_validated_schema(post_norm_schema_path: Path) -> ValidatedXsvSchema:
    """Return a ``ValidatedXsvSchema`` for the post-normalisation schema fixture.

    :param post_norm_schema_path: Post-normalisation schema file path.
    :type post_norm_schema_path: Path
    :return: Validated XSV schema.
    :rtype: ValidatedXsvSchema
    """
    return ValidatedXsvSchema(jsonschema=json.loads(post_norm_schema_path.read_bytes()))


@pytest.fixture(scope="session")
def derived_first_pass_schema_path() -> Path:
    """Return the path to the manually derived first-pass schema fixture.

    :return: Fixture JSON Schema path.
    :rtype: Path
    """
    return SCHEMAS_DIR / "derived_first_pass_schema.json"


@pytest.fixture(scope="session")
def derived_first_pass_schema(derived_first_pass_schema_path: Path) -> dict[str, Any]:
    """Return the parsed JSON dictionary at ``derived_first_pass_schema_path``.

    :param derived_first_pass_schema_path: Derived schema file path.
    :type derived_first_pass_schema_path: Path
    :return: Parsed JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    return json.loads(derived_first_pass_schema_path.read_bytes())


@pytest.fixture
def make_schema_file(tmp_path: Path) -> Callable[..., Path]:
    """Return a factory writing a JSON-serializable schema object to a temporary file.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :return: Factory writing schema JSON files.
    :rtype: Callable[..., Path]
    """

    def _make(schema: list[Any] | dict[str, Any], file_name: str = "schema.json") -> Path:
        path = tmp_path / file_name
        path.write_text(json.dumps(schema), encoding="utf-8")
        return path

    return _make


@pytest.fixture
def tmp_dir_path(tmp_path: Path) -> Path:
    """Create and return a temporary working directory for ``CleanerValidatorArgs.tmp_dir_path``.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :return: Working directory path.
    :rtype: Path
    """
    work_dir = tmp_path / "tmp"
    work_dir.mkdir()
    return work_dir


@pytest.fixture
def output_dir_path(tmp_path: Path) -> Path:
    """Create and return the base temporary output directory.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :return: Output directory path.
    :rtype: Path
    """
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    return out_dir


@pytest.fixture
def output_dir_path_derivatives(output_dir_path: Path) -> dict[str, Path]:
    """Create ``qsv_output`` and ``validated`` subdirectories under ``output_dir_path``.

    :param output_dir_path: Base output directory.
    :type output_dir_path: Path
    :return: Mapping of derivative output directory paths.
    :rtype: dict[str, Path]
    """
    (output_dir_path / "qsv_output").mkdir(exist_ok=True, parents=True)
    (output_dir_path / "validated").mkdir(exist_ok=True, parents=True)
    return {
        "qsv_output_dir_path": output_dir_path / "qsv_output",
        "validated_file_dir_path": output_dir_path / "validated",
    }


@pytest.fixture
def source_dir_path(tmp_path: Path) -> Path:
    """Create and return a source directory for incoming XSV files.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :return: Source directory path.
    :rtype: Path
    """
    src_dir = tmp_path / "source"
    src_dir.mkdir()
    return src_dir


@pytest.fixture
def write_source_file(source_dir_path: Path) -> WriteFile:
    """Return a file writer bound to ``source_dir_path``.

    :param source_dir_path: Incoming source directory.
    :type source_dir_path: Path
    :return: XSV file writer callable.
    :rtype: WriteFile
    """
    return make_xsv_writer(source_dir_path)


@pytest.fixture
def write_working_file(tmp_dir_path: Path) -> WriteFile:
    """Return a file writer bound to ``tmp_dir_path``.

    :param tmp_dir_path: Temporary working directory.
    :type tmp_dir_path: Path
    :return: XSV file writer callable.
    :rtype: WriteFile
    """
    return make_xsv_writer(tmp_dir_path)


@pytest.fixture
def with_invalid_flag_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject an unrecognized CLI flag into ``qsv`` subcommand invocations before running them.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """
    real_subprocess_run = qsv.subprocess.run

    def _patched_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[Any]:
        target_cmd = [*cmd[:2], "--some-invalid-flag", *cmd[2:]] if len(cmd) >= MIN_STEP_CMD_LEN else cmd
        return real_subprocess_run(target_cmd, **kwargs)  # type: ignore[call-overload]

    monkeypatch.setattr(qsv.subprocess, "run", _patched_run)


@pytest.fixture
def mock_qsv_run(monkeypatch: pytest.MonkeyPatch) -> Callable[..., MagicMock]:
    """Return a factory replacing ``qsv.subprocess.run`` with a configurable fake.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Installer returning the configured ``MagicMock``.
    :rtype: Callable[..., MagicMock]
    """

    def _install(
        returncode: int = 0,
        stderr: str = "",
        output_content: str | None = None,
        output_flag: str = "--output",
        extra_files: dict[Path, str] | None = None,
    ) -> MagicMock:
        def _fake_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            if output_content is not None and output_flag in cmd:
                output_path = Path(cmd[cmd.index(output_flag) + 1])
                output_path.write_text(output_content, encoding="utf-8")
            for path, content in (extra_files or {}).items():
                path.write_text(content, encoding="utf-8")
            return subprocess.CompletedProcess(cmd, returncode=returncode, stdout=None, stderr=stderr)

        mock = MagicMock(side_effect=_fake_run)
        monkeypatch.setattr(qsv.subprocess, "run", mock)
        return mock

    return _install


@pytest.fixture
def fake_args() -> SimpleNamespace:
    """Return a minimal stand-in for ``CleanerValidatorArgs`` for ``copy_safely``/``move_safely`` tests.

    :return: Namespace with ``errors`` and ``file_name``.
    :rtype: SimpleNamespace
    """
    return SimpleNamespace(errors=[], file_name="source.txt")


@pytest.fixture
def make_args(
    tmp_dir_path: Path,
    output_dir_path_derivatives: dict[str, Path],
    first_pass_validated_schema: ValidatedXsvSchema,
    post_norm_validated_schema: ValidatedXsvSchema,
    qsv_cmd: str,
) -> Callable[..., CleanerValidatorArgs]:
    """Return a ``CleanerValidatorArgs`` factory bound to the real ``qsv_cmd``.

    :param tmp_dir_path: Temporary working directory.
    :type tmp_dir_path: Path
    :param output_dir_path_derivatives: Derivative output directories.
    :type output_dir_path_derivatives: dict[str, Path]
    :param first_pass_validated_schema: First-pass validated schema.
    :type first_pass_validated_schema: ValidatedXsvSchema
    :param post_norm_validated_schema: Post-normalisation validated schema.
    :type post_norm_validated_schema: ValidatedXsvSchema
    :param qsv_cmd: Resolved ``qsv`` binary path.
    :type qsv_cmd: str
    :return: Factory building ``CleanerValidatorArgs``.
    :rtype: Callable[..., CleanerValidatorArgs]
    """
    return make_args_factory(
        qsv_cmd,
        tmp_dir_path,
        output_dir_path_derivatives,
        first_pass_validated_schema,
        post_norm_validated_schema,
    )


@pytest.fixture
def make_mock_args(
    tmp_dir_path: Path,
    output_dir_path_derivatives: dict[str, Path],
    first_pass_validated_schema: ValidatedXsvSchema,
    post_norm_validated_schema: ValidatedXsvSchema,
) -> Callable[..., CleanerValidatorArgs]:
    """Return a ``CleanerValidatorArgs`` factory bound to ``FAKE_QSV_CMD``.

    :param tmp_dir_path: Temporary working directory.
    :type tmp_dir_path: Path
    :param output_dir_path_derivatives: Derivative output directories.
    :type output_dir_path_derivatives: dict[str, Path]
    :param first_pass_validated_schema: First-pass validated schema.
    :type first_pass_validated_schema: ValidatedXsvSchema
    :param post_norm_validated_schema: Post-normalisation validated schema.
    :type post_norm_validated_schema: ValidatedXsvSchema
    :return: Factory building mock ``CleanerValidatorArgs``.
    :rtype: Callable[..., CleanerValidatorArgs]
    """
    return make_args_factory(
        FAKE_QSV_CMD,
        tmp_dir_path,
        output_dir_path_derivatives,
        first_pass_validated_schema,
        post_norm_validated_schema,
    )
