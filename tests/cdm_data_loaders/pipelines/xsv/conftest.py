"""Fixtures shared by ``tests/cdm_data_loaders/pipelines/xsv``."""

from collections.abc import Callable
from pathlib import Path

import pytest

from cdm_data_loaders.pipelines.xsv.pipeline import XsvWorkPaths
from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from cdm_data_loaders.validation.jsonschema.models import generate_header
from tests.helpers.fixtures_xsv import (
    make_xsv_settings_factory,
    resolve_qsv_cmd,
    write_xsv_input_file,
)


@pytest.fixture(scope="session")
def qsv_cmd() -> str:
    """Resolve the ``qsv`` binary from ``PATH`` or ``QSV_BIN``, calling ``pytest.skip`` when absent.

    :return: Path to the ``qsv`` executable.
    :rtype: str
    """
    return resolve_qsv_cmd()


@pytest.fixture
def settings_factory(tmp_path: Path, dlt_destination_config: str) -> Callable[..., XsvIngestSettings]:
    """Return a factory building ``XsvIngestSettings`` staged under ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :return: Settings factory callable.
    :rtype: Callable[..., XsvIngestSettings]
    """
    return make_xsv_settings_factory(tmp_path, dlt_destination_config)


@pytest.fixture
def work_paths_factory(tmp_path: Path, qsv_cmd: str) -> Callable[[XsvIngestSettings], XsvWorkPaths]:
    """Return a factory building ``XsvWorkPaths`` (scratch dirs and header file) for one settings object.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param qsv_cmd: Resolved ``qsv`` executable path fixture.
    :type qsv_cmd: str
    :return: Work-paths factory callable.
    :rtype: Callable[[XsvIngestSettings], XsvWorkPaths]
    """

    def _factory(settings: XsvIngestSettings) -> XsvWorkPaths:
        tmp_dir = tmp_path / "tmp"
        qsv_output_dir = tmp_path / "qsv_output"
        validated_dir = tmp_path / "validated"
        download_dir = tmp_path / "download"
        for directory in (tmp_dir, qsv_output_dir, validated_dir, download_dir):
            directory.mkdir(exist_ok=True)
        return XsvWorkPaths(
            qsv_cmd=qsv_cmd,
            header_file_path=generate_header(
                settings.validated_schema,
                tmp_dir,
                delimiter=settings.parsing_config.get("delimiter", "\t"),
            ),
            tmp_dir=tmp_dir,
            qsv_output_dir=qsv_output_dir,
            validated_dir=validated_dir,
            download_dir=download_dir,
        )

    return _factory


@pytest.fixture
def write_xsv_file() -> Callable[[XsvIngestSettings, str, str], Path]:
    """Return a helper writing text content into ``settings.input_dir``.

    :return: Writer callable ``(settings, file_name, content) -> Path``.
    :rtype: Callable[[XsvIngestSettings, str, str], Path]
    """
    return write_xsv_input_file
