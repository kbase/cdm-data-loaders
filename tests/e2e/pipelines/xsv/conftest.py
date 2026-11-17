"""Shared fixtures for ``tests/e2e/pipelines/xsv``."""

from collections.abc import Callable
from pathlib import Path

import pytest

from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from tests.helpers.fixtures_xsv import (
    make_xsv_settings_factory,
    write_xsv_input_file,
)


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
def write_xsv_file() -> Callable[[XsvIngestSettings, str, str], Path]:
    """Return a helper writing text content into ``settings.input_dir``.

    :return: Writer callable ``(settings, file_name, content) -> Path``.
    :rtype: Callable[[XsvIngestSettings, str, str], Path]
    """
    return write_xsv_input_file
