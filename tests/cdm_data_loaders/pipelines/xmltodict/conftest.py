"""Shared fixtures for ``tests/cdm_data_loaders/pipelines/xmltodict``."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from tests.helpers.cli_assertions import make_cts_settings


@pytest.fixture
def settings_factory(tmp_path: Path) -> Callable[..., XmlToDictSettings]:
    """Return a factory building ``XmlToDictSettings`` with overridable fields.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Settings factory callable.
    :rtype: Callable[..., XmlToDictSettings]
    """

    def _factory(**overrides: object) -> XmlToDictSettings:
        defaults: dict[str, Any] = {
            "dlt_dev_mode": True,
            "dataset_name": "xml_test_dataset",
            "table_name": "book",
            "xml_tag": "book",
            "file_glob": "*.xml*",
        }
        defaults.update(overrides)
        return make_cts_settings(XmlToDictSettings, tmp_path, **defaults)

    return _factory
