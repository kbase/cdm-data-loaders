"""Shared fixtures for ``tests/cdm_data_loaders/pipelines/xmltopyarrow``."""

import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from tests.helpers.cli_assertions import make_cts_settings


@pytest.fixture
def settings_factory(tmp_path: Path, library_xsd: Path) -> Callable[..., Any]:
    """Return a factory building ``XmlToPyArrowSettings`` (or ``CtsSettings``) with overridable fields.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param library_xsd: Minimal library XSD file fixture.
    :type library_xsd: Path
    :return: Settings factory callable.
    :rtype: Callable[..., Any]
    """

    def _factory(
        settings_cls: type[CtsSettings] = XmlToPyArrowSettings,
        **overrides: object,
    ) -> CtsSettings:
        defaults: dict[str, Any] = {
            "dlt_dev_mode": True,
        }
        if settings_cls is XmlToPyArrowSettings:
            defaults.update(
                {
                    "dataset_name": "xml_test_dataset",
                    "xsd_file": str(library_xsd),
                }
            )
            if overrides.get("xml_level") is None:
                defaults.update(
                    {
                        "table_name": "book",
                        "xml_tag": "book",
                    }
                )
        defaults.update(overrides)
        return make_cts_settings(settings_cls, tmp_path, **defaults)

    return _factory


@pytest.fixture
def copy_xsd_fixture(tmp_path: Path, test_data_dir: Path) -> Callable[[str], str]:
    """Return a helper copying an XSD file from ``tests/data/xsd`` into ``tmp_path / 'input'``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param test_data_dir: Root ``tests/data`` directory fixture.
    :type test_data_dir: Path
    :return: Copy callable returning the destination XSD path string.
    :rtype: Callable[[str], str]
    """

    def _copy(filename: str) -> str:
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        dest = input_dir / filename
        shutil.copy(test_data_dir / "xsd" / filename, dest)
        return str(dest)

    return _copy
