"""Shared fixtures for ``tests/e2e/pipelines/xmltopyarrow``."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo
from frozendict import frozendict

from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from tests.helpers.cli_assertions import make_cts_settings
from tests.helpers.paths_and_constants import REFERENCE_XML_TAG

DEFAULT_XMLTOPYARROW_SETTINGS: frozendict[str, Any] = frozendict(
    {
        "buffer_size": 10,
        "dlt_dev_mode": False,
        "file_glob": "*.xml*",
        "log_interval": 1000,
        "use_output_dir_for_pipeline_metadata": False,
    }
)


@pytest.fixture
def settings_factory(
    tmp_path: Path,
    library_xsd: Path,
    dlt_destination_config: str,
) -> Callable[..., Any]:
    """Return a factory building ``XmlToPyArrowSettings`` (or subclass) with overridable fields.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param library_xsd: Minimal library XSD file fixture.
    :type library_xsd: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :return: Settings factory callable.
    :rtype: Callable[..., Any]
    """

    def _factory(
        settings_cls: type[CtsSettings] = XmlToPyArrowSettings,
        **overrides: object,
    ) -> CtsSettings:
        defaults: dict[str, Any] = {
            **DEFAULT_XMLTOPYARROW_SETTINGS,
            "use_destination": dlt_destination_config,
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
def run_xmltopyarrow_pipeline_fxt(
    reference_xml_data_dir: Path,
    dlt_destination_config: str,
    isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]],
) -> Callable[..., tuple[LoadInfo | None, Path]]:
    """Return a factory running ``run_xmltopyarrow_pipeline`` on a chunk dir with isolated output directories.

    :param reference_xml_data_dir: UniRef reference directory fixture.
    :type reference_xml_data_dir: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :param isolated_run_factory: Builder for per-run isolated pipeline factories.
    :type isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]]
    :return: Chunk-running callable ``(chunk_dir, **overrides) -> (load_info, output_dir)``.
    :rtype: Callable[..., tuple[LoadInfo | None, Path]]
    """
    base_kwargs: dict[str, Any] = {
        **DEFAULT_XMLTOPYARROW_SETTINGS,
        "settings_cls": XmlToPyArrowSettings,
        "table_name": "entry",
        "use_destination": dlt_destination_config,
        "xml_tag": REFERENCE_XML_TAG,
        "xsd_file": str(reference_xml_data_dir / "uniref.xsd"),
    }
    runner = isolated_run_factory(
        dataset_prefix="reference_run",
        run_fn=run_xmltopyarrow_pipeline,
        base_kwargs=base_kwargs,
    )

    def _factory(chunk_dir: str, **overrides: object) -> tuple[LoadInfo | None, Path]:
        return runner(input_dir=str(reference_xml_data_dir / chunk_dir), **overrides)

    return _factory
