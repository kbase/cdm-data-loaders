"""Shared fixtures for xml pipeline integration tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo
from frozendict import frozendict

from cdm_data_loaders.pipelines.xmltodict.pipeline import run_xmltodict_pipeline
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from tests.conftest import REFERENCE_XML_TAG

DEFAULT_XMLTODICT_SETTINGS: frozendict = frozendict(
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
    settings_factory: Callable[..., XmlToDictSettings], dlt_destination_config: str
) -> Callable[..., XmlToDictSettings]:
    """Return a factory that builds a valid XmlToDictSettings, with fields open to override."""
    return lambda **overrides: settings_factory(
        **{
            **DEFAULT_XMLTODICT_SETTINGS,
            "use_destination": dlt_destination_config,
            **overrides,
        }
    )


@pytest.fixture
def scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Return a function that maps a scenario name to its XML data directory."""

    def _resolve(scenario: str) -> str:
        path = test_data_dir / "xml" / scenario
        assert path.is_dir(), f"missing test fixture directory: {path}"
        return str(path)

    return _resolve


@pytest.fixture(scope="session")
def sorted_reference_xml_entries(reference_xml_entries: list[dict[str, Any]]) -> list[str]:
    """Sorted entries for the uniref reference dataset."""
    return sorted(json.dumps(entry.get("entry"), sort_keys=True, default=str) for entry in reference_xml_entries)


@pytest.fixture
def run_xmltodict_pipeline_fxt(
    reference_xml_data_dir: Path,
    dlt_destination_config: str,
    isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]],
) -> Callable[..., Any]:
    """Return a factory that runs the xmltodict pipeline on a chunk dir with full isolation.

    Each run gets a unique output_dir and dataset_name under tmp_path, so
    parallel-safe repeated runs never collide or accumulate rows.
    """
    base_kwargs = {
        **DEFAULT_XMLTODICT_SETTINGS,
        "settings_cls": XmlToDictSettings,
        "table_name": "entry",
        "use_destination": dlt_destination_config,
        "xml_tag": REFERENCE_XML_TAG,
    }
    runner = isolated_run_factory(
        dataset_prefix="reference_run",
        run_fn=run_xmltodict_pipeline,
        base_kwargs=base_kwargs,
    )

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:  # noqa: ANN401
        return runner(input_dir=str(reference_xml_data_dir / chunk_dir), **overrides)

    return _factory
