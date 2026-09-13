"""Shared fixtures for xml_ingest pipeline integration tests."""

import gzip
from collections.abc import Callable
from itertools import count
from pathlib import Path
from typing import Any, Final

import dlt
import pytest

import cdm_data_loaders.pipelines.xml_to_dict_ingest as xml_to_dict_ingest_module
from cdm_data_loaders.pipelines.xml_to_dict_ingest import (
    XmlToDictIngestSettings,
    xml_to_dict_reader,
)

SIMPLE_LIBRARY_XML: Final[str] = """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
    <book id="3"><title>The Tommyknockers</title></book>
</library>
"""


@pytest.fixture
def fresh_xml_to_dict_reader(monkeypatch: pytest.MonkeyPatch) -> Callable[[], Any]:
    """Return a factory that rebuilds the module-level xml_to_dict_reader transformer.

    run_xml_ingest_pipeline binds the module-level transformer in place, so a
    second pipeline run in the same test session would raise TypeError. Each
    call replaces the module attribute with a fresh transformer built from the
    original wrapped function and restores it after the test.
    """
    original = xml_to_dict_reader
    counter = count()

    def _factory() -> Any:
        fresh = dlt.transformer(
            original.__wrapped__,
            name=f"xml_to_dict_reader_{next(counter)}",
            parallelized=True,
        )
        monkeypatch.setattr(xml_to_dict_ingest_module, "xml_to_dict_reader", fresh)
        return fresh

    return _factory


@pytest.fixture
def settings_factory(tmp_path: Path, dlt_destination_config: str) -> Callable[..., XmlToDictIngestSettings]:
    """Return a factory that builds a valid XmlToDictIngestSettings, with fields open to override."""

    def _factory(**overrides: Any) -> XmlToDictIngestSettings:
        log_config_file = tmp_path / "logging.conf"
        log_config_file.touch()
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)

        kwargs: dict[str, Any] = {
            "log_config_file": str(log_config_file),
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "dev_mode": False,
            "use_destination": dlt_destination_config,
            "use_output_dir_for_pipeline_metadata": False,
            "buffer_size": 100,
            "log_interval": 1000,
            "dataset_name": "xml_test_dataset",
            "table_name": "book",
            "xml_tag": "book",
            "file_glob": "*.xml*",
        }
        kwargs.update(overrides)
        return XmlToDictIngestSettings(**kwargs)

    return _factory


@pytest.fixture
def scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Return a function that maps a scenario name to its XML data directory."""

    def _resolve(scenario: str) -> str:
        path = test_data_dir / "xml" / scenario
        assert path.is_dir(), f"missing test fixture directory: {path}"
        return str(path)

    return _resolve


@pytest.fixture
def write_gzip_xml_file() -> Callable[[Path, str, str], Path]:
    """Return a function that writes XML content to a gzip-compressed file."""

    def _write(directory: Path, filename: str, content: str) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / filename
        with gzip.open(file_path, "wb") as f:
            f.write(content.encode("utf-8"))
        return file_path

    return _write
