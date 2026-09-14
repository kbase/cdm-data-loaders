"""Shared fixtures for xml_ingest pipeline tests."""

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

MIXED_TAGS_XML: Final[str] = """<?xml version="1.0"?>
<catalog>
    <book id="1"><title>Cell</title></book>
    <magazine id="1"><title>National Geographic</title></magazine>
    <book id="2"><title>Cujo</title></book>
</catalog>
"""

NESTED_CONTENT_XML: Final[str] = """<?xml version="1.0"?>
<library>
    <book id="1">
        <title>Cell</title>
        <author><name>Stephen King</name></author>
    </book>
</library>
"""

MALFORMED_XML: Final[str] = '<library><book id="1"><title>Broken</title></library>'

EXPECTED_BOOK_IDS: Final[list[str]] = ["1", "2", "3"]


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

    def _factory() -> Any:  # noqa: ANN401
        fresh = dlt.transformer(
            original.__wrapped__,
            name=f"xml_to_dict_reader_{next(counter)}",
            parallelized=True,
        )
        monkeypatch.setattr(xml_to_dict_ingest_module, "xml_to_dict_reader", fresh)
        return fresh

    return _factory


@pytest.fixture
def settings_factory(tmp_path: Path) -> Callable[..., XmlToDictIngestSettings]:
    """Return a factory that builds a valid XmlToDictIngestSettings, with fields open to override."""

    def _factory(**overrides: Any) -> XmlToDictIngestSettings:  # noqa: ANN401
        log_config_file = tmp_path / "logging.json"
        log_config_file.write_text('{"version": 1}')
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)

        kwargs: dict[str, Any] = {
            "log_config_file": str(log_config_file),
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "dev_mode": True,
            "use_destination": "local_fs",
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
def xml_dir_factory(tmp_path: Path) -> Callable[..., Path]:
    """Return a factory that writes XML content to files in a directory under tmp_path."""

    def _make(
        *contents: str,
        dirname: str = "input",
        gzip_compress: bool = False,
    ) -> Path:
        directory = tmp_path / dirname
        directory.mkdir(parents=True, exist_ok=True)
        for index, content in enumerate(contents):
            filename = f"data_{index}.xml"
            if gzip_compress:
                with gzip.open(directory / f"{filename}.gz", "wb") as f:
                    f.write(content.encode("utf-8"))
            else:
                (directory / filename).write_text(content, encoding="utf-8")
        return directory

    return _make
