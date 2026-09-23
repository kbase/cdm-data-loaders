"""Shared fixtures for xml2db pipeline integration tests."""

import gzip
from collections.abc import Callable
from itertools import count
from pathlib import Path
from typing import Any, Final

import dlt
import pytest
from dlt.common.pipeline import LoadInfo
from frozendict import frozendict

import cdm_data_loaders.pipelines.xml2db.pipeline as xml2db_ingest_module
from cdm_data_loaders.pipelines.xml2db.pipeline import (
    run_xml2db_ingest_pipeline,
    xml2db_reader,
)
from cdm_data_loaders.pipelines.xml2db.settings import Xml2DbSettings

LIBRARY_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="library">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="book" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="xs:string"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

SIMPLE_LIBRARY_XML: Final[str] = """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
    <book id="3"><title>The Tommyknockers</title></book>
</library>
"""

REFERENCE_XML_NS: Final[str] = "http://uniprot.org/uniref"
REFERENCE_XML_FIXTURE_DIR: Final[Path] = Path("tests") / "data" / "uniprot" / "uniref"
REFERENCE_XSD: Final[Path] = REFERENCE_XML_FIXTURE_DIR / "uniref.xsd"
N_REFERENCE_XML_ENTRIES: Final[int] = 100

DEFAULT_XML2DB_SETTINGS: frozendict = frozendict(
    {
        "buffer_size": 10,
        "dev_mode": False,
        "file_glob": "*.xml*",
        "log_interval": 1000,
        "use_output_dir_for_pipeline_metadata": False,
    }
)


@pytest.fixture
def library_xsd(tmp_path: Path) -> Path:
    """Write the small library.xsd schema used for basic pipeline-mechanics tests."""
    xsd_path = tmp_path / "library.xsd"
    xsd_path.write_text(LIBRARY_XSD, encoding="utf-8")
    return xsd_path


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


@pytest.fixture(scope="session")
def reference_xml_data_dir() -> Path:
    """Path to the uniref fixture directory (shared with the xmltodict reference tests)."""
    return REFERENCE_XML_FIXTURE_DIR


@pytest.fixture(scope="session")
def reference_xsd() -> Path:
    """Path to the uniref.xsd schema."""
    return REFERENCE_XSD


@pytest.fixture
def fresh_xml2db_reader(monkeypatch: pytest.MonkeyPatch) -> Callable[[], Any]:
    """Return a factory that rebuilds the module-level xml2db_reader transformer.

    run_xml2db_ingest_pipeline binds the module-level transformer in place, so a
    second pipeline run in the same test session would raise TypeError. Each
    call replaces the module attribute with a fresh transformer built from the
    original wrapped function and restores it after the test.
    """
    original = xml2db_reader
    counter = count()

    def _factory() -> Any:
        fresh = dlt.transformer(
            original.__wrapped__,
            name=f"xml2db_reader_{next(counter)}",
            parallelized=True,
        )
        monkeypatch.setattr(xml2db_ingest_module, "xml2db_reader", fresh)
        return fresh

    return _factory


def make_settings(
    tmp_path: Path, dlt_destination_config: str, xsd_file: str | Path, **overrides: Any
) -> dict[str, Any]:
    """Build a valid xml2db settings dict, with fields open to override."""
    log_config_file = overrides.get("log_config_file", tmp_path / "logging.conf")
    log_config_file.touch()
    output_dir = overrides.get("output_dir", tmp_path / "output")
    output_dir.mkdir(exist_ok=True)
    input_dir = overrides.get("input_dir", tmp_path / "input")

    kwargs: dict[str, Any] = {
        **DEFAULT_XML2DB_SETTINGS,
        "dataset_name": "xml2db_test_dataset",
        "input_dir": str(input_dir),
        "log_config_file": str(log_config_file),
        "output_dir": str(output_dir),
        "use_destination": dlt_destination_config,
        "xsd_file": str(xsd_file),
    }
    kwargs.update(overrides)
    return kwargs


@pytest.fixture
def settings_factory(tmp_path: Path, dlt_destination_config: str, library_xsd: Path) -> Callable[..., Xml2DbSettings]:
    """Return a factory that builds a valid Xml2DbSettings using the library.xsd schema."""

    def _factory(**overrides: Any) -> Xml2DbSettings:
        settings_dict = make_settings(tmp_path, dlt_destination_config, xsd_file=library_xsd, **overrides)
        input_dir = Path(settings_dict["input_dir"])
        input_dir.mkdir(exist_ok=True)
        return Xml2DbSettings(**settings_dict)

    return _factory


@pytest.fixture
def run_xml2db_pipeline(
    tmp_path: Path,
    reference_xml_data_dir: Path,
    reference_xsd: Path,
    monkeypatch: pytest.MonkeyPatch,
    dlt_destination_config: str,
) -> Callable[..., Any]:
    """Return a factory that runs the xml2db pipeline on a chunk dir with full isolation.

    Each run gets a unique output_dir and dataset_name under tmp_path, and a fresh
    module-level xml2db_reader transformer, so parallel-safe repeated runs never
    collide or accumulate rows.
    """
    original_reader = xml2db_reader
    run_counter = count()

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:
        run_index = next(run_counter)

        fresh_reader = dlt.transformer(
            original_reader.__wrapped__,
            name=f"xml2db_reader_{run_index}",
            parallelized=True,
        )
        monkeypatch.setattr(xml2db_ingest_module, "xml2db_reader", fresh_reader)

        output_dir = tmp_path / f"output_{run_index}"
        output_dir.mkdir()
        dataset_name = f"reference_run_{run_index}"
        log_config_file = tmp_path / f"logging_{run_index}.conf"
        log_config_file.touch()
        kwargs = {
            "buffer_size": 10,
            "dataset_name": dataset_name,
            "dev_mode": False,
            "file_glob": "*.xml*",
            "input_dir": str(reference_xml_data_dir / chunk_dir),
            "log_config_file": str(log_config_file),
            "log_interval": 1000,
            "output_dir": str(output_dir),
            "short_name": "uniref",
            "use_destination": dlt_destination_config,
            "use_output_dir_for_pipeline_metadata": False,
            "xsd_file": str(reference_xsd),
        }
        kwargs.update(overrides)
        settings = Xml2DbSettings(**kwargs)
        load_info: LoadInfo | None = run_xml2db_ingest_pipeline(settings)
        return (load_info, output_dir)

    return _factory
