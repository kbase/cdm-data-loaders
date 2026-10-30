"""End-to-end tests: pipelines load the same data from use_resource (local or S3) as from input_dir.

Each test runs a pipeline twice over the same input files: once reading them from `input_dir` and
once from a dlt filesystem source named by `use_resource`. The source is parametrized (via the
`input_source_factory` fixture) over a local directory and an S3 bucket on a moto server.
"""

import gzip
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import dlt
import pytest
from dlt.common.pipeline import LoadInfo
from pydantic import BaseModel

from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import run_jsonlines_pipeline
from cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline import (
    run_jsonlines_with_validation_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings, JsonlPydanticIngestSettings
from cdm_data_loaders.pipelines.uniref_cdm import UnirefSettings, run_uniref_pipeline
from cdm_data_loaders.pipelines.xml2db.pipeline import run_xml2db_pipeline
from cdm_data_loaders.pipelines.xml2db.settings import Xml2DbSettings
from cdm_data_loaders.pipelines.xmltodict.pipeline import run_xmltodict_pipeline
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.pipelines.xsv.pipeline import run_xsv_pipeline
from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from tests.cdm_data_loaders.pipelines.helpers import assert_run_ok, read_data_rows
from tests.conftest import LOCAL_FS, TEST_DATA_DIR, InputSourceFactory
from tests.helpers import make_cts_settings
from tests.jsonlines_fixtures import Widget

pytestmark = pytest.mark.s3

INPUT_DIR: Final[str] = "input_dir"
USE_RESOURCE: Final[str] = "use_resource"
INPUT_MODES: Final[tuple[str, str]] = (INPUT_DIR, USE_RESOURCE)

PEOPLE_XML: Final[str] = """<?xml version="1.0"?>
<people>
    <person id="{id}"><name>{name}</name></person>
</people>
"""

LIBRARY_XML: Final[str] = """<?xml version="1.0"?>
<library>
    <book id="{id}"><title>{title}</title></book>
</library>
"""

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

XSV_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["number", "string"],
    "properties": {"number": {"type": "integer"}, "string": {"type": "string"}},
    "x-xsv-config": {"x-delimiter": ",", "x-has-header": True},
}

UNIREF_FIXTURE: Final[Path] = TEST_DATA_DIR / "uniprot" / "uniref" / "integration" / "uniref_chunk_00001.xml"

type PipelineFn = Callable[[Any], LoadInfo | None]


def write_file(path: Path, content: str, *, gzipped: bool = False) -> None:
    """Write text content to path, gzip-compressed if requested, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if gzipped:
        path.write_bytes(gzip.compress(content.encode("utf-8")))
    else:
        path.write_text(content, encoding="utf-8")


def canonical_rows(tables: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    """Order-independent, JSON-serialized form of each table's rows, for comparison across runs."""
    return {
        table: sorted(json.dumps(row, sort_keys=True, default=str) for row in rows) for table, rows in tables.items()
    }


def run_each_input_mode(
    settings_cls: type[CtsSettings],
    pipeline_fn: PipelineFn,
    source_dir: Path,
    tmp_path: Path,
    input_source_factory: InputSourceFactory,
    *,
    file_format: str = "jsonl",
    **settings_kwargs: Any,  # noqa: ANN401
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Run a pipeline over source_dir via input_dir and via use_resource; return each run's loaded rows.

    Each run loads into its own local filesystem destination bucket.
    """
    results: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for mode in INPUT_MODES:
        bucket = tmp_path / f"bucket_{mode}"
        bucket.mkdir()
        dlt.config[f"destination.{LOCAL_FS}.destination_type"] = "filesystem"
        dlt.config[f"destination.{LOCAL_FS}.bucket_url"] = str(bucket)

        input_kwargs = (
            {INPUT_DIR: str(source_dir)} if mode == INPUT_DIR else {USE_RESOURCE: input_source_factory(source_dir)}
        )
        settings = make_cts_settings(
            settings_cls,
            tmp_path / mode,
            use_destination=LOCAL_FS,
            dlt_dev_mode=True,
            **settings_kwargs,
            **input_kwargs,
        )
        if mode == USE_RESOURCE:
            assert settings.input_dir is None
        load_info = assert_run_ok(pipeline_fn(settings))
        results[mode] = read_data_rows(bucket / load_info.dataset_name, file_format)
    return results


def assert_same_rows(results: dict[str, dict[str, list[dict[str, Any]]]]) -> dict[str, list[dict[str, Any]]]:
    """Assert both input modes loaded identical, non-empty data; return the rows from the input_dir run."""
    assert results[INPUT_DIR]
    assert canonical_rows(results[USE_RESOURCE]) == canonical_rows(results[INPUT_DIR])
    return results[INPUT_DIR]


def test_jsonl_extract_pass_use_resource(tmp_path: Path, input_source_factory: InputSourceFactory) -> None:
    """jsonl_extract loads plain and gzipped files, including nested ones, identically from both inputs."""
    source = tmp_path / "source"
    write_file(source / "a.jsonl", '{"widget_id": "a", "count": 1}\nnot json\n')
    write_file(source / "nested" / "b.jsonl.gz", '{"widget_id": "b", "count": 2}\n', gzipped=True)

    rows = assert_same_rows(
        run_each_input_mode(
            JsonlExtractSettings,
            run_jsonlines_pipeline,
            source,
            tmp_path,
            input_source_factory,
            dataset_name="widgets",
            table_name="widget",
            file_glob="**/*.jsonl*",
            loader_file_format="jsonl",
        )
    )
    assert sorted(row["widget_id"] for row in rows["widget"]) == ["a", "b"]
    assert {row["source_file"] for row in rows["widget_invalid"]} == {"a.jsonl"}


def test_jsonl_pydantic_pass_use_resource(
    tmp_path: Path,
    input_source_factory: InputSourceFactory,
    entity_models_module_factory: Callable[[dict[str, type[BaseModel]]], str],
) -> None:
    """jsonl_pydantic reads per-table subdirectories (or S3 prefixes); a missing one yields no rows."""
    source = tmp_path / "source"
    write_file(source / "widget" / "w.jsonl", '{"widget_id": "a", "count": 1}\n{"widget_id": "", "count": -1}\n')
    module = entity_models_module_factory({"widget": Widget, "gadget": Widget})

    rows = assert_same_rows(
        run_each_input_mode(
            JsonlPydanticIngestSettings,
            run_jsonlines_with_validation_pipeline,
            source,
            tmp_path,
            input_source_factory,
            dataset_name="widgets",
            entity_models_module=module,
            loader_file_format="jsonl",
        )
    )
    assert [row["widget_id"] for row in rows["widget"]] == ["a"]
    assert [row["source_file"] for row in rows["widget_rejected"]] == ["w.jsonl"]
    assert "gadget" not in rows


def test_xmltodict_pass_use_resource(tmp_path: Path, input_source_factory: InputSourceFactory) -> None:
    """The xmltodict pipeline streams plain and gzipped XML identically from both inputs."""
    source = tmp_path / "source"
    write_file(source / "one.xml", PEOPLE_XML.format(id="1", name="Ann"))
    write_file(source / "two.xml.gz", PEOPLE_XML.format(id="2", name="Bob"), gzipped=True)

    rows = assert_same_rows(
        run_each_input_mode(
            XmlToDictSettings,
            run_xmltodict_pipeline,
            source,
            tmp_path,
            input_source_factory,
            dataset_name="people",
            table_name="person",
            xml_tag="person",
            loader_file_format="jsonl",
        )
    )
    # column names depend on dlt's flattening; check the values made it through instead
    serialized = canonical_rows(rows)["person"]
    assert len(serialized) == 2  # noqa: PLR2004
    assert any("Ann" in row for row in serialized)
    assert any("Bob" in row for row in serialized)


@pytest.mark.parametrize("xml_tag", [None, "book"], ids=["whole_file", "chunked"])
def test_xml2db_pass_use_resource(
    tmp_path: Path, input_source_factory: InputSourceFactory, xml_tag: str | None
) -> None:
    """xml2db produces identical rows (and keys) from both inputs, whole-file and chunked."""
    source = tmp_path / "source"
    write_file(source / "lib_a.xml", LIBRARY_XML.format(id="1", title="The Shining"))
    write_file(source / "lib_b.xml.gz", LIBRARY_XML.format(id="2", title="The Stand"), gzipped=True)
    xsd = tmp_path / "library.xsd"
    write_file(xsd, LIBRARY_XSD)

    rows = assert_same_rows(
        run_each_input_mode(
            Xml2DbSettings,
            run_xml2db_pipeline,
            source,
            tmp_path,
            input_source_factory,
            dataset_name="library",
            xsd_file=str(xsd),
            loader_file_format="jsonl",
            compact_reused_tables=False,
            **({"xml_tag": xml_tag} if xml_tag else {}),
        )
    )
    assert sum(len(table_rows) for table_rows in rows.values()) > 0


@pytest.mark.skipif(shutil.which("qsv") is None, reason="qsv binary not available")
def test_xsv_pass_use_resource(tmp_path: Path, input_source_factory: InputSourceFactory) -> None:
    """The xsv pipeline copies remote files locally for qsv and loads identical rows from both inputs."""
    source = tmp_path / "source"
    write_file(source / "data.csv", "number,string\n1,one\n2,two\n")
    schema_file = tmp_path / "schema.json"
    write_file(schema_file, json.dumps(XSV_SCHEMA))

    rows = assert_same_rows(
        run_each_input_mode(
            XsvIngestSettings,
            run_xsv_pipeline,
            source,
            tmp_path,
            input_source_factory,
            dataset_name="xsv_data",
            table_name="my_table",
            schema_file=str(schema_file),
            file_glob="*.csv",
            loader_file_format="jsonl",
        )
    )
    assert sorted(row["number"] for row in rows["my_table"]) == [1, 2]


def test_uniref_cdm_pass_use_resource(tmp_path: Path, input_source_factory: InputSourceFactory) -> None:
    """uniref_cdm loads the same tables from both inputs and records the relative source file."""
    source = tmp_path / "source"
    source.mkdir()
    shutil.copy(UNIREF_FIXTURE, source / UNIREF_FIXTURE.name)

    results = run_each_input_mode(
        UnirefSettings, run_uniref_pipeline, source, tmp_path, input_source_factory, file_format="parquet", variant="50"
    )

    # rows carry a per-run timestamp, so compare table sizes and provenance rather than whole rows
    for mode in INPUT_MODES:
        assert {row["source_file"] for row in results[mode]["entity_x_source_file"]} == {UNIREF_FIXTURE.name}
    assert {table: len(rows) for table, rows in results[USE_RESOURCE].items()} == {
        table: len(rows) for table, rows in results[INPUT_DIR].items()
    }
