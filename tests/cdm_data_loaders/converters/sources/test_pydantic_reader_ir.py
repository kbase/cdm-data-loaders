"""Unit and integration tests for PydanticReader and converter IR emitters."""

import datetime as dt
from decimal import Decimal
from typing import Annotated, Final

import pyarrow as pa
import pytest
from linkml_runtime.linkml_model.meta import SchemaDefinition
from pydantic import BaseModel, Field, ValidationError
from pyspark.sql.types import StructType

from cdm_data_loaders.converters.emitters.dlt import DltEmitter
from cdm_data_loaders.converters.emitters.json_schema.emitter import JsonSchemaEmitter
from cdm_data_loaders.converters.emitters.linkml import LinkMLEmitter
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.emitters.pyspark import PySparkEmitter
from cdm_data_loaders.converters.exceptions.sources.pyarrow import PyArrowReaderError
from cdm_data_loaders.converters.sources.pyarrow import PyArrowReader
from cdm_data_loaders.converters.sources.pydantic.reader import PydanticReader

EXPECTED_COVERAGE_PRECISION: Final[int] = 8
EXPECTED_COVERAGE_SCALE: Final[int] = 2


class NestedTag(BaseModel):
    """Key-value tag annotation."""

    key: str
    value: str | None = None


class SampleAssemblyRecord(BaseModel):
    """Assembly record used for multi-target IR emission tests."""

    accession: str = Field(description="Primary assembly accession")
    contig_count: int = Field(alias="contigCount")
    gc_ratio: float
    is_refseq: bool = True
    coverage: Annotated[Decimal, Field(max_digits=8, decimal_places=2)]
    release_date: dt.date
    updated_at: dt.datetime
    checksum: bytes | None = None
    tags: list[NestedTag] = Field(default_factory=list)
    metrics: dict[str, int] = Field(default_factory=dict)


def test_pydantic_reader_pass_document_provenance_and_fields() -> None:
    """PydanticReader produces a frozen SchemaDocument with source='pydantic' provenance."""
    reader = PydanticReader()
    doc = reader.read(SampleAssemblyRecord)

    assert doc.name == "SampleAssemblyRecord"
    assert doc.provenance.source == "pydantic"
    assert doc.annotations.get("description") == "Assembly record used for multi-target IR emission tests."
    assert doc.root.type == "object"
    assert not doc.root.nullable
    assert "accession" in doc.root.required_names
    assert "checksum" not in doc.root.required_names

    props = {f.name: f for f in doc.root.properties}
    assert props["accession"].annotations.get("description") == "Primary assembly accession"
    assert props["accession"].provenance.metadata.get("pydantic_type") == "string"
    assert props["coverage"].node.hints.logical_type == "decimal"
    assert props["coverage"].node.hints.precision == EXPECTED_COVERAGE_PRECISION
    assert props["coverage"].node.hints.scale == EXPECTED_COVERAGE_SCALE
    assert props["release_date"].node.hints.logical_type == "date"
    assert props["updated_at"].node.hints.logical_type == "timestamp-with-tz"
    assert props["checksum"].node.hints.logical_type == "binary"
    assert props["checksum"].node.nullable is True


def test_pydantic_reader_pass_by_alias_mode() -> None:
    """PydanticReader(by_alias=True) emits aliased field names in the IR."""
    doc = PydanticReader(by_alias=True).read(SampleAssemblyRecord)
    field_names = [f.name for f in doc.root.properties]
    assert "contigCount" in field_names
    assert "contig_count" not in field_names


def test_pyarrow_reader_pass_read_field_and_read_node() -> None:
    """PyArrowReader.read_field and read_node convert individual PyArrow fields and types."""
    reader = PyArrowReader()
    pa_f = pa.field("event_time", pa.time64("us"), nullable=False, metadata={"comment": "Time of day"})
    ir_field = reader.read_field(pa_f)
    assert ir_field.name == "event_time"
    assert ir_field.required is True
    assert ir_field.annotations["description"] == "Time of day"
    assert ir_field.node.hints.logical_type == "time"

    naive_ts_node = reader.read_node(pa.timestamp("us"), nullable=True, path=("naive_ts",))
    assert naive_ts_node.hints.logical_type == "timestamp-without-tz"
    assert naive_ts_node.nullable is True


class LinkMLCompatibleModel(BaseModel):
    """Model with scalar and nested list fields compatible with LinkML emission."""

    id: str
    count: int
    tags: list[NestedTag]


@pytest.mark.parametrize(
    ("target", "model_cls", "emitter"),
    [
        ("pyarrow", SampleAssemblyRecord, PyArrowEmitter()),
        ("pyspark", SampleAssemblyRecord, PySparkEmitter()),
        ("dlt", SampleAssemblyRecord, DltEmitter(schema_name="sample_assembly")),
        ("jsonschema", SampleAssemblyRecord, JsonSchemaEmitter()),
        ("linkml", LinkMLCompatibleModel, LinkMLEmitter()),
    ],
    ids=["pyarrow", "pyspark", "dlt", "jsonschema", "linkml"],
)
def test_pydantic_reader_pass_emit_to_pyarrow_pyspark_dlt_jsonschema_linkml(
    target: str,
    model_cls: type[BaseModel],
    emitter: PyArrowEmitter | PySparkEmitter | DltEmitter | JsonSchemaEmitter | LinkMLEmitter,
) -> None:
    """IR document from PydanticReader can be emitted to PyArrow, PySpark, dlt, JSON Schema, and LinkML."""
    doc = PydanticReader().read(model_cls)
    emitted = emitter.emit(doc)
    match target:
        case "pyarrow":
            assert isinstance(emitted, pa.Schema)
            assert emitted.field("accession").type == pa.string()
            assert emitted.field("accession").nullable is False
            assert emitted.field("coverage").type == pa.decimal128(8, 2)
            assert emitted.field("release_date").type == pa.date32()
        case "pyspark":
            assert isinstance(emitted, StructType)
            spark_fields = {f.name: f for f in emitted.fields}
            assert spark_fields["accession"].nullable is False
            assert spark_fields["checksum"].nullable is True
        case "dlt":
            assert isinstance(emitted, dict)
            dlt_columns = emitted["tables"]["sample_assembly"]["columns"]
            assert dlt_columns["accession"]["data_type"] == "text"
            assert dlt_columns["accession"]["nullable"] is False
            assert dlt_columns["contig_count"]["data_type"] == "bigint"
            assert dlt_columns["coverage"]["data_type"] == "decimal"
        case "jsonschema":
            assert isinstance(emitted, dict)
            assert emitted["type"] == "object"
            assert "accession" in emitted["required"]
            assert emitted["properties"]["accession"]["description"] == "Primary assembly accession"
        case "linkml":
            assert isinstance(emitted, SchemaDefinition)
            assert emitted.name == "LinkMLCompatibleModel"
            assert "LinkMLCompatibleModel" in emitted.classes
        case _:
            pytest.fail(f"Unhandled target: {target}")


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_recursion_depth": 0}, "max_recursion_depth"),
        ({"max_recursion_depth": True}, "max_recursion_depth"),
        ({"by_alias": "yes"}, "by_alias"),
    ],
    ids=["non-positive-depth", "bool-depth", "non-bool-by-alias"],
)
def test_pydantic_reader_fail_invalid_configuration(kwargs: dict[str, object], match: str) -> None:
    """PydanticReader validates max_recursion_depth and by_alias on construction."""
    with pytest.raises(ValidationError, match=match):
        PydanticReader(**kwargs)  # type: ignore[arg-type]


def test_pyarrow_reader_fail_unsupported_arrow_type() -> None:
    """PyArrowReader.read_node raises PyArrowReaderError on unsupported Arrow types."""
    with pytest.raises(PyArrowReaderError, match="Unsupported PyArrow scalar type"):
        PyArrowReader().read_node(pa.duration("us"))
