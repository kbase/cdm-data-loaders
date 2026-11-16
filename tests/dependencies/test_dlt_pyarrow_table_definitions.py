"""Characterisation tests for PyArrow table and column definitions through dlt pipelines.

Checks what table metadata, column descriptions, types, nullability, field names, and row values
dlt keeps or drops across:
- Input mode: ``pa.Table`` passed straight to ``pipeline.run`` vs pages yielded from ``DictBuffer``.
- Destination format: ``loader_file_format="parquet"`` vs ``loader_file_format="iceberg"``
  (``loader_file_format="parquet", table_format="iceberg"``).
- Schema shape: flat scalar table vs four-level nested ``struct`` / ``list`` / ``map`` table,
  plus non-``snake_case`` column and nested field names.
"""

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.libs.pyarrow import deserialize_type
from dlt.common.libs.pyiceberg import get_iceberg_tables
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta
from frozendict import frozendict
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.utils.buffer import DictBuffer
from cdm_data_loaders.utils.iceberg_cleanup import install_iceberg_catalog_cleanup
from tests.cdm_data_loaders.pipelines.helpers import is_metadata_table

TABLE_NAME: Final[str] = "assay_record"
DATASET_NAME: Final[str] = "pyarrow_definitions_check"
TABLE_METADATA_KEYS: Final[frozenset[str]] = frozenset({"comment", "description", "doc", "title"})


class InputMode(StrEnum):
    """How PyArrow data enters ``pipeline.run``."""

    DIRECT_TABLE = "direct_table"
    DICT_BUFFER = "dict_buffer"


class TableShape(StrEnum):
    """Structural shape of the input PyArrow schema."""

    FLAT = "flat"
    DEEP_NESTED = "deep_nested"


INPUT_MODES: Final[tuple[InputMode, ...]] = (InputMode.DIRECT_TABLE, InputMode.DICT_BUFFER)
TABLE_SHAPES: Final[tuple[TableShape, ...]] = (TableShape.FLAT, TableShape.DEEP_NESTED)
LOADER_FORMATS: Final[tuple[LoaderFileFormatEnum, ...]] = (
    LoaderFileFormatEnum.PARQUET,
    LoaderFileFormatEnum.ICEBERG,
)

FLAT_TABLE_METADATA: Final[dict[str, str]] = {
    "comment": "Flat assay table comment",
    "description": "Flat assay table description",
    "doc": "Flat assay table doc",
    "title": "AssayRecord",
}

FLAT_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field(
            "id",
            pa.int64(),
            nullable=False,
            metadata={
                "comment": "Primary record comment",
                "description": "Primary record description",
                "doc": "Primary record identifier",
                "xsd_type": "xs:long",
            },
        ),
        pa.field(
            "count_32",
            pa.int32(),
            nullable=False,
            metadata={
                "comment": "32-bit count comment",
                "description": "32-bit count description",
                "xsd_type": "xs:int",
            },
        ),
        pa.field(
            "small_code",
            pa.int16(),
            nullable=True,
            metadata={
                "comment": "16-bit status comment",
                "doc": "16-bit status code",
                "xsd_type": "xs:short",
            },
        ),
        pa.field(
            "ratio_32",
            pa.float32(),
            nullable=True,
            metadata={
                "comment": "Single-precision ratio comment",
                "description": "Single-precision ratio description",
            },
        ),
        pa.field(
            "score_64",
            pa.float64(),
            nullable=False,
            metadata={
                "comment": "Double-precision score comment",
                "doc": "64-bit quality score",
            },
        ),
        pa.field(
            "amount",
            pa.decimal128(10, 2),
            nullable=False,
            metadata={
                "comment": "Decimal amount comment",
                "doc": "Exact decimal amount",
            },
        ),
        pa.field(
            "is_active",
            pa.bool_(),
            nullable=False,
            metadata={
                "comment": "Active flag comment",
                "description": "Active flag description",
            },
        ),
        pa.field(
            "release_date",
            pa.date32(),
            nullable=True,
            metadata={
                "comment": "Release date comment",
                "doc": "Release calendar date",
            },
        ),
        pa.field(
            "updated_at",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
            metadata={
                "comment": "UTC timestamp comment",
                "description": "UTC timestamp description",
            },
        ),
        pa.field(
            "label",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Label comment",
                "doc": "Human-readable label",
            },
        ),
    ],
    metadata=FLAT_TABLE_METADATA,
)

FLAT_ROWS: Final[tuple[dict[str, Any], ...]] = (
    {
        "id": 1,
        "count_32": 42,
        "small_code": 7,
        "ratio_32": 0.5,
        "score_64": 98.25,
        "amount": Decimal("12345.67"),
        "is_active": True,
        "release_date": date(2026, 3, 15),
        "updated_at": datetime(2026, 3, 15, 8, 30, 0, tzinfo=UTC),
        "label": "alpha",
    },
    {
        "id": 2,
        "count_32": 0,
        "small_code": None,
        "ratio_32": None,
        "score_64": -1.5,
        "amount": Decimal("-0.05"),
        "is_active": False,
        "release_date": None,
        "updated_at": datetime(2026, 3, 16, 14, 0, 0, tzinfo=UTC),
        "label": None,
    },
)

EXPECTED_FLAT_DLT_COLUMNS: Final[dict[str, dict[str, Any]]] = {
    "id": {"name": "id", "data_type": "bigint", "nullable": False},
    "count_32": {"name": "count_32", "data_type": "bigint", "precision": 32, "nullable": False},
    "small_code": {"name": "small_code", "data_type": "bigint", "precision": 16, "nullable": True},
    "ratio_32": {"name": "ratio_32", "data_type": "double", "nullable": True},
    "score_64": {"name": "score_64", "data_type": "double", "nullable": False},
    "amount": {"name": "amount", "data_type": "decimal", "precision": 10, "scale": 2, "nullable": False},
    "is_active": {"name": "is_active", "data_type": "bool", "nullable": False},
    "release_date": {"name": "release_date", "data_type": "date", "nullable": True},
    # dlt omits default microsecond precision=6 when finalizing timestamp columns
    "updated_at": {"name": "updated_at", "data_type": "timestamp", "nullable": False},
    "label": {"name": "label", "data_type": "text", "nullable": True},
}

EXPECTED_FLAT_ICEBERG_SCHEMA: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True, doc="Primary record identifier"),
    NestedField(field_id=2, name="count_32", field_type=IntegerType(), required=True, doc=None),
    NestedField(field_id=3, name="small_code", field_type=IntegerType(), required=False, doc="16-bit status code"),
    NestedField(field_id=4, name="ratio_32", field_type=FloatType(), required=False, doc=None),
    NestedField(field_id=5, name="score_64", field_type=DoubleType(), required=True, doc="64-bit quality score"),
    NestedField(field_id=6, name="amount", field_type=DecimalType(10, 2), required=True, doc="Exact decimal amount"),
    NestedField(field_id=7, name="is_active", field_type=BooleanType(), required=True, doc=None),
    NestedField(field_id=8, name="release_date", field_type=DateType(), required=False, doc="Release calendar date"),
    NestedField(field_id=9, name="updated_at", field_type=TimestamptzType(), required=True, doc=None),
    NestedField(field_id=10, name="label", field_type=StringType(), required=False, doc="Human-readable label"),
)

EXPECTED_FLAT_ICEBERG_DOCS: Final[dict[str, str | None]] = {
    "id": "Primary record identifier",
    "count_32": None,
    "small_code": "16-bit status code",
    "ratio_32": None,
    "score_64": "64-bit quality score",
    "amount": "Exact decimal amount",
    "is_active": None,
    "release_date": "Release calendar date",
    "updated_at": None,
    "label": "Human-readable label",
}

EXPECTED_FLAT_ICEBERG_SCAN_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False, metadata={"doc": "Primary record identifier"}),
        pa.field("count_32", pa.int32(), nullable=False),
        pa.field("small_code", pa.int32(), nullable=True, metadata={"doc": "16-bit status code"}),
        pa.field("ratio_32", pa.float32(), nullable=True),
        pa.field("score_64", pa.float64(), nullable=False, metadata={"doc": "64-bit quality score"}),
        pa.field("amount", pa.decimal128(10, 2), nullable=False, metadata={"doc": "Exact decimal amount"}),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("release_date", pa.date32(), nullable=True, metadata={"doc": "Release calendar date"}),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("label", pa.string(), nullable=True, metadata={"doc": "Human-readable label"}),
    ]
)

DEEP_NESTED_TABLE_METADATA: Final[dict[str, str]] = {
    "comment": "Deep nested table comment",
    "description": "Deep nested table description",
    "doc": "Deep nested table doc",
    "title": "DeepNestedAssayRecord",
}

DEEP_NESTED_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field(
            "id",
            pa.int64(),
            nullable=False,
            metadata={
                "comment": "Entry key comment",
                "description": "Entry key description",
                "doc": "Entry primary key",
            },
        ),
        pa.field(
            "title",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Entry title comment",
                "description": "Entry title description",
            },
        ),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field(
                        "taxon_id",
                        pa.int32(),
                        nullable=False,
                        metadata={"comment": "Taxon ID comment", "doc": "NCBI taxonomy ID"},
                    ),
                    pa.field(
                        "lineage",
                        pa.struct(
                            [
                                pa.field(
                                    "rank",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"comment": "Rank comment", "doc": "Taxonomic rank"},
                                ),
                                pa.field(
                                    "detail",
                                    pa.struct(
                                        [
                                            pa.field(
                                                "division_code",
                                                pa.int16(),
                                                nullable=False,
                                                metadata={"comment": "Division code comment"},
                                            ),
                                            pa.field(
                                                "synonyms",
                                                pa.list_(
                                                    pa.field(
                                                        "synonym_item",
                                                        pa.string(),
                                                        nullable=False,
                                                        metadata={
                                                            "comment": "Synonym item comment",
                                                            "doc": "Synonym item doc",
                                                        },
                                                    )
                                                ),
                                                nullable=True,
                                                metadata={"comment": "Synonyms comment", "doc": "Taxon synonyms"},
                                            ),
                                        ]
                                    ),
                                    nullable=True,
                                    metadata={"comment": "Detail comment", "doc": "Deep lineage detail"},
                                ),
                            ]
                        ),
                        nullable=True,
                        metadata={"comment": "Lineage comment", "description": "Lineage description"},
                    ),
                ]
            ),
            nullable=False,
            metadata={
                "comment": "Organism comment",
                "description": "Organism description",
                "doc": "Source organism record",
            },
        ),
        pa.field(
            "features",
            pa.list_(
                pa.field(
                    "feature_item",
                    pa.struct(
                        [
                            pa.field(
                                "kind",
                                pa.string(),
                                nullable=False,
                                metadata={"comment": "Feature kind comment", "doc": "Feature kind"},
                            ),
                            pa.field(
                                "evidences",
                                pa.list_(
                                    pa.field(
                                        "evidence_item",
                                        pa.struct(
                                            [
                                                pa.field(
                                                    "code",
                                                    pa.string(),
                                                    nullable=False,
                                                    metadata={"comment": "Evidence code comment"},
                                                ),
                                                pa.field(
                                                    "source",
                                                    pa.struct(
                                                        [
                                                            pa.field(
                                                                "db",
                                                                pa.string(),
                                                                nullable=False,
                                                                metadata={
                                                                    "comment": "Database name comment",
                                                                    "doc": "Database name",
                                                                },
                                                            ),
                                                            pa.field(
                                                                "accession",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"comment": "Accession comment"},
                                                            ),
                                                        ]
                                                    ),
                                                    nullable=True,
                                                    metadata={
                                                        "comment": "Citation source comment",
                                                        "doc": "Citation source",
                                                    },
                                                ),
                                                pa.field(
                                                    "scores",
                                                    pa.map_(
                                                        pa.field(
                                                            "key",
                                                            pa.string(),
                                                            nullable=False,
                                                            metadata={
                                                                "comment": "Score key comment",
                                                                "doc": "Score key doc",
                                                            },
                                                        ),
                                                        pa.field(
                                                            "value",
                                                            pa.float64(),
                                                            nullable=True,
                                                            metadata={
                                                                "comment": "Score value comment",
                                                                "doc": "Score value doc",
                                                            },
                                                        ),
                                                    ),
                                                    nullable=True,
                                                    metadata={
                                                        "comment": "Scores map comment",
                                                        "doc": "Named confidence scores",
                                                    },
                                                ),
                                            ]
                                        ),
                                        nullable=False,
                                        metadata={
                                            "comment": "Evidence item comment",
                                            "doc": "Evidence item doc",
                                        },
                                    )
                                ),
                                nullable=False,
                                metadata={"comment": "Evidences list comment", "doc": "Supporting evidences"},
                            ),
                        ]
                    ),
                    nullable=False,
                    metadata={"comment": "Feature item comment", "doc": "Feature item doc"},
                )
            ),
            nullable=False,
            metadata={"comment": "Features list comment", "description": "Features list description"},
        ),
        pa.field(
            "annotations",
            pa.map_(
                pa.field(
                    "key",
                    pa.string(),
                    nullable=False,
                    metadata={
                        "comment": "Annotation key comment",
                        "doc": "Annotation key doc",
                    },
                ),
                pa.field(
                    "value",
                    pa.struct(
                        [
                            pa.field(
                                "note",
                                pa.string(),
                                nullable=False,
                                metadata={"comment": "Note comment", "doc": "Annotation text"},
                            ),
                            pa.field(
                                "weight",
                                pa.decimal128(6, 2),
                                nullable=True,
                                metadata={"comment": "Weight comment"},
                            ),
                            pa.field(
                                "tag_groups",
                                pa.list_(
                                    pa.field(
                                        "group_item",
                                        pa.list_(
                                            pa.field(
                                                "tag_item",
                                                pa.string(),
                                                nullable=False,
                                                metadata={
                                                    "comment": "Inner tag item comment",
                                                    "doc": "Inner tag item doc",
                                                },
                                            )
                                        ),
                                        nullable=True,
                                        metadata={
                                            "comment": "Outer group item comment",
                                            "doc": "Outer group item doc",
                                        },
                                    )
                                ),
                                nullable=False,
                                metadata={"comment": "Tag groups comment", "doc": "Nested tag groups"},
                            ),
                        ]
                    ),
                    nullable=False,
                    metadata={
                        "comment": "Annotation value comment",
                        "doc": "Annotation value doc",
                    },
                ),
            ),
            nullable=False,
            metadata={"comment": "Annotations map comment", "doc": "Keyed structured annotations"},
        ),
    ],
    metadata=DEEP_NESTED_TABLE_METADATA,
)

DEEP_NESTED_ROWS: Final[tuple[dict[str, Any], ...]] = (
    {
        "id": 10,
        "title": "kinase subunit",
        "organism": {
            "taxon_id": 9606,
            "lineage": {
                "rank": "species",
                "detail": {
                    "division_code": 5,
                    "synonyms": ["Homo sapiens", "human"],
                },
            },
        },
        "features": [
            {
                "kind": "domain",
                "evidences": [
                    {
                        "code": "ECO:0000269",
                        "source": {"db": "PubMed", "accession": "12345678"},
                        "scores": [("p_value", 0.01), ("posterior", None)],
                    },
                    {
                        "code": "ECO:0000255",
                        "source": None,
                        "scores": None,
                    },
                ],
            }
        ],
        "annotations": [
            ("function", {"note": "ATP binding", "weight": Decimal("12.50"), "tag_groups": [["go", "atp"], None]}),
            ("pathway", {"note": "signaling", "weight": None, "tag_groups": []}),
        ],
    },
    {
        "id": 20,
        "title": None,
        "organism": {
            "taxon_id": 562,
            "lineage": None,
        },
        "features": [],
        "annotations": [],
    },
)

EXPECTED_DEEP_NESTED_DLT_COLUMNS: Final[dict[str, dict[str, Any]]] = {
    "id": {"name": "id", "data_type": "bigint", "nullable": False},
    "title": {"name": "title", "data_type": "text", "nullable": True},
    "organism": {"name": "organism", "data_type": "json", "nullable": False},
    "features": {"name": "features", "data_type": "json", "nullable": False},
    "annotations": {"name": "annotations", "data_type": "json", "nullable": False},
}

EXPECTED_DEEP_NESTED_ICEBERG_SCHEMA: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True, doc="Entry primary key"),
    NestedField(field_id=2, name="title", field_type=StringType(), required=False, doc=None),
    NestedField(
        field_id=3,
        name="organism",
        field_type=StructType(
            NestedField(field_id=6, name="taxon_id", field_type=IntegerType(), required=True, doc="NCBI taxonomy ID"),
            NestedField(
                field_id=7,
                name="lineage",
                field_type=StructType(
                    NestedField(field_id=8, name="rank", field_type=StringType(), required=True, doc="Taxonomic rank"),
                    NestedField(
                        field_id=9,
                        name="detail",
                        field_type=StructType(
                            NestedField(
                                field_id=10,
                                name="division_code",
                                field_type=IntegerType(),
                                required=True,
                                doc=None,
                            ),
                            NestedField(
                                field_id=11,
                                name="synonyms",
                                field_type=ListType(
                                    element_id=12,
                                    element_type=StringType(),
                                    element_required=True,
                                ),
                                required=False,
                                doc="Taxon synonyms",
                            ),
                        ),
                        required=False,
                        doc="Deep lineage detail",
                    ),
                ),
                required=False,
                doc=None,
            ),
        ),
        required=True,
        doc="Source organism record",
    ),
    NestedField(
        field_id=4,
        name="features",
        field_type=ListType(
            element_id=13,
            element_type=StructType(
                NestedField(field_id=14, name="kind", field_type=StringType(), required=True, doc="Feature kind"),
                NestedField(
                    field_id=15,
                    name="evidences",
                    field_type=ListType(
                        element_id=16,
                        element_type=StructType(
                            NestedField(field_id=17, name="code", field_type=StringType(), required=True, doc=None),
                            NestedField(
                                field_id=18,
                                name="source",
                                field_type=StructType(
                                    NestedField(
                                        field_id=20,
                                        name="db",
                                        field_type=StringType(),
                                        required=True,
                                        doc="Database name",
                                    ),
                                    NestedField(
                                        field_id=21,
                                        name="accession",
                                        field_type=StringType(),
                                        required=False,
                                        doc=None,
                                    ),
                                ),
                                required=False,
                                doc="Citation source",
                            ),
                            NestedField(
                                field_id=19,
                                name="scores",
                                field_type=MapType(
                                    key_id=22,
                                    key_type=StringType(),
                                    value_id=23,
                                    value_type=DoubleType(),
                                    value_required=False,
                                ),
                                required=False,
                                doc="Named confidence scores",
                            ),
                        ),
                        element_required=True,
                    ),
                    required=True,
                    doc="Supporting evidences",
                ),
            ),
            element_required=True,
        ),
        required=True,
        doc=None,
    ),
    NestedField(
        field_id=5,
        name="annotations",
        field_type=MapType(
            key_id=24,
            key_type=StringType(),
            value_id=25,
            value_type=StructType(
                NestedField(field_id=26, name="note", field_type=StringType(), required=True, doc="Annotation text"),
                NestedField(field_id=27, name="weight", field_type=DecimalType(6, 2), required=False, doc=None),
                NestedField(
                    field_id=28,
                    name="tag_groups",
                    field_type=ListType(
                        element_id=29,
                        element_type=ListType(
                            element_id=30,
                            element_type=StringType(),
                            element_required=True,
                        ),
                        element_required=False,
                    ),
                    required=True,
                    doc="Nested tag groups",
                ),
            ),
            value_required=True,
        ),
        required=True,
        doc="Keyed structured annotations",
    ),
)

EXPECTED_DEEP_NESTED_ICEBERG_DOCS: Final[dict[str, str | None]] = {
    "id": "Entry primary key",
    "title": None,
    "organism": "Source organism record",
    "organism.taxon_id": "NCBI taxonomy ID",
    "organism.lineage": None,
    "organism.lineage.rank": "Taxonomic rank",
    "organism.lineage.detail": "Deep lineage detail",
    "organism.lineage.detail.division_code": None,
    "organism.lineage.detail.synonyms": "Taxon synonyms",
    "features": None,
    "features.element.kind": "Feature kind",
    "features.element.evidences": "Supporting evidences",
    "features.element.evidences.element.code": None,
    "features.element.evidences.element.source": "Citation source",
    "features.element.evidences.element.source.db": "Database name",
    "features.element.evidences.element.source.accession": None,
    "features.element.evidences.element.scores": "Named confidence scores",
    "annotations": "Keyed structured annotations",
    "annotations.value.note": "Annotation text",
    "annotations.value.weight": None,
    "annotations.value.tag_groups": "Nested tag groups",
}

EXPECTED_DEEP_NESTED_ICEBERG_SCAN_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False, metadata={"doc": "Entry primary key"}),
        pa.field("title", pa.string(), nullable=True),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field("taxon_id", pa.int32(), nullable=False, metadata={"doc": "NCBI taxonomy ID"}),
                    pa.field(
                        "lineage",
                        pa.struct(
                            [
                                pa.field("rank", pa.string(), nullable=False, metadata={"doc": "Taxonomic rank"}),
                                pa.field(
                                    "detail",
                                    pa.struct(
                                        [
                                            pa.field("division_code", pa.int32(), nullable=False),
                                            pa.field(
                                                "synonyms",
                                                pa.list_(pa.field("element", pa.string(), nullable=False)),
                                                nullable=True,
                                                metadata={"doc": "Taxon synonyms"},
                                            ),
                                        ]
                                    ),
                                    nullable=True,
                                    metadata={"doc": "Deep lineage detail"},
                                ),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
            metadata={"doc": "Source organism record"},
        ),
        pa.field(
            "features",
            pa.list_(
                pa.field(
                    "element",
                    pa.struct(
                        [
                            pa.field("kind", pa.string(), nullable=False, metadata={"doc": "Feature kind"}),
                            pa.field(
                                "evidences",
                                pa.list_(
                                    pa.field(
                                        "element",
                                        pa.struct(
                                            [
                                                pa.field("code", pa.string(), nullable=False),
                                                pa.field(
                                                    "source",
                                                    pa.struct(
                                                        [
                                                            pa.field(
                                                                "db",
                                                                pa.string(),
                                                                nullable=False,
                                                                metadata={"doc": "Database name"},
                                                            ),
                                                            pa.field("accession", pa.string(), nullable=True),
                                                        ]
                                                    ),
                                                    nullable=True,
                                                    metadata={"doc": "Citation source"},
                                                ),
                                                pa.field(
                                                    "scores",
                                                    pa.map_(
                                                        pa.field("key", pa.string(), nullable=False),
                                                        pa.field("value", pa.float64(), nullable=True),
                                                    ),
                                                    nullable=True,
                                                    metadata={"doc": "Named confidence scores"},
                                                ),
                                            ]
                                        ),
                                        nullable=False,
                                    )
                                ),
                                nullable=False,
                                metadata={"doc": "Supporting evidences"},
                            ),
                        ]
                    ),
                    nullable=False,
                )
            ),
            nullable=False,
        ),
        pa.field(
            "annotations",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field(
                    "value",
                    pa.struct(
                        [
                            pa.field("note", pa.string(), nullable=False, metadata={"doc": "Annotation text"}),
                            pa.field("weight", pa.decimal128(6, 2), nullable=True),
                            pa.field(
                                "tag_groups",
                                pa.list_(
                                    pa.field(
                                        "element",
                                        pa.list_(pa.field("element", pa.string(), nullable=False)),
                                        nullable=True,
                                    )
                                ),
                                nullable=False,
                                metadata={"doc": "Nested tag groups"},
                            ),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=False,
            metadata={"doc": "Keyed structured annotations"},
        ),
    ]
)


@dataclass(frozen=True, slots=True)
class ShapeExpectation:
    """Input schema, rows, and per-layer expectations for one ``TableShape``."""

    input_arrow_schema: pa.Schema
    table_metadata: dict[str, str]
    rows: tuple[dict[str, Any], ...]
    expected_dlt_columns: dict[str, dict[str, Any]]
    expected_iceberg_schema: IcebergSchema
    expected_iceberg_docs: dict[str, str | None]
    expected_iceberg_scan_arrow_schema: pa.Schema


SHAPE_EXPECTATIONS: Final[frozendict[TableShape, ShapeExpectation]] = frozendict(
    {
        TableShape.FLAT: ShapeExpectation(
            input_arrow_schema=FLAT_ARROW_SCHEMA,
            table_metadata=FLAT_TABLE_METADATA,
            rows=FLAT_ROWS,
            expected_dlt_columns=EXPECTED_FLAT_DLT_COLUMNS,
            expected_iceberg_schema=EXPECTED_FLAT_ICEBERG_SCHEMA,
            expected_iceberg_docs=EXPECTED_FLAT_ICEBERG_DOCS,
            expected_iceberg_scan_arrow_schema=EXPECTED_FLAT_ICEBERG_SCAN_ARROW_SCHEMA,
        ),
        TableShape.DEEP_NESTED: ShapeExpectation(
            input_arrow_schema=DEEP_NESTED_ARROW_SCHEMA,
            table_metadata=DEEP_NESTED_TABLE_METADATA,
            rows=DEEP_NESTED_ROWS,
            expected_dlt_columns=EXPECTED_DEEP_NESTED_DLT_COLUMNS,
            expected_iceberg_schema=EXPECTED_DEEP_NESTED_ICEBERG_SCHEMA,
            expected_iceberg_docs=EXPECTED_DEEP_NESTED_ICEBERG_DOCS,
            expected_iceberg_scan_arrow_schema=EXPECTED_DEEP_NESTED_ICEBERG_SCAN_ARROW_SCHEMA,
        ),
    }
)

CAMEL_FLAT_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entryId", pa.int64(), nullable=False, metadata={"comment": "ID comment", "doc": "Entry ID doc"}),
        pa.field("_rawCode", pa.string(), nullable=True, metadata={"comment": "Code comment"}),
    ],
    metadata={"comment": "Camel flat comment", "doc": "Camel flat doc"},
)

CAMEL_FLAT_ROWS: Final[tuple[dict[str, Any], ...]] = (
    {"entryId": 1, "_rawCode": "A1"},
    {"entryId": 2, "_rawCode": None},
)

EXPECTED_CAMEL_FLAT_PARQUET_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entry_id", pa.int64(), nullable=False, metadata={"comment": "ID comment", "doc": "Entry ID doc"}),
        pa.field("_raw_code", pa.string(), nullable=True, metadata={"comment": "Code comment"}),
    ],
    metadata={"comment": "Camel flat comment", "doc": "Camel flat doc"},
)

EXPECTED_CAMEL_FLAT_ROWS: Final[list[dict[str, Any]]] = [
    {"entry_id": 1, "_raw_code": "A1"},
    {"entry_id": 2, "_raw_code": None},
]

CAMEL_NESTED_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entryId", pa.int64(), nullable=False, metadata={"comment": "ID comment", "doc": "Entry ID doc"}),
        pa.field(
            "outerBlock",
            pa.struct(
                [
                    pa.field(
                        "camelChild",
                        pa.string(),
                        nullable=False,
                        metadata={"comment": "Child comment", "doc": "Child doc"},
                    ),
                    pa.field(
                        "deepList",
                        pa.list_(
                            pa.field(
                                "listElem",
                                pa.struct(
                                    [
                                        pa.field(
                                            "_innerCode",
                                            pa.int64(),
                                            nullable=False,
                                            metadata={"comment": "Inner comment", "doc": "Inner doc"},
                                        )
                                    ]
                                ),
                                nullable=False,
                                metadata={"comment": "Elem comment", "doc": "Elem doc"},
                            )
                        ),
                        nullable=False,
                        metadata={"comment": "Deep list comment"},
                    ),
                ]
            ),
            nullable=False,
            metadata={"comment": "Outer block comment", "doc": "Outer block doc"},
        ),
    ],
    metadata={"comment": "Camel nested comment", "doc": "Camel nested doc"},
)

CAMEL_NESTED_ROWS: Final[tuple[dict[str, Any], ...]] = (
    {"entryId": 1, "outerBlock": {"camelChild": "keepCase", "deepList": [{"_innerCode": 99}]}},
)

EXPECTED_CAMEL_NESTED_PARQUET_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("entry_id", pa.int64(), nullable=False, metadata={"comment": "ID comment", "doc": "Entry ID doc"}),
        CAMEL_NESTED_ARROW_SCHEMA.field("outerBlock").with_name("outer_block"),
    ],
    metadata={"comment": "Camel nested comment", "doc": "Camel nested doc"},
)

EXPECTED_CAMEL_NESTED_ROWS: Final[list[dict[str, Any]]] = [
    {"entry_id": 1, "outer_block": {"camelChild": "keepCase", "deepList": [{"_innerCode": 99}]}},
]


def decode_arrow_metadata(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    """Decode a PyArrow ``bytes -> bytes`` metadata dict into ``str -> str``.

    :param metadata: raw ``pa.Schema.metadata`` or ``pa.Field.metadata`` mapping
    :type metadata: dict[bytes, bytes] | None
    :return: UTF-8 decoded key/value mapping
    :rtype: dict[str, str]
    """
    if not metadata:
        return {}
    return {key.decode("utf-8"): value.decode("utf-8") for key, value in metadata.items()}


def with_parquet_compliant_list_elements(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` with ``pa.list_`` element fields renamed to ``"element"``, matching Parquet output.

    ``dlt`` writes Parquet with ``use_compliant_nested_type=True``, which standardizes ``pa.list_``
    child field names to ``"element"`` while keeping their nullability and ``pa.Field.metadata``.

    :param schema: input PyArrow schema
    :type schema: pa.Schema
    :return: schema with ``pa.list_`` child field names set to ``"element"``
    :rtype: pa.Schema
    """

    def _convert_field(field: pa.Field) -> pa.Field:
        dtype = field.type
        if pa.types.is_struct(dtype):
            return field.with_type(pa.struct([_convert_field(child) for child in dtype]))
        if pa.types.is_list(dtype):
            elem_field = _convert_field(dtype.value_field).with_name("element")
            return field.with_type(pa.list_(elem_field))
        if pa.types.is_large_list(dtype):
            elem_field = _convert_field(dtype.value_field).with_name("element")
            return field.with_type(pa.large_list(elem_field))
        if pa.types.is_map(dtype):
            return field.with_type(pa.map_(_convert_field(dtype.key_field), _convert_field(dtype.item_field)))
        return field

    return pa.schema([_convert_field(top_field) for top_field in schema], metadata=schema.metadata)


def extract_arrow_field_metadata(schema: pa.Schema) -> dict[str, dict[str, str]]:
    """Collect decoded ``pa.Field.metadata`` at every path in ``schema``.

    :param schema: PyArrow schema to inspect
    :type schema: pa.Schema
    :return: mapping from dotted field path (including list element and map key/value fields) to decoded metadata
    :rtype: dict[str, dict[str, str]]
    """
    collected: dict[str, dict[str, str]] = {}

    def _visit_field(field: pa.Field, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        collected[path] = decode_arrow_metadata(field.metadata)
        dtype = field.type
        if pa.types.is_struct(dtype):
            for child in dtype:
                _visit_field(child, path)
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            _visit_field(dtype.value_field, path)
        elif pa.types.is_map(dtype):
            _visit_field(dtype.key_field, path)
            _visit_field(dtype.item_field, path)

    for top_field in schema:
        _visit_field(top_field, "")
    return collected


def assert_arrow_schemas_equal_with_metadata(actual: pa.Schema, expected: pa.Schema) -> None:
    """Assert two PyArrow schemas match on names, types, nullability, and metadata at every depth.

    PyArrow's Parquet C++ reader names ``MapType``'s internal wrapper struct after the column rather
    than ``pa.map_``'s ``"entries"``, making ``Schema.equals(..., check_metadata=True)`` return
    ``False`` on schemas with ``pa.map_`` even when ``key_field``, ``item_field``, and all metadata match.

    :param actual: actual ``pa.Schema``
    :type actual: pa.Schema
    :param expected: expected ``pa.Schema``
    :type expected: pa.Schema
    """
    assert actual == expected
    assert decode_arrow_metadata(actual.metadata) == decode_arrow_metadata(expected.metadata)
    assert actual.names == expected.names

    def _assert_field_equal(actual_field: pa.Field, expected_field: pa.Field) -> None:
        assert actual_field.name == expected_field.name
        assert actual_field.nullable == expected_field.nullable
        assert decode_arrow_metadata(actual_field.metadata) == decode_arrow_metadata(expected_field.metadata)
        actual_type = actual_field.type
        expected_type = expected_field.type
        if pa.types.is_struct(actual_type):
            assert pa.types.is_struct(expected_type)
            assert len(actual_type) == len(expected_type)
            for actual_child, expected_child in zip(actual_type, expected_type, strict=True):
                _assert_field_equal(actual_child, expected_child)
        elif pa.types.is_list(actual_type) or pa.types.is_large_list(actual_type):
            assert type(actual_type) is type(expected_type)
            _assert_field_equal(actual_type.value_field, expected_type.value_field)
        elif pa.types.is_map(actual_type):
            assert pa.types.is_map(expected_type)
            assert actual_type.keys_sorted == expected_type.keys_sorted
            _assert_field_equal(actual_type.key_field, expected_type.key_field)
            _assert_field_equal(actual_type.item_field, expected_type.item_field)
        else:
            assert actual_field.equals(expected_field, check_metadata=True)

    for actual_top, expected_top in zip(actual, expected, strict=True):
        _assert_field_equal(actual_top, expected_top)


def extract_iceberg_field_docs(schema: IcebergSchema) -> dict[str, str | None]:
    """Collect ``NestedField.doc`` across root and nested struct fields in ``schema``.

    :param schema: Iceberg table schema
    :type schema: IcebergSchema
    :return: mapping from dotted field path to ``NestedField.doc``
    :rtype: dict[str, str | None]
    """
    docs: dict[str, str | None] = {}

    def _visit_type(iceberg_type: IcebergType, prefix: str) -> None:
        if isinstance(iceberg_type, StructType):
            for child in iceberg_type.fields:
                child_path = f"{prefix}.{child.name}" if prefix else child.name
                docs[child_path] = child.doc
                _visit_type(child.field_type, child_path)
        elif isinstance(iceberg_type, ListType):
            _visit_type(iceberg_type.element_type, f"{prefix}.element")
        elif isinstance(iceberg_type, MapType):
            _visit_type(iceberg_type.key_type, f"{prefix}.key")
            _visit_type(iceberg_type.value_type, f"{prefix}.value")

    _visit_type(schema.as_struct(), "")
    return docs


def build_dict_buffer_resource(
    rows: tuple[dict[str, Any], ...],
    schema: pa.Schema,
    table_name: str,
) -> DltResource:
    """Build a minimal ``@dlt.resource`` that yields ``pa.Table`` pages via ``DictBuffer``.

    :param rows: input row dicts matching ``schema``
    :type rows: tuple[dict[str, Any], ...]
    :param schema: target ``pa.Schema`` registered in ``ArrowSchemaRegistry``
    :type schema: pa.Schema
    :param table_name: target table name for ``DictBuffer`` routing
    :type table_name: str
    :return: configured ``DltResource``
    :rtype: DltResource
    """

    @dlt.resource(name=f"{table_name}_buffered")
    def _buffered_rows() -> Generator[DataItemWithMeta]:
        # max_items=1 exercises both per-item page emission and final flush
        buffer = DictBuffer(
            max_items=1,
            schema_map=ArrowSchemaRegistry(registry={table_name: schema}),
        )
        for row in rows[:-1]:
            yield from buffer.add_item(table_name, dict(row))
        if rows:
            buffer.add_items({table_name: []})
            yield from buffer.add_item(table_name, dict(rows[-1]))
        yield from buffer.flush()

    return _buffered_rows


def run_simple_pyarrow_pipeline(
    tmp_path: Path,
    *,
    rows: tuple[dict[str, Any], ...],
    schema: pa.Schema,
    input_mode: InputMode,
    loader_file_format: LoaderFileFormatEnum,
    table_name: str = TABLE_NAME,
) -> tuple[dlt.Pipeline, Path]:
    """Run a minimal ``dlt`` filesystem pipeline for a PyArrow schema and format.

    :param tmp_path: temporary directory for pipeline state and destination bucket
    :type tmp_path: Path
    :param rows: input rows matching ``schema``
    :type rows: tuple[dict[str, Any], ...]
    :param schema: PyArrow schema with table and field metadata
    :type schema: pa.Schema
    :param input_mode: ``DIRECT_TABLE`` or ``DICT_BUFFER``
    :type input_mode: InputMode
    :param loader_file_format: ``PARQUET`` or ``ICEBERG``
    :type loader_file_format: LoaderFileFormatEnum
    :param table_name: destination table name
    :type table_name: str
    :return: tuple of ``(pipeline, bucket_dir)``
    :rtype: tuple[dlt.Pipeline, Path]
    """
    pipelines_dir = tmp_path / "pipelines"
    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name=f"defs_{table_name}_{input_mode}_{loader_file_format}",
        destination=dlt.destinations.filesystem(bucket_url=str(bucket_dir)),
        dataset_name=DATASET_NAME,
        pipelines_dir=str(pipelines_dir),
    )

    run_kwargs: dict[str, Any] = (
        {"loader_file_format": str(LoaderFileFormatEnum.PARQUET), "table_format": str(LoaderFileFormatEnum.ICEBERG)}
        if loader_file_format == LoaderFileFormatEnum.ICEBERG
        else {"loader_file_format": str(loader_file_format)}
    )
    if loader_file_format == LoaderFileFormatEnum.ICEBERG:
        install_iceberg_catalog_cleanup()

    if input_mode == InputMode.DIRECT_TABLE:
        arrow_table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
        load_info = pipeline.run(arrow_table, table_name=table_name, **run_kwargs)
    else:
        resource = build_dict_buffer_resource(rows, schema, table_name)
        load_info = pipeline.run(resource, **run_kwargs)

    assert not load_info.has_failed_jobs
    assert len(load_info.loads_ids) == 1
    return pipeline, bucket_dir


def assert_dlt_schema_definitions(
    pipeline: dlt.Pipeline,
    *,
    input_schema: pa.Schema,
    expected_columns: dict[str, dict[str, Any]],
    table_name: str = TABLE_NAME,
) -> None:
    """Assert what ``pipeline.default_schema`` keeps and drops from ``input_schema``.

    :param pipeline: executed ``dlt.Pipeline``
    :type pipeline: dlt.Pipeline
    :param input_schema: source ``pa.Schema`` (after any expected top-level name normalization)
    :type input_schema: pa.Schema
    :param expected_columns: expected ``dlt`` column dicts excluding ``x-nested-type``
    :type expected_columns: dict[str, dict[str, Any]]
    :param table_name: expected ``dlt`` table name
    :type table_name: str
    """
    data_tables = [name for name in pipeline.default_schema.tables if not is_metadata_table(name)]
    assert data_tables == [table_name]

    table_def = pipeline.default_schema.tables[table_name]
    # dlt ignores pa.Schema.metadata; neither table description nor Iceberg table properties are set
    assert table_def.get("description") is None
    assert "x-iceberg-table-properties" not in table_def

    columns = table_def["columns"]
    assert list(columns) == list(expected_columns)

    for col_name, col_def in columns.items():
        # dlt ignores top-level pa.Field.metadata when building TTableSchemaColumns
        assert col_def.get("description") is None
        comparable = {k: v for k, v in col_def.items() if k != "x-nested-type"}
        assert comparable == expected_columns[col_name]

        input_field = input_schema.field(col_name)
        if pa.types.is_nested(input_field.type):
            nested_hint = col_def["x-nested-type"]
            assert isinstance(nested_hint, str)
            assert nested_hint.startswith("arrow-ipc:")
            # nested Arrow IPC type hint keeps inner field names, types, nullability, and inner pa.Field.metadata
            roundtripped_type = deserialize_type(nested_hint)
            assert roundtripped_type.equals(input_field.type, check_metadata=True)
        else:
            assert "x-nested-type" not in col_def


def assert_parquet_definitions(
    bucket_dir: Path,
    *,
    expected_schema: pa.Schema,
    expected_table_metadata: dict[str, str],
    expected_rows: list[dict[str, Any]],
    table_name: str = TABLE_NAME,
) -> None:
    """Assert Parquet output preserves schema metadata, recursive field metadata, types, and rows.

    :param bucket_dir: filesystem destination root directory
    :type bucket_dir: Path
    :param expected_schema: expected ``pa.Schema`` including table and field metadata
    :type expected_schema: pa.Schema
    :param expected_table_metadata: expected decoded ``pa.Schema.metadata``
    :type expected_table_metadata: dict[str, str]
    :param expected_rows: expected table rows
    :type expected_rows: list[dict[str, Any]]
    :param table_name: target table directory name
    :type table_name: str
    """
    dataset_dir = bucket_dir / DATASET_NAME
    table_dirs = [p.name for p in sorted(dataset_dir.iterdir()) if p.is_dir() and not is_metadata_table(p.name)]
    assert table_dirs == [table_name]

    parquet_files = sorted((dataset_dir / table_name).glob("*.parquet"))
    assert len(parquet_files) == 1

    expected_parquet_schema = with_parquet_compliant_list_elements(expected_schema)
    pq_schema = pq.read_schema(parquet_files[0])
    assert decode_arrow_metadata(pq_schema.metadata) == expected_table_metadata
    assert extract_arrow_field_metadata(pq_schema) == extract_arrow_field_metadata(expected_parquet_schema)
    assert_arrow_schemas_equal_with_metadata(pq_schema, expected_parquet_schema)

    loaded_rows = pq.read_table(parquet_files[0]).to_pylist()
    assert loaded_rows == expected_rows


def assert_iceberg_definitions(
    pipeline: dlt.Pipeline,
    bucket_dir: Path,
    *,
    expected_iceberg_schema: IcebergSchema,
    expected_iceberg_docs: dict[str, str | None],
    expected_scan_arrow_schema: pa.Schema,
    expected_rows: list[dict[str, Any]],
    table_name: str = TABLE_NAME,
) -> None:
    """Assert what Iceberg output keeps and drops across catalog metadata, Arrow scan, and data files.

    :param pipeline: executed ``dlt.Pipeline``
    :type pipeline: dlt.Pipeline
    :param bucket_dir: filesystem destination root directory
    :type bucket_dir: Path
    :param expected_iceberg_schema: expected ``IcebergSchema`` including ``NestedField.doc``
    :type expected_iceberg_schema: IcebergSchema
    :param expected_iceberg_docs: expected ``{dotted_path: doc}`` mapping across root and struct fields
    :type expected_iceberg_docs: dict[str, str | None]
    :param expected_scan_arrow_schema: expected ``table.scan().to_arrow().schema`` with metadata
    :type expected_scan_arrow_schema: pa.Schema
    :param expected_rows: expected table rows
    :type expected_rows: list[dict[str, Any]]
    :param table_name: target Iceberg table name
    :type table_name: str
    """
    iceberg_tables = get_iceberg_tables(pipeline)
    try:
        assert set(iceberg_tables) == {table_name}
        table = iceberg_tables[table_name]

        # pa.Schema.metadata is not forwarded to Iceberg table properties
        assert TABLE_METADATA_KEYS.isdisjoint(table.properties)

        iceberg_schema = table.schema()
        assert extract_iceberg_field_docs(iceberg_schema) == expected_iceberg_docs
        assert iceberg_schema == expected_iceberg_schema

        arrow_table = table.scan().to_arrow()
        scan_schema = arrow_table.schema
        assert decode_arrow_metadata(scan_schema.metadata) == {}
        assert extract_arrow_field_metadata(scan_schema) == extract_arrow_field_metadata(expected_scan_arrow_schema)
        assert_arrow_schemas_equal_with_metadata(scan_schema, expected_scan_arrow_schema)
        assert scan_schema.equals(expected_scan_arrow_schema, check_metadata=True)
        assert arrow_table.to_pylist() == expected_rows
    finally:
        for table in iceberg_tables.values():
            table.catalog.close()

    # parquet data files rewritten by PyIceberg drop pa.Schema.metadata and tag fields with PARQUET:field_id
    data_files = sorted((bucket_dir / DATASET_NAME / table_name / "data").glob("*.parquet"))
    assert len(data_files) == 1
    data_file_schema = pq.read_schema(data_files[0])
    assert decode_arrow_metadata(data_file_schema.metadata) == {}
    assert "PARQUET:field_id" in decode_arrow_metadata(data_file_schema.field(0).metadata)


@pytest.mark.parametrize("loader_file_format", LOADER_FORMATS, ids=[fmt.value for fmt in LOADER_FORMATS])
@pytest.mark.parametrize("table_shape", TABLE_SHAPES, ids=[shape.value for shape in TABLE_SHAPES])
@pytest.mark.parametrize("input_mode", INPUT_MODES, ids=[mode.value for mode in INPUT_MODES])
def test_dlt_pyarrow_table_and_column_definitions(
    tmp_path: Path,
    input_mode: InputMode,
    table_shape: TableShape,
    loader_file_format: LoaderFileFormatEnum,
) -> None:
    """Check what table and column definitions survive across direct vs DictBuffer, flat vs deep nested, and Parquet vs Iceberg."""
    spec = SHAPE_EXPECTATIONS[table_shape]
    # every field at every depth in the input schema carries non-empty metadata
    assert all(extract_arrow_field_metadata(spec.input_arrow_schema).values())
    pipeline, bucket_dir = run_simple_pyarrow_pipeline(
        tmp_path,
        rows=spec.rows,
        schema=spec.input_arrow_schema,
        input_mode=input_mode,
        loader_file_format=loader_file_format,
    )

    assert_dlt_schema_definitions(
        pipeline,
        input_schema=spec.input_arrow_schema,
        expected_columns=spec.expected_dlt_columns,
    )

    if loader_file_format == LoaderFileFormatEnum.PARQUET:
        assert_parquet_definitions(
            bucket_dir,
            expected_schema=spec.input_arrow_schema,
            expected_table_metadata=spec.table_metadata,
            expected_rows=list(spec.rows),
        )
    else:
        assert_iceberg_definitions(
            pipeline,
            bucket_dir,
            expected_iceberg_schema=spec.expected_iceberg_schema,
            expected_iceberg_docs=spec.expected_iceberg_docs,
            expected_scan_arrow_schema=spec.expected_iceberg_scan_arrow_schema,
            expected_rows=list(spec.rows),
        )


@pytest.mark.parametrize("loader_file_format", LOADER_FORMATS, ids=[fmt.value for fmt in LOADER_FORMATS])
@pytest.mark.parametrize("table_shape", TABLE_SHAPES, ids=[shape.value for shape in TABLE_SHAPES])
@pytest.mark.parametrize("input_mode", INPUT_MODES, ids=[mode.value for mode in INPUT_MODES])
def test_dlt_pyarrow_identifier_normalization_definitions(
    tmp_path: Path,
    input_mode: InputMode,
    table_shape: TableShape,
    loader_file_format: LoaderFileFormatEnum,
) -> None:
    """Check dlt snake_case normalization renames root Arrow columns while keeping nested struct field names and metadata rules."""
    is_flat = table_shape == TableShape.FLAT
    input_schema = CAMEL_FLAT_ARROW_SCHEMA if is_flat else CAMEL_NESTED_ARROW_SCHEMA
    input_rows = CAMEL_FLAT_ROWS if is_flat else CAMEL_NESTED_ROWS
    expected_parquet_schema = EXPECTED_CAMEL_FLAT_PARQUET_SCHEMA if is_flat else EXPECTED_CAMEL_NESTED_PARQUET_SCHEMA
    expected_rows = EXPECTED_CAMEL_FLAT_ROWS if is_flat else EXPECTED_CAMEL_NESTED_ROWS

    pipeline, bucket_dir = run_simple_pyarrow_pipeline(
        tmp_path,
        rows=input_rows,
        schema=input_schema,
        input_mode=input_mode,
        loader_file_format=loader_file_format,
    )

    expected_dlt_columns: dict[str, dict[str, Any]] = (
        {
            "entry_id": {"name": "entry_id", "data_type": "bigint", "nullable": False},
            "_raw_code": {"name": "_raw_code", "data_type": "text", "nullable": True},
        }
        if is_flat
        else {
            "entry_id": {"name": "entry_id", "data_type": "bigint", "nullable": False},
            "outer_block": {"name": "outer_block", "data_type": "json", "nullable": False},
        }
    )
    assert_dlt_schema_definitions(
        pipeline,
        input_schema=expected_parquet_schema,
        expected_columns=expected_dlt_columns,
    )

    if loader_file_format == LoaderFileFormatEnum.PARQUET:
        assert_parquet_definitions(
            bucket_dir,
            expected_schema=expected_parquet_schema,
            expected_table_metadata=decode_arrow_metadata(input_schema.metadata),
            expected_rows=expected_rows,
        )
    elif is_flat:
        assert_iceberg_definitions(
            pipeline,
            bucket_dir,
            expected_iceberg_schema=IcebergSchema(
                NestedField(field_id=1, name="entry_id", field_type=LongType(), required=True, doc="Entry ID doc"),
                NestedField(field_id=2, name="_raw_code", field_type=StringType(), required=False, doc=None),
            ),
            expected_iceberg_docs={"entry_id": "Entry ID doc", "_raw_code": None},
            expected_scan_arrow_schema=pa.schema(
                [
                    pa.field("entry_id", pa.int64(), nullable=False, metadata={"doc": "Entry ID doc"}),
                    pa.field("_raw_code", pa.string(), nullable=True),
                ]
            ),
            expected_rows=expected_rows,
        )
    else:
        assert_iceberg_definitions(
            pipeline,
            bucket_dir,
            expected_iceberg_schema=IcebergSchema(
                NestedField(field_id=1, name="entry_id", field_type=LongType(), required=True, doc="Entry ID doc"),
                NestedField(
                    field_id=2,
                    name="outer_block",
                    field_type=StructType(
                        NestedField(
                            field_id=3,
                            name="camelChild",
                            field_type=StringType(),
                            required=True,
                            doc="Child doc",
                        ),
                        NestedField(
                            field_id=4,
                            name="deepList",
                            field_type=ListType(
                                element_id=5,
                                element_type=StructType(
                                    NestedField(
                                        field_id=6,
                                        name="_innerCode",
                                        field_type=LongType(),
                                        required=True,
                                        doc="Inner doc",
                                    ),
                                ),
                                element_required=True,
                            ),
                            required=True,
                            doc=None,
                        ),
                    ),
                    required=True,
                    doc="Outer block doc",
                ),
            ),
            expected_iceberg_docs={
                "entry_id": "Entry ID doc",
                "outer_block": "Outer block doc",
                "outer_block.camelChild": "Child doc",
                "outer_block.deepList": None,
                "outer_block.deepList.element._innerCode": "Inner doc",
            },
            expected_scan_arrow_schema=pa.schema(
                [
                    pa.field("entry_id", pa.int64(), nullable=False, metadata={"doc": "Entry ID doc"}),
                    pa.field(
                        "outer_block",
                        pa.struct(
                            [
                                pa.field("camelChild", pa.string(), nullable=False, metadata={"doc": "Child doc"}),
                                pa.field(
                                    "deepList",
                                    pa.list_(
                                        pa.field(
                                            "element",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "_innerCode",
                                                        pa.int64(),
                                                        nullable=False,
                                                        metadata={"doc": "Inner doc"},
                                                    )
                                                ]
                                            ),
                                            nullable=False,
                                        )
                                    ),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                        metadata={"doc": "Outer block doc"},
                    ),
                ]
            ),
            expected_rows=expected_rows,
        )
