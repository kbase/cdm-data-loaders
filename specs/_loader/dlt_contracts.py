"""Static PyArrow probe schemas and dlt normalization contracts shared by docs macros and tests."""

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import BaseModel, ConfigDict
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

from cdm_data_loaders.utils.arrow_schema import decode_field_metadata

PYARROW_PROBE_TABLE_NAME: Final[str] = "assay_record"
PYARROW_PROBE_DATASET_NAME: Final[str] = "pyarrow_definitions_check"
TABLE_METADATA_KEYS: Final[frozenset[str]] = frozenset({"comment", "description", "doc", "title"})


class TableShape(StrEnum):
    """Structural shape of the input PyArrow schema."""

    FLAT = "flat"
    DEEP_NESTED = "deep_nested"


TABLE_SHAPES: Final[tuple[TableShape, ...]] = (TableShape.FLAT, TableShape.DEEP_NESTED)

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


class ShapeExpectation(BaseModel):
    """Input schema, rows, and per-layer expectations for one ``TableShape``."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

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

    :param metadata: Raw ``pa.Schema.metadata`` or ``pa.Field.metadata`` mapping.
    :type metadata: dict[bytes, bytes] | None
    :return: UTF-8 decoded key/value mapping.
    :rtype: dict[str, str]
    """
    return decode_field_metadata(metadata)


def with_parquet_compliant_list_elements(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` with ``pa.list_`` element fields renamed to ``"element"``, matching Parquet output.

    :param schema: Input PyArrow schema.
    :type schema: pa.Schema
    :return: Schema with ``pa.list_`` child field names set to ``"element"``.
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

    :param schema: PyArrow schema to inspect.
    :type schema: pa.Schema
    :return: Mapping from dotted field path to decoded metadata.
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


NESTED_TABLE_NAME: Final[str] = "widget"
NESTED_DATASET_NAME: Final[str] = "nested_check_data"

NESTED_ROWS: Final[tuple[dict[str, Any], ...]] = (
    {
        "id": 1,
        "name": "alpha",
        "address": {"street": ["Kaiser", "Permanente"], "city": "Berlin", "zip": "10115"},
        "tags": ["a", "b", "c"],
        "children": [{"k": "x", "v": 1}, {"k": "y", "v": 2}],
        "metrics": [("score", 98.5)],
    },
    {
        "id": 2,
        "name": "beta",
        "address": {"street": ["Marco Polo"], "city": "Turin", "zip": "10121"},
        "tags": ["d"],
        "children": [],
        "metrics": [("score", 72.0)],
    },
)

NESTED_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field(
            "address",
            pa.struct([("street", pa.list_(pa.string())), ("city", pa.string()), ("zip", pa.string())]),
        ),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("children", pa.list_(pa.struct([("k", pa.string()), ("v", pa.int64())]))),
        pa.field("metrics", pa.map_(pa.string(), pa.float64())),
    ]
)

COMPLEX_COLUMNS: Final[tuple[str, ...]] = ("address", "tags", "children", "metrics")

EXPECTED_JSON_STRING_ROWS: Final[list[dict[str, Any]]] = [
    {
        "id": 1,
        "name": "alpha",
        "address": '{"street":["Kaiser","Permanente"],"city":"Berlin","zip":"10115"}',
        "tags": '["a","b","c"]',
        "children": '[{"k":"x","v":1},{"k":"y","v":2}]',
        "metrics": '[["score",98.5]]',
    },
    {
        "id": 2,
        "name": "beta",
        "address": '{"street":["Marco Polo"],"city":"Turin","zip":"10121"}',
        "tags": '["d"]',
        "children": "[]",
        "metrics": '[["score",72.0]]',
    },
]

EXPECTED_PARSED_JSON_ROWS: Final[list[dict[str, Any]]] = [
    {
        "id": 1,
        "name": "alpha",
        "address": {"street": ["Kaiser", "Permanente"], "city": "Berlin", "zip": "10115"},
        "tags": ["a", "b", "c"],
        "children": [{"k": "x", "v": 1}, {"k": "y", "v": 2}],
        "metrics": [["score", 98.5]],
    },
    {
        "id": 2,
        "name": "beta",
        "address": {"street": ["Marco Polo"], "city": "Turin", "zip": "10121"},
        "tags": ["d"],
        "children": [],
        "metrics": [["score", 72.0]],
    },
]

EXPECTED_DLT_COLUMNS_DICT_NESTING_0: Final[dict[str, dict[str, str]]] = {
    NESTED_TABLE_NAME: {
        "id": "bigint",
        "name": "text",
        "address": "json",
        "tags": "json",
        "children": "json",
        "metrics": "json",
        "_dlt_load_id": "text",
        "_dlt_id": "text",
    }
}

EXPECTED_DLT_COLUMNS_PYARROW: Final[dict[str, dict[str, str]]] = {
    NESTED_TABLE_NAME: {
        "id": "bigint",
        "name": "text",
        "address": "json",
        "tags": "json",
        "children": "json",
        "metrics": "json",
    }
}

EXPECTED_DLT_COLUMNS_DICT_UNNESTED: Final[dict[str, dict[str, str]]] = {
    "widget": {
        "id": "bigint",
        "name": "text",
        "address__city": "text",
        "address__zip": "text",
        "_dlt_load_id": "text",
        "_dlt_id": "text",
    },
    "widget__address__street": {
        "value": "text",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__children": {
        "k": "text",
        "v": "bigint",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__metrics": {
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__metrics__list": {
        "value": "text",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__tags": {
        "value": "text",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
}

EXPECTED_ARROW_SCHEMA_DICT_NESTING_0: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("address", pa.string()),
        pa.field("tags", pa.string()),
        pa.field("children", pa.string()),
        pa.field("metrics", pa.string()),
        pa.field("_dlt_load_id", pa.string(), nullable=False),
        pa.field("_dlt_id", pa.string(), nullable=False),
    ]
)

CHILD_VALUE_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("value", pa.string()),
        pa.field("_dlt_parent_id", pa.string(), nullable=False),
        pa.field("_dlt_list_idx", pa.int64(), nullable=False),
        pa.field("_dlt_id", pa.string(), nullable=False),
    ]
)

EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED: Final[frozendict[str, pa.Schema]] = frozendict(
    {
        "widget": pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("address__city", pa.string()),
                pa.field("address__zip", pa.string()),
                pa.field("_dlt_load_id", pa.string(), nullable=False),
                pa.field("_dlt_id", pa.string(), nullable=False),
            ]
        ),
        "widget__address__street": CHILD_VALUE_ARROW_SCHEMA,
        "widget__children": pa.schema(
            [
                pa.field("k", pa.string()),
                pa.field("v", pa.int64()),
                pa.field("_dlt_parent_id", pa.string(), nullable=False),
                pa.field("_dlt_list_idx", pa.int64(), nullable=False),
                pa.field("_dlt_id", pa.string(), nullable=False),
            ]
        ),
        "widget__metrics": pa.schema(
            [
                pa.field("_dlt_parent_id", pa.string(), nullable=False),
                pa.field("_dlt_list_idx", pa.int64(), nullable=False),
                pa.field("_dlt_id", pa.string(), nullable=False),
            ]
        ),
        "widget__metrics__list": CHILD_VALUE_ARROW_SCHEMA,
        "widget__tags": CHILD_VALUE_ARROW_SCHEMA,
    }
)

EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="address", field_type=StringType(), required=False),
    NestedField(field_id=4, name="tags", field_type=StringType(), required=False),
    NestedField(field_id=5, name="children", field_type=StringType(), required=False),
    NestedField(field_id=6, name="metrics", field_type=StringType(), required=False),
    NestedField(field_id=7, name="_dlt_load_id", field_type=StringType(), required=True),
    NestedField(field_id=8, name="_dlt_id", field_type=StringType(), required=True),
)

CHILD_VALUE_ICEBERG_SCHEMA: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="value", field_type=StringType(), required=False),
    NestedField(field_id=2, name="_dlt_parent_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="_dlt_list_idx", field_type=LongType(), required=True),
    NestedField(field_id=4, name="_dlt_id", field_type=StringType(), required=True),
)

EXPECTED_ICEBERG_SCHEMAS_DICT_UNNESTED: Final[frozendict[str, IcebergSchema]] = frozendict(
    {
        "widget": IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
            NestedField(field_id=3, name="address__city", field_type=StringType(), required=False),
            NestedField(field_id=4, name="address__zip", field_type=StringType(), required=False),
            NestedField(field_id=5, name="_dlt_load_id", field_type=StringType(), required=True),
            NestedField(field_id=6, name="_dlt_id", field_type=StringType(), required=True),
        ),
        "widget__address__street": CHILD_VALUE_ICEBERG_SCHEMA,
        "widget__children": IcebergSchema(
            NestedField(field_id=1, name="k", field_type=StringType(), required=False),
            NestedField(field_id=2, name="v", field_type=LongType(), required=False),
            NestedField(field_id=3, name="_dlt_parent_id", field_type=StringType(), required=True),
            NestedField(field_id=4, name="_dlt_list_idx", field_type=LongType(), required=True),
            NestedField(field_id=5, name="_dlt_id", field_type=StringType(), required=True),
        ),
        "widget__metrics": IcebergSchema(
            NestedField(field_id=1, name="_dlt_parent_id", field_type=StringType(), required=True),
            NestedField(field_id=2, name="_dlt_list_idx", field_type=LongType(), required=True),
            NestedField(field_id=3, name="_dlt_id", field_type=StringType(), required=True),
        ),
        "widget__metrics__list": CHILD_VALUE_ICEBERG_SCHEMA,
        "widget__tags": CHILD_VALUE_ICEBERG_SCHEMA,
    }
)

EXPECTED_ICEBERG_SCHEMA_PYARROW: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(
        field_id=3,
        name="address",
        field_type=StructType(
            NestedField(
                field_id=7,
                name="street",
                field_type=ListType(element_id=10, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(field_id=8, name="city", field_type=StringType(), required=False),
            NestedField(field_id=9, name="zip", field_type=StringType(), required=False),
        ),
        required=False,
    ),
    NestedField(
        field_id=4,
        name="tags",
        field_type=ListType(element_id=11, element_type=StringType(), element_required=False),
        required=False,
    ),
    NestedField(
        field_id=5,
        name="children",
        field_type=ListType(
            element_id=12,
            element_type=StructType(
                NestedField(field_id=13, name="k", field_type=StringType(), required=False),
                NestedField(field_id=14, name="v", field_type=LongType(), required=False),
            ),
            element_required=False,
        ),
        required=False,
    ),
    NestedField(
        field_id=6,
        name="metrics",
        field_type=MapType(
            key_id=15,
            key_type=StringType(),
            value_id=16,
            value_type=DoubleType(),
            value_required=False,
        ),
        required=False,
    ),
)


class NestingPlacement(StrEnum):
    """Where ``max_table_nesting=0`` is configured (or ``UNSET`` when omitted everywhere)."""

    RESOURCE_AND_TRANSFORMER = "resource_and_transformer"
    TRANSFORMER_ONLY = "transformer_only"
    DESTINATION = "destination"
    RESOURCE_ONLY = "resource_only"
    UNSET = "unset"


NESTING_ZERO_PLACEMENTS: Final[tuple[NestingPlacement, ...]] = (
    NestingPlacement.RESOURCE_AND_TRANSFORMER,
    NestingPlacement.TRANSFORMER_ONLY,
    NestingPlacement.DESTINATION,
)

UNNESTED_PLACEMENTS: Final[tuple[NestingPlacement, ...]] = (
    NestingPlacement.UNSET,
    NestingPlacement.RESOURCE_ONLY,
)

ALL_NESTING_PLACEMENTS: Final[tuple[NestingPlacement, ...]] = (
    *NESTING_ZERO_PLACEMENTS,
    *UNNESTED_PLACEMENTS,
)
