"""PyArrow schema and reusable struct components for UniRef (xml_tag='entry')."""

from typing import Final

import pyarrow as pa

from cdm_data_loaders.converters.emitters.pyarrow import assemble_pyarrow_schema

PROPERTY_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_value", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)

DB_REFERENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_type",
            pa.string(),
            nullable=False,
            metadata={
                "comment": "The name of the database this cross-reference is referring to.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "_id", pa.string(), nullable=False, metadata={"comment": "The ID referred to.", "xsd_type": "xs:string"}
        ),
        pa.field("property", pa.list_(pa.field("item", PROPERTY_STRUCT, nullable=True)), nullable=True),
    ]
)

SEQUENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_length", pa.int32(), nullable=False, metadata={"xsd_type": "xs:int"}),
        pa.field("_checksum", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

MEMBER_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("dbReference", DB_REFERENCE_STRUCT, nullable=False, metadata={"comment": "Database cross-references"}),
        pa.field("sequence", SEQUENCE_STRUCT, nullable=True),
    ]
)

ENTRY_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_id", pa.string(), nullable=False, metadata={"comment": "The entry identifier.", "xsd_type": "xs:ID"}
        ),
        pa.field(
            "_updated",
            pa.date32(),
            nullable=False,
            metadata={"comment": "The update date for the entry.", "xsd_type": "xs:date"},
        ),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("property", pa.list_(pa.field("item", PROPERTY_STRUCT, nullable=True)), nullable=True),
        pa.field(
            "representativeMember",
            MEMBER_STRUCT,
            nullable=False,
            metadata={"comment": "UniRef50/90/100 cluster members"},
        ),
        pa.field(
            "member",
            pa.list_(pa.field("item", MEMBER_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "UniRef50/90/100 cluster members"},
        ),
    ]
)

SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(ENTRY_STRUCT)
SCHEMAS: Final[dict[str, pa.Schema]] = {"entry": SCHEMA}
