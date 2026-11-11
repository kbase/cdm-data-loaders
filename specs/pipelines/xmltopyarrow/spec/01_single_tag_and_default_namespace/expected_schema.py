"""Expected PyArrow schema for 01_single_tag_and_default_namespace (xml_tag='item')."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_active", pa.bool_(), nullable=False, metadata={"xsd_type": "xs:boolean"}),
        pa.field(
            "name",
            pa.string(),
            nullable=False,
            metadata={"comment": "Display name of the catalog item.", "xsd_type": "xs:string"},
        ),
        pa.field("quantity", pa.int64(), nullable=False, metadata={"xsd_type": "xs:integer"}),
        pa.field("price", pa.float64(), nullable=False, metadata={"xsd_type": "xs:double"}),
        pa.field("released", pa.date32(), nullable=True, metadata={"xsd_type": "xs:date"}),
        pa.field("notes", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "tag",
            pa.list_(
                pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
            ),
            nullable=True,
        ),
    ]
)
