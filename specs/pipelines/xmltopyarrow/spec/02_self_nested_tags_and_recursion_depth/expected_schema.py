"""Expected PyArrow schemas for 02_self_nested_tags_and_recursion_depth (xml_tag='category')."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "category",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                            pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                            pa.field(
                                "category",
                                pa.list_(
                                    pa.field("item", pa.string(), nullable=True),
                                ),
                                nullable=True,
                            ),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
    ]
)

EXPECTED_SCHEMA_DEPTH_1 = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "category",
            pa.list_(
                pa.field("item", pa.string(), nullable=True),
            ),
            nullable=True,
        ),
    ]
)
