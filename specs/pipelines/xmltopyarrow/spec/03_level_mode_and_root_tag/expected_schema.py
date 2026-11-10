"""Expected PyArrow schemas for 03_level_mode_and_root_tag."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("pmid", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("pages", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
    ]
)

EXPECTED_RETRACTION_SCHEMA = pa.schema(
    [
        pa.field("pmid", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("reason", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)

EXPECTED_SCHEMAS = {
    "article": EXPECTED_SCHEMA,
    "retraction": EXPECTED_RETRACTION_SCHEMA,
}
