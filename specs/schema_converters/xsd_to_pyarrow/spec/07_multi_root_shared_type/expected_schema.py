"""Expected PyArrow schema for 07_multi_root_shared_type (xml_tag='entry')."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)
