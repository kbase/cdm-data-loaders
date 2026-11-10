"""Expected PyArrow schema for 04_multi_file_gzip_and_subdirs (xml_tag='entry')."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("score", pa.float64(), nullable=False, metadata={"xsd_type": "xs:double"}),
    ]
)
