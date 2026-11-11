"""Expected PyArrow schema for 05_md5_sidecar_preprocessing_and_rejected."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("pmid", pa.int64(), nullable=False, metadata={"xsd_type": "xs:integer"}),
        pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)
