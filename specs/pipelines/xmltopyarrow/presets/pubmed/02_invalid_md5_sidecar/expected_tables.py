"""Expected PyArrow tables for pubmed 02_invalid_md5_sidecar."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EXPECTED_TABLES: dict[str, pa.Table] = {
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": None,
                "source_file": "corrupt_sample.xml.gz",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for corrupt_sample.xml.gz: "
                    "expected 00000000000000000000000000000000, got 0db850c2bce102b7757053a174fed330"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
