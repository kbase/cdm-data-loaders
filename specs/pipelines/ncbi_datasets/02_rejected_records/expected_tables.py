"""Expected PyArrow tables for ncbi_datasets 02_rejected_records."""

import pyarrow as pa

from cdm_data_loaders.schemas.ncbi_datasets.schema import (
    ANNOTATION_REPORT_SCHEMA,
    DATASET_REPORT_SCHEMA,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EXPECTED_TABLES: dict[str, pa.Table] = {
    "annotation_report": pa.Table.from_pylist(
        [
            {
                "reports": [
                    {
                        "annotation": {"name": "integrase/recombinase", "symbol": "int"},
                        "row_id": "1",
                    }
                ],
                "total_count": 1,
            }
        ],
        schema=ANNOTATION_REPORT_SCHEMA,
    ),
    "dataset_report": pa.Table.from_pylist(
        [
            {
                "accession": "GCF_000005845.2",
                "source_database": "SOURCE_DATABASE_REFSEQ",
            }
        ],
        schema=DATASET_REPORT_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "annotation_report",
                "source_file": "annotation_report/batch.jsonl",
                "line_no": 3,
                "raw_record": '{"total_count":"not-an-int"}',
                "error_detail": '["\'not-an-int\' is not of type \'integer\'"]',
            },
            {
                "table_name": "dataset_report",
                "source_file": "dataset_report/batch.jsonl",
                "line_no": 2,
                "raw_record": "{malformed json",
                "error_detail": "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
            },
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
