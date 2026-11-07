"""Expected PyArrow table for 09_annotated_and_custom_types."""

from decimal import Decimal

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "accession": "GCF_000005845.2",
            "overridden_accession": "GCA_000005845.2",
            "coverage": Decimal("125.375"),
            "quality_phred": 40,
        },
        {
            "accession": "GCF_000009045.1",
            "overridden_accession": "GCA_000009045.1",
            "coverage": Decimal("50.000"),
            "quality_phred": 35,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
