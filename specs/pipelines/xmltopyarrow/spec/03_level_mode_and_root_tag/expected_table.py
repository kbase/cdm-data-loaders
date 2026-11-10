"""Expected PyArrow tables for 03_level_mode_and_root_tag."""

import pyarrow as pa
from expected_schema import EXPECTED_RETRACTION_SCHEMA, EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"pmid": "10001", "title": "CRISPR Defense Systems", "pages": 12},
        {"pmid": "10002", "title": "Metagenomic Binning", "pages": None},
    ],
    schema=EXPECTED_SCHEMA,
)

EXPECTED_RETRACTION_TABLE = pa.Table.from_pylist(
    [
        {"pmid": "09999", "reason": "Contaminated reagent batch"},
    ],
    schema=EXPECTED_RETRACTION_SCHEMA,
)

EXPECTED_TABLES = {
    "article": EXPECTED_TABLE,
    "retraction": EXPECTED_RETRACTION_TABLE,
}
