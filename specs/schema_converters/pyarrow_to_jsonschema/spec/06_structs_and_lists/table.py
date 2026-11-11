"""Sample PyArrow table for 06_structs_and_lists."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "feature_id": "CDS_0001",
            "location": {"contig": "chr1", "start": 100, "end": 950, "strand": "+"},
            "go_terms": ["GO:0003677", "GO:0006355"],
            "nullable_scores": [10, None, 42],
            "aliases_view": ["dnaA", "b3702"],
            "notes_large_view": ["verified", None],
            "rgb_triplet": [255, 128, 0],
            "score_matrix": [[1, 2], [3, 4, 5]],
        },
        {
            "feature_id": "CDS_0002",
            "location": {"contig": "chr2", "start": 2050, "end": 3100, "strand": None},
            "go_terms": [],
            "nullable_scores": None,
            "aliases_view": [],
            "notes_large_view": None,
            "rgb_triplet": [0, 64, 128],
            "score_matrix": [[]],
        },
    ],
    schema=SCHEMA,
)
