"""Expected PyArrow tables for jsonlines_pydantic 02_md5_sidecar_preprocessing."""

import pyarrow as pa

from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from specs.pipelines.jsonlines_pydantic.sample_registry import SampleModel

SAMPLES_SCHEMA = PydanticToPyArrow().convert(SampleModel)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "samples": pa.Table.from_pylist(
        [
            {"sample_id": "s-10", "ph": 7.1, "biome": "aquatic", "tags": ["lake"]},
        ],
        schema=SAMPLES_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "samples",
                "source_file": "samples/bad.jsonl",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for samples/bad.jsonl: "
                    "expected 00000000000000000000000000000000, got d8981895f8c957b2edf853991bd4f5c4"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
