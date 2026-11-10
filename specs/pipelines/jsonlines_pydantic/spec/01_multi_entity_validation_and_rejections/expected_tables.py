"""Expected PyArrow tables for jsonlines_pydantic 01_multi_entity_validation_and_rejections."""

import json

import pyarrow as pa
from pydantic import ValidationError

from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from specs.pipelines.jsonlines_pydantic.sample_registry import InstrumentModel, SampleModel

CONVERTER = PydanticToPyArrow()
SAMPLES_SCHEMA = CONVERTER.convert(SampleModel)
INSTRUMENTS_SCHEMA = CONVERTER.convert(InstrumentModel)


def _sample_validation_error() -> str:
    try:
        SampleModel.model_validate({"sample_id": "s-bad-model", "ph": "not-a-float"})
    except ValidationError as exc:
        return json.dumps(exc.errors(), default=str)
    return ""


EXPECTED_TABLES: dict[str, pa.Table] = {
    "samples": pa.Table.from_pylist(
        [
            {"sample_id": "s-1", "ph": 6.5, "biome": "terrestrial", "tags": ["soil", "core"]},
            {"sample_id": "s-2", "ph": 7.8, "biome": "marine", "tags": ["water"]},
        ],
        schema=SAMPLES_SCHEMA,
    ),
    "instruments": pa.Table.from_pylist(
        [
            {"instrument_id": "inst-1", "platform": "Illumina", "active": True},
        ],
        schema=INSTRUMENTS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "samples",
                "source_file": "samples/samples.jsonl",
                "line_no": 3,
                "raw_record": '{"sample_id": "s-bad-json", "ph": }',
                "error_detail": "Expecting value: line 1 column 35 (char 34)",
            },
            {
                "table_name": "samples",
                "source_file": "samples/samples.jsonl",
                "line_no": 4,
                "raw_record": '{"sample_id": "s-bad-model", "ph": "not-a-float"}',
                "error_detail": _sample_validation_error(),
            },
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
