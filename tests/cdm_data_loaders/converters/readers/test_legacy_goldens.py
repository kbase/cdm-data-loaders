"""Fixed legacy outputs for the four converter routes before facade rewiring."""

import json
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.schema import Schema

from cdm_data_loaders.converters.readers.dlt import DltReader
from cdm_data_loaders.converters.readers.iceberg import IcebergReader
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

GOLDEN_PATH = Path(__file__).parents[3] / "data" / "converters" / "ir" / "legacy_outputs.json"
CASES: list[dict[str, Any]] = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_read_pass_golden_inputs(case: dict[str, Any]) -> None:
    """Each golden source is readable as a structural IR tree."""
    source = case["input"]
    if case["route"].startswith("json-"):
        document = JsonSchemaReader().read(source)
        assert tuple(prop.name for prop in document.root.properties) == tuple(source["properties"])
    elif case["route"] == "dlt-json":
        options = {key: value for key, value in case["options"].items() if key != "preserve_unknown_hints"}
        documents = DltReader(**options).read(source)
        assert tuple(documents) == tuple(case["expected"])
    else:
        schema = Schema.model_validate(source["schema"])
        document = IcebergReader().read(schema)
        assert tuple(prop.name for prop in document.root.properties) == tuple(field.name for field in schema.fields)
