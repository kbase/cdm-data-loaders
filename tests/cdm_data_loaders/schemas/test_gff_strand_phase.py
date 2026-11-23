"""Check that the GFF schema's ``strand`` and ``phase`` choices restrict values."""

import json
from pathlib import Path
from typing import Any, Final

import jsonschema
import pyarrow as pa
import pytest

import cdm_data_loaders.schemas
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import convert_xsv_schema_to_pyarrow
from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema

GFF_SCHEMA_PATH: Final[Path] = Path(cdm_data_loaders.schemas.__file__).parent / "gff" / "gff.schema.json"
GFF_SCHEMA: Final[dict[str, Any]] = json.loads(GFF_SCHEMA_PATH.read_text())


@pytest.mark.parametrize(
    ("column", "value", "is_valid"),
    [
        ("strand", "+", True),
        ("strand", "-", True),
        ("strand", ".", True),
        ("strand", "?", True),
        ("strand", "x", False),
        ("phase", 0, True),
        ("phase", 1, True),
        ("phase", 2, True),
        ("phase", ".", True),
        ("phase", 7, False),
        ("phase", "x", False),
    ],
    ids=[
        "strand_plus",
        "strand_minus",
        "strand_unstranded",
        "strand_unknown",
        "strand_x_rejected",
        "phase_0",
        "phase_1",
        "phase_2",
        "phase_dot",
        "phase_7_rejected",
        "phase_x_rejected",
    ],
)
def test_gff_schema_strand_and_phase_values(column: str, value: str | int, is_valid: bool) -> None:
    """Only the values the GFF3 specification allows pass the column's subschema."""
    errors = list(jsonschema.Draft202012Validator(GFF_SCHEMA["properties"][column]).iter_errors(value))
    assert (not errors) is is_valid


def test_convert_xsv_schema_to_pyarrow_gff_schema() -> None:
    """The xsv pipeline's PyArrow converter accepts the GFF schema and types strand and phase as strings."""
    pyarrow_schema = convert_xsv_schema_to_pyarrow(ValidatedXsvSchema(jsonschema=GFF_SCHEMA))
    for column in ("strand", "phase"):
        field = pyarrow_schema.field(column)
        assert (field.type, field.nullable) == (pa.string(), False)
