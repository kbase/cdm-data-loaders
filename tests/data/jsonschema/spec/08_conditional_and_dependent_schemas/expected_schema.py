"""Expected PyArrow schema for 08_conditional_and_dependent_schemas."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("specimen_type", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("sequencing_depth", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("depth_unit", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "platform",
            pa.string(),
            nullable=True,
            metadata={"comment": "Sequencing platform when depth is reported.", "jsonschema_type": "string"},
        ),
        pa.field(
            "strain_name",
            pa.string(),
            nullable=True,
            metadata={"comment": "Cultured isolate strain identifier.", "jsonschema_type": "string"},
        ),
        pa.field(
            "biome",
            pa.string(),
            nullable=True,
            metadata={"comment": "Environmental biome for metagenome samples.", "jsonschema_type": "string"},
        ),
    ]
)
