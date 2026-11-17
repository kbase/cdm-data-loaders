"""PyArrow schemas and metadata constants for the UniProt ID mapping pipeline."""

from pathlib import Path
from typing import Final

import pyarrow as pa

IDMAPPING_SCHEMA_FILE: Final[Path] = Path(__file__).resolve().parent / "idmapping.schema.json"

ID_MAPPING_TABLE_NAME: Final[str] = "id_mapping"
RAW_ID_MAPPING_TABLE_NAME: Final[str] = "raw_id_mapping"

RAW_ID_MAPPING_DESCRIPTION: Final[str] = "raw ID mapping data from UniProt"
ID_MAPPING_DESCRIPTION: Final[str] = "Standardised mapping data from UniProt"

UNIPROT_ID_COL: Final[str] = "uniprot_id"
DB_REFERENCE_TYPE_COL: Final[str] = "db_reference_type"
DB_REFERENCE_ID_COL: Final[str] = "db_reference_id"
DB_REFERENCE_COL: Final[str] = "db_reference"
SOURCE_COL: Final[str] = "source"

RAW_UNIPROT_ID_DESCRIPTION: Final[str] = "Identifier for the protein in the UniProt database."
DB_REFERENCE_TYPE_DESCRIPTION: Final[str] = (
    "Abbreviation for the database from which the internal ID in the `db_reference_id` column is derived."
)
DB_REFERENCE_ID_DESCRIPTION: Final[str] = "Internal ID at the database in the `db_reference_type` column."

UNIPROT_ID_DESCRIPTION: Final[str] = "Standardised identifier for the protein in the UniProt database"
DB_REFERENCE_DESCRIPTION: Final[str] = "Database cross reference for the mapped entity"
SOURCE_DESCRIPTION: Final[str] = "Mapping source"

RAW_ID_MAPPING_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field(
            UNIPROT_ID_COL,
            pa.string(),
            nullable=False,
            metadata={"comment": RAW_UNIPROT_ID_DESCRIPTION, "jsonschema_type": "string"},
        ),
        pa.field(
            DB_REFERENCE_TYPE_COL,
            pa.string(),
            nullable=False,
            metadata={"comment": DB_REFERENCE_TYPE_DESCRIPTION, "jsonschema_type": "string"},
        ),
        pa.field(
            DB_REFERENCE_ID_COL,
            pa.string(),
            nullable=False,
            metadata={"comment": DB_REFERENCE_ID_DESCRIPTION, "jsonschema_type": "string"},
        ),
    ],
    metadata={"comment": RAW_ID_MAPPING_DESCRIPTION},
)

ID_MAPPING_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field(
            UNIPROT_ID_COL,
            pa.string(),
            nullable=False,
            metadata={"comment": UNIPROT_ID_DESCRIPTION},
        ),
        pa.field(
            DB_REFERENCE_COL,
            pa.string(),
            nullable=False,
            metadata={"comment": DB_REFERENCE_DESCRIPTION},
        ),
        pa.field(
            SOURCE_COL,
            pa.string(),
            nullable=False,
            metadata={"comment": SOURCE_DESCRIPTION},
        ),
    ],
    metadata={"comment": ID_MAPPING_DESCRIPTION},
)
