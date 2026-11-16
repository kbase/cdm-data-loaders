"""Expected PyArrow tables for uniprot_idmapping 01_idmapping_ingest."""

from typing import Final

import pyarrow as pa

from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    ID_MAPPING_SCHEMA,
    RAW_ID_MAPPING_SCHEMA,
)

EXPECTED_TABLES: Final[dict[str, pa.Table]] = {
    "raw_id_mapping": pa.Table.from_pylist(
        [
            {
                "uniprot_id": "A0A024RBG1",
                "db_reference_type": "UniProtKB-ID",
                "db_reference_id": "NUD4B_HUMAN",
            },
            {
                "uniprot_id": "P12345",
                "db_reference_type": "NCBI_TaxID",
                "db_reference_id": "9606",
            },
            {
                "uniprot_id": "Q8N158",
                "db_reference_type": "GeneID",
                "db_reference_id": "284697",
            },
        ],
        schema=RAW_ID_MAPPING_SCHEMA,
    ),
    "id_mapping": pa.Table.from_pylist(
        [
            {
                "uniprot_id": "uniprot:A0A024RBG1",
                "db_reference": "UniProtKB-ID:NUD4B_HUMAN",
                "source": "HUMAN_9606_idmapping.dat.gz",
            },
            {
                "uniprot_id": "uniprot:P12345",
                "db_reference": "NCBI_TaxID:9606",
                "source": "HUMAN_9606_idmapping.dat.gz",
            },
            {
                "uniprot_id": "uniprot:Q8N158",
                "db_reference": "GeneID:284697",
                "source": "HUMAN_9606_idmapping.dat.gz",
            },
        ],
        schema=ID_MAPPING_SCHEMA,
    ),
}
