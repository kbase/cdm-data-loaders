"""Table transforms and output schemas for UniProt ID mapping batches."""

from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.compute as pc

from cdm_data_loaders.core.registry import ArrowSchemaRegistry, TableTransformFn, TransformContext
from cdm_data_loaders.core.transforms.routing import emit_raw_table
from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    DB_REFERENCE_ID_COL,
    DB_REFERENCE_TYPE_COL,
    ID_MAPPING_SCHEMA,
    ID_MAPPING_TABLE_NAME,
    RAW_ID_MAPPING_SCHEMA,
    RAW_ID_MAPPING_TABLE_NAME,
    UNIPROT_ID_COL,
)

UNIPROT_CURIE_PREFIX: Final[str] = "uniprot:"

UNIPROT_IDMAPPING_OUTPUT_SCHEMAS: Final[ArrowSchemaRegistry] = ArrowSchemaRegistry(
    registry={
        RAW_ID_MAPPING_TABLE_NAME: RAW_ID_MAPPING_SCHEMA,
        ID_MAPPING_TABLE_NAME: ID_MAPPING_SCHEMA,
    }
)


def transform_uniprot_id_mapping(
    table: pa.Table,
    ctx: TransformContext,
) -> dict[str, pa.Table]:
    """Transform a validated batch of raw UniProt ID mapping rows into standardised ``id_mapping`` rows.

    Constructs ``uniprot_id`` as ``f"uniprot:{uniprot_id}"``, ``db_reference`` as
    ``f"{db_reference_type}:{db_reference_id}"``, and ``source`` as the basename of the
    input file being parsed.

    :param table: validated PyArrow table of raw ID mapping records
    :type table: pa.Table
    :param ctx: transform context carrying the input table name and source file metadata
    :type ctx: TransformContext
    :return: mapping routing the standardised PyArrow table to ``id_mapping`` (or empty dict for 0-row input)
    :rtype: dict[str, pa.Table]
    """
    if table.num_rows == 0:
        return {}
    source_name = ctx.file_name or (Path(ctx.source_file).name if ctx.source_file else "")
    uniprot_col = pc.binary_join_element_wise(UNIPROT_CURIE_PREFIX, table.column(UNIPROT_ID_COL), "")
    db_ref_col = pc.binary_join_element_wise(
        table.column(DB_REFERENCE_TYPE_COL),
        table.column(DB_REFERENCE_ID_COL),
        ":",
    )
    source_col = pa.repeat(pa.scalar(source_name, type=pa.string()), table.num_rows)
    out_table = pa.Table.from_arrays(
        [uniprot_col, db_ref_col, source_col],
        schema=ID_MAPPING_SCHEMA,
    )
    return {ID_MAPPING_TABLE_NAME: out_table}


TABLE_TRANSFORMS: Final[dict[str, tuple[TableTransformFn, ...]]] = {
    ID_MAPPING_TABLE_NAME: (emit_raw_table, transform_uniprot_id_mapping),
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = dict(UNIPROT_IDMAPPING_OUTPUT_SCHEMAS.registry)
