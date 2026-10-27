"""Integration tests for the xmltopyarrow pipeline against the uniref reference dataset."""

from collections.abc import Callable
from typing import Any

import duckdb

from cdm_data_loaders.core.fields import PARQUET
from tests.cdm_data_loaders.pipelines.helpers import (
    read_pipeline_tables,
    reference_entries_from_duckdb,
    run_and_read,
    sorted_json,
)
from tests.cdm_data_loaders.pipelines.xmltopyarrow.integration.conftest import (
    normalize_uniref_entry_for_xsd,
    reconstruct_xsd_entries,
    strip_xsd_arrow_row,
)

EXPECTED_ENTRY_COUNT = 100


def test_xmltopyarrow_pass_uniref_chunk_5_el_matches_reference(
    run_xmltopyarrow_pipeline_fxt: Callable[..., Any],
    reference_xml_entries: list[dict[str, Any]],
) -> None:
    """Running on chunk_5_el with uniref.xsd produces a single Arrow-typed entry table matching the 100 reference entries."""
    (load_info, output_dir) = run_and_read(
        run_xmltopyarrow_pipeline_fxt,
        "chunk_5_el",
    )
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, PARQUET)
    assert set(tables) == {"entry"}
    loaded_rows = [strip_xsd_arrow_row(row) for row in tables["entry"]]
    expected_rows = [normalize_uniref_entry_for_xsd(entry) for entry in reference_xml_entries]
    assert len(loaded_rows) == EXPECTED_ENTRY_COUNT
    assert sorted_json(loaded_rows) == sorted_json(expected_rows)


def test_xmltopyarrow_pass_reconstructed_matches_duckdb_reference(
    run_xmltopyarrow_pipeline_fxt: Callable[..., Any],
    reference_xml_dataset: duckdb.DuckDBPyConnection,
) -> None:
    """Reconstructed pipeline entries equal the entries loaded from the DuckDB reference."""
    (load_info, output_dir) = run_and_read(
        run_xmltopyarrow_pipeline_fxt,
        "chunk_100_el",
        loader_file_format=PARQUET,
    )
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, PARQUET)
    reconstructed = reconstruct_xsd_entries(tables)
    from_duckdb = [
        normalize_uniref_entry_for_xsd(entry) for entry in reference_entries_from_duckdb(reference_xml_dataset)
    ]
    assert len(from_duckdb) == EXPECTED_ENTRY_COUNT
    assert sorted_json(reconstructed) == sorted_json(from_duckdb)
