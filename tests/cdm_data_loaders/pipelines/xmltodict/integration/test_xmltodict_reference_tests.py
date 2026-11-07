"""Integration tests for the xmltodict pipeline against the uniref reference dataset."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

from cdm_data_loaders.core.fields import PARQUET
from tests.cdm_data_loaders.pipelines.helpers import (
    read_pipeline_tables,
    reconstruct_entries,
    reference_entries_from_duckdb,
    run_and_read,
    sorted_json,
)

EXPECTED_ENTRY_COUNT = 100


# tests for the "reference" data structure
def test_xmltodict_reference_xml_as_jsonl_pass_matches_source(
    reference_xml_entries: list[dict[str, Any]],
    reference_xml_as_jsonl: Path,
) -> None:
    """The generated reference JSONL has 100 entries with the expected shape."""
    assert len(reference_xml_entries) == EXPECTED_ENTRY_COUNT
    with reference_xml_as_jsonl.open(encoding="utf-8") as fh:
        line_count = sum(1 for line in fh if line.strip())
    assert line_count == EXPECTED_ENTRY_COUNT
    for key in ("_id", "name", "property", "representativeMember"):
        assert key in reference_xml_entries[0].get("entry", {})


def test_xml_pass_reference_dataset_has_nested_structures(
    reference_xml_dataset: duckdb.DuckDBPyConnection,
) -> None:
    """The DuckDB reference stores nested JSON as STRUCT/LIST types, not JSON text columns."""
    reference_xml_table = reference_xml_dataset.table("reference")
    reference_xml_df = reference_xml_table.df()
    entry = reference_xml_df["entry"].iloc[0]
    assert isinstance(entry, dict)
    assert isinstance(entry["property"], list)
    assert isinstance(entry["representativeMember"], dict)
    assert len(reference_xml_df) == EXPECTED_ENTRY_COUNT

    # same thing, but using native duckdb
    entry_type = reference_xml_table.types[0]
    entry_fields = dict(entry_type.children)

    property_type = entry_fields["property"]
    member_type = entry_fields["representativeMember"]
    assert property_type.id == "list"
    assert member_type.id == "struct"

    count = reference_xml_dataset.execute("SELECT COUNT(*) FROM reference").fetchone()
    assert count == (EXPECTED_ENTRY_COUNT,)


def test_xml_pass_reconstructed_matches_duckdb_reference(
    run_xmltodict_pipeline_fxt: Callable[..., Any],
    reference_xml_dataset: duckdb.DuckDBPyConnection,
) -> None:
    """Reconstructed pipeline entries equal the entries loaded from the DuckDB reference."""
    (load_info, output_dir) = run_and_read(run_xmltodict_pipeline_fxt, "chunk_100_el", loader_file_format=PARQUET)
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, PARQUET)
    reconstructed = reconstruct_entries(tables)
    from_duckdb = reference_entries_from_duckdb(reference_xml_dataset)
    assert len(from_duckdb) == EXPECTED_ENTRY_COUNT
    assert sorted_json(reconstructed) == sorted_json(from_duckdb)
