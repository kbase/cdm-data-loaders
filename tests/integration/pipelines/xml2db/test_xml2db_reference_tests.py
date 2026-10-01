"""Integration tests for the xml2db_ingest pipeline against the uniref reference dataset."""

import gzip
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import xmltodict

from cdm_data_loaders.readers.xml2db_doc import build_xml2db_model
from tests.integration.pipelines.helpers import sorted_json
from tests.integration.pipelines.xml2db.xml2db_reference_helpers import (
    assert_uniref_referential_integrity,
    count_rows_without_run_wide_dedup,
    read_iceberg_tables,
    reconstruct_xml2db_entries,
)

CHUNK_DIRS = ("chunk_5_el", "chunk_20_el", "chunk_100_el")
EXPECTED_ENTRY_COUNT = 100
MASTER_XML = Path("chunk_100_el") / "uniref_100_part_01.xml.gz"
CONTENT_ADDRESSED_TABLES = ("entry", "property", "representative_member")
CHUNK_SIZE = 7


def _as_list(value: Any) -> list[Any]:
    """Normalize an xmltodict field that may be a single dict, a list, or missing."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _properties(properties_field: Any) -> list[list[str]]:
    """Convert xmltodict property elements into sorted [type, value] pairs."""
    return sorted([[p["_type"], p["_value"]] for p in _as_list(properties_field)])


def _member_from_xmltodict(member: dict[str, Any]) -> dict[str, Any]:
    """Convert one xmltodict (representative)member element into the comparable summary shape."""
    dbref = member["dbReference"]
    sequence = member.get("sequence") or {}
    return {
        "dbReference_type": dbref.get("_type"),
        "dbReference_id": dbref.get("_id"),
        "sequence_length": int(sequence["_length"]) if sequence.get("_length") is not None else None,
        "sequence_checksum": sequence.get("_checksum"),
        "sequence_value": sequence.get("#text"),
        "properties": _properties(dbref.get("property")),
    }


def _entry_from_xmltodict(entry: dict[str, Any]) -> dict[str, Any]:
    """Convert one xmltodict entry element into the same shape as reconstruct_xml2db_entries."""
    return {
        "id": entry["_id"],
        "updated": entry["_updated"],
        "name": entry["name"],
        "properties": _properties(entry.get("property")),
        "representative_member": _member_from_xmltodict(entry["representativeMember"]),
        "members": sorted(
            (_member_from_xmltodict(m) for m in _as_list(entry.get("member"))),
            key=lambda m: json.dumps(m, sort_keys=True, default=str),
        ),
    }


def xmltodict_reference_entries(reference_xml_data_dir: Path) -> list[dict[str, Any]]:
    """Parse the master 100-entry UniRef file directly with xmltodict, in comparable form."""
    with gzip.open(reference_xml_data_dir / MASTER_XML, "rb") as fh:
        document = xmltodict.parse(fh.read(), attr_prefix="_")
    entries = _as_list(document["UniRef50"]["entry"])
    assert len(entries) == EXPECTED_ENTRY_COUNT
    return [_entry_from_xmltodict(entry) for entry in entries]


def run_and_read(run_pipeline: Callable[..., Any], chunk_dir: str, **overrides: Any) -> dict[str, list[dict[str, Any]]]:
    """Run the pipeline on a chunk dir and read back every iceberg table it wrote."""
    (load_info, output_dir) = run_pipeline(chunk_dir, **overrides)
    assert load_info is not None
    assert not load_info.has_failed_jobs
    return read_iceberg_tables(output_dir / load_info.dataset_name)


def run_and_reconstruct(run_pipeline: Callable[..., Any], chunk_dir: str, **overrides: Any) -> list[dict[str, Any]]:
    """Run the pipeline on a chunk dir and reconstruct nested entries from its output tables."""
    return reconstruct_xml2db_entries(run_and_read(run_pipeline, chunk_dir, **overrides))


def test_xml2db_ingest_pass_reference_dataset_matches_xmltodict(
    run_xml2db_pipeline: Callable[..., Any],
    reference_xml_data_dir: Path,
) -> None:
    """Reconstructed entries from the xml2db pipeline match a direct xmltodict parse, 1:1."""
    reconstructed = run_and_reconstruct(run_xml2db_pipeline, "chunk_100_el")
    assert len(reconstructed) == EXPECTED_ENTRY_COUNT

    reference = xmltodict_reference_entries(reference_xml_data_dir)
    assert sorted_json(reconstructed) == sorted_json(reference)


def test_xml2db_ingest_pass_output_identical_across_chunk_dirs(run_xml2db_pipeline: Callable[..., Any]) -> None:
    """Splitting the same 100 entries across differently-sized file chunks does not change the result."""
    reconstructed_by_chunk = {
        chunk_dir: run_and_reconstruct(run_xml2db_pipeline, chunk_dir) for chunk_dir in CHUNK_DIRS
    }

    for chunk_dir, entries in reconstructed_by_chunk.items():
        assert len(entries) == EXPECTED_ENTRY_COUNT, chunk_dir

    sorted_by_chunk = {chunk_dir: sorted_json(entries) for chunk_dir, entries in reconstructed_by_chunk.items()}
    baseline = sorted_by_chunk[CHUNK_DIRS[0]]
    for chunk_dir in CHUNK_DIRS[1:]:
        assert sorted_by_chunk[chunk_dir] == baseline, chunk_dir


def test_xml2db_ingest_pass_referential_integrity_across_merged_files(
    run_xml2db_pipeline: Callable[..., Any],
) -> None:
    """Every foreign key in the merged, multi-file dataset resolves to a row in its target table."""
    assert_uniref_referential_integrity(run_and_read(run_xml2db_pipeline, "chunk_5_el"))


def test_xml2db_ingest_pass_multi_file_run_removes_content_repeated_across_files(
    run_xml2db_pipeline: Callable[..., Any],
    reference_xml_data_dir: Path,
    reference_xsd: Path,
) -> None:
    """Content repeated across 20 separate files is loaded once, though parsing each file alone repeats it.

    chunk_5_el splits the same 100 entries across 20 files with no chunk_element_tag, so each file
    is its own xml2db Document and xml2db alone cannot see what the others contain. In this
    fixture `property` content is repeated heavily across files, so its per-file row counts sum to
    much more than the distinct content. The loaded tables must hold one row per key.
    """
    xml_files = sorted((reference_xml_data_dir / "chunk_5_el").glob("*.xml.gz"))
    rows_if_not_deduplicated = count_rows_without_run_wide_dedup(build_xml2db_model(reference_xsd, "uniref"), xml_files)

    tables = run_and_read(run_xml2db_pipeline, "chunk_5_el")

    assert rows_if_not_deduplicated["property"] > len(tables["property"])
    assert rows_if_not_deduplicated["representativeMember"] >= len(tables["representative_member"])
    for table_name in CONTENT_ADDRESSED_TABLES:
        keys = [row[f"pk_{table_name}"] for row in tables[table_name]]
        assert len(keys) == len(set(keys)), table_name
    assert len(tables["entry"]) == EXPECTED_ENTRY_COUNT


@pytest.mark.parametrize("chunk_size", [1, CHUNK_SIZE, 33])
def test_xml2db_ingest_pass_chunked_reference_dataset_matches_xmltodict(
    run_xml2db_pipeline: Callable[..., Any],
    reference_xml_data_dir: Path,
    chunk_size: int,
) -> None:
    """With chunk_element_tag set, the pipeline still reconstructs the same entries as xmltodict.

    Runs against the single 100-entry master file (one file, many chunks per file) with chunk
    sizes that don't evenly divide 100, to exercise remainder handling as well as multi-chunk
    behaviour.
    """
    reconstructed = run_and_reconstruct(
        run_xml2db_pipeline, "chunk_100_el", chunk_element_tag="entry", chunk_size=chunk_size
    )
    assert len(reconstructed) == EXPECTED_ENTRY_COUNT

    reference = xmltodict_reference_entries(reference_xml_data_dir)
    assert sorted_json(reconstructed) == sorted_json(reference)


def test_xml2db_ingest_pass_chunked_output_matches_unchunked_output(run_xml2db_pipeline: Callable[..., Any]) -> None:
    """Enabling chunking does not change the reconstructed content, only how it's parsed internally."""
    unchunked = run_and_reconstruct(run_xml2db_pipeline, "chunk_5_el")
    chunked = run_and_reconstruct(run_xml2db_pipeline, "chunk_5_el", chunk_element_tag="entry", chunk_size=2)

    assert len(chunked) == len(unchunked) == EXPECTED_ENTRY_COUNT
    assert sorted_json(chunked) == sorted_json(unchunked)


def test_xml2db_ingest_pass_chunked_referential_integrity_across_files_and_chunks(
    run_xml2db_pipeline: Callable[..., Any],
) -> None:
    """Foreign keys still resolve correctly when both multiple files and multiple chunks per file are involved."""
    tables = run_and_read(run_xml2db_pipeline, "chunk_5_el", chunk_element_tag="entry", chunk_size=2)
    assert_uniref_referential_integrity(tables)


def test_xml2db_ingest_pass_chunked_row_counts_match_unchunked_exactly(
    run_xml2db_pipeline: Callable[..., Any],
) -> None:
    """Chunked content-addressed tables end up with the exact same row count as unchunked.

    Not just the same *content*, which the reconstruction-based tests above already establish,
    but the same physical row count, with nothing repeated across chunks.
    """
    unchunked_tables = run_and_read(run_xml2db_pipeline, "chunk_100_el")
    chunked_tables = run_and_read(run_xml2db_pipeline, "chunk_100_el", chunk_element_tag="entry", chunk_size=CHUNK_SIZE)

    for table_name in CONTENT_ADDRESSED_TABLES:
        assert len(chunked_tables[table_name]) == len(unchunked_tables[table_name]), table_name


def test_xml2db_ingest_pass_chunked_root_table_has_one_row_per_chunk(
    run_xml2db_pipeline: Callable[..., Any],
) -> None:
    """The root table is not content-addressed, so it keeps one row per chunk.

    See `cdm_data_loaders.readers.xml2db_doc.is_xml2db_content_addressed_table` for why.
    """
    tables = run_and_read(run_xml2db_pipeline, "chunk_100_el", chunk_element_tag="entry", chunk_size=CHUNK_SIZE)

    n_chunks = -(-EXPECTED_ENTRY_COUNT // CHUNK_SIZE)  # ceil division
    assert len(tables["uniref"]) == n_chunks
