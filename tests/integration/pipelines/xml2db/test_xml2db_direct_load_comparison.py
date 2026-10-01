"""Compare the xml2db pipeline's output with a plain xml2db load into a DuckDB database.

The baseline loads every input file with xml2db's own database loader (`Document.insert_into_target_tables`),
which merges all files into one set of tables and deduplicates across them with database constraints. The
pipeline must end up with the same rows. Keys differ by design (database integers versus the pipeline's
string keys), so rows are compared through their content: each foreign key is replaced by the content hash
of the row it points to.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pytest

from cdm_data_loaders.readers.xml2db_doc import build_xml2db_model
from tests.integration.pipelines.xml2db.xml2db_reference_helpers import (
    all_null_columns,
    content_view,
    count_rows_without_run_wide_dedup,
    load_with_xml2db_into_duckdb,
    normalise_names_like_dlt,
    read_duckdb_tables,
    read_iceberg_tables,
)

SHORT_NAME: Final[str] = "uniref"
ROOT_TABLE: Final[str] = "uniref"
CONTENT_ADDRESSED_TABLES: Final[tuple[str, ...]] = ("entry", "property", "representative_member")
EXPECTED_TABLES: Final[set[str]] = {
    "entry",
    "entry_member_representative_member",
    "entry_property",
    "property",
    "representative_member",
    "representative_member_db_reference_property",
    ROOT_TABLE,
    "uniref_uni_ref50_entry",
}


def is_root_table(table_name: str) -> bool:
    """Whether a table is the schema's root table, or a junction table linking to it."""
    return table_name == ROOT_TABLE or table_name.startswith(f"{ROOT_TABLE}_")


@pytest.mark.parametrize(
    ("chunk_dir", "pipeline_overrides", "compare_root_tables"),
    [
        ("chunk_5_el", {}, True),
        ("chunk_100_el", {}, True),
        ("chunk_5_el", {"chunk_element_tag": "entry", "chunk_size": 2}, False),
    ],
    ids=["twenty_files", "one_file", "twenty_files_chunked"],
)
def test_xml2db_ingest_pass_output_matches_direct_xml2db_duckdb_load(
    run_xml2db_pipeline: Callable[..., Any],
    reference_xml_data_dir: Path,
    reference_xsd: Path,
    tmp_path: Path,
    chunk_dir: str,
    pipeline_overrides: dict[str, Any],
    compare_root_tables: bool,
) -> None:
    """The pipeline's tables hold exactly the rows a direct xml2db load into DuckDB produces.

    When chunking, the root table (and the junction table linking it to entries) legitimately has one
    row per chunk rather than one per file, so those tables are left out of the comparison.
    """
    xml_files = sorted((reference_xml_data_dir / chunk_dir).glob("*.xml.gz"))
    db_path = tmp_path / "xml2db_direct.duckdb"
    load_with_xml2db_into_duckdb(reference_xsd, SHORT_NAME, xml_files, db_path)
    direct_tables = normalise_names_like_dlt(read_duckdb_tables(db_path))

    (load_info, output_dir) = run_xml2db_pipeline(chunk_dir, **pipeline_overrides)
    assert load_info is not None
    assert not load_info.has_failed_jobs
    pipeline_tables = read_iceberg_tables(output_dir / load_info.dataset_name)

    exclude = set() if compare_root_tables else {name for name in direct_tables if is_root_table(name)}
    direct_view = content_view(direct_tables, exclude)
    pipeline_view = content_view(pipeline_tables, exclude)

    assert set(direct_view) == set(pipeline_view)
    assert set(direct_view) >= EXPECTED_TABLES - exclude
    for table_name in sorted(direct_view):
        # dlt creates only columns that receive data; the database load creates every column of the schema.
        # So the pipeline may lack columns, but only ones that are null in every row of the direct load.
        # Primary keys are compared through content, not by value, and the pipeline also gives each junction
        # table a derived `pk_<table>` merge key that the database load does not have, so leave that column out.
        ignored_columns = {"_dlt_id", "_dlt_load_id", f"pk_{table_name}"}
        direct_columns = set(direct_tables[table_name][0]) - ignored_columns
        pipeline_columns = set(pipeline_tables[table_name][0]) - ignored_columns
        assert pipeline_columns <= direct_columns, table_name
        assert direct_columns - pipeline_columns <= all_null_columns(direct_tables[table_name]), table_name
        assert len(pipeline_view[table_name]) == len(direct_view[table_name]), table_name
        assert pipeline_view[table_name] == direct_view[table_name], table_name


def test_xml2db_ingest_pass_direct_duckdb_load_deduplicates_across_files(
    reference_xml_data_dir: Path,
    reference_xsd: Path,
    tmp_path: Path,
) -> None:
    """The baseline really does deduplicate across files, so matching it means the pipeline does too.

    Parsing each of the 20 files alone repeats a lot of `property` content; the database holds each
    distinct row once, and the root table keeps one row per file.
    """
    xml_files = sorted((reference_xml_data_dir / "chunk_5_el").glob("*.xml.gz"))
    db_path = tmp_path / "xml2db_direct.duckdb"
    rows_if_not_deduplicated = count_rows_without_run_wide_dedup(
        build_xml2db_model(reference_xsd, SHORT_NAME), xml_files
    )

    load_with_xml2db_into_duckdb(reference_xsd, SHORT_NAME, xml_files, db_path)
    direct_tables = normalise_names_like_dlt(read_duckdb_tables(db_path))

    assert rows_if_not_deduplicated["property"] > len(direct_tables["property"])
    assert rows_if_not_deduplicated["representativeMember"] >= len(direct_tables["representative_member"])
    assert len(direct_tables["entry"]) == len(xml_files) * 5
    assert len(direct_tables[ROOT_TABLE]) == len(xml_files)
