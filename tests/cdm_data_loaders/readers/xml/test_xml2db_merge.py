"""Unit tests for `cdm_data_loaders.readers.xml2db_merge.MergePreparer`.

These use the UniRef reference fixtures (`uniref.xsd` and the `chunk_5_el` XML files).
"""

import gzip
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from typing import Any, Final

import pytest
from xml2db import DataModel, Document

from cdm_data_loaders.readers.xml2db_doc import build_xml2db_model, flatten_xml2db_document
from cdm_data_loaders.readers.xml2db_merge import MergePreparer

REFERENCE_XML_DIR: Final[Path] = Path("tests") / "data" / "uniprot" / "uniref"
UNIREF_XSD: Final[Path] = REFERENCE_XML_DIR / "uniref.xsd"
PART_01: Final[Path] = REFERENCE_XML_DIR / "chunk_5_el" / "uniref_100_part_01.xml.gz"
PART_02: Final[Path] = REFERENCE_XML_DIR / "chunk_5_el" / "uniref_100_part_02.xml.gz"

ROOT_TABLE: Final[str] = "uniref_test"
ROOT_JUNCTION: Final[str] = "uniref_test_UniRef50_entry"
ENTRY: Final[str] = "entry"
PROPERTY: Final[str] = "property"
MEMBER: Final[str] = "representativeMember"
CONTENT_TABLES: Final[tuple[str, ...]] = (ENTRY, PROPERTY, MEMBER)
JUNCTION_TABLES: Final[tuple[str, ...]] = (
    "entry_property",
    "entry_member_representativeMember",
    "representativeMember_dbReference_property",
)
EXPECTED_MERGE_DISPOSITION: Final[dict[str, str]] = {"disposition": "merge", "strategy": "insert-only"}
SHA1_HEX_LENGTH: Final[int] = 40
THREADS: Final[int] = 8

type Tables = dict[str, list[dict[str, Any]]]


@pytest.fixture(scope="module")
def uniref_model() -> DataModel:
    """Build the xml2db DataModel from uniref.xsd once for the whole test module."""
    return build_xml2db_model(UNIREF_XSD, short_name=ROOT_TABLE)


def flatten(model: DataModel, path: Path, file_key: str) -> Tables:
    """Parse a gzipped UniRef file with xml2db and flatten it into table name -> rows."""
    document = Document(model)
    with gzip.open(path, "rb") as fh:
        document.parse_xml(BytesIO(fh.read()), skip_validation=True)
    return flatten_xml2db_document(model, document, file_key=file_key)


def distinct_keys(tables: Tables, table_name: str) -> set[Any]:
    """The distinct primary keys of a table's rows."""
    return {row[f"pk_{table_name}"] for row in tables[table_name]}


def test_merge_preparer_pass_table_hints_cover_every_table_with_single_column_keys(uniref_model: DataModel) -> None:
    """Every output table, junction tables included, is merged on a single-column `pk_<table>` insert-only."""
    preparer = MergePreparer(uniref_model)

    expected_tables = {ROOT_TABLE, ENTRY, PROPERTY, MEMBER, ROOT_JUNCTION, *JUNCTION_TABLES}
    assert expected_tables <= set(preparer.table_hints)
    for table_name, hints in preparer.table_hints.items():
        assert hints == {"primary_key": f"pk_{table_name}", "write_disposition": EXPECTED_MERGE_DISPOSITION}


def test_merge_preparer_pass_hints_are_independent_copies(uniref_model: DataModel) -> None:
    """Mutating one table's hints does not change another's."""
    preparer = MergePreparer(uniref_model)

    preparer.table_hints[ENTRY]["write_disposition"]["strategy"] = "changed"

    assert preparer.table_hints[PROPERTY]["write_disposition"] == EXPECTED_MERGE_DISPOSITION


def test_prepare_pass_first_document_keeps_every_entity_row_and_keys_junctions(uniref_model: DataModel) -> None:
    """The first document is unchanged apart from a unique single-column key added to each junction row."""
    tables = flatten(uniref_model, PART_01, PART_01.name)

    prepared = MergePreparer(uniref_model).prepare(tables)

    assert set(prepared) == set(tables)
    for table_name in (ROOT_TABLE, *CONTENT_TABLES):
        assert prepared[table_name] == tables[table_name], table_name
    for table_name in (ROOT_JUNCTION, *JUNCTION_TABLES):
        junction_pks = [row[f"pk_{table_name}"] for row in prepared[table_name]]
        assert len(junction_pks) == len(tables[table_name]), table_name
        assert len(set(junction_pks)) == len(junction_pks), table_name
        assert all(len(pk) == SHA1_HEX_LENGTH for pk in junction_pks), table_name
        for prepared_row, original_row in zip(prepared[table_name], tables[table_name], strict=True):
            assert {k: v for k, v in prepared_row.items() if k != f"pk_{table_name}"} == original_row


def test_prepare_pass_junction_key_depends_only_on_the_linked_pair(uniref_model: DataModel) -> None:
    """The same (parent, child) pair always gets the same junction key; different pairs get different ones."""
    link_p = {"fk_entry": "entry:a", "fk_property": "property:p"}
    link_q = {"fk_entry": "entry:a", "fk_property": "property:q"}
    entry = [{"pk_entry": "entry:a"}]

    first = MergePreparer(uniref_model).prepare({ENTRY: entry, "entry_property": [link_p]})
    second = MergePreparer(uniref_model).prepare({ENTRY: entry, "entry_property": [link_p, link_q]})

    key_p_first = first["entry_property"][0]["pk_entry_property"]
    key_p_second, key_q_second = (row["pk_entry_property"] for row in second["entry_property"])
    assert key_p_first == key_p_second
    assert key_q_second != key_p_second


def test_prepare_pass_identical_document_again_yields_only_non_content_addressed_rows(
    uniref_model: DataModel,
) -> None:
    """A second parse of the same content yields nothing but the root rows, which are not content-addressed."""
    preparer = MergePreparer(uniref_model)
    preparer.prepare(flatten(uniref_model, PART_01, PART_01.name))

    second = preparer.prepare(flatten(uniref_model, PART_01, "same_content_other_file.xml"))

    for table_name in (*CONTENT_TABLES, *JUNCTION_TABLES):
        assert second[table_name] == [], table_name
    assert len(second[ROOT_TABLE]) == 1
    assert len(second[ROOT_JUNCTION]) == len(flatten(uniref_model, PART_01, "x")[ROOT_JUNCTION])


def test_prepare_pass_second_file_drops_only_rows_shared_with_the_first(uniref_model: DataModel) -> None:
    """Across two files, shared content-addressed rows are emitted once and the union is complete."""
    tables_1 = flatten(uniref_model, PART_01, PART_01.name)
    tables_2 = flatten(uniref_model, PART_02, PART_02.name)
    preparer = MergePreparer(uniref_model)

    prepared_1 = preparer.prepare(tables_1)
    prepared_2 = preparer.prepare(tables_2)

    for table_name in CONTENT_TABLES:
        keys_1 = distinct_keys(prepared_1, table_name)
        keys_2 = distinct_keys(prepared_2, table_name)
        assert keys_1.isdisjoint(keys_2), table_name
        assert keys_1 | keys_2 == distinct_keys(tables_1, table_name) | distinct_keys(tables_2, table_name), table_name
        assert len(prepared_2[table_name]) == len(keys_2), table_name
    # the fixture files share content, so something must actually have been dropped
    assert len(prepared_2[PROPERTY]) < len(tables_2[PROPERTY])


def test_prepare_pass_junction_rows_kept_only_for_newly_seen_parents(uniref_model: DataModel) -> None:
    """Every kept junction row's parent was emitted by the same call, and its child exists in some call."""
    tables_1 = flatten(uniref_model, PART_01, PART_01.name)
    tables_2 = flatten(uniref_model, PART_02, PART_02.name)
    preparer = MergePreparer(uniref_model)
    preparer.prepare(tables_1)

    prepared_2 = preparer.prepare(tables_2)

    entry_keys_2 = distinct_keys(prepared_2, ENTRY)
    assert {row["fk_entry"] for row in prepared_2["entry_property"]} <= entry_keys_2
    member_keys_2 = distinct_keys(prepared_2, MEMBER)
    assert {row["fk_representativeMember"] for row in prepared_2["representativeMember_dbReference_property"]} <= (
        member_keys_2
    )


def test_prepare_pass_repeated_junction_pairs_collapsed_to_one_row(uniref_model: DataModel) -> None:
    """Repeating the same (parent, child) pair within one parent yields a single junction row."""
    tables = {
        ENTRY: [{"pk_entry": "entry:a"}],
        "entry_property": [
            {"fk_entry": "entry:a", "fk_property": "property:p"},
            {"fk_entry": "entry:a", "fk_property": "property:p"},
            {"fk_entry": "entry:a", "fk_property": "property:q"},
        ],
    }

    prepared = MergePreparer(uniref_model).prepare(tables)

    assert [(r["fk_entry"], r["fk_property"]) for r in prepared["entry_property"]] == [
        ("entry:a", "property:p"),
        ("entry:a", "property:q"),
    ]


def test_prepare_pass_repeated_rows_within_one_call_emitted_once(uniref_model: DataModel) -> None:
    """A content-addressed key repeated inside one call is emitted once, keeping its first row."""
    first_row = {"pk_property": "property:a", "value": "first"}
    tables = {PROPERTY: [first_row, {"pk_property": "property:a", "value": "second"}, {"pk_property": "property:b"}]}

    prepared = MergePreparer(uniref_model).prepare(tables)

    assert prepared[PROPERTY] == [first_row, {"pk_property": "property:b"}]


def test_prepare_pass_root_rows_with_repeated_key_are_all_kept(uniref_model: DataModel) -> None:
    """Root rows are not content-addressed, so a repeated root key is left for the merge to resolve."""
    root_row = {"pk_uniref_test": "file.xml:1"}

    preparer = MergePreparer(uniref_model)
    first = preparer.prepare({ROOT_TABLE: [root_row]})
    second = preparer.prepare({ROOT_TABLE: [root_row]})

    assert first[ROOT_TABLE] == second[ROOT_TABLE] == [root_row]


def test_prepare_pass_empty_input_returns_empty_output(uniref_model: DataModel) -> None:
    """No tables in, no tables out."""
    assert MergePreparer(uniref_model).prepare({}) == {}


def test_prepare_pass_does_not_modify_its_input(uniref_model: DataModel) -> None:
    """The caller's flattened tables are left as they were."""
    tables = flatten(uniref_model, PART_01, PART_01.name)
    snapshot = {name: [dict(row) for row in rows] for name, rows in tables.items()}

    MergePreparer(uniref_model).prepare(tables)

    assert tables == snapshot


def test_prepare_pass_concurrent_calls_emit_each_content_row_exactly_once(uniref_model: DataModel) -> None:
    """Threads preparing the same content together never emit a content-addressed row twice."""
    tables = flatten(uniref_model, PART_01, PART_01.name)
    preparer = MergePreparer(uniref_model)

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        results = list(pool.map(preparer.prepare, [tables] * THREADS))

    for table_name in CONTENT_TABLES:
        emitted = [row[f"pk_{table_name}"] for result in results for row in result[table_name]]
        assert sorted(emitted) == sorted(distinct_keys(tables, table_name)), table_name
    for table_name in JUNCTION_TABLES:
        assert sum(len(result[table_name]) for result in results) == len(tables[table_name]), table_name
