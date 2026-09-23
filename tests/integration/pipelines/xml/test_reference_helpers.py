"""Pure XML row reconstruction tests without pipeline or Spark fixtures."""

from copy import deepcopy
from typing import Any

import pytest

from tests.integration.pipelines.xml.xmltodict_reference_helpers import (
    _child_table_path,
    reconstruct_entries,
    set_nested,
)


@pytest.mark.parametrize(
    ("table_name", "root", "expected"),
    [
        ("entry__entry__property", "entry", ["property"]),
        (
            "entry__entry__representative_member__db_reference__aid",
            "entry",
            ["representativeMember", "dbReference", "@id"],
        ),
        ("entry__entry", "entry", ["entry"]),
        ("other__entry__property", "entry", ["other", "entry", "property"]),
        ("entry__entry__db_reference___text", "entry", ["dbReference", "#text"]),
    ],
    ids=["repeated-root", "xml-names-and-attribute", "retain-final-fragment", "unrelated-prefix", "xml-text"],
)
def test_child_table_path_pass_xml_semantics(table_name: str, root: str, expected: list[str]) -> None:
    """Keep XML denormalization and repeated-root stripping separate from schema keys."""
    assert _child_table_path(table_name, root) == expected


@pytest.mark.parametrize(
    ("target", "path", "message"),
    [
        ({}, [], "nonempty"),
        ({"member": None}, ["member", "id"], "collision"),
        ({"member": []}, ["member", "id"], "collision"),
        ({"member": "scalar"}, ["member", "id"], "collision"),
    ],
    ids=["empty-path", "null-intermediate", "list-intermediate", "scalar-intermediate"],
)
def test_set_nested_fail_invalid_path(target: dict[str, Any], path: list[str], message: str) -> None:
    """Reject empty paths and intermediate collisions without mutating the target."""
    original = deepcopy(target)
    with pytest.raises(ValueError, match=message):
        set_nested(target, path, "new")
    assert target == original


def test_reconstruct_entries_pass_row_links_and_xml_paths() -> None:
    """Attach sorted rows using row parent IDs even when table names suggest another parent."""
    tables: dict[str, list[dict[str, Any]]] = {
        "entry": [{"_dlt_id": "main", "entry__aid": "entry-id", "entry__axmlns": "ignored", "none": None}],
        "entry__entry__member": [
            {"_dlt_id": "later", "_dlt_parent_id": "main", "_dlt_list_idx": 1, "aid": "second"},
            {"_dlt_id": "first", "_dlt_parent_id": "main", "_dlt_list_idx": 0, "aid": "first"},
        ],
        "entry__entry__representative_member__db_reference__property": [
            {"_dlt_id": "direct", "_dlt_parent_id": "main", "_dlt_list_idx": 0, "avalue": "direct-child"},
        ],
        "entry__entry__member__db_reference": [
            {"_dlt_id": "reference", "_dlt_parent_id": "first", "_dlt_list_idx": 0, "aid": "db-id", "none": None},
        ],
        "_dlt__loads": [{"ignored": True}],
    }
    original = deepcopy(tables)
    assert reconstruct_entries(tables) == [
        {
            "@id": "entry-id",
            "member": [{"@id": "first", "dbReference": [{"@id": "db-id"}]}, {"@id": "second"}],
            "representativeMember": {"dbReference": {"property": [{"@value": "direct-child"}]}},
        }
    ]
    assert tables == original


@pytest.mark.parametrize(
    "tables", [{}, {"child__table": []}, {"first": [], "second": []}], ids=["empty", "no-root", "multiple-roots"]
)
def test_reconstruct_entries_fail_ambiguous_root(tables: dict[str, list[dict[str, Any]]]) -> None:
    """Reject datasets without exactly one top-level table."""
    with pytest.raises(ValueError, match="appropriate top-level table"):
        reconstruct_entries(tables)


def test_reconstruct_entries_pass_empty_root() -> None:
    """An empty main table reconstructs to an empty entry list."""
    assert reconstruct_entries({"entry": []}) == []
