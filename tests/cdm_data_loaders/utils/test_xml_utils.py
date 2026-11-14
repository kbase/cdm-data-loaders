"""Tests for the xml_utils module."""

import pytest

from cdm_data_loaders.utils.xml_utils import local_name


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("{http://uniprot.org/uniprot}entry", "entry"),
        ("{http://uniprot.org/uniref}", ""),
        ("{}entry", "entry"),
        ("{http://example.org/{nested}}item", "item"),
        ("entry", "entry"),
        ("", ""),
        (None, None),
    ],
    ids=[
        "clark_notation",
        "namespace_only_empty_string",
        "empty_namespace_braces",
        "nested_braces_in_namespace",
        "unprefixed_tag",
        "empty_string",
        "non_string_input",
    ],
)
def test_local_name_pass_extracts_local_part(tag: str | None, expected: str | None) -> None:
    """local_name strips Clark-notation namespaces and preserves unprefixed names."""
    assert local_name(tag) == expected  # pyright: ignore[reportArgumentType]
