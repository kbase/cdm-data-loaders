"""Tests for the fields module in cdm_data_loaders.core.fields."""

import pytest
from pydantic import AliasChoices

from cdm_data_loaders.core.fields import generate_aliases


@pytest.mark.parametrize(
    ("field_name", "short_alias", "expected_aliases"),
    [
        # field with a short alias
        ("use_destination", "d", ["d", "use_destination", "use-destination"]),
        # field with no short alias
        ("dev_mode", None, ["dev_mode", "dev-mode"]),
        # field with no underscores, no short alias
        ("verbose", None, ["verbose"]),
        # field with underscores and short alias
        ("non_existent_field", "n", ["n", "non_existent_field", "non-existent-field"]),
        # short alias is an empty string
        ("verbose", "", ["verbose"]),
        # field with hyphens and short alias -- not allowed by pydantic but whatevs
        ("hyphens-are-fun", "h", ["h", "hyphens-are-fun"]),
    ],
)
def test_generate_aliases(field_name: str, short_alias: str | None, expected_aliases: list[str]) -> None:
    """Test the generate_aliases function."""
    assert generate_aliases(field_name, short_alias=short_alias) == AliasChoices(*expected_aliases)


@pytest.mark.parametrize("field_name", [None, ""])
@pytest.mark.parametrize("short_name", [None, "", "short_name"])
def test_generate_aliases_invalid_input(field_name: str | None, short_name: str | None) -> None:
    """Test generate_aliases with invalid input."""
    with pytest.raises(ValueError, match="No field_name supplied"):
        generate_aliases(field_name, short_name)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize(
    ("field_name", "short_alias", "expected_aliases"),
    [
        # field with a short alias
        ("a", "b", ["b", "a"]),
        # field with no short alias
        ("a", None, ["a"]),
        # short alias is an empty string
        ("a", "", ["a"]),
        # short alias duplicates field name
        ("a", "a", ["a"]),
        # non-single character short alias
    ],
)
@pytest.mark.parametrize("values", [("a", "b"), ("ax", "bx"), ("axolotl", "benefit")])
def test_generate_aliases_duplicate_aliases(
    field_name: str, short_alias: str | None, expected_aliases: list[str], values: tuple[str, str]
) -> None:
    """Ensure that duplicate aliases are not created by accident."""
    for f in [field_name, short_alias, *expected_aliases]:
        if not f:
            continue
        f = f.replace("a", values[0]).replace("b", values[1])

    assert generate_aliases(field_name, short_alias=short_alias) == AliasChoices(*expected_aliases)
