"""Tests for the fields module in cdm_data_loaders.core.fields."""

import pytest

from cdm_data_loaders.core.fields import generate_aliases
from tests.core.conftest import generate_cli_arguments


@pytest.mark.parametrize(
    ("field_name", "expected_aliases"),
    [
        # field with a short alias
        ("use_destination", ["d", "use_destination", "use-destination"]),
        # field with no short alias
        ("dev_mode", ["dev_mode", "dev-mode"]),
        # field with no underscores, no short aliases - short alias is generated
        ("verbose", ["v", "verbose"]),
        # field with underscores, short alias is generated
        ("non_existent_field", ["n", "non_existent_field", "non-existent-field"]),
    ],
)
def test_generate_aliases(field_name: str, expected_aliases: list[str]) -> None:
    """Test the generate_aliases function."""
    assert generate_aliases(field_name) == expected_aliases


@pytest.mark.parametrize(
    ("field_name", "expected_aliases"),
    [
        # field with a short alias
        ("use_destination", ["use_destination", "use-destination"]),
        # field with no short alias
        ("dev_mode", ["dev_mode", "dev-mode"]),
        # field with no underscores, no short aliases
        ("verbose", ["verbose"]),
        # ifield with underscores, short alias is generated
        ("non_existent_field", ["non_existent_field", "non-existent-field"]),
    ],
)
def test_generate_aliases_no_short_aliases(field_name: str, expected_aliases: list[str]) -> None:
    """Test the generate_aliases function."""
    assert generate_aliases(field_name, short_aliases=False) == expected_aliases


def test_generate_cli_arguments() -> None:
    """Test the generate_cli_arguments function."""
    aliases = {k: generate_aliases(k) for k in ["use_destination", "dev_mode", "verbose", "non_existent_field"]}

    assert generate_cli_arguments(aliases) == {
        # field with a short alias
        "use_destination": ["-d", "--use_destination", "--use-destination"],
        # field with no short alias
        "dev_mode": ["--dev_mode", "--dev-mode"],
        # field with no underscores, no short aliases - short alias is generated
        "verbose": ["-v", "--verbose"],
        # field with underscores, short alias is generated
        "non_existent_field": ["-n", "--non_existent_field", "--non-existent-field"],
    }


def test_generate_cli_arguments_no_short_aliases() -> None:
    """Test the generate_cli_arguments function, no short aliases."""
    aliases = {
        k: generate_aliases(k, short_aliases=False)
        for k in ["use_destination", "dev_mode", "verbose", "non_existent_field"]
    }

    assert generate_cli_arguments(aliases) == {
        # field with a short alias
        "use_destination": ["--use_destination", "--use-destination"],
        # field with no short alias
        "dev_mode": ["--dev_mode", "--dev-mode"],
        # field with no underscores, no short aliases - short alias is generated
        "verbose": ["--verbose"],
        # field with underscores, short alias is generated
        "non_existent_field": ["--non_existent_field", "--non-existent-field"],
    }


def test_generate_cli_arguments_multiple_args() -> None:
    """Test the generate_cli_arguments function, multiple dictionaries."""
    aliases = [{k: generate_aliases(k)} for k in ["use_destination", "dev_mode", "verbose", "non_existent_field"]]

    assert generate_cli_arguments(*aliases) == {
        # field with a short alias
        "use_destination": ["-d", "--use_destination", "--use-destination"],
        # field with no short alias
        "dev_mode": ["--dev_mode", "--dev-mode"],
        # field with no underscores, no short aliases - short alias is generated
        "verbose": ["-v", "--verbose"],
        # field with underscores, short alias is generated
        "non_existent_field": ["-n", "--non_existent_field", "--non-existent-field"],
    }
