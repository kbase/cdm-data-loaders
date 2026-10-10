"""Tests for the UniRef DLT pipeline."""

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.pipelines.uniref import (
    UNIREF_VARIANTS,
    VARIANT,
    UnirefSettings,
)
from tests.cdm_data_loaders.core.conftest import (
    check_settings,
)
from tests.cdm_data_loaders.pipelines.uniref.conftest import (
    TEST_SETTINGS,
    TEST_SETTINGS_RECONCILED,
    UNIREF_VARIANT_ALIASES,
)
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes, make_cli_arg


def test_uniref_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified.

    Note that TEST_SETTINGS includes a value for pipeline_dir.
    """
    s = UnirefSettings(**TEST_SETTINGS)  # type: ignore[reportReturnType]
    check_settings(s, TEST_SETTINGS_RECONCILED)


# model_fields
def test_no_cli_param_clashes() -> None:
    """Ensure that the settings object does not have any internal alias collisions."""
    assert_no_cli_clashes(UnirefSettings)


@pytest.mark.parametrize("field_name", list(TEST_SETTINGS))
def test_cli_fields_parse_correctly(field_name: str) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    assert_cli_field_roundtrips(
        UnirefSettings,
        field_name,
        TEST_SETTINGS[field_name],
        TEST_SETTINGS_RECONCILED[field_name],
        {VARIANT: TEST_SETTINGS[VARIANT]},
    )


@pytest.mark.parametrize("uniref_variant_value", UNIREF_VARIANTS)
def test_settings_valid_variants_accepted(uniref_variant_value: str) -> None:
    """Ensure that each valid variant value is accepted without error."""
    s = UnirefSettings(VARIANT=uniref_variant_value)  # pyright: ignore[reportCallIssue]
    assert isinstance(s, UnirefSettings)
    assert s.variant == uniref_variant_value


@pytest.mark.parametrize("uniref_variant_value", UNIREF_VARIANTS)
@pytest.mark.parametrize(VARIANT, UNIREF_VARIANT_ALIASES)
def test_cli_valid_variants_accepted(uniref_variant_value: str, variant: str) -> None:
    """Ensure that each valid uniref variant value is accepted without error when passed via CLI."""
    s = CliApp.run(
        UnirefSettings,
        cli_args=[make_cli_arg(variant), uniref_variant_value],
    )
    assert isinstance(s, UnirefSettings)
    assert s.variant == uniref_variant_value


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
def test_invalid_variant_raises(value: str) -> None:
    """Ensure that an unrecognised uniref variant raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        UnirefSettings(VARIANT=value)  # pyright: ignore[reportCallIssue]

    exc_message = str(exc_info.value)
    assert "Input should be '50', '90' or '100'" in exc_message


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize(VARIANT, UNIREF_VARIANT_ALIASES)
def test_cli_invalid_variant_via_cli_raises(
    value: str,
    variant: str,
) -> None:
    """Ensure that an invalid uniref variant passed via CLI raises an error."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(UnirefSettings, cli_args=[make_cli_arg(variant), value])

    exc_message = str(exc_info.value)
    assert "Input should be '50', '90' or '100'" in exc_message


def test_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref variant argument raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        UnirefSettings()  # pyright: ignore[reportCallIssue]

    exc_message = str(exc_info.value)
    assert "Field required" in exc_message


def test_cli_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref variant argument raises an error."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(UnirefSettings)

    exc_message = str(exc_info.value)
    assert "Field required" in exc_message


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize(VARIANT, UNIREF_VARIANT_ALIASES)
def test_cli_invalid_variant_and_destination_via_cli_raises(value: str, variant: str) -> None:
    """Ensure that invalid uniref variant and use_destination passed via CLI raises an error with both errors.

    N.b. Pydantic only reports the first error!!!
    """
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(
            UnirefSettings,
            cli_args=[
                make_cli_arg(variant),
                value,
                "--use_destination",
                "some invalid destination",
            ],
        )

    # Check that both errors are present in the exception message
    exc_message = str(exc_info.value)
    assert "Input should be '50', '90' or '100'" in exc_message
