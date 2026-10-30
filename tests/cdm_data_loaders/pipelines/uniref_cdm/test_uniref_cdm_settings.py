"""Tests for the UniRef DLT pipeline."""

from typing import Any

import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.uniref_cdm import (
    UNIREF_VARIANTS,
    VARIANT,
    UnirefSettings,
)
from tests.cdm_data_loaders.conftest import check_settings
from tests.cdm_data_loaders.pipelines.uniref_cdm.conftest import (
    UNIREF_VARIANT_ALIASES,
)
from tests.helpers import assert_cli_field_roundtrips, make_cli_arg, settings_to_argv


def test_uniref_settings_all_params_set(
    test_uniref_settings: frozendict[str, Any],
    test_uniref_settings_reconciled: frozendict[str, Any],
) -> None:
    """Ensure that settings are set correctly when all args are specified."""
    s = UnirefSettings(**test_uniref_settings)  # type: ignore[reportReturnType]
    check_settings(s, test_uniref_settings_reconciled)


@pytest.mark.parametrize("field_name", list(UnirefSettings.model_fields))
def test_cli_fields_parse_correctly(
    field_name: str,
    test_uniref_settings: frozendict[str, Any],
    test_uniref_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    assert_cli_field_roundtrips(
        UnirefSettings,
        field_name,
        test_uniref_settings[field_name],
        test_uniref_settings_reconciled[field_name],
        {**test_defaults, VARIANT: test_uniref_settings[VARIANT]},
    )


def _instantiate_uniref_settings(
    test_defaults: dict[str, Any],
    *,
    use_cli: bool = False,
    variant_alias: str = VARIANT,
    variant_value: str | None = None,
) -> UnirefSettings:
    """Instantiate UnirefSettings either directly or via CliApp.run."""
    if not use_cli:
        kwargs = {**test_defaults, VARIANT: variant_value} if variant_value is not None else test_defaults
        return UnirefSettings(**kwargs)  # pyright: ignore[reportCallIssue]
    base_settings = CtsSettings(**test_defaults)
    cli_args = settings_to_argv(base_settings)
    if variant_value is not None:
        cli_args = [*cli_args, make_cli_arg(variant_alias), variant_value]
    return CliApp.run(UnirefSettings, cli_args=cli_args)


@pytest.mark.parametrize("uniref_variant_value", UNIREF_VARIANTS)
@pytest.mark.parametrize("variant_alias", [None, *UNIREF_VARIANT_ALIASES])
def test_valid_variants_accepted(
    uniref_variant_value: str,
    variant_alias: str | None,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that each valid uniref variant value is accepted via direct instantiation and CLI."""
    s = _instantiate_uniref_settings(
        test_defaults,
        use_cli=variant_alias is not None,
        variant_alias=variant_alias or VARIANT,
        variant_value=uniref_variant_value,
    )
    assert isinstance(s, UnirefSettings)
    assert s.variant == uniref_variant_value


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize("variant_alias", [None, *UNIREF_VARIANT_ALIASES])
def test_invalid_variant_raises(
    value: str,
    variant_alias: str | None,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that an unrecognised uniref variant raises a ValidationError via direct instantiation and CLI."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        _instantiate_uniref_settings(
            test_defaults,
            use_cli=variant_alias is not None,
            variant_alias=variant_alias or VARIANT,
            variant_value=value,
        )

    assert "Input should be '50', '90' or '100'" in str(exc_info.value)


@pytest.mark.parametrize("use_cliapp", [False, True])
def test_missing_required_uniref_variant_raises(use_cliapp: bool, test_defaults: dict[str, Any]) -> None:
    """Ensure that omitting the required uniref variant argument raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        _instantiate_uniref_settings(test_defaults, use_cli=use_cliapp)

    assert "Field required" in str(exc_info.value)
