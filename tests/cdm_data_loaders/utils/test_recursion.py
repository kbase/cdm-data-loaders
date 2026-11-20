"""Unit tests for ``cdm_data_loaders.utils.recursion``."""

from typing import Final

import pytest
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.utils.recursion import (
    DEFAULT_MAX_RECURSION_DEPTH,
    StrictPositiveRecursionDepth,
    validate_max_recursion_depth,
    validate_positive_int_param,
)

CUSTOM_DEPTH: Final[int] = 3


class _DepthModel(BaseModel):
    depth: StrictPositiveRecursionDepth = DEFAULT_MAX_RECURSION_DEPTH


@pytest.mark.parametrize(
    ("value", "name"),
    [
        pytest.param(1, "max_recursion_depth", id="one-default-name"),
        pytest.param(2, "max_recursion_depth", id="two-default-name"),
        pytest.param(50, "max_ref_depth", id="fifty-custom-name"),
    ],
)
def test_validate_positive_int_param_pass_valid_values(value: int, name: str) -> None:
    """Return positive integers unchanged for both default and custom parameter names."""
    assert validate_positive_int_param(value, name) == value


@pytest.mark.parametrize("depth", [pytest.param(1, id="one"), pytest.param(4, id="four")])
def test_validate_max_recursion_depth_pass_valid_depths(depth: int) -> None:
    """Return positive integer recursion depths unchanged."""
    assert validate_max_recursion_depth(depth) == depth


positive_int_params = [
    pytest.param(0, "max_recursion_depth", ValueError, "must be >= 1", id="zero"),
    pytest.param(-3, "max_ref_depth", ValueError, "must be >= 1", id="negative"),
    pytest.param(True, "max_recursion_depth", TypeError, "must be an integer >= 1", id="bool-true"),
    pytest.param(False, "max_ref_depth", TypeError, "must be an integer >= 1", id="bool-false"),
    pytest.param(1.5, "max_recursion_depth", TypeError, "must be an integer >= 1", id="float"),
    pytest.param("2", "max_recursion_depth", TypeError, "must be an integer >= 1", id="string"),
    pytest.param(None, "max_recursion_depth", TypeError, "must be an integer >= 1", id="none"),
]


@pytest.mark.parametrize(("bad_value", "name", "exc_cls", "match"), positive_int_params)
def test_validate_positive_int_param_fail_invalid_values(
    bad_value: object,
    name: str,
    exc_cls: type[Exception],
    match: str,
) -> None:
    """Reject booleans, non-integers, zero, and negative integers."""
    with pytest.raises(exc_cls, match=f"{name} {match}"):
        validate_positive_int_param(bad_value, name)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param({}, DEFAULT_MAX_RECURSION_DEPTH, id="default-depth"),
        pytest.param({"depth": CUSTOM_DEPTH}, CUSTOM_DEPTH, id="custom-depth"),
    ],
)
def test_strict_positive_recursion_depth_pydantic_field_pass_valid_inputs(
    kwargs: dict[str, int],
    expected: int,
) -> None:
    """Accept default and positive integer recursion depths in Pydantic models."""
    assert _DepthModel(**kwargs).depth == expected


@pytest.mark.parametrize(
    ("bad_value", "match"),
    [
        pytest.param(0, "Input should be greater than 0", id="zero"),
        pytest.param(-3, "Input should be greater than 0", id="negative"),
        pytest.param(True, "Input should be a valid integer", id="bool-true"),
        pytest.param(False, "Input should be a valid integer", id="bool-false"),
        pytest.param(1.5, "Input should be a valid integer", id="float"),
        pytest.param("2", "Input should be a valid integer", id="string"),
        pytest.param(None, "Input should be a valid integer", id="none"),
    ],
)
def test_strict_positive_recursion_depth_pydantic_field_fail_invalid_inputs(
    bad_value: object,
    match: str,
) -> None:
    """Reject booleans, strings, floats, zero, and negative values in Pydantic models."""
    with pytest.raises(ValidationError, match=match):
        _DepthModel(depth=bad_value)  # type: ignore[arg-type]
