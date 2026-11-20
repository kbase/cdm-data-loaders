"""Shared positive-integer recursion and reference depth validators."""

from typing import Annotated, Final

from pydantic import Field, PositiveInt

DEFAULT_MAX_RECURSION_DEPTH: Final[int] = 2


def validate_positive_int_param(value: int, name: str = "max_recursion_depth") -> int:
    """Validate that ``value`` is an integer ``>= 1`` and not a ``bool``.

    :param value: Candidate positive integer value.
    :type value: int
    :param name: Parameter name included in error messages.
    :type name: str
    :return: Validated integer ``>= 1``.
    :rtype: int
    :raises TypeError: If ``value`` is a ``bool`` or not an ``int``.
    :raises ValueError: If ``value`` is less than ``1``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{name} must be an integer >= 1"
        raise TypeError(msg)
    if value < 1:
        msg = f"{name} must be >= 1"
        raise ValueError(msg)
    return value


def validate_max_recursion_depth(depth: int) -> int:
    """Validate that ``depth`` is a positive integer (excluding booleans).

    :param depth: Recursion depth limit to validate.
    :type depth: int
    :return: Validated recursion depth integer ``>= 1``.
    :rtype: int
    :raises TypeError: If ``depth`` is a boolean or not an integer.
    :raises ValueError: If ``depth`` is less than ``1``.
    """
    return validate_positive_int_param(depth, "max_recursion_depth")


StrictPositiveRecursionDepth = Annotated[PositiveInt, Field(strict=True)]
