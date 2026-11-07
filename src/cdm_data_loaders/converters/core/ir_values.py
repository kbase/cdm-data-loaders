"""Owned immutable values shared by IR models and extension contracts."""

from collections.abc import Mapping
from decimal import Decimal
from math import isfinite
from typing import overload

from frozendict import frozendict

type Value = bool | int | float | Decimal | str | tuple[Value, ...] | Mapping[str, Value] | None
type MutableValue = bool | int | float | Decimal | str | list[MutableValue] | dict[str, MutableValue] | None


@overload
def freeze_value[T: (bool, int, float, Decimal, str, None)](value: T) -> T: ...


@overload
def freeze_value(value: Mapping[str, object]) -> Mapping[str, Value]: ...


@overload
def freeze_value(value: list[object] | tuple[object, ...]) -> tuple[Value, ...]: ...


@overload
def freeze_value(value: object) -> Value: ...


def freeze_value(value: object) -> Value:
    """Own JSON-shaped values deeply, retaining finite Decimal numbers exactly."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (float, Decimal)):
        if not (value.is_finite() if isinstance(value, Decimal) else isfinite(value)):
            msg = "Schema values must be finite"
            raise ValueError(msg)
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            msg = "Schema mapping keys must be strings"
            raise TypeError(msg)
        return frozendict({key: freeze_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze_value(item) for item in value)
    msg = f"Unsupported schema value: {type(value).__name__}"
    raise TypeError(msg)


def freeze_mapping(value: Mapping[str, object]) -> Mapping[str, Value]:
    """Copy a mapping into recursively immutable schema values."""
    result = freeze_value(value)
    if not isinstance(result, Mapping):
        msg = "Expected a schema mapping"
        raise TypeError(msg)
    return result


@overload
def mutable_value[T: (bool, int, float, Decimal, str, None)](value: T) -> T: ...


@overload
def mutable_value(value: Mapping[str, Value]) -> dict[str, MutableValue]: ...


@overload
def mutable_value(value: tuple[Value, ...]) -> list[MutableValue]: ...


@overload
def mutable_value(value: Value) -> MutableValue: ...


def mutable_value(value: Value) -> MutableValue:
    """Make independent dict/list values without changing numeric precision."""
    if isinstance(value, Mapping):
        return {key: mutable_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [mutable_value(item) for item in value]
    return value
