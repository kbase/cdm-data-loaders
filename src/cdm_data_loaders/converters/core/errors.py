"""Shared error types and helpers for the converter packages."""

from collections.abc import Mapping
from typing import ClassVar, NoReturn

from frozendict import frozendict


class ConversionError(ValueError):
    """Base class for all converter errors; direction-specific errors subclass this."""

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


def build_error_by_code[E: ConversionError](errors: tuple[type[E], ...]) -> frozendict[str, type[E]]:
    """Build an immutable mapping from specification ``code`` to exception class.

    :param errors: Ordered tuple of numbered ``ConversionError`` subclasses.
    :type errors: tuple[type[E], ...]
    :return: Read-only mapping from ``cls.code`` to ``cls``.
    :rtype: frozendict[str, type[E]]
    """
    return frozendict({cls.code: cls for cls in errors})


def raise_spec_error(
    code: str,
    msg: str,
    *,
    error_type: type[ConversionError] = ConversionError,
    error_by_code: Mapping[str, type[ConversionError]] = frozendict(),
    cause: BaseException | None = None,
) -> NoReturn:
    """Raise the configured ``ConversionError`` subclass for ``code`` or fall back to ``error_type``.

    :param code: Specification error code (e.g. ``E1``..``E12``).
    :type code: str
    :param msg: Diagnostic error message.
    :type msg: str
    :param error_type: Fallback error class when ``code`` is not in ``error_by_code``.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :param cause: Optional underlying exception to chain via ``raise ... from cause``.
    :type cause: BaseException | None
    :raises ConversionError: Always raised with the resolved error class.
    """
    cls = error_by_code.get(code, error_type)
    if cause is not None:
        raise cls(msg) from cause
    raise cls(msg)
