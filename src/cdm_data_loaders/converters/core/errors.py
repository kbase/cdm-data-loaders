"""Shared error types and helpers for the converter packages."""

from typing import ClassVar

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
