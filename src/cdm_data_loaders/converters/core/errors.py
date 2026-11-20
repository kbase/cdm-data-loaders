"""Shared error types and helpers for the converter packages."""

from collections.abc import Mapping
from typing import Annotated, ClassVar, NoReturn

from frozendict import frozendict
from pydantic import AfterValidator, ValidationError
from pydantic.dataclasses import rebuild_dataclass


class ConversionError(ValueError):
    """Base class for all converter errors; direction-specific errors subclass this."""

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


def _freeze_error_by_code(
    value: Mapping[str, type[ConversionError]],
) -> frozendict[str, type[ConversionError]]:
    """Freeze a validated error-code mapping into an immutable ``frozendict``.

    :param value: Validated mapping from specification code to exception class.
    :type value: Mapping[str, type[ConversionError]]
    :return: Immutable ``frozendict`` copy.
    :rtype: frozendict[str, type[ConversionError]]
    """
    return frozendict(value)


type ErrorCodeMap = Annotated[Mapping[str, type[ConversionError]], AfterValidator(_freeze_error_by_code)]


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


def unwrap_conversion_errors[ModelT](cls: type[ModelT]) -> type[ModelT]:
    """Re-raise sole ``ConversionError`` causes of Pydantic ``ValidationError`` unwrapped.

    Pydantic dataclasses wrap ``ValueError`` subclasses (including
    ``ConversionError``) raised inside validators into ``ValidationError``.
    Rebuilding the dataclass schema first and wrapping ``__init__`` preserves
    domain ``ConversionError`` contracts while keeping standard Pydantic
    ``ValidationError`` behavior for type and constraint failures.

    :param cls: Pydantic dataclass to patch.
    :type cls: type[ModelT]
    :return: ``cls`` with its ``__init__`` patched.
    :rtype: type[ModelT]
    """
    rebuild_dataclass(cls)
    generated_init = cls.__init__

    def _init(self: ModelT, *args: object, **kwargs: object) -> None:
        try:
            generated_init(self, *args, **kwargs)
        except ValidationError as error:
            causes = [item.get("ctx", {}).get("error") for item in error.errors()]
            if causes and all(isinstance(cause, ConversionError) for cause in causes):
                raise causes[0] from None
            raise

    cls.__init__ = _init  # pyright: ignore[reportAttributeAccessIssue]
    return cls
