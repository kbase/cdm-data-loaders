"""Exception hierarchy for the Pydantic-to-PyArrow converter."""

from typing import ClassVar, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, build_error_by_code


class PydanticToPyArrowError(ConversionError):
    """Raised when a Pydantic model construct cannot be converted to a PyArrow schema.

    Subclasses raised by
    :func:`~cdm_data_loaders.converters.facades.pydantic_to_pyarrow.generate_pyarrow_schema`
    carry a stable specification ``code`` and a short ``title``; their docstrings state why the
    construct is rejected. The converter specification document is generated from these attributes.
    """


class InvalidPydanticModelError(PydanticToPyArrowError):
    """Converter requires a concrete Pydantic `BaseModel` subclass as input."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Input is not a concrete Pydantic `BaseModel` subclass"


class NonStructRootError(PydanticToPyArrowError):
    """Root of a PyArrow schema must be a `struct` (a table has named columns, not a bare scalar, list, or map)."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Non-struct `RootModel`"


class EmptyModelError(PydanticToPyArrowError):
    """Parquet cannot persist a `struct` with zero child fields."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Empty model or tuple (zero fields)"


class UnconstrainedFieldError(PydanticToPyArrowError):
    """PyArrow requires a concrete static column type and has no dynamic `any` or untyped container column."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Unconstrained field (`Any`, `object`, untyped container, or unbound `TypeVar`)"


class OpenTupleError(PydanticToPyArrowError):
    """Positional tuple structs require a fixed element count so every position maps to a deterministic field."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Open tuple mixing positional and variadic elements"


class NonStringMapKeyError(PydanticToPyArrowError):
    """Fixed-field structs cannot mix open `extra='allow'` maps, and Arrow maps require string keys for JSONL round-tripping."""

    code: ClassVar[str] = "E6"
    title: ClassVar[str] = "Open `extra='allow'` model or non-string mapping key type"


class UnsupportedFieldTypeError(PydanticToPyArrowError):
    """Non-serializable Python runtime types and null-only fields have no valid Arrow column representation."""

    code: ClassVar[str] = "E7"
    title: ClassVar[str] = "Unsupported field type or null-only field"


class IncompatibleUnionBranchesError(PydanticToPyArrowError):
    """PyArrow cannot merge lists, maps, and structs into a single column without an opaque union type."""

    code: ClassVar[str] = "E8"
    title: ClassVar[str] = "Incompatible structural union branches"


class UnresolvedForwardRefError(PydanticToPyArrowError):
    """Schema generation requires all forward type annotations to be resolved to concrete types."""

    code: ClassVar[str] = "E9"
    title: ClassVar[str] = "Unresolvable forward reference"


class RecursiveNonJsonTypeError(PydanticToPyArrowError):
    """Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`; cyclic type aliases without a `BaseModel` boundary cannot be finitely unrolled."""

    code: ClassVar[str] = "E10"
    title: ClassVar[str] = "Non-JSON-serializable type in a recursive model cycle or cyclic type alias"


class InvalidDecimalConstraintsError(PydanticToPyArrowError):
    """Arrow `decimal128` requires `1 <= max_digits <= 38` and `0 <= decimal_places <= max_digits`."""

    code: ClassVar[str] = "E11"
    title: ClassVar[str] = "Invalid or out-of-range `Decimal` precision/scale constraints"


class ValueFieldCollisionError(PydanticToPyArrowError):
    """The synthesized `_value` field for scalar union branches would collide with the model's declared `_value` field."""

    code: ClassVar[str] = "E12"
    title: ClassVar[str] = "`_value` collision in a scalar + model union"


PYDANTIC_TO_PYARROW_ERRORS: Final[tuple[type[PydanticToPyArrowError], ...]] = (
    InvalidPydanticModelError,
    NonStructRootError,
    EmptyModelError,
    UnconstrainedFieldError,
    OpenTupleError,
    NonStringMapKeyError,
    UnsupportedFieldTypeError,
    IncompatibleUnionBranchesError,
    UnresolvedForwardRefError,
    RecursiveNonJsonTypeError,
    InvalidDecimalConstraintsError,
    ValueFieldCollisionError,
)

ERROR_BY_CODE: Final[frozendict[str, type[PydanticToPyArrowError]]] = build_error_by_code(PYDANTIC_TO_PYARROW_ERRORS)
