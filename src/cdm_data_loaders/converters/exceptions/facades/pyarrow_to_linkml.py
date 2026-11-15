"""Exception hierarchy for the PyArrow-to-LinkML converter."""

from typing import ClassVar, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, build_error_by_code


class PyArrowToLinkMLError(ConversionError):
    """Raised when a PyArrow schema, field, or data type cannot be converted to a LinkML schema.

    Subclasses raised by
    :func:`~cdm_data_loaders.converters.facades.pyarrow_to_linkml.generate_linkml_schema`
    carry a stable specification ``code`` and a short ``title``; their docstrings state why the
    construct is rejected. The converter specification document is generated from these attributes.
    """


class InvalidPyArrowSchemaError(PyArrowToLinkMLError):
    """Schema generation requires a concrete PyArrow `pa.Schema` object; bare `pa.DataType`, `pa.Field`, or Python mappings are not table schemas."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Input is not a `pa.Schema` instance"


class EmptyStructError(PyArrowToLinkMLError):
    """Arrow tables and LinkML record classes generated from Arrow structs require at least one named attribute."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Empty root schema or nested `pa.struct([])` (zero fields)"


class DuplicateFieldNameError(PyArrowToLinkMLError):
    """LinkML class `attributes` is keyed by slot name and cannot represent duplicate sibling field names."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Duplicate field name in schema or `pa.struct`"


class NullOnlyTypeError(PyArrowToLinkMLError):
    """A column or container element typed solely as `pa.null()` carries no value domain; nullable columns must wrap a concrete Arrow value type with `nullable=True`."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Standalone `pa.null()` field or element type"


class InvalidDecimalTypeError(PyArrowToLinkMLError):
    """Fixed-point decimal representations require `0 <= scale <= precision`; negative scales or `scale > precision` have no plain decimal slot representation."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Decimal type with negative scale or `scale > precision`"


class UnsupportedTemporalIntervalError(PyArrowToLinkMLError):
    """Relative durations and `month_day_nano` intervals have no lossless built-in `linkml:types` scalar range."""

    code: ClassVar[str] = "E6"
    title: ClassVar[str] = "Duration or calendar interval type (`pa.duration`, `pa.month_day_nano_interval`)"


class UnsupportedUnionTypeError(PyArrowToLinkMLError):
    """Arrow type-code unions cannot be represented as typed LinkML slots without class/union redesign."""

    code: ClassVar[str] = "E7"
    title: ClassVar[str] = "Arrow union type (`pa.sparse_union` or `pa.dense_union`)"


class UnsupportedArrowTypeError(PyArrowToLinkMLError):
    """Specialized physical encodings such as `pa.run_end_encoded` and custom `pa.ExtensionType` subclasses have no general LinkML mapping."""

    code: ClassVar[str] = "E8"
    title: ClassVar[str] = "Unsupported Arrow physical or extension type (`run_end_encoded`, `ExtensionType`, etc.)"


class EmptyFieldNameError(PyArrowToLinkMLError):
    """LinkML `SlotDefinition` identifiers require a non-empty attribute name; blank Arrow field names cannot form valid LinkML slots."""

    code: ClassVar[str] = "E9"
    title: ClassVar[str] = "Blank or empty field name"


class NestedMultivaluedSlotError(PyArrowToLinkMLError):
    """LinkML slots use a flat `multivalued: true` flag on a single `range` and cannot represent multidimensional lists or lists of map-entry lists without intermediate wrapper classes."""

    code: ClassVar[str] = "E10"
    title: ClassVar[str] = "Directly nested list or map inside a list (`list<list<...>>` or `list<map<...>>`)"


PYARROW_TO_LINKML_ERRORS: Final[tuple[type[PyArrowToLinkMLError], ...]] = (
    InvalidPyArrowSchemaError,
    EmptyStructError,
    DuplicateFieldNameError,
    NullOnlyTypeError,
    InvalidDecimalTypeError,
    UnsupportedTemporalIntervalError,
    UnsupportedUnionTypeError,
    UnsupportedArrowTypeError,
    EmptyFieldNameError,
    NestedMultivaluedSlotError,
)

ERROR_BY_CODE: Final[frozendict[str, type[PyArrowToLinkMLError]]] = build_error_by_code(PYARROW_TO_LINKML_ERRORS)
