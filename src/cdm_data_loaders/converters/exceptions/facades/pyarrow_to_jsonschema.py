"""Exception hierarchy for the PyArrow-to-JSON Schema converter."""

from typing import ClassVar, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, build_error_by_code


class PyArrowToJSONSchemaError(ConversionError):
    """Raised when a PyArrow schema, field, or data type cannot be converted to JSON Schema.

    Subclasses raised by
    :func:`~cdm_data_loaders.converters.facades.pyarrow_to_jsonschema.generate_json_schema`
    carry a stable specification ``code`` and a short ``title``; their docstrings state why the
    construct is rejected. The converter specification document is generated from these attributes.
    """


class InvalidPyArrowSchemaError(PyArrowToJSONSchemaError):
    """Schema generation requires a concrete PyArrow `pa.Schema` object; bare `pa.DataType`, `pa.Field`, or Python mappings are not table schemas."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Input is not a `pa.Schema` instance"


class EmptyStructError(PyArrowToJSONSchemaError):
    """Arrow tables and Parquet struct columns require at least one named child field."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Empty root schema or nested `pa.struct([])` (zero fields)"


class DuplicateFieldNameError(PyArrowToJSONSchemaError):
    """JSON Schema `properties` is an object mapping keyed by property name and cannot represent duplicate sibling field names."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Duplicate field name in schema or `pa.struct`"


class NullOnlyTypeError(PyArrowToJSONSchemaError):
    """A column or container element typed solely as `pa.null()` carries no value domain; nullable columns must wrap a concrete Arrow value type with `nullable=True`."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Standalone `pa.null()` field or element type"


class InvalidDecimalTypeError(PyArrowToJSONSchemaError):
    """Fixed-point decimal string patterns require `0 <= scale <= precision`; negative scales or `scale > precision` have no plain fixed-point string regex."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Decimal type with negative scale or `scale > precision`"


class UnsupportedTemporalIntervalError(PyArrowToJSONSchemaError):
    """Relative durations and `month_day_nano` intervals have no lossless canonical scalar representation across JSON Schema validators without losing unit or sign-per-component semantics."""

    code: ClassVar[str] = "E6"
    title: ClassVar[str] = "Duration or calendar interval type (`pa.duration`, `pa.month_day_nano_interval`)"


class UnsupportedUnionTypeError(PyArrowToJSONSchemaError):
    """Arrow type-code unions lack stable JSON property discriminators and Parquet storage support; union columns must be normalized before schema export."""

    code: ClassVar[str] = "E7"
    title: ClassVar[str] = "Arrow union type (`pa.sparse_union` or `pa.dense_union`)"


class UnsupportedArrowTypeError(PyArrowToJSONSchemaError):
    """Specialized physical encodings such as `pa.run_end_encoded` and custom `pa.ExtensionType` subclasses have no general target-independent JSON Schema contract."""

    code: ClassVar[str] = "E8"
    title: ClassVar[str] = "Unsupported Arrow physical or extension type (`run_end_encoded`, `ExtensionType`, etc.)"


PYARROW_TO_JSONSCHEMA_ERRORS: Final[tuple[type[PyArrowToJSONSchemaError], ...]] = (
    InvalidPyArrowSchemaError,
    EmptyStructError,
    DuplicateFieldNameError,
    NullOnlyTypeError,
    InvalidDecimalTypeError,
    UnsupportedTemporalIntervalError,
    UnsupportedUnionTypeError,
    UnsupportedArrowTypeError,
)

ERROR_BY_CODE: Final[frozendict[str, type[PyArrowToJSONSchemaError]]] = build_error_by_code(
    PYARROW_TO_JSONSCHEMA_ERRORS
)
