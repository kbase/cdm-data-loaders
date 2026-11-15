"""Exception hierarchy for the JSON Schema-to-PyArrow converter."""

from typing import ClassVar, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, build_error_by_code


class JSONSchemaToPyArrowError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted.

    Subclasses raised by
    :func:`~cdm_data_loaders.converters.facades.jsonschema_to_pyarrow.generate_pyarrow_schema`
    carry a stable specification ``code`` and a short ``title``; their docstrings state why the
    construct is rejected. The converter specification document is generated from these attributes.
    """


class InvalidJSONSchemaError(JSONSchemaToPyArrowError):
    """Raised when the input document is not a valid JSON Schema."""


class UnsupportedDialectError(InvalidJSONSchemaError):
    """Only JSON Schema Draft 2020-12 and the KBase Draft 2020-12 XSV metaschema are supported."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Missing or unsupported `$schema` dialect"


class NonStructRootError(JSONSchemaToPyArrowError):
    """A `pa.Schema` needs top-level columns, so the root must resolve to a fixed-property object."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Non-struct top-level schema"


class EmptyObjectError(JSONSchemaToPyArrowError):
    """Parquet cannot store a `pa.struct([])` with no child fields."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Empty object or tuple (zero fields)"


class UnconstrainedSchemaError(JSONSchemaToPyArrowError):
    """An untyped JSON value cannot be assigned a concrete Arrow `DataType`."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Unconstrained or boolean schema"


class OpenTupleError(JSONSchemaToPyArrowError):
    """A field cannot be both a positional `pa.struct` and an unbounded `pa.list_`."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Open tuple (`prefixItems` with schema-valued `items`)"


class MixedPropertiesAndMapError(JSONSchemaToPyArrowError):
    """An object cannot be both a fixed-field `pa.struct` and a dynamic `pa.map_`."""

    code: ClassVar[str] = "E6"
    title: ClassVar[str] = "Fixed `properties` mixed with a dynamic map"


class NegationError(JSONSchemaToPyArrowError):
    """A complement type does not define a positive storage `DataType`."""

    code: ClassVar[str] = "E7"
    title: ClassVar[str] = "Negation (`not`)"


class IncompatibleBranchShapesError(JSONSchemaToPyArrowError):
    """Branches with different structural shapes (object vs array, array vs scalar, object vs map) cannot share one column."""

    code: ClassVar[str] = "E8"
    title: ClassVar[str] = "Incompatible structural union branches"


class UnsupportedRefError(JSONSchemaToPyArrowError):
    """Every `$ref` must resolve to a local `#/$defs/...` definition; `$dynamicRef` and `$recursiveRef` are not supported."""

    code: ClassVar[str] = "E9"
    title: ClassVar[str] = "Unresolvable or unsupported reference"


class RecursiveNonJsonTypeError(JSONSchemaToPyArrowError):
    """Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`."""

    code: ClassVar[str] = "E10"
    title: ClassVar[str] = "Non-JSON-serializable type in a recursive `$ref` cycle"


class NullOnlySchemaError(JSONSchemaToPyArrowError):
    """Parquet has no physical storage type for a `pa.null()` column."""

    code: ClassVar[str] = "E11"
    title: ClassVar[str] = "Null-only schema"


class ValueFieldCollisionError(JSONSchemaToPyArrowError):
    """The synthesized `_value` field of a scalar + object union would collide with a declared `_value` property."""

    code: ClassVar[str] = "E12"
    title: ClassVar[str] = "`_value` collision in a scalar + object union"


JSONSCHEMA_TO_PYARROW_ERRORS: Final[tuple[type[JSONSchemaToPyArrowError], ...]] = (
    UnsupportedDialectError,
    NonStructRootError,
    EmptyObjectError,
    UnconstrainedSchemaError,
    OpenTupleError,
    MixedPropertiesAndMapError,
    NegationError,
    IncompatibleBranchShapesError,
    UnsupportedRefError,
    RecursiveNonJsonTypeError,
    NullOnlySchemaError,
    ValueFieldCollisionError,
)

ERROR_BY_CODE: Final[frozendict[str, type[JSONSchemaToPyArrowError]]] = build_error_by_code(
    JSONSCHEMA_TO_PYARROW_ERRORS
)
