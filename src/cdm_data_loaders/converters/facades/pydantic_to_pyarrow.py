"""Convert Pydantic v2 BaseModel schemas to PyArrow schemas."""

from pathlib import Path
from typing import ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.converters.core.ir import SchemaDocument
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.readers.pydantic import PydanticReader

DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(max_nesting=100)


class PydanticToPyArrowError(ConversionError):
    """Raised when a Pydantic model construct cannot be converted to a PyArrow schema.

    Subclasses raised by :func:`generate_pyarrow_schema` carry a stable specification ``code`` and a short
    ``title``; their docstrings state why the construct is rejected. The converter specification document is
    generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class InvalidPydanticModelError(PydanticToPyArrowError):
    """Converter requires a concrete Pydantic `BaseModel` subclass as input."""

    code = "E1"
    title = "Input is not a concrete Pydantic `BaseModel` subclass"


class NonStructRootError(PydanticToPyArrowError):
    """Root of a PyArrow schema must be a `struct` (a table has named columns, not a bare scalar, list, or map)."""

    code = "E2"
    title = "Non-struct `RootModel`"


class EmptyModelError(PydanticToPyArrowError):
    """Parquet cannot persist a `struct` with zero child fields."""

    code = "E3"
    title = "Empty model or tuple (zero fields)"


class UnconstrainedFieldError(PydanticToPyArrowError):
    """PyArrow requires a concrete static column type and has no dynamic `any` or untyped container column."""

    code = "E4"
    title = "Unconstrained field (`Any`, `object`, untyped container, or unbound `TypeVar`)"


class OpenTupleError(PydanticToPyArrowError):
    """Positional tuple structs require a fixed element count so every position maps to a deterministic field."""

    code = "E5"
    title = "Open tuple mixing positional and variadic elements"


class NonStringMapKeyError(PydanticToPyArrowError):
    """Fixed-field structs cannot mix open `extra='allow'` maps, and Arrow maps require string keys for JSONL round-tripping."""

    code = "E6"
    title = "Open `extra='allow'` model or non-string mapping key type"


class UnsupportedFieldTypeError(PydanticToPyArrowError):
    """Non-serializable Python runtime types and null-only fields have no valid Arrow column representation."""

    code = "E7"
    title = "Unsupported field type or null-only field"


class IncompatibleUnionBranchesError(PydanticToPyArrowError):
    """PyArrow cannot merge lists, maps, and structs into a single column without an opaque union type."""

    code = "E8"
    title = "Incompatible structural union branches"


class UnresolvedForwardRefError(PydanticToPyArrowError):
    """Schema generation requires all forward type annotations to be resolved to concrete types."""

    code = "E9"
    title = "Unresolvable forward reference"


class RecursiveNonJsonTypeError(PydanticToPyArrowError):
    """Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`; cyclic type aliases without a `BaseModel` boundary cannot be finitely unrolled."""

    code = "E10"
    title = "Non-JSON-serializable type in a recursive model cycle or cyclic type alias"


CyclicTypeAliasError = RecursiveNonJsonTypeError


class InvalidDecimalConstraintsError(PydanticToPyArrowError):
    """Arrow `decimal128` requires `1 <= max_digits <= 38` and `0 <= decimal_places <= max_digits`."""

    code = "E11"
    title = "Invalid or out-of-range `Decimal` precision/scale constraints"


NullOnlyFieldError = UnsupportedFieldTypeError


class ValueFieldCollisionError(PydanticToPyArrowError):
    """The synthesized `_value` field for scalar union branches would collide with the model's declared `_value` field."""

    code = "E12"
    title = "`_value` collision in a scalar + model union"


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

ERROR_BY_CODE: Final[frozendict[str, type[PydanticToPyArrowError]]] = frozendict(
    {cls.code: cls for cls in PYDANTIC_TO_PYARROW_ERRORS}
)


def resolve_scalar_type(annotation: object) -> tuple[pa.DataType, str]:
    """Map a scalar Python / Pydantic type annotation to its PyArrow DataType and ``pydantic_type`` label.

    :param annotation: scalar type annotation (e.g. ``int``, ``AwareDatetime``, ``Annotated[Decimal, Field(...)]``)
    :type annotation: object
    :return: ``(pa.DataType, pydantic_type)``
    :rtype: tuple[pa.DataType, str]
    :raises PydanticToPyArrowError: if ``annotation`` is not a supported scalar type
    """
    node = PydanticReader(
        error_type=PydanticToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    ).read_scalar_annotation(annotation)
    dtype = DEFAULT_EMITTER.emit_node(node)
    pyd_type = str(node.provenance.metadata["pydantic_type"]) if node.provenance else ""
    return dtype, pyd_type


def generate_pyarrow_schema(
    model: type[BaseModel],
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
    by_alias: bool = False,
) -> pa.Schema:
    """Generate a deterministic PyArrow Schema from a Pydantic v2 ``BaseModel`` subclass.

    :param model: concrete Pydantic ``BaseModel`` subclass
    :type model: type[BaseModel]
    :param max_recursion_depth: maximum expansion depth for recursive ``BaseModel`` references
    :type max_recursion_depth: int
    :param by_alias: when ``True``, use ``serialization_alias`` or ``alias`` for field names
    :type by_alias: bool
    :return: PyArrow schema with top-level fields corresponding to model fields
    :rtype: pa.Schema
    :raises PydanticToPyArrowError: if ``model`` is invalid or uses an unsupported construct
    """
    doc = PydanticReader(
        max_recursion_depth=max_recursion_depth,
        by_alias=by_alias,
        error_type=PydanticToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    ).read(model)
    return DEFAULT_EMITTER.emit(doc)


class PydanticToPyArrow(BaseModel):
    """Facade converting Pydantic v2 ``BaseModel`` classes to PyArrow schemas and empty tables."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    max_recursion_depth: PositiveInt = Field(default=DEFAULT_MAX_RECURSION_DEPTH)
    by_alias: bool = False

    def _read(self, model: type[BaseModel]) -> SchemaDocument:
        """Read ``model`` into a converter IR ``SchemaDocument``.

        :param model: Concrete Pydantic ``BaseModel`` subclass.
        :type model: type[BaseModel]
        :return: Parsed ``SchemaDocument``.
        :rtype: SchemaDocument
        """
        return PydanticReader(
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            error_type=PydanticToPyArrowError,
            error_by_code=ERROR_BY_CODE,
        ).read(model)

    def convert(self, model: type[BaseModel]) -> pa.Schema:
        """Convert a Pydantic ``BaseModel`` subclass to a ``pa.Schema``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :return: converted PyArrow schema
        :rtype: pa.Schema
        """
        return DEFAULT_EMITTER.emit(self._read(model))

    def convert_to_table(self, model: type[BaseModel]) -> pa.Table:
        """Convert a Pydantic ``BaseModel`` subclass to a zero-row ``pa.Table``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :return: zero-row PyArrow table with the converted schema
        :rtype: pa.Table
        """
        return DEFAULT_EMITTER.emit_table(self._read(model))

    def convert_to_python(self, model: type[BaseModel], *, docstring: str | None = None) -> str:
        """Convert a Pydantic ``BaseModel`` subclass to a static Python schema module.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        """
        return DEFAULT_EMITTER.emit_python(self._read(model), docstring=docstring)

    def convert_to_python_file(
        self,
        model: type[BaseModel],
        output: str | Path,
        *,
        docstring: str | None = None,
    ) -> Path:
        """Convert a Pydantic ``BaseModel`` subclass and write a static Python schema module to ``output``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        """
        return DEFAULT_EMITTER.emit_python_file(self._read(model), output, docstring=docstring)
