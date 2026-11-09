"""Convert Pydantic v2 BaseModel schemas to PyArrow schemas."""

from pathlib import Path
from typing import Final

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.converters.core.ir import SchemaDocument
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.readers.pydantic import PydanticReader

DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(max_nesting=100)


def resolve_scalar_type(annotation: object) -> tuple[pa.DataType, str]:
    """Map a scalar Python / Pydantic type annotation to its PyArrow DataType and ``pydantic_type`` label.

    :param annotation: scalar type annotation (e.g. ``int``, ``AwareDatetime``, ``Annotated[Decimal, Field(...)]``)
    :type annotation: object
    :return: ``(pa.DataType, pydantic_type)``
    :rtype: tuple[pa.DataType, str]
    :raises PydanticToPyArrowError: if ``annotation`` is not a supported scalar type
    """
    node = PydanticReader().read_scalar_annotation(annotation)
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
    doc = PydanticReader(max_recursion_depth=max_recursion_depth, by_alias=by_alias).read(model)
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
        return PydanticReader(max_recursion_depth=self.max_recursion_depth, by_alias=self.by_alias).read(model)

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
