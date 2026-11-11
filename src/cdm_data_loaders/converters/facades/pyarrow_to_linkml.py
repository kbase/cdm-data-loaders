"""Convert PyArrow schemas, fields, and data types to official LinkML schema definitions."""

from pathlib import Path
from typing import ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from linkml_runtime.dumpers import yaml_dumper
from linkml_runtime.linkml_model.meta import SchemaDefinition, SlotDefinition
from pydantic import BaseModel, ConfigDict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.emitters.linkml import (
    EmptyLinkMLSlotNameError,
    LinkMLEmitter,
    LinkMLEmitterError,
    NestedLinkMLMultivaluedError,
)
from cdm_data_loaders.converters.readers.pyarrow import PyArrowReader


class PyArrowToLinkMLError(ConversionError):
    """Raised when a PyArrow schema, field, or data type cannot be converted to a LinkML schema.

    Subclasses raised by :func:`generate_linkml_schema` carry a stable specification ``code`` and a short
    ``title``; their docstrings state why the construct is rejected. The converter specification document is
    generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class InvalidPyArrowSchemaError(PyArrowToLinkMLError):
    """Schema generation requires a concrete PyArrow `pa.Schema` object; bare `pa.DataType`, `pa.Field`, or Python mappings are not table schemas."""

    code = "E1"
    title = "Input is not a `pa.Schema` instance"


class EmptyStructError(PyArrowToLinkMLError):
    """Arrow tables and LinkML record classes generated from Arrow structs require at least one named attribute."""

    code = "E2"
    title = "Empty root schema or nested `pa.struct([])` (zero fields)"


class DuplicateFieldNameError(PyArrowToLinkMLError):
    """LinkML class `attributes` is keyed by slot name and cannot represent duplicate sibling field names."""

    code = "E3"
    title = "Duplicate field name in schema or `pa.struct`"


class NullOnlyTypeError(PyArrowToLinkMLError):
    """A column or container element typed solely as `pa.null()` carries no value domain; nullable columns must wrap a concrete Arrow value type with `nullable=True`."""

    code = "E4"
    title = "Standalone `pa.null()` field or element type"


class InvalidDecimalTypeError(PyArrowToLinkMLError):
    """Fixed-point decimal representations require `0 <= scale <= precision`; negative scales or `scale > precision` have no plain decimal slot representation."""

    code = "E5"
    title = "Decimal type with negative scale or `scale > precision`"


class UnsupportedTemporalIntervalError(PyArrowToLinkMLError):
    """Relative durations and `month_day_nano` intervals have no lossless built-in `linkml:types` scalar range."""

    code = "E6"
    title = "Duration or calendar interval type (`pa.duration`, `pa.month_day_nano_interval`)"


class UnsupportedUnionTypeError(PyArrowToLinkMLError):
    """Arrow type-code unions cannot be represented as typed LinkML slots without class/union redesign."""

    code = "E7"
    title = "Arrow union type (`pa.sparse_union` or `pa.dense_union`)"


class UnsupportedArrowTypeError(PyArrowToLinkMLError):
    """Specialized physical encodings such as `pa.run_end_encoded` and custom `pa.ExtensionType` subclasses have no general LinkML mapping."""

    code = "E8"
    title = "Unsupported Arrow physical or extension type (`run_end_encoded`, `ExtensionType`, etc.)"


class EmptyFieldNameError(PyArrowToLinkMLError):
    """LinkML `SlotDefinition` identifiers require a non-empty attribute name; blank Arrow field names cannot form valid LinkML slots."""

    code = "E9"
    title = "Blank or empty field name"


class NestedMultivaluedSlotError(PyArrowToLinkMLError):
    """LinkML slots use a flat `multivalued: true` flag on a single `range` and cannot represent multidimensional lists or lists of map-entry lists without intermediate wrapper classes."""

    code = "E10"
    title = "Directly nested list or map inside a list (`list<list<...>>` or `list<map<...>>`)"


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

ERROR_BY_CODE: Final[frozendict[str, type[PyArrowToLinkMLError]]] = frozendict(
    {cls.code: cls for cls in PYARROW_TO_LINKML_ERRORS}
)


def _reader() -> PyArrowReader:
    """Construct a ``PyArrowReader`` bound to ``PyArrowToLinkMLError`` codes.

    :return: Configured ``PyArrowReader`` instance.
    :rtype: PyArrowReader
    """
    return PyArrowReader(
        include_pyarrow_metadata=False,
        error_type=PyArrowToLinkMLError,
        error_by_code=ERROR_BY_CODE,
    )


def _translate_linkml_error(exc: ConversionError) -> PyArrowToLinkMLError:
    """Translate ``LinkMLEmitterError`` and ``ConversionError`` instances into numbered ``PyArrowToLinkMLError``s.

    :param exc: Source conversion or emitter error.
    :type exc: ConversionError
    :return: Mapped ``PyArrowToLinkMLError`` instance.
    :rtype: PyArrowToLinkMLError
    """
    if isinstance(exc, PyArrowToLinkMLError):
        return exc
    if isinstance(exc, EmptyLinkMLSlotNameError):
        return EmptyFieldNameError(str(exc))
    if isinstance(exc, NestedLinkMLMultivaluedError):
        return NestedMultivaluedSlotError(str(exc))
    return PyArrowToLinkMLError(str(exc))


def convert_type(dtype: pa.DataType) -> SlotDefinition:
    """Convert a standalone PyArrow ``DataType`` into a LinkML ``SlotDefinition``.

    :param dtype: PyArrow data type to convert.
    :type dtype: pa.DataType
    :return: Emitted LinkML ``SlotDefinition`` named ``node``.
    :rtype: SlotDefinition
    :raises PyArrowToLinkMLError: If ``dtype`` is invalid or has no faithful LinkML representation.
    """
    try:
        node = _reader().read_node(dtype)
        return LinkMLEmitter().emit_node(node)
    except PyArrowToLinkMLError:
        raise
    except (LinkMLEmitterError, ConversionError) as exc:
        raise _translate_linkml_error(exc) from exc


def resolve_scalar_type(dtype: pa.DataType) -> SlotDefinition:
    """Resolve a scalar (or dictionary-encoded scalar) PyArrow ``DataType`` to its LinkML ``SlotDefinition``.

    :param dtype: Scalar PyArrow data type.
    :type dtype: pa.DataType
    :return: Emitted LinkML ``SlotDefinition`` for ``dtype``.
    :rtype: SlotDefinition
    :raises PyArrowToLinkMLError: If ``dtype`` is not a supported scalar or dictionary type.
    """
    if isinstance(
        dtype,
        pa.StructType
        | pa.ListType
        | pa.LargeListType
        | pa.ListViewType
        | pa.LargeListViewType
        | pa.FixedSizeListType
        | pa.MapType,
    ):
        msg = f"Expected a scalar PyArrow DataType in resolve_scalar_type, got container type {dtype}."
        raise InvalidPyArrowSchemaError(msg)
    return convert_type(dtype)


def convert_field(pa_field: pa.Field) -> SlotDefinition:
    """Convert a PyArrow ``Field`` into a LinkML ``SlotDefinition`` with cardinality and description.

    :param pa_field: PyArrow field to convert.
    :type pa_field: pa.Field
    :return: Named LinkML ``SlotDefinition``.
    :rtype: SlotDefinition
    :raises PyArrowToLinkMLError: If ``pa_field`` is invalid or has no faithful LinkML representation.
    """
    try:
        ir_field = _reader().read_field(pa_field)
        return LinkMLEmitter().emit_field(ir_field)
    except PyArrowToLinkMLError:
        raise
    except (LinkMLEmitterError, ConversionError) as exc:
        raise _translate_linkml_error(exc) from exc


def generate_linkml_schema(
    schema: pa.Schema,
    *,
    schema_name: str | None = None,
    schema_id: str | None = None,
    destination: SchemaDefinition | None = None,
) -> SchemaDefinition:
    """Generate an official LinkML ``SchemaDefinition`` from a PyArrow ``pa.Schema``.

    :param schema: Input PyArrow table schema.
    :type schema: pa.Schema
    :param schema_name: Optional LinkML schema and root class name (defaults to ``metadata["title"]`` or ``"schema"`` / ``"Root"``).
    :type schema_name: str | None
    :param schema_id: Optional LinkML schema ``id`` URI (defaults to ``metadata["$id"]`` or ``urn:linkml:<name>``).
    :type schema_id: str | None
    :param destination: Optional existing ``SchemaDefinition`` to append generated classes into.
    :type destination: SchemaDefinition | None
    :return: Emitted LinkML ``SchemaDefinition``, or ``destination`` when provided.
    :rtype: SchemaDefinition
    :raises PyArrowToLinkMLError: If ``schema`` is invalid or uses constructs unsupported by LinkML.
    """
    try:
        doc = _reader().read(schema, schema_name=schema_name, schema_id=schema_id)
        emitter = LinkMLEmitter(schema_name=schema_name)
        return emitter.emit(doc) if destination is None else emitter.emit_into(doc, destination)
    except PyArrowToLinkMLError:
        raise
    except (LinkMLEmitterError, ConversionError) as exc:
        raise _translate_linkml_error(exc) from exc


def generate_linkml_yaml(
    schema: pa.Schema,
    *,
    schema_name: str | None = None,
    schema_id: str | None = None,
) -> str:
    """Generate a LinkML YAML schema string from a PyArrow ``pa.Schema``.

    :param schema: Input PyArrow table schema.
    :type schema: pa.Schema
    :param schema_name: Optional LinkML schema and root class name.
    :type schema_name: str | None
    :param schema_id: Optional LinkML schema ``id`` URI.
    :type schema_id: str | None
    :return: Serialized LinkML YAML string.
    :rtype: str
    """
    model = generate_linkml_schema(schema, schema_name=schema_name, schema_id=schema_id)
    return yaml_dumper.dumps(model)


class PyArrowToLinkML(BaseModel):
    """Frozen facade converting PyArrow ``pa.Schema`` objects to LinkML ``SchemaDefinition`` models and YAML."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    schema_name: str | None = None
    schema_id: str | None = None

    def convert(self, schema: pa.Schema) -> SchemaDefinition:
        """Convert a PyArrow ``pa.Schema`` into a LinkML ``SchemaDefinition``.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :return: Emitted LinkML ``SchemaDefinition``.
        :rtype: SchemaDefinition
        """
        return generate_linkml_schema(
            schema,
            schema_name=self.schema_name,
            schema_id=self.schema_id,
        )

    def convert_to_yaml(self, schema: pa.Schema) -> str:
        """Convert a PyArrow ``pa.Schema`` into a LinkML YAML string.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :return: Serialized LinkML YAML string.
        :rtype: str
        """
        return yaml_dumper.dumps(self.convert(schema))

    def convert_to_yaml_file(self, schema: pa.Schema, output: str | Path) -> Path:
        """Convert a PyArrow ``pa.Schema`` and write the LinkML YAML schema to ``output``.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :param output: Destination YAML file path.
        :type output: str | Path
        :return: Resolved output ``Path``.
        :rtype: Path
        """
        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(self.convert_to_yaml(schema), encoding="utf-8")
        return out_path
