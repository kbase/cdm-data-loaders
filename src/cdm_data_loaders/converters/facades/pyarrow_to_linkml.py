"""Convert PyArrow schemas, fields, and data types to official LinkML schema definitions."""

from pathlib import Path

import pyarrow as pa
from linkml_runtime.dumpers import yaml_dumper
from linkml_runtime.linkml_model.meta import SchemaDefinition, SlotDefinition
from pydantic import BaseModel, ConfigDict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.emitters.linkml import LinkMLEmitter
from cdm_data_loaders.converters.exceptions.emitters.linkml import (
    EmptyLinkMLSlotNameError,
    LinkMLEmitterError,
    NestedLinkMLMultivaluedError,
)
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_linkml import (
    ERROR_BY_CODE,
    EmptyFieldNameError,
    InvalidPyArrowSchemaError,
    NestedMultivaluedSlotError,
    PyArrowToLinkMLError,
)
from cdm_data_loaders.converters.sources.pyarrow import PyArrowReader


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
