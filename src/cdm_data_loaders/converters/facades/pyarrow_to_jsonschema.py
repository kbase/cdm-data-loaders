"""Convert PyArrow schemas, fields, and data types to Draft 2020-12 JSON Schema documents."""

import json
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, InstanceOf

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import (
    DEFAULT_EXTENSIONS,
    ExtensionError,
    ExtensionRegistry,
    default_extension_registry,
)
from cdm_data_loaders.converters.emitters.json_schema.emitter import JsonSchemaEmitter
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_jsonschema import (
    ERROR_BY_CODE,
    InvalidPyArrowSchemaError,
    PyArrowToJSONSchemaError,
)
from cdm_data_loaders.converters.sources.pyarrow import PyArrowReader


def _reader(
    *,
    include_pyarrow_metadata: bool = True,
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS,
) -> PyArrowReader:
    """Construct a configured ``PyArrowReader`` bound to ``PyArrowToJSONSchemaError`` codes.

    :param include_pyarrow_metadata: Whether to attach ``x-pyarrow`` extension blocks.
    :type include_pyarrow_metadata: bool
    :param extension_registry: Extension namespace contract registry.
    :type extension_registry: ExtensionRegistry
    :return: Configured ``PyArrowReader`` instance.
    :rtype: PyArrowReader
    """
    return PyArrowReader(
        extension_registry=extension_registry,
        include_pyarrow_metadata=include_pyarrow_metadata,
        error_type=PyArrowToJSONSchemaError,
        error_by_code=ERROR_BY_CODE,
    )


def convert_type(
    dtype: pa.DataType,
    *,
    nullable: bool = False,
    include_pyarrow_metadata: bool = True,
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS,
) -> dict[str, Any]:
    """Convert a standalone PyArrow ``DataType`` into a Draft 2020-12 JSON Schema subschema.

    :param dtype: PyArrow data type to convert.
    :type dtype: pa.DataType
    :param nullable: Whether to wrap the subschema in a nullable ``anyOf`` union.
    :type nullable: bool
    :param include_pyarrow_metadata: Whether to include ``x-pyarrow`` type metadata.
    :type include_pyarrow_metadata: bool
    :param extension_registry: Registry for extension validation.
    :type extension_registry: ExtensionRegistry
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    :raises PyArrowToJSONSchemaError: If ``dtype`` is invalid or unsupported.
    """
    try:
        node = _reader(
            include_pyarrow_metadata=include_pyarrow_metadata,
            extension_registry=extension_registry,
        ).read_node(dtype, nullable=nullable)
        return cast("dict[str, Any]", JsonSchemaEmitter().emit_node(node))
    except PyArrowToJSONSchemaError:
        raise
    except (ExtensionError, ConversionError) as exc:
        raise PyArrowToJSONSchemaError(str(exc)) from exc


def resolve_scalar_type(
    dtype: pa.DataType,
    *,
    include_pyarrow_metadata: bool = True,
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS,
) -> dict[str, Any]:
    """Resolve a scalar (or dictionary-encoded scalar) PyArrow ``DataType`` to its JSON Schema fragment.

    :param dtype: Scalar PyArrow data type.
    :type dtype: pa.DataType
    :param include_pyarrow_metadata: Whether to include ``x-pyarrow`` type metadata.
    :type include_pyarrow_metadata: bool
    :param extension_registry: Registry for extension validation.
    :type extension_registry: ExtensionRegistry
    :return: Rendered JSON Schema dictionary for ``dtype``.
    :rtype: dict[str, Any]
    :raises PyArrowToJSONSchemaError: If ``dtype`` is not a supported scalar or dictionary type.
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
    return convert_type(
        dtype,
        nullable=False,
        include_pyarrow_metadata=include_pyarrow_metadata,
        extension_registry=extension_registry,
    )


def convert_field(
    pa_field: pa.Field,
    *,
    include_pyarrow_metadata: bool = True,
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS,
) -> dict[str, Any]:
    """Convert a PyArrow ``Field`` into a JSON Schema property subschema with nullability and field metadata.

    :param pa_field: PyArrow field to convert.
    :type pa_field: pa.Field
    :param include_pyarrow_metadata: Whether to include ``x-pyarrow`` metadata.
    :type include_pyarrow_metadata: bool
    :param extension_registry: Registry for extension validation.
    :type extension_registry: ExtensionRegistry
    :return: Rendered JSON Schema property dictionary.
    :rtype: dict[str, Any]
    :raises PyArrowToJSONSchemaError: If ``pa_field`` is invalid or uses an unsupported type.
    """
    try:
        ir_field = _reader(
            include_pyarrow_metadata=include_pyarrow_metadata,
            extension_registry=extension_registry,
        ).read_field(pa_field)
        return cast("dict[str, Any]", JsonSchemaEmitter().emit_field(ir_field))
    except PyArrowToJSONSchemaError:
        raise
    except (ExtensionError, ConversionError) as exc:
        raise PyArrowToJSONSchemaError(str(exc)) from exc


def generate_json_schema(
    schema: pa.Schema,
    *,
    schema_name: str | None = None,
    schema_id: str | None = None,
    include_pyarrow_metadata: bool = True,
    destination: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate a Draft 2020-12 JSON Schema document from a PyArrow ``pa.Schema``.

    :param schema: Input PyArrow table schema.
    :type schema: pa.Schema
    :param schema_name: Optional schema ``title`` (and ``$defs`` key when ``destination`` is provided).
    :type schema_name: str | None
    :param schema_id: Optional schema ``$id`` URI.
    :type schema_id: str | None
    :param include_pyarrow_metadata: Whether to emit ``x-pyarrow`` extension blocks.
    :type include_pyarrow_metadata: bool
    :param destination: Optional namespace JSON Schema document to populate under ``$defs``.
    :type destination: dict[str, Any] | None
    :return: Draft 2020-12 JSON Schema dictionary, or ``destination`` when provided.
    :rtype: dict[str, Any]
    :raises PyArrowToJSONSchemaError: If ``schema`` is invalid or contains unsupported Arrow types.
    """
    converter = PyArrowToJSONSchema(
        schema_name=schema_name,
        schema_id=schema_id,
        include_pyarrow_metadata=include_pyarrow_metadata,
    )
    return converter.convert(schema) if destination is None else converter.convert_into(schema, destination)


class PyArrowToJSONSchema(BaseModel):
    """Frozen facade converting PyArrow ``pa.Schema`` objects to Draft 2020-12 JSON Schema documents."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    schema_name: str | None = None
    schema_id: str | None = None
    include_pyarrow_metadata: bool = True
    extension_registry: InstanceOf[ExtensionRegistry] = Field(default_factory=default_extension_registry)

    def _emit_schema(
        self,
        schema: pa.Schema,
        *,
        destination: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Read ``schema`` and emit either a standalone JSON Schema or a ``$defs`` entry in ``destination``.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :param destination: Optional namespace JSON Schema dictionary to populate.
        :type destination: dict[str, Any] | None
        :return: Rendered JSON Schema dictionary, or ``destination`` when provided.
        :rtype: dict[str, Any]
        :raises PyArrowToJSONSchemaError: If ``schema`` is invalid or extension validation fails.
        """
        try:
            doc = _reader(
                include_pyarrow_metadata=self.include_pyarrow_metadata,
                extension_registry=self.extension_registry,
            ).read(schema, schema_name=self.schema_name, schema_id=self.schema_id)
            emitter = JsonSchemaEmitter()
            return emitter.emit(doc) if destination is None else emitter.emit_into(doc, destination)
        except PyArrowToJSONSchemaError:
            raise
        except (ExtensionError, ConversionError) as exc:
            raise PyArrowToJSONSchemaError(str(exc)) from exc

    def convert(self, schema: pa.Schema) -> dict[str, Any]:
        """Convert a PyArrow ``pa.Schema`` into a Draft 2020-12 JSON Schema dictionary.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        return self._emit_schema(schema)

    def convert_into(self, schema: pa.Schema, destination: dict[str, Any]) -> dict[str, Any]:
        """Convert ``schema`` and insert it under ``destination["$defs"][<name>]``.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :param destination: Namespace JSON Schema dictionary to populate.
        :type destination: dict[str, Any]
        :return: Updated ``destination`` dictionary.
        :rtype: dict[str, Any]
        """
        return self._emit_schema(schema, destination=destination)

    def convert_to_json(self, schema: pa.Schema, *, indent: int = 2) -> str:
        """Convert a PyArrow ``pa.Schema`` into a formatted JSON Schema string.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :param indent: JSON indentation spaces (default ``2``).
        :type indent: int
        :return: Formatted JSON string with trailing newline.
        :rtype: str
        """
        return json.dumps(self.convert(schema), indent=indent) + "\n"

    def convert_to_json_file(self, schema: pa.Schema, output: str | Path, *, indent: int = 2) -> Path:
        """Convert a PyArrow ``pa.Schema`` and write the resulting JSON Schema to ``output``.

        :param schema: Input PyArrow schema.
        :type schema: pa.Schema
        :param output: Destination file path.
        :type output: str | Path
        :param indent: JSON indentation spaces (default ``2``).
        :type indent: int
        :return: Resolved output ``Path``.
        :rtype: Path
        """
        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(self.convert_to_json(schema, indent=indent), encoding="utf-8")
        return out_path
