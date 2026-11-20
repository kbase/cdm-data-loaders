"""Emit JSON Schema from typed schema facts and scoped metadata."""

from typing import Any

from pydantic import BaseModel, ConfigDict, StrictBool

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import Field, SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.emitters.json_schema.common import JSON_SCHEMA_DIALECT
from cdm_data_loaders.converters.emitters.json_schema.dlt_policy import emit_dlt_node
from cdm_data_loaders.converters.emitters.json_schema.iceberg_policy import emit_iceberg_field, emit_iceberg_node
from cdm_data_loaders.converters.emitters.json_schema.json_policy import (
    emit_json_document,
    emit_json_field,
    emit_json_node,
)
from cdm_data_loaders.converters.emitters.json_schema.pyarrow_policy import (
    emit_pyarrow_document,
    emit_pyarrow_field,
    emit_pyarrow_node,
)


class JsonSchemaEmitter(BaseModel):
    """Render typed IR trees using each source's JSON Schema compatibility policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    preserve_unknown_hints: StrictBool = True

    def emit(self, document: SchemaDocument) -> dict[str, Any]:
        """Return a fresh JSON Schema document dictionary for ``document``.

        :param document: IR schema document to emit.
        :type document: SchemaDocument
        :return: Rendered JSON Schema dictionary.
        :rtype: dict[str, Any]
        """
        provenance = document.provenance or document.root.provenance
        source = provenance.source if provenance is not None else None
        if source == "dlt":
            result = emit_dlt_node(document.root, preserve_unknown_hints=self.preserve_unknown_hints)
        elif source == "iceberg":
            result = emit_iceberg_node(document.root)
        elif source == "pyarrow":
            return emit_pyarrow_document(document)
        else:
            return emit_json_document(document)
        annotations = mutable_value(document.annotations)
        if source == "iceberg" and not annotations.get("description"):
            annotations.pop("description", None)
        result.update(annotations)
        result.update(mutable_value(document.extensions))
        result["$schema"] = document.dialect or JSON_SCHEMA_DIALECT
        if document.identifier is not None:
            result["$id"] = document.identifier
        elif source == "dlt":
            result["$id"] = f"urn:dlt:{document.name}"
        if document.name is not None:
            result["title"] = document.name
        return result

    def emit_into(self, document: SchemaDocument, schema: dict[str, Any]) -> dict[str, Any]:
        """Add ``document``'s schema under ``$defs[document.name]`` in ``schema``.

        :param document: Named, table-rooted document to convert.
        :type document: SchemaDocument
        :param schema: Destination namespace document, updated after conversion succeeds.
        :type schema: dict[str, Any]
        :return: Destination namespace document.
        :rtype: dict[str, Any]
        :raises ConversionError: If ``document.name`` is ``None``.
        """
        if document.name is None:
            msg = "A namespaced JSON Schema entry requires a named document"
            raise ConversionError(msg)
        table_schema = self.emit(document)
        table_schema.pop("$schema", None)
        schema.setdefault("$defs", {})[document.name] = table_schema
        return schema

    def emit_node(self, node: TypedNode) -> dict[str, Any] | bool:
        """Emit a standalone node using its source's rendering policy.

        :param node: IR node to emit.
        :type node: TypedNode
        :return: Rendered JSON Schema dictionary or boolean schema.
        :rtype: dict[str, Any] | bool
        """
        source = node.provenance.source if node.provenance is not None else None
        if source == "iceberg":
            return emit_iceberg_node(node)
        if source == "pyarrow":
            return emit_pyarrow_node(node)
        if source == "dlt":
            return emit_dlt_node(node, preserve_unknown_hints=self.preserve_unknown_hints)
        return emit_json_node(node)

    def emit_field(self, field: Field) -> dict[str, Any] | bool:
        """Emit a field while retaining source-specific metadata placement.

        :param field: IR field to emit.
        :type field: Field
        :return: Rendered JSON Schema dictionary or boolean schema.
        :rtype: dict[str, Any] | bool
        """
        source = field.node.provenance.source if field.node.provenance is not None else None
        if source == "iceberg":
            return emit_iceberg_field(field)
        if source == "pyarrow":
            return emit_pyarrow_field(field)
        if source == "dlt":
            return emit_dlt_node(field.node, preserve_unknown_hints=self.preserve_unknown_hints)
        return emit_json_field(field)
