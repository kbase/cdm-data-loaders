"""Convert Iceberg types and loaded tables through typed readers and emitters."""

from typing import Any, cast

from pyiceberg.table import Table
from pyiceberg.types import IcebergType, NestedField, StructType

from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry
from cdm_data_loaders.converters.emitters.linkml import LinkMLEmitter
from cdm_data_loaders.converters.readers.iceberg import IcebergReader

__all__ = [
    "convert_field",
    "convert_struct",
    "convert_type",
    "table_to_linkml",
]


def convert_type(
    field_type: IcebergType, *, extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
) -> dict[str, Any]:
    """Read and emit a single Iceberg type."""
    return cast(
        "dict[str, Any]",
        LinkMLEmitter().emit_node(IcebergReader(extension_registry, mapping_target="LinkML").read_node(field_type)),
    )


def convert_field(field: NestedField, *, extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS) -> dict[str, Any]:
    """Read and emit a field with its nullable wrapper and scoped metadata."""
    return cast(
        "dict[str, Any]",
        LinkMLEmitter().emit_field(IcebergReader(extension_registry, mapping_target="LinkML").read_field(field)),
    )


def convert_struct(
    fields: tuple[NestedField, ...], *, extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
) -> dict[str, Any]:
    """Read and emit a struct without a document envelope."""
    return convert_type(StructType(*fields), extension_registry=extension_registry)


def table_to_linkml(
    table: Table, identifier: tuple[str, ...], *, extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
) -> dict[str, Any]:
    """Read loaded table metadata and emit a LinkML document without I/O."""
    return LinkMLEmitter().emit(
        IcebergReader(extension_registry, mapping_target="LinkML").read_table(table, identifier)
    )
