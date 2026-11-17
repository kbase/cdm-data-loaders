"""Per-table schema description and Iceberg partition hints for dlt resources."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from dlt.destinations.impl.filesystem.iceberg_adapter import (
    PARTITION_HINT,
    TABLE_PROPERTIES_HINT,
    PartitionSpec,
)
from dlt.extract import DltResource
from pydantic import BaseModel, ConfigDict, InstanceOf

from cdm_data_loaders.core.registry import RegistryKey

if TYPE_CHECKING:
    from dlt.common.schema.typing import TAnySchemaColumns


def extract_schema_descriptions(schema: pa.Schema) -> tuple[str | None, dict[str, str]]:
    """Extract the table description and per-column descriptions from ``schema`` metadata.

    :param schema: PyArrow schema with optional ``b"comment"`` metadata on the schema and fields
    :type schema: pa.Schema
    :return: tuple of ``(table_description, {column_name: column_description})``
    :rtype: tuple[str | None, dict[str, str]]
    """
    table_desc: str | None = None
    if schema.metadata and b"comment" in schema.metadata:
        decoded = schema.metadata[b"comment"].decode("utf-8")
        if decoded:
            table_desc = decoded

    col_descs: dict[str, str] = {}
    for field in schema:
        if field.metadata and b"comment" in field.metadata:
            col_comment = field.metadata[b"comment"].decode("utf-8")
            if col_comment:
                col_descs[field.name] = col_comment
    return table_desc, col_descs


class OutputTableHints(BaseModel):
    """Per-table schema descriptions and Iceberg partition hints registered on a ``DltResource``.

    :param table_name: destination table name
    :type table_name: str
    :param pyarrow_schema: optional PyArrow schema carrying table and field ``b"comment"`` metadata
    :type pyarrow_schema: pa.Schema | None
    :param description: explicit table description overriding ``pyarrow_schema`` metadata when set
    :type description: str | None
    :param iceberg_partitions: Iceberg partition specifications applied via ``x-iceberg-partition``
    :type iceberg_partitions: tuple[PartitionSpec, ...]
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    table_name: RegistryKey
    pyarrow_schema: InstanceOf[pa.Schema] | None = None
    description: str | None = None
    iceberg_partitions: tuple[InstanceOf[PartitionSpec], ...] = ()

    def apply_to_resource(self, resource: DltResource) -> DltResource:
        """Register an isolated table hint variant for ``self.table_name`` on ``resource``.

        :param resource: target dlt resource
        :type resource: DltResource
        :return: ``resource`` with the table variant hints applied
        :rtype: DltResource
        """
        schema_desc: str | None = None
        col_descs: dict[str, str] = {}
        if self.pyarrow_schema is not None:
            schema_desc, col_descs = extract_schema_descriptions(self.pyarrow_schema)

        resolved_description = self.description if self.description is not None else schema_desc
        columns_hint: TAnySchemaColumns | None = (
            {col: {"description": desc} for col, desc in col_descs.items()} if col_descs else None
        )

        extra_hints: dict[str, Any] = {}
        if resolved_description:
            extra_hints["description"] = resolved_description
            extra_hints[TABLE_PROPERTIES_HINT] = {"comment": resolved_description}
        if self.iceberg_partitions:
            extra_hints[PARTITION_HINT] = [spec.to_dict() for spec in self.iceberg_partitions]

        # initialize the variant first so dlt preserves additional_table_hints even when resource._hints is None
        resource.apply_hints(table_name=self.table_name, create_table_variant=True)
        resource.apply_hints(
            table_name=self.table_name,
            columns=columns_hint,
            additional_table_hints=extra_hints or None,
            create_table_variant=True,
        )
        return resource


def propagate_run_hints_to_variants(
    settings: object,
    resource: DltResource | list[DltResource],
    pipeline_run_kwargs: Mapping[str, Any] | None,
) -> None:
    """Propagate ``pipeline.run`` table/file format and write disposition kwargs onto registered table variants.

    :param settings: pipeline settings optionally exposing ``get_output_table_hints()``
    :type settings: object
    :param resource: target dlt resource or list of resources
    :type resource: DltResource | list[DltResource]
    :param pipeline_run_kwargs: keyword arguments forwarded to ``pipeline.run``
    :type pipeline_run_kwargs: Mapping[str, Any] | None
    :return: ``None``
    :rtype: None
    """
    if not pipeline_run_kwargs:
        return
    get_hints = getattr(settings, "get_output_table_hints", None)
    output_table_hints: tuple[OutputTableHints, ...] = get_hints() if callable(get_hints) else ()
    if not output_table_hints:
        return
    table_format = pipeline_run_kwargs.get("table_format")
    file_format = pipeline_run_kwargs.get("loader_file_format")
    write_disposition = pipeline_run_kwargs.get("write_disposition")
    if table_format is None and file_format is None and write_disposition is None:
        return
    resources = resource if isinstance(resource, list) else [resource]
    for res in resources:
        for table_hint in output_table_hints:
            res.apply_hints(
                table_name=table_hint.table_name,
                table_format=table_format,
                file_format=file_format,
                write_disposition=write_disposition,
                create_table_variant=True,
            )
