"""Pluggable record filtering, transformation, multi-table splitting, and PyArrow schema overrides."""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, ClassVar, Self

import pyarrow as pa
from pydantic import PrivateAttr, model_validator
from pydantic_settings import BaseSettings

from cdm_data_loaders.coercion.pyarrow import coerce_record_for_pyarrow
from cdm_data_loaders.core.fields import (
    OutputSchemasModuleSpec,
    PreprocessorsModuleSpec,
    TransformsModuleSpec,
)
from cdm_data_loaders.core.preprocessors import validate_file_preprocessors
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    FilePreprocessFn,
    PreprocessorRegistry,
    RecordTransformFn,
    TransformRegistry,
    load_file_preprocessors,
    load_registry_dict,
)


def resolve_schema_map(
    default_table: str,
    *,
    transform: RecordTransformFn | None = None,
    unreconciled_output_schemas: ArrowSchemaRegistry | None = None,
    default_pyarrow_schema: pa.Schema | None = None,
) -> ArrowSchemaRegistry | None:
    """Resolve the active table-to-PyArrow-schema registry for a reader.

    :param default_table: default target table name for the pipeline/entity
    :type default_table: str
    :param transform: optional ``RecordTransformFn`` configured for ``default_table``
    :type transform: RecordTransformFn | None
    :param unreconciled_output_schemas: optional ``ArrowSchemaRegistry``
    :type unreconciled_output_schemas: ArrowSchemaRegistry | None
    :param default_pyarrow_schema: default PyArrow schema when no override is configured
    :type default_pyarrow_schema: pa.Schema | None
    :return: ``ArrowSchemaRegistry`` mapping destination table names to ``pa.Schema`` objects, or ``None``
    """
    if unreconciled_output_schemas is not None:
        if transform is None and default_table not in unreconciled_output_schemas.registry:
            err_msg = f"unreconciled_output_schemas for {default_table!r} must include {default_table!r} when transform is None"
            raise ValueError(err_msg)
        return unreconciled_output_schemas
    if default_pyarrow_schema is not None and transform is None:
        return ArrowSchemaRegistry(
            registry={
                default_table: default_pyarrow_schema,
            }
        )
    return None


def validate_routed_output(raw_routed: object) -> dict[str, list[dict[str, Any]]]:
    """Validate the return value of ``transform_fn`` and strip empty table lists.

    :param raw_routed: raw output from ``transform_fn`` for a single record
    :type  raw_routed: object
    :raises ValueError: if ``raw_routed`` is not a mapping, any table name is empty,
        or rows for any table are not a sequence of non-empty dicts
    :return: validated dictionary mapping destination table names to non-empty lists of row dictionaries
    :rtype : dict[str, list[dict[str, Any]]]
    """
    routed: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    if not isinstance(raw_routed, Mapping):
        errors.append("transform_fn must return a mapping of table names to sequences of row dicts")
    else:
        for table_name, rows in raw_routed.items():
            if not isinstance(table_name, str) or not table_name.strip():
                errors.append("transform_fn output keys must be non-empty table names")

            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray, Mapping)):
                errors.append(f"transform_fn rows for table {table_name!r} must be a sequence of dicts")
                continue

            row_list = list(rows)
            if not all(isinstance(row, dict) for row in row_list) or not all(row for row in row_list):
                errors.append(f"transform_fn rows for table {table_name!r} must be non-empty dicts")
                continue

            if not errors and row_list:
                routed[table_name.strip()] = row_list

    if errors:
        raise ValueError("\n".join(errors))
    return routed


def route_and_coerce_record(
    record: dict[str, Any],
    default_table: str,
    *,
    transform: RecordTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Route a validated record through ``transform`` and optionally coerce rows for PyArrow.

    :param record: parsed and validated input record dictionary
    :type record: dict[str, Any]
    :param default_table: destination table name when ``transform`` is ``None``
    :type default_table: str
    :param transform: optional ``RecordTransformFn`` for ``default_table``
    :type transform: RecordTransformFn | None
    :param schema_map: active destination ``ArrowSchemaRegistry``, if any
    :type schema_map: ArrowSchemaRegistry | None
    :return: dictionary mapping destination table names to non-empty lists of row dictionaries
    """
    routed = validate_routed_output(transform(record)) if transform is not None else {default_table: [record]}

    if not routed:
        return {}

    if schema_map is not None:
        return {
            out_table: (
                [coerce_record_for_pyarrow(row, schema_map.registry[out_table]) for row in rows]
                if out_table in schema_map.registry
                else rows
            )
            for out_table, rows in routed.items()
        }

    return routed


def validate_table_transforms(
    settings: Any,  # noqa: ANN401
    valid_tables: Iterable[str],
) -> None:
    """Ensure every key in ``settings.table_transforms`` matches a configured input table."""
    table_transforms: TransformRegistry | None = getattr(settings, "table_transforms", None)
    if table_transforms is None:
        return
    unknown_transforms = sorted(set(table_transforms.registry) - set(valid_tables))
    if unknown_transforms:
        err_msg = f"Unknown table names in transforms_module: {', '.join(unknown_transforms)}"
        raise ValueError(err_msg)


class TransformSettingsMixin(BaseSettings):
    """Mixin for pipelines that accept `FILE_PREPROCESSORS`, `TABLE_TRANSFORMS`, and `OUTPUT_SCHEMAS` modules."""

    _DEFAULT_FILE_PREPROCESSORS: ClassVar[tuple[FilePreprocessFn, ...]] = ()

    _file_preprocessors: PreprocessorRegistry | None = PrivateAttr(default=None)
    _table_transforms: TransformRegistry | None = PrivateAttr(default=None)
    _unreconciled_output_schemas: ArrowSchemaRegistry | None = PrivateAttr(default=None)

    """User-supplied output table schemas in pyarrow format."""

    _schema_map: ArrowSchemaRegistry | None = PrivateAttr(default=None)

    """Output table schemas, derived from the user-supplied schemas and/or from the input schema.

    Tables without a user-supplied schema and with a transform specified will not have their
    schema generated from the input schema, since the transform may change the output fields.
    """

    preprocessors_module: PreprocessorsModuleSpec
    transforms_module: TransformsModuleSpec
    output_schemas_module: OutputSchemasModuleSpec

    @model_validator(mode="after")
    def _validate_transforms_module(self) -> Self:
        """Load and validate `FILE_PREPROCESSORS`, `TABLE_TRANSFORMS`, and `OUTPUT_SCHEMAS` if configured."""
        if self.preprocessors_module is not None:
            self._file_preprocessors = load_file_preprocessors(self.preprocessors_module)
        elif self._DEFAULT_FILE_PREPROCESSORS:
            self._file_preprocessors = PreprocessorRegistry(default=self._DEFAULT_FILE_PREPROCESSORS)
        if self.transforms_module is not None:
            self._table_transforms = load_registry_dict(self.transforms_module, "TABLE_TRANSFORMS")
        if self.output_schemas_module is not None:
            self._unreconciled_output_schemas = load_registry_dict(self.output_schemas_module, "OUTPUT_SCHEMAS")
        self._resolve_settings_schema_map()
        return self

    def _needs_default_pyarrow_schema(self, table_name: str) -> bool:
        """Return whether ``table_name`` needs its PyArrow schema generated from the input schema."""
        if self.get_table_transform(table_name) is not None:
            return False
        return not (
            self._unreconciled_output_schemas is not None and table_name in self._unreconciled_output_schemas.registry
        )

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Resolve `_schema_map` from `_unreconciled_output_schemas`, `_table_transforms`, and any default PyArrow schema."""
        table_name = getattr(self, "table_name", None)
        if isinstance(table_name, str):
            validate_file_preprocessors(self, {table_name})
            validate_table_transforms(self, {table_name})
            self._schema_map = resolve_schema_map(
                table_name,
                transform=self.get_table_transform(table_name),
                unreconciled_output_schemas=self._unreconciled_output_schemas,
                default_pyarrow_schema=default_pyarrow_schema,
            )
        else:
            self._schema_map = self._unreconciled_output_schemas

    @property
    def file_preprocessors(self) -> PreprocessorRegistry | None:
        """The loaded `FILE_PREPROCESSORS` registry (`None` if not configured)."""
        return self._file_preprocessors

    @property
    def table_transforms(self) -> TransformRegistry | None:
        """The loaded `TABLE_TRANSFORMS` registry (`None` if not configured)."""
        return self._table_transforms

    @property
    def schema_map(self) -> ArrowSchemaRegistry | None:
        """The resolved `ArrowSchemaRegistry` for this pipeline (`None` if unschematized)."""
        return self._schema_map

    def get_file_preprocessors(self, table_name: str | None = None) -> tuple[FilePreprocessFn, ...]:
        """Return the `FilePreprocessFn` sequence for `table_name`, if configured."""
        if self._file_preprocessors is None:
            return ()
        return self._file_preprocessors.get(table_name)

    def get_table_transform(self, table_name: str) -> RecordTransformFn | None:
        """Return the `RecordTransformFn` for `table_name`, if configured."""
        if self._table_transforms is None:
            return None
        return self._table_transforms.registry.get(table_name)
