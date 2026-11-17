"""Settings mixin and table validation helpers for pluggable pre-processors, transforms, and schemas."""

from collections.abc import Callable, Iterable
from typing import Any, ClassVar, Self

import pyarrow as pa
from pydantic import PrivateAttr, model_validator
from pydantic_settings import BaseSettings

from cdm_data_loaders.core.fields import (
    PYARROW_FILE_FORMATS,
    LoaderFileFormatEnum,
    OutputSchemasModuleSpec,
    PreprocessorsModuleSpec,
    TransformsModuleSpec,
)
from cdm_data_loaders.core.preprocessors import validate_file_preprocessors
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    FilePreprocessFn,
    PreprocessorRegistry,
    TableTransformFn,
    TransformRegistry,
    load_file_preprocessors,
    load_registry_dict,
    validate_registry_table_names,
)
from cdm_data_loaders.core.transforms.hints import OutputTableHints
from cdm_data_loaders.core.transforms.routing import compose_transforms, resolve_schema_map


def validate_table_transforms(
    settings: object,
    valid_tables: Iterable[str],
) -> None:
    """Ensure every key in ``settings.table_transforms`` matches a configured input table.

    :param settings: pipeline settings instance
    :type settings: object
    :param valid_tables: valid input table names for the pipeline
    :type valid_tables: Iterable[str]
    :return: ``None``
    :rtype: None
    """
    table_transforms: TransformRegistry | None = getattr(settings, "table_transforms", None)
    if table_transforms is None:
        return
    validate_registry_table_names(table_transforms.registry, valid_tables, "transforms_module")


def uses_pyarrow_format(settings: object) -> bool:
    """Return whether ``settings.loader_file_format`` is a PyArrow-backed loader format.

    :param settings: pipeline settings instance
    :type settings: object
    :return: ``True`` if ``loader_file_format`` uses PyArrow pages
    :rtype: bool
    """
    return getattr(settings, "loader_file_format", LoaderFileFormatEnum.PARQUET) in PYARROW_FILE_FORMATS


def needs_default_pyarrow_schema(
    table_name: str,
    *,
    use_pyarrow: bool = True,
    table_transforms: TransformRegistry | None = None,
    output_schemas: ArrowSchemaRegistry | None = None,
) -> bool:
    """Return whether ``table_name`` needs its PyArrow schema generated from the input schema.

    :param table_name: target table name
    :type table_name: str
    :param use_pyarrow: whether PyArrow output is enabled
    :type use_pyarrow: bool
    :param table_transforms: optional configured table transforms registry
    :type table_transforms: TransformRegistry | None
    :param output_schemas: optional user-supplied output schemas registry
    :type output_schemas: ArrowSchemaRegistry | None
    :return: ``True`` if a default PyArrow schema is needed for ``table_name``
    :rtype: bool
    """
    if not use_pyarrow:
        return False
    if table_transforms is not None and table_name in table_transforms.registry:
        return False
    return not (output_schemas is not None and table_name in output_schemas.registry)


class TransformSettingsMixin(BaseSettings):
    """Mixin for pipelines that accept ``FILE_PREPROCESSORS``, ``TABLE_TRANSFORMS``, and ``OUTPUT_SCHEMAS`` modules."""

    DEFAULT_FILE_PREPROCESSORS: ClassVar[tuple[FilePreprocessFn, ...]] = ()

    _file_preprocessors: PreprocessorRegistry | None = PrivateAttr(default=None)
    _table_transforms: TransformRegistry | None = PrivateAttr(default=None)
    _composed_transforms_source: TransformRegistry | None = PrivateAttr(default=None)
    _composed_transforms: dict[str, TableTransformFn] | None = PrivateAttr(default=None)
    _unreconciled_output_schemas: ArrowSchemaRegistry | None = PrivateAttr(default=None)
    _schema_map: ArrowSchemaRegistry | None = PrivateAttr(default=None)

    preprocessors_module: PreprocessorsModuleSpec
    transforms_module: TransformsModuleSpec
    output_schemas_module: OutputSchemasModuleSpec

    @model_validator(mode="after")
    def _validate_transforms_module(self) -> Self:
        """Load and validate ``FILE_PREPROCESSORS``, ``TABLE_TRANSFORMS``, and ``OUTPUT_SCHEMAS``.

        :return: validated settings instance
        :rtype: Self
        """
        if self.preprocessors_module is not None:
            self._file_preprocessors = load_file_preprocessors(self.preprocessors_module)
        elif self.DEFAULT_FILE_PREPROCESSORS:
            self._file_preprocessors = PreprocessorRegistry(default=self.DEFAULT_FILE_PREPROCESSORS)
        if self.transforms_module is not None:
            self._table_transforms = load_registry_dict(self.transforms_module, "TABLE_TRANSFORMS")
        if self.output_schemas_module is not None and self.use_pyarrow:
            self._unreconciled_output_schemas = load_registry_dict(self.output_schemas_module, "OUTPUT_SCHEMAS")
        self._resolve_settings_schema_map()
        self._sync_composed_transforms()
        return self

    def _sync_composed_transforms(self) -> dict[str, TableTransformFn] | None:
        """Build and cache per-table composed transforms on this instance for ``self._table_transforms``.

        :return: mapping of table names to single or composed transform callables, or ``None``
        :rtype: dict[str, TableTransformFn] | None
        """
        if self._table_transforms is None:
            return None
        if self._composed_transforms is None or self._composed_transforms_source is not self._table_transforms:
            self._composed_transforms = {
                name: compose_transforms(self._table_transforms.get_tuple(name))
                for name in self._table_transforms.registry
            }
            self._composed_transforms_source = self._table_transforms
        return self._composed_transforms

    @property
    def use_pyarrow(self) -> bool:
        """Return whether this pipeline yields ``pyarrow.Table`` pages instead of row dicts.

        :return: ``True`` when PyArrow page output is enabled
        :rtype: bool
        """
        return uses_pyarrow_format(self)

    def _validate_configured_tables(
        self,
        valid_tables: Iterable[str],
        *,
        validate_transforms_fn: Callable[[Any, Iterable[str]], None] = validate_table_transforms,
    ) -> None:
        """Validate both ``file_preprocessors`` and ``table_transforms`` against ``valid_tables``.

        :param valid_tables: valid table names for the pipeline
        :type valid_tables: Iterable[str]
        :param validate_transforms_fn: callable validating ``table_transforms`` keys
        :type validate_transforms_fn: Callable[[Any, Iterable[str]], None]
        :return: ``None``
        :rtype: None
        """
        validate_file_preprocessors(self, valid_tables)
        validate_transforms_fn(self, valid_tables)

    def _needs_default_pyarrow_schema(self, table_name: str) -> bool:
        """Return whether ``table_name`` needs its PyArrow schema generated from the input schema.

        :param table_name: target table name
        :type table_name: str
        :return: ``True`` when a default PyArrow schema is needed
        :rtype: bool
        """
        if not self.use_pyarrow or self.get_table_transform(table_name) is not None:
            return False
        return needs_default_pyarrow_schema(
            table_name,
            use_pyarrow=self.use_pyarrow,
            output_schemas=self._unreconciled_output_schemas,
        )

    def _resolve_default_table_schema(
        self,
        table_name: str,
        default_pyarrow_schema: pa.Schema | None,
        build_default: Callable[[], pa.Schema],
    ) -> pa.Schema | None:
        """Return ``default_pyarrow_schema`` if provided, else call ``build_default`` when needed.

        :param table_name: target table name
        :type table_name: str
        :param default_pyarrow_schema: optional pre-built default PyArrow schema
        :type default_pyarrow_schema: pa.Schema | None
        :param build_default: zero-arg factory producing the default PyArrow schema
        :type build_default: Callable[[], pa.Schema]
        :return: resolved default PyArrow schema or ``None``
        :rtype: pa.Schema | None
        """
        if default_pyarrow_schema is not None or not self._needs_default_pyarrow_schema(table_name):
            return default_pyarrow_schema
        return build_default()

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Resolve ``_schema_map`` from ``_unreconciled_output_schemas``, ``_table_transforms``, and ``default_pyarrow_schema``.

        :param default_pyarrow_schema: optional default PyArrow schema for single-table pipelines
        :type default_pyarrow_schema: pa.Schema | None
        :return: ``None``
        :rtype: None
        """
        table_name = getattr(self, "table_name", None)
        if isinstance(table_name, str):
            self._validate_configured_tables({table_name})
            self._schema_map = (
                resolve_schema_map(
                    table_name,
                    transform=self.get_table_transform(table_name),
                    unreconciled_output_schemas=self._unreconciled_output_schemas,
                    default_pyarrow_schema=default_pyarrow_schema,
                )
                if self.use_pyarrow
                else None
            )
        else:
            self._schema_map = self._unreconciled_output_schemas if self.use_pyarrow else None

    @property
    def file_preprocessors(self) -> PreprocessorRegistry | None:
        """Return the loaded ``FILE_PREPROCESSORS`` registry, or ``None`` if unconfigured.

        :return: loaded file pre-processors registry or ``None``
        :rtype: PreprocessorRegistry | None
        """
        return self._file_preprocessors

    @property
    def table_transforms(self) -> TransformRegistry | None:
        """Return the loaded ``TABLE_TRANSFORMS`` registry, or ``None`` if unconfigured.

        :return: loaded table transforms registry or ``None``
        :rtype: TransformRegistry | None
        """
        return self._table_transforms

    @property
    def schema_map(self) -> ArrowSchemaRegistry | None:
        """Return the resolved ``ArrowSchemaRegistry`` for this pipeline, or ``None`` if unschematized.

        :return: resolved output schema map or ``None``
        :rtype: ArrowSchemaRegistry | None
        """
        return self._schema_map

    def get_file_preprocessors(self, table_name: str | None = None) -> tuple[FilePreprocessFn, ...]:
        """Return the ``FilePreprocessFn`` sequence for ``table_name``, if configured.

        :param table_name: target table name or ``None`` for multi-table streams
        :type table_name: str | None
        :return: tuple of file pre-processor callables
        :rtype: tuple[FilePreprocessFn, ...]
        """
        if self._file_preprocessors is None:
            return ()
        return self._file_preprocessors.get(table_name)

    def get_table_transforms(self, table_name: str) -> tuple[TableTransformFn, ...]:
        """Return the ordered tuple of transform callables configured for ``table_name``.

        :param table_name: input table name
        :type table_name: str
        :return: tuple of configured transform callables (empty if unconfigured)
        :rtype: tuple[TableTransformFn, ...]
        """
        if self._table_transforms is None:
            return ()
        return self._table_transforms.get_tuple(table_name)

    def get_table_transform(self, table_name: str) -> TableTransformFn | None:
        """Return the single or composed ``TableTransformFn`` for ``table_name``, if configured.

        :param table_name: input table name
        :type table_name: str
        :return: configured or composed transform callable, or ``None``
        :rtype: TableTransformFn | None
        """
        composed = self._sync_composed_transforms()
        if composed is None:
            return None
        return composed.get(table_name)

    def get_output_table_hints(self) -> tuple[OutputTableHints, ...]:
        """Return per-table output hints (descriptions and Iceberg partitions) for this pipeline.

        :return: tuple of output table hints
        :rtype: tuple[OutputTableHints, ...]
        """
        return ()
