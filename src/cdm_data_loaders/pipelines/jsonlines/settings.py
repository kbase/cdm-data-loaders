"""Setting for the JSONL validate-and-load pipeline for the KBase CTS."""

from logging import Logger, getLogger
from typing import TYPE_CHECKING, Annotated, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable

import pyarrow as pa
from frozendict import frozendict
from jsonschema.exceptions import SchemaError
from pydantic import Field, PositiveInt, PrivateAttr
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters.exceptions.facades.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrowError,
)
from cdm_data_loaders.converters.exceptions.facades.pydantic_to_pyarrow import (
    PydanticToPyArrowError,
)
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    convert_schema_to_pyarrow,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import (
    PydanticToPyArrow,
)
from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    TABLE_NAME,
    FileGlob,
    NonEmptyStr,
    TableName,
    file_glob_description,
)
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    ModelRegistry,
    SchemaRegistry,
    TransformRegistry,
    load_registry_dict,
)
from cdm_data_loaders.core.settings import (
    FileFormatIngestSettings,
    default_settings_with_shortcuts,
    select_cli_shortcuts,
)
from cdm_data_loaders.core.transforms.settings_mixin import (
    TransformSettingsMixin,
    reconcile_multi_table_schema_map,
    validate_table_transforms,
)
from cdm_data_loaders.pipelines.jsonlines.exceptions import (
    InvalidEntitySchemasRegistryError,
    InvalidJsonlEntitySchemaError,
    JsonlJsonschemaError,
    UnresolvableJsonlSchemaRefError,
    UnsupportedJsonlArrowSchemaError,
    UnsupportedJsonlSchemaDialectError,
)
from cdm_data_loaders.utils.recursion import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.validation.jsonschema.dereference import DereferencingError

logger: Logger = getLogger(__name__)


DEFAULT_JSONL_FILE_GLOB: Final[str] = "*.jsonl*"
EXTRACT_PIPELINE_NAME: Final[str] = "jsonlines_extract"
PYDANTIC_PIPELINE_NAME: Final[str] = "jsonlines_pydantic"
JSONSCHEMA_PIPELINE_NAME: Final[str] = "jsonlines_jsonschema"

ENTITY_MODELS_MODULE: Final[str] = "entity_models"
ENTITY_SCHEMAS_MODULE: Final[str] = "entity_schemas"
BY_ALIAS: Final[str] = "by_alias"
MAX_RECURSION_DEPTH: Final[str] = "max_recursion_depth"

LOCAL_CLI_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    {
        ENTITY_MODELS_MODULE: "m",
        FILE_GLOB: "g",
        ENTITY_SCHEMAS_MODULE: "m",
        TABLE_NAME: "t",
    }
)


def _classify_jsonl_schema_error(exc: Exception) -> JsonlJsonschemaError:
    """Classify an ``ENTITY_SCHEMAS`` loading or schema validation exception into a :class:`JsonlJsonschemaError` subclass."""
    if isinstance(exc, SchemaError) or isinstance(exc.__cause__, SchemaError):
        return InvalidJsonlEntitySchemaError(str(exc))
    msg = str(exc)
    if "JSON Schema is missing the $schema keyword" in msg or "Unsupported JSON Schema draft:" in msg:
        return UnsupportedJsonlSchemaDialectError(msg)
    return InvalidEntitySchemasRegistryError(msg)


def build_output_schema_registry(
    validation_schema_registry: SchemaRegistry | ModelRegistry,
    *,
    table_transforms: TransformRegistry | None = None,
    output_schemas: ArrowSchemaRegistry | None = None,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
    by_alias: bool = False,
) -> ArrowSchemaRegistry | None:
    """Convert all entity JSON Schemas or Pydantic models to PyArrow schemas up front.

    Conserves all entries in ``output_schemas`` and skips automatic
    input-schema -> PyArrow conversion for any entity table whose PyArrow schema
    is overridden by ``output_schemas`` or that has a configured
    ``table_transforms`` entry. Collects any dereferencing or conversion errors
    across all remaining tables and raises a single ``RuntimeError`` listing
    every failing table.

    :param validation_schema_registry: schema or model registry of table name to validated JSON Schema or Pydantic model
    :type  validation_schema_registry: SchemaRegistry | ModelRegistry
    :param table_transforms: optional registry of entity table name to `RecordTransformFn`
    :type  table_transforms: TransformRegistry | None
    :param output_schemas: optional PyArrow schema registry
    :type  output_schemas: ArrowSchemaRegistry | None
    :param max_recursion_depth: maximum expansion depth for recursive Pydantic ``BaseModel`` references
    :type  max_recursion_depth: int
    :param by_alias: when ``True``, use Pydantic ``serialization_alias`` or ``alias`` for field names
    :type  by_alias: bool
    :return: PyArrow schema registry combining ``output_schemas`` and converted entity schemas, or ``None`` if empty
    :rtype: ArrowSchemaRegistry | None
    :raises RuntimeError: if one or more schemas cannot be converted to PyArrow schemas
    """
    errors: dict[str, str] = {}
    has_arrow_error = False
    is_model_registry = isinstance(validation_schema_registry, ModelRegistry)
    if is_model_registry:
        pydantic_converter = PydanticToPyArrow(max_recursion_depth=max_recursion_depth, by_alias=by_alias)
        fn: Callable[..., Any] = pydantic_converter.convert
    else:
        fn = convert_schema_to_pyarrow

    def _convert_table_schema(table_name: str) -> pa.Schema | None:
        nonlocal has_arrow_error
        model_or_schema = validation_schema_registry.registry[table_name]
        try:
            return fn(model_or_schema)
        except PydanticToPyArrowError as error:
            logger.exception("Error converting Pydantic model to PyArrow for %s", table_name)
            errors[table_name] = str(error)
        except DereferencingError as error:
            logger.exception("Error converting JSON Schema to PyArrow for %s", table_name)
            errors[table_name] = str(error)
        except JSONSchemaToPyArrowError as error:
            logger.exception("Error converting JSON Schema to PyArrow for %s", table_name)
            has_arrow_error = True
            errors[table_name] = str(error)
        return None

    registry_result = reconcile_multi_table_schema_map(
        validation_schema_registry.registry,
        _convert_table_schema,
        table_transforms=table_transforms,
        output_schemas=output_schemas,
    )

    if errors:
        details = "".join(f"{table_name}: {error}\n" for table_name, error in errors.items())
        err_msg = f"The following errors were found when converting schemas to PyArrow:\n{details}"
        if is_model_registry:
            raise RuntimeError(err_msg)
        if has_arrow_error:
            raise UnsupportedJsonlArrowSchemaError(err_msg)
        raise UnresolvableJsonlSchemaRefError(err_msg)

    return registry_result


class JsonlIngestSettings(FileFormatIngestSettings):
    """Settings for the JSONL ingest pipeline."""

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_JSONL_FILE_GLOB,
            description=file_glob_description("Glob pattern for JSONL files"),
        ),
    ]

    def _resolve_entity_registry_schema_map(
        self: Any,
        registry: ModelRegistry | SchemaRegistry,
    ) -> None:
        """Validate configured tables against ``registry`` and resolve ``_schema_map``."""
        self._validate_configured_tables(
            registry.registry,
            validate_transforms_fn=validate_table_transforms,
        )
        self._schema_map = (
            build_output_schema_registry(
                registry,
                table_transforms=self._table_transforms,
                output_schemas=self._unreconciled_output_schemas,
                max_recursion_depth=getattr(self, MAX_RECURSION_DEPTH, DEFAULT_MAX_RECURSION_DEPTH),
                by_alias=getattr(self, BY_ALIAS, False),
            )
            if self.use_pyarrow
            else None
        )


class JsonlExtractSettings(JsonlIngestSettings, TransformSettingsMixin):
    """Settings for the JSONL extraction-only pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=EXTRACT_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, FILE_GLOB, TABLE_NAME),  # pyright: ignore[reportArgumentType]
    )

    table_name: TableName


class JsonlPydanticIngestSettings(JsonlIngestSettings, TransformSettingsMixin):
    """Pipeline settings for JSONL pipeline that uses Pydantic for validation."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PYDANTIC_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, ENTITY_MODELS_MODULE, FILE_GLOB),  # pyright: ignore[reportArgumentType]
    )

    _model_registry: ModelRegistry = PrivateAttr()

    entity_models: Annotated[
        NonEmptyStr,
        Field(
            description=(
                "Dotted import path to a module that defines `ENTITY_MODELS: dict[str, type[BaseModel]]`. "
                "Each key is a table name and a subdirectory of `input_dir`. "
                "Each value is the Pydantic model for that table."
            ),
        ),
    ]
    max_recursion_depth: Annotated[
        PositiveInt,
        Field(
            default=DEFAULT_MAX_RECURSION_DEPTH,
            description=(
                "Maximum recursion depth for self-referential Pydantic models before serializing "
                "deeper content as JSON."
            ),
        ),
    ]
    by_alias: Annotated[
        bool,
        Field(
            default=False,
            description=(
                "Use field `serialization_alias` or `alias` (instead of Python attribute names) "
                "for generated PyArrow column names and serialized record keys."
            ),
        ),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Load and validate the entity registry from the configured module and resolve ``_schema_map``."""
        del default_pyarrow_schema
        self._model_registry = load_registry_dict(self.entity_models, "ENTITY_MODELS")
        self._resolve_entity_registry_schema_map(self._model_registry)

    @property
    def model_registry(self) -> ModelRegistry:
        """The loaded `ENTITY_MODELS` mapping."""
        return self._model_registry


class JsonlJsonschemaIngestSettings(JsonlIngestSettings, TransformSettingsMixin):
    """Pipeline settings for JSONL ingestion with JSON Schema validation."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=JSONSCHEMA_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, FILE_GLOB, ENTITY_SCHEMAS_MODULE),  # pyright: ignore[reportArgumentType]
    )

    _schema_registry: SchemaRegistry = PrivateAttr()

    entity_schemas: Annotated[
        NonEmptyStr,
        Field(
            description=(
                "Dotted import path to a module that defines `ENTITY_SCHEMAS: dict[str, dict]`. "
                "Each key is a table name and a subdirectory of `input_dir`. "
                "Each value is a JSON Schema dictionary declaring its draft with `$schema`."
            ),
        ),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Load and validate the schema registry and precompute PyArrow schemas."""
        del default_pyarrow_schema
        try:
            self._schema_registry = load_registry_dict(self.entity_schemas, "ENTITY_SCHEMAS")
        except Exception as exc:
            raise _classify_jsonl_schema_error(exc) from exc
        self._resolve_entity_registry_schema_map(self._schema_registry)

    @property
    def schema_registry(self) -> SchemaRegistry:
        """The loaded `ENTITY_SCHEMAS` mapping."""
        return self._schema_registry
