"""Setting for the JSONL validate-and-load pipeline for the KBase CTS."""

from logging import Logger, getLogger
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Final

if TYPE_CHECKING:
    from collections.abc import Callable

import pyarrow as pa
from frozendict import frozendict
from jsonschema.exceptions import SchemaError
from pydantic import Field, PrivateAttr
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrowError,
    convert_schema_to_pyarrow,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import (
    PydanticToPyArrow,
    PydanticToPyArrowError,
)
from cdm_data_loaders.core.fields import (
    DEFAULT_JSONL_FILE_GLOB,
    FILE_GLOB,
    TABLE_NAME,
    FileGlob,
    NonEmptyStr,
    TableName,
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
from cdm_data_loaders.core.transforms import (
    TransformSettingsMixin,
    needs_default_pyarrow_schema,
    validate_table_transforms,
)
from cdm_data_loaders.schemas.jsonschema import DereferencingError

logger: Logger = getLogger(__name__)


EXTRACT_PIPELINE_NAME: Final[str] = "jsonlines_extract"
PYDANTIC_PIPELINE_NAME: Final[str] = "jsonlines_pydantic"
JSONSCHEMA_PIPELINE_NAME: Final[str] = "jsonlines_jsonschema"
NCBI_DATASETS_PIPELINE_NAME: Final[str] = "ncbi_datasets"
NCBI_DATASETS_SCHEMA_MODULE: Final[str] = "cdm_data_loaders.schemas.ncbi_datasets.ncbi_openapi"

ENTITY_MODELS_MODULE: Final[str] = "entity_models"
ENTITY_SCHEMAS_MODULE: Final[str] = "entity_schemas"

LOCAL_CLI_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    {
        ENTITY_MODELS_MODULE: "m",
        FILE_GLOB: "g",
        ENTITY_SCHEMAS_MODULE: "m",
        TABLE_NAME: "t",
    }
)


class JsonlJsonschemaError(RuntimeError):
    """Base error raised when an ``ENTITY_SCHEMAS`` registry or entity JSON Schema is invalid or cannot convert to PyArrow."""

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class InvalidEntitySchemasRegistryError(JsonlJsonschemaError):
    """The `entity_schemas` module must define a non-empty `ENTITY_SCHEMAS` dictionary and cannot use the reserved `_rejected` table name."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Invalid or empty ENTITY_SCHEMAS registry"


class UnsupportedJsonlSchemaDialectError(JsonlJsonschemaError):
    """Every entity JSON Schema must declare a supported `$schema` draft URI (Draft 4, 6, 7, 2019-09, 2020-12, or KBase 2020-12-xsv)."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Missing or unsupported JSON Schema $schema dialect"


class InvalidJsonlEntitySchemaError(JsonlJsonschemaError):
    """Every entity JSON Schema must be a valid schema document under its declared `$schema` metaschema."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Entity JSON Schema fails draft metaschema validation"


class UnresolvableJsonlSchemaRefError(JsonlJsonschemaError):
    """All `$ref` pointers and `allOf` branches in an entity JSON Schema must resolve without cycles before PyArrow conversion."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Unresolvable or circular $ref in entity JSON Schema"


class UnsupportedJsonlArrowSchemaError(JsonlJsonschemaError):
    """Each non-transformed, non-overridden entity JSON Schema must have `type: "object"` with at least one property and only supported PyArrow column constructs."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Entity JSON Schema cannot be converted to a PyArrow table schema"


JSONLINES_JSONSCHEMA_ERRORS: Final[tuple[type[JsonlJsonschemaError], ...]] = (
    InvalidEntitySchemasRegistryError,
    UnsupportedJsonlSchemaDialectError,
    InvalidJsonlEntitySchemaError,
    UnresolvableJsonlSchemaRefError,
    UnsupportedJsonlArrowSchemaError,
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
    :return: PyArrow schema registry combining ``output_schemas`` and converted entity schemas, or ``None`` if empty
    :rtype: ArrowSchemaRegistry | None
    :raises RuntimeError: if one or more schemas cannot be converted to PyArrow schemas
    """
    pyarrow_schemas: dict[str, pa.Schema] = dict(output_schemas.registry) if output_schemas is not None else {}
    errors: dict[str, str] = {}
    has_arrow_error = False
    is_model_registry = isinstance(validation_schema_registry, ModelRegistry)
    if is_model_registry:
        pydantic_converter = PydanticToPyArrow()
        fn: Callable[..., Any] = pydantic_converter.convert
    else:
        fn = convert_schema_to_pyarrow

    for table_name, model_or_schema in validation_schema_registry.registry.items():
        if not needs_default_pyarrow_schema(
            table_name,
            table_transforms=table_transforms,
            output_schemas=output_schemas,
        ):
            continue
        try:
            pyarrow_schemas[table_name] = fn(model_or_schema)
        except PydanticToPyArrowError as error:
            logger.exception("Error converting Pydantic model to PyArrow for %s", table_name)
            errors[table_name] = str(error)
        except (DereferencingError, RecursionError) as error:
            logger.exception("Error converting JSON Schema to PyArrow for %s", table_name)
            errors[table_name] = str(error)
        except JSONSchemaToPyArrowError as error:
            logger.exception("Error converting JSON Schema to PyArrow for %s", table_name)
            has_arrow_error = True
            errors[table_name] = str(error)

    if errors:
        details = "".join(f"{table_name}: {error}\n" for table_name, error in errors.items())
        err_msg = f"The following errors were found when converting schemas to PyArrow:\n{details}"
        if is_model_registry:
            raise RuntimeError(err_msg)
        if has_arrow_error:
            raise UnsupportedJsonlArrowSchemaError(err_msg)
        raise UnresolvableJsonlSchemaRefError(err_msg)

    if not pyarrow_schemas:
        return None

    return ArrowSchemaRegistry(registry=pyarrow_schemas)


class JsonlIngestSettings(FileFormatIngestSettings):
    """Settings for the JSONL ingest pipeline."""

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_JSONL_FILE_GLOB,
            description="Glob pattern for JSONL files",
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

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:  # noqa: ARG002
        """Load and validate the entity registry from the configured module and resolve `_schema_map`."""
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

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:  # noqa: ARG002
        """Load and validate the schema registry and precompute PyArrow schemas."""
        try:
            self._schema_registry = load_registry_dict(self.entity_schemas, "ENTITY_SCHEMAS")
        except Exception as exc:
            raise _classify_jsonl_schema_error(exc) from exc
        self._resolve_entity_registry_schema_map(self._schema_registry)

    @property
    def schema_registry(self) -> SchemaRegistry:
        """The loaded `ENTITY_SCHEMAS` mapping."""
        return self._schema_registry


class NcbiDatasetsSettings(JsonlJsonschemaIngestSettings):
    """Pipeline settings for NCBI Datasets JSONL ingestion (`annotation_report` and `dataset_report`)."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=NCBI_DATASETS_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, FILE_GLOB),  # pyright: ignore[reportArgumentType]
    )

    dataset_name: ClassVar[str] = NCBI_DATASETS_PIPELINE_NAME
    entity_schemas: ClassVar[str] = NCBI_DATASETS_SCHEMA_MODULE
    transforms_module: ClassVar[str | None] = NCBI_DATASETS_SCHEMA_MODULE
    output_schemas_module: ClassVar[str | None] = NCBI_DATASETS_SCHEMA_MODULE
