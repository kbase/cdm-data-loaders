"""Exception hierarchy for the JSONL JSON Schema ingestion pipeline."""

from typing import ClassVar, Final

from cdm_data_loaders.core.cli import SpecError


class JsonlJsonschemaError(SpecError, RuntimeError):
    """Base error raised when an ``ENTITY_SCHEMAS`` registry or entity JSON Schema is invalid or cannot convert to PyArrow."""


class InvalidEntitySchemasRegistryError(JsonlJsonschemaError):
    """The `entity_schemas` module must define a non-empty `ENTITY_SCHEMAS` dictionary and cannot use the reserved `_rejected` table name."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Invalid or empty ENTITY_SCHEMAS registry"


class UnsupportedJsonlSchemaDialectError(JsonlJsonschemaError):
    """Every entity JSON Schema must declare a supported `$schema` draft URI (Draft 4, 6, 7, 2019-09, 2020-12, or KBase xsv_config)."""

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
