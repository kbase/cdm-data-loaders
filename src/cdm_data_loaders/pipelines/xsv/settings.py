"""Settings for the XSV (CSV/TSV) ingestion pipeline for the KBase CTS."""

from logging import Logger, getLogger
from typing import Annotated, Any, Final

import pyarrow as pa
from pydantic import Field, FilePath, PrivateAttr
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import ExtensionError
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import convert_xsv_schema_to_pyarrow
from cdm_data_loaders.core.fields import (
    FileGlob,
    TableName,
)
from cdm_data_loaders.core.settings import (
    FILE_FORMAT_CLI_SHORTCUTS,
    FileFormatIngestSettings,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.core.transforms.settings_mixin import TransformSettingsMixin
from cdm_data_loaders.pipelines.xsv.exceptions import (
    InvalidXsvSchemaDefinitionError,
    MissingRequiredColumnsError,
    UnresolvableXsvRefError,
    UnsupportedXsvArrowConstructError,
    UnsupportedXsvDialectError,
    XsvSchemaError,
)
from cdm_data_loaders.readers.xsv.duckdb_reader import resolve_xsv_parsing_config
from cdm_data_loaders.validation.jsonschema.dereference import DereferencingError
from cdm_data_loaders.validation.jsonschema.models import (
    ValidatedXsvSchema,
    generate_first_pass_schema,
    validate_xsv_jsonschema,
)

PIPELINE_NAME: Final[str] = "xsv"

logger: Logger = getLogger(__name__)


SHORTCUTS: Final[dict[str, str]] = {
    **FILE_FORMAT_CLI_SHORTCUTS,
    "file_glob": "g",
    "schema_file": "s",
    "table_name": "t",
}


def _classify_xsv_schema_error(exc: Exception, err_msg: str) -> XsvSchemaError:
    """Classify an underlying schema validation or conversion exception into an `XsvSchemaError` subclass."""
    if isinstance(exc, DereferencingError):
        return UnresolvableXsvRefError(err_msg)
    if isinstance(exc, ConversionError) and not isinstance(exc.__cause__, ExtensionError):
        return UnsupportedXsvArrowConstructError(err_msg)
    raw = str(exc)
    if "JSON Schema is missing the $schema keyword" in raw or "Unsupported JSON Schema draft:" in raw:
        return UnsupportedXsvDialectError(err_msg)
    if "Could not find any required cols in schema" in raw:
        return MissingRequiredColumnsError(err_msg)
    return InvalidXsvSchemaDefinitionError(err_msg)


class XsvIngestSettings(FileFormatIngestSettings, TransformSettingsMixin):
    """Settings for the XSV (CSV/TSV) ingestion pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=SHORTCUTS,  # pyright: ignore[reportArgumentType]
    )

    _validated_schema: ValidatedXsvSchema | None = PrivateAttr(default=None)
    _first_pass_schema: ValidatedXsvSchema | None = PrivateAttr(default=None)
    _parsing_config: dict[str, Any] | None = PrivateAttr(default=None)
    _pyarrow_schema: pa.Schema | None = PrivateAttr(default=None)

    file_glob: FileGlob
    schema_file: Annotated[
        FilePath,
        Field(
            description=(
                "Path to a JSON Schema file describing the XSV data's columns and types. "
                "May include an x-xsv-config block with xsv parser config metadata."
            ),
        ),
    ]
    table_name: TableName

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Load and validate the schema file, first-pass schema, parsing config, PyArrow schema, and `_schema_map`."""
        try:
            self._validated_schema = validate_xsv_jsonschema(self.schema_file)
            self._first_pass_schema = generate_first_pass_schema(self._validated_schema)
            self._parsing_config = resolve_xsv_parsing_config(self._validated_schema)
            if (
                self.get_table_transform(self.table_name) is None
                and self._unreconciled_output_schemas is not None
                and self.table_name in self._unreconciled_output_schemas.registry
            ):
                self._pyarrow_schema = self._unreconciled_output_schemas.registry[self.table_name]
            else:
                self._pyarrow_schema = default_pyarrow_schema or convert_xsv_schema_to_pyarrow(self.validated_schema)
        except Exception as e:
            err_msg = f"Could not load JSON Schema for XSV data: {e!s}"
            logger.exception(err_msg)
            raise _classify_xsv_schema_error(e, err_msg) from e

        super()._resolve_settings_schema_map(default_pyarrow_schema=self._pyarrow_schema)

    @property
    def validated_schema(self) -> ValidatedXsvSchema:
        """The parsed, validated JSON Schema describing the XSV data."""
        if self._validated_schema is None:
            err_msg = "Schema has not been validated"
            raise RuntimeError(err_msg)
        return self._validated_schema

    @property
    def first_pass_schema(self) -> ValidatedXsvSchema:
        """A loose schema used to validate only that each row has the correct number of columns."""
        if self._first_pass_schema is None:
            err_msg = "Schema has not been validated"
            raise RuntimeError(err_msg)
        return self._first_pass_schema

    @property
    def parsing_config(self) -> dict[str, Any]:
        """Cleaning/validation parameters for qsv, derived from the schema's x-xsv-config block."""
        if self._parsing_config is None:
            err_msg = "Schema has not been validated"
            raise RuntimeError(err_msg)
        return self._parsing_config

    @property
    def pyarrow_schema(self) -> pa.Schema:
        """The PyArrow schema used to type XSV columns during reading and output."""
        if self._pyarrow_schema is None:
            err_msg = "Schema has not been validated"
            raise RuntimeError(err_msg)
        return self._pyarrow_schema
