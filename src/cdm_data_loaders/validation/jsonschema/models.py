"""Validated JSON Schema and XSV JSON Schema models and header generation helpers."""

import json
import logging
from pathlib import Path
from typing import Annotated, Any

import jsonschema.exceptions
import jsonschema.validators
from pydantic import (
    BaseModel,
    DirectoryPath,
    StringConstraints,
    computed_field,
    field_validator,
    validate_call,
)

from cdm_data_loaders.validation.jsonschema.metaschema import (
    CUSTOM_META_SCHEMA_URI,
    X_XSV_CONFIG_KEY,
    register_xsv_validator,
)

logger: logging.Logger = logging.getLogger(__name__)

ModelNonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
ModelCharStr = Annotated[str, StringConstraints(min_length=1, max_length=1)]


class ValidatedSchema(BaseModel):
    """A JSON Schema document validated against its declared ``$schema`` metaschema."""

    jsonschema: dict[str, Any]

    @field_validator("jsonschema", mode="after")
    @classmethod
    def validate_jsonschema(cls, schema: dict[str, Any]) -> dict[str, Any]:
        """Validate ``schema`` against the metaschema identified by its ``$schema`` keyword.

        :param schema: candidate JSON Schema dictionary
        :type schema: dict[str, Any]
        :return: validated JSON Schema dictionary
        :rtype: dict[str, Any]
        :raises ValueError: if ``$schema`` is missing or identifies an unsupported draft
        :raises jsonschema.exceptions.SchemaError: if the schema fails metaschema validation
        :raises jsonschema.exceptions.ValidationError: if validation raises a schema validation error
        """
        if "$schema" not in schema or not isinstance(schema["$schema"], str):
            err_msg = "JSON Schema is missing the $schema keyword"
            raise ValueError(err_msg)

        declared = schema["$schema"]
        if declared == CUSTOM_META_SCHEMA_URI:
            register_xsv_validator()

        validator = jsonschema.validators.validator_for(schema, default=None)  # type: ignore[arg-type]
        if validator is None:
            err_msg = f"Unsupported JSON Schema draft: {declared}"
            raise ValueError(err_msg)

        try:
            validator.check_schema(schema)
        except (jsonschema.exceptions.SchemaError, jsonschema.exceptions.ValidationError):
            logger.exception("Error validating JSON Schema")
            raise
        return schema


class ValidatedXsvSchema(ValidatedSchema):
    """A JSON Schema document validated for tabular XSV parsing and validation."""

    @computed_field
    @property
    def has_xsv_parser_config(self) -> bool:
        """Return whether this schema contains non-empty ``x-xsv-config`` metadata."""
        return len(self.jsonschema.get(X_XSV_CONFIG_KEY, {}).keys()) > 0

    @computed_field
    @property
    def has_xsv_metaschema(self) -> bool:
        """Return whether ``$schema`` equals ``CUSTOM_META_SCHEMA_URI``."""
        return self.jsonschema.get("$schema") == CUSTOM_META_SCHEMA_URI

    @computed_field
    @property
    def required_cols(self) -> list[str]:
        """Return the ordered column names from the top-level ``required`` list."""
        return self.jsonschema.get("required", [])

    @field_validator("jsonschema", mode="after")
    @classmethod
    def validate_jsonschema(cls, schema: dict[str, Any]) -> dict[str, Any]:
        """Validate ``schema`` and require a non-empty top-level ``required`` column list.

        :param schema: candidate XSV JSON Schema dictionary
        :type schema: dict[str, Any]
        :return: validated XSV JSON Schema dictionary
        :rtype: dict[str, Any]
        :raises ValueError: if ``$schema`` or ``required`` is missing/empty or uses an unsupported draft
        :raises jsonschema.exceptions.SchemaError: if the schema fails metaschema validation
        """
        validated_schema = super().validate_jsonschema(schema)
        required_cols = validated_schema.get("required")
        if not isinstance(required_cols, list) or not required_cols:
            err_msg = "Could not find any required cols in schema"
            raise ValueError(err_msg)
        return validated_schema


def validate_xsv_jsonschema(schema_path: Path) -> ValidatedXsvSchema:
    """Load and validate an XSV JSON Schema file from ``schema_path``.

    :param schema_path: path to the JSON Schema file
    :type schema_path: Path
    :return: validated XSV JSON Schema model
    :rtype: ValidatedXsvSchema
    :raises jsonschema.exceptions.SchemaError: if the schema fails metaschema validation
    """
    schema = json.loads(schema_path.read_bytes())
    return ValidatedXsvSchema(jsonschema=schema)


@validate_call
def generate_first_pass_schema(validated_schema: ValidatedXsvSchema) -> ValidatedXsvSchema:
    """Build a permissive string-or-null first-pass schema checking top-level required columns.

    :param validated_schema: parsed, validated XSV JSON Schema
    :type validated_schema: ValidatedXsvSchema
    :return: validated first-pass XSV JSON Schema
    :rtype: ValidatedXsvSchema
    """
    first_pass_schema = {
        k: v for k, v in validated_schema.jsonschema.items() if k in ["$schema", "$id", "title", "required"]
    }
    first_pass_schema["type"] = "object"
    first_pass_schema["properties"] = {req: {"type": ["string", "null"]} for req in validated_schema.required_cols}

    validator = jsonschema.validators.validator_for(first_pass_schema)
    validator.check_schema(first_pass_schema)

    return ValidatedXsvSchema(jsonschema=first_pass_schema)


@validate_call
def generate_header(
    validated_schema: ValidatedXsvSchema,
    target_dir: DirectoryPath,
    header_file_name: ModelNonEmptyStr = "header.txt",
    delimiter: ModelCharStr = "\t",
) -> Path:
    r"""Write an XSV header file from ``validated_schema.required_cols`` inside ``target_dir``.

    :param validated_schema: parsed, validated XSV JSON Schema
    :type validated_schema: ValidatedXsvSchema
    :param target_dir: directory in which to save the header file
    :type target_dir: DirectoryPath
    :param header_file_name: file name for the header file, defaults to ``'header.txt'``
    :type header_file_name: ModelNonEmptyStr
    :param delimiter: column delimiter, defaults to ``'\t'``
    :type delimiter: ModelCharStr
    :return: path to the generated header file
    :rtype: Path
    """
    header_file_path = target_dir / header_file_name
    header_file_path.write_text(delimiter.join(validated_schema.required_cols) + "\n", encoding="utf-8")
    return header_file_path


def get_schema_parsing_metadata(validated_schema: ValidatedXsvSchema) -> dict[str, Any]:
    """Extract normalized ``x-xsv-config`` parser parameters from ``validated_schema``.

    :param validated_schema: parsed, validated XSV JSON Schema
    :type validated_schema: ValidatedXsvSchema
    :return: dictionary of XSV parser parameters with ``x-`` stripped and ``-`` replaced by ``_``
    :rtype: dict[str, Any]
    :raises ValueError: if ``validated_schema`` has no ``x-xsv-config`` metadata
    """
    if not validated_schema.has_xsv_parser_config:
        err_msg = "No xsv config information found in schema"
        raise ValueError(err_msg)

    return {
        k.replace("x-", "").replace("-", "_"): v
        for k, v in validated_schema.jsonschema.get(X_XSV_CONFIG_KEY, {}).items()
    }
