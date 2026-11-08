"""XSV-specific JSON Schema validation and header generation utilities."""

import json
from pathlib import Path
from typing import Any

import jsonschema
import jsonschema.validators
from pydantic import (
    DirectoryPath,
    computed_field,
    field_validator,
    validate_call,
)

from cdm_data_loaders.core.fields import CharStr, NonEmptyStr
from cdm_data_loaders.schemas.jsonschema import (
    CUSTOM_META_SCHEMA_URI,
    X_XSV_CONFIG_KEY,
    ValidatedSchema,
    register_xsv_validator,
)


class ValidatedXsvSchema(ValidatedSchema):
    """A JSON Schema to be used for parsing and validating XSV data."""

    @computed_field
    @property
    def has_xsv_parser_config(self) -> bool:
        """Whether or not this schema has custom fields for parsing XSV files."""
        return len(self.jsonschema.get(X_XSV_CONFIG_KEY, {}).keys()) > 0

    @computed_field
    @property
    def has_xsv_metaschema(self) -> bool:
        """Whether or not this schema has the custom metaschema URI indicating it holds XSV metadata."""
        return self.jsonschema.get("$schema") == CUSTOM_META_SCHEMA_URI

    @computed_field
    @property
    def required_cols(self) -> list[str]:
        """Retrieve the list of headers from the top-level ``required`` field."""
        return self.jsonschema.get("required", [])

    @field_validator("jsonschema", mode="after")
    @classmethod
    def validate_jsonschema(cls, schema: dict[str, Any]) -> dict[str, Any]:
        """Validate the JSON schema in the jsonschema field and ensure it can be used for xsv validation.

        :param schema: the schema
        :type  schema: dict[str, Any]
        :raises ValueError: if there is no ``$schema`` field to identify the metaschema used
        :raises jsonschema.exceptions.SchemaError: if the schema is invalid
        :raises jsonschema.exceptions.ValidationError: if the schema fails validation
        :raises ValueError: if there are no ``required`` fields to indicate the CSV columns and order
        :return: the validated schema
        :rtype : dict[str, Any]
        """
        register_xsv_validator()
        validated_schema = super().validate_jsonschema(schema)
        required_cols = validated_schema.get("required")
        if not isinstance(required_cols, list) or not required_cols:
            err_msg = "Could not find any required cols in schema"
            raise ValueError(err_msg)
        return validated_schema


def validate_jsonschema(schema_path: Path) -> ValidatedXsvSchema:
    """Ensure that a given file contains a valid XSV JSON schema.

    :param schema_path: path to the JSON Schema file
    :type schema_path: Path
    :raises jsonschema.exceptions.SchemaError: if the schema is invalid
    :return: validated XSV JSON Schema
    :rtype: ValidatedXsvSchema
    """
    schema = json.loads(schema_path.read_bytes())
    return ValidatedXsvSchema(jsonschema=schema)


@validate_call
def generate_first_pass_schema(validated_schema: ValidatedXsvSchema) -> ValidatedXsvSchema:
    """Given a full schema file, generate a schema to perform a loose first-pass validation with.

    The first pass schema is used for verifying that the top-level columns are correct; no further checks
    are performed.

    :param validated_schema: parsed, validated JSON schema
    :type validated_schema: ValidatedXsvSchema
    :return: the validated first pass schema
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
    header_file_name: NonEmptyStr = "header.txt",
    delimiter: CharStr = "\t",
) -> Path:
    r"""Generate a header file for an xSV file from a JSON Schema.

    If no file name is supplied, the header file is saved as ``header.txt``.

    The top level required properties are assumed to be the columns of the xSV file.

    :param validated_schema: parsed, validated JSON schema
    :type validated_schema: ValidatedXsvSchema
    :param target_dir: directory in which to save the file
    :type target_dir: DirectoryPath
    :param header_file_name: name for the header file, defaults to "header.txt"
    :type header_file_name: NonEmptyStr, optional
    :param delimiter: delimiter to use for the headers, defaults to "\t"
    :type delimiter: CharStr, optional
    :return: path to the newly-created header.txt file
    :rtype: Path
    """
    header_file_path = target_dir / header_file_name
    header_file_path.write_text(delimiter.join(validated_schema.required_cols) + "\n")
    return header_file_path


def get_schema_parsing_metadata(validated_schema: ValidatedXsvSchema) -> dict[str, Any]:
    """Read in the schema parser config information from the supplied schema.

    :param validated_schema: parsed, validated JSON schema
    :type validated_schema: ValidatedXsvSchema
    :raises ValueError: if no xsv config information is found in the schema
    :return: parsed xsv schema metadata
    :rtype: dict[str, Any]
    """
    if not validated_schema.has_xsv_parser_config:
        err_msg = "No xsv config information found in schema"
        raise ValueError(err_msg)

    return {
        k.replace("x-", "").replace("-", "_"): v
        for k, v in validated_schema.jsonschema.get(X_XSV_CONFIG_KEY, {}).items()
    }
