"""Helpers for the qsv validator."""

import json
import os
import subprocess
from functools import cached_property
from logging import Logger, getLogger
from pathlib import Path
from shutil import copy, move
from typing import Annotated, Any, Final, Self

import jsonschema
import jsonschema.exceptions
import jsonschema.validators
from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    FilePath,
    StringConstraints,
    computed_field,
    validate_call,
)

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
CharStr = Annotated[str, StringConstraints(min_length=1, max_length=1)]


VALIDATION_ERRORS: Final[str] = ".validation-errors.tsv"

SEP_TO_EXT: Final[dict[str, str]] = {"\t": ".tsv", ",": ".csv", ";": ".ssv"}

# Suffixes used when constructing intermediate/output file names for each pipeline stage.
HEADER_SUFFIX: Final[str] = "-header"
CLEANED_SUFFIX: Final[str] = "-cleaned"
NORM_SUFFIX: Final[str] = "-norm"
VALID_SUFFIX: Final[str] = "-valid"


logger: Logger = getLogger(__name__)


class ErrorRecord(BaseModel):
    """A single error encountered while cleaning/validating a file.

    Used so that ``errors`` always contains a uniform type, regardless of whether the
    error came from a failed qsv subprocess, an ``OSError`` raised while copying/moving files, or
    any other type of fun error that might occur.

    :param file: name of the xsv file being processed when the error occurred
    :type file: str
    :param message: human-readable description of the error
    :type message: str
    :param returncode: return code of the qsv subprocess, if the error originated from one
    :type returncode: int | None, optional
    """

    file: str
    message: str
    returncode: int | None = None

    @classmethod
    def from_qsv_result(cls, file_name: str, result: subprocess.CompletedProcess[str]) -> Self:
        """Build an `ErrorRecord` from a failed qsv subprocess result."""
        return cls(file=file_name, message=(result.stderr or "").strip(), returncode=result.returncode)

    @classmethod
    def from_exception(cls, file_name: str, exc: Exception) -> Self:
        """Build an `ErrorRecord` from a raised exception (e.g. `OSError`)."""
        return cls(file=file_name, message=str(exc))


class CleanerValidatorArgs(BaseModel):
    """Class containing required parameters for running the XSV file clean/validate functions.

    :param qsv_cmd: command for executing qsv
    :type qsv_cmd: str
    :param xsv_file_path: path to the data file to be validated
    :type xsv_file_path: FilePath
    :param header_file_path: path to the generated header file
    :type header_file_path: FilePath
    :param first_pass_schema: schema for initial validation of the data
    :type first_pass_schema: FilePath
    :param post_norm_schema: schema for validating data after cleaning and normalisation
    :type post_norm_schema: FilePath
    :param tmp_dir_path: temporary working directory
    :type tmp_dir_path: DirectoryPath
    :param qsv_output_dir_path: directory for qsv output from failed executions
    :type qsv_output_dir_path: DirectoryPath
    :param validated_file_dir_path: directory to put the cleaned, validated files in
    :type validated_file_dir_path: DirectoryPath
    :param errors: list of errors accumulated during processing
    :type errors: list[ErrorRecord]
    :param delimiter: delimiter used in the xsv file, defaults to tab
    :type delimiter: CharStr, optional
    :param comment_char: comment character used in the xsv file, defaults to `#`
    :type comment_char: CharStr, optional
    :param quote: quote character used in the xsv file; if unset, qsv will assume it is `"`
    :type quote: CharStr, optional
    :param escape: the escape character used in the xsv file. If not specified, qsv escapes quotes by doubling them. Default None
    :type escape: NonEmptyStr, optional
    :param null_regex: regex to replace with an empty string
    :type null_regex: NonEmptyStr, optional
    :param null_regex_cols: list of columns to apply the null regex to, optional; defaults to None (all cols)
    :type null_regex_cols: list[NonEmptyStr] | None
    :param missing_header: whether or not the xsv file has a missing header, defaults to False
    :type missing_header: bool, optional
    """

    qsv_cmd: NonEmptyStr

    xsv_file_path: FilePath
    header_file_path: FilePath
    first_pass_schema: FilePath
    post_norm_schema: FilePath

    tmp_dir_path: DirectoryPath
    qsv_output_dir_path: DirectoryPath
    validated_file_dir_path: DirectoryPath

    errors: list[ErrorRecord] = Field(default_factory=lambda _: [])

    delimiter: CharStr = "\t"
    comment_char: CharStr = "#"
    quote: CharStr | None = None
    escape: NonEmptyStr | None = None
    null_regex: NonEmptyStr | None = None
    null_regex_cols: list[NonEmptyStr] | None = None
    missing_header: bool = False

    @computed_field
    @cached_property
    def qsv_env(self: Self) -> dict[str, str]:
        """Environment for qsv commands to run in."""
        return {**os.environ, "QSV_COMMENT_CHAR": self.comment_char}

    @computed_field
    @property
    def xsv_file_base_name(self: Self) -> str:
        """Base name of the input xsv file, without the final suffix."""
        return self.xsv_file_path.stem

    @computed_field
    @property
    def file_name(self) -> str:
        """Name of the input xsv file."""
        return self.xsv_file_path.name

    @computed_field
    @property
    def ext(self) -> str:
        """Extension for the xsv files; includes the `.`."""
        return SEP_TO_EXT.get(self.delimiter) or self.xsv_file_path.suffix

    @computed_field
    @property
    def valid_file_suffix(self) -> str:
        """Suffix added to validator output file containing valid lines."""
        return f"valid{self.ext}"

    @computed_field
    @property
    def invalid_file_suffix(self) -> str:
        """Suffix added to validator output file containing valid lines."""
        return f"invalid{self.ext}"


def copy_safely(src: Path, dst: Path, args: "CleanerValidatorArgs") -> bool:
    """Copy `src` to `dst`, recording an `ErrorRecord` and returning False on failure.

    :param src: source path
    :type src: Path
    :param dst: destination path
    :type dst: Path
    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :return: True if the copy succeeded, False otherwise
    :rtype: bool
    """
    try:
        copy(src, dst)
    except OSError as exc:
        args.errors.append(ErrorRecord.from_exception(args.file_name, exc))
        return False
    return True


def move_safely(src: Path, dst: Path, args: "CleanerValidatorArgs") -> bool:
    """Move `src` to `dst`, recording an `ErrorRecord` and returning False on failure.

    :param src: source path
    :type src: Path
    :param dst: destination path
    :type dst: Path
    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :return: True if the move succeeded, False otherwise
    :rtype: bool
    """
    try:
        move(src, dst)
    except OSError as exc:
        args.errors.append(ErrorRecord.from_exception(args.file_name, exc))
        return False
    return True


def non_header_lines_present(path: Path) -> bool:
    """Return True if `path` has any non-blank lines beyond the header line.

    Stops reading as soon as a second non-blank line is found.
    """
    non_blank_lines_seen = 0
    with path.open() as f:
        for line in f:
            if line.strip():
                non_blank_lines_seen += 1
                if non_blank_lines_seen > 1:
                    return True
    return False


def prepend_header(header_file_path: Path, xsv_file_path: Path, dest: Path) -> None:
    """Write a new file at `dest` consisting of the header file's contents followed by the data file's contents."""
    with dest.open("wb") as out:
        out.write(header_file_path.read_bytes())
        out.write(xsv_file_path.read_bytes())


def validate_jsonschema(schema_path: Path) -> dict[str, Any]:
    """Ensure that a given data structure is a valid JSON schema.

    :param schema: JSON schema, loaded as a python data structure
    :type schema: dict[str, Any]
    :raises jsonschema.exceptions.SchemaError: if the schema is invalid
    :return: validated JSON Schema
    :rtype: dict[str, Any]
    """
    schema = json.loads(schema_path.read_bytes())

    if not isinstance(schema, dict):
        err_msg = f"Error reading schema at {schema_path!s}: JSON Schema must be a dictionary"
        raise TypeError(err_msg)
    # $schema is not required by the JSON Schema metaschema, but our standards are a little higher here...
    if "$schema" not in schema:
        err_msg = f"Error reading schema at {schema_path!s}: JSON Schema is missing the $schema keyword"
        raise ValueError(err_msg)

    # retrieve the appropriate validator for the schema and ensure it is valid
    # if the $schema value is invalid, jsonschema will use the most recent draft by default and emit a warning
    validator = jsonschema.validators.validator_for(schema)
    try:
        validator.check_schema(schema)
    except (jsonschema.exceptions.SchemaError, jsonschema.exceptions.ValidationError):
        logger.exception("Error validating JSON Schema at %s", str(schema_path))
        raise
    return schema


def _get_schema_required_cols(schema_path: Path) -> tuple[dict[str, Any], list[str]]:
    """Retrieve the schema object and required top-level columns from a JSON schema file.

    Validates the input schema when the schema is read in.

    :param schema_path: path to the schema file
    :type schema_path: Path
    :raises ValueError: if there are no required columns in the schema
    :return: schema, required cols
    :rtype: tuple[dict[str, Any], list[str]]
    """
    schema = validate_jsonschema(schema_path)

    required_cols = schema.get("required")
    if not isinstance(required_cols, list) or not required_cols:
        err_msg = f"Could not find any required cols in {schema_path!s}"
        raise ValueError(err_msg)

    return schema, required_cols


@validate_call
def generate_first_pass_schema(schema_path: FilePath, output_dir: DirectoryPath) -> Path:
    """Given a full schema file, generate a schema to perform a loose first-pass validation with.

    The first pass schema is used for verifying that the top-level columns are correct; no further checks
    are performed.

    :param schema_path: path to the schema file
    :type schema_path: FilePath
    :param output_dir: directory to save the generated schema in
    :type output_dir: DirectoryPath
    :return: path to the first pass schema file
    :rtype: Path
    """
    schema, required_cols = _get_schema_required_cols(schema_path)
    new_schema = {k: v for k, v in schema.items() if k in ["$schema", "$id", "title", "required"]}

    new_schema["type"] = "object"
    new_schema["properties"] = {req: {"type": ["string", "null"]} for req in required_cols}

    # retrieve the appropriate validator for the schema and ensure it is valid
    validator = jsonschema.validators.validator_for(new_schema)
    validator.check_schema(new_schema)

    # save to the output dir as <schema_name>.first-pass.json
    first_pass_schema_path = output_dir / f"{schema_path.stem}.first-pass.json"
    json.dump(new_schema, first_pass_schema_path.open("w"), indent=2, sort_keys=True)

    return first_pass_schema_path


@validate_call
def generate_header(
    schema_path: FilePath,
    target_dir: DirectoryPath,
    header_file_name: NonEmptyStr = "header.txt",
    delimiter: CharStr = "\t",
) -> Path:
    r"""Generate a header file for an xSV file from a JSON Schema.

    If no file name is supplied, the header file is saved as `header.txt`.

    The top level required properties are assumed to be the columns of the xSV file.

    :param schema_path: path to the schema
    :type schema_path: Path
    :param target_dir: directory in which to save the file
    :type target_dir: DirectoryPath
    :param delimiter: delimiter to use for the headers, defaults to "\t"
    :type delimiter: CharStr, optional
    :param header_file_name: name for the header file, defaults to "header.txt"
    :type header_file_name: str, optional
    :raises TypeError: if the schema is not in the correct format (dictionary)
    :raises ValueError: if the schema file has no `required` cols
    :return: path to the newly-created header.txt file
    :rtype: Path
    """
    _, required_cols = _get_schema_required_cols(schema_path)
    header_file_path = target_dir / header_file_name
    header_file_path.write_text(delimiter.join(required_cols) + "\n")
    return header_file_path


def generate_qsv_validate_file_names(
    args: CleanerValidatorArgs,
    input_file_name: str,
    first_pass: bool = True,  # noqa: FBT001, FBT002
) -> tuple[Path, Path, Path, Path]:
    """Compute the file names run_qsv_validate generates from a file with validation errors.

    :param args: arguments
    :type args: CleanerValidatorArgs
    :param input_file_name: input file name
    :type input_file_name: str
    :param first_pass: whether or not this is the first validation pass, defaults to True
    :type first_pass: bool, optional
    :return: (valid_output_file, errors_file, valid_lines_file, invalid_lines_file)
    :rtype: tuple[Path, Path, Path, Path]
    """
    # file name for validated lines: {file_base_name}-header-valid.ext
    valid_output_file_name = (
        f"{args.xsv_file_base_name}-first-pass{VALID_SUFFIX}{args.ext}"
        if first_pass
        else f"{args.xsv_file_base_name}-normalised-validated{VALID_SUFFIX}{args.ext}"
    )

    valid_output_file = args.tmp_dir_path / valid_output_file_name
    errors_file = args.tmp_dir_path / f"{input_file_name}{VALIDATION_ERRORS}"
    valid_lines_file = args.tmp_dir_path / f"{input_file_name}.{args.valid_file_suffix}"
    invalid_lines_file = args.tmp_dir_path / f"{input_file_name}.{args.invalid_file_suffix}"

    return (valid_output_file, errors_file, valid_lines_file, invalid_lines_file)
