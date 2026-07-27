import json
import os
import subprocess
from functools import cached_property
from pathlib import Path
from shutil import copy, move
from typing import Annotated, Any, Final, Self, TypedDict

from pydantic import BaseModel, DirectoryPath, FilePath, StringConstraints, computed_field, validate_call

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
CharStr = Annotated[str, StringConstraints(min_length=1, max_length=1)]


VALIDATION_ERRORS: Final[str] = ".validation-errors.tsv"

SEP_TO_EXT: Final[dict[str, str]] = {"\t": ".tsv", ",": ".csv", ";": ".ssv"}

# Suffixes used when constructing intermediate/output file names for each pipeline stage.
HEADER_SUFFIX: Final[str] = "-header"
CLEANED_SUFFIX: Final[str] = "-cleaned"
NORM_SUFFIX: Final[str] = "-norm"
VALID_SUFFIX: Final[str] = "-valid"


class ErrorRecord(BaseModel):
    """A single error encountered while cleaning/validating a file.

    Used so that ``summary["errors"]`` always contains a uniform type, regardless of whether the
    error came from a failed qsv subprocess or an ``OSError`` raised while copying/moving files.

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
        return cls(file=file_name, message=result.stderr or "", returncode=result.returncode)

    @classmethod
    def from_exception(cls, file_name: str, exc: Exception) -> Self:
        """Build an `ErrorRecord` from a raised exception (e.g. `OSError`)."""
        return cls(file=file_name, message=str(exc))


class Summary(TypedDict):
    """Structure of the summary object threaded through the clean/validate pipeline.

    A single `Summary` instance is shared across every `CleanerValidatorArgs` built for a batch
    of xsv files, accumulating results as each file is processed by `clean_validate_file`.

    :param errors: errors encountered while cleaning/validating files, from either failed qsv
        subprocess calls or filesystem operations (copy/move)
    :type errors: list[ErrorRecord]
    :param valid: mapping of original input file name to the path of its validated output file
    :type valid: dict[str, Path]
    :param invalid: paths to the `*.validation-errors.tsv` reports produced for files that failed
        the post-normalisation schema check
    :type invalid: list[Path]
    """

    errors: list[ErrorRecord]
    valid: dict[str, Path]
    invalid: list[Path]


def new_summary() -> Summary:
    """Construct a fresh, empty `Summary` object.

    :return: an empty summary with all fields initialised
    :rtype: Summary
    """
    return Summary(errors=[], valid={}, invalid=[])


def _run_qsv(cmd: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run a qsv subcommand, capturing stderr as text."""
    return subprocess.run(cmd, text=True, stderr=subprocess.PIPE, env=env)


def _run_qsv_step(cmd: list[str], args: "CleanerValidatorArgs") -> subprocess.CompletedProcess[str]:
    """Run a qsv subcommand and record an `ErrorRecord` in the summary if it fails.

    :param cmd: qsv command and arguments to run
    :type cmd: list[str]
    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :return: the completed process, whether it succeeded or failed
    :rtype: subprocess.CompletedProcess[str]
    """
    result = _run_qsv(cmd, args.qsv_env)
    if result.returncode != 0:
        args.summary["errors"].append(ErrorRecord.from_qsv_result(args.file_name, result))
    return result


def _safe_copy(src: Path, dst: Path, args: "CleanerValidatorArgs") -> bool:
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
        args.summary["errors"].append(ErrorRecord.from_exception(args.file_name, exc))
        return False
    return True


def _safe_move(src: Path, dst: Path, args: "CleanerValidatorArgs") -> bool:
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
        args.summary["errors"].append(ErrorRecord.from_exception(args.file_name, exc))
        return False
    return True


def count_lines(path: Path) -> int:
    """Count the number of lines in a file without loading it fully into memory.

    :param path: path to the file to count lines in
    :type path: Path
    :return: number of lines in the file
    :rtype: int
    """
    with path.open("rb") as fh:
        return sum(1 for _ in fh)


def _prepend_header(header_file_path: Path, xsv_file_path: Path, dest: Path) -> None:
    """Write a new file at `dest` consisting of the header file's contents followed by the data file's contents."""
    with dest.open("wb") as out:
        out.write(header_file_path.read_bytes())
        out.write(xsv_file_path.read_bytes())


@validate_call
def generate_header(
    target_dir: DirectoryPath,
    schema_path: FilePath,
    header_file_name: NonEmptyStr = "header.txt",
    delimiter: CharStr = "\t",
) -> Path:
    r"""Generate a header file for an xSV file from a JSON Schema.

    If no file name is supplied, the header file is saved as `header.txt`.

    The top level required properties are assumed to be the columns of the xSV file.

    :param target_dir: directory in which to save the file
    :type target_dir: DirectoryPath
    :param schema_path: path to the schema
    :type schema_path: Path
    :param delimiter: delimiter to use for the headers, defaults to "\t"
    :type delimiter: CharStr, optional
    :param header_file_name: name for the header file, defaults to "header.txt"
    :type header_file_name: str, optional
    :raises RuntimeError: if the schema file has no `required` cols
    :return: path to the newly-created header.txt file
    :rtype: Path
    """
    schema = json.loads(schema_path.read_bytes())
    if not isinstance(schema, dict):
        err_msg = f"The JSON Schema at {schema_path!s} must be a dictionary"
        raise TypeError(err_msg)

    required_cols = schema.get("required")
    if not isinstance(required_cols, list) or not required_cols:
        err_msg = f"Could not find any required cols in {schema_path!s}"
        raise ValueError(err_msg)

    header_file_path = target_dir / header_file_name
    header_file_path.write_text(delimiter.join(required_cols) + "\n")
    return header_file_path


class CleanerValidatorArgs(BaseModel):
    """Class containing required parameters for running the XSV file clean/validate functions.

    :param summary: dictionary with summary of output files, errors, etc.
    :type summary: Summary
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
    :param output_dir_path: directory to put the cleaned, validated files in
    :type output_dir_path: DirectoryPath
    :param header_line_count: number of lines in the header file, used to detect header-only (i.e. empty)
        files. Since `header_file_path` is typically shared across many `CleanerValidatorArgs` instances
        (one per xsv file being processed), callers should compute this once via `count_lines` and pass
        it in, rather than have it recomputed on every instance.
    :type header_line_count: int
    :param delimiter: delimiter used in the xsv file, defaults to tab
    :type delimiter: CharStr, optional
    :param comment_char: comment character used in the xsv file, defaults to "#"
    :type comment_char: CharStr, optional
    :param null_value: exact string value to replace with an empty string (e.g. a literal "NA" placeholder
        for nulls). Passed to `qsv replace --exact`, so this is matched as an exact string, not a regex.
    :type null_value: NonEmptyStr, optional
    :param missing_header: whether or not the xsv file has a missing header, defaults to False
    :type missing_header: bool, optional
    """

    summary: Summary
    qsv_cmd: NonEmptyStr

    xsv_file_path: FilePath
    header_file_path: FilePath
    first_pass_schema: FilePath
    post_norm_schema: FilePath

    tmp_dir_path: DirectoryPath
    output_dir_path: DirectoryPath

    header_line_count: int

    delimiter: CharStr = "\t"
    comment_char: CharStr = "#"
    null_value: NonEmptyStr | None = None
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


@validate_call
def clean_validate_file(args: CleanerValidatorArgs) -> None:
    """Clean and validate an xSV file.

    Files undergo the following steps:

    - headers are added if missing_header=True
    - preliminary validation removes ragged rows
    - files are normalised using xsv input
    - nulls are replaced if null_value != None
    - second validation occurs with stricter criteria to ensure fields match

    Files are validated in two stages: the first pass schema is used for very rough validation and basically
    ensures that there are the correct number of columns in each row.

    The second validation occurs after the files are tidied up, nulls removed, and so on.

    Note: there is currently no way to set custom escape characters or quotes in `qsv validate`, so lines with
    a delimiter character as part of the field may fail the first pass validation for having too many cols.

    :param args: object with all the useful params required for running the clean/validate workflow
    :type args: CleanerValidatorArgs
    """
    file_with_headers = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"

    if args.missing_header:
        # add header and save in tmp dir
        try:
            _prepend_header(args.header_file_path, args.xsv_file_path, args.tmp_dir_path / file_with_headers)
        except OSError as exc:
            args.summary["errors"].append(ErrorRecord.from_exception(args.file_name, exc))
            return
    # otherwise, just copy the file into the temp dir with the appropriate extension
    elif not _safe_copy(args.xsv_file_path, args.tmp_dir_path / file_with_headers, args):
        return

    file_for_input_cmd = run_first_pass_validation(args, file_with_headers)

    if not file_for_input_cmd:
        return

    file_for_rplc_cmd = run_qsv_input(args, file_for_input_cmd)
    if not file_for_rplc_cmd:
        return

    # run qsv replace
    # if there is nothing specified as null_value, do not run the command
    if args.null_value:
        file_for_validate_cmd = run_qsv_null_replacement(args, file_for_rplc_cmd)
        if not file_for_validate_cmd:
            return
    else:
        file_for_validate_cmd = file_for_rplc_cmd

    run_validate(args, file_for_validate_cmd)


def run_qsv_input(args: CleanerValidatorArgs, input_file_name: str) -> str | None:
    """Run qsv input to normalise the file input_file_name.

    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :param input_file_name: name of the input file
    :type input_file_name: str
    :return: name of the output file if all went ok; otherwise None
    :rtype: str | None
    """
    if not (args.tmp_dir_path / input_file_name).is_file():
        err_msg = f"Input file not found at {(args.tmp_dir_path / input_file_name)!s}"
        raise RuntimeError(err_msg)

    output_file_name = f"{args.xsv_file_base_name}{CLEANED_SUFFIX}{args.ext}"

    cmd = [
        args.qsv_cmd,
        "input",
        "--trim-headers",
        "--trim-fields",
        "--delimiter",
        args.delimiter,
        "--encoding-errors",
        "strict",
        "--output",
        str(args.tmp_dir_path / output_file_name),
        str(args.tmp_dir_path / input_file_name),
    ]
    result = _run_qsv_step(cmd, args)
    if result.returncode == 0:
        return output_file_name

    output_path = args.tmp_dir_path / output_file_name
    if output_path.exists():
        _safe_copy(output_path, args.output_dir_path / output_file_name, args)
    return None


def run_qsv_null_replacement(args: CleanerValidatorArgs, input_file_name: str) -> str | None:
    """Run qsv replace to replace any exact matches of `args.null_value` in input_file_name with "".

    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :param input_file_name: name of the input file
    :type input_file_name: str
    :return: name of the output file if all went ok; otherwise None
    :rtype: str | None
    """
    if not (args.tmp_dir_path / input_file_name).is_file():
        err_msg = f"Input file not found at {(args.tmp_dir_path / input_file_name)!s}"
        raise RuntimeError(err_msg)

    output_file_name = f"{args.xsv_file_base_name}{NORM_SUFFIX}{args.ext}"
    cmd = [
        args.qsv_cmd,
        "replace",
        "--exact",
        args.null_value,
        "",
        "--delimiter",
        args.delimiter,
        "--output",
        str(args.tmp_dir_path / output_file_name),
        str(args.tmp_dir_path / input_file_name),
    ]
    result = _run_qsv_step(cmd, args)
    if result.returncode == 0:
        return output_file_name

    # if no matches are found, returncode will be 1 and stderr will be set to '0\n'
    if result.returncode == 1 and result.stderr.strip() == "0":
        if (args.tmp_dir_path / output_file_name).exists():
            return output_file_name
        return input_file_name

    # some other kind of error - copy the input file into the output dir, return
    rplc_input_path = args.tmp_dir_path / input_file_name
    if rplc_input_path.exists():
        _safe_copy(rplc_input_path, args.output_dir_path / input_file_name, args)
    return None


def run_first_pass_validation(args: CleanerValidatorArgs, input_file_name: str) -> str | None:
    """Run the first pass of the validator over the file input_file_name.

    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :param input_file_name: name of the input file
    :type input_file_name: str
    :return: name of the output file if all went ok; otherwise None
    :rtype: str | None
    """
    if not (args.tmp_dir_path / input_file_name).is_file():
        err_msg = f"Input file not found at {(args.tmp_dir_path / input_file_name)!s}"
        raise RuntimeError(err_msg)

    # run initial validation: removes lines with incorrect number of cols, type mismatches, and any comments
    valid_lines_file = args.tmp_dir_path / f"{input_file_name}.{args.valid_file_suffix}"

    cmd = [
        args.qsv_cmd,
        "validate",
        "--delimiter",
        args.delimiter,
        "--no-format-validation",
        "--split-ragged",
        "--trim",
        str(args.tmp_dir_path / input_file_name),
        str(args.first_pass_schema),
        "--valid",
        args.valid_file_suffix,
    ]
    result = _run_qsv_step(cmd, args)
    if result.returncode == 0:
        # no output files produced; all lines are valid.
        # Use the current file as input for the next step
        return input_file_name

    # if errors are found, produces three files:
    # {input_file_name}.valid -- valid lines, CSV format
    # {input_file_name}.invalid -- lines that fail validation, CSV format
    # {input_file_name}{VALIDATION_ERRORS} -- list of all the errors found
    # comments are automatically removed and do not appear in .valid or .invalid files
    errors_file = args.tmp_dir_path / f"{input_file_name}{VALIDATION_ERRORS}"

    if errors_file.exists():
        # save error report to the output dir, if it was produced
        _safe_copy(errors_file, args.output_dir_path / errors_file.name, args)

    # exit back to caller if there is no valid_lines file
    if not valid_lines_file.exists():
        return None

    # new file name with appropriate extension -- will be name {file_base-name}-header-valid.ext
    valid_lines_renamed = f"{input_file_name.removesuffix(args.ext)}{VALID_SUFFIX}{args.ext}"
    # copy the valid lines to output dir
    _safe_copy(valid_lines_file, args.output_dir_path / valid_lines_renamed, args)

    # check whether the valid lines file contains only the header; if so, there is no useful content
    if count_lines(valid_lines_file) <= args.header_line_count:
        return None

    # otherwise, rename to remove the extra extension and continue the analysis
    if not _safe_move(valid_lines_file, args.tmp_dir_path / valid_lines_renamed, args):
        return None
    return valid_lines_renamed


def run_validate(args: CleanerValidatorArgs, input_file_name: str) -> None:
    """Run qsv validate on the normalised file input_file_name.

    Format checking is enabled.

    :param args: args for the cleaner validator
    :type args: CleanerValidatorArgs
    :param file_for_validate_cmd: name of the normalised file to run the final validation pass on
    :type file_for_validate_cmd: str
    """
    if not (args.tmp_dir_path / input_file_name).is_file():
        err_msg = f"Input file not found at {(args.tmp_dir_path / input_file_name)!s}"
        raise RuntimeError(err_msg)

    cmd = [
        args.qsv_cmd,
        "validate",
        str(args.tmp_dir_path / input_file_name),
        str(args.post_norm_schema),
        "--delimiter",
        args.delimiter,
        "--valid",
        args.valid_file_suffix,
    ]
    result = _run_qsv_step(cmd, args)

    validated_file_name = f"{args.xsv_file_base_name}{VALID_SUFFIX}{args.ext}"
    if result.returncode == 0:
        # all lines valid. Copy the input file into the output dir
        if _safe_copy(args.tmp_dir_path / input_file_name, args.output_dir_path / validated_file_name, args):
            args.summary["valid"][args.file_name] = args.output_dir_path / validated_file_name
        return

    # copy over the validation errors, if produced
    errors_path = args.tmp_dir_path / f"{input_file_name}{VALIDATION_ERRORS}"
    if errors_path.exists() and _safe_copy(errors_path, args.output_dir_path / errors_path.name, args):
        args.summary["invalid"].append(args.output_dir_path / errors_path.name)

    # and copy over the valid lines, if produced
    valid_path = args.tmp_dir_path / f"{input_file_name}.{args.valid_file_suffix}"
    if valid_path.exists():
        _safe_copy(valid_path, args.output_dir_path / validated_file_name, args)
