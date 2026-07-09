"""xSV Validation using xsv-validate shell script with qsv back-end."""

import contextlib
import json
import subprocess
from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


class Status(StrEnum):
    """Validation result status."""

    VALID = auto()
    INVALID = auto()
    ERROR = auto()


_STATUS_MESSAGE: dict[Status, str] = {
    Status.VALID: "All data are valid",
    Status.INVALID: "At least one invalid record",
    Status.ERROR: "An error occurred during validation",
}


@dataclass(frozen=True)
class ValidationResults:
    """Status and statistics from a validation attempt."""

    status: Status
    message: str
    valid_rows: int | None
    invalid_rows: int | None
    valid_records_file: Path | None
    invalid_records_file: Path | None
    errors_file: Path | None


def validate(  # noqa: PLR0913
    file_path: Path,
    *,
    schema: Path | dict,
    output_path: Path,
    comment_char: str = "#",
    delimiter: str | None = None,
    missing_header: bool = False,
    null_strings: set[str] | None = None,
    skip_lines: int = 0,
    summary: bool = False,
) -> ValidationResults:
    """Validate a character-delimited text file against a provided JSONSchema.

    When `summary` is `False`, the results include only the status and generic message.

    :param file_path: data file to validate
    :type file_path: Path
    :param schema: JSONSchema (as a file or a dict)
    :type schema: Path | dict
    :param output_path: Path to the folder for output files (will be created if necessary)
    :type output_path: Path
    :param comment_char: Character used to indicate a comment line (default: #)
    :type comment_char: str
    :param delimiter: Character used to separate columns (set to None to auto-detect; default: None)
    :type delimiter: str | None
    :param missing_header: Flag indicating a xSV file has no header (one will be generated from the schema; default: False)
    :type missing_header: bool
:param null_strings: Set of strings that indicate NULL values (set to None to use standard set; default: None)
    :type null_strings: set[str] | None
    :param skip_lines: Number of lines to ignore at the top of the file (default: 0)
    :type skip_lines: int
    :param summary: Flag to return summary info from the validation (default: False)
    :type summary: bool
    :return: Summary information if `summary==True`, otherwise just the status and generic message
    :rtype: ValidationResults
    """
    # validation
if len(comment_char) != 1:
        msg = f"Invalid comment character: '{comment_char}'; must be a single character."
        raise ValueError(msg)
    if delimiter is not None and len(delimiter) != 1:
        msg = f"Invalid delimiter:'{delimiter}'; must be a single character or None."
        raise ValueError(msg)
    null_strings = null_strings or set()
    if "" in null_strings:
        msg = "Null strings must include at least one character each"
        raise ValueError(msg)
    if not file_path.is_file():
        msg = f"Input file '{file_path}' does not exist."
        raise ValueError(msg)

    with contextlib.ExitStack() as stack:
        # prepare inputs for shell script
        if isinstance(schema, dict):
            temp_dir = stack.enter_context(TemporaryDirectory())
            schema_path = Path(temp_dir) / "schema.json"
            with schema_path.open("w") as f:
                json.dump(schema, f)
        else:
            schema_path: Path = schema

        # valid with xsv-validate.sh
        args: list[str] = [
            "xsv-validate.sh",
            str(file_path),
            "-s",
            str(schema_path),
            "-o",
            str(output_path),
            "--comment",
            comment_char,
            *(["--delimiter", delimiter] if delimiter is not None else []),
            *(["--missing-header"] if missing_header else []),
            *[arg for elem in null_strings for arg in ("--null", elem)],
            "--skip-lines",
            str(skip_lines),
            *(["--summary-file"] if summary else []),
        ]
        result = subprocess.run(args, capture_output=True, text=True)  # noqa: S603, PLW1510

    # assemble results
    match result.returncode:
        case 0:
            status = Status.VALID
        case 1:
            status = Status.INVALID
        case _:
            status = Status.ERROR
    summary_path = output_path / f"{file_path.name}.summary.json"
    summary_data: dict[str, Any] = {}
    if summary_path.is_file():
        with summary_path.open("r") as f:
            summary_data = json.load(f)

    return ValidationResults(
        status=status,
        message=summary_data.get("status_message", _STATUS_MESSAGE[status]),
        valid_rows=summary_data.get("valid_rows"),
        invalid_rows=summary_data.get("invalid_rows"),
        valid_records_file=(Path(p) if (p := summary_data.get("valid_records_file")) else None),
        invalid_records_file=(Path(p) if (p := summary_data.get("invalid_records_file")) else None),
        errors_file=(Path(p) if (p := summary_data.get("errors_file")) else None),
    )
