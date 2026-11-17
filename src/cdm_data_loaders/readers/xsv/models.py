"""Models, path helpers, and safe file operations for the qsv XSV validator."""

import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from shutil import copy, move
from typing import Any, Final, Self

from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    FilePath,
    computed_field,
)

from cdm_data_loaders.core.buffer import make_rejected_row
from cdm_data_loaders.core.fields import CharStr, NonEmptyStr
from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema

VALIDATION_ERRORS: Final[str] = ".validation-errors.tsv"

SEP_TO_EXT: Final[dict[str, str]] = {"\t": ".tsv", ",": ".csv", ";": ".ssv"}

HEADER_SUFFIX: Final[str] = "-header"
CLEANED_SUFFIX: Final[str] = "-cleaned"
NORM_SUFFIX: Final[str] = "-norm"
VALID_SUFFIX: Final[str] = "-valid"


class ErrorRecord(BaseModel):
    """A single error encountered while cleaning or validating an XSV file.

    :param file: Name of the XSV file being processed when the error occurred.
    :type file: str
    :param message: Human-readable description of the error.
    :type message: str
    :param returncode: Return code of the qsv subprocess, if the error originated from one.
    :type returncode: int | None
    """

    file: str
    message: str
    returncode: int | None = None

    @classmethod
    def from_qsv_result(cls, file_name: str, result: subprocess.CompletedProcess[str]) -> Self:
        """Build an :class:`ErrorRecord` from a failed qsv subprocess result.

        :param file_name: Source file identifier.
        :type file_name: str
        :param result: Completed qsv subprocess result.
        :type result: subprocess.CompletedProcess[str]
        :return: Populated error record.
        :rtype: Self
        """
        return cls(file=file_name, message=(result.stderr or "").strip(), returncode=result.returncode)

    @classmethod
    def from_exception(cls, file_name: str, exc: Exception) -> Self:
        """Build an :class:`ErrorRecord` from a raised exception.

        :param file_name: Source file identifier.
        :type file_name: str
        :param exc: Exception raised during processing.
        :type exc: Exception
        :return: Populated error record.
        :rtype: Self
        """
        return cls(file=file_name, message=str(exc))

    def to_rejected_row(self, table_name: str) -> dict[str, str | int | None]:
        """Format this error record for the ``_rejected`` table.

        :param table_name: Target table name.
        :type table_name: str
        :return: Rejected row dictionary.
        :rtype: dict[str, str | int | None]
        """
        return make_rejected_row(table_name, self.message, source_file=self.file)


class CleanerValidatorArgs(BaseModel):
    """Parameters for running the XSV file clean and validate workflow.

    :param qsv_cmd: Command for executing ``qsv``.
    :type qsv_cmd: NonEmptyStr
    :param xsv_file_path: Path to the data file to validate.
    :type xsv_file_path: FilePath
    :param header_file_path: Path to the generated header file.
    :type header_file_path: FilePath
    :param first_pass_schema: Schema for initial column-count validation.
    :type first_pass_schema: ValidatedXsvSchema
    :param post_norm_schema: Schema for validating data after cleaning and normalisation.
    :type post_norm_schema: ValidatedXsvSchema
    :param tmp_dir_path: Temporary working directory.
    :type tmp_dir_path: DirectoryPath
    :param qsv_output_dir_path: Directory for qsv output from failed executions.
    :type qsv_output_dir_path: DirectoryPath
    :param validated_file_dir_path: Directory for cleaned and validated files.
    :type validated_file_dir_path: DirectoryPath
    :param errors: Errors accumulated during processing.
    :type errors: list[ErrorRecord]
    :param delimiter: Delimiter used in the XSV file (defaults to tab).
    :type delimiter: CharStr
    :param comment_char: Comment character used in the XSV file (defaults to ``#``).
    :type comment_char: CharStr
    :param quote: Quote character used in the XSV file; if unset, ``qsv`` assumes ``"``.
    :type quote: CharStr | None
    :param escape: Escape character used in the XSV file; if unset, ``qsv`` escapes quotes by doubling.
    :type escape: NonEmptyStr | None
    :param null_regex: Regex pattern to replace with an empty string.
    :type null_regex: NonEmptyStr | None
    :param null_regex_cols: Columns to apply ``null_regex`` to (``None`` applies to all columns).
    :type null_regex_cols: list[NonEmptyStr] | None
    :param missing_header: Whether the XSV file lacks a header row.
    :type missing_header: bool
    :param source_id: Identifier recorded for the input file in error records.
    :type source_id: NonEmptyStr | None
    """

    qsv_cmd: NonEmptyStr

    xsv_file_path: FilePath
    header_file_path: FilePath

    first_pass_schema: ValidatedXsvSchema
    post_norm_schema: ValidatedXsvSchema

    tmp_dir_path: DirectoryPath
    qsv_output_dir_path: DirectoryPath
    validated_file_dir_path: DirectoryPath

    errors: list[ErrorRecord] = Field(default_factory=list)

    delimiter: CharStr = "\t"
    comment_char: CharStr = "#"
    quote: CharStr | None = None
    escape: NonEmptyStr | None = None
    null_regex: NonEmptyStr | None = None
    null_regex_cols: list[NonEmptyStr] | None = None
    missing_header: bool = False
    source_id: NonEmptyStr | None = None

    @computed_field
    @cached_property
    def qsv_env(self: Self) -> dict[str, str]:
        """Environment dictionary passed to ``qsv`` subprocesses.

        :return: Environment mapping with ``QSV_COMMENT_CHAR`` set.
        :rtype: dict[str, str]
        """
        return {**os.environ, "QSV_COMMENT_CHAR": self.comment_char}

    @computed_field
    @property
    def xsv_file_base_name(self: Self) -> str:
        """Base name of the input XSV file without its final suffix.

        :return: File stem.
        :rtype: str
        """
        return self.xsv_file_path.stem

    @computed_field
    @property
    def file_name(self) -> str:
        """Identifier of the input XSV file used in error records.

        :return: ``source_id`` if set, else ``xsv_file_path.name``.
        :rtype: str
        """
        return self.source_id or self.xsv_file_path.name

    @computed_field
    @property
    def ext(self) -> str:
        """File extension for XSV intermediate files (including leading ``.``).

        :return: File extension string.
        :rtype: str
        """
        return SEP_TO_EXT.get(self.delimiter) or self.xsv_file_path.suffix

    @computed_field
    @property
    def valid_file_suffix(self) -> str:
        """Suffix added to validator output files containing valid lines.

        :return: Valid lines file suffix.
        :rtype: str
        """
        return f"valid{self.ext}"

    @computed_field
    @property
    def invalid_file_suffix(self) -> str:
        """Suffix added to validator output files containing invalid lines.

        :return: Invalid lines file suffix.
        :rtype: str
        """
        return f"invalid{self.ext}"


def _transfer_safely(
    op: Callable[[Path, Path], Any],
    src: Path,
    dst: Path,
    args: CleanerValidatorArgs,
) -> bool:
    """Execute a file transfer operation (``copy`` or ``move``), recording an :class:`ErrorRecord` on ``OSError``.

    :param op: File operation callable (``shutil.copy`` or ``shutil.move``).
    :type op: Callable[[Path, Path], Any]
    :param src: Source file path.
    :type src: Path
    :param dst: Destination file path.
    :type dst: Path
    :param args: Cleaner/validator arguments holding ``errors`` and ``file_name``.
    :type args: CleanerValidatorArgs
    :return: ``True`` if the operation succeeded, ``False`` on ``OSError``.
    :rtype: bool
    """
    try:
        op(src, dst)
    except OSError as exc:
        args.errors.append(ErrorRecord.from_exception(args.file_name, exc))
        return False
    return True


def copy_safely(src: Path, dst: Path, args: CleanerValidatorArgs) -> bool:
    """Copy ``src`` to ``dst``, recording an :class:`ErrorRecord` and returning ``False`` on failure.

    :param src: Source path.
    :type src: Path
    :param dst: Destination path.
    :type dst: Path
    :param args: Arguments for the cleaner/validator.
    :type args: CleanerValidatorArgs
    :return: ``True`` if the copy succeeded, ``False`` otherwise.
    :rtype: bool
    """
    return _transfer_safely(copy, src, dst, args)


def move_safely(src: Path, dst: Path, args: CleanerValidatorArgs) -> bool:
    """Move ``src`` to ``dst``, recording an :class:`ErrorRecord` and returning ``False`` on failure.

    :param src: Source path.
    :type src: Path
    :param dst: Destination path.
    :type dst: Path
    :param args: Arguments for the cleaner/validator.
    :type args: CleanerValidatorArgs
    :return: ``True`` if the move succeeded, ``False`` otherwise.
    :rtype: bool
    """
    return _transfer_safely(move, src, dst, args)


def non_header_lines_present(path: Path) -> bool:
    """Return ``True`` if ``path`` has any non-blank lines beyond the header line.

    Stops reading once a second non-blank line is found.

    :param path: Path to the XSV file.
    :type path: Path
    :return: ``True`` when at least two non-blank lines exist.
    :rtype: bool
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
    """Write ``dest`` with ``header_file_path`` bytes followed by ``xsv_file_path`` bytes.

    :param header_file_path: Path to the header line file.
    :type header_file_path: Path
    :param xsv_file_path: Path to the headerless data file.
    :type xsv_file_path: Path
    :param dest: Destination combined file path.
    :type dest: Path
    """
    with dest.open("wb") as out:
        out.write(header_file_path.read_bytes())
        out.write(xsv_file_path.read_bytes())


@dataclass(slots=True)
class FileNames:
    """Derived file paths produced during a ``qsv validate`` pass."""

    valid_output: Path
    errors: Path
    valid_lines: Path
    invalid_lines: Path
    schema: Path


def generate_qsv_validate_file_names(
    args: CleanerValidatorArgs,
    input_file_name: str,
    *,
    first_pass: bool = True,
) -> FileNames:
    """Compute the file paths that :func:`run_qsv_validate` writes for ``input_file_name``.

    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param input_file_name: Input file name in ``args.tmp_dir_path``.
    :type input_file_name: str
    :param first_pass: Whether this is the first validation pass (defaults to ``True``).
    :type first_pass: bool
    :return: Dataclass containing derived output, error, valid/invalid split, and schema paths.
    :rtype: FileNames
    """
    output_file_stem = (
        f"{args.xsv_file_base_name}-first-pass" if first_pass else f"{args.xsv_file_base_name}-normalised-validated"
    )

    return FileNames(
        valid_output=args.tmp_dir_path / f"{output_file_stem}{VALID_SUFFIX}{args.ext}",
        errors=args.tmp_dir_path / f"{input_file_name}{VALIDATION_ERRORS}",
        valid_lines=args.tmp_dir_path / f"{input_file_name}.{args.valid_file_suffix}",
        invalid_lines=args.tmp_dir_path / f"{input_file_name}.{args.invalid_file_suffix}",
        schema=args.tmp_dir_path / f"{output_file_stem}.jsonschema.json",
    )
