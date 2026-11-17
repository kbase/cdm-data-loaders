"""Subprocess wrapper around ``qsv`` for two-stage XSV cleaning and JSON Schema validation."""

import json
import os
import shutil
import subprocess
from pathlib import Path

from pydantic import validate_call

from cdm_data_loaders.readers.xsv.models import (
    CLEANED_SUFFIX,
    HEADER_SUFFIX,
    NORM_SUFFIX,
    CleanerValidatorArgs,
    ErrorRecord,
    copy_safely,
    generate_qsv_validate_file_names,
    move_safely,
    non_header_lines_present,
    prepend_header,
)
from cdm_data_loaders.validation.jsonschema.dereference import dereference_schema
from cdm_data_loaders.validation.jsonschema.metaschema import DRAFT_2020_12_SCHEMA_URI
from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema


def qsv_check() -> str:
    """Locate the ``qsv`` binary and check ``qsv --version``.

    :return: Resolved path to the ``qsv`` executable.
    :rtype: str
    :raises RuntimeError: If ``qsv`` cannot be located or exits with a non-zero status.
    """
    qsv_cmd = shutil.which("qsv") or os.environ.get("QSV_BIN")
    if qsv_cmd:
        err_msg = None
        try:
            result = subprocess.run([qsv_cmd, "--version"], text=True, stderr=subprocess.PIPE, check=True)
            if result.returncode == 0:
                return qsv_cmd
            err_msg = f"`qsv --version` exited with code {result.returncode}"
            if result.stderr:
                err_msg += f"; STDERR: {result.stderr.strip()}"
        except Exception as e:
            err_msg = f"Cannot perform validation with qsv: {e!s}"
            raise RuntimeError(err_msg) from e
    else:
        err_msg = "Could not locate the qsv binary"
    raise RuntimeError(err_msg)


def _stage_header_file(args: CleanerValidatorArgs, file_with_headers: str) -> bool:
    """Write or copy the input file with headers into ``args.tmp_dir_path``.

    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param file_with_headers: Target filename inside ``args.tmp_dir_path``.
    :type file_with_headers: str
    :return: ``True`` if staging succeeded, ``False`` on ``OSError``.
    :rtype: bool
    """
    dest = args.tmp_dir_path / file_with_headers
    if args.missing_header:
        try:
            prepend_header(args.header_file_path, args.xsv_file_path, dest)
        except OSError as exc:
            args.errors.append(ErrorRecord.from_exception(args.file_name, exc))
            return False
        return True
    return copy_safely(args.xsv_file_path, dest, args)


@validate_call
def clean_validate_file(args: CleanerValidatorArgs) -> str | None:
    """Clean and validate an XSV file in two ``qsv`` passes.

    Stages headers if ``missing_header`` is ``True``, removes ragged rows in a first-pass validation, normalizes the
    surviving rows with ``qsv input``, applies ``qsv replace`` when ``null_regex`` is configured, and runs a strict
    post-normalization ``qsv validate`` pass.

    :param args: Parameters for running the clean and validate workflow.
    :type args: CleanerValidatorArgs
    :return: Validated filename inside ``args.validated_file_dir_path``, or ``None`` if no valid rows survive.
    :rtype: str | None
    """
    file_with_headers = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"
    if not _stage_header_file(args, file_with_headers):
        return None

    file_for_input_cmd = run_qsv_validate(args, file_with_headers, schema=args.first_pass_schema, first_pass=True)
    if not file_for_input_cmd:
        return None

    file_for_rplc_cmd = run_qsv_input(args, file_for_input_cmd)
    if not file_for_rplc_cmd:
        return None

    file_for_validate_cmd = run_qsv_null_replacement(args, file_for_rplc_cmd) if args.null_regex else file_for_rplc_cmd
    if not file_for_validate_cmd:
        return None

    validated_file = run_qsv_validate(args, file_for_validate_cmd, args.post_norm_schema, first_pass=False)
    if validated_file and move_safely(
        args.tmp_dir_path / validated_file,
        args.validated_file_dir_path / validated_file,
        args,
    ):
        return validated_file
    return None


def _run_qsv_step(cmd: list[str], args: CleanerValidatorArgs) -> subprocess.CompletedProcess[str] | None:
    """Run a ``qsv`` subcommand with ``args.qsv_env``, recording raised exceptions in ``args.errors``.

    :param cmd: ``qsv`` command and arguments to execute.
    :type cmd: list[str]
    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :return: Completed process, or ``None`` if execution raised an exception.
    :rtype: subprocess.CompletedProcess[str] | None
    """
    try:
        return subprocess.run(cmd, check=False, text=True, stderr=subprocess.PIPE, env=args.qsv_env)
    except Exception as exc:  # noqa: BLE001
        args.errors.append(ErrorRecord.from_exception(args.file_name, exc))
    return None


def _require_input_file(args: CleanerValidatorArgs, input_file_name: str) -> Path | None:
    """Return ``args.tmp_dir_path / input_file_name`` if present, else record ``FileNotFoundError``.

    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param input_file_name: Expected file name inside ``args.tmp_dir_path``.
    :type input_file_name: str
    :return: Resolved input path or ``None`` when missing.
    :rtype: Path | None
    """
    input_path = args.tmp_dir_path / input_file_name
    if not input_path.is_file():
        err_msg = f"Input file not found at {input_path!s}"
        args.errors.append(ErrorRecord.from_exception(args.file_name, FileNotFoundError(err_msg)))
        return None
    return input_path


def _run_qsv_transform_step(
    cmd: list[str],
    output_file_name: str,
    args: CleanerValidatorArgs,
    *,
    failed_input_to_copy: Path | None = None,
) -> str | None:
    """Execute a ``qsv`` transformation command and return ``output_file_name`` on exit code ``0``.

    :param cmd: ``qsv`` command and arguments.
    :type cmd: list[str]
    :param output_file_name: Expected output file name in ``args.tmp_dir_path``.
    :type output_file_name: str
    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param failed_input_to_copy: Optional input path copied to ``args.qsv_output_dir_path`` on failure.
    :type failed_input_to_copy: Path | None
    :return: ``output_file_name`` on success, else ``None``.
    :rtype: str | None
    """
    result = _run_qsv_step(cmd, args)
    if result is None:
        return None

    if result.returncode == 0:
        return output_file_name

    args.errors.append(ErrorRecord.from_qsv_result(args.file_name, result))
    if failed_input_to_copy is not None:
        copy_safely(failed_input_to_copy, args.qsv_output_dir_path / failed_input_to_copy.name, args)
    return None


def run_qsv_input(args: CleanerValidatorArgs, input_file_name: str) -> str | None:
    """Run ``qsv input`` to normalize ``input_file_name``.

    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param input_file_name: Name of the input file in ``args.tmp_dir_path``.
    :type input_file_name: str
    :return: Normalized output file name on exit code ``0``, else ``None``.
    :rtype: str | None
    """
    input_path = _require_input_file(args, input_file_name)
    if input_path is None:
        return None

    output_file_name = f"{args.xsv_file_base_name}{CLEANED_SUFFIX}{args.ext}"
    output_path = args.tmp_dir_path / output_file_name

    xsv_file_config = ["--delimiter", args.delimiter]
    for conf in ["quote", "escape"]:
        if value := getattr(args, conf):
            xsv_file_config.extend([f"--{conf}", value])

    cmd = [
        args.qsv_cmd,
        "input",
        "--trim-headers",
        "--trim-fields",
        "--encoding-errors",
        "strict",
        *xsv_file_config,
        "--output",
        str(output_path),
        str(input_path),
    ]
    return _run_qsv_transform_step(cmd, output_file_name, args)


def run_qsv_null_replacement(args: CleanerValidatorArgs, input_file_name: str) -> str | None:
    """Run ``qsv replace`` to replace matches of ``args.null_regex`` in ``input_file_name`` with ``""``.

    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param input_file_name: Name of the input file in ``args.tmp_dir_path``.
    :type input_file_name: str
    :return: Output file name on exit code ``0``, else ``None``.
    :rtype: str | None
    """
    input_path = _require_input_file(args, input_file_name)
    if input_path is None or args.null_regex is None:
        return None

    output_file_name = f"{args.xsv_file_base_name}{NORM_SUFFIX}{args.ext}"
    regex_cols = ["-s", f"{','.join(args.null_regex_cols)}"] if args.null_regex_cols else []

    cmd = [
        args.qsv_cmd,
        "replace",
        "--not-one",
        "--delimiter",
        args.delimiter,
        "--output",
        str(args.tmp_dir_path / output_file_name),
        *regex_cols,
        args.null_regex,
        "",
        str(input_path),
    ]
    return _run_qsv_transform_step(cmd, output_file_name, args, failed_input_to_copy=input_path)


def run_qsv_validate(
    args: CleanerValidatorArgs,
    input_file_name: str,
    schema: ValidatedXsvSchema,
    *,
    first_pass: bool = True,
) -> str | None:
    """Run ``qsv validate`` over ``input_file_name``.

    :param args: Cleaner/validator arguments.
    :type args: CleanerValidatorArgs
    :param input_file_name: Name of the input file in ``args.tmp_dir_path``.
    :type input_file_name: str
    :param schema: Validated XSV JSON Schema to check against.
    :type schema: ValidatedXsvSchema
    :param first_pass: When ``True``, pass ``--no-format-validation``.
    :type first_pass: bool
    :return: Validated output file name when valid rows exist, else ``None``.
    :rtype: str | None
    """
    input_path = _require_input_file(args, input_file_name)
    if input_path is None:
        return None

    files = generate_qsv_validate_file_names(args, input_file_name, first_pass=first_pass)

    # rewrite custom XSV metaschema URI to Draft 2020-12 and dereference second-pass $ref/allOf for qsv CSV typing
    qsv_schema = (
        {**schema.jsonschema, "$schema": DRAFT_2020_12_SCHEMA_URI} if schema.has_xsv_metaschema else schema.jsonschema
    )
    if not first_pass:
        qsv_schema = dereference_schema(qsv_schema)
    files.schema.write_text(json.dumps(qsv_schema))

    cmd = [
        args.qsv_cmd,
        "validate",
        "--delimiter",
        args.delimiter,
        "--no-format-validation" if first_pass else None,
        "--split-ragged",
        "--trim",
        "--valid",
        args.valid_file_suffix,
        "--invalid",
        args.invalid_file_suffix,
        "--valid-output",
        str(files.valid_output),
        str(input_path),
        str(files.schema),
    ]
    result = _run_qsv_step([c for c in cmd if c], args)
    if result is None:
        return None

    if files.valid_output.is_file():
        return files.valid_output.name

    if result.returncode != 0:
        args.errors.append(ErrorRecord.from_qsv_result(args.file_name, result))

    valid_lines_found = files.valid_lines.exists() and non_header_lines_present(files.valid_lines)

    for f in [files.errors, files.invalid_lines]:
        if f.is_file():
            copy_safely(f, args.qsv_output_dir_path / f.name, args)

    if valid_lines_found:
        copy_safely(files.valid_lines, args.qsv_output_dir_path / files.valid_lines.name, args)
        if move_safely(files.valid_lines, files.valid_output, args):
            return files.valid_output.name

    return None
