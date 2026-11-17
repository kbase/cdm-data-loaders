"""Shared constants and helper functions for XSV reader and validator tests."""

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final

from cdm_data_loaders.readers.xsv.models import (
    CLEANED_SUFFIX,
    HEADER_SUFFIX,
    SEP_TO_EXT,
    CleanerValidatorArgs,
)
from cdm_data_loaders.validation.jsonschema.models import (
    ValidatedXsvSchema,
    generate_header,
)
from tests.helpers.fixtures_xsv import (
    COLUMNS,
    VALID_ROWS,
    Row,
    WriteFile,
    build_xsv_content,
)
from tests.helpers.paths_and_constants import TEST_DATA_DIR

FAKE_QSV_CMD: Final[str] = "mock-qsv"
SCHEMAS_DIR: Final[Path] = TEST_DATA_DIR / "qsv" / "schemas"
DELIMITERS: Final[list[str]] = list(SEP_TO_EXT)


def make_args_factory(
    qsv_cmd: str,
    tmp_dir_path: Path,
    output_dir_path_derivatives: dict[str, Path],
    first_pass_schema: ValidatedXsvSchema,
    post_norm_schema: ValidatedXsvSchema,
) -> Callable[..., CleanerValidatorArgs]:
    """Build a ``CleanerValidatorArgs`` factory closed over a fixed ``qsv_cmd``.

    :param qsv_cmd: Path or command name for ``qsv``.
    :type qsv_cmd: str
    :param tmp_dir_path: Working directory for intermediate validator files.
    :type tmp_dir_path: Path
    :param output_dir_path_derivatives: Mapping containing ``qsv_output_dir_path`` and ``validated_file_dir_path``.
    :type output_dir_path_derivatives: dict[str, Path]
    :param first_pass_schema: Default first-pass schema.
    :type first_pass_schema: ValidatedXsvSchema
    :param post_norm_schema: Default post-normalization schema.
    :type post_norm_schema: ValidatedXsvSchema
    :return: Factory constructing ``CleanerValidatorArgs`` instances.
    :rtype: Callable[..., CleanerValidatorArgs]
    """

    def _make_args(
        xsv_file_path: Path,
        delimiter: str = "\t",
        comment_char: str = "#",
        null_regex: str | None = None,
        missing_header: bool = False,
        first_pass_schema_override: ValidatedXsvSchema | None = None,
        post_norm_schema_override: ValidatedXsvSchema | None = None,
    ) -> CleanerValidatorArgs:
        header_file_path = generate_header(
            first_pass_schema_override or first_pass_schema,
            tmp_dir_path,
            header_file_name=f"header-{ord(delimiter)}.txt",
            delimiter=delimiter,
        )
        return CleanerValidatorArgs(
            errors=[],
            qsv_cmd=qsv_cmd,
            xsv_file_path=xsv_file_path,
            header_file_path=header_file_path,
            first_pass_schema=first_pass_schema_override or first_pass_schema,
            post_norm_schema=post_norm_schema_override or post_norm_schema,
            tmp_dir_path=tmp_dir_path,
            delimiter=delimiter,
            comment_char=comment_char,
            null_regex=null_regex,
            missing_header=missing_header,
            qsv_output_dir_path=output_dir_path_derivatives["qsv_output_dir_path"],
            validated_file_dir_path=output_dir_path_derivatives["validated_file_dir_path"],
        )

    return _make_args


def touch(path: Path, content: str = "hello") -> Path:
    """Write ``content`` to ``path`` and return ``path``.

    :param path: Target file path.
    :type path: Path
    :param content: UTF-8 text to write.
    :type content: str
    :return: Written file path.
    :rtype: Path
    """
    path.write_text(content, encoding="utf-8")
    return path


def interleave_comments(content: str, comment_char: str) -> str:
    """Insert a comment line before every line in ``content``.

    :param content: Multiline XSV string.
    :type content: str
    :param comment_char: Leading comment character.
    :type comment_char: str
    :return: Content with interleaved comment lines.
    :rtype: str
    """
    lines = content.splitlines()
    commented: list[str] = []
    for idx, line in enumerate(lines):
        commented.extend((f"{comment_char} comment line {idx}", line))
    comment_lines = [line for line in commented if line.startswith(f"{comment_char} comment line")]
    assert len(comment_lines) >= 1
    return "\n".join(commented) + "\n"


def write_validator_input_file(
    args: CleanerValidatorArgs,
    write_working_file: WriteFile,
    rows: Sequence[Row] = VALID_ROWS,
    suffix: str = HEADER_SUFFIX,
    delimiter: str | None = None,
) -> str:
    """Write a working file with the given stage suffix inside ``args.tmp_dir_path``.

    :param args: Validator arguments.
    :type args: CleanerValidatorArgs
    :param write_working_file: Writer fixture bound to ``args.tmp_dir_path``.
    :type write_working_file: WriteFile
    :param rows: Data rows to serialize.
    :type rows: Sequence[Row]
    :param suffix: Filename suffix before the extension.
    :type suffix: str
    :param delimiter: Optional delimiter override.
    :type delimiter: str | None
    :return: Relative filename written inside ``args.tmp_dir_path``.
    :rtype: str
    """
    input_file_name = f"{args.xsv_file_base_name}{suffix}{args.ext}"
    write_working_file(
        build_xsv_content(rows, header=COLUMNS, delimiter=delimiter or args.delimiter),
        input_file_name,
    )
    return input_file_name


def write_header_input_file(
    args: CleanerValidatorArgs,
    write_working_file: WriteFile,
    rows: Sequence[Row] = VALID_ROWS,
    delimiter: str | None = None,
) -> str:
    """Write a ``-header``-suffixed working file for ``args`` and return its filename.

    :param args: Validator arguments.
    :type args: CleanerValidatorArgs
    :param write_working_file: Writer fixture bound to ``args.tmp_dir_path``.
    :type write_working_file: WriteFile
    :param rows: Data rows to serialize.
    :type rows: Sequence[Row]
    :param delimiter: Optional delimiter override.
    :type delimiter: str | None
    :return: Relative filename written inside ``args.tmp_dir_path``.
    :rtype: str
    """
    return write_validator_input_file(args, write_working_file, rows, HEADER_SUFFIX, delimiter=delimiter)


def write_cleaned_input_file(
    args: CleanerValidatorArgs,
    write_working_file: WriteFile,
    rows: Sequence[Row],
    delimiter: str | None = None,
) -> str:
    """Write a ``-cleaned``-suffixed working file for ``args`` and return its filename.

    :param args: Validator arguments.
    :type args: CleanerValidatorArgs
    :param write_working_file: Writer fixture bound to ``args.tmp_dir_path``.
    :type write_working_file: WriteFile
    :param rows: Data rows to serialize.
    :type rows: Sequence[Row]
    :param delimiter: Optional delimiter override.
    :type delimiter: str | None
    :return: Relative filename written inside ``args.tmp_dir_path``.
    :rtype: str
    """
    return write_validator_input_file(args, write_working_file, rows, CLEANED_SUFFIX, delimiter=delimiter)


def snapshot_dir(dir_path: Path) -> set[str]:
    """Return the set of entry names present in ``dir_path``.

    :param dir_path: Directory to inspect.
    :type dir_path: Path
    :return: Set of entry names.
    :rtype: set[str]
    """
    return {p.name for p in dir_path.iterdir()}


def parse_xsv(content: str, delimiter: str = "\t") -> list[list[str]]:
    """Split simple XSV content into rows of fields.

    :param content: Raw XSV text.
    :type content: str
    :param delimiter: Field delimiter.
    :type delimiter: str
    :return: Parsed rows of string fields.
    :rtype: list[list[str]]
    """
    return [line.split(delimiter) for line in content.splitlines() if line]
