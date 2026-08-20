"""Tests for the xsv validator helper (non-qsv-interacting) code."""

import json
import os
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Final

import jsonschema
import jsonschema.exceptions
import pytest
from _pytest.mark.structures import ParameterSet
from pydantic import ValidationError

from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.helpers import (
    HEADER_SUFFIX,
    NORM_SUFFIX,
    SEP_TO_EXT,
    VALID_SUFFIX,
    VALIDATION_ERRORS,
    CleanerValidatorArgs,
    ErrorRecord,
    copy_safely,
    generate_first_pass_schema,
    generate_header,
    generate_qsv_validate_file_names,
    move_safely,
    non_header_lines_present,
    prepend_header,
    validate_jsonschema,
)
from tests.readers.jsonschema_xsv.xsv_validator.conftest import (
    COLUMNS,
    DELIMITERS,
    VALID_ROWS,
    WriteFile,
    _touch,
    build_xsv_content,
)

VALID_SCHEMA_URI: Final[str] = "https://json-schema.org/draft/2019-09/schema"
SCHEMA_KEY_VALUE: dict[str, str] = {"$schema": VALID_SCHEMA_URI}

INVALID_TOP_LEVEL_SCHEMA_LIST = [
    pytest.param(["a", "b"], id="schema-is-a-list"),
    pytest.param("just a string", id="schema-is-string"),
    pytest.param(42, id="schema-is-a-number"),
    pytest.param(None, id="schema-is-null"),
    pytest.param(True, id="schema-is-bool"),
]
MISSING_EMPTY_REQUIRED_LIST = [
    pytest.param({}, id="missing-required-key"),
    pytest.param({"required": []}, id="empty-required-list"),
]
INVALID_REQUIRED_LIST = [
    pytest.param({"required": "not-a-list"}, id="required-is-a-string"),
    pytest.param({"required": None}, id="required-is-null"),
    pytest.param({"required": {"a": 1}}, id="required-is-a-dict"),
]

"""copy_safely and move_safely"""


@pytest.mark.parametrize("fn", [copy_safely, move_safely])
def test_copy_move_safely_pass_correct_content_at_destination(
    tmp_path: Path, fake_args: SimpleNamespace, fn: Callable
) -> None:
    """On success, the destination file is created with the source's content."""
    src = _touch(tmp_path / "src.txt", "hello world")
    dst = tmp_path / "dst.txt"

    assert fn(src, dst, fake_args) is True  # pyright: ignore[reportArgumentType]
    assert dst.read_text() == "hello world"
    assert fake_args.errors == []


@pytest.mark.parametrize("fn", [copy_safely, move_safely])
def test_copy_move_safely_pass_overwrites_existing_destination(
    tmp_path: Path, fake_args: SimpleNamespace, fn: Callable
) -> None:
    """An existing destination file is overwritten with the source's content."""
    src = _touch(tmp_path / "src.txt", "new content")
    dst = _touch(tmp_path / "dst.txt", "stale content")

    assert fn(src, dst, fake_args) is True  # pyright: ignore[reportArgumentType]
    assert dst.read_text() == "new content"


def test_copy_safely_pass_leaves_source_file_intact(tmp_path: Path, fake_args: SimpleNamespace) -> None:
    """Unlike move_safely, copy_safely must not remove the source file."""
    src = _touch(tmp_path / "src.txt", "hello")
    dst = tmp_path / "dst.txt"

    assert copy_safely(src, dst, fake_args) is True  # pyright: ignore[reportArgumentType]
    assert src.exists()
    assert src.read_text() == "hello"


def test_move_safely_pass_removes_source_file(tmp_path: Path, fake_args: SimpleNamespace) -> None:
    """Unlike copy_safely, move_safely must remove the source file on success."""
    src = _touch(tmp_path / "src.txt", "hello")
    dst = tmp_path / "dst.txt"

    assert move_safely(src, dst, fake_args) is True  # pyright: ignore[reportArgumentType]
    assert not src.exists()


@pytest.mark.parametrize(
    ("make_src", "make_dst"),
    [
        pytest.param(
            lambda tmp_path: tmp_path / "missing-src.txt",
            lambda tmp_path: tmp_path / "dst.txt",
            id="missing-source-file",
        ),
        pytest.param(
            lambda tmp_path: _touch(tmp_path / "src.txt"),
            lambda tmp_path: tmp_path / "no-such-dir" / "dst.txt",
            id="missing-destination-directory",
        ),
    ],
)
@pytest.mark.parametrize("fn", [copy_safely, move_safely])
def test_copy_safely_fail_records_error_and_returns_false(
    tmp_path: Path,
    fake_args: SimpleNamespace,
    make_src: Callable[[Path], Path],
    make_dst: Callable[[Path], Path],
    fn: Callable,
) -> None:
    """On an OSError, copy_safely and move_safely return False and records a matching ErrorRecord."""
    src = make_src(tmp_path)
    dst = make_dst(tmp_path)
    src_existed = src.exists()
    assert src_existed == (src.name == "src.txt")

    assert fn(src, dst, fake_args) is False  # pyright: ignore[reportArgumentType]
    assert not dst.exists()
    # if the source existed before the failed operation, it must not have been consumed
    assert src.exists() == src_existed

    assert len(fake_args.errors) == 1
    error = fake_args.errors[0]
    assert isinstance(error, ErrorRecord)
    assert error.file == fake_args.file_name
    assert error.returncode is None
    assert "No such file or directory" in error.message


"""non_header_lines_present"""


NON_HEADER_LINES_CASES: list[ParameterSet] = [
    pytest.param("", False, id="empty-file"),
    pytest.param("header\n", False, id="header-only"),
    pytest.param("header", False, id="header-only-no-trailing-newline"),
    pytest.param("header\n\n", False, id="header-plus-trailing-blank-line"),
    pytest.param("\n\n\n", False, id="only-blank-lines"),
    pytest.param(" \n\t\n", False, id="only-whitespace-lines"),
    pytest.param("header\ndata\n", True, id="header-plus-one-data-line"),
    pytest.param("header\ndata1\ndata2\n", True, id="header-plus-multiple-data-lines"),
    pytest.param("header\n\ndata\n", True, id="blank-line-between-header-and-data-is-ignored"),
    pytest.param("header\n \ndata\n", True, id="whitespace-only-line-is-ignored"),
]


@pytest.mark.parametrize(("content", "expected"), NON_HEADER_LINES_CASES)
def test_non_header_lines_present_pass_matches_expected_result(tmp_path: Path, content: str, expected: bool) -> None:
    """Blank/whitespace-only lines are ignored when deciding if non-header lines are present."""
    path = _touch(tmp_path / "file.txt", content)
    assert non_header_lines_present(path) is expected


def test_non_header_lines_present_fail_missing_file(tmp_path: Path) -> None:
    """A missing file raises FileNotFoundError rather than being silently treated as empty."""
    with pytest.raises(FileNotFoundError, match="No such file or directory:"):
        non_header_lines_present(tmp_path / "does-not-exist.txt")


"""prepend_header"""


def test_prepend_header_pass_concatenates_header_and_data(tmp_path: Path) -> None:
    """The destination file contains the header file's contents followed by the data file's contents."""
    header_path = _touch(tmp_path / "header.txt", "col1\tcol2\n")
    data_path = _touch(tmp_path / "data.txt", "1\t2\n3\t4\n")
    dest = tmp_path / "combined.txt"

    prepend_header(header_path, data_path, dest)

    assert dest.read_text() == "col1\tcol2\n1\t2\n3\t4\n"


def test_prepend_header_pass_writes_bytes_exactly(tmp_path: Path) -> None:
    """Content is copied byte-for-byte, so non-UTF-8/binary content is handled correctly."""
    header_path = tmp_path / "header.bin"
    header_path.write_bytes(b"col1,col2\n")
    data_path = tmp_path / "data.bin"
    data_path.write_bytes(b"\x00\x01,\xff\n")
    dest = tmp_path / "combined.bin"

    prepend_header(header_path, data_path, dest)

    assert dest.read_bytes() == b"col1,col2\n\x00\x01,\xff\n"


def test_prepend_header_pass_overwrites_existing_destination(tmp_path: Path) -> None:
    """A pre-existing destination file is fully overwritten, not appended to."""
    header_path = _touch(tmp_path / "header.txt", "h\n")
    data_path = _touch(tmp_path / "data.txt", "d\n")
    dest = _touch(tmp_path / "combined.txt", "stale content that should be gone\n")

    prepend_header(header_path, data_path, dest)

    assert dest.read_text() == "h\nd\n"


def test_prepend_header_fail_missing_header_file(tmp_path: Path) -> None:
    """A missing header file raises FileNotFoundError."""
    header_path = tmp_path / "missing-header.txt"
    data_path = _touch(tmp_path / "data.txt", "d\n")
    dest = tmp_path / "combined.txt"

    with pytest.raises(FileNotFoundError, match="No such file or directory:"):
        prepend_header(header_path, data_path, dest)


def test_prepend_header_fail_missing_data_file(tmp_path: Path) -> None:
    """A missing data file raises FileNotFoundError (no partial output file is left dangling)."""
    header_path = _touch(tmp_path / "header.txt", "h\n")
    data_path = tmp_path / "missing-data.txt"
    dest = tmp_path / "combined.txt"

    with pytest.raises(FileNotFoundError, match="No such file or directory:"):
        prepend_header(header_path, data_path, dest)


"""generate_qsv_validate_file_names"""


def test_generate_qsv_validate_file_names_pass_default_first_pass_uses_first_pass_naming(
    make_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """With no explicit `first_pass` argument, the default (True) naming convention is used."""
    source = write_source_file("content", "data.tsv")
    args = make_args(source)
    input_file_name = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"

    valid_output_file, _errors_file, _valid_lines_file, _invalid_lines_file = generate_qsv_validate_file_names(
        args, input_file_name
    )

    expected_name = f"{args.xsv_file_base_name}-first-pass{VALID_SUFFIX}{args.ext}"
    assert valid_output_file == args.tmp_dir_path / expected_name


def test_generate_qsv_validate_file_names_pass_explicit_first_pass_true_uses_first_pass_naming(
    make_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """Explicitly passing first_pass=True produces the same "-first-pass-valid" naming as the default."""
    source = write_source_file("content", "data.tsv")
    args = make_args(source)
    input_file_name = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"

    valid_output_file, *_ = generate_qsv_validate_file_names(args, input_file_name, first_pass=True)

    expected_name = f"{args.xsv_file_base_name}-first-pass{VALID_SUFFIX}{args.ext}"
    assert valid_output_file == args.tmp_dir_path / expected_name


def test_generate_qsv_validate_file_names_pass_first_pass_false_uses_normalised_validated_naming(
    make_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """Passing first_pass=False switches to the "-normalised-validated-valid" naming."""
    source = write_source_file("content", "data.tsv")
    args = make_args(source)
    input_file_name = f"{args.xsv_file_base_name}{NORM_SUFFIX}{args.ext}"

    valid_output_file, *_ = generate_qsv_validate_file_names(args, input_file_name, first_pass=False)

    expected_name = f"{args.xsv_file_base_name}-normalised-validated{VALID_SUFFIX}{args.ext}"
    assert valid_output_file == args.tmp_dir_path / expected_name


def test_generate_qsv_validate_file_names_pass_valid_output_file_name_is_independent_of_input_file_name(
    make_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """valid_output_file is derived from args.xsv_file_base_name, not input_file_name."""
    source = write_source_file("content", "data.tsv")
    args = make_args(source)

    result_one = generate_qsv_validate_file_names(args, "some_random_input_file.txt", first_pass=True)
    result_two = generate_qsv_validate_file_names(args, "another_random_input_file_name.txt", first_pass=True)

    assert result_one[0] == result_two[0]
    assert "random_input_file" not in str(result_one[0])
    for f in [1, 2, 3]:
        assert "some_random_input_file" in str(result_one[f])
        assert "another_random_input_file" in str(result_two[f])


@pytest.mark.parametrize("first_pass", [True, False])
def test_generate_qsv_validate_file_names_pass_does_not_touch_the_filesystem(
    make_args: Callable[..., CleanerValidatorArgs],
    write_source_file: WriteFile,
    first_pass: bool,
) -> None:
    """None of the files named by generate_qsv_validate_file_names exists."""
    source = write_source_file("content", "data.tsv")
    args = make_args(source)
    input_file_name = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"

    result = generate_qsv_validate_file_names(args, input_file_name, first_pass=first_pass)

    for path in result:
        assert path.parent == args.tmp_dir_path
        assert not path.exists()


@pytest.mark.parametrize("delimiter", DELIMITERS)
@pytest.mark.parametrize("first_pass", [True, False])
def test_generate_qsv_validate_file_names_pass_uses_delimiter_derived_extension(
    make_args: Callable[..., CleanerValidatorArgs],
    write_source_file: WriteFile,
    delimiter: str,
    first_pass: bool,
) -> None:
    """The file extension embedded in every returned path tracks args.ext."""
    source = write_source_file("content", f"data{SEP_TO_EXT[delimiter]}")
    args = make_args(source, delimiter=delimiter)
    input_file_name = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"

    valid_output_file, _errors_file, valid_lines_file, invalid_lines_file = generate_qsv_validate_file_names(
        args, input_file_name, first_pass=first_pass
    )
    assert valid_output_file.suffix == args.ext
    assert valid_lines_file.name.endswith(args.ext)
    assert invalid_lines_file.name.endswith(args.ext)


@pytest.mark.parametrize(
    "input_file_name",
    [
        pytest.param("data-header.tsv", id="simple-name"),
        pytest.param("data.with.dots-header.tsv", id="name-with-embedded-dots"),
        pytest.param("data-normalised.norm.tsv", id="name-resembling-a-previous-suffix"),
    ],
)
def test_generate_qsv_validate_file_names_pass_handles_various_input_file_names(
    make_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile, input_file_name: str
) -> None:
    """input_file_name is used verbatim as a prefix for errors_file/valid_lines_file/invalid_lines_file.

    Embedded dots or substrings that resemble other pipeline suffixes are ignored.
    """
    source = write_source_file("content", "data.tsv")
    args = make_args(source)

    _valid_output_file, errors_file, valid_lines_file, invalid_lines_file = generate_qsv_validate_file_names(
        args, input_file_name
    )

    # straight concatenation
    assert errors_file.name == f"{input_file_name}{VALIDATION_ERRORS}"
    # a dot is added before the suffix
    assert valid_lines_file.name == f"{input_file_name}.{args.valid_file_suffix}"
    assert invalid_lines_file.name == f"{input_file_name}.{args.invalid_file_suffix}"


def test_generate_qsv_validate_file_names_pass_returns_four_distinct_paths(
    make_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """Sanity check that the four returned paths never collide with one another for typical inputs."""
    source = write_source_file("content", "data.tsv")
    args = make_args(source)
    input_file_name = f"{args.xsv_file_base_name}{HEADER_SUFFIX}{args.ext}"

    result = generate_qsv_validate_file_names(args, input_file_name)

    assert len(set(result)) == len(result)


"""ErrorRecord"""


def test_error_record_from_qsv_result_captures_file_message_and_returncode() -> None:
    """from_qsv_result pulls the file name, stripped stderr, and returncode straight from the completed process."""
    result = subprocess.CompletedProcess(args=["qsv", "validate"], returncode=1, stdout=None, stderr="boom\n")

    record = ErrorRecord.from_qsv_result("data.tsv", result)

    assert record.file == "data.tsv"
    assert record.message == "boom"
    assert record.returncode == 1


def test_error_record_from_qsv_result_strips_surrounding_whitespace_from_stderr() -> None:
    """Leading/trailing whitespace (including embedded newlines) around stderr is stripped."""
    result = subprocess.CompletedProcess(args=["qsv"], returncode=2, stdout=None, stderr="\n\n  multi-line error  \n\n")

    record = ErrorRecord.from_qsv_result("data.tsv", result)

    assert record.message == "multi-line error"


def test_error_record_from_qsv_result_handles_none_stderr_as_empty_message() -> None:
    """A None stderr (e.g. if it wasn't captured) results in an empty string message rather than raising."""
    result = subprocess.CompletedProcess(args=["qsv"], returncode=0, stdout=None, stderr=None)

    record = ErrorRecord.from_qsv_result("data.tsv", result)

    assert record.message == ""
    assert record.returncode == 0


def test_error_record_from_exception_captures_file_and_stringified_message() -> None:
    """from_exception stores the exception's str() as the message, leaving returncode unset (None)."""
    exc = OSError("No such file or directory: 'missing.tsv'")

    record = ErrorRecord.from_exception("data.tsv", exc)

    assert record.file == "data.tsv"
    assert record.message == "No such file or directory: 'missing.tsv'"
    assert record.returncode is None


def test_error_record_from_exception_works_with_arbitrary_exception_types() -> None:
    """from_exception isn't limited to OSError; any Exception subclass is supported."""
    exc = ValueError("some validation problem")

    record = ErrorRecord.from_exception("data.tsv", exc)

    assert record.message == "some validation problem"
    assert record.returncode is None


"""CleanerValidatorArgs -- defaults"""


def test_cleaner_validator_args_defaults_are_applied_when_omitted(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """Optional fields fall back to their documented defaults when not explicitly supplied."""
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS), "data.tsv")
    args = make_mock_args(source)

    assert args.delimiter == "\t"
    assert args.comment_char == "#"
    assert args.quote is None
    assert args.escape is None
    assert args.null_regex is None
    assert args.null_regex_cols is None
    assert args.missing_header is False
    assert args.errors == []


def test_cleaner_validator_args_errors_default_factory_is_not_shared_between_instances(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """Each instance gets its own independent `errors` list; mutating one must not affect another.

    Pins down that the (unusually-shaped) `default_factory=lambda _: []` produces a fresh list per
    instance rather than a single shared mutable default.
    """
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS), "data.tsv")
    first = make_mock_args(source)
    second = make_mock_args(source)

    first.errors.append(ErrorRecord(file="a.tsv", message="oops"))

    assert first.errors != second.errors
    assert second.errors == []


"""CleanerValidatorArgs -- computed fields"""


def test_cleaner_validator_args_qsv_env_includes_comment_char_and_inherits_os_environ(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """qsv_env merges the process environment with QSV_COMMENT_CHAR set to comment_char."""
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS), "data.tsv")
    args = make_mock_args(source, comment_char=";")

    env = args.qsv_env

    assert env["QSV_COMMENT_CHAR"] == ";"
    # every pre-existing environment variable is inherited too
    for key, value in os.environ.items():
        assert env[key] == value


def test_cleaner_validator_args_qsv_env_is_cached_after_first_access(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """qsv_env is a cached_property: mutating comment_char after first access does not change the cached value."""
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS), "data.tsv")
    args = make_mock_args(source, comment_char="#")

    first_env = args.qsv_env
    assert first_env["QSV_COMMENT_CHAR"] == "#"

    args.comment_char = ";"

    assert args.qsv_env is first_env
    assert args.qsv_env["QSV_COMMENT_CHAR"] == "#"


def test_cleaner_validator_args_xsv_file_base_name_and_file_name(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """xsv_file_base_name is the file stem; file_name is the full file name including extension."""
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS), "some-data.tsv")
    args = make_mock_args(source)

    assert args.xsv_file_base_name == "some-data"
    assert args.file_name == "some-data.tsv"


@pytest.mark.parametrize("delimiter", DELIMITERS)
def test_cleaner_validator_args_ext_uses_sep_to_ext_mapping_regardless_of_actual_file_suffix(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile, delimiter: str
) -> None:
    """For any delimiter known to SEP_TO_EXT, `ext` reflects the mapped extension.

    This is true even when the source file's suffix (`.weird`) is not the correct extension for the format.
    """
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS, delimiter=delimiter), "data.weird")
    args = make_mock_args(source, delimiter=delimiter)

    assert args.ext == SEP_TO_EXT[delimiter]


def test_cleaner_validator_args_ext_falls_back_to_file_suffix_for_unmapped_delimiter(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """A delimiter not present in SEP_TO_EXT (e.g. a pipe) falls back to the source file's actual suffix."""
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS, delimiter="|"), "data.psv")
    args = make_mock_args(source, delimiter="|")

    assert "|" not in SEP_TO_EXT
    assert args.ext == ".psv"


def test_cleaner_validator_args_valid_and_invalid_file_suffixes_are_derived_from_ext(
    make_mock_args: Callable[..., CleanerValidatorArgs], write_source_file: WriteFile
) -> None:
    """valid_file_suffix/invalid_file_suffix are literally 'valid'/'invalid' with `ext` appended."""
    source = write_source_file(build_xsv_content(VALID_ROWS, header=COLUMNS), "data.tsv")
    args = make_mock_args(source)

    assert args.valid_file_suffix == f"valid{args.ext}"
    assert args.invalid_file_suffix == f"invalid{args.ext}"


"""validate_jsonschema"""


def test_validate_jsonschema_pass_valid_schema(make_schema_file: Callable[..., Path]) -> None:
    """A well-formed schema, complete with $schema keyword, is returned unchanged."""
    schema = {
        "$schema": VALID_SCHEMA_URI,
        "type": "object",
        "required": ["a"],
        "properties": {"a": {"type": "string"}},
    }
    schema_path = make_schema_file(schema)

    result = validate_jsonschema(schema_path)
    assert result == schema


def test_validate_jsonschema_fail_missing_file(tmp_path: Path) -> None:
    """A schema_path that doesn't exist on disk raises FileNotFoundError when read."""
    with pytest.raises(FileNotFoundError):
        validate_jsonschema(tmp_path / "does-not-exist.json")


def test_validate_jsonschema_fail_invalid_json_content(tmp_path: Path) -> None:
    """A file that isn't valid JSON raises a JSONDecodeError."""
    bad_json_path = tmp_path / "not-json.json"
    bad_json_path.write_text("{this is not: valid json,,,")

    with pytest.raises(json.JSONDecodeError):
        validate_jsonschema(bad_json_path)


@pytest.mark.parametrize("schema", INVALID_TOP_LEVEL_SCHEMA_LIST)
def test_validate_jsonschema_fail_non_dict_schema_or_invalid_schema_raises_error(
    make_schema_file: Callable[..., Path], schema: list | str | int | None
) -> None:
    """A schema whose top-level JSON value isn't a dict raises TypeError."""
    schema_path = make_schema_file(schema)

    with pytest.raises(TypeError, match="JSON Schema must be a dictionary"):
        validate_jsonschema(schema_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST + MISSING_EMPTY_REQUIRED_LIST)
def test_validate_jsonschema_fail_no_schema_keyword_missing_or_empty_required_raises_value_error(
    make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema with no `$schema` keyword raises a ValueError."""
    schema_path = make_schema_file(schema)

    with pytest.raises(ValueError, match=r"JSON Schema is missing the \$schema keyword"):
        validate_jsonschema(schema_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST)
def test_validate_jsonschema_fail_invalid_required_format_error(
    make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field isn't a list of strings raises SchemaError."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})

    with pytest.raises(jsonschema.exceptions.SchemaError, match="is not of type 'array'"):
        validate_jsonschema(schema_path)


@pytest.mark.parametrize("schema", MISSING_EMPTY_REQUIRED_LIST)
def test_validate_jsonschema_pass_missing_empty_required_list_is_fine_fine_fine(
    make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field is missing or empty validates just fine."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})
    assert validate_jsonschema(schema_path) == {**SCHEMA_KEY_VALUE, **schema}


def test_validate_jsonschema_pass_validator_dgaf_about_unresolvable_schema_uri(
    make_schema_file: Callable[..., Path],
) -> None:
    """A `$schema` value that doesn't correspond to a known JSON Schema draft does not throw an error.

    The jsonschema module uses the most recent draft to validate against if the metaschema is invalid or absent.
    """
    schema = {"$schema": "https://not-a-real-schema.uri", "required": ["a"]}
    schema_path = make_schema_file(schema)
    assert validate_jsonschema(schema_path) == schema


def test_validate_jsonschema_pass_validator_does_gaf_about_invalid_schema_uri(
    make_schema_file: Callable[..., Path],
) -> None:
    """A `$schema` value that doesn't correspond to a known JSON Schema draft does not throw an error.

    The jsonschema module uses the most recent draft to validate against if the metaschema is invalid or absent.
    """
    schema = {"$schema": "not-a-real-schema-uri", "required": ["a"]}
    schema_path = make_schema_file(schema)
    with pytest.raises(jsonschema.exceptions.SchemaError, match="'not-a-real-schema-uri' is not a 'uri'"):
        validate_jsonschema(schema_path)


def test_validate_jsonschema_fail_invalid_schema_content_logs_and_raises_schema_error(
    make_schema_file: Callable[..., Path], caplog: pytest.LogCaptureFixture
) -> None:
    """A schema with other invalid fields raises a SchemaError."""
    schema = {"$schema": VALID_SCHEMA_URI, "type": "not-a-real-type", "required": ["a"]}
    schema_path = make_schema_file(schema)

    with pytest.raises(jsonschema.exceptions.SchemaError, match="is not valid under any of the given schemas"):
        validate_jsonschema(schema_path)

    assert len(caplog.records) == 1
    assert caplog.records[0].message.startswith("Error validating JSON Schema at ")


"""generate_header"""


def test_generate_header_pass_uses_defaults(tmp_path: Path, make_schema_file: Callable[..., Path]) -> None:
    """With no overrides, the header is written to `header.txt` using a tab delimiter."""
    schema_path = make_schema_file({"$schema": VALID_SCHEMA_URI, "required": COLUMNS})

    result = generate_header(schema_path, tmp_path)

    assert result == tmp_path / "header.txt"
    assert result.read_text() == "\t".join(COLUMNS) + "\n"


@pytest.mark.parametrize("delimiter", DELIMITERS)
def test_generate_header_pass_with_various_delimiters(
    tmp_path: Path, make_schema_file: Callable[..., Path], delimiter: str
) -> None:
    """Every delimiter supported by SEP_TO_EXT produces a correctly-joined header row."""
    schema_path = make_schema_file({"$schema": VALID_SCHEMA_URI, "required": COLUMNS})

    result = generate_header(schema_path, tmp_path, delimiter=delimiter)

    assert result.read_text() == delimiter.join(COLUMNS) + "\n"


def test_generate_header_pass_with_custom_file_name(tmp_path: Path, make_schema_file: Callable[..., Path]) -> None:
    """A custom header_file_name is respected in both the returned path and the file written."""
    schema_path = make_schema_file({"$schema": VALID_SCHEMA_URI, "required": COLUMNS})

    result = generate_header(schema_path, tmp_path, header_file_name="custom-header.tsv")

    assert result == tmp_path / "custom-header.tsv"
    assert result.read_text() == "\t".join(COLUMNS) + "\n"


def test_generate_header_pass_overwrites_existing_file(tmp_path: Path, make_schema_file: Callable[..., Path]) -> None:
    """An existing header file at the target path is overwritten."""
    schema_path = make_schema_file({"$schema": VALID_SCHEMA_URI, "required": COLUMNS})
    (tmp_path / "header.txt").write_text("stale content\n")

    result = generate_header(schema_path, tmp_path)

    assert result.read_text() == "\t".join(COLUMNS) + "\n"


@pytest.mark.parametrize("schema", INVALID_TOP_LEVEL_SCHEMA_LIST)
def test_generate_header_fail_non_dict_schema_raises_type_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: list | str | int | None
) -> None:
    """A schema whose top-level JSON value isn't a dict raises TypeError."""
    schema_path = make_schema_file(schema)
    with pytest.raises(TypeError, match="JSON Schema must be a dictionary"):
        generate_header(schema_path, tmp_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST + MISSING_EMPTY_REQUIRED_LIST)
def test_generate_header_fail_no_schema_keyword_missing_or_empty_required_raises_value_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema with no `$schema` keyword raises a ValueError."""
    schema_path = make_schema_file(schema)

    with pytest.raises(ValueError, match=r"JSON Schema is missing the \$schema keyword"):
        generate_header(schema_path, tmp_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST)
def test_generate_header_fail_invalid_required_raises_schema_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field isn't a list of strings raises SchemaError."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})

    with pytest.raises(jsonschema.exceptions.SchemaError, match="is not of type 'array'"):
        generate_header(schema_path, tmp_path)


@pytest.mark.parametrize("schema", MISSING_EMPTY_REQUIRED_LIST)
def test_generate_header_fail_missing_or_empty_required_raises_value_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field isn't a list of strings raises SchemaError."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})

    with pytest.raises(ValueError, match="Could not find any required cols in"):
        generate_header(schema_path, tmp_path)


def test_generate_header_fail_missing_schema_file(tmp_path: Path) -> None:
    """A schema_path that doesn't point to an existing file fails FilePath validation."""
    with pytest.raises(ValidationError, match="Path does not point to a file"):
        generate_header(tmp_path / "does-not-exist.json", tmp_path)


def test_generate_header_fail_missing_target_dir(tmp_path: Path, make_schema_file: Callable[..., Path]) -> None:
    """A target_dir that doesn't exist fails DirectoryPath validation."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, "required": COLUMNS})
    missing_dir = tmp_path / "no-such-dir"

    with pytest.raises(ValidationError, match="Path does not point to a directory"):
        generate_header(schema_path, missing_dir)


def test_generate_header_fail_empty_header_file_name(tmp_path: Path, make_schema_file: Callable[..., Path]) -> None:
    """An empty header_file_name fails the NonEmptyStr constraint."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, "required": COLUMNS})

    with pytest.raises(ValidationError, match="String should have at least 1 character"):
        generate_header(schema_path, tmp_path, header_file_name="")


@pytest.mark.parametrize(
    "delimiter",
    [
        pytest.param("", id="empty-delimiter"),
        pytest.param(",;", id="multi-character-delimiter"),
    ],
)
def test_generate_header_fail_invalid_delimiter_length(
    tmp_path: Path, make_schema_file: Callable[..., Path], delimiter: str
) -> None:
    """A delimiter that isn't exactly one character fails the CharStr constraint."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, "required": COLUMNS})
    err_msg = re.compile("String should have at (mo|lea)st 1 character")
    with pytest.raises(ValidationError, match=err_msg):
        generate_header(schema_path, tmp_path, delimiter=delimiter)


"""generate_first_pass_schema"""


def test_generate_first_pass_schema_pass_happy_path(
    output_dir_path: Path, post_norm_schema: Path, derived_first_pass_schema: dict[str, Any]
) -> None:
    """A first pass schema can be generated from an existing schema file."""
    output = generate_first_pass_schema(post_norm_schema, output_dir_path)
    assert json.loads(output.read_bytes()) == derived_first_pass_schema


def test_generate_first_pass_schema_pass_overwrites_existing_file(
    output_dir_path: Path, post_norm_schema: Path, derived_first_pass_schema: dict[str, Any]
) -> None:
    """An existing schema file at the target path is overwritten."""
    file_contents = b"some content"
    existing_file = output_dir_path / f"{post_norm_schema.stem}.first-pass.json"
    existing_file.write_bytes(file_contents)
    assert existing_file.exists()
    assert existing_file.read_bytes() == file_contents

    output = generate_first_pass_schema(post_norm_schema, output_dir_path)
    assert json.loads(output.read_bytes()) == derived_first_pass_schema


@pytest.mark.parametrize("schema", INVALID_TOP_LEVEL_SCHEMA_LIST)
def test_generate_first_pass_schema_fail_non_dict_schema_raises_type_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: list | str | int | None
) -> None:
    """A schema whose top-level JSON value isn't a dict raises TypeError."""
    schema_path = make_schema_file(schema)
    with pytest.raises(TypeError, match="JSON Schema must be a dictionary"):
        generate_first_pass_schema(schema_path, tmp_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST + MISSING_EMPTY_REQUIRED_LIST)
def test_generate_first_pass_schema_fail_no_schema_keyword_missing_or_empty_required_raises_value_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema with no `$schema` keyword raises a ValueError."""
    schema_path = make_schema_file(schema)

    with pytest.raises(ValueError, match=r"JSON Schema is missing the \$schema keyword"):
        generate_first_pass_schema(schema_path, tmp_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST)
def test_generate_first_pass_schema_fail_invalid_required_raises_schema_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field isn't a list of strings raises SchemaError."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})

    with pytest.raises(jsonschema.exceptions.SchemaError, match="is not of type 'array'"):
        generate_first_pass_schema(schema_path, tmp_path)


@pytest.mark.parametrize("schema", MISSING_EMPTY_REQUIRED_LIST)
def test_generate_first_pass_schema_fail_missing_or_empty_required_raises_value_error(
    tmp_path: Path, make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field isn't a list of strings raises SchemaError."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})

    with pytest.raises(ValueError, match="Could not find any required cols in"):
        generate_first_pass_schema(schema_path, tmp_path)


def test_generate_first_pass_schema_fail_missing_schema_file(tmp_path: Path) -> None:
    """A schema_path that doesn't point to an existing file fails FilePath validation."""
    with pytest.raises(ValidationError, match="Path does not point to a file"):
        generate_first_pass_schema(tmp_path / "does-not-exist.json", tmp_path)


def test_generate_first_pass_schema_fail_missing_target_dir(tmp_path: Path, post_norm_schema: Path) -> None:
    """A target_dir that doesn't exist fails DirectoryPath validation."""
    with pytest.raises(ValidationError, match="Path does not point to a directory"):
        generate_first_pass_schema(post_norm_schema, tmp_path / "no-such-dir")


def test_generate_first_pass_schema_pass_bare_minimum(
    tmp_path: Path, make_schema_file: Callable[[Any, str], Path]
) -> None:
    """Test that a schema can be generated from the very barest of bare minimum JSON schema."""
    bare_minimum = {"required": ["this", "that"], **SCHEMA_KEY_VALUE}
    type_dict = {"type": ["string", "null"]}
    schema_path = make_schema_file(bare_minimum)
    output = generate_first_pass_schema(schema_path, tmp_path)
    assert json.loads(output.read_bytes()) == {
        **bare_minimum,
        "type": "object",
        "properties": {"this": type_dict, "that": type_dict},
    }


def test_generate_first_pass_schema_pass_validator_dgaf_about_unresolvable_schema_uri(
    output_dir_path: Path, make_schema_file: Callable[[Any, str], Path]
) -> None:
    """An unrecognised `$schema` draft URI is ignored by the validator."""
    schema = {"$schema": "https://not-a-real.schema-uri", "required": ["a"]}
    schema_path = make_schema_file(schema)
    output = generate_first_pass_schema(schema_path, output_dir_path)
    assert json.loads(output.read_bytes()) == {
        "$schema": "https://not-a-real.schema-uri",
        "required": ["a"],
        "type": "object",
        "properties": {
            "a": {"type": ["string", "null"]},
        },
    }


def test_generate_first_pass_schema_pass_validator_does_gaf_about_non_uris(
    output_dir_path: Path, make_schema_file: Callable[[Any, str], Path]
) -> None:
    """An unrecognised `$schema` draft URI is ignored by the validator."""
    schema = {"$schema": "not-a-real-schema-uri", "required": ["a"]}
    schema_path = make_schema_file(schema)
    with pytest.raises(jsonschema.exceptions.SchemaError, match="'not-a-real-schema-uri' is not a 'uri'"):
        generate_first_pass_schema(schema_path, output_dir_path)


def test_generate_first_pass_schema_fail_invalid_schema_content_raises_schema_error(
    output_dir_path: Path, make_schema_file: Callable[[Any, str], Path]
) -> None:
    """A structurally invalid schema fails with a SchemaError."""
    schema = {"$schema": VALID_SCHEMA_URI, "type": "not-a-real-type", "required": ["a"]}
    schema_path = make_schema_file(schema)

    with pytest.raises(
        jsonschema.exceptions.SchemaError, match="'not-a-real-type' is not valid under any of the given schemas"
    ):
        generate_first_pass_schema(schema_path, output_dir_path)


def test_generate_first_pass_schema_pass_retains_only_allowed_top_level_keys(
    output_dir_path: Path, make_schema_file: Callable[[Any, str], Path]
) -> None:
    """Only $schema, $id, title, and required are copied from the original schema."""
    schema = {
        "$schema": VALID_SCHEMA_URI,
        "$id": "https://example.com/schemas/my-schema.json",
        "title": "My Schema",
        "description": "This should not appear in the first-pass schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["a", "b"],
        "properties": {
            "a": {"type": "string"},
            "b": {"type": "integer"},
        },
    }
    schema_path = make_schema_file(schema)

    output = generate_first_pass_schema(schema_path, output_dir_path)
    result = json.loads(output.read_bytes())

    assert result == {
        "$schema": VALID_SCHEMA_URI,
        "$id": "https://example.com/schemas/my-schema.json",
        "title": "My Schema",
        "type": "object",
        "required": ["a", "b"],
        "properties": {
            "a": {"type": ["string", "null"]},
            "b": {"type": ["string", "null"]},
        },
    }
