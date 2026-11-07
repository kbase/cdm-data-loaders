"""Shared XSV test fixtures, sample rows, and file-building helpers."""

import os
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final

import pytest

Row = Sequence[str]
WriteFile = Callable[[str | bytes, str], Path]

COLUMNS: Final[list[str]] = ["number", "date", "float", "boolean", "string"]

VALID_ROWS: Final[list[list[str]]] = [
    ["2", "2023-01-15", "3.14", "true", "key:value1"],
    ["3", "2023-02-20", "1.11", "false", "key:value2"],
]

# One ragged row (missing the trailing `string` field) among otherwise-valid rows: recoverable.
PARTIAL_RAGGED_ROWS: Final[list[list[str]]] = [
    ["2", "2023-01-15", "3.14", "true", "key:value1"],
    ["3", "2023-02-20", "1.11", "false"],
    ["4", "2023-03-25", "2.22", "true", "key:value3"],
]

# Every row here is ragged (too few or too many fields): unrecoverable.
ALL_RAGGED_ROWS: Final[list[list[str]]] = [
    ["2", "2023-01-15", "3.14", "true"],
    ["3", "2023-02-20", "1.11", "false", "key:value2", "extra"],
]


def build_xsv_content(
    rows: Sequence[Row],
    *,
    delimiter: str = "\t",
    header: Sequence[str] | None = None,
) -> str:
    """Build delimited text content from a list of rows, optionally prefixed with a header row."""
    lines: list[str] = []
    if header is not None:
        lines.append(delimiter.join(header))
    lines.extend(delimiter.join(row) for row in rows)
    return "\n".join(lines) + "\n"


def rows_to_csv(rows: Sequence[Row], header: Sequence[str] = COLUMNS, delimiter: str = ",") -> str:
    """Render rows (plus header) as delimited text content, one row per line."""
    return build_xsv_content(rows, delimiter=delimiter, header=header)


def write_xsv(directory: Path, content: str | bytes, file_name: str = "data.tsv") -> Path:
    """Write `content` (str or bytes) to `directory / file_name` and return the Path."""
    path = directory / file_name
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content)
    return path


def make_xsv_writer(directory: Path) -> WriteFile:
    """Build a helper that writes `content` (str or bytes) to `directory / file_name` and returns the Path."""
    return lambda content, file_name: write_xsv(directory, content, file_name)


@pytest.fixture(scope="session")
def qsv_cmd() -> str:
    """Provide the path/name of the qsv binary; override via the QSV_BIN env var if not on PATH.

    Tests using this fixture are skipped when the qsv binary is not available.
    """
    cmd = shutil.which("qsv") or os.environ.get("QSV_BIN")
    if not cmd or not Path(cmd).is_file():
        pytest.skip(f"qsv binary not found at {cmd!r}; set QSV_BIN to point at a real binary")
    return cmd
