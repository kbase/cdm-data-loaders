"""Shared XSV sample rows, file-building helpers, and qsv binary discovery."""

import json
import os
import shutil
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path
from typing import Any, Final

import pytest

from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from tests.helpers.cli_assertions import make_cts_settings

type Row = Sequence[str]
type WriteFile = Callable[[str | bytes, str], Path]

COLUMNS: Final[list[str]] = ["number", "date", "float", "boolean", "string"]
VALID_SCHEMA_URI: Final[str] = "https://json-schema.org/draft/2020-12/schema"

DEFAULT_XSV_SCHEMA: Final[dict[str, Any]] = {
    "$schema": VALID_SCHEMA_URI,
    "title": "XSV integration test schema",
    "type": "object",
    "required": COLUMNS,
    "properties": {
        "number": {"type": "integer"},
        "date": {"type": "string"},
        "float": {"type": ["string", "null"]},
        "boolean": {"type": "boolean"},
        "string": {"type": "string"},
    },
    "x-xsv-config": {"x-delimiter": ",", "x-has-header": True},
}

VALID_ROWS: Final[list[list[str]]] = [
    ["2", "2023-01-15", "3.14", "true", "key:value1"],
    ["3", "2023-02-20", "1.11", "false", "key:value2"],
]

PARTIAL_RAGGED_ROWS: Final[list[list[str]]] = [
    ["2", "2023-01-15", "3.14", "true", "key:value1"],
    ["3", "2023-02-20", "1.11", "false"],
    ["4", "2023-03-25", "2.22", "true", "key:value3"],
]

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
    """Build delimited text content from rows, prefixed with ``header`` when supplied.

    :param rows: Sequence of row cell sequences.
    :type rows: Sequence[Row]
    :param delimiter: Field delimiter character.
    :type delimiter: str
    :param header: Optional header cell sequence.
    :type header: Sequence[str] | None
    :return: Newline-terminated delimited text.
    :rtype: str
    """
    lines: list[str] = []
    if header is not None:
        lines.append(delimiter.join(header))
    lines.extend(delimiter.join(row) for row in rows)
    return "\n".join(lines) + "\n"


def rows_to_csv(rows: Sequence[Row], header: Sequence[str] = COLUMNS, delimiter: str = ",") -> str:
    """Render rows and ``header`` as delimited text content.

    :param rows: Sequence of row cell sequences.
    :type rows: Sequence[Row]
    :param header: Header column names.
    :type header: Sequence[str]
    :param delimiter: Field delimiter character.
    :type delimiter: str
    :return: Newline-terminated delimited text.
    :rtype: str
    """
    return build_xsv_content(rows, delimiter=delimiter, header=header)


def write_xsv(directory: Path, content: str | bytes, file_name: str = "data.tsv") -> Path:
    """Write ``content`` to ``directory / file_name`` and return the written ``Path``.

    :param directory: Target directory.
    :type directory: Path
    :param content: Text or raw byte payload.
    :type content: str | bytes
    :param file_name: File basename inside ``directory``.
    :type file_name: str
    :return: Written file path.
    :rtype: Path
    """
    path = directory / file_name
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return path


def make_xsv_writer(directory: Path) -> WriteFile:
    """Build a helper that writes ``content`` to ``directory / file_name``.

    :param directory: Target directory.
    :type directory: Path
    :return: Curried file writer callable.
    :rtype: WriteFile
    """
    return partial(write_xsv, directory)


def resolve_qsv_cmd() -> str:
    """Resolve the ``qsv`` binary from ``PATH`` or ``QSV_BIN``, calling ``pytest.skip`` when absent.

    :return: Path to the ``qsv`` executable.
    :rtype: str
    """
    cmd = shutil.which("qsv") or os.environ.get("QSV_BIN")
    if not cmd or not Path(cmd).is_file():
        pytest.skip(f"qsv binary not found at {cmd!r}; set QSV_BIN to point at a real binary")
    return cmd


def make_xsv_settings_factory(tmp_path: Path, dlt_destination_config: str) -> Callable[..., XsvIngestSettings]:
    """Return a factory building validated ``XsvIngestSettings`` instances staged under ``tmp_path``.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active ``dlt`` destination name.
    :type dlt_destination_config: str
    :return: Factory building ``XsvIngestSettings``.
    :rtype: Callable[..., XsvIngestSettings]
    """

    def _factory(
        schema: dict[str, Any] | None = DEFAULT_XSV_SCHEMA,
        data_files: dict[str, str] | None = None,
        **overrides: object,
    ) -> XsvIngestSettings:
        raw_input_dir = overrides.get("input_dir", tmp_path / "input")
        input_dir = Path(str(raw_input_dir))
        input_dir.mkdir(parents=True, exist_ok=True)

        if schema is not None:
            (input_dir / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
        if data_files:
            for file_name, content in data_files.items():
                file_path = input_dir / file_name
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(content, encoding="utf-8")

        defaults: dict[str, object] = {
            "dataset_name": "xsv_test_dataset",
            "file_glob": "*",
            "schema_file": str(input_dir / "schema.json"),
            "table_name": "my_table",
            "use_destination": dlt_destination_config,
        }
        defaults.update(overrides)
        return make_cts_settings(XsvIngestSettings, tmp_path, **defaults)

    return _factory


def write_xsv_input_file(settings: XsvIngestSettings, file_name: str, content: str) -> Path:
    """Write ``content`` to ``settings.input_dir / file_name`` and return its ``Path``.

    :param settings: Configured ``XsvIngestSettings``.
    :type settings: XsvIngestSettings
    :param file_name: Relative filename inside ``settings.input_dir``.
    :type file_name: str
    :param content: Raw XSV text.
    :type content: str
    :return: Written file path.
    :rtype: Path
    """
    return write_xsv(Path(settings.input_dir), content, file_name)
