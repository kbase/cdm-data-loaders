"""Smoke tests for NCBI FTP notebooks — syntax and import validation."""

import ast
import json
from pathlib import Path

import pytest

from cdm_data_loaders.ncbi_ftp.assembly import FTP_HOST  # noqa: F401
from cdm_data_loaders.ncbi_ftp.manifest import (  # noqa: F401
    AssemblyRecord,
    compute_diff,
    download_assembly_summary,
    filter_by_prefix_range,
    parse_assembly_summary,
    write_diff_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)
from cdm_data_loaders.ncbi_ftp.promote import (
    DEFAULT_LAKEHOUSE_KEY_PREFIX,
    promote_from_s3,
)
from cdm_data_loaders.utils.s3 import split_s3_path  # noqa: F401

NOTEBOOKS_DIR = Path(__file__).resolve().parents[2] / "notebooks"

NCBI_NOTEBOOKS = [
    "ncbi_ftp_manifest.ipynb",
    "ncbi_ftp_promote.ipynb",
]


def _extract_code_cells(notebook_path: Path) -> list[str]:
    """Extract source code from all code cells in a notebook.

    :param notebook_path: path to the .ipynb file
    :return: list of source code strings, one per code cell
    """
    with notebook_path.open() as f:
        nb = json.load(f)
    return ["".join(cell.get("source", [])) for cell in nb.get("cells", []) if cell.get("cell_type") == "code"]


@pytest.mark.parametrize("notebook", NCBI_NOTEBOOKS)
def test_notebook_syntax(notebook: str) -> None:
    """Every code cell is syntactically valid Python and non-empty."""
    path = NOTEBOOKS_DIR / notebook
    assert path.exists(), f"Notebook not found: {path}"
    cells = _extract_code_cells(path)
    assert len(cells) > 0, f"No code cells found in {notebook}"
    for i, source in enumerate(cells, 1):
        assert source.strip(), f"{notebook} cell {i} is empty"
        try:
            ast.parse(source, filename=f"{notebook}:cell{i}")
        except SyntaxError as exc:
            pytest.fail(f"{notebook} cell {i} has a syntax error: {exc}")


def test_manifest_notebook_imports() -> None:
    """All manifest notebook imports are verified at module load time above."""
    assert isinstance(FTP_HOST, str) and FTP_HOST
    assert AssemblyRecord is not None
    assert callable(download_assembly_summary)
    assert callable(compute_diff)
    assert callable(write_updated_manifest)


def test_promote_notebook_imports() -> None:
    """All promote notebook imports are verified at module load time above."""
    assert callable(promote_from_s3)
    assert isinstance(DEFAULT_LAKEHOUSE_KEY_PREFIX, str)
    assert callable(split_s3_path)
