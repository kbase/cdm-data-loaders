"""Tests for ``docs_macros.tables.registry`` and Markdown table rendering."""

from typing import Final

import markdown
import pytest

from docs_macros.render import render_reference_table
from docs_macros.tables.registry import TABLE_TEMPLATE_REGISTRY

pytestmark = pytest.mark.docs

MIN_MARKDOWN_TABLE_LINES: Final[int] = 3


@pytest.mark.parametrize("table_id", sorted(TABLE_TEMPLATE_REGISTRY), ids=str)
def test_render_reference_table_all_registered_tables_pass(table_id: str) -> None:
    """Verify every registered reference table in TABLE_TEMPLATE_REGISTRY renders an HTML Markdown table."""
    rendered = render_reference_table(table_id)
    lines = rendered.splitlines()
    assert len(lines) >= MIN_MARKDOWN_TABLE_LINES
    assert lines[0].startswith("| ")
    assert lines[1].startswith("| ")
    assert "||" not in lines[1]
    assert "<table>" in markdown.markdown(rendered, extensions=["tables"])
