"""
Shared XML helper utilities.

This module centralizes common operations:
- Safe text extraction
- Safe attribute extraction
"""

from logging import Logger, getLogger

from lxml.etree import Element

logger: Logger = getLogger(__name__)


def get_text(elem: Element | None, default: str | None = None) -> str | None:
    """Return elem.text if exists and non-empty."""
    if elem is None:
        return default
    if elem.text is None:
        return default
    text = elem.text.strip()
    return text or default


def get_attr(elem: Element | None, name: str, default: str | None = None) -> str | None:
    """Return elem.get(name) safely."""
    if elem is None:
        return default
    val = elem.get(name)
    return val.strip() if isinstance(val, str) else default
