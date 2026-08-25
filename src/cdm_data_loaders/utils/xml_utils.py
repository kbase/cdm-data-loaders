"""
Shared XML helper utilities used by UniProt and UniRef parsers.

This module centralizes common operations:
- Safe text extraction
- Safe attribute extraction
- Property parsing
- Evidence / dbReference parsing
- Cleaning dictionaries
- Deduplicating lists
"""

from logging import Logger, getLogger
from typing import Any

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


# ============================================================
# List / Node Finders
# ============================================================


def find_one(elem: Element, xpath: str, ns: dict[str, str]) -> Element | None:
    """Return first element matching xpath or None."""
    results = elem.findall(xpath, ns)
    return results[0] if results else None


def find_all_text(elem: Element, xpath: str, ns: dict[str, str]) -> list[str]:
    """Return list of text values from xpath matches (deduped)."""
    texts = []
    for node in elem.findall(xpath, ns):
        txt = get_text(node)
        if txt:
            texts.append(txt)
    return list(dict.fromkeys(texts))  # preserve order, dedupe


def safe_list(x) -> list[Any]:
    """Convert None → []."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


# ============================================================
# dbReference / property parsing (shared by UniProt + UniRef)
# ============================================================


def parse_properties(dbref: Element | None, ns: dict[str, str]) -> dict[str, list[str]]:
    """
    Extract key/value pairs from <property type="..." value="..."> blocks.
    """
    if dbref is None:
        return {}
    props = {}
    for prop in dbref.findall("ns:property", ns):
        ptype = prop.attrib.get("type")
        pval = prop.attrib.get("value")
        if ptype and pval:
            if ptype not in props:
                props[ptype] = []
            props[ptype].append(pval)
    return props


def parse_db_references(elem: Element, ns: dict[str, str], pub_types=("PubMed", "DOI")):
    """Generic dbReference parser.

    - Identify publication IDs (PubMed, DOI)
    - Identify other cross-references (dbType:dbId)
    """
    publications = []
    others = []

    for dbref in elem.findall("ns:dbReference", ns):
        db_type = dbref.get("type")
        db_id = dbref.get("id")

        if not db_type or not db_id:
            continue

        if db_type in pub_types:
            publications.append(f"{db_type.upper()}:{db_id}")
        else:
            others.append(f"{db_type}:{db_id}")

    return publications, others


def clean_dict(d: dict[str, Any]) -> dict[str, Any]:
    """
    Remove keys whose value is None or empty list.
    """
    return {k: v for k, v in d.items() if v not in (None, [], {})}
