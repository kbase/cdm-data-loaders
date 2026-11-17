"""Identifier naming, string escaping, and spec models for PyArrow Python codegen."""

import re
from collections.abc import Mapping
from typing import Any, Final

from pydantic import BaseModel, ConfigDict

MAX_LINE_LENGTH: Final[int] = 120
TYPE_SUFFIX: Final[str] = "Type"

type ComponentKey = tuple[int, bool]


class RenderedFieldSpec(BaseModel):
    """Specification for rendering a single ``pa.field(...)`` call."""

    model_config = ConfigDict(frozen=True)

    name: str
    type_expr: str
    nullable: bool
    metadata: Mapping[str, str] | None
    inline_struct_fields: tuple["RenderedFieldSpec", ...] | None = None
    is_list: bool = False
    list_item_nullable: bool = True


class StructComponentSpec(BaseModel):
    """Specification for a reusable top-level ``pa.StructType`` constant."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    key: ComponentKey
    el_name: str
    parent_el_name: str | None
    type_local_name: str | None
    fields: tuple[RenderedFieldSpec, ...]
    pa_struct: Any = None


def fmt_str(val: str) -> str:
    """Format a Python string literal using Ruff double-quote style.

    :param val: Raw string value.
    :type val: str
    :return: Quoted Python string literal.
    :rtype: str
    """
    if '"' in val:
        return repr(val)
    escaped = val.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return f'"{escaped}"'


def to_upper_snake(name: str, *, strip_type_suffix: bool = False) -> str:
    """Convert a camelCase, PascalCase, or snake_case identifier to ``UPPER_SNAKE_CASE``.

    :param name: Raw type, model, or field name.
    :type name: str
    :param strip_type_suffix: Whether to strip a trailing ``Type`` suffix before conversion.
    :type strip_type_suffix: bool
    :return: ``UPPER_SNAKE_CASE`` identifier stem.
    :rtype: str
    """
    if strip_type_suffix and name.endswith(TYPE_SUFFIX) and len(name) > len(TYPE_SUFFIX):
        name = name[: -len(TYPE_SUFFIX)]
    step1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    step2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", step1)
    stem = re.sub(r"[^A-Za-z0-9]+", "_", step2).upper().strip("_")
    if not stem:
        return "FIELD"
    return f"F_{stem}" if stem[0].isdigit() else stem


def decode_sorted_metadata(raw_meta: Mapping[str | bytes, str | bytes] | None) -> dict[str, str] | None:
    """Convert a PyArrow metadata mapping into a sorted ``str`` dictionary.

    :param raw_meta: Raw metadata dictionary with ``str`` or ``bytes`` keys and values.
    :type raw_meta: Mapping[str | bytes, str | bytes] | None
    :return: Sorted string dictionary, or ``None`` if ``raw_meta`` is empty.
    :rtype: dict[str, str] | None
    """
    if not raw_meta:
        return None
    decoded = {
        (k.decode("utf-8") if isinstance(k, bytes) else k): (v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in raw_meta.items()
    }
    return dict(sorted(decoded.items()))
