"""Pure path, code-fence, PyArrow expression, and value formatting helpers for documentation contexts."""

import functools
import gzip
import importlib.util
import json
import os
from collections.abc import Callable, Mapping
from enum import Enum
from pathlib import Path, PurePath
from typing import Annotated

import pyarrow as pa
from pydantic import BaseModel, BeforeValidator, ConfigDict, ValidationInfo, model_validator
from pydantic_core import PydanticUndefined

from cdm_data_loaders.converters.emitters.pyarrow_codegen.formatting import render_scalar_type_expr
from cdm_data_loaders.utils.arrow_schema import is_any_binary_type, is_any_string_type
from docs_macros.constants import (
    BACKTICK_RUN_RE,
    CODE_LANG_BY_SUFFIX,
    GITHUB_BLOB_BASE,
    NON_ALNUM_SLUG_RE,
    REPO_ROOT,
    SRC_ROOT,
)


class FrozenDocModel(BaseModel):
    """Base frozen Pydantic model for documentation view and configuration contexts."""

    model_config = ConfigDict(frozen=True)


class CodeBlockContext(FrozenDocModel):
    """Pre-loaded source block with collision-safe backtick fence metadata for Jinja templates."""

    content: str
    lang: str
    fence: str = ""
    title: str = ""
    linenums: bool = False

    @model_validator(mode="before")
    @classmethod
    def _populate_default_fence(cls, data: object) -> object:
        """Populate ``fence`` from ``content`` when not explicitly provided.

        :param data: Raw input mapping or model data.
        :type data: object
        :return: Input data with ``fence`` populated when empty or omitted.
        :rtype: object
        """
        if isinstance(data, dict) and not data.get("fence"):
            content = str(data.get("content", ""))
            return {**data, "fence": code_fence_for_text(content)}
        return data


def rel_posix(path: Path, start: Path = REPO_ROOT) -> str:
    """Return a POSIX relative path string from ``start`` to ``path``.

    :param path: Target path.
    :type path: Path
    :param start: Base directory path.
    :type start: Path
    :return: POSIX relative path string.
    :rtype: str
    """
    return Path(os.path.relpath(path.resolve(), start.resolve())).as_posix()


def repo_blob_url(path: Path, repo_root: Path = REPO_ROOT) -> str:
    """Return the GitHub ``blob/develop`` URL for a repository file path.

    :param path: Target repository file path.
    :type path: Path
    :param repo_root: Repository root directory.
    :type repo_root: Path
    :return: GitHub blob URL string.
    :rtype: str
    """
    return f"{GITHUB_BLOB_BASE}/{rel_posix(path, repo_root)}"


def escape_table_cell(text: str) -> str:
    """Normalize newlines and escape pipe characters for safe inclusion inside a Markdown table cell.

    :param text: Raw cell text.
    :type text: str
    :return: Single-line Markdown-escaped cell text.
    :rtype: str
    """
    return text.replace("\r\n", " ").replace("\n", " ").replace("|", r"\|")


def code_fence_for_text(text: str, min_backticks: int = 4) -> str:
    """Return a backtick fence string strictly longer than any backtick run in ``text``.

    :param text: Text whose backtick runs must not prematurely close a fenced code block.
    :type text: str
    :param min_backticks: Minimum fence width in backticks.
    :type min_backticks: int
    :return: Backtick fence delimiter.
    :rtype: str
    """
    longest = max((len(run) for run in BACKTICK_RUN_RE.findall(text)), default=0)
    return "`" * max(min_backticks, longest + 1)


def slugify_anchor(prefix: str, value: str) -> str:
    """Build a deterministic lowercase HTML anchor identifier from ``prefix`` and ``value``.

    :param prefix: Leading namespace prefix (e.g. ``"schema"``).
    :type prefix: str
    :param value: Raw path or identifier text.
    :type value: str
    :return: Hyphen-delimited lowercase HTML anchor ID.
    :rtype: str
    """
    cleaned = NON_ALNUM_SLUG_RE.sub("-", value.lower()).strip("-")
    return f"{prefix}-{cleaned}" if cleaned else prefix


def _resolve_module_origin(module_or_symbol: str) -> Path | None:
    """Resolve a Python module, ``module:attr``, or ``module.symbol`` string to its source file path.

    :param module_or_symbol: Dotted Python module path, ``module:attr`` entry point, or ``module.symbol`` path.
    :type module_or_symbol: str
    :return: Resolved source file ``Path`` or ``None`` if not resolvable to a file.
    :rtype: Path | None
    """
    base = module_or_symbol.split(":", maxsplit=1)[0].strip()
    if not base:
        return None
    candidates = (base, base.rsplit(".", maxsplit=1)[0]) if "." in base else (base,)
    for candidate in candidates:
        try:
            spec = importlib.util.find_spec(candidate)
        except (ImportError, ValueError):
            spec = None
        if spec is not None and spec.origin:
            origin_path = Path(spec.origin).resolve()
            if origin_path.is_file():
                return origin_path
    return None


def module_blob_url(module_or_symbol: str, repo_root: Path = REPO_ROOT) -> str | None:
    """Resolve a Python module, ``module:attr``, or ``module.symbol`` string to its GitHub ``blob/develop`` URL.

    :param module_or_symbol: Dotted Python module path, ``module:attr`` entry point, or ``module.symbol`` path.
    :type module_or_symbol: str
    :param repo_root: Repository root directory.
    :type repo_root: Path
    :return: GitHub ``blob/develop`` URL when resolved inside ``repo_root``, or ``None``.
    :rtype: str | None
    """
    origin_path = _resolve_module_origin(module_or_symbol)
    if origin_path is not None and origin_path.is_relative_to(repo_root.resolve()):
        return repo_blob_url(origin_path, repo_root)
    return None


def module_api_url(module_or_symbol: str, src_root: Path = SRC_ROOT) -> str | None:
    """Resolve a Python module, ``module:attr``, or ``module.symbol`` string to its API reference page path.

    :param module_or_symbol: Dotted Python module path, ``module:attr`` entry point, or ``module.symbol`` path.
    :type module_or_symbol: str
    :param src_root: Package source root directory (defaults to ``SRC_ROOT``).
    :type src_root: Path
    :return: Docs-root-relative API page path (e.g. ``"api/cdm_data_loaders/core/settings.md"``), or ``None``.
    :rtype: str | None
    """
    origin_path = _resolve_module_origin(module_or_symbol)
    resolved_src = src_root.resolve()
    if origin_path is None or not origin_path.is_relative_to(resolved_src):
        return None
    rel_file = origin_path.relative_to(resolved_src)
    if origin_path.name == "__init__.py":
        doc_rel = (
            Path("api", src_root.name, *rel_file.parent.parts, "index.md")
            if rel_file.parent.parts
            else Path("api", src_root.name, "index.md")
        )
    else:
        doc_rel = Path("api", src_root.name, *rel_file.with_suffix(".md").parts)
    return doc_rel.as_posix()


def _summarize_container_value(val: object) -> str | None:
    """Return a concise Markdown summary for ``pa.Schema``, callable tuples, or mappings.

    :param val: Candidate container or schema value.
    :type val: object
    :return: Concise Markdown label, or ``None`` when ``val`` is not a summarized container.
    :rtype: str | None
    """
    if isinstance(val, pa.Schema):
        return f"`pa.Schema` ({len(val)} fields)"
    if isinstance(val, tuple) and val and all(callable(item) for item in val):
        names = ", ".join(getattr(fn, "__name__", repr(fn)) for fn in val)
        return f"`({names},)`"
    if isinstance(val, Mapping):
        keys = sorted(str(k) for k in val)
        return f"mapping with keys `{keys}`"
    return None


def format_value_code(
    val: object,
    *,
    repo_root: Path = REPO_ROOT,
    undefined_label: str = "*(required)*",
    summarize_containers: bool = True,
) -> str:
    """Format a Python default or attribute value as a concise display label.

    :param val: Value to format.
    :type val: object
    :param repo_root: Repository root used to relativize ``Path`` instances.
    :type repo_root: Path
    :param undefined_label: Label returned when ``val`` is ``PydanticUndefined``.
    :type undefined_label: str
    :param summarize_containers: When ``True``, summarize callable tuples and dicts instead of ``repr``.
    :type summarize_containers: bool
    :return: Formatted value representation.
    :rtype: str
    """
    if val is PydanticUndefined:
        return undefined_label
    if isinstance(val, Enum):
        return f"`{val.value!r}`"
    if isinstance(val, Path | PurePath):
        rel = val.relative_to(repo_root) if isinstance(val, Path) and val.is_relative_to(repo_root) else val
        return f"`{rel.as_posix()}`"
    if summarize_containers and (summary := _summarize_container_value(val)) is not None:
        return summary
    return f"`{val!r}`"


def infer_code_lang(path: Path, default_lang: str = "text") -> str:
    """Resolve the Pygments fenced-code language identifier for ``path``.

    :param path: Fixture or schema file path.
    :type path: Path
    :param default_lang: Fallback language identifier when ``path`` has no recognized suffix.
    :type default_lang: str
    :return: Pygments language identifier string.
    :rtype: str
    """
    if path.suffix in {".md5", ".txt"}:
        return "text"
    if default_lang != "text":
        return default_lang
    inner_name = path.name.removesuffix(".gz")
    inner_suffix = Path(inner_name).suffix
    return CODE_LANG_BY_SUFFIX.get(inner_suffix, default_lang)


def read_code_block(
    path: Path,
    lang: str | None = None,
    *,
    min_backticks: int = 3,
    title: str = "",
    linenums: bool = False,
) -> CodeBlockContext:
    """Read ``path`` (decompressing ``.gz`` archives if needed) into a ``CodeBlockContext``.

    :param path: File path to read.
    :type path: Path
    :param lang: Optional explicit Pygments language override.
    :type lang: str | None
    :param min_backticks: Minimum fence width in backticks.
    :type min_backticks: int
    :param title: Optional code block header title.
    :type title: str
    :param linenums: Whether line numbers should be enabled on the code fence.
    :type linenums: bool
    :return: Populated ``CodeBlockContext`` instance.
    :rtype: CodeBlockContext
    """
    effective_lang = infer_code_lang(path, lang or "text")
    raw = path.read_bytes()
    text = (gzip.decompress(raw) if path.name.endswith(".gz") else raw).decode("utf-8").rstrip()
    return CodeBlockContext(
        content=text,
        lang=effective_lang,
        fence=code_fence_for_text(text, min_backticks=min_backticks),
        title=title,
        linenums=linenums,
    )


def pa_ctor_expr(dtype: pa.DataType) -> str:
    """Format ``dtype`` as its ``pa.*`` constructor expression string (without surrounding backticks).

    :param dtype: PyArrow scalar or nested ``DataType``.
    :type dtype: pa.DataType
    :return: Python ``pa.*`` constructor string or stringified fallback.
    :rtype: str
    """
    try:
        return render_scalar_type_expr(dtype)
    except KeyError:
        return str(dtype)


def pyarrow_to_python_type_label(data_type: pa.DataType) -> str:
    """Return the Markdown-formatted Python runtime type label for a coerced PyArrow ``DataType``.

    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :return: Backtick-wrapped Python runtime type label (e.g. ``"`int`"``, ``"`datetime.datetime` (UTC)"``).
    :rtype: str
    """
    if isinstance(data_type, pa.DictionaryType):
        return pyarrow_to_python_type_label(data_type.value_type)
    if isinstance(
        data_type,
        pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType,
    ):
        inner = pyarrow_to_python_type_label(data_type.value_field.type).strip("`")
        return f"`list[{inner}]`"
    if isinstance(data_type, pa.TimestampType):
        tz_suffix = f"({data_type.tz})" if data_type.tz is not None else "(naive)"
        return f"`datetime.datetime` {tz_suffix}"
    for predicate, label in (
        (pa.types.is_boolean, "`bool`"),
        (pa.types.is_integer, "`int`"),
        (pa.types.is_floating, "`float`"),
        (pa.types.is_decimal, "`decimal.Decimal`"),
        (pa.types.is_date, "`datetime.date`"),
        (pa.types.is_time, "`datetime.time`"),
        (is_any_binary_type, "`bytes`"),
        (is_any_string_type, "`str`"),
    ):
        if predicate(data_type):
            return label
    return f"`{data_type}`"


def _coerce_pa_or_str(value: object, formatter: Callable[[pa.DataType], str]) -> str:
    """Coerce a PyArrow ``DataType`` or string using ``formatter``.

    :param value: PyArrow ``DataType`` instance or pre-formatted string.
    :type value: object
    :param formatter: Formatter applied when ``value`` is a ``pa.DataType``.
    :type formatter: Callable[[pa.DataType], str]
    :return: Formatted string.
    :rtype: str
    """
    if isinstance(value, pa.DataType):
        return formatter(value)
    if isinstance(value, str):
        return value
    msg = f"Expected pa.DataType or str, got {type(value).__name__}"
    raise TypeError(msg)


def coerce_json_text(value: object) -> str:
    """Coerce a JSON-serializable mapping, sequence, or string into a JSON string.

    :param value: Pre-serialized JSON string, ``Mapping``, or sequence.
    :type value: object
    :return: JSON-encoded string.
    :rtype: str
    """
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return json.dumps(dict(value))
    if isinstance(value, list | tuple):
        return json.dumps(list(value))
    msg = f"Expected str, Mapping, or sequence, got {type(value).__name__}"
    raise TypeError(msg)


def coerce_normalized_prose(value: object) -> str:
    """Coerce ``None`` or multi-line prose into collapsed single-spaced text.

    :param value: Raw prose string or ``None``.
    :type value: object
    :return: Whitespace-normalized string.
    :rtype: str
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.split())
    msg = f"Expected str or None, got {type(value).__name__}"
    raise TypeError(msg)


def coerce_non_null_str(value: object) -> str:
    """Coerce ``None`` to an empty string while preserving string values.

    :param value: Raw string or ``None``.
    :type value: object
    :return: String value or ``""``.
    :rtype: str
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    msg = f"Expected str or None, got {type(value).__name__}"
    raise TypeError(msg)


def coerce_module_api_url(value: object, info: ValidationInfo) -> str | None:
    """Coerce a module name, ``module:attr``, class, or API page path into an API doc URL.

    :param value: Pre-resolved API URL, dotted module/symbol string, Python class, or ``None``.
    :type value: object
    :param info: Pydantic validation context optionally providing ``src_root``.
    :type info: ValidationInfo
    :return: Docs-root-relative API page path or ``None``.
    :rtype: str | None
    """
    if value is None:
        return None
    src_root = info.context.get("src_root", SRC_ROOT) if isinstance(info.context, dict) else SRC_ROOT
    if isinstance(value, type):
        return module_api_url(value.__module__, src_root=src_root)
    if isinstance(value, str):
        if value.startswith("api/") and value.endswith(".md"):
            return value
        return module_api_url(value, src_root=src_root)
    msg = f"Expected str, type, or None, got {type(value).__name__}"
    raise TypeError(msg)


def coerce_module_blob_url(value: object, info: ValidationInfo) -> str | None:
    """Coerce a module name, ``module:attr``, class, or GitHub blob URL into a blob URL.

    :param value: Pre-resolved GitHub URL, dotted module/symbol string, Python class, or ``None``.
    :type value: object
    :param info: Pydantic validation context optionally providing ``repo_root``.
    :type info: ValidationInfo
    :return: GitHub ``blob/develop`` URL or ``None``.
    :rtype: str | None
    """
    if value is None:
        return None
    repo_root = info.context.get("repo_root", REPO_ROOT) if isinstance(info.context, dict) else REPO_ROOT
    if isinstance(value, type):
        return module_blob_url(value.__module__, repo_root=repo_root)
    if isinstance(value, str):
        if value.startswith(GITHUB_BLOB_BASE):
            return value
        return module_blob_url(value, repo_root=repo_root)
    msg = f"Expected str, type, or None, got {type(value).__name__}"
    raise TypeError(msg)


type PyArrowTypeExpr = Annotated[str, BeforeValidator(functools.partial(_coerce_pa_or_str, formatter=pa_ctor_expr))]
type PyArrowPythonTypeLabel = Annotated[
    str,
    BeforeValidator(functools.partial(_coerce_pa_or_str, formatter=pyarrow_to_python_type_label)),
]
type JsonText = Annotated[str, BeforeValidator(coerce_json_text)]
type NormalizedProse = Annotated[str, BeforeValidator(coerce_normalized_prose)]
type NonNullStr = Annotated[str, BeforeValidator(coerce_non_null_str)]
type ModuleApiUrl = Annotated[str | None, BeforeValidator(coerce_module_api_url)]
type ModuleBlobUrl = Annotated[str | None, BeforeValidator(coerce_module_blob_url)]
