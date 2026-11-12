"""Generate CLI reference, schema source pages, and published JSON Schemas from codebase metadata.

Usage::

    uv run python -m scripts.gen_docs          # write docs/cli_reference.md, docs/bundled_schemas.md, docs/schema_source/, and docs/schemas/
    uv run python -m scripts.gen_docs --check  # exit 1 if generated docs are stale
"""

import argparse
import ast
import importlib
import os
import re
import sys
import tomllib
import types
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePath
from types import MappingProxyType
from typing import Annotated, Any, Final, Literal, get_args, get_origin

from pydantic import AliasChoices
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from cdm_data_loaders.core.settings import CdmDataLoadersBase

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
DOCS_DIR: Final[Path] = REPO_ROOT / "docs"
PYPROJECT_PATH: Final[Path] = REPO_ROOT / "pyproject.toml"
SRC_ROOT: Final[Path] = REPO_ROOT / "src" / "cdm_data_loaders"
CLI_REFERENCE_PATH: Final[Path] = DOCS_DIR / "cli_reference.md"
BUNDLED_SCHEMAS_PATH: Final[Path] = DOCS_DIR / "bundled_schemas.md"
SCHEMAS_SUBDIR: Final[str] = "schemas"
SCHEMA_SOURCE_DOCS_SUBDIR: Final[str] = "schema_source"
SCHEMA_SOURCE_DOCS_DIR: Final[Path] = DOCS_DIR / SCHEMA_SOURCE_DOCS_SUBDIR
PUBLISHED_SCHEMAS_DOCS_DIR: Final[Path] = DOCS_DIR / SCHEMAS_SUBDIR
PUBLISHED_JSONSCHEMA_SUBDIRS: Final[tuple[str, ...]] = ("xsv_config",)
SCHEMAS_SUMMARY_FILENAME: Final[str] = "SUMMARY.md"
SITE_URL_BASE: Final[str] = "https://kbase.github.io/cdm-data-loaders"
GITHUB_BLOB_BASE: Final[str] = "https://github.com/kbase/cdm-data-loaders/blob/develop"
PYTHON_ARTIFACT_SUFFIXES: Final[frozenset[str]] = frozenset({".py", ".pyc", ".pyo", ".pyd", ".pyi"})
EXCLUDED_SCHEMA_DIR_NAMES: Final[frozenset[str]] = frozenset({"__pycache__"})
BACKTICK_RUN_RE: Final[re.Pattern[str]] = re.compile(r"`+")
SCHEMA_EXTENSION_METADATA: Final[MappingProxyType[str, tuple[str, str]]] = MappingProxyType(
    {
        ".avsc": ("Avro Schema", "json"),
        ".csv": ("CSV", "text"),
        ".graphql": ("GraphQL Schema", "graphql"),
        ".json": ("JSON Schema", "json"),
        ".jsonl": ("JSON Lines", "json"),
        ".md": ("Markdown", "markdown"),
        ".proto": ("Protocol Buffers", "protobuf"),
        ".sql": ("SQL", "sql"),
        ".toml": ("TOML", "toml"),
        ".tsv": ("TSV", "text"),
        ".txt": ("Text", "text"),
        ".xml": ("XML", "xml"),
        ".xsd": ("XML Schema (XSD)", "xml"),
        ".yaml": ("YAML", "yaml"),
        ".yml": ("YAML", "yaml"),
    }
)

GENERATED_BANNER: Final[str] = (
    "<!-- GENERATED FILE - DO NOT EDIT. Source: `pyproject.toml`, `src/cdm_data_loaders/`. "
    "Regenerate with `uv run python -m scripts.gen_docs`. -->"
)


@dataclass(frozen=True, slots=True)
class CliFieldDoc:
    """Documentation metadata for a single Pydantic CLI setting field."""

    name: str
    cli_flag: str
    shortcuts: tuple[str, ...]
    env_var: str
    type_label: str
    default_label: str
    required: bool
    description: str


@dataclass(frozen=True, slots=True)
class CliEntryDoc:
    """Documentation metadata for a registered ``[project.scripts]`` CLI entry point."""

    cli_name: str
    entry_point: str
    module_name: str
    settings_class_name: str
    inheritance_chain: tuple[str, ...]
    summary: str
    fields: tuple[CliFieldDoc, ...]


@dataclass(frozen=True, slots=True)
class ModuleDoc:
    """Documentation metadata for a Python module under ``src/cdm_data_loaders/``."""

    module_path: str
    rel_file: Path
    doc_rel_path: Path
    nav_parts: tuple[str, ...]
    summary: str
    is_package: bool


@dataclass(frozen=True, slots=True)
class SchemaAssetDoc:
    """Documentation metadata for a bundled non-Python file under ``src/cdm_data_loaders/schemas/``."""

    rel_path: Path
    doc_rel_path: Path
    kind: str
    highlight_lang: str
    package_area: str
    published_rel_path: Path | None = None


def _format_origin_annotation(origin: object, args: tuple[object, ...]) -> str:
    """Format a generic or union origin type with its type arguments."""
    if origin is Annotated and args:
        return _format_type_annotation(args[0])
    if origin is Literal:
        return " | ".join(repr(arg) for arg in args)
    if origin is types.UnionType or getattr(origin, "__name__", "") == "Union":
        return " | ".join(_format_type_annotation(arg) for arg in args)
    origin_name = getattr(origin, "__name__", str(origin))
    if args:
        inner = ", ".join(_format_type_annotation(arg) for arg in args)
        return f"{origin_name}[{inner}]"
    return origin_name


def _format_type_annotation(annotation: object) -> str:
    """Render a concise human-readable type label for a field annotation."""
    if annotation is None or annotation is type(None):
        return "None"

    origin = get_origin(annotation)
    if origin is not None:
        return _format_origin_annotation(origin, get_args(annotation))

    if isinstance(annotation, type):
        if issubclass(annotation, Enum):
            return " | ".join(repr(member.value) for member in annotation)
        return annotation.__name__

    return str(annotation)


def rel_posix(path: Path, start: Path = REPO_ROOT) -> str:
    """Return a POSIX relative path string from ``start`` to ``path``.

    :param path: target path
    :type path: Path
    :param start: base directory path
    :type start: Path
    :return: POSIX relative path string
    :rtype: str
    """
    return Path(os.path.relpath(path.resolve(), start.resolve())).as_posix()


def repo_blob_url(path: Path, repo_root: Path = REPO_ROOT) -> str:
    """Return the GitHub ``blob/develop`` URL for a repository file path.

    :param path: target repository file path
    :type path: Path
    :param repo_root: repository root directory
    :type repo_root: Path
    :return: GitHub blob URL string
    :rtype: str
    """
    return f"{GITHUB_BLOB_BASE}/{rel_posix(path, repo_root)}"


def escape_table_cell(text: str) -> str:
    """Escape pipe characters for safe inclusion inside a Markdown table cell.

    :param text: raw cell text
    :type text: str
    :return: Markdown-escaped cell text
    :rtype: str
    """
    return text.replace("|", r"\|")


def mro_class_names(cls: type[Any], base_cls: type[Any] = CdmDataLoadersBase) -> tuple[str, ...]:
    """Return the MRO class names of ``cls`` that are subclasses of ``base_cls``.

    :param cls: target class to inspect
    :type cls: type[Any]
    :param base_cls: base class upper bound for the MRO chain
    :type base_cls: type[Any]
    :return: ordered tuple of class names from ``cls`` up to ``base_cls``
    :rtype: tuple[str, ...]
    """
    return tuple(c.__name__ for c in cls.__mro__ if isinstance(c, type) and issubclass(c, base_cls))


def format_value_code(
    val: object,
    *,
    repo_root: Path = REPO_ROOT,
    undefined_label: str = "*(required)*",
    summarize_containers: bool = True,
) -> str:
    """Format a Python default or attribute value as a Markdown cell string.

    :param val: value to format
    :type val: object
    :param repo_root: repository root used to relativize ``Path`` instances
    :type repo_root: Path
    :param undefined_label: label returned when ``val`` is ``PydanticUndefined``
    :type undefined_label: str
    :param summarize_containers: when ``True``, summarize callable tuples and dicts instead of ``repr``
    :type summarize_containers: bool
    :return: Markdown-formatted value representation
    :rtype: str
    """
    if val is PydanticUndefined:
        return undefined_label
    if isinstance(val, Enum):
        return f"`{val.value!r}`"
    if isinstance(val, Path | PurePath):
        rel = val.relative_to(repo_root) if isinstance(val, Path) and val.is_relative_to(repo_root) else val
        return f"`{rel.as_posix()}`"
    if summarize_containers and isinstance(val, tuple) and val and all(callable(item) for item in val):
        names = ", ".join(getattr(fn, "__name__", repr(fn)) for fn in val)
        return f"`({names},)`"
    if summarize_containers and isinstance(val, dict):
        keys = sorted(str(k) for k in val)
        return f"mapping with keys `{keys}`"
    return f"`{val!r}`"


def _format_default_value(field_info: FieldInfo, repo_root: Path = REPO_ROOT) -> tuple[str, bool]:
    """Return ``(default_label, required)`` for a Pydantic ``FieldInfo``."""
    if field_info.is_required():
        return ("*(Required)*", True)
    return (
        format_value_code(
            field_info.default,
            repo_root=repo_root,
            undefined_label="*(Dynamic)*",
            summarize_containers=False,
        ),
        False,
    )


def _extract_shortcuts(
    field_name: str,
    field_info: FieldInfo,
    cli_shortcuts: dict[str, str | list[str]],
) -> tuple[str, ...]:
    """Extract CLI shortcut flags (e.g. ``-d``, ``--dev``) for ``field_name``."""
    raw_shortcuts: list[str] = []
    kebab_name = field_name.replace("_", "-")

    for candidate_key in (field_name, kebab_name):
        if candidate_key in cli_shortcuts:
            entry = cli_shortcuts[candidate_key]
            items = [entry] if isinstance(entry, str) else entry
            for item in items:
                if item not in raw_shortcuts:
                    raw_shortcuts.append(item)

    val_alias = field_info.validation_alias
    if isinstance(val_alias, AliasChoices):
        for choice in val_alias.choices:
            if isinstance(choice, str) and choice not in {field_name, kebab_name} and choice not in raw_shortcuts:
                raw_shortcuts.append(choice)

    formatted: list[str] = []
    for shortcut in raw_shortcuts:
        flag = f"-{shortcut}" if len(shortcut) == 1 else f"--{shortcut}"
        if flag not in formatted:
            formatted.append(flag)
    return tuple(formatted)


def _first_docstring_line(docstring: str | None, fallback: str) -> str:
    """Return the cleaned first summary line of a module or class docstring."""
    if not docstring:
        return fallback
    for line in docstring.strip().splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return fallback


def inspect_settings_class(
    cli_name: str,
    entry_point: str,
    settings_cls: type[CdmDataLoadersBase],
    summary: str,
    repo_root: Path = REPO_ROOT,
) -> CliEntryDoc:
    """Introspect a ``CdmDataLoadersBase`` subclass into a ``CliEntryDoc``."""
    module_name = entry_point.split(":", maxsplit=1)[0]
    env_prefix = str(settings_cls.model_config.get("env_prefix", "CDL_"))
    raw_shortcuts = settings_cls.model_config.get("cli_shortcuts")
    cli_shortcuts: dict[str, str | list[str]] = dict(raw_shortcuts) if isinstance(raw_shortcuts, dict) else {}

    inheritance_chain = mro_class_names(settings_cls)

    fields: list[CliFieldDoc] = []
    for field_name, field_info in settings_cls.model_fields.items():
        cli_flag = f"--{field_name.replace('_', '-')}"
        shortcuts = _extract_shortcuts(field_name, field_info, cli_shortcuts)
        env_var = f"{env_prefix}{field_name.upper()}"
        type_label = _format_type_annotation(field_info.annotation)
        default_label, required = _format_default_value(field_info, repo_root=repo_root)
        description = (field_info.description or "").strip().replace("\n", " ")
        fields.append(
            CliFieldDoc(
                name=field_name,
                cli_flag=cli_flag,
                shortcuts=shortcuts,
                env_var=env_var,
                type_label=type_label,
                default_label=default_label,
                required=required,
                description=description,
            )
        )

    return CliEntryDoc(
        cli_name=cli_name,
        entry_point=entry_point,
        module_name=module_name,
        settings_class_name=settings_cls.__name__,
        inheritance_chain=inheritance_chain,
        summary=summary,
        fields=tuple(fields),
    )


def load_cli_entries(pyproject_path: Path = PYPROJECT_PATH, repo_root: Path = REPO_ROOT) -> list[CliEntryDoc]:
    """Load and introspect all ``[project.scripts]`` CLI entry points from ``pyproject.toml``."""
    with pyproject_path.open("rb") as fh:
        pyproject_data = tomllib.load(fh)

    scripts: dict[str, str] = pyproject_data.get("project", {}).get("scripts", {})
    entries: list[CliEntryDoc] = []

    for cli_name, entry_point in sorted(scripts.items()):
        module_name, attr_name = entry_point.split(":", maxsplit=1)
        module = importlib.import_module(module_name)
        cli_obj = getattr(module, attr_name)
        settings_cls = getattr(cli_obj, "settings_cls", None)
        if not isinstance(settings_cls, type) or not issubclass(settings_cls, CdmDataLoadersBase):
            msg = f"Entry point {cli_name} ({entry_point}) does not expose a CdmDataLoadersBase settings_cls"
            raise TypeError(msg)

        summary = _first_docstring_line(
            settings_cls.__doc__ or module.__doc__,
            fallback=f"CLI pipeline for {cli_name}.",
        )
        entries.append(inspect_settings_class(cli_name, entry_point, settings_cls, summary, repo_root=repo_root))

    return entries


def prune_empty_parents(directory: Path, stop_at: Path) -> None:
    """Remove empty directories upward from ``directory`` without removing ``stop_at``.

    :param directory: starting directory to prune if empty
    :type directory: Path
    :param stop_at: upper-bound directory that must not be removed
    :type stop_at: Path
    :return: None
    :rtype: None
    """
    resolved_stop = stop_at.resolve()
    current = directory
    while current.resolve() != resolved_stop and current.is_relative_to(stop_at):
        if not current.is_dir() or any(current.iterdir()):
            break
        current.rmdir()
        current = current.parent


def _sync_managed_directories(
    managed_dirs: Iterable[Path],
    expected_paths: set[Path],
    *,
    check: bool,
) -> list[Path]:
    """Detect or delete orphaned files inside ``managed_dirs`` that are not in ``expected_paths``."""
    orphans: list[Path] = []
    for managed_dir in managed_dirs:
        if not managed_dir.is_dir():
            continue
        for existing_path in sorted(managed_dir.rglob("*")):
            if not existing_path.is_file() or existing_path.resolve() in expected_paths:
                continue
            if check:
                orphans.append(existing_path)
            else:
                parent_dir = existing_path.parent
                existing_path.unlink()
                prune_empty_parents(parent_dir, managed_dir)
    return orphans


def check_or_write_files(
    outputs: Iterable[tuple[Path, str]],
    *,
    check: bool,
    repo_root: Path = REPO_ROOT,
    managed_dirs: Iterable[Path] = (),
) -> int:
    """Compare each ``(output_path, rendered_text)`` pair against disk and either report stale files or write updates.

    :param outputs: iterable of ``(output_path, rendered_text)`` pairs
    :type outputs: Iterable[tuple[Path, str]]
    :param check: when ``True``, report stale or orphaned files to ``sys.stderr`` and return ``1`` without writing
    :type check: bool
    :param repo_root: repository root used to format relative paths on ``stderr``
    :type repo_root: Path
    :param managed_dirs: directories whose extraneous files should be flagged (``check=True``) or deleted
    :type managed_dirs: Iterable[Path]
    :return: ``0`` if all files are up to date (or written), ``1`` if ``check`` is ``True`` and any file is stale
    :rtype: int
    """
    output_items = list(outputs)
    expected_paths = {output_path.resolve() for output_path, _ in output_items}
    stale: list[Path] = []
    for output_path, text in output_items:
        current = output_path.read_text(encoding="utf-8") if output_path.exists() else None
        if current == text:
            continue
        if check:
            stale.append(output_path)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(text, encoding="utf-8")
    stale.extend(_sync_managed_directories(managed_dirs, expected_paths, check=check))
    for path in stale:
        rel_out = (
            path.relative_to(repo_root).as_posix() if path.is_relative_to(repo_root) else rel_posix(path, repo_root)
        )
        sys.stderr.write(f"stale: {rel_out} is out of date\n")
    return 1 if stale else 0


def render_cli_reference(entries: Sequence[CliEntryDoc] | None = None) -> str:
    """Render the complete ``docs/cli_reference.md`` Markdown document."""
    resolved_entries = list(entries) if entries is not None else load_cli_entries()

    lines: list[str] = [
        GENERATED_BANNER,
        "",
        "# CLI Reference",
        "",
        "Auto-generated reference for all registered pipeline CLIs in `pyproject.toml` (`[project.scripts]`).",
        "",
        "- **Local execution**: `uv run <cli_command> [OPTIONS]`",
        "- **Container execution**: `./scripts/entrypoint.sh pipeline <cli_command> [OPTIONS]`",
        "- **Environment variables**: Every CLI option can also be configured via its `CDL_*` environment variable.",
        "- **Execution & CTS semantics**: See [Core Pipeline & KBase CTS Execution Guide](core_cts_execution.md).",
        "",
        "## Registered Pipeline CLIs",
        "",
        "| CLI Command | Settings Class | Entry Point | Summary |",
        "| :--- | :--- | :--- | :--- |",
    ]

    for entry in resolved_entries:
        anchor = f"#{entry.cli_name.lower()}"
        lines.append(
            f"| [`{entry.cli_name}`]({anchor}) | `{entry.settings_class_name}` | "
            f"`{entry.entry_point}` | {escape_table_cell(entry.summary)} |"
        )

    lines.append("")

    for entry in resolved_entries:
        chain_str = " -> ".join(f"`{cls_name}`" for cls_name in entry.inheritance_chain)
        lines.extend(
            [
                f"## `{entry.cli_name}`",
                "",
                f"{entry.summary}",
                "",
                f"- **Entry point**: `{entry.entry_point}`",
                f"- **Settings hierarchy**: {chain_str}",
                "",
                "| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |",
                "| :--- | :--- | :--- | :--- | :--- | :--- |",
            ]
        )
        for field_doc in entry.fields:
            shortcut_str = ", ".join(f"`{s}`" for s in field_doc.shortcuts) if field_doc.shortcuts else "—"
            type_cell = escape_table_cell(f"`{field_doc.type_label}`")
            default_cell = escape_table_cell(field_doc.default_label)
            desc_cell = escape_table_cell(field_doc.description or "—")
            lines.append(
                f"| `{field_doc.cli_flag}` | {shortcut_str} | `{field_doc.env_var}` | "
                f"{type_cell} | {default_cell} | {desc_cell} |"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _read_ast_docstring(py_file: Path) -> str:
    """Extract the module docstring from a Python source file using ``ast`` without importing it."""
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(py_file))
    return _first_docstring_line(ast.get_docstring(tree), fallback="—")


def discover_api_modules(src_root: Path = SRC_ROOT) -> list[ModuleDoc]:
    """Discover all Python packages and modules under ``src/cdm_data_loaders/``."""
    modules: list[ModuleDoc] = []
    package_root_name = src_root.name

    for py_file in sorted(src_root.rglob("*.py")):
        rel_file = py_file.relative_to(src_root)
        is_package = py_file.name == "__init__.py"
        if is_package:
            parts = (package_root_name, *rel_file.parent.parts) if rel_file.parent.parts else (package_root_name,)
            doc_rel_path = Path(*rel_file.parent.parts, "index.md") if rel_file.parent.parts else Path("index.md")
        else:
            parts = (package_root_name, *rel_file.with_suffix("").parts)
            doc_rel_path = rel_file.with_suffix(".md")

        module_path = ".".join(parts)
        summary = _read_ast_docstring(py_file)
        modules.append(
            ModuleDoc(
                module_path=module_path,
                rel_file=rel_file,
                doc_rel_path=doc_rel_path,
                nav_parts=parts,
                summary=summary,
                is_package=is_package,
            )
        )

    modules.sort(key=lambda m: (m.nav_parts if m.is_package else m.nav_parts[:-1], not m.is_package, m.nav_parts[-1]))
    return modules


def resolve_schema_file_metadata(path: Path) -> tuple[str, str]:
    """Return ``(kind, highlight_lang)`` metadata for a non-Python schema file.

    :param path: schema asset file path
    :type path: Path
    :return: human-readable format label and Pygments highlight language identifier
    :rtype: tuple[str, str]
    """
    suffix = path.suffix.lower()
    if suffix in SCHEMA_EXTENSION_METADATA:
        return SCHEMA_EXTENSION_METADATA[suffix]
    if suffix:
        return (f"{suffix.lstrip('.').upper()} File", "text")
    return ("File", "text")


def code_fence_for_text(text: str, min_backticks: int = 4) -> str:
    """Return a backtick fence string strictly longer than any backtick run in ``text``.

    :param text: text whose backtick runs must not prematurely close a fenced code block
    :type text: str
    :param min_backticks: minimum fence width in backticks
    :type min_backticks: int
    :return: backtick fence delimiter
    :rtype: str
    """
    longest = max((len(run) for run in BACKTICK_RUN_RE.findall(text)), default=0)
    return "`" * max(min_backticks, longest + 1)


def is_published_jsonschema(
    rel_to_schemas: Path,
    published_subdirs: Sequence[str] = PUBLISHED_JSONSCHEMA_SUBDIRS,
) -> bool:
    """Return whether ``rel_to_schemas`` is a JSON Schema served directly under ``/schemas/`` on the docs site.

    :param rel_to_schemas: path relative to ``src/cdm_data_loaders/schemas/``
    :type rel_to_schemas: Path
    :param published_subdirs: top-level schema subdirectories published as static JSON Schemas
    :type published_subdirs: Sequence[str]
    :return: ``True`` if the file should be copied into ``docs/schemas/``
    :rtype: bool
    """
    return (
        rel_to_schemas.suffix.lower() == ".json"
        and len(rel_to_schemas.parts) > 1
        and rel_to_schemas.parts[0] in published_subdirs
    )


def discover_schema_assets(
    src_root: Path = SRC_ROOT,
    published_subdirs: Sequence[str] = PUBLISHED_JSONSCHEMA_SUBDIRS,
) -> list[SchemaAssetDoc]:
    """Discover all bundled non-Python files under ``src_root / "schemas"``.

    :param src_root: package source root directory
    :type src_root: Path
    :param published_subdirs: top-level schema subdirectories published as static JSON Schemas
    :type published_subdirs: Sequence[str]
    :return: sorted list of discovered non-Python schema asset descriptors
    :rtype: list[SchemaAssetDoc]
    """
    schemas_dir = src_root / SCHEMAS_SUBDIR
    if not schemas_dir.is_dir():
        return []

    assets: list[SchemaAssetDoc] = []
    for path in sorted(schemas_dir.rglob("*")):
        if not path.is_file():
            continue
        rel_to_schemas = path.relative_to(schemas_dir)
        if any(part in EXCLUDED_SCHEMA_DIR_NAMES or part.startswith(".") for part in rel_to_schemas.parts):
            continue
        if path.suffix.lower() in PYTHON_ARTIFACT_SUFFIXES:
            continue

        rel_path = path.relative_to(src_root)
        doc_rel_path = Path(SCHEMA_SOURCE_DOCS_SUBDIR) / rel_to_schemas.with_name(f"{rel_to_schemas.name}.md")
        kind, highlight_lang = resolve_schema_file_metadata(path)
        package_area = rel_to_schemas.parts[0] if len(rel_to_schemas.parts) > 1 else SCHEMAS_SUBDIR
        published_rel_path = (
            Path(SCHEMAS_SUBDIR) / rel_to_schemas
            if is_published_jsonschema(rel_to_schemas, published_subdirs)
            else None
        )
        assets.append(
            SchemaAssetDoc(
                rel_path=rel_path,
                doc_rel_path=doc_rel_path,
                kind=kind,
                highlight_lang=highlight_lang,
                package_area=package_area,
                published_rel_path=published_rel_path,
            )
        )
    return assets


def _format_asset_source_links(asset: SchemaAssetDoc, src_root: Path, repo_root: Path) -> str:
    """Format the ``Source`` table cell links for ``asset`` in ``docs/bundled_schemas.md``."""
    github_link = f"[GitHub]({repo_blob_url(src_root / asset.rel_path, repo_root)})"
    if asset.published_rel_path is None:
        return github_link
    return f"{github_link} · [Site JSON]({asset.published_rel_path.as_posix()})"


def render_schema_assets_page(
    assets: Sequence[SchemaAssetDoc] | None = None,
    src_root: Path = SRC_ROOT,
    repo_root: Path = REPO_ROOT,
) -> str:
    """Render the ``docs/bundled_schemas.md`` page cataloging all non-Python files in ``schemas/``.

    :param assets: optional sequence of discovered schema assets
    :type assets: Sequence[SchemaAssetDoc] | None
    :param src_root: package source root directory
    :type src_root: Path
    :param repo_root: repository root directory
    :type repo_root: Path
    :return: rendered Markdown document text
    :rtype: str
    """
    resolved_assets = list(assets) if assets is not None else discover_schema_assets(src_root)
    lines = [
        GENERATED_BANNER,
        "",
        "# Bundled Schemas & XSD Assets",
        "",
        (
            "Static non-Python schema assets bundled inside `src/cdm_data_loaders/schemas/` for validation and "
            "PyArrow schema generation. Select any schema path to open its syntax-highlighted source page."
        ),
        "",
        "| Relative Path (`src/cdm_data_loaders/`) | Format | Schema Group | Source |",
        "| :--- | :--- | :--- | :--- |",
    ]
    lines.extend(
        f"| [`{asset.rel_path.as_posix()}`]({asset.doc_rel_path.as_posix()}) | "
        f"{escape_table_cell(asset.kind)} | `{asset.package_area}` | "
        f"{_format_asset_source_links(asset, src_root, repo_root)} |"
        for asset in resolved_assets
    )
    lines.append("")
    return "\n".join(lines)


def render_schema_asset_detail_page(
    asset: SchemaAssetDoc,
    src_root: Path = SRC_ROOT,
    repo_root: Path = REPO_ROOT,
) -> str:
    """Render an individual ``docs/schema_source/<subpath>.md`` page embedding syntax-highlighted source via Snippets.

    :param asset: schema asset metadata
    :type asset: SchemaAssetDoc
    :param src_root: package source root directory
    :type src_root: Path
    :param repo_root: repository root directory
    :type repo_root: Path
    :return: rendered Markdown document text
    :rtype: str
    """
    source_path = src_root / asset.rel_path
    repo_rel = rel_posix(source_path, repo_root)
    short_title = (
        asset.rel_path.relative_to(SCHEMAS_SUBDIR).as_posix()
        if asset.rel_path.is_relative_to(SCHEMAS_SUBDIR)
        else asset.rel_path.as_posix()
    )
    source_text = source_path.read_text(encoding="utf-8") if source_path.is_file() else ""
    fence = code_fence_for_text(source_text)
    metadata_lines = [
        f"- **Repository path**: [`{repo_rel}`]({repo_blob_url(source_path, repo_root)})",
    ]
    if asset.published_rel_path is not None:
        pub_posix = asset.published_rel_path.as_posix()
        up_prefix = "../" * len(asset.doc_rel_path.parent.parts)
        metadata_lines.append(f"- **Published URL**: [`{SITE_URL_BASE}/{pub_posix}`]({up_prefix}{pub_posix})")
    metadata_lines.extend(
        [
            f"- **Format**: {asset.kind}",
            f"- **Schema group**: `{asset.package_area}`",
        ]
    )
    lines = [
        GENERATED_BANNER,
        "",
        f"# `{short_title}`",
        "",
        *metadata_lines,
        "",
        f'{fence}{asset.highlight_lang} linenums="1" title="{repo_rel}"',
        f'--8<-- "{repo_rel}"',
        fence,
        "",
    ]
    return "\n".join(lines)


def render_schemas_summary(
    assets: Sequence[SchemaAssetDoc] | None = None,
    src_root: Path = SRC_ROOT,
) -> str:
    """Render ``docs/schema_source/SUMMARY.md`` for Zensical ``literate-nav`` sidebar navigation.

    :param assets: optional sequence of discovered schema assets
    :type assets: Sequence[SchemaAssetDoc] | None
    :param src_root: package source root directory
    :type src_root: Path
    :return: rendered literate-navigation Markdown summary
    :rtype: str
    """
    resolved_assets = list(assets) if assets is not None else discover_schema_assets(src_root)
    lines: list[str] = [GENERATED_BANNER, ""]
    emitted_dirs: set[tuple[str, ...]] = set()

    for asset in resolved_assets:
        rel_to_schemas = (
            asset.rel_path.relative_to(SCHEMAS_SUBDIR)
            if asset.rel_path.is_relative_to(SCHEMAS_SUBDIR)
            else asset.rel_path
        )
        doc_rel_to_source_dir = (
            asset.doc_rel_path.relative_to(SCHEMA_SOURCE_DOCS_SUBDIR)
            if asset.doc_rel_path.is_relative_to(SCHEMA_SOURCE_DOCS_SUBDIR)
            else asset.doc_rel_path
        )
        dir_parts = rel_to_schemas.parent.parts
        for depth, dir_name in enumerate(dir_parts):
            prefix = dir_parts[: depth + 1]
            if prefix not in emitted_dirs:
                emitted_dirs.add(prefix)
                lines.append(f"{'    ' * depth}- {dir_name}")
        leaf_indent = "    " * len(dir_parts)
        lines.append(f"{leaf_indent}- [{rel_to_schemas.name}]({doc_rel_to_source_dir.as_posix()})")

    lines.append("")
    return "\n".join(lines)


def build_schema_doc_outputs(
    assets: Sequence[SchemaAssetDoc] | None = None,
    *,
    schemas_output: Path = BUNDLED_SCHEMAS_PATH,
    schema_source_dir: Path = SCHEMA_SOURCE_DOCS_DIR,
    published_schemas_dir: Path = PUBLISHED_SCHEMAS_DOCS_DIR,
    src_root: Path = SRC_ROOT,
) -> list[tuple[Path, str]]:
    """Build ``(output_path, rendered_text)`` pairs for the catalog, source pages, and published JSON Schemas.

    :param assets: optional sequence of discovered schema assets
    :type assets: Sequence[SchemaAssetDoc] | None
    :param schemas_output: output path for ``bundled_schemas.md``
    :type schemas_output: Path
    :param schema_source_dir: output directory for ``SUMMARY.md`` and per-schema ``.md`` source pages
    :type schema_source_dir: Path
    :param published_schemas_dir: output directory for static JSON Schemas served under ``/schemas/``
    :type published_schemas_dir: Path
    :param src_root: package source root directory (``<repo_root>/src/cdm_data_loaders``)
    :type src_root: Path
    :return: ordered list of ``(output_path, rendered_text)`` pairs
    :rtype: list[tuple[Path, str]]
    """
    repo_root = src_root.parent.parent
    resolved_assets = list(assets) if assets is not None else discover_schema_assets(src_root)
    outputs: list[tuple[Path, str]] = [
        (schemas_output, render_schema_assets_page(resolved_assets, src_root=src_root, repo_root=repo_root)),
        (schema_source_dir / SCHEMAS_SUMMARY_FILENAME, render_schemas_summary(resolved_assets, src_root=src_root)),
    ]
    for asset in resolved_assets:
        rel_doc = (
            asset.doc_rel_path.relative_to(SCHEMA_SOURCE_DOCS_SUBDIR)
            if asset.doc_rel_path.is_relative_to(SCHEMA_SOURCE_DOCS_SUBDIR)
            else asset.doc_rel_path
        )
        outputs.append(
            (
                schema_source_dir / rel_doc,
                render_schema_asset_detail_page(asset, src_root=src_root, repo_root=repo_root),
            )
        )
        if asset.published_rel_path is not None:
            rel_pub = (
                asset.published_rel_path.relative_to(SCHEMAS_SUBDIR)
                if asset.published_rel_path.is_relative_to(SCHEMAS_SUBDIR)
                else asset.published_rel_path
            )
            outputs.append((published_schemas_dir / rel_pub, (src_root / asset.rel_path).read_text(encoding="utf-8")))
    return outputs


def main(argv: Sequence[str] | None = None) -> int:
    """Generate or check CLI docs, schema source pages, and published JSON Schemas.

    :param argv: optional CLI argument sequence
    :type argv: Sequence[str] | None
    :return: exit code (``0`` on success, ``1`` if ``--check`` detects stale or orphaned files)
    :rtype: int
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail with exit code 1 if generated documentation files are out of date.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=CLI_REFERENCE_PATH,
        help="Output path for the generated CLI reference Markdown file.",
    )
    parser.add_argument(
        "--schemas-output",
        type=Path,
        default=BUNDLED_SCHEMAS_PATH,
        help="Output path for the generated bundled schemas Markdown file.",
    )
    parser.add_argument(
        "--schema-source-dir",
        type=Path,
        default=None,
        help="Output directory for generated per-schema Markdown pages and SUMMARY.md.",
    )
    parser.add_argument(
        "--published-schemas-dir",
        type=Path,
        default=None,
        help="Output directory for static JSON Schemas served under /schemas/.",
    )
    args = parser.parse_args(argv)
    base_docs_dir = args.schemas_output.parent
    schema_source_dir = (
        args.schema_source_dir if args.schema_source_dir is not None else base_docs_dir / SCHEMA_SOURCE_DOCS_SUBDIR
    )
    published_schemas_dir = (
        args.published_schemas_dir if args.published_schemas_dir is not None else base_docs_dir / SCHEMAS_SUBDIR
    )

    return check_or_write_files(
        [
            (args.output, render_cli_reference()),
            *build_schema_doc_outputs(
                schemas_output=args.schemas_output,
                schema_source_dir=schema_source_dir,
                published_schemas_dir=published_schemas_dir,
            ),
        ],
        check=args.check,
        repo_root=REPO_ROOT,
        managed_dirs=(schema_source_dir, published_schemas_dir),
    )


if __name__ == "__main__":
    sys.exit(main())
