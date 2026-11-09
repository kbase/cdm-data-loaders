"""Generate CLI reference and MkDocs API documentation from codebase metadata.

Usage::

    uv run python -m scripts.gen_docs                          # write docs/cli_reference.md
    uv run python -m scripts.gen_docs --check                  # exit 1 if docs/cli_reference.md is stale
    uv run python -m scripts.gen_docs --mkdocs-api-dir <dir>   # write MkDocs API reference tree into <dir>
"""

import argparse
import ast
import importlib
import os
import sys
import tomllib
import types
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePath
from typing import Annotated, Literal, get_args, get_origin

from pydantic import AliasChoices
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from cdm_data_loaders.core.settings import CdmDataLoadersBase

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"
SRC_ROOT = REPO_ROOT / "src" / "cdm_data_loaders"
CLI_REFERENCE_PATH = REPO_ROOT / "docs" / "cli_reference.md"

GENERATED_BANNER = (
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
    """Documentation metadata for a bundled JSON Schema or XSD file under ``src/cdm_data_loaders/``."""

    rel_path: Path
    kind: str
    package_area: str


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


def _format_default_value(field_info: FieldInfo, repo_root: Path = REPO_ROOT) -> tuple[str, bool]:
    """Return ``(default_label, required)`` for a Pydantic ``FieldInfo``."""
    if field_info.is_required():
        return ("*(Required)*", True)

    default = field_info.default
    if default is PydanticUndefined:
        label = "*(Dynamic)*"
    elif default is None:
        label = "`None`"
    elif isinstance(default, Enum):
        label = f"`{default.value!r}`"
    elif isinstance(default, Path | PurePath):
        rel = (
            default.relative_to(repo_root)
            if isinstance(default, Path) and default.is_relative_to(repo_root)
            else default
        )
        label = f"`{rel.as_posix()}`"
    else:
        label = f"`{default!r}`"
    return (label, False)


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

    inheritance_chain = tuple(
        cls.__name__ for cls in settings_cls.__mro__ if isinstance(cls, type) and issubclass(cls, CdmDataLoadersBase)
    )

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


def rel_posix(path: Path, start: Path = REPO_ROOT) -> str:
    """Return a POSIX relative path string from ``start`` to ``path``.

    :param path: target path
    :type path: Path
    :param start: base directory path
    :type start: Path
    :return: POSIX relative path string
    :rtype: str
    """
    return Path(os.path.relpath(path, start)).as_posix()


def escape_table_cell(text: str) -> str:
    """Escape pipe characters for safe inclusion inside a Markdown table cell.

    :param text: raw cell text
    :type text: str
    :return: Markdown-escaped cell text
    :rtype: str
    """
    return text.replace("|", r"\|")


_escape_table_cell = escape_table_cell


def check_or_write_files(
    outputs: Iterable[tuple[Path, str]],
    *,
    check: bool,
    repo_root: Path = REPO_ROOT,
) -> int:
    """Compare each ``(output_path, rendered_text)`` pair against disk and either report stale files or write updates.

    :param outputs: iterable of ``(output_path, rendered_text)`` pairs
    :type outputs: Iterable[tuple[Path, str]]
    :param check: when ``True``, report stale files to ``sys.stderr`` and return ``1`` without writing
    :type check: bool
    :param repo_root: repository root used to format relative paths on ``stderr``
    :type repo_root: Path
    :return: ``0`` if all files are up to date (or written), ``1`` if ``check`` is ``True`` and any file is stale
    :rtype: int
    """
    stale: list[Path] = []
    for output_path, text in outputs:
        current = output_path.read_text(encoding="utf-8") if output_path.exists() else None
        if current == text:
            continue
        if check:
            stale.append(output_path)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(text, encoding="utf-8")
    for path in stale:
        rel_out = (
            path.relative_to(repo_root).as_posix() if path.is_relative_to(repo_root) else rel_posix(path, repo_root)
        )
        sys.stderr.write(f"stale: {rel_out}\n")
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
            f"`{entry.entry_point}` | {_escape_table_cell(entry.summary)} |"
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
            type_cell = _escape_table_cell(f"`{field_doc.type_label}`")
            default_cell = _escape_table_cell(field_doc.default_label)
            desc_cell = _escape_table_cell(field_doc.description or "—")
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


def discover_schema_assets(src_root: Path = SRC_ROOT) -> list[SchemaAssetDoc]:
    """Discover all bundled JSON Schema (``.json``) and XML Schema (``.xsd``) files under ``src_root``."""
    assets: list[SchemaAssetDoc] = []
    for path in sorted(src_root.rglob("*")):
        if not path.is_file() or path.suffix not in {".json", ".xsd"}:
            continue
        rel_path = path.relative_to(src_root)
        kind = "JSON Schema" if path.suffix == ".json" else "XML Schema (XSD)"
        package_area = rel_path.parts[0] if len(rel_path.parts) > 1 else "root"
        assets.append(SchemaAssetDoc(rel_path=rel_path, kind=kind, package_area=package_area))
    return assets


def render_schema_assets_page(assets: Sequence[SchemaAssetDoc]) -> str:
    """Render the ``api/schemas.md`` page cataloging bundled JSON Schemas and XSDs."""
    lines = [
        "# Bundled Schemas & XSD Assets",
        "",
        "Static schema assets bundled inside `src/cdm_data_loaders/` for validation and PyArrow schema generation.",
        "",
        "| Relative Path (`src/cdm_data_loaders/`) | Format | Package Area |",
        "| :--- | :--- | :--- |",
    ]
    lines.extend(f"| `{asset.rel_path.as_posix()}` | {asset.kind} | `{asset.package_area}` |" for asset in assets)
    lines.append("")
    return "\n".join(lines)


def write_mkdocs_api_tree(
    src_root: Path = SRC_ROOT,
    writer: Callable[[Path, str], None] | None = None,
    output_dir: Path | None = None,
) -> list[Path]:
    """Write the MkDocs API reference tree using either ``output_dir`` or a custom ``writer`` callback."""
    if writer is None:
        if output_dir is None:
            msg = "Either writer or output_dir must be provided"
            raise ValueError(msg)

        def _fs_writer(rel_path: Path, content: str) -> None:
            target = output_dir / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")

        writer = _fs_writer

    modules = discover_api_modules(src_root)
    assets = discover_schema_assets(src_root)
    written: list[Path] = []
    summary_lines: list[str] = ["# API Reference", ""]
    emitted_prefixes: set[tuple[str, ...]] = set()

    for mod in modules:
        for depth in range(1, len(mod.nav_parts)):
            prefix = mod.nav_parts[:depth]
            if prefix not in emitted_prefixes:
                prefix_indent = "    " * (depth - 1)
                prefix_label = prefix[-1] if depth > 1 else prefix[0]
                summary_lines.append(f"{prefix_indent}* {prefix_label}")
                emitted_prefixes.add(prefix)

        page_path = Path("api") / mod.doc_rel_path
        indent = "    " * (len(mod.nav_parts) - 1)
        label = mod.nav_parts[-1] if len(mod.nav_parts) > 1 else mod.module_path
        summary_lines.append(f"{indent}* [`{label}`]({mod.doc_rel_path.as_posix()})")
        emitted_prefixes.add(mod.nav_parts)

        page_lines = [
            f"# `{mod.module_path}`",
            "",
            f"::: {mod.module_path}",
            "",
        ]
        writer(page_path, "\n".join(page_lines))
        written.append(page_path)

    schemas_rel = Path("schemas.md")
    schemas_page = Path("api") / schemas_rel
    summary_lines.append(f"* [Bundled Schemas & XSDs]({schemas_rel.as_posix()})")
    writer(schemas_page, render_schema_assets_page(assets))
    written.append(schemas_page)

    summary_path = Path("api") / "SUMMARY.md"
    writer(summary_path, "\n".join(summary_lines) + "\n")
    written.append(summary_path)

    return written


def _run_mkdocs_gen_files() -> None:
    """Entry hook invoked when ``mkdocs-gen-files`` executes this script during ``mkdocs build``."""
    import mkdocs_gen_files  # noqa: PLC0415

    def _mkdocs_writer(rel_path: Path, content: str) -> None:
        with mkdocs_gen_files.open(rel_path.as_posix(), "w") as fh:
            fh.write(content)

    write_mkdocs_api_tree(src_root=SRC_ROOT, writer=_mkdocs_writer)


def main(argv: Sequence[str] | None = None) -> int:
    """Generate or verify ``docs/cli_reference.md`` and optional MkDocs API reference pages."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail with exit code 1 if docs/cli_reference.md is out of date.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=CLI_REFERENCE_PATH,
        help="Output path for the generated CLI reference Markdown file.",
    )
    parser.add_argument(
        "--mkdocs-api-dir",
        type=Path,
        default=None,
        help="Optional directory into which to write the MkDocs API reference tree.",
    )
    args = parser.parse_args(argv)

    if args.mkdocs_api_dir is not None:
        write_mkdocs_api_tree(src_root=SRC_ROOT, output_dir=args.mkdocs_api_dir)

    rendered = render_cli_reference()
    output_path: Path = args.output
    return check_or_write_files([(output_path, rendered)], check=args.check, repo_root=REPO_ROOT)


if __name__ == "__main__":
    sys.exit(main())
elif __name__ == "<run_path>":
    _run_mkdocs_gen_files()
