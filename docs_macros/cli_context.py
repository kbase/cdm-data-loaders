"""CLI entry-point and Pydantic settings introspection builders for CLI reference documentation."""

import importlib
import types
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal, get_args, get_origin

from pydantic import AliasChoices, BeforeValidator, computed_field, model_validator
from pydantic.fields import FieldInfo

from cdm_data_loaders.core.pipeline_catalog import load_project_scripts
from cdm_data_loaders.core.settings import CdmDataLoadersBase
from docs_macros.constants import GITHUB_BLOB_BASE, PYPROJECT_PATH, REPO_ROOT, SRC_ROOT
from docs_macros.formatting import FrozenDocModel, ModuleApiUrl, format_value_code


def _format_origin_annotation(origin: object, args: tuple[object, ...]) -> str:
    """Format a generic or union origin type with its type arguments.

    :param origin: Generic origin object from ``typing.get_origin``.
    :type origin: object
    :param args: Type arguments tuple from ``typing.get_args``.
    :type args: tuple[object, ...]
    :return: Concise type label string.
    :rtype: str
    """
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
    """Render a concise human-readable type label for a field annotation.

    :param annotation: Field type annotation object.
    :type annotation: object
    :return: Human-readable type label.
    :rtype: str
    """
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


def coerce_type_annotation(value: object) -> str:
    """Coerce a Python type annotation object or string into a human-readable type label.

    :param value: Pre-formatted type label string or Python annotation object.
    :type value: object
    :return: Human-readable type label.
    :rtype: str
    """
    if isinstance(value, str):
        return value
    return _format_type_annotation(value)


def coerce_field_description(value: object) -> str:
    """Coerce a Pydantic field description (``str | None``) into a single-line string.

    :param value: Raw field description or ``None``.
    :type value: object
    :return: Single-line trimmed description string.
    :rtype: str
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip().replace("\n", " ")
    msg = f"Expected str or None, got {type(value).__name__}"
    raise TypeError(msg)


def coerce_shortcut_flags(value: object) -> tuple[str, ...]:
    """Coerce raw shortcut names or pre-formatted flags into a deduplicated tuple of CLI flags.

    :param value: Sequence of shortcut names or CLI flags.
    :type value: object
    :return: Tuple of deduplicated ``-x`` / ``--xx`` CLI flag strings.
    :rtype: tuple[str, ...]
    """
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        msg = f"Expected sequence of shortcut strings, got {type(value).__name__}"
        raise TypeError(msg)
    formatted: list[str] = []
    for item in value:
        raw = str(item)
        flag = raw if raw.startswith("-") else f"-{raw}" if len(raw) == 1 else f"--{raw}"
        if flag not in formatted:
            formatted.append(flag)
    return tuple(formatted)


type TypeLabel = Annotated[str, BeforeValidator(coerce_type_annotation)]
type FieldDescription = Annotated[str, BeforeValidator(coerce_field_description)]
type ShortcutFlags = Annotated[tuple[str, ...], BeforeValidator(coerce_shortcut_flags)]


class CliFieldDoc(FrozenDocModel):
    """Documentation metadata for a single Pydantic CLI setting field."""

    name: str
    shortcuts: ShortcutFlags
    env_var: str
    type_label: TypeLabel
    default_label: str
    required: bool
    description: FieldDescription

    @computed_field
    @property
    def cli_flag(self) -> str:
        """Canonical kebab-case ``--*`` CLI flag derived from ``name``.

        :return: CLI flag string.
        :rtype: str
        """
        return f"--{self.name.replace('_', '-')}"


class CliEntryDoc(FrozenDocModel):
    """Documentation metadata for a registered ``[project.scripts]`` CLI entry point."""

    cli_name: str
    entry_point: str
    entry_api_url: ModuleApiUrl = None
    settings_class_name: str
    settings_api_url: ModuleApiUrl = None
    summary: str
    fields: tuple[CliFieldDoc, ...]

    @model_validator(mode="before")
    @classmethod
    def _populate_default_entry_api_url(cls, data: object) -> object:
        """Default ``entry_api_url`` from ``entry_point`` when omitted.

        :param data: Raw input mapping or model data.
        :type data: object
        :return: Input data with ``entry_api_url`` populated when omitted.
        :rtype: object
        """
        if isinstance(data, dict) and "entry_api_url" not in data and "entry_point" in data:
            return {**data, "entry_api_url": data["entry_point"]}
        return data

    @computed_field
    @property
    def module_name(self) -> str:
        """Dotted Python module path extracted from ``entry_point``.

        :return: Dotted module path.
        :rtype: str
        """
        return self.entry_point.split(":", maxsplit=1)[0]


class CliReferencePageContext(FrozenDocModel):
    """View model passed to ``pages/cli_reference.md.j2``."""

    entries: tuple[CliEntryDoc, ...]
    pyproject_url: str = f"{GITHUB_BLOB_BASE}/pyproject.toml"
    entrypoint_url: str = f"{GITHUB_BLOB_BASE}/scripts/entrypoint.sh"


def _format_default_value(field_info: FieldInfo, repo_root: Path = REPO_ROOT) -> tuple[str, bool]:
    """Return ``(default_label, required)`` for a Pydantic ``FieldInfo``.

    :param field_info: Pydantic field metadata descriptor.
    :type field_info: FieldInfo
    :param repo_root: Repository root used to relativize ``Path`` defaults.
    :type repo_root: Path
    :return: Formatted default value label and boolean required flag.
    :rtype: tuple[str, bool]
    """
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
    """Extract raw CLI shortcut names (e.g. ``d``, ``dev``) for ``field_name``.

    :param field_name: Python field name on the settings model.
    :type field_name: str
    :param field_info: Pydantic field metadata descriptor.
    :type field_info: FieldInfo
    :param cli_shortcuts: Shortcut mapping from ``model_config['cli_shortcuts']``.
    :type cli_shortcuts: dict[str, str | list[str]]
    :return: Tuple of raw CLI shortcut name strings.
    :rtype: tuple[str, ...]
    """
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

    return tuple(raw_shortcuts)


def first_docstring_line(docstring: str | None, fallback: str) -> str:
    """Return the cleaned first summary line of a module or class docstring.

    :param docstring: Raw docstring or ``None``.
    :type docstring: str | None
    :param fallback: Fallback summary when ``docstring`` is empty or blank.
    :type fallback: str
    :return: Single-line docstring summary.
    :rtype: str
    """
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
    """Introspect a ``CdmDataLoadersBase`` subclass into a ``CliEntryDoc``.

    :param cli_name: Console script CLI command name.
    :type cli_name: str
    :param entry_point: ``module:attr`` entry point string from ``pyproject.toml``.
    :type entry_point: str
    :param settings_cls: Concrete ``CdmDataLoadersBase`` subclass bound to the CLI.
    :type settings_cls: type[CdmDataLoadersBase]
    :param summary: One-line summary description for the CLI.
    :type summary: str
    :param repo_root: Repository root directory for relativizing ``Path`` defaults.
    :type repo_root: Path
    :return: Populated ``CliEntryDoc`` descriptor.
    :rtype: CliEntryDoc
    """
    env_prefix = str(settings_cls.model_config.get("env_prefix", "CDL_"))
    raw_shortcuts = settings_cls.model_config.get("cli_shortcuts")
    cli_shortcuts: dict[str, str | list[str]] = dict(raw_shortcuts) if isinstance(raw_shortcuts, dict) else {}

    src_root = SRC_ROOT if repo_root == REPO_ROOT else repo_root / "src" / SRC_ROOT.name

    fields: list[CliFieldDoc] = []
    for field_name, field_info in settings_cls.model_fields.items():
        default_label, required = _format_default_value(field_info, repo_root=repo_root)
        fields.append(
            CliFieldDoc.model_validate(
                {
                    "name": field_name,
                    "shortcuts": _extract_shortcuts(field_name, field_info, cli_shortcuts),
                    "env_var": f"{env_prefix}{field_name.upper()}",
                    "type_label": field_info.annotation,
                    "default_label": default_label,
                    "required": required,
                    "description": field_info.description,
                }
            )
        )

    return CliEntryDoc.model_validate(
        {
            "cli_name": cli_name,
            "entry_point": entry_point,
            "settings_class_name": settings_cls.__name__,
            "settings_api_url": settings_cls,
            "summary": summary,
            "fields": tuple(fields),
        },
        context={"src_root": src_root},
    )


def load_cli_entries(pyproject_path: Path = PYPROJECT_PATH, repo_root: Path = REPO_ROOT) -> list[CliEntryDoc]:
    """Load and introspect all ``[project.scripts]`` CLI entry points from ``pyproject.toml``.

    :param pyproject_path: Path to ``pyproject.toml``.
    :type pyproject_path: Path
    :param repo_root: Repository root directory.
    :type repo_root: Path
    :return: Sorted list of ``CliEntryDoc`` descriptors.
    :rtype: list[CliEntryDoc]
    """
    scripts = load_project_scripts(pyproject_path)
    entries: list[CliEntryDoc] = []

    for cli_name, entry_point in scripts.items():
        module_name, attr_name = entry_point.split(":", maxsplit=1)
        module = importlib.import_module(module_name)
        cli_obj = getattr(module, attr_name)
        settings_cls = getattr(cli_obj, "settings_cls", None)
        if not isinstance(settings_cls, type) or not issubclass(settings_cls, CdmDataLoadersBase):
            msg = f"Entry point {cli_name} ({entry_point}) does not expose a CdmDataLoadersBase settings_cls"
            raise TypeError(msg)

        summary = first_docstring_line(
            settings_cls.__doc__ or module.__doc__,
            fallback=f"CLI pipeline for {cli_name}.",
        )
        entries.append(inspect_settings_class(cli_name, entry_point, settings_cls, summary, repo_root=repo_root))

    return entries


def build_cli_reference_context(entries: Sequence[CliEntryDoc] | None = None) -> CliReferencePageContext:
    """Build the ``CliReferencePageContext`` view model for ``pages/cli_reference.md.j2``.

    :param entries: Optional pre-loaded CLI entry descriptors (introspected when ``None``).
    :type entries: Sequence[CliEntryDoc] | None
    :return: Populated ``CliReferencePageContext``.
    :rtype: CliReferencePageContext
    """
    resolved = tuple(entries) if entries is not None else tuple(load_cli_entries())
    return CliReferencePageContext(entries=resolved)
