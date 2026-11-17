"""Declarative console-script and pipeline entry-point catalog shared by docs and tests."""

import sys
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path
from types import ModuleType
from typing import Any, Final

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.settings import LoggerSettings

PACKAGE_DIST_NAME: Final[str] = "cdm-data-loaders"
DEFAULT_PYPROJECT_PATH: Final[Path] = Path(__file__).resolve().parents[3] / "pyproject.toml"


@dataclass(frozen=True, slots=True)
class PipelineSpec:
    """Resolved metadata for executing or inspecting a registered CLI or pipeline entry point.

    :param cli_command: console-script command name
    :type cli_command: str
    :param module_path: dotted Python module path defining the entry point
    :type module_path: str
    :param settings_cls: settings class bound to the entry point, if any
    :type settings_cls: type[LoggerSettings] | None
    :param pipeline_fn: underlying pipeline or action callable, if any
    :type pipeline_fn: Callable[[Any], Any] | None
    :param cli_callable: console-script callable, if any
    :type cli_callable: Callable[[], Any] | None
    """

    cli_command: str
    module_path: str
    settings_cls: type[LoggerSettings] | None = None
    pipeline_fn: Callable[[Any], Any] | None = None
    cli_callable: Callable[[], Any] | None = None


type PipelineIdentifier = str | ModuleType | CliRunner[Any, Any] | PipelineSpec | Callable[..., Any]


def load_project_scripts(pyproject_path: Path | None = None) -> dict[str, str]:
    """Load ``[project.scripts]`` mappings from ``pyproject.toml`` or installed package metadata.

    :param pyproject_path: optional explicit ``pyproject.toml`` path; defaults to repo root when present
    :type pyproject_path: Path | None
    :return: mapping of ``cli_command`` to ``module.path:attr_name``
    :rtype: dict[str, str]
    """
    resolved_toml = pyproject_path if pyproject_path is not None else DEFAULT_PYPROJECT_PATH
    if pyproject_path is not None or resolved_toml.is_file():
        with resolved_toml.open("rb") as fh:
            data = tomllib.load(fh)
        scripts = data.get("project", {}).get("scripts", {})
        return {str(name): str(spec) for name, spec in sorted(scripts.items())}

    discovered: dict[str, str] = {}
    for ep in entry_points(group="console_scripts"):
        if ep.dist is not None and ep.dist.name == PACKAGE_DIST_NAME:
            discovered[ep.name] = ep.value
    return dict(sorted(discovered.items()))


def discover_pipeline_targets(pyproject_path: Path | None = None) -> dict[str, tuple[str, str]]:
    """Return ``{cli_command: (cli_command, module_path)}`` for all registered console scripts.

    :param pyproject_path: optional explicit ``pyproject.toml`` path
    :type pyproject_path: Path | None
    :return: mapping from command name to ``(cli_command, module_path)``
    :rtype: dict[str, tuple[str, str]]
    """
    scripts = load_project_scripts(pyproject_path)
    return {cli_name: (cli_name, spec.split(":", maxsplit=1)[0]) for cli_name, spec in scripts.items()}


def _build_module_to_cli_map(scripts: Mapping[str, str]) -> dict[str, str]:
    """Map each module path in ``scripts`` to its primary ``cli_command`` (preferring ``:cli``).

    :param scripts: ``cli_command -> module.path:attr_name`` mapping
    :type scripts: Mapping[str, str]
    :return: mapping of ``module_path`` to primary ``cli_command``
    :rtype: dict[str, str]
    """
    module_to_cli: dict[str, str] = {}
    for cli_name, spec in sorted(scripts.items()):
        module_path, _, attr_name = spec.partition(":")
        if attr_name == "cli" or module_path not in module_to_cli:
            module_to_cli[module_path] = cli_name
    return module_to_cli


def _spec_from_module(
    cli_command: str,
    module: ModuleType,
    attr_name: str | None = None,
) -> PipelineSpec:
    """Build a :class:`PipelineSpec` by inspecting ``module`` for ``cli_command``.

    :param cli_command: console-script command name
    :type cli_command: str
    :param module: imported Python module object
    :type module: ModuleType
    :param attr_name: explicit attribute name from ``[project.scripts]``, if known
    :type attr_name: str | None
    :return: populated :class:`PipelineSpec`
    :rtype: PipelineSpec
    """
    cli_attr = (
        getattr(module, attr_name, None)
        if attr_name is not None
        else (getattr(module, f"{cli_command}_cli", None) or getattr(module, "cli", None))
    )
    if isinstance(cli_attr, CliRunner):
        return PipelineSpec(
            cli_command=cli_command,
            module_path=module.__name__,
            settings_cls=cli_attr.settings_cls,
            pipeline_fn=cli_attr.action_fn,
            cli_callable=cli_attr,
        )
    return PipelineSpec(
        cli_command=cli_command,
        module_path=module.__name__,
        settings_cls=getattr(module, "Settings", None),
        pipeline_fn=getattr(module, "run_pipeline", None),
        cli_callable=cli_attr if callable(cli_attr) else None,
    )


def _resolve_runner_spec(
    runner: CliRunner[Any, Any],
    scripts: Mapping[str, str],
    module_to_cli: Mapping[str, str],
) -> PipelineSpec:
    """Resolve a :class:`CliRunner` instance into a :class:`PipelineSpec`.

    :param runner: ``CliRunner`` entry-point instance
    :type runner: CliRunner[Any, Any]
    :param scripts: registered ``[project.scripts]`` mapping
    :type scripts: Mapping[str, str]
    :param module_to_cli: module-to-primary-CLI mapping
    :type module_to_cli: Mapping[str, str]
    :return: resolved :class:`PipelineSpec`
    :rtype: PipelineSpec
    """
    prog_name = runner.settings_cls.model_config.get("cli_prog_name")
    if prog_name and str(prog_name) in scripts:
        cli_command = str(prog_name)
        module_path = scripts[cli_command].split(":", maxsplit=1)[0]
    else:
        module_path = runner.action_fn.__module__
        cli_command = module_to_cli.get(
            module_path,
            str(prog_name) if prog_name else module_path.rsplit(".", maxsplit=1)[-1],
        )
    return PipelineSpec(
        cli_command=cli_command,
        module_path=module_path,
        settings_cls=runner.settings_cls,
        pipeline_fn=runner.action_fn,
        cli_callable=runner,
    )


def _resolve_string_spec(
    identifier: str,
    scripts: Mapping[str, str],
    module_to_cli: Mapping[str, str],
) -> PipelineSpec | None:
    """Resolve a string command name or module path into a :class:`PipelineSpec` when registered.

    :param identifier: command name, short pipeline name, or dotted module path
    :type identifier: str
    :param scripts: registered ``[project.scripts]`` mapping
    :type scripts: Mapping[str, str]
    :param module_to_cli: module-to-primary-CLI mapping
    :type module_to_cli: Mapping[str, str]
    :return: resolved :class:`PipelineSpec` or ``None`` if unknown
    :rtype: PipelineSpec | None
    """
    stripped = identifier.strip()
    if stripped in scripts:
        module_path, _, attr_name = scripts[stripped].partition(":")
        return _spec_from_module(stripped, import_module(module_path), attr_name or None)

    normalized = stripped.removeprefix("cdm_data_loaders.pipelines.")
    if normalized in scripts:
        module_path, _, attr_name = scripts[normalized].partition(":")
        return _spec_from_module(normalized, import_module(module_path), attr_name or None)

    for candidate_module in (stripped, f"cdm_data_loaders.pipelines.{normalized}"):
        if candidate_module in module_to_cli:
            cli_command = module_to_cli[candidate_module]
            _, _, attr_name = scripts[cli_command].partition(":")
            return _spec_from_module(cli_command, import_module(candidate_module), attr_name or None)

    return None


def _resolve_module_spec(
    module: ModuleType,
    scripts: Mapping[str, str],
    module_to_cli: Mapping[str, str],
) -> PipelineSpec:
    """Resolve a Python module object into a :class:`PipelineSpec`.

    :param module: imported pipeline or CLI module
    :type module: ModuleType
    :param scripts: registered ``[project.scripts]`` mapping
    :type scripts: Mapping[str, str]
    :param module_to_cli: module-to-primary-CLI mapping
    :type module_to_cli: Mapping[str, str]
    :return: resolved :class:`PipelineSpec`
    :rtype: PipelineSpec
    """
    module_path = module.__name__
    if module_path in module_to_cli:
        cli_command = module_to_cli[module_path]
        _, _, attr_name = scripts[cli_command].partition(":")
        return _spec_from_module(cli_command, module, attr_name or None)
    cli_attr = getattr(module, "cli", None)
    prog_name = cli_attr.settings_cls.model_config.get("cli_prog_name") if isinstance(cli_attr, CliRunner) else None
    short_key = module_path.removeprefix("cdm_data_loaders.pipelines.")
    cli_command = str(prog_name) if prog_name else short_key.rsplit(".", maxsplit=1)[-1]
    return _spec_from_module(cli_command, module)


def _resolve_callable_spec(
    fn: Callable[..., Any],
    scripts: Mapping[str, str],
    module_to_cli: Mapping[str, str],
) -> PipelineSpec:
    """Resolve an action or pipeline callable into a :class:`PipelineSpec`.

    :param fn: action or pipeline callable
    :type fn: Callable[..., Any]
    :param scripts: registered ``[project.scripts]`` mapping
    :type scripts: Mapping[str, str]
    :param module_to_cli: module-to-primary-CLI mapping
    :type module_to_cli: Mapping[str, str]
    :raises ValueError: if ``fn`` cannot be mapped to a registered or module-level entry point
    :return: resolved :class:`PipelineSpec`
    :rtype: PipelineSpec
    """
    module_path = getattr(fn, "__module__", "")
    module = (sys.modules.get(module_path) or import_module(module_path)) if module_path else None
    if module is not None:
        if module_path in module_to_cli:
            cli_command = module_to_cli[module_path]
            _, _, attr_name = scripts[cli_command].partition(":")
            return _spec_from_module(cli_command, module, attr_name or None)
        cli_attr = getattr(module, "cli", None)
        if isinstance(cli_attr, CliRunner) and cli_attr.action_fn is fn:
            return _resolve_runner_spec(cli_attr, scripts, module_to_cli)
    err_msg = f"Cannot resolve pipeline from callable {fn!r} (module {module_path!r})"
    raise ValueError(err_msg)


def resolve_pipeline(
    pipeline: PipelineIdentifier,
    *,
    pyproject_path: Path | None = None,
) -> PipelineSpec:
    """Resolve a command name, module, ``CliRunner``, ``PipelineSpec``, or callable into a ``PipelineSpec``.

    :param pipeline: pipeline or CLI identifier
    :type pipeline: PipelineIdentifier
    :param pyproject_path: optional explicit ``pyproject.toml`` path
    :type pyproject_path: Path | None
    :raises ValueError: if ``pipeline`` cannot be resolved
    :return: resolved :class:`PipelineSpec`
    :rtype: PipelineSpec
    """
    if isinstance(pipeline, PipelineSpec):
        return pipeline

    scripts = load_project_scripts(pyproject_path)
    module_to_cli = _build_module_to_cli_map(scripts)

    if isinstance(pipeline, CliRunner):
        return _resolve_runner_spec(pipeline, scripts, module_to_cli)
    if isinstance(pipeline, ModuleType):
        return _resolve_module_spec(pipeline, scripts, module_to_cli)
    if callable(pipeline):
        return _resolve_callable_spec(pipeline, scripts, module_to_cli)
    if isinstance(pipeline, str):
        resolved = _resolve_string_spec(pipeline, scripts, module_to_cli)
        if resolved is not None:
            return resolved

    err_msg = f"Unknown pipeline {pipeline!r}. Known pipelines: {', '.join(sorted(scripts))}"
    raise ValueError(err_msg)
