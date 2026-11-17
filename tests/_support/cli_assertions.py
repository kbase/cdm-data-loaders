"""Shared CLI argument parsing, settings comparison, and temporary module helpers for tests."""

import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Final, cast
from uuid import uuid4

import pytest
import yaml
from frozendict import frozendict
from pydantic_settings import BaseSettings, CliApp, CliSettingsSource
from pyspark.sql import Row

from cdm_data_loaders.core.fields import (
    BUFFER_SIZE,
    DEFAULTS,
    DLT_DEV_MODE,
    FILE_LISTING_FILE_NAME,
    LOG_CONFIG_FILE,
    LOG_INTERVAL,
    PIPELINE_DIR_NAME,
    REPORT_FILE_NAME,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
)
from cdm_data_loaders.core.settings import CdmDataLoadersBase, CtsSettings
from tests._support.paths_and_constants import LOCAL_FS

HOOK_ENV_VAR: Final[str] = "RUNTIME__SLACK_INCOMING_HOOK"
HOOK_PART_ENV_VARS: Final[tuple[str, ...]] = ("VARIABLE_B", "VARIABLE_T", "CHAR_STR")
SLACK_HOOK: Final[str] = "https://slack.hook/what/ever"
LOAD_INFO_SAVE_FAILED: Final[str] = "Loading pipeline load info failed: "
LOAD_INFO_SAVE_FAILED_JOBS: Final[str] = "Loading pipeline load info had failed jobs: "

INIT: Final[str] = "init"
ENV: Final[str] = "env"
CLI: Final[str] = "cli"
SETTINGS_SOURCES: Final[tuple[str, ...]] = (INIT, ENV, CLI)

type SettingsFactory = Callable[[type[BaseSettings], str, Mapping[str, str]], BaseSettings]


def make_cli_arg(arg: str) -> str:
    """Format a settings field name as its CLI option flag.

    :param arg: Field name or single-character shortcut.
    :type arg: str
    :return: ``--kebab-case`` or ``-x`` CLI flag string.
    :rtype: str
    """
    return f"{'--' if len(arg) > 1 else '-'}{arg.replace('_', '-')}"


def check_settings(
    settings_object: BaseSettings,
    expected: dict[str, Any] | frozendict[str, Any],
) -> None:
    """Assert that ``settings_object.model_dump()`` equals ``expected``.

    :param settings_object: Instantiated ``BaseSettings`` object.
    :type settings_object: BaseSettings
    :param expected: Expected dumped field dictionary.
    :type expected: dict[str, Any] | frozendict[str, Any]
    """
    assert settings_object.model_dump() == expected


def local_io_expected(input_dir: Path | str, output_dir: Path) -> dict[str, Any]:
    """Build expected ``model_dump`` entries for ``InputLocalOutputSettings`` with default logging.

    :param input_dir: Local input directory ``Path`` or remote URL string.
    :type input_dir: Path | str
    :param output_dir: Local output directory ``Path``.
    :type output_dir: Path
    :return: Expected base IO settings dictionary.
    :rtype: dict[str, Any]
    """
    resolved_input = str(input_dir.resolve()) if isinstance(input_dir, Path) else input_dir
    return {
        "log_config_file": DEFAULTS[LOG_CONFIG_FILE],
        "input_dir": resolved_input,
        "output_dir": output_dir.resolve(),
    }


def cts_expected(input_dir: Path | str, output_dir: Path, **overrides: object) -> dict[str, Any]:
    """Build expected ``model_dump`` dictionary for ``CtsSettings`` with optional overrides.

    :param input_dir: Local input directory ``Path`` or remote URL string.
    :type input_dir: Path | str
    :param output_dir: Local output directory ``Path``.
    :type output_dir: Path
    :param overrides: Field overrides merged into the expected dictionary.
    :type overrides: object
    :return: Expected ``CtsSettings`` dictionary.
    :rtype: dict[str, Any]
    """
    resolved_output_dir = output_dir.resolve()
    use_pipeline_dir = bool(
        overrides.get(USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, DEFAULTS[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA])
    )
    default_pipeline_dir = resolved_output_dir / PIPELINE_DIR_NAME if use_pipeline_dir else None
    pipeline_dir = cast("Path | None", overrides.get("pipeline_dir", default_pipeline_dir))
    return {
        **local_io_expected(input_dir, output_dir),
        BUFFER_SIZE: DEFAULTS[BUFFER_SIZE],
        DLT_DEV_MODE: DEFAULTS[DLT_DEV_MODE],
        LOG_INTERVAL: DEFAULTS[LOG_INTERVAL],
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: DEFAULTS[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA],
        "pipeline_dir": pipeline_dir,
        "report_path": (pipeline_dir or resolved_output_dir) / REPORT_FILE_NAME,
        "file_listing_path": (pipeline_dir or resolved_output_dir) / FILE_LISTING_FILE_NAME,
        **overrides,
    }


def make_cts_settings[S: CtsSettings](
    settings_cls: type[S],
    base_dir: Path,
    **overrides: object,
) -> S:
    """Create standard input/output directories and log config under ``base_dir`` and instantiate ``settings_cls``.

    :param settings_cls: Concrete ``CtsSettings`` subclass to construct.
    :type settings_cls: type[S]
    :param base_dir: Parent directory for default ``input/``, ``output/``, and ``logging.json``.
    :type base_dir: Path
    :param overrides: Keyword arguments overriding the default settings fields.
    :type overrides: object
    :return: Instantiated ``settings_cls`` object.
    :rtype: S
    """
    input_dir = base_dir / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_config_file = base_dir / "logging.json"
    if not log_config_file.exists():
        log_config_file.write_text('{"version": 1}', encoding="utf-8")

    kwargs: dict[str, Any] = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "log_config_file": str(log_config_file),
        "use_destination": LOCAL_FS,
        "dlt_dev_mode": False,
        "use_output_dir_for_pipeline_metadata": False,
    }
    kwargs.update(overrides)
    return settings_cls(**kwargs)


def settings_to_argv(settings: BaseSettings, *, exclude: Iterable[str] = ()) -> list[str]:
    """Derive CLI arguments from a settings instance.

    :param settings: ``BaseSettings`` instance to serialize as CLI flags.
    :type settings: BaseSettings
    :param exclude: Field names to omit from the resulting argument list.
    :type exclude: Iterable[str]
    :return: Command-line argument list reproducing ``settings``.
    :rtype: list[str]
    """
    argv: list[str] = []
    excluded_fields: set[str] = set(getattr(type(settings), "model_computed_fields", {}).keys()) | set(exclude)
    for name, value in settings.model_dump(exclude=excluded_fields).items():
        flag = make_cli_arg(name)
        if isinstance(value, bool):
            argv.extend([flag, "true" if value else "false"])
        elif value is None:
            continue
        elif isinstance(value, list | tuple):
            if value:
                argv.extend([flag, *(str(item) for item in value)])
        else:
            argv.extend([flag, str(value)])
    return argv


@dataclass(frozen=True, slots=True)
class CliArgSpec:
    """Introspected CLI parser option metadata for a single settings field."""

    field_name: str
    option_strings: tuple[str, ...]
    is_flag: bool
    nargs: str | int | None


def build_cli_arg_specs(settings_cls: type[BaseSettings]) -> dict[str, CliArgSpec]:
    """Introspect the ``argparse`` parser that ``pydantic-settings`` builds for ``settings_cls``.

    :param settings_cls: Settings class to introspect.
    :type settings_cls: type[BaseSettings]
    :return: Mapping from field name to ``CliArgSpec``.
    :rtype: dict[str, CliArgSpec]
    """
    parser = CliSettingsSource(settings_cls).root_parser
    actions = getattr(parser, "_actions", ())
    specs: dict[str, CliArgSpec] = {}
    for action in actions:
        if not action.option_strings or action.dest == "help" or action.dest.endswith(":subcommand"):
            continue
        field_info = settings_cls.model_fields.get(action.dest)
        if field_info is not None and field_info.exclude:
            continue
        specs[action.dest] = CliArgSpec(
            field_name=action.dest,
            option_strings=tuple(action.option_strings),
            is_flag=action.nargs == 0,
            nargs=action.nargs,
        )
    return specs


def assert_no_cli_clashes(settings_cls: type[CdmDataLoadersBase]) -> None:
    """Assert that no two CLI option names on ``settings_cls`` collide.

    :param settings_cls: Settings class to check.
    :type settings_cls: type[CdmDataLoadersBase]
    """
    settings_cls.check_aliases()
    build_cli_arg_specs(settings_cls)


def assert_cli_field_roundtrips(
    settings_cls: type[BaseSettings],
    field_name: str,
    raw_value: object,
    expected_value: object,
    required: dict[str, Any] | None = None,
) -> None:
    """Assert every registered CLI flag for ``field_name`` parses ``raw_value`` to ``expected_value``.

    :param settings_cls: Settings class under test.
    :type settings_cls: type[BaseSettings]
    :param field_name: Target model field name.
    :type field_name: str
    :param raw_value: Raw string-convertible CLI value.
    :type raw_value: object
    :param expected_value: Expected parsed Python value on the model.
    :type expected_value: object
    :param required: Optional required field values needed to construct ``settings_cls``.
    :type required: dict[str, Any] | None
    """
    specs = build_cli_arg_specs(settings_cls)
    option_strings = specs[field_name].option_strings

    required_args = {k: str(v) for k, v in (required or {}).items() if k != field_name}
    required_list = [
        element for item in {make_cli_arg(k): v for k, v in required_args.items()}.items() for element in item
    ]

    results = [
        CliApp.run(model_cls=settings_cls, cli_args=[*required_list, option, str(raw_value)])
        for option in option_strings
    ]

    assert all(getattr(r, field_name) == getattr(results[0], field_name) for r in results), (
        f"{settings_cls.__name__}.{field_name}: flags {option_strings} disagree: {results}"
    )
    assert getattr(results[0], field_name) == expected_value


def assert_spark_rows_equal(result_rows: list[Row], expected_rows: list[Row]) -> None:
    """Compare collected PySpark DataFrame rows as dictionaries independent of row order.

    :param result_rows: Actual rows returned by ``df.collect()``.
    :type result_rows: list[Row]
    :param expected_rows: Expected rows returned by ``df.collect()``.
    :type expected_rows: list[Row]
    """
    results_dict = [r.asDict() for r in result_rows]
    expected_dict = [r.asDict() for r in expected_rows]
    assert len(results_dict) == len(expected_dict)
    for row in results_dict:
        assert row in expected_dict
    for row in expected_dict:
        assert row in results_dict


def write_json_or_yaml(path: Path, data_structure: dict[str, Any]) -> Path:
    """Write ``data_structure`` to ``path`` as JSON or YAML based on the file suffix.

    :param path: Target file path.
    :type path: Path
    :param data_structure: Mapping to serialize.
    :type data_structure: dict[str, Any]
    :return: Written ``path``.
    :rtype: Path
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data_structure) if path.suffix.lower() == ".json" else yaml.safe_dump(data_structure)
    path.write_text(text, encoding="utf-8")
    return path


def jsonlines_text_to_objects(text: str) -> list[dict[str, Any]]:
    """Parse all non-empty lines of a JSON Lines string.

    :param text: Newline-delimited JSON text.
    :type text: str
    :return: Parsed list of JSON dictionaries.
    :rtype: list[dict[str, Any]]
    """
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def register_temp_module(
    monkeypatch: pytest.MonkeyPatch,
    prefix: str = "_test_module",
    *,
    name: str | None = None,
    **attrs: object,
) -> str:
    """Create an in-memory module with ``attrs`` and register it in ``sys.modules`` via ``monkeypatch``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param prefix: Module name prefix when ``name`` is omitted.
    :type prefix: str
    :param name: Optional explicit module name.
    :type name: str | None
    :param attrs: Attributes to bind on the synthetic module.
    :type attrs: object
    :return: Registered module name.
    :rtype: str
    """
    module_name = name or f"{prefix}_{uuid4().hex}"
    module = ModuleType(module_name)
    for attr_name, attr_val in attrs.items():
        setattr(module, attr_name, attr_val)
    monkeypatch.setitem(sys.modules, module_name, module)
    return module_name


def strip_none_values[T](value: T) -> T:
    """Recursively remove keys with ``None`` values from nested dicts and lists.

    :param value: Input Python value.
    :type value: T
    :return: Copy with ``None``-valued dictionary entries removed.
    :rtype: T
    """
    if isinstance(value, dict):
        return cast("T", {k: strip_none_values(v) for k, v in value.items() if v is not None})
    if isinstance(value, list):
        return cast("T", [strip_none_values(item) for item in value])
    return value


class NestedPathError(ValueError):
    """Raised for an invalid nested path or a non-dictionary intermediate value."""


def set_nested(target: dict[str, Any], path: Sequence[str], value: object) -> None:
    """Set a leaf through a nonempty sequence of string keys, creating missing dicts.

    :param target: Dictionary to mutate in place.
    :type target: dict[str, Any]
    :param path: Nonempty sequence of string keys leading to the leaf.
    :type path: Sequence[str]
    :param value: Value to assign at the leaf key.
    :type value: object
    :raises NestedPathError: If ``path`` is empty, not a sequence of strings, or collides with a non-dict intermediate.
    """
    if not path or isinstance(path, str | bytes) or not all(isinstance(fragment, str) for fragment in path):
        message = "Path must be a nonempty sequence of string keys."
        raise NestedPathError(message)
    current = target
    for fragment in path[:-1]:
        if fragment not in current:
            current[fragment] = {}
        child = current[fragment]
        if not isinstance(child, dict):
            message = f"Path collision at {fragment!r}: intermediate value must be a dict."
            raise NestedPathError(message)
        current = child
    current[path[-1]] = value
