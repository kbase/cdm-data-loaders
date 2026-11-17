"""Cached YAML, settings-file, and Python fixture-module loaders for specification suites."""

import copy
import functools
import importlib.util
import json
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any, Final

import yaml

CASE_MANIFEST: Final[str] = "case.yaml"
SUPPORTED_SETTINGS_SUFFIXES: Final[frozenset[str]] = frozenset({".toml", ".yaml", ".yml", ".json"})


@functools.cache
def _read_yaml_mapping(resolved_path: Path) -> dict[str, Any]:
    """Parse and cache a top-level YAML mapping from a resolved file path.

    :param resolved_path: Resolved path to the YAML file.
    :type resolved_path: Path
    :raises TypeError: If the parsed YAML document is not a dictionary.
    :return: Parsed YAML dictionary.
    :rtype: dict[str, Any]
    """
    with resolved_path.open(encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)
    if not isinstance(loaded, dict):
        msg = f"{resolved_path}: expected a mapping at the top level"
        raise TypeError(msg)
    return loaded


def load_yaml_dict(path: Path) -> dict[str, Any]:
    """Load a YAML file whose top-level document is a mapping.

    Returns a fresh copy from the parsed cache so callers can mutate nested
    dictionaries without affecting subsequent reads.

    :param path: Path to the YAML file.
    :type path: Path
    :raises TypeError: If the parsed YAML document is not a dictionary.
    :return: Deep copy of the parsed YAML mapping.
    :rtype: dict[str, Any]
    """
    return copy.deepcopy(_read_yaml_mapping(path.resolve()))


def load_settings_file(path: Path) -> dict[str, Any]:
    """Load a settings configuration dictionary from a TOML, YAML, or JSON file.

    :param path: Path to a ``.toml``, ``.yaml``, ``.yml``, or ``.json`` settings file.
    :type path: Path
    :raises ValueError: If the file extension is unsupported.
    :raises TypeError: If the parsed document is not a mapping.
    :return: Dictionary of settings values.
    :rtype: dict[str, Any]
    """
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SETTINGS_SUFFIXES:
        msg = f"Unsupported settings file extension {suffix!r} for {path}; expected .toml, .yaml, .yml, or .json"
        raise ValueError(msg)
    text = path.read_text(encoding="utf-8")
    if suffix == ".toml":
        loaded: object = tomllib.loads(text)
    elif suffix in {".yaml", ".yml"}:
        loaded = yaml.safe_load(text)
    else:
        loaded = json.loads(text)

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        msg = f"{path}: expected a mapping at the top level, got {type(loaded).__name__}"
        raise TypeError(msg)
    return dict(loaded)


def load_spec_module(path: Path) -> ModuleType:
    """Import a standalone Python specification fixture file without mutating ``sys.modules``.

    :param path: Path to the ``.py`` fixture module.
    :type path: Path
    :raises ImportError: If module spec creation fails.
    :return: Executed Python module object.
    :rtype: ModuleType
    """
    spec = importlib.util.spec_from_file_location(f"_spec_fixture_{abs(hash(path.resolve()))}", path)
    if spec is None or spec.loader is None:
        msg = f"Cannot create module spec from {path}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
