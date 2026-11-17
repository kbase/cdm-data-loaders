"""Structured JSON logging configuration and uncaught exception hook helpers."""

import copy
import json
import logging
import logging.config
import os
import sys
from pathlib import Path
from types import TracebackType
from typing import Any, Final

import yaml
from dlt.common.runtime.json_logging import JSONLogFormatter

DLT_LOGGER_NAME: Final[str] = "dlt"
JSON_FORMATTER_NAME: Final[str] = "dlt_json"
LOG_CONFIG_ENV_VAR: Final[str] = "CDL_LOG_CONFIG_FILE"
FALLBACK_KEY: Final[tuple[str, int]] = ("<built-in>", 0)
applied_key: tuple[str, int] | None = None


def _default_config() -> dict[str, Any]:
    """Return the built-in logging dictionary config (WARNING and above to STDOUT).

    :return: logging dictionary configuration
    :rtype: dict[str, Any]
    """
    return {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "root": {"level": "WARNING", "handlers": ["console"]},
        "disable_existing_loggers": False,
    }


def _load_config_from_path(path: str | Path) -> dict[str, Any]:
    """Load and parse a JSON or YAML logging config file from ``path``.

    :param path: path to a JSON or YAML logging config file
    :type path: str | Path
    :return: parsed configuration dictionary
    :rtype: dict[str, Any]
    :raises ValueError: if the file extension is not ``.json``, ``.yaml``, or ``.yml``
    :raises TypeError: if the parsed document is not a mapping
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in (".yml", ".yaml"):
        with path.open(encoding="utf-8") as f:
            config = yaml.safe_load(f)
    elif suffix == ".json":
        with path.open(encoding="utf-8") as f:
            config = json.load(f)
    else:
        err_msg = f"Unsupported config file format: {path.name}"
        raise ValueError(err_msg)

    if not isinstance(config, dict):
        err_msg = f"Logging config in {path.name} must be a mapping, not {type(config).__name__}"
        raise TypeError(err_msg)

    return config


def _enforce_json_formatter(config: dict[str, Any]) -> dict[str, Any]:
    """Return a deep copy of ``config`` in which every handler uses ``JSONLogFormatter``.

    :param config: raw logging configuration dictionary
    :type config: dict[str, Any]
    :return: configuration copy with formatters set to ``dlt``'s ``JSONLogFormatter``
    :rtype: dict[str, Any]
    """
    config = copy.deepcopy(config)
    # dictConfig accepts a callable for the "()" factory key
    config.setdefault("formatters", {})[JSON_FORMATTER_NAME] = {"()": JSONLogFormatter}
    for handler_cfg in config.get("handlers", {}).values():
        handler_cfg["formatter"] = JSON_FORMATTER_NAME
    config["disable_existing_loggers"] = False
    return config


def adopt_dlt_logger() -> None:
    """Route ``dlt``'s logger through the root logger's JSON-formatted handlers."""
    dlt_logger = logging.getLogger(DLT_LOGGER_NAME)
    dlt_logger.propagate = True
    dlt_logger.handlers.clear()


def log_exception(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: TracebackType | None) -> None:
    """Log uncaught exceptions on the root logger, delegating ``KeyboardInterrupt`` to ``sys.__excepthook__``.

    :param exc_type: exception class
    :type exc_type: type[BaseException]
    :param exc_value: exception instance
    :type exc_value: BaseException
    :param exc_traceback: traceback object, if available
    :type exc_traceback: TracebackType | None
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.getLogger().error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def init_logger(log_config_file: str | Path | None = None, *, force: bool = False) -> None:
    """Initialize structured JSON logging and install ``log_exception`` as ``sys.excepthook``.

    Repeat calls skip reconfiguration unless ``force`` is ``True`` or the config file path/mtime changes.
    When ``log_config_file`` is ``None``, falls back to ``CDL_LOG_CONFIG_FILE`` if set in the environment.

    :param log_config_file: optional path to a JSON or YAML logging configuration file
    :type log_config_file: str | Path | None
    :param force: whether to reapply the configuration even when unchanged
    :type force: bool
    """
    global applied_key  # noqa: PLW0603

    resolved_input = log_config_file if log_config_file is not None else (os.environ.get(LOG_CONFIG_ENV_VAR) or None)
    path = Path(resolved_input).resolve() if resolved_input else None
    key = (str(path), path.stat().st_mtime_ns) if path else FALLBACK_KEY

    if force or key != applied_key:
        config = _load_config_from_path(path) if path else _default_config()
        logging.config.dictConfig(_enforce_json_formatter(config))
        applied_key = key
        if path is None:
            logging.getLogger(__name__).warning("No logging config file found. Falling back to built-in config.")

    sys.excepthook = log_exception
    adopt_dlt_logger()
