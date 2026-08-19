"""
Provides structured logging with contextual metadata for CDM data import pipelines.
"""

import json
import logging
import logging.config
import sys
from pathlib import Path
from types import TracebackType
from typing import Any

import yaml
from dlt.common.runtime.json_logging import config_root_logger
from frozendict import frozendict

from cdm_data_loaders.core.settings import LoggerSettings

ROOT_LOGGER_CONFIGURED: bool = False


# There is no default logger config file (see cdm_data_loaders.core.fields.DEFAULTS[LOG_CONFIG_FILE])
# so the logger falls back to LOGGING_CONFIG if an external config file is not found.
# Immutable fallback config
LOGGING_CONFIG = frozendict(
    {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {"root": {"level": "INFO", "handlers": ["console"]}},
        "disable_existing_loggers": False,
    }
)


def _load_config_from_path(path: str | Path) -> dict[str, Any]:
    """Attempt to load and parse a logging config from the given path.

    :param path: path to a JSON or YAML logging config file
    :type path: str | Path
    :return: parsed config dict
    :rtype: dict[str, Any]
    """
    if not isinstance(path, Path):
        path = Path(path)

    suffix = path.suffix.lower()

    if suffix in (".yml", ".yaml"):
        with path.open() as f:
            return yaml.safe_load(f)

    if suffix == ".json":
        with path.open() as f:
            return json.load(f)

    err_msg = f"Unsupported config file format: {path.name}"
    raise ValueError(err_msg)


def get_cdm_logger(logger_name: str, settings: LoggerSettings | None = None) -> logging.Logger:
    """Retrieve the default CDM logger, initialising it if necessary.

    :return: initialised logger
    :rtype: logging.Logger
    """
    init_logger(settings)
    return logging.getLogger(logger_name)


def init_logger(settings: LoggerSettings | None = None) -> None:
    """Initialise logger configuration."""
    if not settings:
        # init settings (pulling in log_config_file from cmd line args / env vars) if not provided
        settings = LoggerSettings()  # pyright: ignore[reportCallIssue]

    if settings and settings.log_config_file:
        config = _load_config_from_path(settings.log_config_file)
    else:
        logging.getLogger(__name__).warning("No logging config file found. Falling back to built-in config.")
        config = dict(LOGGING_CONFIG)

    # make sure there's nothing too crazy going on
    config["disable_existing_loggers"] = False
    logging.config.dictConfig(config)

    configure_root_logger_from_dlt()


def configure_root_logger_from_dlt() -> None:
    """Configure the root logger using settings from the dlt logger.

    Copies over the log formatter used by the dlt logger.
    """
    global ROOT_LOGGER_CONFIGURED  # noqa: PLW0603
    if ROOT_LOGGER_CONFIGURED is False and "dlt" in logging.Logger.manager.loggerDict:
        dlt_logger = logging.getLogger("dlt")
        # this passes dlt's formatter up to the root logger
        config_root_logger()
        # pass messages from the dlt logger up to the root
        dlt_logger.propagate = True
        # clear the dlt logger handlers and let everything be handled by the root logger instead.
        dlt_logger.handlers.clear()
        ROOT_LOGGER_CONFIGURED = True


def log_exception(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: TracebackType | None) -> None:
    """Log uncaught exceptions using the exception handler."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger = logging.getLogger()
    logger.exception("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = log_exception
