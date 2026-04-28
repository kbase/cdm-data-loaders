"""
Provides structured logging with contextual metadata for CDM data import pipelines.
"""

import json
import logging
import logging.config
import logging.handlers
import os
from pathlib import Path

from frozendict import frozendict

DEFAULT_LOGGER_NAME = "cdm_data_loader"
GENERIC_ERROR_MESSAGE = "An error of unknown origin occurred."

LOG_FILENAME = "cdm_data_loader.log"
MAX_LOG_FILE_SIZE = 2**30  # 1 GiB
MAX_LOG_BACKUPS = 5

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
LOGGING_CONFIG_FILENAME = "logging_config.json"

JSON_LOG_CONFIG = '{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "msg": "%(message)s"}'

_module_logger = logging.getLogger(__name__)

# Immutable fallback config used when no external config file can be found.
# Note: disable_existing_loggers is intentionally absent — it is always
# injected at runtime by _load_logging_config() to guarantee it is False.
LOGGING_CONFIG = frozendict(
    {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json",
                "level": "INFO",
                "stream": "ext://sys.stdout",
            },
        },
        "formatters": {"json": {"format": JSON_LOG_CONFIG}},
        "loggers": {DEFAULT_LOGGER_NAME: {"level": "INFO", "handlers": ["console"]}},
    }
)


def _load_config_from_path(path: Path) -> dict:
    """Attempt to load and parse a JSON logging config from the given path.

    :param path: path to a JSON logging config file
    :type path: Path
    :return: parsed config dict
    :rtype: dict
    :raises FileNotFoundError: if no file exists at the given path
    :raises ValueError: if the file content is not valid JSON
    """
    with path.open() as f:
        return json.load(f)


def _load_logging_config(config_file: str | Path | None = None) -> dict:
    """Resolve and load a logging config, working through a priority chain until a source succeeds.

    Resolution order:
      1. ``config_file`` argument (if provided)
      2. ``LOG_CONFIG_FILE`` environment variable (if set)
      3. ``logging_config.json`` in the current working directory
      4. Built-in ``LOGGING_CONFIG`` frozendict

    If a file is found but sets ``disable_existing_loggers`` to ``True``,
    a warning is emitted and the value is overridden. The key is always
    set to ``False`` in the returned dict, regardless of source, to prevent
    ``dictConfig`` from silently disabling pre-existing loggers.

    :param config_file: explicit path to a logging config file, defaults to None
    :type config_file: str | Path | None, optional
    :return: logging config dict ready to pass to dictConfig
    :rtype: dict
    """
    candidates: list[tuple[Path, str]] = []

    if config_file is not None:
        candidates.append((Path(config_file), "config_file argument"))

    env_path = os.getenv("LOG_CONFIG_FILE")
    if env_path:
        candidates.append((Path(env_path), "LOG_CONFIG_FILE env var"))

    candidates.append((Path.cwd() / LOGGING_CONFIG_FILENAME, "current working directory"))

    for path, source in candidates:
        try:
            config = _load_config_from_path(path)
            if config.get("disable_existing_loggers", True):
                _module_logger.warning(
                    "Logging config loaded from %s (%s) sets disable_existing_loggers "
                    "to True. Overriding to prevent existing loggers being silently disabled.",
                    path,
                    source,
                )
            _module_logger.info("Loaded logging config from %s (%s).", path, source)
            break
        except FileNotFoundError:
            _module_logger.warning("Logging config not found at %s (%s). Trying next source.", path, source)
        except ValueError:
            _module_logger.warning("Logging config at %s (%s) is not valid JSON. Trying next source.", path, source)
    else:
        _module_logger.warning("No logging config file found. Falling back to built-in config.")
        config = dict(LOGGING_CONFIG)

    config["disable_existing_loggers"] = False
    return config


def _json_formatter() -> logging.Formatter:
    """Construct the standard CDM JSON log formatter.

    :return: configured Formatter instance
    :rtype: logging.Formatter
    """
    return logging.Formatter(JSON_LOG_CONFIG)


def _set_level_safe(logger: logging.Logger, level: str) -> None:
    """Set the log level on a logger, raising a descriptive error for invalid values.

    :param logger: the logger to configure
    :type logger: logging.Logger
    :param level: log level string
    :type level: str
    :raises ValueError: if the level string is not a recognised logging level
    """
    normalised = level.upper()
    if normalised not in VALID_LOG_LEVELS:
        msg = f"Invalid log level {level!r}. Must be one of: {', '.join(sorted(VALID_LOG_LEVELS))}"
        raise ValueError(msg)
    logger.setLevel(normalised)


def get_cdm_logger() -> logging.Logger:
    """Retrieve the default CDM logger, initialising it if necessary.

    Prefers the 'dlt' logger if it has already been configured, otherwise
    falls back to the CDM logger, creating it if needed.

    :return: initialised logger
    :rtype: logging.Logger
    """
    all_loggers = logging.root.manager.loggerDict
    if "dlt" in all_loggers:
        return logging.getLogger("dlt")
    if DEFAULT_LOGGER_NAME in all_loggers:
        return logging.getLogger(DEFAULT_LOGGER_NAME)
    return init_logger()


def _attach_file_handler(logger: logging.Logger, log_file: str | Path) -> None:
    """Attach a file handler to the given logger if no file handler is already present.

    Checks for any existing handler whose class name contains 'FileHandler'
    to avoid attaching duplicate file handlers of any type. Creates the parent
    directory of log_file if it does not already exist.

    :param logger: the logger to attach the handler to
    :type logger: logging.Logger
    :param log_file: path to the log file
    :type log_file: str | Path
    """
    if any("FileHandler" in type(h).__name__ for h in logger.handlers):
        return

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=MAX_LOG_FILE_SIZE,
        backupCount=MAX_LOG_BACKUPS,
    )
    handler.setFormatter(_json_formatter())
    logger.addHandler(handler)


def init_logger(
    log_level: str | None = None,
    config_file: str | Path | None = None,
    enable_file_logging: bool = False,  # noqa: FBT001, FBT002
    log_file: str | Path | None = None,
) -> logging.Logger:
    """Initialise the logger for the module.

    Loads config by working through the following priority chain:
      1. ``config_file`` argument (if provided)
      2. ``LOG_CONFIG_FILE`` environment variable (if set)
      3. ``logging_config.json`` in the current working directory
      4. Built-in ``LOGGING_CONFIG`` frozendict

    If ``log_level`` is specified or the ``LOG_LEVEL`` env var is set, the
    logger is set to that level. File logging is opt-in via
    ``enable_file_logging`` or the ``ENABLE_FILE_LOGGING`` env var.

    :param log_level: logger level, defaults to None
    :type log_level: str | None, optional
    :param config_file: explicit path to a logging config file, defaults to None
    :type config_file: str | Path | None, optional
    :param enable_file_logging: attach a file handler, defaults to False
    :type enable_file_logging: bool, optional
    :param log_file: path to the log file, defaults to LOG_FILENAME in the CWD
    :type log_file: str | Path, optional
    :return: initialised logger
    :rtype: logging.Logger
    """
    logging.config.dictConfig(_load_logging_config(config_file))

    logger = logging.getLogger(DEFAULT_LOGGER_NAME)

    new_log_level = log_level or os.getenv("LOG_LEVEL")
    if new_log_level:
        _set_level_safe(logger, new_log_level)

    if enable_file_logging or os.getenv("ENABLE_FILE_LOGGING", "").lower() == "true":
        _attach_file_handler(logger, log_file or LOG_FILENAME)

    return logger
