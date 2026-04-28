"""Tests for cdm_data_loaders/utils/cdm_logger.py."""

import json
import logging
import logging.handlers
from collections.abc import Generator
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict

import cdm_data_loaders.utils.cdm_logger as cdm_logger_module
from cdm_data_loaders.utils.cdm_logger import (
    DEFAULT_LOGGER_NAME,
    JSON_LOG_CONFIG,
    LOG_FILENAME,
    LOGGING_CONFIG,
    MAX_LOG_BACKUPS,
    MAX_LOG_FILE_SIZE,
    _attach_file_handler,
    _load_logging_config,
    _set_level_safe,
    get_cdm_logger,
    init_logger,
)

# Add near the top of the test file, alongside the other imports
MODULE_LOGGER_NAME = "cdm_data_loaders.utils.cdm_logger"


VALID_JSON_CONFIG = frozendict(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "CONFIGURE_HANDLER_NAME": {
                "class": "logging.StreamHandler",
                "formatter": "json",
                "level": "INFO",
                "stream": "ext://sys.stdout",
            }
        },
        "formatters": {"json": {"format": JSON_LOG_CONFIG}},
        "loggers": {DEFAULT_LOGGER_NAME: {"level": "INFO", "handlers": ["CONFIGURE_HANDLER_NAME"]}},
    }
)


def _write_config(path: Path, test_name: str, config: dict[str, Any] | None = None) -> Path:
    """Write a JSON logging config to path, using VALID_JSON_CONFIG by default."""
    # edit the config to ensure that it is recognisable as being from a specific source
    if not config:
        config = deepcopy(dict(VALID_JSON_CONFIG))
        # switch out the handler name for a test-specific name
        config["loggers"][DEFAULT_LOGGER_NAME]["handlers"] = [test_name]
        config["handlers"][test_name] = config["handlers"].pop("CONFIGURE_HANDLER_NAME")

    path.write_text(json.dumps(config if config is not None else VALID_JSON_CONFIG))
    return path


@pytest.fixture(autouse=True)
def reset_logging() -> Generator[None, Any]:
    """Remove CDM and dlt loggers from the manager and clear their handlers before and after tests."""

    def _clean() -> None:
        for name in (DEFAULT_LOGGER_NAME, "dlt"):
            logger = logging.root.manager.loggerDict.pop(name, None)
            if isinstance(logger, logging.Logger):
                logger.handlers.clear()

    _clean()
    yield
    _clean()


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove logging-related env vars during tests."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_CONFIG_FILE", raising=False)
    monkeypatch.delenv("ENABLE_FILE_LOGGING", raising=False)


@pytest.fixture
def empty_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove LOG_CONFIG_FILE env vars and chdir to an empty temporary directory with no config file."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LOG_CONFIG_FILE", raising=False)


@pytest.fixture
def cdm_logger(clean_env: None, empty_cwd: None) -> logging.Logger:  # noqa: ARG001
    """Return a CDM logger initialised with LOGGING_CONFIG."""
    return init_logger()


@pytest.fixture
def dlt_logger() -> logging.Logger:
    """Register a pre-configured dlt logger in the logging manager."""
    logger = logging.getLogger("dlt")
    logger.setLevel(logging.WARNING)
    return logger


@pytest.fixture
def config_in_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Write a valid config file in a temporary dir and chdir into it, simulating a config found in the CWD."""
    path = _write_config(tmp_path / cdm_logger_module.LOGGING_CONFIG_FILENAME, "config_in_cwd")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LOG_CONFIG_FILE", raising=False)
    return path


@pytest.fixture
def config_at_explicit_path(tmp_path: Path) -> Path:
    """Write a valid config file in a temporary directory."""
    return _write_config(tmp_path / "custom_logging_config.json", "config_at_explicit_path")


# _load_logging_config — resolution order
@pytest.mark.usefixtures("config_in_cwd")
def test_load_logging_config_uses_explicit_path_first(config_at_explicit_path: Path) -> None:
    """Ensure that the config file argument is used in preference to other choices."""
    config = _load_logging_config(config_file=config_at_explicit_path)
    assert config["disable_existing_loggers"] is False
    assert config["loggers"][DEFAULT_LOGGER_NAME]["handlers"] == ["config_at_explicit_path"]
    assert set(config["handlers"]) == {"config_at_explicit_path"}


@pytest.mark.usefixtures("config_in_cwd")
def test_load_logging_config_uses_env_var_over_cwd(
    config_at_explicit_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that the LOG_CONFIG_FILE env var is used in preference to the LOGGING_CONFIG fallback."""
    monkeypatch.setenv("LOG_CONFIG_FILE", str(config_at_explicit_path))
    config = _load_logging_config()
    assert config["loggers"][DEFAULT_LOGGER_NAME]["handlers"] == ["config_at_explicit_path"]
    assert set(config["handlers"]) == {"config_at_explicit_path"}


@pytest.mark.usefixtures("config_in_cwd")
def test_load_logging_config_uses_cwd_when_no_arg_or_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """When no argument or env var is provided, the logging_config.json in the current working directory is loaded."""
    monkeypatch.delenv("LOG_CONFIG_FILE", raising=False)
    config = _load_logging_config()
    assert config["disable_existing_loggers"] is False
    assert config["loggers"][DEFAULT_LOGGER_NAME]["handlers"] == ["config_in_cwd"]
    assert set(config["handlers"]) == {"config_in_cwd"}


@pytest.mark.usefixtures("empty_cwd")
def test_load_logging_config_falls_back_to_frozendict_when_all_sources_fail(caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that logging falls back to the default frozendict if no other sources are found."""
    with caplog.at_level(logging.WARNING, logger=MODULE_LOGGER_NAME):
        config = _load_logging_config()
    assert config == {**LOGGING_CONFIG, "disable_existing_loggers": False}
    log_message = caplog.records[-1]
    assert log_message.message == "No logging config file found. Falling back to built-in config."


@pytest.mark.usefixtures("config_in_cwd")
def test_load_logging_config_skips_bad_argument_path_and_tries_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Ensure that a non-existent or invalid config file is ignored and the next source tried."""
    monkeypatch.delenv("LOG_CONFIG_FILE", raising=False)
    with caplog.at_level(logging.DEBUG, logger=MODULE_LOGGER_NAME):
        config = _load_logging_config(config_file=tmp_path / "nonexistent.json")
    assert config["disable_existing_loggers"] is False
    assert any("nonexistent.json" in m for m in caplog.messages)


@pytest.mark.usefixtures("config_in_cwd")
def test_load_logging_config_skips_bad_env_var_path_and_tries_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Ensure that a non-existent or invalid config file is ignored and the next source tried."""
    monkeypatch.setenv("LOG_CONFIG_FILE", str(tmp_path / "nonexistent.json"))
    with caplog.at_level(logging.WARNING, logger=MODULE_LOGGER_NAME):
        config = _load_logging_config()
    assert config["disable_existing_loggers"] is False
    assert any("nonexistent.json" in m for m in caplog.messages)


@pytest.mark.usefixtures("config_in_cwd")
def test_load_logging_config_skips_invalid_json_and_tries_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Ensure that a non-existent or invalid config file is ignored and the next source tried."""
    bad_path = tmp_path / "bad_config.json"
    bad_path.write_text("this is not json {{{")
    monkeypatch.delenv("LOG_CONFIG_FILE", raising=False)
    with caplog.at_level(logging.WARNING, logger=MODULE_LOGGER_NAME):
        config = _load_logging_config(config_file=bad_path)
    assert config["disable_existing_loggers"] is False
    assert any("bad_config.json" in m for m in caplog.messages)


def test_load_logging_config_overrides_disable_existing_loggers_when_true_in_file(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, request: pytest.FixtureRequest
) -> None:
    """If a config file sets disable_existing_loggers to True, _load_logging_config should override it."""
    path = _write_config(
        tmp_path / "bad.json", request.node.originalname, {**VALID_JSON_CONFIG, "disable_existing_loggers": True}
    )
    with caplog.at_level(logging.WARNING, logger=MODULE_LOGGER_NAME):
        config = _load_logging_config(config_file=path)
    assert config["disable_existing_loggers"] is False
    assert any(
        "sets disable_existing_loggers to True. Overriding to prevent existing loggers being silently disabled" in m
        for m in caplog.messages
    )


@pytest.mark.usefixtures("clean_env")
def test_init_logger_accepts_explicit_config_file(config_at_explicit_path: Path) -> None:
    """init_logger should accept a config_file argument and pass it through to _load_logging_config without error."""
    logger = init_logger(config_file=config_at_explicit_path)
    assert isinstance(logger, logging.Logger)


@pytest.mark.usefixtures("empty_cwd")
def test_init_logger_uses_log_config_file_env_var(
    config_at_explicit_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When LOG_CONFIG_FILE is set in the environment and no config_file argument is passed, init_logger should load config from that path."""
    monkeypatch.setenv("LOG_CONFIG_FILE", str(config_at_explicit_path))
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    logger = init_logger()
    assert isinstance(logger, logging.Logger)


def test_init_logger_happy_path(cdm_logger: logging.Logger) -> None:
    """init_logger should return a Logger with the CDM default name."""
    assert isinstance(cdm_logger, logging.Logger)
    assert cdm_logger.name == DEFAULT_LOGGER_NAME
    # calling init_logger more than once returns the same object
    assert init_logger() is init_logger()
    assert cdm_logger.level == logging.INFO


@pytest.mark.parametrize("level", ["DEBUG", "Info", "warning"])
@pytest.mark.usefixtures("clean_env", "empty_cwd")
def test_init_logger_explicit_level_argument(level: str) -> None:
    """Ensure that the log level can be set explicitly."""
    logger = init_logger(log_level=level)
    assert logger.level == getattr(logging, level.upper())


@pytest.mark.usefixtures("clean_env", "empty_cwd")
def test_init_logger_env_var_sets_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """When LOG_LEVEL is set in the environment, init_logger should apply it to the logger level."""
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    assert init_logger().level == logging.ERROR


@pytest.mark.usefixtures("clean_env", "empty_cwd")
def test_init_logger_argument_takes_priority_over_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit log_level argument should take precedence over the LOG_LEVEL env var when both are set."""
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    assert init_logger(log_level="DEBUG").level == logging.DEBUG


def test_init_logger_log_level_gates_emission_correctly(
    cdm_logger: logging.Logger, caplog: pytest.LogCaptureFixture
) -> None:
    """Messages at or above the configured level should be captured; messages below it should be suppressed."""
    with caplog.at_level(logging.INFO, logger=DEFAULT_LOGGER_NAME):
        cdm_logger.info("should appear")
        cdm_logger.debug("should not appear")

    assert "should appear" in caplog.messages
    assert "should not appear" not in caplog.messages


# console
def test_init_logger_console_handler_configured_correctly(cdm_logger: logging.Logger) -> None:
    """The logger should have a StreamHandler at INFO level by default.

    No RotatingFileHandler should be attached unless explicitly requested.
    """
    stream_handlers = [h for h in cdm_logger.handlers if type(h) is logging.StreamHandler]
    assert len(stream_handlers) == 1
    assert stream_handlers[0].level == logging.INFO
    rotating_handlers = [h for h in cdm_logger.handlers if isinstance(h, logging.handlers.RotatingFileHandler)]
    assert rotating_handlers == []


@pytest.mark.usefixtures("clean_env", "empty_cwd")
def test_get_cdm_logger_creates_cdm_logger_when_none_exists() -> None:
    """When neither a dlt nor a CDM logger exists, get_cdm_logger should initialise and return a new CDM logger."""
    logger = get_cdm_logger()
    assert isinstance(logger, logging.Logger)
    assert logger.name == DEFAULT_LOGGER_NAME
    assert DEFAULT_LOGGER_NAME in logging.root.manager.loggerDict
    # get_cdm_logger should returns the existing logger rather than creating a new one.
    assert get_cdm_logger() is init_logger()


@pytest.mark.usefixtures("clean_env", "empty_cwd", "dlt_logger")
def test_get_cdm_logger_prefers_dlt_over_cdm_logger() -> None:
    """When a dlt logger is present in the logging manager, get_cdm_logger should return it even if a CDM logger also exists."""
    init_logger()
    assert get_cdm_logger().name == "dlt"


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_set_level_safe_accepts_all_valid_levels(cdm_logger: logging.Logger, level: str) -> None:
    """_set_level_safe should accept all standard logging level strings."""
    _set_level_safe(cdm_logger, level)
    assert cdm_logger.level == getattr(logging, level)


@pytest.mark.parametrize("level", ["debug", "Info", "wARNING"])
def test_set_level_safe_is_case_insensitive(cdm_logger: logging.Logger, level: str) -> None:
    """Ensure that _set_level_safe normalises case issues."""
    _set_level_safe(cdm_logger, level)
    assert cdm_logger.level == getattr(logging, level.upper())


def test_set_level_safe_raises_descriptive_error_for_invalid_level(cdm_logger: logging.Logger) -> None:
    """_set_level_safe should raise a ValueError for an unrecognised level string.

    The error message should echo back the bad value and list the valid options.
    """
    with pytest.raises(ValueError, match=r"Invalid log level 'VERBOS'\. Must be one of"):
        _set_level_safe(cdm_logger, "VERBOS")


# file handler
def test_attach_file_handler_adds_correctly_configured_handler(cdm_logger: logging.Logger, tmp_path: Path) -> None:
    """_attach_file_handler should add a single RotatingFileHandler with a log file at the specified path."""
    log_path = tmp_path / LOG_FILENAME
    _attach_file_handler(cdm_logger, log_path)

    rotating_handlers = [h for h in cdm_logger.handlers if isinstance(h, logging.handlers.RotatingFileHandler)]
    assert len(rotating_handlers) == 1

    handler = rotating_handlers[0]
    assert handler.maxBytes == MAX_LOG_FILE_SIZE
    assert handler.backupCount == MAX_LOG_BACKUPS
    assert handler.formatter._fmt == JSON_LOG_CONFIG  # noqa: SLF001

    assert log_path.exists()

    # try attaching a second handler
    _attach_file_handler(cdm_logger, log_path)

    assert len([h for h in cdm_logger.handlers if isinstance(h, logging.handlers.RotatingFileHandler)]) == 1


def test_attach_file_handler_creates_missing_parent_directory(cdm_logger: logging.Logger, tmp_path: Path) -> None:
    """_attach_file_handler should create the parent directory of the log file path if it does not exist."""
    log_path = tmp_path / "nested" / "dirs" / LOG_FILENAME
    assert not log_path.parent.exists()
    _attach_file_handler(cdm_logger, log_path)
    assert log_path.parent.exists()
    assert log_path.exists()


def test_attach_file_handler_detects_any_file_handler_type(cdm_logger: logging.Logger, tmp_path: Path) -> None:
    """_attach_file_handler should not add a second file handler."""
    existing = logging.FileHandler(tmp_path / "other.log")
    cdm_logger.addHandler(existing)

    _attach_file_handler(cdm_logger, tmp_path / LOG_FILENAME)

    file_handlers = [h for h in cdm_logger.handlers if "FileHandler" in type(h).__name__]
    assert len(file_handlers) == 1


@pytest.mark.parametrize("env_value", ["true", "TRUE", "True"])
@pytest.mark.usefixtures("clean_env")
def test_init_logger_file_handler_added_when_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env_value: str
) -> None:
    """A file handler should be attached when enable_file_logging=True or ENABLE_FILE_LOGGING env var is set."""
    log_path = tmp_path / LOG_FILENAME

    logger_arg = init_logger(enable_file_logging=True, log_file=log_path)
    assert any("FileHandler" in type(h).__name__ for h in logger_arg.handlers)

    logger_arg.handlers.clear()
    logging.root.manager.loggerDict.pop(DEFAULT_LOGGER_NAME, None)

    monkeypatch.setenv("ENABLE_FILE_LOGGING", env_value)
    logger_env = init_logger(log_file=log_path)
    assert any("FileHandler" in type(h).__name__ for h in logger_env.handlers)
