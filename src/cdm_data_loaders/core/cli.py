"""Generic CLI entry-point runner and specification error base class."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from logging import Logger, getLogger
from typing import ClassVar

from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.settings import LoggerSettings
from cdm_data_loaders.utils.logging import init_logger

logger: Logger = getLogger(__name__)


class SpecError(Exception):
    """Base exception carrying specification ``code`` and ``title`` metadata."""

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


@dataclass(frozen=True, slots=True)
class CliRunner[S: LoggerSettings, R = None]:
    """Console-script entry point binding a ``LoggerSettings`` subclass to an action callable.

    :param settings_cls: Pydantic settings class instantiated from CLI flags and environment variables
    :type settings_cls: type[S]
    :param action_fn: callable invoked with the instantiated settings object
    :type action_fn: Callable[[S], R]
    """

    settings_cls: type[S]
    action_fn: Callable[[S], R]

    @property
    def pipeline_fn(self) -> Callable[[S], R]:
        """Return ``action_fn`` for pipeline runner introspection.

        :return: bound action or pipeline callable
        :rtype: Callable[[S], R]
        """
        return self.action_fn

    def __call__(self) -> None:
        """Parse ``sys.argv`` and execute ``action_fn``, returning ``None`` so the process exits with code 0.

        :return: ``None``
        :rtype: None
        """
        self.run()

    def run(self, cli_args: list[str] | None = None, **settings_kwargs: object) -> R:
        """Parse ``cli_args`` (default ``sys.argv[1:]``), execute ``action_fn``, and return its result.

        :param cli_args: command-line arguments, or ``None`` to read ``sys.argv[1:]``
        :type cli_args: list[str] | None
        :param settings_kwargs: additional keyword overrides forwarded to ``CliApp.run``
        :type settings_kwargs: object
        :return: return value from ``action_fn``
        :rtype: R
        """
        return run_cli(self.settings_cls, self.action_fn, settings_kwargs, cli_args)


def run_cli[S: LoggerSettings, R = None](
    settings_cls: type[S],
    action_fn: Callable[[S], R],
    settings_kwargs: Mapping[str, object] | None = None,
    cli_args: list[str] | None = None,
) -> R:
    """Instantiate ``settings_cls`` from CLI args, initialize logging, and execute ``action_fn``.

    :param settings_cls: settings class to instantiate
    :type settings_cls: type[S]
    :param action_fn: callable invoked with the validated settings instance
    :type action_fn: Callable[[S], R]
    :param settings_kwargs: optional extra keyword settings forwarded to ``CliApp.run``
    :type settings_kwargs: Mapping[str, object] | None
    :param cli_args: command-line arguments, or ``None`` to read ``sys.argv[1:]``
    :type cli_args: list[str] | None
    :return: return value of ``action_fn``
    :rtype: R
    """
    try:
        settings = CliApp.run(settings_cls, cli_args=cli_args, **dict(settings_kwargs or {}))
        init_logger(settings.log_config_file)
    except (SettingsError, ValidationError, ValueError):
        logger.exception("Error initialising config")
        raise
    except Exception:
        logger.exception("Unexpected error setting up config")
        raise
    return action_fn(settings)
