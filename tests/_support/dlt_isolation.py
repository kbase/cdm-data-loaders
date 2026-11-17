"""Shared dlt config isolation helpers for tests.

Swaps the config provider chain held by ``PluggableRunContext`` in dlt's global
``Container`` for disposable in-memory providers and restores the original chain on exit.
"""

import tomllib
from collections.abc import Generator
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.providers.doc import BaseDocProvider
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.pipeline import PipelineContext


class InMemoryConfigProvider(BaseDocProvider):
    """Writable, non-secret, in-memory config provider."""

    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        """Initialize the provider with a deep copy of ``initial``.

        :param initial: Optional initial configuration document.
        :type initial: dict[str, Any] | None
        """
        super().__init__(deepcopy(initial or {}))

    @property
    def name(self) -> str:
        """Return provider display name.

        :return: Provider name.
        :rtype: str
        """
        return "InMemoryConfigProvider"

    @property
    def supports_secrets(self) -> bool:
        """Return ``False`` so ``dlt.config`` selects this provider as its writable target.

        :return: ``False``.
        :rtype: bool
        """
        return False

    @property
    def supports_sections(self) -> bool:
        """Return whether nested configuration sections are supported.

        :return: ``True``.
        :rtype: bool
        """
        return True

    @property
    def is_writable(self) -> bool:
        """Return whether runtime writes are supported.

        :return: ``True``.
        :rtype: bool
        """
        return True


class InMemorySecretsProvider(InMemoryConfigProvider):
    """Writable, secrets-capable, in-memory provider."""

    @property
    def name(self) -> str:
        """Return provider display name.

        :return: Provider name.
        :rtype: str
        """
        return "InMemorySecretsProvider"

    @property
    def supports_secrets(self) -> bool:
        """Return ``True`` for secrets resolution.

        :return: ``True``.
        :rtype: bool
        """
        return True


@contextmanager
def dlt_config_unset() -> Generator[None]:
    """Set ``dlt.config`` to ``None`` for the duration of a test block.

    :return: Context manager yielding ``None``.
    :rtype: Generator[None]
    """
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(dlt, "config", None)
        yield


@contextmanager
def deactivated_pipeline() -> Generator[None]:
    """Deactivate any active dlt pipeline before and after the wrapped block.

    :return: Context manager yielding ``None``.
    :rtype: Generator[None]
    """
    container = Container()
    if container[PipelineContext].is_active():
        container[PipelineContext].deactivate()
    try:
        yield
    finally:
        if container[PipelineContext].is_active():
            container[PipelineContext].deactivate()


@contextmanager
def isolated_dlt_config(
    config: dict[str, Any] | None = None,
    secrets: dict[str, Any] | None = None,
) -> Generator[InMemoryConfigProvider]:
    """Swap dlt's config provider chain for isolated in-memory providers, restored on exit.

    :param config: Optional seeded configuration document.
    :type config: dict[str, Any] | None
    :param secrets: Optional seeded secrets document.
    :type secrets: dict[str, Any] | None
    :return: Generator yielding the writable ``InMemoryConfigProvider``.
    :rtype: Generator[InMemoryConfigProvider]
    """
    run_ctx: PluggableRunContext = Container()[PluggableRunContext]
    old_providers: ConfigProvidersContainer = run_ctx.providers
    config_provider = InMemoryConfigProvider(config)
    secrets_provider = InMemorySecretsProvider(secrets)
    run_ctx.providers = ConfigProvidersContainer(
        initial_providers=[EnvironProvider(), config_provider, secrets_provider]
    )
    try:
        with deactivated_pipeline():
            yield config_provider
    finally:
        run_ctx.providers = old_providers


def load_dlt_toml_files(config_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load ``config.toml`` and ``secrets.toml`` from ``config_dir`` as nested dicts.

    :param config_dir: Directory containing ``config.toml`` and/or ``secrets.toml``.
    :type config_dir: Path
    :return: Pair of ``(config_dict, secrets_dict)``.
    :rtype: tuple[dict[str, Any], dict[str, Any]]
    :raises FileNotFoundError: If ``config_dir`` does not exist or is not a directory.
    """
    resolved_dir = Path(config_dir)
    if not resolved_dir.is_dir():
        msg = f"dlt config directory does not exist: {resolved_dir}"
        raise FileNotFoundError(msg)

    config_path = resolved_dir / "config.toml"
    secrets_path = resolved_dir / "secrets.toml"
    config_doc = tomllib.loads(config_path.read_text(encoding="utf-8")) if config_path.is_file() else {}
    secrets_doc = tomllib.loads(secrets_path.read_text(encoding="utf-8")) if secrets_path.is_file() else {}
    return config_doc, secrets_doc
