"""Shared fixtures for ``tests/cdm_data_loaders``."""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from frozendict import frozendict
from pydantic import BaseModel
from pydantic_settings import BaseSettings, CliApp

from cdm_data_loaders.core import reporting
from cdm_data_loaders.core.fields import (
    BUFFER_SIZE,
    DLT_DEV_MODE,
    FILE_LISTING_FILE_NAME,
    INPUT_DIR,
    LOG_CONFIG_FILE,
    LOG_INTERVAL,
    OUTPUT_DIR,
    PIPELINE_DIR_NAME,
    REPORT_FILE_NAME,
    USE_DESTINATION,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
)
from cdm_data_loaders.core.settings import CtsSettings
from tests.helpers.cli_assertions import (
    CLI,
    ENV,
    INIT,
    SettingsFactory,
    make_cli_arg,
    make_cts_settings,
)
from tests.helpers.fixtures_jsonlines import Widget, make_entity_models_factory
from tests.helpers.fixtures_jsonschema import build_widget_schema, make_entity_schemas_factory
from tests.helpers.paths_and_constants import LOCAL_FS


@pytest.fixture
def make_settings(monkeypatch: pytest.MonkeyPatch) -> SettingsFactory:
    """Build a settings object from string values supplied as init kwargs, env vars, or CLI args.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Settings factory callable.
    :rtype: SettingsFactory
    """

    def _make(settings_cls: type[BaseSettings], source: str, values: Mapping[str, str]) -> BaseSettings:
        if source == INIT:
            return settings_cls(**values)  # pyright: ignore[reportArgumentType]
        if source == ENV:
            prefix = settings_cls.model_config.get("env_prefix", "")
            for name, value in values.items():
                monkeypatch.setenv(f"{prefix}{name}".upper(), value)
            return settings_cls()
        if source == CLI:
            cli_args = [arg for name, value in values.items() for arg in (make_cli_arg(name), value)]
            return CliApp.run(settings_cls, cli_args=cli_args)
        err_msg = f"Unknown settings source: {source!r}"
        raise ValueError(err_msg)

    return _make


@pytest.fixture
def input_dir(tmp_path: Path) -> Path:
    """Create and return an existing input directory under ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Resolved input directory path.
    :rtype: Path
    """
    path = tmp_path / "input"
    path.mkdir(exist_ok=True)
    return path.resolve()


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Return a resolved output directory path under ``tmp_path`` that does not exist yet.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Resolved output directory path.
    :rtype: Path
    """
    return (tmp_path / "output").resolve()


@pytest.fixture
def test_defaults(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    """Return defaults for required ``CtsSettings`` fields.

    :param input_dir: Existing input directory fixture.
    :type input_dir: Path
    :param output_dir: Target output directory fixture.
    :type output_dir: Path
    :return: Dictionary of required field defaults.
    :rtype: dict[str, Any]
    """
    return {INPUT_DIR: input_dir, OUTPUT_DIR: output_dir, USE_DESTINATION: LOCAL_FS}


@pytest.fixture
def test_cts_settings(input_dir: Path, output_dir: Path) -> frozendict[str, Any]:
    """Return a raw settings dictionary with all ``CtsSettings`` fields specified.

    :param input_dir: Existing input directory fixture.
    :type input_dir: Path
    :param output_dir: Target output directory fixture.
    :type output_dir: Path
    :return: Frozen dictionary of raw field values.
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            DLT_DEV_MODE: "false",
            LOG_CONFIG_FILE: "some/path",
            INPUT_DIR: input_dir,
            OUTPUT_DIR: output_dir,
            USE_DESTINATION: LOCAL_FS,
            USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: "true",
            LOG_INTERVAL: "5000",
            BUFFER_SIZE: 25,
        }
    )


@pytest.fixture
def test_cts_settings_reconciled(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return reconciled ``CtsSettings`` dictionary matching ``model_dump()``.

    :param test_cts_settings: Raw ``CtsSettings`` dictionary fixture.
    :type test_cts_settings: frozendict[str, Any]
    :return: Expected ``model_dump()`` dictionary.
    :rtype: frozendict[str, Any]
    """
    pipeline_dir = test_cts_settings[OUTPUT_DIR] / PIPELINE_DIR_NAME
    return frozendict(
        {
            **test_cts_settings,
            INPUT_DIR: str(test_cts_settings[INPUT_DIR].resolve()),
            DLT_DEV_MODE: False,
            USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: True,
            LOG_INTERVAL: 5000,
            "pipeline_dir": pipeline_dir,
            "report_path": pipeline_dir / REPORT_FILE_NAME,
            "file_listing_path": pipeline_dir / FILE_LISTING_FILE_NAME,
        }
    )


@pytest.fixture
def dlt_test_settings(tmp_path: Path, dlt_destination_config: str) -> CtsSettings:
    """Return ``CtsSettings`` configured for ``local_fs`` with pipeline metadata in ``output_dir``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :return: Configured ``CtsSettings`` instance.
    :rtype: CtsSettings
    """
    return make_cts_settings(
        CtsSettings,
        tmp_path,
        log_config_file=None,
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )


@pytest.fixture
def slack_mock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the dlt Slack sender used by ``send_slack_message_carefully``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Installed ``MagicMock``.
    :rtype: MagicMock
    """
    mock = MagicMock()
    monkeypatch.setattr(reporting, "send_slack_message", mock)
    return mock


@pytest.fixture
def widget_model() -> type[BaseModel]:
    """Return the sample ``Widget`` Pydantic entity model.

    :return: ``Widget`` model class.
    :rtype: type[BaseModel]
    """
    return Widget


@pytest.fixture
def entity_models_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[dict[str, type[BaseModel]]], str]:
    """Return a factory registering synthetic ``ENTITY_MODELS`` modules in ``sys.modules``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Module-registration callable.
    :rtype: Callable[[dict[str, type[BaseModel]]], str]
    """
    return make_entity_models_factory(monkeypatch)


@pytest.fixture
def widget_schema() -> dict[str, Any]:
    """Return a fresh ``widget`` JSON Schema dictionary.

    :return: JSON Schema mapping.
    :rtype: dict[str, Any]
    """
    return build_widget_schema()


@pytest.fixture
def entity_schemas_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[[object], str]:
    """Return a factory registering synthetic ``ENTITY_SCHEMAS`` modules in ``sys.modules``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Module-registration callable.
    :rtype: Callable[[object], str]
    """
    return make_entity_schemas_factory(monkeypatch)
