"""Shared schema-registry and settings helpers for JSON Schema JSONL pipeline tests."""

from collections.abc import Callable
from itertools import count
from pathlib import Path
from typing import Any

import dlt
import pytest

from cdm_data_loaders.pipelines.jsonlines.settings import JsonlJsonschemaIngestSettings
from tests.helpers.cli_assertions import make_cts_settings, register_temp_module


def build_widget_schema() -> dict[str, Any]:
    """Return an object JSON Schema with required, numeric, format, and nested constraints.

    :return: Fresh JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["widget_id", "count"],
        "properties": {
            "widget_id": {"type": "string", "minLength": 1},
            "count": {"type": "integer", "minimum": 0},
            "collected": {"type": "string", "format": "date"},
            "label": {"type": "string", "default": "not inserted"},
            "nested": {
                "type": "object",
                "properties": {
                    "tags": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    }


def make_entity_schemas_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[[object], str]:
    """Build a factory registering synthetic ``ENTITY_SCHEMAS`` modules in ``sys.modules``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Factory accepting a schema registry mapping and returning module name.
    :rtype: Callable[[object], str]
    """

    def _register(registry: object) -> str:
        return register_temp_module(monkeypatch, "_test_entity_schemas", ENTITY_SCHEMAS=registry)

    return _register


def make_schema_settings_factory(
    tmp_path: Path,
    dlt_destination_config: str,
    entity_schemas: str,
) -> Callable[..., JsonlJsonschemaIngestSettings]:
    """Build a factory creating ``JsonlJsonschemaIngestSettings`` with fresh per-run output directories.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination block name.
    :type dlt_destination_config: str
    :param entity_schemas: Module path holding ``ENTITY_SCHEMAS``.
    :type entity_schemas: str
    :return: Settings factory callable.
    :rtype: Callable[..., JsonlJsonschemaIngestSettings]
    """
    run_counter = count()

    def _factory(**overrides: object) -> JsonlJsonschemaIngestSettings:
        run_index = next(run_counter)
        output_dir = tmp_path / f"output_{run_index}"
        output_dir.mkdir()
        defaults: dict[str, Any] = {
            "output_dir": str(output_dir),
            "use_destination": dlt_destination_config,
            "use_output_dir_for_pipeline_metadata": True,
            "dataset_name": "schema_test",
            "file_glob": "*.jsonl*",
            "loader_file_format": "parquet",
            "buffer_size": 2,
            "entity_schemas": entity_schemas,
        }
        defaults.update(overrides)
        settings = make_cts_settings(JsonlJsonschemaIngestSettings, tmp_path, **defaults)
        dlt.config["destination.local_fs.bucket_url"] = str(settings.output_dir)
        return settings

    return _factory
