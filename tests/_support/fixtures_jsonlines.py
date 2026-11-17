"""Shared model-registry and line-item helpers for JSON Lines reader and pipeline tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pytest
from pydantic import BaseModel, Field, NonNegativeInt

from cdm_data_loaders.core.fields import NonEmptyStr
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings
from specs._loader.converter_spec import Case, Run
from tests._support.cli_assertions import make_cts_settings, register_temp_module
from tests._support.pyarrow_spec import load_fixture_attr, load_json_records

PYDANTIC_SPEC_ENTITY_TABLE_NAME: Final[str] = "items"


def make_item(
    record: object = None,
    line_no: int = 1,
    raw_record: str | None = None,
    parse_error: str | None = None,
    source_file: str = "file.jsonl",
) -> dict[str, Any]:
    """Build a line record matching ``stream_jsonl_lines`` output shape.

    :param record: Parsed JSON value or ``None``.
    :type record: object
    :param line_no: 1-based line number in ``source_file``.
    :type line_no: int
    :param raw_record: Optional raw line text override.
    :type raw_record: str | None
    :param parse_error: Optional JSON parse error message.
    :type parse_error: str | None
    :param source_file: Source relative file path.
    :type source_file: str
    :return: Line item dictionary.
    :rtype: dict[str, Any]
    """
    if raw_record is None:
        raw_record = "" if parse_error is not None and record is None else json.dumps(record, separators=(",", ":"))
    return {
        "record": record,
        "raw_record": raw_record,
        "parse_error": parse_error,
        "source_file": source_file,
        "line_no": line_no,
    }


class Widget(BaseModel):
    """Sample Pydantic entity model for JSONL pipeline tests."""

    widget_id: NonEmptyStr = Field()
    count: NonNegativeInt = Field()


def make_entity_models_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[[dict[str, type[BaseModel]]], str]:
    """Build a factory registering synthetic ``ENTITY_MODELS`` modules in ``sys.modules``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Factory accepting ``{entity_name: model_cls}`` and returning module name.
    :rtype: Callable[[dict[str, type[BaseModel]]], str]
    """

    def _factory(entity_models: dict[str, type[BaseModel]]) -> str:
        return register_temp_module(monkeypatch, "_test_entity_models", ENTITY_MODELS=entity_models)

    return _factory


def make_scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Build a resolver mapping JSONL scenario names to directories under ``test_data_dir / 'jsonlines'``.

    :param test_data_dir: Root ``tests/data`` directory.
    :type test_data_dir: Path
    :return: Scenario path resolver callable.
    :rtype: Callable[[str], str]
    """

    def _resolve(scenario: str) -> str:
        path = test_data_dir / "jsonlines" / scenario
        assert path.is_dir(), f"missing test fixture directory: {path}"
        return str(path)

    return _resolve


def pydantic_run_settings_overrides(run: Run) -> dict[str, Any]:
    """Extract ``JsonlPydanticIngestSettings`` keyword overrides from ``run.params``.

    :param run: Converter specification run entry.
    :type run: Run
    :return: Dictionary of settings overrides for ``max_recursion_depth`` and ``by_alias``.
    :rtype: dict[str, Any]
    """
    overrides: dict[str, Any] = {}
    if "max_recursion_depth" in run.params:
        overrides["max_recursion_depth"] = int(run.params["max_recursion_depth"])
    if "by_alias" in run.params:
        overrides["by_alias"] = bool(run.params["by_alias"])
    return overrides


def stage_pydantic_jsonl_run(
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
    case: Case,
    run: Run,
) -> tuple[type[BaseModel], JsonlPydanticIngestSettings]:
    """Write ``run.data`` as JSONL lines and construct ``JsonlPydanticIngestSettings`` for ``case`` and ``run``.

    :param tmp_path: Temporary test directory.
    :type tmp_path: Path
    :param dlt_destination_config: Configured dlt destination name.
    :type dlt_destination_config: str
    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param case: Converter specification case.
    :type case: Case
    :param run: Converter specification run.
    :type run: Run
    :return: Tuple of ``(model_cls, settings)``.
    :rtype: tuple[type[BaseModel], JsonlPydanticIngestSettings]
    """
    model_cls: type[BaseModel] = load_fixture_attr(case.schema, "MODEL", type)
    input_dir = tmp_path / "input"
    entity_dir = input_dir / PYDANTIC_SPEC_ENTITY_TABLE_NAME
    entity_dir.mkdir(parents=True, exist_ok=True)

    records = load_json_records(run.data)
    jsonl_text = "".join(f"{json.dumps(rec)}\n" for rec in records)
    (entity_dir / "data.jsonl").write_text(jsonl_text, encoding="utf-8")

    mod_name = register_temp_module(
        monkeypatch,
        f"_pydantic_spec_models_{case.suite}_{case.name}_{run.id}".replace("-", "_"),
        ENTITY_MODELS={PYDANTIC_SPEC_ENTITY_TABLE_NAME: model_cls},
    )
    settings = make_cts_settings(
        JsonlPydanticIngestSettings,
        tmp_path,
        dataset_name="jsonl_pydantic_spec_ds",
        input_dir=str(input_dir),
        entity_models=mod_name,
        loader_file_format="parquet",
        use_destination=dlt_destination_config,
        **pydantic_run_settings_overrides(run),
    )
    return model_cls, settings
