"""Shared model-registry and scenario-directory fixtures for the JSONL pipeline tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, Field, NonNegativeInt

from cdm_data_loaders.core.fields import NonEmptyStr
from tests.helpers import register_temp_module


def make_item(
    record: object = None,
    line_no: int = 1,
    raw_record: str | None = None,
    parse_error: str | None = None,
    source_file: str = "file.jsonl",
) -> dict[str, Any]:
    """Build a line record matching stream_jsonl_lines' output shape."""
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
    """Test model. widget_id must be non-empty. count must be zero or more."""

    widget_id: NonEmptyStr = Field()
    count: NonNegativeInt = Field()


@pytest.fixture
def widget_model() -> type[Widget]:
    """Return the Widget model."""
    return Widget


@pytest.fixture
def entity_models_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[[dict[str, type[BaseModel]]], str]:
    """Return a factory that registers a module holding an ENTITY_MODELS dict."""

    def _factory(entity_models: dict[str, type[BaseModel]]) -> str:
        return register_temp_module(monkeypatch, "_test_entity_models", ENTITY_MODELS=entity_models)

    return _factory


@pytest.fixture
def entity_models(
    entity_models_factory: Callable[[dict[str, type[BaseModel]]], str], widget_model: type[Widget]
) -> str:
    """Return a module name that maps 'widget' to the Widget model."""
    return entity_models_factory({"widget": widget_model})


@pytest.fixture
def scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Return a function that maps a scenario name to its data directory."""

    def _resolve(scenario: str) -> str:
        path = test_data_dir / "jsonlines" / scenario
        assert path.is_dir(), f"missing test fixture directory: {path}"
        return str(path)

    return _resolve
