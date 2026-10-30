"""Shared model-registry and scenario-directory fixtures for the JSONL pipeline tests."""

from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import BaseModel, Field

from tests.helpers import register_temp_module


class Widget(BaseModel):
    """Test model. widget_id must be non-empty. count must be zero or more."""

    widget_id: str = Field(min_length=1)
    count: int = Field(ge=0)


@pytest.fixture
def widget_model() -> type[Widget]:
    """Return the Widget model."""
    return Widget


@pytest.fixture
def entity_models_module_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[[dict[str, type[BaseModel]]], str]:
    """Return a factory that registers a module holding an ENTITY_MODELS dict."""

    def _factory(entity_models: dict[str, type[BaseModel]]) -> str:
        return register_temp_module(monkeypatch, "_test_entity_models", ENTITY_MODELS=entity_models)

    return _factory


@pytest.fixture
def entity_models_module(
    entity_models_module_factory: Callable[[dict[str, type[BaseModel]]], str], widget_model: type[Widget]
) -> str:
    """Return a module name that maps 'widget' to the Widget model."""
    return entity_models_module_factory({"widget": widget_model})


@pytest.fixture
def scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Return a function that maps a scenario name to its data directory."""

    def _resolve(scenario: str) -> str:
        path = test_data_dir / "jsonlines" / scenario
        assert path.is_dir(), f"missing test fixture directory: {path}"
        return str(path)

    return _resolve
