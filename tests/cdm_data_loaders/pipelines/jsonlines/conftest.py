"""Shared fixtures for jsonlines_ingest pipeline tests."""

import gzip
import sys
import types
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings


class Widget(BaseModel):
    """Test model. widget_id must be non-empty. count must be zero or more."""

    widget_id: str = Field(min_length=1)
    count: int = Field(ge=0)


@pytest.fixture
def widget_model() -> type[Widget]:
    """Return the Widget model."""
    return Widget


@pytest.fixture
def entity_models_module_factory() -> Generator[Callable[[dict[str, type[BaseModel]]], str]]:
    """Return a factory that registers a module holding an ENTITY_MODELS dict.

    Each call creates a module with a unique name and adds it to
    sys.modules. Removes each created module after the test.
    """
    created_names: list[str] = []

    def _factory(entity_models: dict[str, type[BaseModel]]) -> str:
        module_name = f"_test_entity_models_{uuid4().hex}"
        module = types.ModuleType(module_name)
        module.ENTITY_MODELS = entity_models  # type: ignore[attr-defined]
        sys.modules[module_name] = module
        created_names.append(module_name)
        return module_name

    yield _factory

    for name in created_names:
        sys.modules.pop(name, None)


@pytest.fixture
def entity_models_module(
    entity_models_module_factory: Callable[[dict[str, type[BaseModel]]], str], widget_model: type[Widget]
) -> str:
    """Return a module name that maps 'widget' to the Widget model."""
    return entity_models_module_factory({"widget": widget_model})


@pytest.fixture
def settings_factory(
    tmp_path: Path, dlt_destination_config: str, entity_models_module: str
) -> Callable[..., JsonlPydanticIngestSettings]:
    """Return a factory that builds a valid JsonlPydanticIngestSettings, with fields open to override."""

    def _factory(**overrides: Any) -> JsonlPydanticIngestSettings:  # noqa: ANN401
        log_config_file = tmp_path / "logging.conf"
        log_config_file.touch()
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)

        kwargs: dict[str, Any] = {
            "dataset_name": "test_dataset",
            "dev_mode": True,
            "entity_models_module": entity_models_module,
            "input_dir": str(input_dir),
            "log_config_file": str(log_config_file),
            "output_dir": str(output_dir),
            "use_destination": dlt_destination_config,
            "use_output_dir_for_pipeline_metadata": False,
        }
        kwargs.update(overrides)
        return JsonlPydanticIngestSettings(**kwargs)

    return _factory


@pytest.fixture
def scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Return a function that maps a scenario name to its data directory."""

    def _resolve(scenario: str) -> str:
        path = test_data_dir / "jsonlines" / scenario
        assert path.is_dir(), f"missing test fixture directory: {path}"
        return str(path)

    return _resolve


@pytest.fixture
def write_gzip_jsonl_file() -> Callable[[Path, str, list[str]], Path]:
    """Return a function that writes a list of JSON lines to a gzip-compressed file."""

    def _write(directory: Path, filename: str, lines: list[str]) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / filename
        content = ("\n".join(lines) + "\n").encode("utf-8")
        with gzip.open(file_path, "wb") as f:
            f.write(content)
        return file_path

    return _write
