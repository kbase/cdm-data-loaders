"""Shared fixtures for jsonlines_ingest pipeline tests."""

import gzip
import sys
import types
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any
from uuid import uuid4
from itertools import count
import json
import dlt
import pytest
from pydantic import BaseModel, Field
from dlt.common.pipeline import LoadInfo
import re
import pyarrow.parquet as pq

import cdm_data_loaders.pipelines.jsonlines.extract_pipeline as extract_module
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import run_jsonlines_ingest_pipeline, jsonl_reader
from cdm_data_loaders.pipelines.jsonlines.extract_validate_pipeline import run_jsonlines_ingest_with_validation_pipeline

from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings, JsonlIngestSettings
from tests.conftest import TEST_DATA_DIR

# "2014-08-21T09:55:24.333"
DATETIME_REGEX = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}")


@pytest.fixture
def pure_pipeline(jsonl_reference_entities: list[dict[str, Any]], tmp_path: Path) -> dict[str, list[dict]]:

    pipeline = dlt.pipeline(
        pipeline_name="whatever",
        destination="dummy",
        dataset_name="test_dataset",
        pipelines_dir=str(tmp_path / "pipeline"),
    )

    resource = dlt.resource(jsonl_reference_entities, name="dataset", max_table_nesting=0)
    pipeline.extract(resource)
    normalize_info = pipeline.normalize()
    # read the actual normalized rows:
    rows_by_table: dict[str, list[dict]] = {}
    for package in normalize_info.load_packages:
        for job in package.jobs["new_jobs"]:
            table = job.job_file_info.table_name
            if table.startswith("_dlt"):
                continue
            fmt = job.job_file_info.file_format
            rows = rows_by_table.setdefault(table, [])
            if fmt in ("jsonl", "typed-jsonl"):
                opener = gzip.open if job.job_file_info.is_compressed else open
                with opener(job.file_path, "rb") as f:
                    rows.extend(json.loads(line) for line in f if line.strip())
            elif fmt == "parquet":
                rows.extend(pq.read_table(job.file_path).to_pylist())

    return {
        table: [{key: value for key, value in row.items() if not key.startswith("_dlt")} for row in rows]
        for table, rows in rows_by_table.items()
    }


def add_precision(matchobj) -> str:
    return matchobj.group(0) + "000+00:00"


@pytest.fixture(scope="session")
def jsonl_reference_entities(reference_jsonl: Path) -> list[dict[str, Any]]:
    """Parsed JSONL file."""
    with reference_jsonl.open(encoding="utf-8") as fh:
        return [json.loads(line.strip()) for line in fh if line.strip()]


@pytest.fixture(scope="session")
def reference_jsonl() -> Path:
    """Path to the reference JSONL file."""
    return TEST_DATA_DIR / "ncbi_rest_api" / "dataset_report" / "chunk_52" / "dataset" / "dataset_reports_52.jsonl"


@pytest.fixture
def reference_data_dir() -> Path:
    return TEST_DATA_DIR / "ncbi_rest_api" / "dataset_report"


@pytest.fixture
def sorted_reference_entities(jsonl_reference_entities) -> list[str]:
    lines_as_json = [json.dumps(obj, sort_keys=True, default=str) for obj in jsonl_reference_entities]
    edited_lines = [re.sub(DATETIME_REGEX, add_precision, line) for line in lines_as_json]
    return sorted(edited_lines)


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

    def _factory(**overrides: Any) -> JsonlPydanticIngestSettings:
        log_config_file = tmp_path / "logging.json"
        log_config_file.write_text('{"version": 1}')
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)

        kwargs: dict[str, Any] = {
            "dataset_name": "test_dataset",
            "dev_mode": False,
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


@pytest.fixture
def run_jsonlines_pipeline(
    tmp_path: Path, reference_data_dir: Path, dlt_destination_config: str, monkeypatch
) -> Callable[..., Any]:
    """Run the JSONL extraction-only pipeline."""
    run_counter = count()
    original_reader = jsonl_reader

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:
        run_index = next(run_counter)
        fresh_reader = dlt.transformer(
            original_reader.__wrapped__,
            name=f"jsonl_reader_{run_index}",
            parallelized=True,
        )
        monkeypatch.setattr(extract_module, "jsonl_reader", fresh_reader)

        output_dir = tmp_path / f"output_{run_index}"
        output_dir.mkdir()
        log_config_file = tmp_path / "logging.conf"
        log_config_file.touch()
        kwargs = {
            "buffer_size": 10,  # overrides.get("buffer_size", 10),
            "dataset_name": f"jsonl_extract_run_{run_index}",
            "dev_mode": False,
            "file_glob": "**/*.jsonl*",
            "input_dir": str(reference_data_dir / chunk_dir),
            "log_config_file": str(log_config_file),
            "output_dir": str(output_dir),
            "use_destination": dlt_destination_config,
            "use_output_dir_for_pipeline_metadata": False,
        }
        kwargs.update(overrides)
        settings = JsonlIngestSettings(**kwargs)
        load_info: LoadInfo | None = run_jsonlines_ingest_pipeline(settings)
        return (load_info, output_dir)

    return _factory


@pytest.fixture
def run_jsonl_extract_validate_pipeline(
    tmp_path: Path, reference_data_dir: Path, dlt_destination_config: str, monkeypatch
) -> Callable[..., Any]:
    """Return a factory that runs the jsonlines pipeline on a chunk dir with full isolation.

    Each run gets a unique output_dir and dataset_name under tmp_path, and a new
    module-level jsonlines transformer, so parallel-safe repeated runs
    never collide or accumulate rows.
    """
    run_counter = count()

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:
        run_index = next(run_counter)

        output_dir = tmp_path / f"output_{run_index}"
        output_dir.mkdir()
        log_config_file = tmp_path / "logging.conf"
        log_config_file.touch()
        kwargs = {
            "buffer_size": 10,  # overrides.get("buffer_size", 10),
            "dataset_name": f"jsonl_extract_validate_run_{run_index}",
            "dev_mode": False,
            "file_glob": "*.jsonl*",
            "input_dir": str(reference_data_dir / chunk_dir),
            "log_config_file": str(log_config_file),
            "output_dir": str(output_dir),
            "table_name": "entry",
            "use_destination": dlt_destination_config,
            "use_output_dir_for_pipeline_metadata": False,
        }
        kwargs.update(overrides)
        settings = JsonlPydanticIngestSettings(**kwargs)
        load_info: LoadInfo | None = run_jsonlines_ingest_with_validation_pipeline(settings)
        return (load_info, output_dir)

    return _factory
