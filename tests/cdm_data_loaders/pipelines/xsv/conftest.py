"""Fixtures shared by the XSV ingestion pipeline tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pytest

from cdm_data_loaders.pipelines.xsv.pipeline import XsvWorkPaths
from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import generate_header
from tests.helpers import make_cts_settings
from tests.xsv_fixtures import COLUMNS, write_xsv

VALID_SCHEMA_URI: Final[str] = "https://json-schema.org/draft/2020-12/schema"

# The `float` column is deliberately typed `["string", "null"]`, not `"number"`, matching the
# convention used throughout the xsv_validator test fixtures: it lets a field hold either a
# numeric-looking string or a null placeholder without needing float-parsing semantics.
DEFAULT_XSV_SCHEMA: Final[dict[str, Any]] = {
    "$schema": VALID_SCHEMA_URI,
    "title": "XSV integration test schema",
    "type": "object",
    "required": COLUMNS,
    "properties": {
        "number": {"type": "integer"},
        "date": {"type": "string"},
        "float": {"type": ["string", "null"]},
        "boolean": {"type": "boolean"},
        "string": {"type": "string"},
    },
    "x-xsv-config": {"x-delimiter": ",", "x-has-header": True},
}


@pytest.fixture
def settings_factory(tmp_path: Path, dlt_destination_config: str) -> Callable[..., XsvIngestSettings]:
    """Return a factory that builds a valid XsvIngestSettings, with fields open to override."""

    def _factory(
        schema: dict[str, Any] | None = DEFAULT_XSV_SCHEMA,
        data_files: dict[str, str] | None = None,
        **overrides: Any,  # noqa: ANN401
    ) -> XsvIngestSettings:
        input_dir = Path(overrides.get("input_dir", tmp_path / "input"))
        input_dir.mkdir(parents=True, exist_ok=True)

        if schema is not None:
            (input_dir / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
        if data_files:
            for file_name, content in data_files.items():
                (input_dir / file_name).write_text(content, encoding="utf-8")

        defaults: dict[str, Any] = {
            "dataset_name": "xsv_test_dataset",
            "file_glob": "*",
            "loader_file_format": "jsonl",
            "schema_file": str(input_dir / "schema.json"),
            "table_name": "my_table",
            "use_destination": dlt_destination_config,
        }
        defaults.update(overrides)
        return make_cts_settings(XsvIngestSettings, tmp_path, **defaults)

    return _factory


@pytest.fixture
def work_paths_factory(tmp_path: Path, qsv_cmd: str) -> Callable[[XsvIngestSettings], XsvWorkPaths]:
    """Return a factory that builds XsvWorkPaths (scratch dirs + header file) for one settings object."""

    def _factory(settings: XsvIngestSettings) -> XsvWorkPaths:
        tmp_dir = tmp_path / "tmp"
        qsv_output_dir = tmp_path / "qsv_output"
        validated_dir = tmp_path / "validated"
        for directory in (tmp_dir, qsv_output_dir, validated_dir):
            directory.mkdir(exist_ok=True)
        return XsvWorkPaths(
            qsv_cmd=qsv_cmd,
            header_file_path=generate_header(settings.validated_schema, tmp_dir),
            tmp_dir=tmp_dir,
            qsv_output_dir=qsv_output_dir,
            validated_dir=validated_dir,
        )

    return _factory


@pytest.fixture
def write_xsv_file() -> Callable[[XsvIngestSettings, str, str], Path]:
    """Return a function that writes text content to a new file inside settings.input_dir."""
    return lambda settings, file_name, content: write_xsv(Path(settings.input_dir), content, file_name)
