"""Fixtures shared by the XSV ingestion pipeline tests."""

import json
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from cdm_data_loaders.pipelines.xsv.pipeline import XsvWorkPaths
from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import generate_header


@pytest.fixture(scope="session")
def qsv_cmd() -> str:
    """Provide the path/name of the qsv binary; override via the QSV_BIN env var if not on PATH.

    Tests using this fixture are skipped when the qsv binary is not available.
    """
    cmd = shutil.which("qsv") or os.environ.get("QSV_BIN")
    if not cmd or not Path(cmd).is_file():
        pytest.skip(f"qsv binary not found at {cmd!r}; set QSV_BIN to point at a real binary")
    return cmd


@pytest.fixture
def settings_factory(tmp_path: Path) -> Callable[..., XsvIngestSettings]:
    """Return a factory that builds a valid XsvIngestSettings, with fields open to override."""

    def _factory(schema: dict[str, Any], data_files: dict[str, str], **overrides: object) -> XsvIngestSettings:
        input_dir = tmp_path / "input"
        input_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir(exist_ok=True)

        (input_dir / "schema.json").write_text(json.dumps(schema))
        for file_name, content in data_files.items():
            (input_dir / file_name).write_text(content)

        kwargs: dict[str, object] = {
            "buffer_size": 100,
            "log_interval": 1000,
            "dataset_name": "xsv_test_dataset",
            "loader_file_format": "jsonl",
            "table_name": "my_table",
            "log_config_file": None,
            "dev_mode": False,
            "use_destination": "local_fs",
            "use_output_dir_for_pipeline_metadata": False,
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "file_glob": "*",
            "schema_file": "schema.json",
        }
        kwargs.update(overrides)
        return XsvIngestSettings(**kwargs)

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
