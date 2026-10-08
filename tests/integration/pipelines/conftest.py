"""Shared info and fixtures for pipeline tests."""

from collections.abc import Callable
from itertools import count
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines.core import LOAD_INFO_TABLE_NAME

DEFAULT_DLT_TABLES = {"_dlt_version", "_dlt_loads", "_dlt_pipeline_state", LOAD_INFO_TABLE_NAME}


def isolated_run(
    tmp_path: Path,
    dataset_prefix: str,
    run_fn: Callable[..., LoadInfo | None],
    base_kwargs: dict[str, Any],
) -> Callable[..., tuple[LoadInfo | None, Path]]:
    """Return a factory that runs a pipeline with per-run isolated output state.

    Each call gets a unique output_dir, dataset_name, and log config file under
    tmp_path, so repeated runs never collide or accumulate rows.

    :param tmp_path: pytest tmp_path for this test
    :type tmp_path: Path
    :param dataset_prefix: prefix for each run's unique dataset name
    :type dataset_prefix: str
    :param run_fn: the pipeline run function to invoke with built settings
    :type run_fn: Callable[..., LoadInfo | None]
    :param base_kwargs: base settings kwargs; keys open to override per call
    :type base_kwargs: dict[str, Any]
    :return: factory(chunk_dir=None, **overrides) -> (load_info, output_dir)
    :rtype: Callable[..., tuple[LoadInfo | None, Path]]
    """
    run_counter = count()

    def _factory(**overrides: Any) -> tuple[LoadInfo | None, Path]:
        run_index = next(run_counter)
        output_dir = tmp_path / f"output_{run_index}"
        output_dir.mkdir()
        log_config_file = tmp_path / f"logging_{run_index}.conf"
        log_config_file.touch()
        kwargs: dict[str, Any] = {
            "dataset_name": f"{dataset_prefix}_{run_index}",
            "log_config_file": str(log_config_file),
            "output_dir": str(output_dir),
            **base_kwargs,
        }
        kwargs.update(overrides)
        settings_cls = kwargs.pop("settings_cls")
        load_info = run_fn(settings_cls(**kwargs))
        return load_info, output_dir

    return _factory


@pytest.fixture
def isolated_run_factory(tmp_path: Path) -> Callable[..., Callable[..., tuple[LoadInfo | None, Path]]]:
    """Return a builder for isolated pipeline run factories bound to this test's tmp_path."""

    def _build(
        dataset_prefix: str, run_fn: Callable[..., LoadInfo | None], base_kwargs: dict[str, Any]
    ) -> Callable[..., tuple[LoadInfo | None, Path]]:
        return isolated_run(tmp_path, dataset_prefix, run_fn, base_kwargs)

    return _build
