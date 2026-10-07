"""Shared constants and helpers for the cdm_data_loaders.pipelines.core tests."""

import gzip
import json
import logging
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from typing import Any, Final

import dlt
import pytest
from dlt.extract import DltResource

from cdm_data_loaders.core.settings import LoggerSettings
from cdm_data_loaders.pipelines import core

TINY_RESOURCE_DATA: Final[tuple[dict[str, Any], ...]] = (
    {"entity_id": "tiny:one", "value": 1},
    {"entity_id": "tiny:two", "value": 2},
)
TINY_TABLE_NAME: Final[str] = "tiny"
TINY_PIPELINE_NAME: Final[str] = "test_core_tiny_pipeline"
TINY_DATASET_NAME: Final[str] = "core_test_dataset"
FAILING_RESOURCE_MESSAGE: Final[str] = "Oh crap!!"

SLACK_ENV_VARS: Final[dict[str, str]] = {"VARIABLE_B": "BBB", "VARIABLE_T": "TTT", "CHAR_STR": "CCC"}
SLACK_HOOK_FROM_ENV: Final[str] = "https://hooks.slack.com/services/BBB/TTT/CCC/"
SLACK_HOOK_ENV_VAR: Final[str] = "RUNTIME__SLACK_INCOMING_HOOK"
NO_SLACK_ALERTS_MESSAGE: Final[str] = f"No Slack alerts will be sent: {core.WEBHOOK_NOT_CONFIGURED}"


def tiny_resource() -> DltResource:
    """Return a new dlt resource that emits the two tiny records."""
    return dlt.resource(deepcopy(list(TINY_RESOURCE_DATA)), name=TINY_TABLE_NAME)


def failing_resource() -> DltResource:
    """Return a new dlt resource that emits one record and then raises RuntimeError."""

    def _generate() -> Iterator[dict[str, Any]]:
        yield deepcopy(TINY_RESOURCE_DATA[0])
        raise RuntimeError(FAILING_RESOURCE_MESSAGE)

    return dlt.resource(_generate, name=TINY_TABLE_NAME)


def collect_items(resource: DltResource) -> list[Any]:
    """Iterate over a resource and return its items, flattening any pages (lists) of items."""
    items: list[Any] = []
    for page in resource:
        if isinstance(page, list):
            items.extend(page)
        else:
            items.append(page)
    return items


def read_table_jsonl(
    output_dir: str | Path,
    table_name: str,
    keep_dlt_columns: bool = False,
) -> list[dict[str, Any]]:
    """Read every jsonl record written for a table below the output directory.

    :param output_dir: root directory that the destination wrote to
    :param table_name: name of the table (the directory that holds its files)
    :param keep_dlt_columns: if False, drop columns whose names start with ``_dlt``
    :return: the records, ordered by file name
    """
    records: list[dict[str, Any]] = []
    for path in sorted(Path(output_dir).glob(f"**/{table_name}/*.jsonl*")):
        opener = gzip.open(path, "rt", encoding="utf-8") if path.name.endswith(".gz") else path.open(encoding="utf-8")
        with opener as f:
            records.extend(json.loads(line) for line in f if line.strip())
    if keep_dlt_columns:
        return records
    return [{k: v for k, v in record.items() if not k.startswith("_dlt")} for record in records]


def core_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return the captured log records emitted by the core module logger."""
    return [record for record in caplog.records if record.name == core.__name__]


def core_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the formatted messages of the log records emitted by the core module logger."""
    return [record.getMessage() for record in core_records(caplog)]


def make_raising_settings_cls(exc: BaseException) -> type[LoggerSettings]:
    """Return a settings class that raises ``exc`` when it is instantiated."""

    class RaisingSettings(LoggerSettings):
        def __init__(self, **data: object) -> None:
            raise exc

    return RaisingSettings
