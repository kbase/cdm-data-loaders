"""Shared helpers for end-to-end, Ceph, and reference pipeline tests."""

import gzip
import hashlib
import json
import re
from collections.abc import Callable
from functools import lru_cache
from itertools import count
from pathlib import Path
from typing import Any, Final

import boto3
import botocore.config
import dlt
import xmltodict
from botocore.exceptions import BotoCoreError, ClientError
from dlt.common.pipeline import LoadInfo
from types_boto3_s3.client import S3Client

from cdm_data_loaders.readers.xml import DEFAULT_XMLTODICT_ARGS
from tests.helpers.cli_assertions import make_cts_settings
from tests.e2e.framework import PipelineTestRunner

MAX_BUCKET_LEN: Final[int] = 63


def write_reference_xml_as_jsonl(source_xml: Path, jsonl_path: Path) -> int:
    """Parse the UniRef XML with ``xmltodict`` and write one ``{"entry": ...}`` JSONL row per entry.

    :param source_xml: Path to gzipped UniRef XML file.
    :type source_xml: Path
    :param jsonl_path: Destination JSONL path.
    :type jsonl_path: Path
    :return: Number of written entry lines.
    :rtype: int
    """
    with gzip.open(source_xml, "rb") as fh:
        document = xmltodict.parse(fh.read(), **DEFAULT_XMLTODICT_ARGS)

    entries = document["UniRef50"]["entry"]
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("w", encoding="utf-8") as out:
        for entry in entries:
            out.write(json.dumps({"entry": entry}, ensure_ascii=False) + "\n")
    return len(entries)


def isolated_run(
    tmp_path: Path,
    dataset_prefix: str,
    run_fn: Callable[..., LoadInfo | None],
    base_kwargs: dict[str, Any],
    pipeline_runner: PipelineTestRunner | None = None,
) -> Callable[..., tuple[LoadInfo | None, Path]]:
    """Return a factory that runs a pipeline with per-run isolated output directories under ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dataset_prefix: Prefix for each run's unique dataset name.
    :type dataset_prefix: str
    :param run_fn: Pipeline run function to invoke.
    :type run_fn: Callable[..., LoadInfo | None]
    :param base_kwargs: Base settings keyword arguments including ``settings_cls``.
    :type base_kwargs: dict[str, Any]
    :param pipeline_runner: Optional ``PipelineTestRunner`` to dispatch through.
    :type pipeline_runner: PipelineTestRunner | None
    :return: Factory callable ``(**overrides) -> (load_info, output_dir)``.
    :rtype: Callable[..., tuple[LoadInfo | None, Path]]
    """
    run_counter = count()

    def _factory(**overrides: object) -> tuple[LoadInfo | None, Path]:
        run_index = next(run_counter)
        kwargs: dict[str, Any] = {
            "dataset_name": f"{dataset_prefix}_{run_index}",
            "log_config_file": str(tmp_path / f"logging_{run_index}.conf"),
            "output_dir": str(tmp_path / f"output_{run_index}"),
            **base_kwargs,
        }
        kwargs.update(overrides)
        settings_cls = kwargs.pop("settings_cls")
        settings = make_cts_settings(settings_cls, tmp_path, **kwargs)
        with dlt.config.values({"destination.local_fs.bucket_url": str(settings.output_dir)}):
            load_info = pipeline_runner.run(run_fn, settings) if pipeline_runner is not None else run_fn(settings)
        return load_info, settings.output_dir

    return _factory


@lru_cache(maxsize=1)
def ceph_reachable() -> bool:
    """Return ``True`` when the configured CEPH S3 endpoint accepts connections.

    :return: ``True`` if ``list_buckets`` succeeds within 1 second.
    :rtype: bool
    """
    try:
        s3 = boto3.Session().client(
            "s3",
            config=botocore.config.Config(
                connect_timeout=1,
                read_timeout=1,
                retries={"max_attempts": 1},
            ),
        )
        s3.list_buckets()
    except (BotoCoreError, ClientError, OSError, ValueError):
        return False
    return True


def bucket_name_from_node(node_id: str, prefix: str | None = None) -> str:
    """Derive a DNS-compliant S3 bucket name from a pytest node ID.

    :param node_id: Pytest node identifier string.
    :type node_id: str
    :param prefix: Optional bucket prefix segment.
    :type prefix: str | None
    :return: DNS-compliant lowercase bucket name.
    :rtype: str
    """
    parts = node_id.split("::")
    name = parts[-1] if parts else node_id
    name = f"integ-{prefix}-{name}" if prefix else f"integ-{name}"
    name = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if len(name) > MAX_BUCKET_LEN:
        suffix = hashlib.md5(name.encode("utf-8"), usedforsecurity=False).hexdigest()[:6]
        name = f"{name[: MAX_BUCKET_LEN - 7]}-{suffix}"
    return name


def check_existing_bucket(s3: S3Client, bucket: str) -> None:
    """Create ``bucket`` if absent, or empty all objects from ``bucket`` if it exists.

    :param s3: Boto3 ``S3Client`` instance.
    :type s3: S3Client
    :param bucket: Target bucket name.
    :type bucket: str
    """
    try:
        s3.head_bucket(Bucket=bucket)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except s3.exceptions.NoSuchBucket:
        s3.create_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
        else:
            raise
