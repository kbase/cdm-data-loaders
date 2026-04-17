"""Shared fixtures and helpers for MinIO-backed integration tests.

Integration tests are auto-skipped when MinIO is not reachable.  Each test
method gets its own bucket (derived from the test node name) that is emptied
on re-run but **never deleted** after the test — this lets developers inspect
the final state of the object store via the MinIO console.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import Any
from unittest.mock import patch

import boto3
import botocore.client
import pytest

import cdm_data_loaders.ncbi_ftp.manifest as manifest_mod
import cdm_data_loaders.ncbi_ftp.promote as promote_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.ncbi_ftp.assembly import build_accession_path
from cdm_data_loaders.utils.s3 import reset_s3_client

# ── MinIO connection defaults ───────────────────────────────────────────

MINIO_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Maximum length of a bucket name per S3/DNS spec
_MAX_BUCKET_LEN = 63


# ── MinIO reachability check ────────────────────────────────────────────

_minio_available: bool | None = None


def _minio_reachable() -> bool:
    """Return True if the MinIO endpoint accepts connections."""
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        client.list_buckets()
    except Exception:  # noqa: BLE001
        return False
    return True


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:  # noqa: ARG001
    """Auto-skip ``@pytest.mark.integration`` tests when MinIO is unreachable."""
    global _minio_available  # noqa: PLW0603
    if _minio_available is None:
        _minio_available = _minio_reachable()
    if _minio_available:
        return
    skip_marker = pytest.mark.skip(reason="MinIO not reachable — skipping integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def minio_s3_client() -> botocore.client.BaseClient:
    """Session-scoped real boto3 S3 client pointed at the local MinIO instance.

    Patches ``get_s3_client`` on every module that uses it so internal calls
    are transparently routed to MinIO.
    """
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    reset_s3_client()
    with (
        patch.object(s3_utils, "get_s3_client", return_value=client),
        patch.object(promote_mod, "get_s3_client", return_value=client),
        patch.object(manifest_mod, "head_object", wraps=s3_utils.head_object),
        patch.object(s3_utils, "_s3_client", client),
    ):
        yield client
    reset_s3_client()


def _bucket_name_from_node(node_id: str) -> str:
    """Derive a DNS-compliant S3 bucket name from a pytest node ID.

    :param node_id: e.g. ``tests/integration/test_promote_e2e.py::test_dry_run``
    :return: e.g. ``integ-test-dry-run``
    """
    # Extract test function name from the node ID
    parts = node_id.split("::")
    name = parts[-1] if parts else node_id
    # Lowercase, replace non-alphanumeric with hyphens, collapse multiples
    name = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    name = f"integ-{name}"
    if len(name) > _MAX_BUCKET_LEN:
        # Truncate but keep it unique via a short hash suffix
        suffix = hashlib.md5(name.encode()).hexdigest()[:6]  # noqa: S324
        name = f"{name[: _MAX_BUCKET_LEN - 7]}-{suffix}"
    return name


@pytest.fixture
def test_bucket(minio_s3_client: botocore.client.BaseClient, request: pytest.FixtureRequest) -> str:
    """Create a per-test-method bucket in MinIO and return its name.

    On re-run, any existing objects are deleted first so the test starts clean.
    The bucket is **not** deleted after the test.
    """
    bucket = _bucket_name_from_node(request.node.nodeid)
    s3 = minio_s3_client

    try:
        s3.head_bucket(Bucket=bucket)
        # Bucket exists — empty it for a clean run
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except s3.exceptions.NoSuchBucket:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
        else:
            raise

    return bucket


# ── Helpers ─────────────────────────────────────────────────────────────


def stage_files_to_minio(
    s3: botocore.client.BaseClient,
    bucket: str,
    local_dir: str | Path,
    staging_prefix: str,
) -> list[str]:
    """Upload a local directory tree to a MinIO staging prefix.

    :param s3: boto3 S3 client
    :param bucket: target bucket
    :param local_dir: local root directory to upload
    :param staging_prefix: S3 key prefix (e.g. ``"staging/run1/"``)
    :return: list of S3 keys uploaded
    """
    local_dir = Path(local_dir)
    keys: list[str] = []
    for path in sorted(local_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(local_dir)
        key = f"{staging_prefix.rstrip('/')}/{rel}"
        s3.upload_file(Filename=str(path), Bucket=bucket, Key=key)
        keys.append(key)
    return keys


def seed_lakehouse(  # noqa: PLR0913
    s3: botocore.client.BaseClient,
    bucket: str,
    accession: str,
    files: dict[str, str | bytes],
    path_prefix: str,
    assembly_dir: str | None = None,
) -> list[str]:
    """Seed assembly files at the final Lakehouse path in MinIO.

    :param s3: boto3 S3 client
    :param bucket: target bucket
    :param accession: assembly accession (e.g. ``"GCF_000001215.4"``)
    :param files: mapping of filename → content (str or bytes)
    :param path_prefix: Lakehouse prefix (e.g. ``"tenant-general-warehouse/…/ncbi/"``)
    :param assembly_dir: full assembly dir name; if None, uses ``accession``
    :return: list of S3 keys created
    """
    adir = assembly_dir or accession
    rel = build_accession_path(adir)
    keys: list[str] = []
    for fname, content in files.items():
        key = f"{path_prefix}{rel}{fname}"
        body = content.encode() if isinstance(content, str) else content
        md5 = hashlib.md5(body).hexdigest()  # noqa: S324
        s3.put_object(Bucket=bucket, Key=key, Body=body, Metadata={"md5": md5})
        keys.append(key)
    return keys


def list_all_keys(s3: botocore.client.BaseClient, bucket: str, prefix: str = "") -> list[str]:
    """List all object keys in a bucket under a prefix.

    :param s3: boto3 S3 client
    :param bucket: bucket name
    :param prefix: optional key prefix filter
    :return: sorted list of keys
    """
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


def get_object_metadata(s3: botocore.client.BaseClient, bucket: str, key: str) -> dict[str, Any]:
    """Return the user metadata dict for an S3 object.

    :param s3: boto3 S3 client
    :param bucket: bucket name
    :param key: object key
    :return: metadata dict
    """
    resp = s3.head_object(Bucket=bucket, Key=key)
    return resp.get("Metadata", {})
