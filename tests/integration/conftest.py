"""Shared fixtures and helpers for CEPH-backed integration tests.

Each test method gets its own bucket (derived from the test node name) that is
emptied on re-run but **never deleted** after the test — this lets developers
inspect the final state of the object store. The CEPH dashboard does not currently
allow inspection of the store, but the `s3_local` command-line tool can be used.
"""

import hashlib
import re
from collections.abc import Generator
from pathlib import Path, PurePosixPath
from typing import Any
from unittest.mock import patch

import boto3
import botocore.client
import botocore.config
import pytest
from botocore.exceptions import ClientError

import cdm_data_loaders.ncbi_ftp.manifest as manifest_mod
from cdm_data_loaders.ncbi_ftp.assembly import build_accession_path
from cdm_data_loaders.utils.file_transfer.s3 import client, object_utils
from cdm_data_loaders.utils.file_transfer.s3.client import reset_s3_client

# Maximum length of a bucket name per S3/DNS spec
_MAX_BUCKET_LEN = 63


# CEPH reachability check

_ceph_available: bool | None = None


def _ceph_reachable() -> bool:
    """Return True if the CEPH endpoint accepts connections."""
    try:
        client = boto3.client(
            "s3",
            config=botocore.config.Config(
                connect_timeout=1,
                read_timeout=1,
                retries={"max_attempts": 1},
            ),
        )
        client.list_buckets()
    except Exception:  # noqa: BLE001
        return False
    return True


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Fail CEPH-required tests early when CEPH is unavailable."""
    if "requires_ceph" not in item.keywords:
        return

    global _ceph_available  # noqa: PLW0603
    if _ceph_available is None:
        _ceph_available = _ceph_reachable()

    if not _ceph_available:
        pytest.fail(
            "CEPH not reachable. Start a CEPH test store or deselect with -m 'not requires_ceph'.",
            pytrace=False,
        )


# Fixtures


@pytest.fixture
def ceph_s3_client() -> Generator[botocore.client.BaseClient]:
    """Session-scoped real boto3 S3 client pointed at the local CEPH instance.

    Patches ``get_s3_client`` on every module that uses it so internal calls
    are transparently routed to CEPH.
    """
    s3_client = boto3.client("s3")

    reset_s3_client()
    with (
        patch.object(client, "get_s3_client", return_value=s3_client),
        patch.object(client, "_s3_client", s3_client),
        patch.object(manifest_mod, "head_object", wraps=object_utils.head_object),
    ):
        yield s3_client
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
def test_bucket(ceph_s3_client: botocore.client.BaseClient, request: pytest.FixtureRequest) -> PurePosixPath:
    """Create a per-test-method bucket in CEPH and return its name.

    On re-run, any existing objects are deleted first so the test starts clean.
    The bucket is **not** deleted after the test.
    """
    bucket = _bucket_name_from_node(request.node.nodeid)
    s3 = ceph_s3_client

    try:
        s3.head_bucket(Bucket=bucket)
        # Bucket exists — empty it for a clean run
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except s3.exceptions.NoSuchBucket:
        s3.create_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
        else:
            raise

    return PurePosixPath(bucket)


@pytest.fixture
def staging_test_bucket(ceph_s3_client: botocore.client.BaseClient, request: pytest.FixtureRequest) -> PurePosixPath:
    """Create a per-test staging bucket in CEPH and return its name.

    Mirrors ``test_bucket`` but uses a ``staging-`` prefix so staging and
    Lakehouse buckets are distinct within the same test.
    """
    bucket = "staging-" + _bucket_name_from_node(request.node.nodeid)
    if len(bucket) > _MAX_BUCKET_LEN:
        suffix = hashlib.md5(bucket.encode()).hexdigest()[:6]  # noqa: S324
        bucket = f"{bucket[: _MAX_BUCKET_LEN - 7]}-{suffix}"
    s3 = ceph_s3_client

    try:
        s3.head_bucket(Bucket=bucket)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except s3.exceptions.NoSuchBucket:
        s3.create_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
        else:
            raise

    return PurePosixPath(bucket)


# Helpers


def stage_files_to_ceph(
    s3: botocore.client.BaseClient,
    bucket: PurePosixPath,
    local_dir: Path,
    staging_prefix: PurePosixPath,
) -> list[PurePosixPath]:
    """Upload a local directory tree to a CEPH staging prefix.

    :param s3: boto3 S3 client
    :param bucket: target bucket
    :param local_dir: local root directory to upload
    :param staging_prefix: S3 key prefix (e.g. ``"staging/run1/"``)
    :return: list of S3 keys uploaded
    """
    local_dir = Path(local_dir)
    keys: list[PurePosixPath] = []
    for path in sorted(local_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(local_dir)
        key = staging_prefix / rel
        s3.upload_file(Filename=str(path), Bucket=str(bucket), Key=str(key))
        keys.append(key)
    return keys


def seed_lakehouse(
    s3: botocore.client.BaseClient,
    bucket: PurePosixPath,
    accession: str,
    files: dict[PurePosixPath, str | bytes],
    path_prefix: PurePosixPath,
    assembly_dir: PurePosixPath | None = None,
) -> list[PurePosixPath]:
    """Seed assembly files at the final Lakehouse path in CEPH.

    :param s3: boto3 S3 client
    :param bucket: target bucket
    :param accession: assembly accession (e.g. ``"GCF_000001215.4"``)
    :param files: mapping of filename → content (str or bytes)
    :param path_prefix: Lakehouse prefix (e.g. ``"tenant-general-warehouse/…/ncbi/"``)
    :param assembly_dir: full assembly dir name; if None, uses ``accession``
    :return: list of S3 keys created
    """
    adir = assembly_dir or PurePosixPath(accession)
    rel = build_accession_path(adir)
    keys: list[PurePosixPath] = []
    prefix = PurePosixPath(path_prefix)
    for fname, content in files.items():
        key = prefix / rel / fname
        body = content.encode() if isinstance(content, str) else content
        md5 = hashlib.md5(body).hexdigest()  # noqa: S324
        s3.put_object(Bucket=str(bucket), Key=str(key), Body=body, Metadata={"md5": md5})
        keys.append(key)
    return keys


def list_all_keys(
    s3: botocore.client.BaseClient, bucket: PurePosixPath, prefix: PurePosixPath | None = None
) -> list[PurePosixPath]:
    """List all object keys in a bucket under a prefix.

    :param s3: boto3 S3 client
    :param bucket: bucket name
    :param prefix: optional key prefix filter
    :return: sorted list of keys
    """
    keys: list[PurePosixPath] = []
    paginator = s3.get_paginator("list_objects_v2")
    paginate_kwargs: dict[str, str] = {"Bucket": str(bucket)}
    if prefix is not None:
        paginate_kwargs["Prefix"] = str(prefix)
    for page in paginator.paginate(**paginate_kwargs):
        keys.extend(PurePosixPath(obj["Key"]) for obj in page.get("Contents", []))
    return sorted(keys)


def get_object_metadata(s3: botocore.client.BaseClient, bucket: PurePosixPath, key: PurePosixPath) -> dict[str, Any]:
    """Return the S3 user metadata dict for an S3 object (from HeadObject).

    :param s3: boto3 S3 client
    :param bucket: bucket name
    :param key: object key
    :return: user metadata dict
    """
    resp = s3.head_object(Bucket=str(bucket), Key=str(key))
    return resp.get("Metadata", {})
