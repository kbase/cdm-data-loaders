#!/usr/bin/env python3
# ruff: noqa: T201, EM101, EM102, TRY003, D103
"""Thin S3 CLI for local MinIO testing (no aws-cli install required).

Usage (all commands assume ``uv run`` from the repo root):

    uv run python scripts/s3_local.py mb  s3://cdm-lake
    uv run python scripts/s3_local.py cp  staging/raw_data/ s3://cdm-lake/staging/run1/raw_data/
    uv run python scripts/s3_local.py ls  s3://cdm-lake/staging/run1/
    uv run python scripts/s3_local.py head s3://cdm-lake/some/key.gz

Environment variables (with defaults for the walkthrough):

    MINIO_ENDPOINT_URL  http://localhost:9000
    MINIO_ACCESS_KEY    minioadmin
    MINIO_SECRET_KEY    minioadmin
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3
from botocore.client import BaseClient


def _client() -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def _split(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise SystemExit(f"Expected s3:// URI, got: {uri}")
    parts = uri[5:].split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


# ── subcommands ─────────────────────────────────────────────────────────


def cmd_mb(args: list[str]) -> None:
    """Create a bucket: ``mb s3://bucket``."""
    if not args:
        raise SystemExit("Usage: s3_local.py mb s3://BUCKET")
    bucket, _ = _split(args[0])
    s3 = _client()
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket already exists: {bucket}")
    except Exception:  # noqa: BLE001
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")


def cmd_cp(args: list[str]) -> None:
    """Recursive upload: ``cp LOCAL_DIR s3://bucket/prefix/``."""
    if len(args) < 2:  # noqa: PLR2004
        raise SystemExit("Usage: s3_local.py cp LOCAL_DIR s3://BUCKET/PREFIX/")
    local_dir = Path(args[0])
    bucket, prefix = _split(args[1])
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    s3 = _client()
    count = 0
    for path in sorted(local_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(local_dir)
        key = f"{prefix}{rel}"
        s3.upload_file(Filename=str(path), Bucket=bucket, Key=key)
        count += 1
        print(f"  {key}")
    print(f"Uploaded {count} files to s3://{bucket}/{prefix}")


def cmd_ls(args: list[str]) -> None:
    """List objects: ``ls s3://bucket/prefix/ [--limit N]``."""
    if not args:
        raise SystemExit("Usage: s3_local.py ls s3://BUCKET/PREFIX/ [--limit N]")
    bucket, prefix = _split(args[0])
    limit = 20
    if "--limit" in args:
        idx = args.index("--limit")
        limit = int(args[idx + 1])
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    shown = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(f"  {obj['Size']:>10}  {obj['Key']}")
            shown += 1
            if shown >= limit:
                return


def cmd_head(args: list[str]) -> None:
    """Show metadata: ``head s3://bucket/key``."""
    if not args:
        raise SystemExit("Usage: s3_local.py head s3://BUCKET/KEY")
    bucket, key = _split(args[0])
    s3 = _client()
    resp = s3.head_object(Bucket=bucket, Key=key)
    meta = resp.get("Metadata", {})
    print(json.dumps(meta, indent=2))


# ── dispatch ────────────────────────────────────────────────────────────

COMMANDS = {"mb": cmd_mb, "cp": cmd_cp, "ls": cmd_ls, "head": cmd_head}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:  # noqa: PLR2004
        cmds = ", ".join(COMMANDS)
        raise SystemExit(f"Usage: s3_local.py <{cmds}> [args ...]\n\n{__doc__}")
    COMMANDS[sys.argv[1]](sys.argv[2:])


if __name__ == "__main__":
    main()
