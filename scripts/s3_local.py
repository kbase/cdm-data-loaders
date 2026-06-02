#!/usr/bin/env python3
# ruff: noqa: T201, EM101, EM102, TRY003, D103
"""Thin S3 CLI for local MinIO testing (no aws-cli install required).

Usage (all commands assume ``uv run`` from the repo root):

    uv run python scripts/s3_local.py mb  s3://cdm-lake
    uv run python scripts/s3_local.py cp  staging/raw_data/ s3://cdm-lake/staging/run1/raw_data/
    uv run python scripts/s3_local.py ls  s3://cdm-lake/staging/run1/
    uv run python scripts/s3_local.py head s3://cdm-lake/some/key.gz

Environment variables (with defaults for the walkthrough):

    AWS_ENDPOINT_URL         http://localhost:9000
    AWS_ACCESS_KEY_ID        minioadmin
    AWS_SECRET_ACCESS_KEY    minioadmin
"""

import os
import sys
from pathlib import Path

import cdm_data_loaders.utils.s3 as s3


def _client() -> None:
    s3.reset_s3_client()
    _ = s3.get_s3_client({
        "endpoint_url": os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000"),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    })


# dispatch

COMMANDS = {"mb": s3.cmd_mb, "cp": s3.cmd_cp, "ls": s3.cmd_ls, "head": s3.cmd_head}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:  # noqa: PLR2004
        cmds = ", ".join(COMMANDS)
        raise SystemExit(f"Usage: s3_local.py <{cmds}> [args ...]\n\n{__doc__}")
    _client()
    COMMANDS[sys.argv[1]](sys.argv[2:])


if __name__ == "__main__":
    main()
