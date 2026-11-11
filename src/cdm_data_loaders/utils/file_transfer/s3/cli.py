"""Helper functions for command-line boto3 usage."""
# No warnings for print statements
# ruff: noqa: T201

import json
from pathlib import Path

from botocore.exceptions import ClientError

from cdm_data_loaders.utils.file_transfer.s3 import client
from cdm_data_loaders.utils.file_transfer.s3.object_utils import list_objects, split_s3_path


def _parse_s3_arg(arg: str, *, allow_bucket_only: bool = False) -> tuple[str, str | None]:
    """Parse an S3 CLI argument into ``(bucket, key_or_prefix)`` or exit with the validation error."""
    try:
        return split_s3_path(arg, allow_bucket_only=allow_bucket_only)
    except ValueError as e:
        raise SystemExit(str(e)) from e


def cmd_mb(args: list[str]) -> None:
    """Create a bucket: ``mb s3://bucket``."""
    if not args or len(args) != 1:
        err_msg = "Usage: s3_local.py mb s3://BUCKET"
        raise SystemExit(err_msg)
    bucket, _ = _parse_s3_arg(args[0], allow_bucket_only=True)
    s3 = client.get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket already exists: {bucket}")
    except Exception:  # noqa: BLE001
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")


def cmd_cp(args: list[str]) -> None:
    """Recursive upload: ``cp LOCAL_DIR s3://bucket/prefix/``."""
    if len(args) != 2:  # noqa: PLR2004
        err_msg = "Usage: s3_local.py cp [LOCAL_DIR | LOCAL_FILE] s3://BUCKET[/PREFIX/]"
        raise SystemExit(err_msg)
    local_path = Path(args[0])
    bucket, prefix = _parse_s3_arg(args[1], allow_bucket_only=True)
    s3 = client.get_s3_client()
    count = 0
    if local_path.is_file():
        if not prefix:
            err_msg = "Usage: s3_local.py cp LOCAL_FILE s3://BUCKET/KEY"
            raise SystemExit(err_msg)
        s3.upload_file(Filename=str(local_path), Bucket=bucket, Key=prefix)
        count = 1
        print(f"  {prefix}")
    else:
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        for path in sorted(local_path.rglob("*")):
            if path.is_dir():
                continue
            rel = path.relative_to(local_path)
            key = f"{prefix}{rel}"
            s3.upload_file(Filename=str(path), Bucket=bucket, Key=key)
            count += 1
            print(f"  {key}")
    print(f"Uploaded {count} files to s3://{bucket}/{prefix}")


def cmd_ls(args: list[str]) -> None:
    """List objects: ``ls s3://bucket/prefix/ [--limit N]``."""
    if not args:
        err_msg = "Usage: s3_local.py ls s3://BUCKET[/PREFIX/] [--limit N]"
        raise SystemExit(err_msg)
    _parse_s3_arg(args[0], allow_bucket_only=True)
    limit = 20
    if "--limit" in args:
        idx = args.index("--limit")
        limit = int(args[idx + 1])
    for obj in list_objects(args[0], allow_bucket_only=True)[:limit]:
        print(f"  {obj['Size']:>10}  {obj['Key']}")


def cmd_head(args: list[str]) -> None:
    """Show metadata: ``head s3://bucket/key``."""
    if not args:
        err_msg = "Usage: s3_local.py head s3://BUCKET/KEY"
        raise SystemExit(err_msg)
    bucket, key = _parse_s3_arg(args[0])
    s3 = client.get_s3_client()
    meta = {}
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        meta = resp.get("Metadata", {})
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":  # type: ignore[union-attr]
            print(f"File not found in store: {bucket}/{key}")
            return
        raise
    print(f"Metadata for {bucket}/{key}:")
    print(json.dumps(meta, indent=2))
