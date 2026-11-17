"""Shared S3, manifest, and RefSeq summary helpers for NCBI FTP end-to-end tests."""

import hashlib
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import Any, Final

import pytest
from types_boto3_s3.client import S3Client

from cdm_data_loaders.datasets.ncbi_ftp.assembly import build_accession_path
from cdm_data_loaders.datasets.ncbi_ftp.summary import (
    AssemblyRecord,
    download_assembly_summary,
    filter_by_prefix_range,
    parse_assembly_summary,
)
from tests._support.paths_and_constants import TEST_BUCKET as TEST_BUCKET_STR

TEST_BUCKET: Final[PurePosixPath] = PurePosixPath(TEST_BUCKET_STR)
STABLE_PREFIX: Final[str] = "900"

s3_backends: Final[pytest.MarkDecorator] = pytest.mark.parametrize("s3_client", ["ceph", "mock"], indirect=True)


def md5_hex(data: bytes) -> str:
    """Return the lowercase hexadecimal MD5 digest of ``data``.

    :param data: Payload bytes.
    :type data: bytes
    :return: Hex-encoded MD5 digest.
    :rtype: str
    """
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def write_manifest(tmp_path: Path, accessions: list[str], name: str = "manifest.txt") -> Path:
    """Write a manifest file containing one accession per line.

    :param tmp_path: Target directory.
    :type tmp_path: Path
    :param accessions: Accession strings to write.
    :type accessions: list[str]
    :param name: Filename inside ``tmp_path``.
    :type name: str
    :return: Written manifest file path.
    :rtype: Path
    """
    path = tmp_path / name
    path.write_text("\n".join(accessions) + "\n", encoding="utf-8")
    return path


def stage_files_with_sidecars(
    s3: S3Client,
    staging_base: PurePosixPath,
    files: dict[PurePosixPath, bytes],
    *,
    bucket: PurePosixPath = TEST_BUCKET,
    with_md5: bool = True,
    with_crc64: bool = False,
) -> list[PurePosixPath]:
    """Stage files under ``staging_base`` in S3 with optional ``.md5`` and ``.crc64nvme`` sidecars.

    :param s3: Boto3 ``S3Client`` instance.
    :type s3: S3Client
    :param staging_base: Base S3 key prefix.
    :type staging_base: PurePosixPath
    :param files: Mapping of relative file paths to payload bytes.
    :type files: dict[PurePosixPath, bytes]
    :param bucket: Target S3 bucket name.
    :type bucket: PurePosixPath
    :param with_md5: Whether to upload ``.md5`` sidecar objects.
    :type with_md5: bool
    :param with_crc64: Whether to upload ``.crc64nvme`` sidecar objects.
    :type with_crc64: bool
    :return: Staged data file keys (excluding sidecar keys).
    :rtype: list[PurePosixPath]
    """
    keys: list[PurePosixPath] = []
    for fname, content in files.items():
        key = staging_base / fname
        s3.put_object(Bucket=str(bucket), Key=str(key), Body=content)
        keys.append(key)
        if with_md5:
            s3.put_object(
                Bucket=str(bucket),
                Key=str(key.with_name(f"{key.name}.md5")),
                Body=md5_hex(content).encode("utf-8"),
            )
        if with_crc64:
            s3.put_object(
                Bucket=str(bucket),
                Key=str(key.with_name(f"{key.name}.crc64nvme")),
                Body=b"fake-crc",
            )
    return keys


def download_and_filter_refseq(
    prefix: str = STABLE_PREFIX,
) -> tuple[dict[str, AssemblyRecord], dict[str, AssemblyRecord]]:
    """Download the RefSeq assembly summary from NCBI FTP and filter to ``prefix``.

    :param prefix: Three-digit accession prefix range bound.
    :type prefix: str
    :return: Pair ``(full_parsed, filtered)`` of accession-to-record dicts.
    :rtype: tuple[dict[str, AssemblyRecord], dict[str, AssemblyRecord]]
    """
    raw = download_assembly_summary(database="refseq")
    full = parse_assembly_summary(raw)
    filtered = filter_by_prefix_range(full, prefix_from=prefix, prefix_to=prefix)
    return full, filtered


def clone_assemblies(assemblies: dict[str, AssemblyRecord]) -> dict[str, AssemblyRecord]:
    """Return a copy of ``assemblies`` with cloned ``AssemblyRecord`` instances.

    :param assemblies: Source accession-to-record mapping.
    :type assemblies: dict[str, AssemblyRecord]
    :return: Cloned mapping safe for in-place test mutation.
    :rtype: dict[str, AssemblyRecord]
    """
    return {acc: replace(rec) for acc, rec in assemblies.items()}


def stage_files_to_s3(
    s3: S3Client,
    bucket: PurePosixPath,
    local_dir: Path,
    staging_prefix: PurePosixPath,
) -> list[PurePosixPath]:
    """Upload a local directory tree to ``staging_prefix`` in ``bucket``.

    :param s3: Boto3 ``S3Client`` instance.
    :type s3: S3Client
    :param bucket: Target S3 bucket name.
    :type bucket: PurePosixPath
    :param local_dir: Local root directory to upload.
    :type local_dir: Path
    :param staging_prefix: Target S3 key prefix.
    :type staging_prefix: PurePosixPath
    :return: Uploaded S3 keys.
    :rtype: list[PurePosixPath]
    """
    local_root = Path(local_dir)
    keys: list[PurePosixPath] = []
    for path in sorted(local_root.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(local_root)
        key = staging_prefix / rel
        s3.upload_file(Filename=str(path), Bucket=str(bucket), Key=str(key))
        keys.append(key)
    return keys


def seed_lakehouse(
    s3: S3Client,
    bucket: PurePosixPath,
    accession: str,
    files: dict[PurePosixPath, str | bytes],
    path_prefix: PurePosixPath,
    assembly_dir: PurePosixPath | None = None,
) -> list[PurePosixPath]:
    """Seed assembly files with MD5 user metadata at the Lakehouse destination path.

    :param s3: Boto3 ``S3Client`` instance.
    :type s3: S3Client
    :param bucket: Target S3 bucket name.
    :type bucket: PurePosixPath
    :param accession: Assembly accession identifier.
    :type accession: str
    :param files: Mapping of relative file paths to text or byte contents.
    :type files: dict[PurePosixPath, str | bytes]
    :param path_prefix: Lakehouse key prefix.
    :type path_prefix: PurePosixPath
    :param assembly_dir: Optional full assembly directory name; defaults to ``accession``.
    :type assembly_dir: PurePosixPath | None
    :return: Seeded S3 keys.
    :rtype: list[PurePosixPath]
    """
    adir = assembly_dir or PurePosixPath(accession)
    rel = build_accession_path(adir)
    keys: list[PurePosixPath] = []
    prefix = PurePosixPath(path_prefix)
    for fname, content in files.items():
        key = prefix / rel / fname
        body = content.encode("utf-8") if isinstance(content, str) else content
        md5 = md5_hex(body)
        s3.put_object(Bucket=str(bucket), Key=str(key), Body=body, Metadata={"md5": md5})
        keys.append(key)
    return keys


def list_all_keys(
    s3: S3Client,
    bucket: PurePosixPath,
    prefix: PurePosixPath | None = None,
) -> list[PurePosixPath]:
    """List all object keys in ``bucket`` under an optional ``prefix``.

    :param s3: Boto3 ``S3Client`` instance.
    :type s3: S3Client
    :param bucket: Bucket name.
    :type bucket: PurePosixPath
    :param prefix: Optional key prefix filter.
    :type prefix: PurePosixPath | None
    :return: Sorted list of S3 object keys.
    :rtype: list[PurePosixPath]
    """
    keys: list[PurePosixPath] = []
    paginator = s3.get_paginator("list_objects_v2")
    paginate_kwargs: dict[str, str] = {"Bucket": str(bucket)}
    if prefix is not None:
        paginate_kwargs["Prefix"] = str(prefix)
    for page in paginator.paginate(**paginate_kwargs):
        keys.extend(PurePosixPath(obj["Key"]) for obj in page.get("Contents", []))
    return sorted(keys)


def get_object_metadata(s3: S3Client, bucket: PurePosixPath, key: PurePosixPath) -> dict[str, Any]:
    """Return the user metadata dictionary for an S3 object via ``HeadObject``.

    :param s3: Boto3 ``S3Client`` instance.
    :type s3: S3Client
    :param bucket: Bucket name.
    :type bucket: PurePosixPath
    :param key: S3 object key.
    :type key: PurePosixPath
    :return: User metadata dictionary.
    :rtype: dict[str, Any]
    """
    resp = s3.head_object(Bucket=str(bucket), Key=str(key))
    return resp.get("Metadata", {})
