"""Utilities for s3 interaction."""

from pathlib import Path
from types import ModuleType
from typing import Any

import boto3
import botocore
import botocore.client
import tqdm
from botocore.config import Config

CDM_LAKE_BUCKET = "cdm-lake"
DEFAULT_EXTRA_ARGS = {"ChecksumAlgorithm": "CRC64NVME"}

VALID_S3_PREFIXES = ["s3://", "s3a://"]
VALID_BUCKETS = [CDM_LAKE_BUCKET, "cts"]

# "legacy", "standard", "adaptive"
AWS_CLIENT_RETRY_MODE = "adaptive"
# how many times to retry, including the initial attempt
AWS_CLIENT_TOTAL_MAX_ATTEMPTS = 10


_s3_client: botocore.client.BaseClient | None = None


def get_s3_client(args: dict[str, str] | None = None) -> botocore.client.BaseClient:
    """Create an S3 client using the provided arguments.

    The client is created once and cached for subsequent calls. Call
    reset_s3_client() to force a new client to be created on the next call.

    :param args: arguments for creating the S3 client, defaults to None
    :type args: dict[str, str] | None, optional
    :raises ValueError: if required arguments for creating the S3 client are missing
    :return: initialised s3 client
    :rtype: botocore.client.BaseClient
    """
    global _s3_client  # noqa: PLW0603
    if _s3_client is not None:
        return _s3_client

    config = Config(retries={"total_max_attempts": AWS_CLIENT_TOTAL_MAX_ATTEMPTS, "mode": AWS_CLIENT_RETRY_MODE})

    if not args:
        # try using env vars and skip manual configuration
        client = boto3.client("s3", config=config)
        # check for credentials and endpoint_url
        credentials = client._request_signer._credentials  # noqa: SLF001
        if credentials.access_key and credentials.secret_key and client.meta.endpoint_url:
            _s3_client = client
            return _s3_client

        try:
            from berdl_notebook_utils.berdl_settings import get_settings  # noqa: PLC0415

            settings = get_settings()
            args = {
                "endpoint_url": settings.MINIO_ENDPOINT_URL,
                "aws_access_key_id": settings.MINIO_ACCESS_KEY,
                "aws_secret_access_key": settings.MINIO_SECRET_KEY,
            }
        except (ModuleNotFoundError, ImportError, NameError) as e:
            print(e)  # noqa: T201
            raise

    required_args = ["endpoint_url", "aws_access_key_id", "aws_secret_access_key"]
    keyword_args = {kw: args.get(kw) for kw in required_args}
    missing = [kw for kw in required_args if not keyword_args[kw]]
    if missing:
        msg = "Cannot initialise s3 client: missing arguments: " + ", ".join(missing)
        raise ValueError(msg)

    _s3_client = boto3.client("s3", config=config, **keyword_args)
    return _s3_client


def reset_s3_client() -> None:
    """Reset the cached S3 client, forcing a new one to be created on the next call to get_s3_client."""
    global _s3_client  # noqa: PLW0603
    _s3_client = None


def split_s3_path(s3_path: str) -> tuple[str | None, str]:
    """Convert a full s3 path (including bucket) into a bucket and key pair.

    Returns a tuple of bucket, key

    :param s3_path: an s3 path, including the bucket name
    :type s3_path: str
    :return: tuple of (bucket, key)
    :rtype: tuple[str | None, str]
    """
    if "://" in s3_path:
        # remove the protocol prefix
        (_, unprefixed_path) = s3_path.split("://", 1)
    else:
        unprefixed_path = s3_path

    if not unprefixed_path:
        # raises a value error
        err_msg = f"Invalid path: '{s3_path}\nNo path found"
        raise ValueError(err_msg)

    if unprefixed_path.startswith("/"):
        err_msg = f"Invalid path: '{s3_path}'\ns3 paths must start with the bucket name"
        raise ValueError(err_msg)

    path_parts = unprefixed_path.split("/", 1)
    # the first part should be the bucket and the second part the key
    if len(path_parts) != 2 or not path_parts[1]:  # noqa: PLR2004
        err_msg = f"Invalid path: '{s3_path}'\nCould not parse out bucket and key"
        raise ValueError(err_msg)

    return (path_parts[0], path_parts[1])


def list_matching_objects(s3_path: str) -> list[dict[str, Any]]:
    """List the remote paths that start with ``s3_path``.

    Note: since s3 paths are basically cosmetic, this function returns all paths that start with
    ``s3_path`` minus the bucket name.

    Retrieves all objects under the given prefix; collects all pages of results if there are more than
    1000 files (the max retrievable per `list_objects_v2` query) present.

    :param s3_path: directory to be listed, INCLUDING the bucket name
    :type s3_path: str
    :return: list of object metadata dicts in the directory
    :rtype: list[dict[str, Any]]
    """
    s3 = get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=key)

    contents = []
    for page in page_iterator:
        contents.extend(page.get("Contents", []))

    return contents


def object_exists(s3_path: str) -> bool:
    """Check whether an object exists on s3.

    :param s3_path: path to the object on s3, INCLUDING the bucket name
    :type s3_path: str
    :return: True if the object exists, False otherwise
    :rtype: bool
    """
    s3 = get_s3_client()

    (bucket, key) = split_s3_path(s3_path)
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        error_string = str(e)
        if not error_string.startswith("An error occurred (404) when calling the HeadObject operation: Not Found"):
            print(f"Error performing head operation on s3 object: {e!s}")  # noqa: T201
        return False
    return True


def upload_file(
    local_file_path: Path | str,
    destination_dir: str,
    object_name: str | None = None,
    metadata: dict[str, str] | None = None,
) -> bool:
    """Upload an object to an S3 bucket.

    When *metadata* is supplied the file is always uploaded (no existence check)
    and the dict is attached as S3 user metadata.  When *metadata* is ``None``
    (the default) the existing behaviour is preserved: the upload is skipped if
    the object is already present.

    :param local_file_path: File to upload
    :type local_file_path: Path | str
    :param destination_dir: path to the destination directory on s3, INCLUDING the bucket name and EXCLUDING the file name
    :type destination_dir: str
    :param object_name: S3 object name. If not specified, the name of the file from local_file_path is used.
    :type object_name: str | None
    :param metadata: user metadata key/value pairs to attach to the object; when provided the upload always runs
    :type metadata: dict[str, str] | None
    :return: True if file was uploaded, else False
    :rtype: bool
    """
    if isinstance(local_file_path, str):
        local_file_path = Path(local_file_path)

    if not destination_dir:
        msg = "No destination directory supplied for the file"
        raise ValueError(msg)

    if not object_name:
        object_name = local_file_path.name

    s3_path = f"{destination_dir.removesuffix('/')}/{object_name}"

    if metadata is None:
        if object_exists(s3_path):
            print(f"File already present: {s3_path}")  # noqa: T201
            return True

    s3 = get_s3_client()
    (bucket, key) = split_s3_path(s3_path)

    extra_args = {**DEFAULT_EXTRA_ARGS, **(({"Metadata": metadata}) if metadata is not None else {})}

    # Upload the file
    file_size = local_file_path.stat().st_size
    with tqdm.tqdm(total=file_size, unit="B", unit_scale=True, desc=str(local_file_path)) as pbar:
        print(f"uploading {local_file_path!s} to {s3_path}")  # noqa: T201
        try:
            s3.upload_file(
                Filename=str(local_file_path),
                Bucket=bucket,
                Key=key,
                Callback=pbar.update,
                ExtraArgs=extra_args,
            )
        except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
            print(f"Error uploading to s3: {e!s}")  # noqa: T201
            return False
        return True


def stream_to_s3(url: str, s3_path: str, requests: ModuleType) -> str:
    """Stream directly from an HTTP download to s3.

    :param url: address of the object to transfer to s3
    :type url: str
    :param s3_path: save path on s3
    :type s3_path: str
    :param requests: module implementing requests.get and returning a response
    :type requests: ModuleType
    :return: path of the file on s3, in the form bucket/key
    :rtype: str
    """
    s3_client = get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        s3_client.upload_fileobj(
            # raw stream from urllib3
            response.raw,
            bucket,
            key,
            ExtraArgs={
                **DEFAULT_EXTRA_ARGS,
                "ContentType": response.headers.get("content-type", "application/octet-stream"),
            },
        )
    return f"{bucket}/{key}"


def download_file(s3_path: str, local_file_path: str | Path, version_id: str | None = None) -> None:
    """Download an object from s3.

    WARNING: will overwrite existing files but will not overwrite a file whilst trying to make a directory

    Will attempt to create the local directory if it does not exist.

    :param s3_path: path to the file on s3, INCLUDING the bucket name
    :type s3_path: str
    :param local_file_path: local path (including file name) to save the downloaded file to
    :type local_file_path: str | Path
    :param version_id: version ID of the file to download, defaults to None
    :type version_id: str | None, optional
    """
    local_file_path = Path(local_file_path)
    # check whether the parent directory exists
    parent_dir = local_file_path.parent
    if not parent_dir.is_dir():
        try:
            parent_dir.mkdir(parents=True, exist_ok=False)
        except OSError as e:
            print(f"Could not save s3 file to {local_file_path}: {e!s}")  # noqa: T201
            raise

    s3 = get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    kwargs = {"Bucket": bucket, "Key": key}
    if version_id is not None:
        kwargs["VersionId"] = version_id

    # Get the object size
    try:
        object_size = s3.head_object(**kwargs)["ContentLength"]
    except botocore.exceptions.ClientError as e:
        error_string = str(e)
        if error_string.startswith("An error occurred (404) when calling the HeadObject operation: Not Found"):
            print(f"File not found: {s3_path}")  # noqa: T201
        else:
            print(f"Error downloading {s3_path}: {e!s}")  # noqa: T201
        raise

    extra_args = {"VersionId": version_id} if version_id is not None else None

    # set ``unit_scale=True`` so tqdm uses SI unit prefixes
    # ``unit="B"`` means it adds the string "B" as a suffix
    # progress is reported as (e.g.) "14.5kB/s".
    with tqdm.tqdm(total=object_size, unit="B", unit_scale=True, desc=str(local_file_path)) as pbar:
        s3.download_file(
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
            Filename=str(local_file_path),
            Callback=pbar.update,
        )


def upload_dir(
    local_dir: Path | str,
    destination_dir: str,
    file_glob: str | None = None,
) -> bool:
    """Upload a directory to an s3 bucket.

        If file_glob is not set, it will default to "**/*", i.e. any path with at least
        one segment (recursive listing of all paths from the current directory).

        Wildcards:
        ** (entire segment)
            Matches any number of file or directory segments, including zero.
            "assets/**" matches any path starting with "assets/", including "assets/"

        * (entire segment)
            Matches one file or directory segment.
            "assets/*" matches any file or directory under "assets/" but none of the children

        **/* (two segments)
            Matches any path with at least one segment
            "assets/**/*" matches any file or directory under "assets/" but not "assets/" itself

        * (part of a segment)
            Matches any number of non-separator characters, including zero.
            "file*.txt" would match "file.txt", "file_type.txt", "file12345.txt", but not "file/b.txt"

        ? (part of a segment)
            Matches one non-separator character.
            "file?.txt" would match "filea.txt" or "file1.txt"

    [seq]
        Matches one character in seq, where seq is a sequence of characters. Range expressions are supported; for example, [a-z] matches any lowercase ASCII letter. Multiple ranges can be combined: [a-zA-Z0-9_] matches any ASCII letter, digit, or underscore.

    [!seq]
        Matches one character not in seq, where seq follows the same rules as above.

    For a literal match, wrap the meta-characters in brackets. For example, "[?]" matches the character "?".

    :param local_dir_path: local directory to upload
    :type local_dir_path: Path | str
    :param destination_dir: remote directory to upload to, INCLUDING the bucket name
    :type destination_dir: str
    :param file_glob: glob for selecting files to upload
    :type file_glob: str | None
    :return: True or False, depending on the result of the uploads
    :rtype: bool
    """
    if not local_dir:
        msg = "No source directory supplied for the upload"
        raise ValueError(msg)

    if not destination_dir:
        msg = "No destination directory supplied for the upload"
        raise ValueError(msg)

    if not file_glob:
        file_glob = "**/*"

    if isinstance(local_dir, str):
        local_dir = Path(local_dir)

    all_successful = True
    for path in sorted(local_dir.glob(file_glob)):
        if path.is_dir():
            continue
        # get the path of the current file relative to local_dir and use that as the object name
        success = upload_file(path, destination_dir, object_name=str(path.relative_to(local_dir)))
        if not success:
            all_successful = False

    return all_successful


def copy_object(
    current_s3_path: str,
    new_s3_path: str,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Copy an object from one place to another, adding in a CRC64NVME checksum.

    When *metadata* is supplied the destination object carries exactly those
    key/value pairs (``MetadataDirective='REPLACE'``).  When *metadata* is
    ``None`` (the default) the source metadata is inherited.

    A successful copy operation will return a response where
    resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    Errors (e.g, buckets or keys not existing, wrong credentials, etc.) are passed
    directly to the user without being caught.

    :param current_s3_path: path to the file on s3, INCLUDING the bucket name
    :type current_s3_path: str
    :param new_s3_path: the desired new file path on s3, INCLUDING the bucket name
    :type new_s3_path: str
    :param metadata: user metadata to set on the destination object; when provided the source metadata is replaced
    :type metadata: dict[str, str] | None
    :return: dictionary containing response
    :rtype: dict[str, Any]
    """
    s3 = get_s3_client()
    (current_s3_bucket, current_s3_key) = split_s3_path(current_s3_path)
    (new_s3_bucket, new_s3_key) = split_s3_path(new_s3_path)

    extra: dict[str, Any] = {}
    if metadata is not None:
        extra["Metadata"] = metadata
        extra["MetadataDirective"] = "REPLACE"

    return s3.copy_object(
        CopySource={"Bucket": current_s3_bucket, "Key": current_s3_key},
        Bucket=new_s3_bucket,
        Key=new_s3_key,
        **extra,
        **DEFAULT_EXTRA_ARGS,
    )


def delete_object(s3_path: str) -> dict[str, Any]:
    """Delete an object from s3.

    A successful deletion will return a response where
    resp["ResponseMetadata"]["HTTPStatusCode"] == 204.

    Errors (e.g, buckets or keys not existing, wrong credentials, etc.) are passed
    directly to the user without being caught.

    :param s3_path: path to the file on s3, INCLUDING the bucket name
    :type s3_path: str
    :return: dictionary containing response
    :rtype: dict[str, Any]
    """
    s3 = get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    return s3.delete_object(Bucket=bucket, Key=key)


def head_object(s3_path: str) -> dict[str, Any] | None:
    """Return metadata for an S3 object, or None if it does not exist.

    The returned dict contains:
    - ``size``: content length in bytes
    - ``metadata``: user metadata dict
    - ``checksum_crc64nvme``: CRC64NVME checksum string (if available)

    :param s3_path: path to the object on s3, INCLUDING the bucket name
    :type s3_path: str
    :return: dict with object info, or None if the object does not exist
    :rtype: dict[str, Any] | None
    """
    s3 = get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    try:
        resp = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise
    return {
        "size": resp["ContentLength"],
        "metadata": resp.get("Metadata", {}),
        "checksum_crc64nvme": resp.get("ChecksumCRC64NVME"),
    }
