"""Utilities for s3 interaction."""

from dataclasses import dataclass
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

from botocore.exceptions import ClientError

from cdm_data_loaders.utils.file_transfer.checksums import ChecksumEntry
from cdm_data_loaders.utils.file_transfer.progress import SynchronizedCallback, make_progress_bar
from cdm_data_loaders.utils.file_transfer.s3 import client
from cdm_data_loaders.utils.file_transfer.s3.client import DEFAULT_EXTRA_ARGS, split_s3_path

VALID_S3_PREFIXES: list[str] = ["s3://", "s3a://"]

SUCCESS_RESPONSE: Final[int] = 200

NOT_FOUND_ERROR_CODES: frozenset[str] = frozenset({"404", "NoSuchKey", "NotFound", "NoSuchBucket"})

# S3 object metadata keys used to record the checksum used to verify an upload,
# so future runs can compare against it without re-downloading the source file.
# NOTE: S3 lowercases all user metadata keys, both on write and on read.
CHECKSUM_ALGORITHM_METADATA_KEY: Final[str] = "checksum-algorithm"
CHECKSUM_VALUE_METADATA_KEY: Final[str] = "checksum-value"

logger: Logger = getLogger(__name__)


@dataclass(frozen=True, slots=True)
class S3ObjectInfo:
    """Metadata about an object that already exists in S3.

    :param size: object size in bytes, as reported by S3
    :type size: int
    :param etag: the object's ETag, exactly as reported by the S3 API (including quotes)
    :type etag: str
    :param metadata: user-defined metadata attached to the object (keys are lowercase)
    :type metadata: dict[str, str]
    """

    size: int
    etag: str
    metadata: dict[str, str]


@dataclass(frozen=True, slots=True)
class SkipDecision:
    """The outcome of deciding whether an upload can be skipped.

    :param skip: whether the upload should be skipped
    :type skip: bool
    :param reason: human-readable explanation, used for logging
    :type reason: str
    :param confident: whether `skip` is based on a strong signal (a matching
        checksum) as opposed to a weaker heuristic (size match only),
        defaults to True
    :type confident: bool
    """

    skip: bool
    reason: str
    confident: bool = True


def decide_skip(
    existing: S3ObjectInfo | None,
    remote_size: int | None,
    expected_checksum: ChecksumEntry | None,
) -> SkipDecision:
    """Decide whether an upload can be skipped, based on what's already in S3.

    Preference order:

    1. If no object exists at the destination, never skip.
    2. If both a stored checksum (from a previous upload's metadata) and an
       expected checksum are available, compare them directly — this is the
       only fully reliable check, since S3's ETag is not a dependable
       content hash for multipart uploads.
    3. If a checksum was expected but the existing object has none recorded
       (e.g. uploaded before checksum recording existed), fall back to
       comparing sizes, flagged as unconfident.
    4. If no checksum is available at all, compare sizes only, flagged as unconfident.
    5. If nothing can be compared (no checksum, no size), never skip — safer
       to re-upload than silently skip an unverifiable file.

    :param existing: metadata for the object already in S3, or None if absent
    :type existing: S3ObjectInfo | None
    :param remote_size: size in bytes of the source file, if known (e.g. via
        an HTTP HEAD request), or None if unknown
    :type remote_size: int | None
    :param expected_checksum: the checksum the source file is expected to
        have, if known, or None
    :type expected_checksum: ChecksumEntry | None
    :return: the skip decision, with a human-readable reason
    :rtype: SkipDecision
    """
    if existing is None:
        return SkipDecision(skip=False, reason="object does not exist in S3")

    if expected_checksum is not None:
        stored = extract_stored_checksum(existing)
        if stored is not None:
            if (
                stored.algorithm == expected_checksum.algorithm
                and stored.value.lower() == expected_checksum.value.lower()
            ):
                return SkipDecision(
                    skip=True, reason=f"{stored.algorithm} checksum matches stored value", confident=True
                )
            return SkipDecision(
                skip=False,
                reason=(
                    f"stored checksum ({stored.algorithm}:{stored.value}) does not match "
                    f"expected ({expected_checksum.algorithm}:{expected_checksum.value})"
                ),
                confident=True,
            )
        logger.warning(
            "Object exists but has no stored checksum metadata; falling back to size comparison "
            "(this is not a guaranteed content match)"
        )

    if remote_size is not None:
        if existing.size == remote_size:
            return SkipDecision(
                skip=True,
                reason=f"size matches ({remote_size} bytes) but content was not verified by checksum",
                confident=False,
            )
        return SkipDecision(
            skip=False,
            reason=f"size mismatch: existing={existing.size} bytes, source={remote_size} bytes",
            confident=True,
        )

    return SkipDecision(
        skip=False,
        reason="no checksum or size available to compare; re-uploading to be safe",
        confident=False,
    )


def list_matching_objects(s3_path: str, *, max_keys: int = 1000) -> list[dict[str, Any]]:
    """List the remote paths that start with ``s3_path``.

    Note: since s3 paths are basically cosmetic, this function returns all paths that start with
    ``s3_path`` minus the bucket name.

    Retrieves all objects under the given prefix; collects all pages of results if there are more than
    1000 files (the max retrievable per `list_objects_v2` query) present.

    :param s3_path: directory to be listed, INCLUDING the bucket name
    :type s3_path: str
    :param max_keys: maximum number of keys to return. boto3 defaults to 1000 records max.
    :type max_keys: int
    :return: list of object metadata dicts in the directory
    :rtype: list[dict[str, Any]]
    """
    s3 = client.get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=key, MaxKeys=max_keys)

    contents = []
    for page in page_iterator:
        contents.extend(page.get("Contents", []))

    return contents


def get_existing_object_info(bucket: str, key: str) -> S3ObjectInfo | None:
    """Fetch metadata for an object already in S3, if it exists.

    :param s3_client: a boto3 S3 client
    :type s3_client: Any
    :param bucket: S3 bucket name
    :type bucket: str
    :param key: S3 object key
    :type key: str
    :raises ClientError: for any S3 error other than "object not found"
    :return: metadata for the existing object, or None if no object exists at `bucket`/`key`
    :rtype: S3ObjectInfo | None
    """
    s3 = client.get_s3_client()
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in NOT_FOUND_ERROR_CODES or status_code == 404:  # noqa: PLR2004
            return None
        raise

    return S3ObjectInfo(
        size=response.get("ContentLength", 0),
        etag=response.get("ETag", ""),
        metadata={k.lower(): v for k, v in response.get("Metadata", {}).items()},
    )


def head_object(s3_path: str) -> dict[str, Any]:
    """Check whether an object exists on s3.

    :param s3_path: path to the object on s3, INCLUDING the bucket name
    :type s3_path: str
    :return: response from the head_object request
    :rtype: dict[str, Any]
    """
    s3 = client.get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    return s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")


def object_exists(s3_path: str) -> bool:
    """Check whether an object exists on s3.

    :param s3_path: path to the object on s3, INCLUDING the bucket name
    :type s3_path: str
    :return: True if the object exists, False otherwise
    :rtype: bool
    """
    bucket, key = split_s3_path(s3_path)
    return get_existing_object_info(bucket, key) is not None


def upload_file(
    local_file_path: Path | str,
    destination_dir: str,
    object_name: str | None = None,
    user_metadata: dict[str, str] | None = None,
    *,
    show_progress: bool = True,
) -> bool:
    """Upload an object to an S3 bucket.

    When *user_metadata* is supplied the file is always uploaded (no existence check)
    and the dict is attached as S3 user metadata.  When *user_metadata* is ``None``
    (the default) the existing behaviour is preserved: the upload is skipped if
    the object is already present.

    :param local_file_path: File to upload
    :type local_file_path: Path | str
    :param destination_dir: path to the destination directory on s3, INCLUDING the bucket name and EXCLUDING the file name
    :type destination_dir: str
    :param object_name: S3 object name. If not specified, the name of the file from local_file_path is used.
    :type object_name: str | None
    :param user_metadata: user metadata key/value pairs to attach to the object; when provided the upload always runs
    :type user_metadata: dict[str, str] | None
    :param show_progress: whether to display a tqdm progress bar during upload, defaults to True
    :type show_progress: bool, optional
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
    if user_metadata is None and object_exists(s3_path):
        logger.debug("File already present: %s", s3_path)
        return True

    s3 = client.get_s3_client()
    (bucket, key) = split_s3_path(s3_path)

    extra_args = {**DEFAULT_EXTRA_ARGS, **(({"Metadata": user_metadata}) if user_metadata is not None else {})}

    # Upload the file
    logger.debug("uploading %s to %s", str(local_file_path), s3_path)
    try:
        file_size = local_file_path.stat().st_size
        with make_progress_bar(total=file_size, desc=str(local_file_path), disable=not show_progress) as pbar:
            s3.upload_file(
                Filename=str(local_file_path),
                Bucket=bucket,
                Key=key,
                Callback=SynchronizedCallback(pbar.update),
                ExtraArgs=extra_args,
            )
    except Exception:
        logger.exception("Error uploading to s3")
        return False
    return True


def download_file(
    s3_path: str, local_file_path: str | Path, version_id: str | None = None, show_progress: bool = True
) -> None:
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
        except Exception:
            logger.exception("Could not save s3 file to %s", local_file_path)
            raise

    s3 = client.get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    kwargs = {"Bucket": bucket, "Key": key}
    if version_id is not None:
        kwargs["VersionId"] = version_id

    # Get the object size
    existing = get_existing_object_info(bucket, key)
    if existing is None:
        msg = f"File not found: {s3_path}"
        raise FileNotFoundError(msg)
    object_size = existing.size

    extra_args = {"VersionId": version_id} if version_id is not None else None

    with make_progress_bar(total=object_size, desc=str(local_file_path), disable=not show_progress) as pbar:
        s3.download_file(
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
            Filename=str(local_file_path),
            Callback=SynchronizedCallback(pbar.update),
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
) -> dict[str, Any]:
    """Copy an object from one place to another, inheriting the source user metadata.

    Source user metadata (e.g. ``md5``) is preserved on the destination because
    ``MetadataDirective`` is omitted, which defaults to ``COPY``.

    A successful copy operation will return a response where
    resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    Errors (e.g, buckets or keys not existing, wrong credentials, etc.) are passed
    directly to the user without being caught.

    :param current_s3_path: path to the file on s3, INCLUDING the bucket name
    :type current_s3_path: str
    :param new_s3_path: the desired new file path on s3, INCLUDING the bucket name
    :type new_s3_path: str
    :return: dictionary containing response from the copy operation
    :rtype: dict[str, Any]
    """
    s3 = client.get_s3_client()
    (current_s3_bucket, current_s3_key) = split_s3_path(current_s3_path)
    (new_s3_bucket, new_s3_key) = split_s3_path(new_s3_path)

    return s3.copy_object(
        CopySource={"Bucket": current_s3_bucket, "Key": current_s3_key},
        Bucket=new_s3_bucket,
        Key=new_s3_key,
        **DEFAULT_EXTRA_ARGS,
    )


def copy_directory(current_s3_path: str, new_s3_path: str) -> tuple[dict[str, str], dict[str, Any]]:
    """Copy all objects under a given S3 prefix to a new prefix.

    Preserves the relative key structure under the source prefix. For example,
    copying s3://my-bucket/foo/ to s3://my-bucket/bar/ will copy
    s3://my-bucket/foo/a/b.txt -> s3://my-bucket/bar/a/b.txt

    If the source bucket does not exist, a NoSuchBucket error will be thrown.

    :param current_s3_path: path to the directory on s3, INCLUDING the bucket name
    :type current_s3_path: str
    :param new_s3_path: the desired new directory path on s3, INCLUDING the bucket name
    :type new_s3_path: str
    :return: a tuple of (successes, errors) where:
             - successes maps "bucket/source_key" -> "bucket/dest_key" for each
               successfully copied object
             - errors maps "bucket/source_key" -> the exception or response object
               for each failed copy
    :rtype: tuple[dict[str, str], dict[str, Any]]
    """
    s3 = client.get_s3_client()
    (current_s3_bucket, current_s3_prefix) = split_s3_path(current_s3_path)
    (new_s3_bucket, new_s3_prefix) = split_s3_path(new_s3_path)

    if current_s3_prefix and not current_s3_prefix.endswith("/"):
        current_s3_prefix += "/"
    if new_s3_prefix and not new_s3_prefix.endswith("/"):
        new_s3_prefix += "/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=current_s3_bucket, Prefix=current_s3_prefix)

    successes: dict[str, str] = {}
    errors: dict[str, Any] = {}

    for page in pages:
        for obj in page.get("Contents", []):
            current_key = obj["Key"]
            relative_key = current_key[len(current_s3_prefix) :]
            new_key = new_s3_prefix + relative_key

            source_path = f"{current_s3_bucket}/{current_key}"
            dest_path = f"{new_s3_bucket}/{new_key}"

            try:
                resp = s3.copy_object(
                    CopySource={"Bucket": current_s3_bucket, "Key": current_key},
                    Bucket=new_s3_bucket,
                    Key=new_key,
                    **DEFAULT_EXTRA_ARGS,
                )
                if resp["ResponseMetadata"]["HTTPStatusCode"] == SUCCESS_RESPONSE:
                    successes[source_path] = dest_path
                else:
                    errors[source_path] = resp
            except Exception as e:
                logger.exception("Failed to copy %s to %s", source_path, dest_path)
                errors[source_path] = e

    return successes, errors


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
    s3 = client.get_s3_client()
    (bucket, key) = split_s3_path(s3_path)
    return s3.delete_object(Bucket=bucket, Key=key)


def delete_objects(bucket: str, keys: list[str]) -> list[dict[str, Any]]:
    """Delete multiple objects from an S3 bucket in a single API call.

    Splits into batches of 1000 (the S3 API maximum per request).

    :param bucket: S3 bucket name (no protocol prefix)
    :param keys: list of S3 keys to delete
    :return: list of per-key error dicts returned by S3 (empty if all succeeded)
    :rtype: list[dict[str, Any]]
    """
    if not keys:
        return []

    s3 = client.get_s3_client()
    errors: list[dict[str, Any]] = []
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": False},
        )
        errors.extend(resp.get("Errors", []))
    return errors


def extract_stored_checksum(existing: S3ObjectInfo) -> ChecksumEntry | None:
    """Recover a checksum previously recorded in an S3 object's metadata, if present.

    :param existing: metadata for the existing S3 object
    :type existing: S3ObjectInfo
    :return: the stored checksum, or None if the object has no recorded checksum
    :rtype: ChecksumEntry | None
    """
    algorithm = existing.metadata.get(CHECKSUM_ALGORITHM_METADATA_KEY)
    value = existing.metadata.get(CHECKSUM_VALUE_METADATA_KEY)
    if algorithm and value:
        return ChecksumEntry(algorithm=algorithm, value=value)
    return None


def checksum_metadata(checksum: ChecksumEntry) -> dict[str, str]:
    """Build an S3 object Metadata dict recording a checksum, for use in `ExtraArgs`.

    :param checksum: the checksum to record against the uploaded object
    :type checksum: ChecksumEntry
    :return: a dict suitable for merging into `ExtraArgs["Metadata"]`
    :rtype: dict[str, str]
    """
    return {
        CHECKSUM_ALGORITHM_METADATA_KEY: checksum.algorithm,
        CHECKSUM_VALUE_METADATA_KEY: checksum.value,
    }
