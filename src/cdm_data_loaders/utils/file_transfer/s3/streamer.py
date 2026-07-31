"""Stream HTTP resources directly into S3, without buffering to local disk."""

from logging import WARNING, Logger, getLogger
from types import ModuleType
from typing import Annotated, Any, Final

import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdm_data_loaders.utils.file_transfer.checksums import ChecksumEntry, HashingReader, resolve_checksum_fn
from cdm_data_loaders.utils.file_transfer.core import (
    ChecksumMismatchError,
    DownloadError,
    NonRetryableDownloadError,
)
from cdm_data_loaders.utils.file_transfer.progress import SynchronizedCallback, make_progress_bar
from cdm_data_loaders.utils.file_transfer.s3 import client
from cdm_data_loaders.utils.file_transfer.s3.client import split_s3_path
from cdm_data_loaders.utils.file_transfer.s3.object_utils import (
    DEFAULT_EXTRA_ARGS,
    checksum_metadata,
    decide_skip,
    get_existing_object_info,
)
from cdm_data_loaders.utils.file_transfer.s3.transfer_config import build_transfer_config

logger: Logger = getLogger(__name__)

DEFAULT_CHECKSUM_ALGORITHM: Final[str] = "sha256"


class TransferContext(BaseModel):
    """The external clients used to talk to S3 and to the remote HTTP source.

    Also the base class for `S3UploaderSettings`, since an uploader's
    settings include (and can stand in for) a transfer context.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    s3_client: Annotated[
        Any, Field(default=None, description="a boto3 S3 client; a default one is created if not supplied")
    ]
    requests_module: Annotated[
        ModuleType, Field(default=requests, description="module implementing requests.get/requests.head")
    ]

    @model_validator(mode="after")
    def _default_s3_client(self) -> "TransferContext":
        if self.s3_client is None:
            self.s3_client = client.get_s3_client()
        return self


class TransferTuningFields(BaseModel):
    """Options that tune the underlying S3 transfer itself.

    Shared between `S3UploaderSettings` (uploader-wide defaults) and `UploadJob` (a single resolved transfer).
    """

    extra_args: Annotated[
        dict[str, Any] | None, Field(default=None, description="extra S3 ExtraArgs to merge into the upload")
    ]
    transfer_config_kwargs: Annotated[
        dict[str, Any] | None,
        Field(default=None, description="extra `TransferConfig` keyword arguments (e.g. `max_concurrency`)"),
    ]


class UploadTarget(BaseModel):
    """Fields describing *what* to upload and *where*, and how to check it against what's already there.

    Shared between the caller-facing `UploadRequest` and the fully-resolved `UploadJob`.
    """

    url: Annotated[str, Field(description="address of the object to transfer to s3")]
    s3_path: Annotated[str, Field(description="save path on s3, as 's3://bucket/key' or 'bucket/key'")]
    extra_headers: Annotated[
        dict[str, str] | None, Field(default=None, description="extra headers to pass to the GET/HEAD requests")
    ]
    expected_checksum: Annotated[str | None, Field(default=None, description="expected digest of the downloaded bytes")]
    checksum_fn: Annotated[
        str | None,
        Field(
            default=None,
            description="hashlib algorithm name used to compute/verify `expected_checksum`; "
            "None means 'not yet resolved'",
        ),
    ]
    progress_desc: Annotated[
        str | None, Field(default=None, description="label for the progress bar; defaults to the destination S3 key")
    ]
    force: Annotated[bool, Field(default=False, description="always upload regardless of what already exists in S3")]
    expected_size: Annotated[
        int | None, Field(default=None, description="known size of the source file in bytes, if already available")
    ]


class UploadRequest(UploadTarget):
    """Caller-facing description of a single upload, resolved against an `S3StreamUploader`'s settings.

    `show_progress`/`skip_if_exists` are `None` here (unlike on `UploadJob`,
    where they're required) to mean "use the uploader's setting".
    """

    show_progress: Annotated[
        bool | None, Field(default=None, description="display a tqdm progress bar; None defers to the uploader")
    ]
    skip_if_exists: Annotated[
        bool | None,
        Field(default=None, description="skip if a matching object exists; None defers to the uploader"),
    ]


class S3UploaderSettings(TransferContext, TransferTuningFields):
    """Shared configuration for an `S3StreamUploader` instance.

    Field names deliberately match `UploadJob`'s (`checksum_fn`,
    `show_progress`, `skip_if_exists`) rather than being prefixed with
    `default_`; the class each field lives on is what distinguishes
    "uploader-wide setting" from "resolved, per-job value" — Pydantic's own
    `Field(default=...)` already covers what each field is worth if the
    caller doesn't override it.
    """

    max_attempts: Annotated[int, Field(default=5, description="how many times to retry the upload")]
    min_backoff: Annotated[int, Field(default=1, description="minimum backoff for retries, in seconds")]
    max_backoff: Annotated[int, Field(default=30, description="maximum backoff for retries, in seconds")]
    timeout: Annotated[float, Field(default=30.0, description="request timeout in seconds")]
    checksum_fn: Annotated[
        str,
        Field(
            default=DEFAULT_CHECKSUM_ALGORITHM,
            description="algorithm used when a request gives a checksum without an explicit algorithm",
        ),
    ]
    show_progress: Annotated[
        bool, Field(default=False, description="progress bar setting if not specified in the request")
    ]
    skip_if_exists: Annotated[
        bool, Field(default=True, description="skip-if-exists setting if not specified in the request")
    ]


class UploadJob(UploadTarget, TransferTuningFields):
    """Fully-resolved specification for a single streamed HTTP-to-S3 upload.

    This is the sole argument accepted by every `S3UploadCore` method and by
    `stream_to_s3`. Build one directly for one-off uploads, or via
    `UploadJob.from_request()` when working through an `S3StreamUploader`.
    """

    context: Annotated[
        TransferContext, Field(default_factory=TransferContext, description="the S3/HTTP clients to use")
    ]
    timeout: Annotated[float | None, Field(default=None, description="request timeout in seconds")]
    show_progress: Annotated[bool, Field(default=False, description="display a tqdm progress bar")]
    skip_if_exists: Annotated[
        bool, Field(default=True, description="skip the transfer if a matching object already exists")
    ]

    @model_validator(mode="after")
    def _resolve_checksum_fn(self) -> "UploadJob":
        """Fill in a default `checksum_fn` if `expected_checksum` was given without one.

        :raises ValueError: if the resolved algorithm is not a supported hashlib algorithm
        """
        self.checksum_fn = resolve_checksum_fn(self.expected_checksum, self.checksum_fn, DEFAULT_CHECKSUM_ALGORITHM)
        return self

    @property
    def bucket(self) -> str:
        return split_s3_path(self.s3_path)[0]

    @property
    def key(self) -> str:
        return split_s3_path(self.s3_path)[1]

    @property
    def s3_client(self) -> Any:
        return self.context.s3_client

    @property
    def requests_module(self) -> ModuleType:
        return self.context.requests_module

    @property
    def checksum(self) -> ChecksumEntry | None:
        """The expected checksum as a `ChecksumEntry`, or None if none is expected."""
        if self.expected_checksum and self.checksum_fn:
            return ChecksumEntry(algorithm=self.checksum_fn, value=self.expected_checksum)
        return None

    @classmethod
    def from_request(cls, request: UploadRequest, settings: S3UploaderSettings) -> "UploadJob":
        """Resolve a caller-facing `UploadRequest` into a full `UploadJob`, applying uploader defaults.

        `settings` is passed directly as the job's `context`, since
        `S3UploaderSettings` is itself a `TransferContext`.

        :param request: the per-call upload description
        :type request: UploadRequest
        :param settings: the uploader's shared settings and clients
        :type settings: S3UploaderSettings
        :return: a fully-resolved job
        :rtype: UploadJob
        """
        checksum_fn = request.checksum_fn
        if checksum_fn is None and request.expected_checksum:
            checksum_fn = settings.checksum_fn

        return cls(
            url=request.url,
            s3_path=request.s3_path,
            context=settings,
            extra_headers=request.extra_headers,
            extra_args=settings.extra_args,
            expected_checksum=request.expected_checksum,
            checksum_fn=checksum_fn,
            timeout=settings.timeout,
            show_progress=settings.show_progress if request.show_progress is None else request.show_progress,
            progress_desc=request.progress_desc,
            skip_if_exists=settings.skip_if_exists if request.skip_if_exists is None else request.skip_if_exists,
            force=request.force,
            expected_size=request.expected_size,
            transfer_config_kwargs=settings.transfer_config_kwargs,
        )


class S3UploadCore:
    """
    Core request/upload/verify logic for streaming an HTTP resource into S3.
    """

    @staticmethod
    def validate_response(response: requests.Response) -> None:
        """Check an HTTP response's status code and raise on error.

        :param response: the streamed HTTP response to validate
        :type response: requests.Response
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises DownloadError: for 5xx server errors
        """
        status = response.status_code
        if 400 <= status < 500:  # noqa: PLR2004
            msg = f"Client error: {status} {response.reason}"
            raise NonRetryableDownloadError(msg)
        if status >= 500:  # noqa: PLR2004
            msg = f"Server error: {status} {response.reason}"
            raise DownloadError(msg)

    @staticmethod
    def check_hash(job: UploadJob, hasher: HashingReader) -> None:
        """Compare the expected checksum to the one computed while uploading.

        Deletes the uploaded object before raising if the checksums do not match.

        :param job: the job describing the transfer that was just performed
        :type job: UploadJob
        :param hasher: the HashingReader used during the upload
        :type hasher: HashingReader
        :raises ChecksumMismatchError: if the checksums do not match
        """
        expected = job.expected_checksum
        actual = hasher.hexdigest()
        if expected is not None and actual.lower() != expected.lower():
            job.s3_client.delete_object(Bucket=job.bucket, Key=job.key)
            msg = (
                f"{job.url}: {job.checksum_fn} checksum mismatch uploading to {job.bucket}/{job.key}: "
                f"expected={expected}, actual={actual}"
            )
            raise ChecksumMismatchError(msg)
        logger.info("%s: %s checksum verified for %s/%s", job.url, job.checksum_fn, job.bucket, job.key)

    @staticmethod
    def get_content_length(response: requests.Response) -> int | None:
        """Extract a response's Content-Length header as an int, if present.

        :param response: the HTTP response to inspect
        :type response: requests.Response
        :return: the content length in bytes, or None if not present/parseable
        :rtype: int | None
        """
        content_length = response.headers.get("content-length")
        if content_length is None:
            return None
        try:
            return int(content_length)
        except ValueError:
            return None

    @staticmethod
    def get_remote_size(job: UploadJob) -> int | None:
        """Get the size of a remote resource via an HTTP HEAD request, without downloading it.

        :param job: the job describing the transfer; uses `job.url`, `job.requests_module`,
            `job.extra_headers`, and `job.timeout`
        :type job: UploadJob
        :return: the resource's size in bytes, or None if it could not be determined
        :rtype: int | None
        """
        kwargs: dict[str, Any] = {"headers": job.extra_headers or {}}
        if job.timeout is not None:
            kwargs["timeout"] = job.timeout

        try:
            response = job.requests_module.head(job.url, **kwargs)
            response.raise_for_status()
        except Exception:
            logger.warning("%s: HEAD request failed; cannot pre-check remote size", job.url, exc_info=True)
            return None

        content_length = response.headers.get("content-length")
        if content_length is None:
            return None
        try:
            return int(content_length)
        except ValueError:
            return None

    @staticmethod
    def check_existing(job: UploadJob) -> bool:
        """Check whether the job's destination already holds an equivalent file, and can be skipped.

        :param job: the job describing the candidate transfer
        :type job: UploadJob
        :return: True if the upload can be safely skipped, False otherwise
        :rtype: bool
        """
        existing = get_existing_object_info(job.bucket, job.key)
        if existing is None:
            return False

        remote_size = job.expected_size
        if remote_size is None:
            remote_size = S3UploadCore.get_remote_size(job)

        decision = decide_skip(existing, remote_size, job.checksum)

        log = logger.info if decision.confident else logger.warning
        if decision.skip:
            log("%s: skipping upload to %s/%s (%s)", job.url, job.bucket, job.key, decision.reason)
        else:
            log("%s: will (re-)upload to %s/%s (%s)", job.url, job.bucket, job.key, decision.reason)

        return decision.skip

    @staticmethod
    def perform_upload(job: UploadJob) -> str:
        """Stream `job.url` into `job.s3_path`, skipping, verifying, and sizing the transfer as configured.

        If a checksum is expected and does not match the uploaded data, the
        uploaded object is deleted before raising. If a checksum is
        expected, it is also recorded as object metadata on successful
        upload, so future calls with `skip_if_exists=True` can rely on it.

        The multipart chunksize used for the transfer is automatically
        scaled up (via `transfer_config.build_transfer_config`) if the file
        is large enough that S3's default 8MB chunksize would exceed the
        10,000-part limit.

        :param job: full specification of the transfer to perform
        :type job: UploadJob
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises DownloadError: for 5xx server errors or other transport failures
        :raises ChecksumMismatchError: if the uploaded data does not match the expected checksum
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        bucket, key = job.bucket, job.key

        if job.skip_if_exists and not job.force and S3UploadCore.check_existing(job):
            return f"{bucket}/{key}"

        merged_extra_args = {**DEFAULT_EXTRA_ARGS, **(job.extra_args or {})}
        checksum_entry = job.checksum
        if checksum_entry is not None:
            merged_extra_args["Metadata"] = {
                **merged_extra_args.get("Metadata", {}),
                **checksum_metadata(checksum_entry),
            }

        get_kwargs: dict[str, Any] = {"stream": True, "headers": job.extra_headers or {}}
        if job.timeout is not None:
            get_kwargs["timeout"] = job.timeout

        with job.requests_module.get(job.url, **get_kwargs) as response:
            S3UploadCore.validate_response(response)

            file_size = (
                job.expected_size if job.expected_size is not None else S3UploadCore.get_content_length(response)
            )
            transfer_config = build_transfer_config(file_size, **(job.transfer_config_kwargs or {}))

            hasher = HashingReader(response.raw, job.checksum_fn) if job.expected_checksum else response.raw

            with make_progress_bar(
                total=file_size,
                desc=job.progress_desc or key,
                disable=not job.show_progress,
            ) as pbar:
                job.s3_client.upload_fileobj(
                    hasher,
                    bucket,
                    key,
                    ExtraArgs={
                        **merged_extra_args,
                        "ContentType": response.headers.get("content-type", "application/octet-stream"),
                    },
                    Config=transfer_config,
                    Callback=SynchronizedCallback(pbar.update),
                )

            if job.expected_checksum:
                S3UploadCore.check_hash(job, hasher)

        logger.info("%s: upload to s3 successful", job.url, extra={"s3_path": f"{bucket}/{key}"})
        return f"{bucket}/{key}"


def stream_to_s3(job: UploadJob) -> str:
    """Stream directly from an HTTP download to s3, exactly as described by `job`.

    Thin top-level alias for `S3UploadCore.perform_upload`, kept as the
    module's public, non-retried entry point.

    :param job: full specification of the transfer to perform
    :type job: UploadJob
    :raises NonRetryableDownloadError: for 4xx client errors
    :raises DownloadError: for 5xx server errors or other transport failures
    :raises ChecksumMismatchError: if the uploaded data does not match the expected checksum
    :return: path of the file on s3, in the form bucket/key
    :rtype: str
    """
    return S3UploadCore.perform_upload(job)


class S3StreamUploader:
    """
    Streams HTTP resources into S3 with retry support, mirroring FileDownloader's semantics.
    """

    RETRYABLE_EXCEPTIONS = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        DownloadError,
    )

    def __init__(self, settings: S3UploaderSettings | None = None) -> None:
        """Initialise an S3 streaming uploader.

        :param settings: shared configuration for this uploader; defaults to `S3UploaderSettings()`
        :type settings: S3UploaderSettings | None, optional
        """
        self.settings = settings or S3UploaderSettings()
        self._retry = retry(
            retry=retry_if_exception_type(self.RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(self.settings.max_attempts),
            wait=wait_exponential(min=self.settings.min_backoff, max=self.settings.max_backoff),
            reraise=True,
            before_sleep=before_sleep_log(logger, WARNING),
        )

    def upload(self, request: UploadRequest) -> str:
        """Stream `request.url` into `request.s3_path` with retries, using this uploader's shared settings.

        :param request: the per-call upload description
        :type request: UploadRequest
        :raises ValueError: if `request.checksum_fn` (or the resolved setting) is not a supported hashlib algorithm
        :raises NonRetryableDownloadError: for 4xx client errors (not retried)
        :raises DownloadError: for 5xx server errors or other transport failures (retried)
        :raises ChecksumMismatchError: if the uploaded data does not match `request.expected_checksum` (not retried)
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        job = UploadJob.from_request(request, self.settings)

        @self._retry
        def _once() -> str:
            return self._upload_once(job)

        return _once()

    def _upload_once(self, job: UploadJob) -> str:
        """Perform a single upload attempt, translating unexpected errors into `DownloadError`.

        :param job: full specification of the transfer to perform
        :type job: UploadJob
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises ChecksumMismatchError: if the uploaded data does not match the expected checksum
        :raises DownloadError: for any other failure, wrapping the original exception
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        try:
            return S3UploadCore.perform_upload(job)
        except (NonRetryableDownloadError, ChecksumMismatchError) as exc:
            logger.exception("%s: %s; retry not possible", job.url, exc.args[0], extra={"url": job.url})
            raise
        except Exception as exc:
            logger.exception("%s: %s; retry possible", job.url, exc, extra={"url": job.url})
            raise DownloadError(str(exc)) from exc
