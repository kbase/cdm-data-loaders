"""Stream HTTP resources directly into S3, without buffering to local disk."""

from logging import WARNING, Logger, getLogger
from types import ModuleType
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from cdm_data_loaders.utils.s3 import split_s3_path
from cdm_data_loaders.utils.download.core import DownloadError, NonRetryableDownloadError

logger: Logger = getLogger(__name__)


class S3StreamUploader:
    """
    Streams HTTP resources into S3 with retry support, mirroring FileDownloader's semantics.
    """

    RETRYABLE_EXCEPTIONS = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        DownloadError,
    )

    def __init__(
        self,
        s3_client: Any,
        requests_module: ModuleType = requests,
        max_attempts: int = 5,
        min_backoff: int = 1,
        max_backoff: int = 30,
        timeout: float = 30.0,
        extra_args: dict[str, Any] | None = None,
    ) -> None:
        """Initialise an S3 streaming uploader.

        :param s3_client: a boto3 S3 client
        :type s3_client: Any
        :param requests_module: module implementing requests.get, defaults to `requests`
        :type requests_module: ModuleType, optional
        :param max_attempts: how many times to retry the upload, defaults to 5
        :type max_attempts: int, optional
        :param min_backoff: minimum backoff for retries, defaults to 1
        :type min_backoff: int, optional
        :param max_backoff: maximum backoff for retries, defaults to 30
        :type max_backoff: int, optional
        :param timeout: request timeout in seconds, defaults to 30.0
        :type timeout: float, optional
        :param extra_args: extra S3 ExtraArgs applied to every upload, defaults to None
        :type extra_args: dict[str, Any] | None, optional
        """
        self.s3_client = s3_client
        self.requests = requests_module
        self.timeout = timeout
        self.extra_args = extra_args or {}

        self._retry = retry(
            retry=retry_if_exception_type(self.RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(min=min_backoff, max=max_backoff),
            reraise=True,
            before_sleep=before_sleep_log(logger, WARNING),
        )

    def upload(
        self,
        url: str,
        s3_path: str,
        extra_headers: dict[str, str] | None = None,
    ) -> str:
        """Stream ``url`` into ``s3_path`` with retries.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param extra_headers: extra headers to pass to the GET request, defaults to None
        :type extra_headers: dict[str, str] | None, optional
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """

        @self._retry
        def _once() -> str:
            return self._upload_once(url, s3_path, extra_headers)

        return _once()

    def _upload_once(
        self,
        url: str,
        s3_path: str,
        extra_headers: dict[str, str] | None,
    ) -> str:
        bucket, key = split_s3_path(s3_path)
        try:
            with self.requests.get(
                url,
                stream=True,
                headers=extra_headers or {},
                timeout=self.timeout,
            ) as response:
                status = response.status_code
                if 400 <= status < 500:
                    msg = f"Client error: {status} {response.reason}"
                    raise NonRetryableDownloadError(msg)
                if status >= 500:
                    msg = f"Server error: {status} {response.reason}"
                    raise DownloadError(msg)

                self.s3_client.upload_fileobj(
                    response.raw,
                    bucket,
                    key,
                    ExtraArgs={
                        **self.extra_args,
                        "ContentType": response.headers.get("content-type", "application/octet-stream"),
                    },
                )

        except NonRetryableDownloadError as exc:
            logger.exception("%s: %s; retry not possible", url, exc.args[0], extra={"url": url})
            raise

        except Exception as exc:
            logger.exception("%s: %s; retry possible", url, exc, extra={"url": url})
            raise DownloadError(str(exc)) from exc

        logger.info("%s: upload to s3 successful", url, extra={"s3_path": f"{bucket}/{key}"})
        return f"{bucket}/{key}"
