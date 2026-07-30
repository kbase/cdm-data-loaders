"""Tests for S3 client creation and caching using moto to mock AWS S3."""

import re

import pytest
from botocore.exceptions import PartialCredentialsError
from moto import mock_aws

import cdm_data_loaders.utils.file_transfer.s3.client as s3_client
from cdm_data_loaders.utils.file_transfer.s3.client import (
    get_s3_client,
    reset_s3_client,
    split_s3_path,
)
from tests.utils.file_transfer.s3.conftest import prep_client_init


# Client creation / reset
@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize("endpoint_url", ["http://localhost", None])
def test_get_s3_client_success_via_args(endpoint_url: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that get_s3_client creates a client with the correct credentials and endpoint URL using args for the creds."""
    prep_client_init(monkeypatch)

    args = {
        "aws_access_key_id": "aws_access_key_id_argument",
        "aws_secret_access_key": "aws_secret_access_key_argument",
        "endpoint_url": endpoint_url,
    }

    client = get_s3_client(args)
    assert client is not None
    credentials = client._request_signer._credentials  # noqa: SLF001
    assert credentials.access_key == "aws_access_key_id_argument"
    assert credentials.secret_key == "aws_secret_access_key_argument"  # noqa: S105
    expected_endpoint = endpoint_url or "http://env-endpoint.com"
    assert client.meta.endpoint_url == expected_endpoint

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize("endpoint_url", ["http://localhost", None])
def test_get_s3_client_success_via_env(endpoint_url: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that get_s3_client creates a client with the correct credentials and endpoint URL using env vars for the creds."""
    prep_client_init(monkeypatch)

    args = {
        "endpoint_url": endpoint_url,
    }

    client = get_s3_client(args)
    assert client is not None
    credentials = client._request_signer._credentials  # noqa: SLF001
    assert credentials.access_key == "aws_access_key_id_env_var"
    assert credentials.secret_key == "aws_secret_access_key_env_var"  # noqa: S105
    expected_endpoint = endpoint_url or "http://env-endpoint.com"
    assert client.meta.endpoint_url == expected_endpoint

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize(
    ("aws_access_key_id", "aws_secret_access_key"), [("aws_access_key_id", None), (None, "aws_secret_access_key")]
)
def test_get_s3_client_incomplete_creds_via_args(
    aws_access_key_id: str | None, aws_secret_access_key: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify that get_s3_client raises ValueError when only one of aws_access_key_id or aws_secret_access_key is provided via arguments."""
    prep_client_init(monkeypatch)
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001
    args = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }
    with pytest.raises(
        ValueError,
        match="Cannot initialise s3 client: aws_access_key_id and aws_secret_access_key must be provided together",
    ):
        get_s3_client(args)
    assert s3_client._s3_client is None  # noqa: SLF001

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize(
    ("aws_access_key_id", "aws_secret_access_key", "error_type", "error_msg"),
    [
        (
            "aws_access_key_id",
            None,
            PartialCredentialsError,
            "Partial credentials found in env, missing: AWS_SECRET_ACCESS_KEY",
        ),
        (None, "aws_secret_access_key", ValueError, "missing configuration values: aws_access_key_id"),
        (None, None, ValueError, "missing configuration values: aws_access_key_id, aws_secret_access_key"),
    ],
)
def test_get_s3_client_incomplete_creds_via_env(
    aws_access_key_id: str | None,
    aws_secret_access_key: str | None,
    error_type: type[Exception],
    error_msg: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that get_s3_client raises ValueError when only one of aws_access_key_id or aws_secret_access_key is provided."""
    prep_client_init(monkeypatch)
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001
    args = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }
    for key, value in args.items():
        if value:
            monkeypatch.setenv(key.upper(), value)
        else:
            monkeypatch.delenv(key.upper(), raising=False)
    # ensure that the AWS config file cannot accidentally be used to provide creds
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")

    with pytest.raises(
        error_type,
        match=error_msg,
    ):
        get_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
def test_get_s3_client_retry_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that the S3 client is configured with the expected retry mode and max attempts."""
    prep_client_init(monkeypatch)

    client = get_s3_client()
    retries_config = client.meta.config.retries

    assert retries_config["mode"] == s3_client.AWS_CLIENT_RETRY_MODE
    assert retries_config["total_max_attempts"] == s3_client.AWS_CLIENT_TOTAL_MAX_ATTEMPTS

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
def test_get_s3_client_ignores_unknown_args(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that unknown/unsupported keys in args are silently ignored on first (uncached) client creation."""
    prep_client_init(monkeypatch)

    args = {
        "aws_access_key_id": "valid_key",
        "aws_secret_access_key": "valid_secret",
        "unexpected_key": "should_be_ignored",
        # region_name is a legitimate boto3.client kwarg, but is NOT in the module's
        # allow-list, so it must be dropped rather than passed through to boto3.client()
        "region_name": "us-west-2",
    }

    client = get_s3_client(args)  # type: ignore[reportArgumentType]
    assert client is not None

    credentials = client._request_signer._credentials  # noqa: SLF001
    assert credentials.access_key == "valid_key"
    assert credentials.secret_key == "valid_secret"  # noqa: S105
    # endpoint_url was not in args, so it should fall back to the env var
    assert client.meta.endpoint_url == "http://env-endpoint.com"

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
def test_get_s3_client_default_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that get_s3_client falls back to the default AWS endpoint when none is configured via args or env."""
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001

    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws_access_key_id_env_var")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key_env_var")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    client = get_s3_client()
    assert client is not None
    assert "env-endpoint.com" not in client.meta.endpoint_url
    assert re.search(r"amazonaws\.com", client.meta.endpoint_url)

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize("args", [None, {}], ids=["none", "empty_dict"])
def test_get_s3_client_no_args_variants_are_equivalent(
    args: dict[str, str | None] | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify that get_s3_client(None) and get_s3_client({}) both behave like get_s3_client() with no args."""
    prep_client_init(monkeypatch)

    client = get_s3_client(args)
    assert client is not None

    credentials = client._request_signer._credentials  # noqa: SLF001
    assert credentials.access_key == "aws_access_key_id_env_var"
    assert credentials.secret_key == "aws_secret_access_key_env_var"  # noqa: S105
    assert client.meta.endpoint_url == "http://env-endpoint.com"

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@pytest.mark.s3
def test_reset_s3_client_idempotent() -> None:
    """Verify that reset_s3_client is safe to call repeatedly, including when no client has ever been created."""
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001

    # calling again with nothing to reset should not raise
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
def test_get_s3_client_returns_same_instance() -> None:
    """Verify that repeated calls to get_s3_client return the exact same cached client instance."""
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001

    args = {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
    }
    client_a = get_s3_client(args)  # type: ignore[reportArgumentType]
    assert s3_client._s3_client is not None  # noqa: SLF001
    # call again with no args - should return the stored version
    client_b = get_s3_client()
    assert client_a is client_b
    # call again with invalid args - should return the stored version, ignoring args
    client_c = get_s3_client(args={"this": "that", "pip": "pop"})
    assert client_c == client_a
    # reset the client and call
    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001
    client_d = get_s3_client(
        {
            "endpoint_url": "http://localhost:9000",
            "aws_access_key_id": "not a key",
            "aws_secret_access_key": "not a secret",
        }
    )
    assert client_d != client_a

    reset_s3_client()
    assert s3_client._s3_client is None  # noqa: SLF001


# split_s3_path
PATH = "path"
TO = "to"
TO_FILE = "to/file.txt"
PATH_TO_FILE = f"{PATH}/{TO_FILE}"

EXPECTED = {
    "path/to": (PATH, TO),
    "path/to/": (PATH, "to/"),
    "path/to/file.txt": (PATH, TO_FILE),
    "s3://path/to": (PATH, TO),
    "s3://path/to/": (PATH, "to/"),
    "s3://path/to/file.txt": (PATH, TO_FILE),
    "s3a://path/to": (PATH, TO),
    "s3a://path/to/": (PATH, "to/"),
    "s3a://path/to/file.txt": (PATH, TO_FILE),
}

NO_PATH_FOUND = "No path found"
START_WITH_BUCKET_NAME = "s3 paths must start with the bucket name"
COULD_NOT_PARSE = "Could not parse out bucket and key"

INVALID_PATH_ERRORS = {
    "": NO_PATH_FOUND,
    "/": START_WITH_BUCKET_NAME,
    "/path": START_WITH_BUCKET_NAME,
    "/path/to/file.txt": START_WITH_BUCKET_NAME,
    "path": COULD_NOT_PARSE,
    "path/": COULD_NOT_PARSE,
    "s3://": NO_PATH_FOUND,
    "s3:///": START_WITH_BUCKET_NAME,
    "s3://path": COULD_NOT_PARSE,
    "s3://path/": COULD_NOT_PARSE,
    "s3a://": NO_PATH_FOUND,
    "s3a://path": COULD_NOT_PARSE,
    "s3a://path/": COULD_NOT_PARSE,
}


@pytest.mark.parametrize("invalid_path", list(INVALID_PATH_ERRORS.keys()))
def test_split_s3_path_errors(invalid_path: str) -> None:
    """Ensure that an error is thrown if an invalid s3 path is passed in."""
    with pytest.raises(ValueError, match=INVALID_PATH_ERRORS[invalid_path]):
        split_s3_path(invalid_path)


@pytest.mark.parametrize("valid_path", list(EXPECTED.keys()))
def test_split_s3_path_success(valid_path: str) -> None:
    """Verify that a valid path is correctly split into bucket and key."""
    (bucket, path) = split_s3_path(valid_path)
    assert (bucket, path) == EXPECTED[valid_path]


EXPECTED_ALLOW_BUCKET_ONLY = {
    "path/to": (PATH, TO),
    "path/to/": (PATH, "to/"),
    "path/to/file.txt": (PATH, TO_FILE),
    "path": (PATH, ""),
    "path/": (PATH, ""),
    "s3://path/to": (PATH, TO),
    "s3://path/to/": (PATH, "to/"),
    "s3://path/to/file.txt": (PATH, TO_FILE),
    "s3://path": (PATH, ""),
    "s3://path/": (PATH, ""),
    "s3a://path/to": (PATH, TO),
    "s3a://path/to/": (PATH, "to/"),
    "s3a://path/to/file.txt": (PATH, TO_FILE),
    "s3a://path": (PATH, ""),
    "s3a://path/": (PATH, ""),
}

INVALID_PATH_ALLOW_BUCKET_ONLY_ERRORS = {
    "": NO_PATH_FOUND,
    "/": START_WITH_BUCKET_NAME,
    "/path": START_WITH_BUCKET_NAME,
    "/path/to/file.txt": START_WITH_BUCKET_NAME,
    "s3://": NO_PATH_FOUND,
    "s3:///": START_WITH_BUCKET_NAME,
    "s3a://": NO_PATH_FOUND,
}


@pytest.mark.parametrize("invalid_path", list(INVALID_PATH_ALLOW_BUCKET_ONLY_ERRORS.keys()))
def test_split_s3_path_allow_bucket_only_errors(invalid_path: str) -> None:
    """Ensure that an error is thrown if an invalid s3 path is passed in."""
    with pytest.raises(ValueError, match=INVALID_PATH_ALLOW_BUCKET_ONLY_ERRORS[invalid_path]):
        split_s3_path(invalid_path, allow_bucket_only=True)


@pytest.mark.parametrize("valid_path", list(EXPECTED_ALLOW_BUCKET_ONLY.keys()))
def test_split_s3_path_allow_bucket_only_success(valid_path: str) -> None:
    """Verify that a valid path is correctly split into bucket and key."""
    (bucket, path) = split_s3_path(valid_path, allow_bucket_only=True)
    assert (bucket, path) == EXPECTED_ALLOW_BUCKET_ONLY[valid_path]
