"""Unit tests for ``cdm_data_loaders.utils.json_utils``."""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import PurePosixPath
from uuid import UUID

import pytest

from cdm_data_loaders.utils.json_utils import BytesJsonEncoding, json_default_scalar


@pytest.mark.parametrize(
    ("value", "bytes_encoding", "expected"),
    [
        pytest.param(date(2024, 2, 29), "utf-8", "2024-02-29", id="date"),
        pytest.param(
            datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
            "utf-8",
            "2024-02-29T12:30:00+00:00",
            id="datetime-utc",
        ),
        pytest.param(time(8, 15, 30), "utf-8", "08:15:30", id="time"),
        pytest.param(Decimal("-19.95"), "utf-8", "-19.95", id="decimal-negative"),
        pytest.param(Decimal(0), "utf-8", "0", id="decimal-zero"),
        pytest.param(
            UUID("12345678-1234-5678-1234-567812345678"),
            "utf-8",
            "12345678-1234-5678-1234-567812345678",
            id="uuid",
        ),
        pytest.param(PurePosixPath("/data/input.tsv"), "utf-8", "/data/input.tsv", id="pure-path"),
        pytest.param(b"hello", "utf-8", "hello", id="bytes-utf8"),
        pytest.param(bytearray(b"world"), "utf-8", "world", id="bytearray-utf8"),
        pytest.param(b"\xffa", "utf-8-replace", "\ufffda", id="bytes-utf8-replace"),
        pytest.param(b"\xde\xad\xbe\xef", "base64", "3q2+7w==", id="bytes-base64"),
    ],
)
def test_json_default_scalar_serializes_supported_scalars(
    value: object,
    bytes_encoding: BytesJsonEncoding,
    expected: str,
) -> None:
    """Serialize temporal, Decimal, UUID, PurePath, and binary values according to bytes_encoding."""
    assert json_default_scalar(value, bytes_encoding=bytes_encoding) == expected


@pytest.mark.parametrize(
    ("value", "bytes_encoding"),
    [
        pytest.param(object(), "utf-8", id="arbitrary-object"),
        pytest.param(b"raw", "disallow", id="bytes-disallowed"),
        pytest.param(bytearray(b"raw"), "disallow", id="bytearray-disallowed"),
    ],
)
def test_json_default_scalar_raises_type_error_for_unsupported_values(
    value: object,
    bytes_encoding: BytesJsonEncoding,
) -> None:
    """Raise TypeError for unsupported objects or bytes when bytes_encoding is 'disallow'."""
    with pytest.raises(TypeError, match="is not JSON serializable"):
        json_default_scalar(value, bytes_encoding=bytes_encoding)
