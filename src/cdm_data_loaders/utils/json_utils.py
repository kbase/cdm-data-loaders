"""Shared JSON scalar serialization helpers for temporal, decimal, path, UUID, and binary values."""

import base64
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import PurePath
from typing import Literal
from uuid import UUID

type BytesJsonEncoding = Literal["utf-8", "utf-8-replace", "base64", "disallow"]


def json_default_scalar(
    value: object,
    *,
    bytes_encoding: BytesJsonEncoding = "utf-8",
) -> str:
    """Serialize non-JSON-native Python scalar objects into strings for ``json.dumps``.

    :param value: Candidate scalar object not supported by default in ``json.dumps``.
    :type value: object
    :param bytes_encoding: Strategy for ``bytes`` and ``bytearray`` values.
    :type bytes_encoding: BytesJsonEncoding
    :return: JSON-compatible string representation of ``value``.
    :rtype: str
    :raises TypeError: If ``value`` is not a supported scalar type (or ``bytes`` when disallowed).
    """
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, Decimal | UUID | PurePath):
        return str(value)
    if isinstance(value, bytes | bytearray) and bytes_encoding != "disallow":
        raw = bytes(value)
        if bytes_encoding == "base64":
            return base64.b64encode(raw).decode("ascii")
        if bytes_encoding == "utf-8-replace":
            return raw.decode("utf-8", errors="replace")
        return raw.decode("utf-8")
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)
