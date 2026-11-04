"""Spec 01: Built-in scalars, Pydantic special types, temporal types, binary, and Decimals."""

# ruff: noqa: INP001

import datetime as dt
from decimal import Decimal
from ipaddress import IPv4Address
from pathlib import Path
from typing import Annotated
from uuid import UUID

from pydantic import (
    AnyUrl,
    Base64Bytes,
    Base64Str,
    BaseModel,
    EmailStr,
    Field,
    NaiveDatetime,
    PositiveInt,
    SecretBytes,
    SecretStr,
)


class ScalarShowcaseRecord(BaseModel):
    """Comprehensive scalar type mapping showcase."""

    id: Annotated[str, Field(description="Primary record identifier")]
    active: bool
    count: PositiveInt
    ratio: float
    price: Annotated[Decimal, Field(max_digits=10, decimal_places=2, description="Fixed-scale price")]
    unconstrained_dec: Decimal
    release_date: dt.date
    created_at: dt.datetime
    local_ts: NaiveDatetime
    alarm_time: dt.time
    elapsed: dt.timedelta
    item_uuid: UUID
    config_path: Path
    host_ip: IPv4Address
    homepage: AnyUrl
    contact_email: EmailStr
    api_token: SecretStr
    raw_bytes: bytes
    secret_blob: SecretBytes
    encoded_payload: Base64Bytes
    encoded_text: Base64Str
    optional_note: Annotated[str | None, Field(description="Optional free-text note")] = None


MODEL = ScalarShowcaseRecord
