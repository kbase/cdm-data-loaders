"""E7 fixture: Field using an unsupported Python runtime type (Callable)."""

# ruff: noqa: D101, INP001

from collections.abc import Callable

from pydantic import BaseModel


class CallbackRecord(BaseModel):
    id: str
    handler: Callable[[int], str]


MODEL = CallbackRecord
