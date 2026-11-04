"""E4 fixture: Field annotated as unconstrained Any."""

# ruff: noqa: D101, INP001

from typing import Any

from pydantic import BaseModel


class UnconstrainedPayload(BaseModel):
    id: str
    payload: Any


MODEL = UnconstrainedPayload
