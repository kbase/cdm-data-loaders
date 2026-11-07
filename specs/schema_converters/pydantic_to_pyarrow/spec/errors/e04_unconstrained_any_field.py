"""E4 fixture: Field annotated as unconstrained Any."""

from typing import Any

from pydantic import BaseModel


class UnconstrainedPayload(BaseModel):
    id: str
    payload: Any


MODEL = UnconstrainedPayload
