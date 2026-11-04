"""E6 fixture: Model with fixed fields and extra='allow'."""

# ruff: noqa: D101, INP001

from pydantic import BaseModel, ConfigDict


class OpenExtraRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    count: int


MODEL = OpenExtraRecord
