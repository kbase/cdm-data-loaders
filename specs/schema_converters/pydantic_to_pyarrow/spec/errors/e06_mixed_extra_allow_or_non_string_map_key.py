"""E6 fixture: Model with fixed fields and extra='allow'."""

from pydantic import BaseModel, ConfigDict


class OpenExtraRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    count: int


MODEL = OpenExtraRecord
