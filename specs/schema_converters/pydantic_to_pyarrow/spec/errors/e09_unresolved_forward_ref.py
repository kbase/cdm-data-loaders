"""E9 fixture: Model with an unresolved forward reference."""

from pydantic import BaseModel


class BrokenForwardRefRecord(BaseModel):
    id: str
    child: "UndefinedChild"  # noqa: F821


MODEL = BrokenForwardRefRecord
