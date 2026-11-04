"""E9 fixture: Model with an unresolved forward reference."""

# ruff: noqa: D101, INP001

from pydantic import BaseModel


class BrokenForwardRefRecord(BaseModel):
    id: str
    child: "UndefinedChild"  # noqa: F821


MODEL = BrokenForwardRefRecord
