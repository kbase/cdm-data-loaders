"""E3 fixture: BaseModel with zero fields."""

# ruff: noqa: D101, INP001

from pydantic import BaseModel


class EmptyRecord(BaseModel):
    pass


MODEL = EmptyRecord
