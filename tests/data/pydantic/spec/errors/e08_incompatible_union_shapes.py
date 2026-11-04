"""E8 fixture: Union mixing incompatible structural shapes (BaseModel and list)."""

# ruff: noqa: D101, INP001

from pydantic import BaseModel


class SubItem(BaseModel):
    name: str


class IncompatibleUnionRecord(BaseModel):
    id: str
    value: SubItem | list[int]


MODEL = IncompatibleUnionRecord
