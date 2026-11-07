"""E8 fixture: Union mixing incompatible structural shapes (BaseModel and list)."""

from pydantic import BaseModel


class SubItem(BaseModel):
    name: str


class IncompatibleUnionRecord(BaseModel):
    id: str
    value: SubItem | list[int]


MODEL = IncompatibleUnionRecord
