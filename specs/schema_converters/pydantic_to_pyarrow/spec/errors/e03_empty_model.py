"""E3 fixture: BaseModel with zero fields."""

from pydantic import BaseModel


class EmptyRecord(BaseModel):
    pass


MODEL = EmptyRecord
