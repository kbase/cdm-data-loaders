"""E1 fixture: input is not a Pydantic BaseModel subclass."""

from dataclasses import dataclass


@dataclass
class NotAPydanticModel:
    id: str


MODEL = NotAPydanticModel
