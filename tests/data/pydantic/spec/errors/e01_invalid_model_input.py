"""E1 fixture: input is not a Pydantic BaseModel subclass."""

# ruff: noqa: D101, INP001

from dataclasses import dataclass


@dataclass
class NotAPydanticModel:
    id: str


MODEL = NotAPydanticModel
