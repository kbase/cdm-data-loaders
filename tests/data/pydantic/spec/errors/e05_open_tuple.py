"""E5 fixture: Open tuple mixing fixed prefix elements with a variadic tail."""

# ruff: noqa: D101, INP001

from pydantic import BaseModel, ConfigDict


class OpenTupleRecord(BaseModel):
    model_config = ConfigDict(defer_build=True)

    id: str
    items: tuple[int, *tuple[str, ...]]


MODEL = OpenTupleRecord
