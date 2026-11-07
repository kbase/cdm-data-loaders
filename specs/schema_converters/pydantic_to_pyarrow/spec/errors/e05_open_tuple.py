"""E5 fixture: Open tuple mixing fixed prefix elements with a variadic tail."""

from pydantic import BaseModel, ConfigDict


class OpenTupleRecord(BaseModel):
    model_config = ConfigDict(defer_build=True)

    id: str
    items: tuple[int, *tuple[str, ...]]


MODEL = OpenTupleRecord
