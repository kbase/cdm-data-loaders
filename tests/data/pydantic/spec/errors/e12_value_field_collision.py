"""E12 fixture: Scalar + model union where the model branch declares '_value'."""

# ruff: noqa: D101, INP001

from pydantic import BaseModel


class CollidingModelBranch(BaseModel):
    label: str
    value_col: str


# expose _value as a declared field on CollidingModelBranch
CollidingModelBranch.model_fields["_value"] = CollidingModelBranch.model_fields.pop("value_col")


class ValueCollisionRecord(BaseModel):
    id: str
    item: CollidingModelBranch | int


MODEL = ValueCollisionRecord
