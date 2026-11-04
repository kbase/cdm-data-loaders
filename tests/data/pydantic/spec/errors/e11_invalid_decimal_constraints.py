"""E11 fixture: Decimal with max_digits > 38."""

# ruff: noqa: D101, INP001

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, Field


class ExcessiveDecimalRecord(BaseModel):
    id: str
    amount: Annotated[Decimal, Field(max_digits=50, decimal_places=10)]


MODEL = ExcessiveDecimalRecord
