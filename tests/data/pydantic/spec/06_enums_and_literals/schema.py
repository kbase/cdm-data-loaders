"""Spec 06: StrEnum, IntEnum, general Enum with docstring, and Literal types."""

# ruff: noqa: D101, INP001, PYI061

from enum import Enum, IntEnum, StrEnum
from typing import Literal

from pydantic import BaseModel


class Strand(StrEnum):
    """Genomic strand orientation."""

    PLUS = "+"
    MINUS = "-"


class PriorityLevel(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class MixedCodeEnum(Enum):
    """Mixed integer and string status codes."""

    OK = 200
    CUSTOM = "custom-ok"


class EnumAndLiteralRecord(BaseModel):
    id: str
    strand: Strand
    priority: PriorityLevel
    mixed_code: MixedCodeEnum
    string_literal: Literal["draft", "complete", "archived"]
    int_literal: Literal[10, 20, 30]
    bool_literal: Literal[True]
    num_widened_literal: Literal[1, 2.5]
    heterogeneous_literal: Literal[1, "two", True]
    nullable_literal: Literal["active", None] = None


MODEL = EnumAndLiteralRecord
