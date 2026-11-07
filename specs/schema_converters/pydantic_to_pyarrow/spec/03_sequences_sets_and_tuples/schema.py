"""Spec 03: Lists, sets, frozensets, variadic tuples, and fixed-length tuples."""

from pydantic import BaseModel


class SequenceAndTupleRecord(BaseModel):
    id: str
    tags: list[str]
    nullable_scores: list[int | None]
    matrix: list[list[float]]
    unique_codes: set[str]
    frozen_flags: frozenset[int]
    variadic_labels: tuple[str, ...]
    point_3d: tuple[str, int, float | None]
    optional_pair: tuple[int, str] | None = None


MODEL = SequenceAndTupleRecord
