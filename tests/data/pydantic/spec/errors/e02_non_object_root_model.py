"""E2 fixture: RootModel wrapping a non-model type."""

# ruff: noqa: D101, INP001

from pydantic import RootModel


class IntListRoot(RootModel[list[int]]):
    pass


MODEL = IntListRoot
