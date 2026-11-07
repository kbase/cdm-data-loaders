"""E2 fixture: RootModel wrapping a non-model type."""

from pydantic import RootModel


class IntListRoot(RootModel[list[int]]):
    pass


MODEL = IntListRoot
