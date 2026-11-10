"""Entity Pydantic model registry for JSONLines (Pydantic) specification cases."""

from typing import Final

from pydantic import BaseModel, Field


class SampleModel(BaseModel):
    """Validated biological sample record."""

    sample_id: str
    ph: float
    biome: str = "terrestrial"
    tags: list[str] = Field(default_factory=list)


class InstrumentModel(BaseModel):
    """Sequencing instrument metadata record."""

    instrument_id: str
    platform: str
    active: bool = True


ENTITY_MODELS: Final[dict[str, type[BaseModel]]] = {
    "samples": SampleModel,
    "instruments": InstrumentModel,
    "unpopulated": InstrumentModel,
}
