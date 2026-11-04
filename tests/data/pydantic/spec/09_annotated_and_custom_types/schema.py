"""Spec 09: PEP 695 type aliases, nested Annotated metadata precedence, and functional validators."""

# ruff: noqa: D101, INP001

from decimal import Decimal
from typing import Annotated

from pydantic import AfterValidator, BaseModel, Field

type AccessionStr = Annotated[
    str,
    AfterValidator(str.strip),
    Field(min_length=3, description="Base accession string"),
]
type HighPrecisionCoverage = Annotated[
    Decimal,
    Field(max_digits=8, decimal_places=3, description="Sequencing depth coverage"),
]


class AnnotatedShowcaseRecord(BaseModel):
    accession: AccessionStr
    overridden_accession: Annotated[AccessionStr, Field(description="Overridden accession description")]
    coverage: HighPrecisionCoverage
    quality_phred: Annotated[int, Field(ge=0, le=60, description="Phred quality score")]


MODEL = AnnotatedShowcaseRecord
