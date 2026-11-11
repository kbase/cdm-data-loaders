"""Entity models, file pre-processors, table transforms, and output schemas for JSONLines (Pydantic) spec cases."""

from typing import Any, Final

import pyarrow as pa
from pydantic import BaseModel, Field

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

NEUTRAL_PH: Final[float] = 7.0

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)


class AssayModel(BaseModel):
    """Input assay record validated by Pydantic before table transformation."""

    assay_id: str
    ph: float
    exclude: bool = False
    metrics: list[str] = Field(default_factory=list)


ASSAY_SUMMARY_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("assay_id", pa.string(), nullable=False),
        pa.field("ph", pa.float64(), nullable=False),
        pa.field("ph_bucket", pa.string(), nullable=False),
    ]
)

ASSAY_METRIC_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("assay_id", pa.string(), nullable=False),
        pa.field("metric", pa.string(), nullable=False),
    ]
)


def transform_assay(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Drop excluded assays, classify pH bucket, and fan out metrics to ``assay_metrics``.

    :param row: Validated and dumped ``AssayModel`` dictionary.
    :type row: dict[str, Any]
    :return: Mapping of target table names to lists of output row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if row.get("exclude") is True:
        return {}
    assay_id = str(row["assay_id"]).upper()
    ph = float(row["ph"])
    ph_bucket = "acidic" if ph < NEUTRAL_PH else "neutral_or_basic"
    routed: dict[str, list[dict[str, Any]]] = {
        "assays": [
            {
                "assay_id": assay_id,
                "ph": ph,
                "ph_bucket": ph_bucket,
            }
        ]
    }
    metrics = row.get("metrics") or []
    if metrics:
        routed["assay_metrics"] = [{"assay_id": assay_id, "metric": str(m)} for m in metrics]
    return routed


ENTITY_MODELS: Final[dict[str, type[BaseModel]]] = {
    "assays": AssayModel,
}

TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "assays": transform_assay,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "assays": ASSAY_SUMMARY_SCHEMA,
    "assay_metrics": ASSAY_METRIC_SCHEMA,
}
