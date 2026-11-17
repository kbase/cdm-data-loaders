"""Entity schema registries, file pre-processors, table transforms, and output schemas for JSONLines spec cases."""

import json
from pathlib import Path
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.md5_preprocessor import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

SPEC_DIR = Path(__file__).parent / "spec"
NEUTRAL_PH: Final[float] = 7.0

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

SAMPLE_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("ph", pa.decimal128(4, 2), nullable=False),
        pa.field("ph_bucket", pa.string(), nullable=False),
        pa.field("collected_date", pa.date32(), nullable=False),
        pa.field("collected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sample_time", pa.time64("us"), nullable=False),
        pa.field("checksum", pa.binary(), nullable=True, metadata={"xsd_type": "xs:hexBinary"}),
    ]
)

SAMPLE_MEASUREMENT_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("metric", pa.string(), nullable=False),
    ]
)


def transform_sample(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Drop excluded samples, bucket pH values, and fan out metrics to ``sample_measurements``.

    :param row: Validated input sample dictionary.
    :type row: dict[str, Any]
    :return: Mapping of target table names to lists of output row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if row.get("exclude") is True:
        return {}
    sample_id = str(row["sample_id"]).upper()
    ph_bucket = "acidic" if float(row["ph"]) < NEUTRAL_PH else "neutral_or_basic"
    summary_row: dict[str, Any] = {
        "sample_id": sample_id,
        "ph": row["ph"],
        "ph_bucket": ph_bucket,
        "collected_date": str(row["collected_at"])[:10],
        "collected_at": row["collected_at"],
        "sample_time": row["sample_time"],
    }
    if row.get("checksum_hex") is not None:
        summary_row["checksum"] = row["checksum_hex"]
    routed: dict[str, list[dict[str, Any]]] = {"samples": [summary_row]}
    metrics = row.get("metrics") or []
    if metrics:
        routed["sample_measurements"] = [{"sample_id": sample_id, "metric": str(m)} for m in metrics]
    return routed


ENTITY_SCHEMAS: dict[str, dict[str, Any]] = {
    "samples": json.loads((SPEC_DIR / "06_table_transforms_and_output_schemas" / "schema.json").read_text("utf-8")),
}

TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "samples": transform_sample,
}

OUTPUT_SCHEMAS: dict[str, pa.Schema] = {
    "samples": SAMPLE_SUMMARY_SCHEMA,
    "sample_measurements": SAMPLE_MEASUREMENT_SCHEMA,
}
