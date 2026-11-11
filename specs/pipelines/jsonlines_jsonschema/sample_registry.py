"""Entity schema registry for JSONLines (JSON Schema) base pipeline specification cases."""

import json
from pathlib import Path
from typing import Any

SPEC_DIR = Path(__file__).parent / "spec"


def _load_schema(rel_path: str) -> dict[str, Any]:
    return json.loads((SPEC_DIR / rel_path).read_text(encoding="utf-8"))


ENTITY_SCHEMAS: dict[str, dict[str, Any]] = {
    "genomes": _load_schema("01_scalars_nested_types_and_extra_fields/schema.json"),
    "submissions": _load_schema("02_defs_ref_allof_and_draft_dialects/schema.json"),
    "contacts": _load_schema("03_blank_lines_parse_errors_and_format_rejections/schema.json"),
    "projects": _load_schema("04_multi_entity_gzip_and_subdirs/schema.json"),
    "experiments": _load_schema("04_multi_entity_gzip_and_subdirs/experiment_schema.json"),
    "missing_entity": _load_schema("04_multi_entity_gzip_and_subdirs/schema.json"),
    "articles": _load_schema("05_md5_sidecar_preprocessing/schema.json"),
}
