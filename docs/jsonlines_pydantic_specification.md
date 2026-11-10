<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/pipelines/jsonlines_pydantic/`. Regenerate with `uv run python -m scripts.gen_pipeline_spec` or `uv run python -m scripts.gen_converter_spec`. -->

# JSONLines (Pydantic) Pipeline Specification

This document specifies how an **`ENTITY_MODELS` registry** of Pydantic `BaseModel` classes and the
**JSONLines (Pydantic) ingestion pipeline**
(`cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline`,
`cdm_data_loaders.pipelines.jsonlines.settings.JsonlPydanticIngestSettings`, and
`cdm_data_loaders.converters.facades.pydantic_to_pyarrow`) determine:

1. **Entity model registration and upfront PyArrow schema generation** (`load_registry_dict`, `build_output_schema_registry`, `PydanticToPyArrow`).
2. **Single-pass Pydantic validation, `model_dump(mode="python")` coercion, and dead-letter (`_rejected`) routing** (`_make_validator_callbacks`, `make_page_router`, `REJECTED_PYARROW_SCHEMA`).
3. **Multi-entity directory discovery, MD5 sidecar pre-processing, table transforms, and reporting** (`build_entity_resource`, `run_jsonlines_with_validation_pipeline`).

| Property | Value |
| --- | --- |
| **Base Pipeline Family** | `jsonlines_pydantic` |
| **Base CLI Entry Point** | `jsonlines_pydantic` |
| **Base Settings Class** | `cdm_data_loaders.pipelines.jsonlines.settings.JsonlPydanticIngestSettings` |
| **Pipeline Runner** | `cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline.run_jsonlines_with_validation_pipeline` |
| **Preset Subclasses Covered** | — |
| **Specification Directory** | `specs/pipelines/jsonlines_pydantic/` |

## Running Smoke Tests Across Environments

```bash
# Run all base and preset smoke tests for jsonlines_pydantic across built-in environments
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py --smoke-pipeline jsonlines_pydantic

# Run against a custom environment directory (config.toml, secrets.toml, settings.toml)
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py \
    --smoke-pipeline jsonlines_pydantic \
    --dlt-config-dir /path/to/env_dlt_config \
    --settings-file /path/to/env_dlt_config/settings.toml
```

---

## 1. Conversion & Pipeline Rules

### 1.1 Entity Model Registry and PyArrow Schema Generation

<a id="r-jsonl-pydantic-entity-registry"></a>

- **R-JSONL-PYDANTIC-ENTITY-REGISTRY**: `JsonlPydanticIngestSettings` loads `ENTITY_MODELS` from the module named by `entity_models`
  (`-m` / `--entity-models`) via `load_registry_dict(self.entity_models, "ENTITY_MODELS")` (`ModelRegistry`).
  Every key must be a non-empty table name (excluding the reserved `_rejected` table) and every value must
  be a `pydantic.BaseModel` subclass.

  *Cases:* [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections)

<a id="r-jsonl-pydantic-schema-convert"></a>

- **R-JSONL-PYDANTIC-SCHEMA-CONVERT**: When `use_pyarrow=True` (default), `build_output_schema_registry` converts each entity `BaseModel` in
  `ENTITY_MODELS` to a `pa.Schema` up front via `PydanticToPyArrow().convert(model)` per the scalar mapping
  table below and the [`pydantic_to_pyarrow` specification](pydantic_to_pyarrow_specification.md).

  *Cases:* [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections)

<a id="r-jsonl-pydantic-schema-skip"></a>

- **R-JSONL-PYDANTIC-SCHEMA-SKIP**: Automatic `PydanticToPyArrow` conversion is skipped for any entity table that has a registered transform
  in `TABLE_TRANSFORMS` (`transforms_module`) or an explicit PyArrow schema override in `OUTPUT_SCHEMAS`
  (`output_schemas_module`), allowing transforms to reshape validated models before PyArrow coercion.

  *Cases:* [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

| Pydantic type / annotation | Python type (coerced) | PyArrow `DataType` | `metadata["pydantic_type"]` |
| :--- | :--- | :--- | :--- |
| `bool`, `StrictBool` | `bool` | `pa.bool_()` | `boolean` |
| `int`, `StrictInt`, `PositiveInt`, `NegativeInt`, `NonNegativeInt`, `NonPositiveInt` | `int` | `pa.int64()` | `integer` |
| `float`, `StrictFloat`, `PositiveFloat`, `NegativeFloat`, `NonNegativeFloat`, `NonPositiveFloat` | `float` | `pa.float64()` | `number` |
| `str`, `StrictStr`, `SecretStr`, `EmailStr`, `NameEmail`, `AnyUrl`, `HttpUrl`, `Base64Str` | `str` | `pa.string()` | `string` |
| `Decimal` (unconstrained) | `float` | `pa.float64()` | `number` |
| `Annotated[Decimal, Field(max_digits=10, decimal_places=2)]` | `decimal.Decimal` | `pa.decimal128(10, 2)` | `decimal` |
| `datetime.date`, `PastDate`, `FutureDate` | `datetime.date` | `pa.date32()` | `date` |
| `datetime.datetime`, `AwareDatetime`, `PastDatetime`, `FutureDatetime` | `datetime.datetime` (UTC) | `pa.timestamp("us", tz="UTC")` | `date-time` |
| `NaiveDatetime` | `datetime.datetime` (naive) | `pa.timestamp("us")` | `date-time-naive` |
| `datetime.time` | `datetime.time` | `pa.time64("us")` | `time` |
| `datetime.timedelta` | `str` | `pa.string()` | `duration` |
| `UUID`, `UUID1`..`UUID5` | `str` | `pa.string()` | `uuid` |
| `Path`, `PurePath`, `FilePath`, `DirectoryPath`, `NewPath` | `str` | `pa.string()` | `path` |
| `IPv4Address`, `IPv6Address`, `IPv4Network`, `IPv6Network`, `IPvAnyAddress`, `IPvAnyInterface`, `IPvAnyNetwork` | `str` | `pa.string()` | `ip` |
| `bytes`, `bytearray`, `SecretBytes` | `bytes` | `pa.binary()` | `binary` |
| `Base64Bytes` | `bytes` | `pa.binary()` | `binary (base64)` |
| `StrEnum`, `Enum` (str values) | `str` | `pa.string()` | `string` |
| `IntEnum`, `Enum` (int values) | `int` | `pa.int64()` | `integer` |
| `Literal[1, 2]` | `int` | `pa.int64()` | `integer` |
| `Literal[1, 2.5]` | `float` | `pa.float64()` | `integer \| number` |
| `Literal[1, "two", True]` | `str` | `pa.string()` | `integer \| string \| boolean` |
| `int \| float` | `float` | `pa.float64()` | `integer \| number` |
| `int \| str` | `str` | `pa.string()` | `integer \| string` |
| `str \| None` | `str` | `pa.string()` | `string` |

### 1.2 Single-Pass Record Validation, Dumping, and Dead-Letter Routing

<a id="r-jsonl-pydantic-validate-coerce"></a>

- **R-JSONL-PYDANTIC-VALIDATE-COERCE**: `_make_validator_callbacks` validates each non-blank JSON object at most once via
  `model.model_validate(record).model_dump(mode="python")`, caching the dumped Python dictionary across
  `_validate` and `_dump_validated` so default values, type coercions, and nested models are materialized
  before routing into `DictBuffer` (`schema_map=settings.schema_map`).

  *Cases:* [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

<a id="r-jsonl-pydantic-blank-and-reject"></a>

- **R-JSONL-PYDANTIC-BLANK-AND-REJECT**: Blank or whitespace-only lines are skipped without error while preserving 1-based `line_no` offsets.
  Lines that fail `json.loads` (`error_detail=str(e)`) bypass Pydantic validation, and lines that fail
  `model.model_validate` (`error_detail=json.dumps(e.errors(), default=str)`) are routed to `_rejected`
  (`REJECTED_PYARROW_SCHEMA`) with `table_name=<entity>`, `source_file` relative to `input_dir`, 1-based
  `line_no`, and stripped `raw_record`.

  *Cases:* [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections)

| Column | PyArrow `DataType` | `Field.nullable` | Description |
| :--- | :--- | :--- | :--- |
| `table_name` | `pa.string()` | `True` | Target entity table name (`None` for level-mode file pre-processor errors) |
| `source_file` | `pa.string()` | `True` | Input file path relative to `input_dir` |
| `line_no` | `pa.int64()` | `True` | 1-based physical line number in `source_file` (`None` for file-level errors) |
| `raw_record` | `pa.string()` | `True` | Stripped raw input line (`None` for file-level errors) |
| `error_detail` | `pa.string()` | `True` | Parser error message, JSON-encoded list of schema errors, or pre-processor error |

### 1.3 Multi-Entity Discovery, Pre-processing, Transforms, and Reporting

<a id="r-jsonl-pydantic-multi-entity-glob"></a>

- **R-JSONL-PYDANTIC-MULTI-ENTITY-GLOB**: `run_jsonlines_with_validation_pipeline` builds one resource per entity in `sorted(ENTITY_MODELS)` scanning
  `<input_dir>/<table_name>/<settings.file_glob>` (default `*.jsonl*`) and transparently decompressing `.gz`
  files. Entities whose `<input_dir>/<table_name>` directory does not exist produce zero rows without error.

  *Cases:* [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections)

<a id="r-jsonl-pydantic-preprocess-md5"></a>

- **R-JSONL-PYDANTIC-PREPROCESS-MD5**: Before reading an entity file, `build_filesystem_resource` runs `settings.get_file_preprocessors(table_name)`
  (from `preprocessors_module`, e.g. `validate_md5_sidecar`). When a pre-processor raises `FilePreprocessError`,
  the file is skipped, omitted from `ingested_files`, recorded in `report.txt` `load_errors`
  (`status: completed with errors`), and routed to `_rejected` with `table_name=<entity>`,
  `source_file=<entity>/<file>`, `line_no=None`, and `raw_record=None`.

  *Cases:* [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing)

<a id="r-jsonl-pydantic-transforms"></a>

- **R-JSONL-PYDANTIC-TRANSFORMS**: When `transforms_module` (`TABLE_TRANSFORMS`) and `output_schemas_module` (`OUTPUT_SCHEMAS`) are configured,
  validated `model_dump(mode="python")` records are routed through `route_and_coerce_record`, allowing
  transforms to filter rows (`{}` / `None`), compute derived columns, or fan out records across multiple
  destination tables coerced to their `OUTPUT_SCHEMAS`.

  *Cases:* [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

<a id="r-jsonl-pydantic-report"></a>

- **R-JSONL-PYDANTIC-REPORT**: All entity resources execute in a single `pipeline.run(resources)` call with `max_table_nesting=0` and
  write `report.txt` (`status`, `ingested_files`, `load_errors`, `pipeline_load_info_errs`) and `_load_info`.

  *Cases:* [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections), [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

---

## 2. Case Coverage Matrix

| Suite | Target CLI | Settings Class | Case | Title | Rules | Expected Table Counts |
| --- | --- | --- | --- | --- | --- | --- |
| `spec` | `jsonlines_pydantic` | `JsonlPydanticIngestSettings` | [`01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections) | Multi-Entity Validation, Model Coercion, Gzip Decompression, and `_rejected` Routing | [`R-JSONL-PYDANTIC-ENTITY-REGISTRY`](#r-jsonl-pydantic-entity-registry), [`R-JSONL-PYDANTIC-SCHEMA-CONVERT`](#r-jsonl-pydantic-schema-convert), [`R-JSONL-PYDANTIC-VALIDATE-COERCE`](#r-jsonl-pydantic-validate-coerce), [`R-JSONL-PYDANTIC-BLANK-AND-REJECT`](#r-jsonl-pydantic-blank-and-reject), [`R-JSONL-PYDANTIC-MULTI-ENTITY-GLOB`](#r-jsonl-pydantic-multi-entity-glob), [`R-JSONL-PYDANTIC-REPORT`](#r-jsonl-pydantic-report) | `default`: `{'instruments': 1, 'samples': 2, '_rejected': 2}` |
| `spec` | `jsonlines_pydantic` | `JsonlPydanticIngestSettings` | [`02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing) | MD5 Sidecar Pre-processing and File-Level `_rejected` Routing | [`R-JSONL-PYDANTIC-PREPROCESS-MD5`](#r-jsonl-pydantic-preprocess-md5), [`R-JSONL-PYDANTIC-REPORT`](#r-jsonl-pydantic-report) | `partial_md5_failure`: `{'samples': 1, '_rejected': 1}` |
| `spec` | `jsonlines_pydantic` | `JsonlPydanticIngestSettings` | [`03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas) | Table Transforms, Schema Conversion Skip, Multi-Table Fan-Out, and Output Schemas | [`R-JSONL-PYDANTIC-SCHEMA-SKIP`](#r-jsonl-pydantic-schema-skip), [`R-JSONL-PYDANTIC-VALIDATE-COERCE`](#r-jsonl-pydantic-validate-coerce), [`R-JSONL-PYDANTIC-TRANSFORMS`](#r-jsonl-pydantic-transforms), [`R-JSONL-PYDANTIC-REPORT`](#r-jsonl-pydantic-report) | `default`: `{'assays': 2, 'assay_metrics': 3}` |

---

## 3. Base Pipeline Specification Cases (`jsonlines_pydantic` / `spec/`)

**Shared Fixture Module (`specs/pipelines/jsonlines_pydantic/sample_registry.py`)**

```python
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
```

**Shared Fixture Module (`specs/pipelines/jsonlines_pydantic/transforms.py`)**

```python
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
```

<a id="case-spec-01-multi-entity-validation-and-rejections"></a>

### Multi-Entity Validation, Model Coercion, Gzip Decompression, and `_rejected` Routing (`spec/01_multi_entity_validation_and_rejections`)

**Suite:** `spec` · **Target CLI:** `jsonlines_pydantic` · **Settings Class:** `JsonlPydanticIngestSettings`  
**Directory:** `specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/`  
**Rules:** [`R-JSONL-PYDANTIC-ENTITY-REGISTRY`](#r-jsonl-pydantic-entity-registry), [`R-JSONL-PYDANTIC-SCHEMA-CONVERT`](#r-jsonl-pydantic-schema-convert), [`R-JSONL-PYDANTIC-VALIDATE-COERCE`](#r-jsonl-pydantic-validate-coerce), [`R-JSONL-PYDANTIC-BLANK-AND-REJECT`](#r-jsonl-pydantic-blank-and-reject), [`R-JSONL-PYDANTIC-MULTI-ENTITY-GLOB`](#r-jsonl-pydantic-multi-entity-glob), [`R-JSONL-PYDANTIC-REPORT`](#r-jsonl-pydantic-report)

- `entity_models = "specs.pipelines.jsonlines_pydantic.sample_registry"` registers `samples` (`SampleModel`), `instruments` (`InstrumentModel`), and `unpopulated` (`InstrumentModel`).
- `samples/samples.jsonl` exercises `model_validate(...).model_dump(mode="python")` (coercing `"ph": "6.5"` to `6.5` and populating default `biome = "terrestrial"`), skips a blank line at line 2, and routes a JSON syntax error (line 3) and a Pydantic `ValidationError` (line 4) to `_rejected`.
- `instruments/instruments.jsonl.gz` is transparently decompressed and loaded into `instruments`, while the absent `unpopulated/` directory produces zero rows without error.

**Settings (`specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/settings.toml`)**

```toml
dataset_name = "jsonlines_pydantic_spec"
entity_models = "specs.pipelines.jsonlines_pydantic.sample_registry"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `instruments/instruments.jsonl.gz` (`specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/input/instruments/instruments.jsonl.gz`) (decompressed preview)**

```json
{"instrument_id": "inst-1", "platform": "Illumina", "active": true}
```

**Input `samples/samples.jsonl` (`specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/input/samples/samples.jsonl`)**

```json
{"sample_id": "s-1", "ph": "6.5", "tags": ["soil", "core"]}

{"sample_id": "s-bad-json", "ph": }
{"sample_id": "s-bad-model", "ph": "not-a-float"}
{"sample_id": "s-2", "ph": 7.8, "biome": "marine", "tags": ["water"]}
```

**Expected `report.txt` Summary & Row Counts (`{'instruments': 1, 'samples': 2, '_rejected': 2}`)**

```yaml
status: completed
ingested_files:
- instruments/instruments.jsonl.gz
- samples/samples.jsonl
load_errors: []
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for jsonlines_pydantic 01_multi_entity_validation_and_rejections."""

import json

import pyarrow as pa
from pydantic import ValidationError

from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from specs.pipelines.jsonlines_pydantic.sample_registry import InstrumentModel, SampleModel

CONVERTER = PydanticToPyArrow()
SAMPLES_SCHEMA = CONVERTER.convert(SampleModel)
INSTRUMENTS_SCHEMA = CONVERTER.convert(InstrumentModel)


def _sample_validation_error() -> str:
    try:
        SampleModel.model_validate({"sample_id": "s-bad-model", "ph": "not-a-float"})
    except ValidationError as exc:
        return json.dumps(exc.errors(), default=str)
    return ""


EXPECTED_TABLES: dict[str, pa.Table] = {
    "samples": pa.Table.from_pylist(
        [
            {"sample_id": "s-1", "ph": 6.5, "biome": "terrestrial", "tags": ["soil", "core"]},
            {"sample_id": "s-2", "ph": 7.8, "biome": "marine", "tags": ["water"]},
        ],
        schema=SAMPLES_SCHEMA,
    ),
    "instruments": pa.Table.from_pylist(
        [
            {"instrument_id": "inst-1", "platform": "Illumina", "active": True},
        ],
        schema=INSTRUMENTS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "samples",
                "source_file": "samples/samples.jsonl",
                "line_no": 3,
                "raw_record": '{"sample_id": "s-bad-json", "ph": }',
                "error_detail": "Expecting value: line 1 column 35 (char 34)",
            },
            {
                "table_name": "samples",
                "source_file": "samples/samples.jsonl",
                "line_no": 4,
                "raw_record": '{"sample_id": "s-bad-model", "ph": "not-a-float"}',
                "error_detail": _sample_validation_error(),
            },
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/expected.json`)**

```json
{
  "_rejected": [
    {
      "error_detail": "Expecting value: line 1 column 35 (char 34)",
      "line_no": 3,
      "raw_record": "{\"sample_id\": \"s-bad-json\", \"ph\": }",
      "source_file": "samples/samples.jsonl",
      "table_name": "samples"
    },
    {
      "error_detail": "[{\"type\": \"float_parsing\", \"loc\": [\"ph\"], \"msg\": \"Input should be a valid number, unable to parse string as a number\", \"input\": \"not-a-float\", \"url\": \"https://errors.pydantic.dev/2.14/v/float_parsing\"}]",
      "line_no": 4,
      "raw_record": "{\"sample_id\": \"s-bad-model\", \"ph\": \"not-a-float\"}",
      "source_file": "samples/samples.jsonl",
      "table_name": "samples"
    }
  ],
  "instruments": [
    {
      "active": true,
      "instrument_id": "inst-1",
      "platform": "Illumina"
    }
  ],
  "samples": [
    {
      "biome": "terrestrial",
      "ph": 6.5,
      "sample_id": "s-1",
      "tags": [
        "soil",
        "core"
      ]
    },
    {
      "biome": "marine",
      "ph": 7.8,
      "sample_id": "s-2",
      "tags": [
        "water"
      ]
    }
  ]
}
```

---

<a id="case-spec-02-md5-sidecar-preprocessing"></a>

### MD5 Sidecar Pre-processing and File-Level `_rejected` Routing (`spec/02_md5_sidecar_preprocessing`)

**Suite:** `spec` · **Target CLI:** `jsonlines_pydantic` · **Settings Class:** `JsonlPydanticIngestSettings`  
**Directory:** `specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/`  
**Rules:** [`R-JSONL-PYDANTIC-PREPROCESS-MD5`](#r-jsonl-pydantic-preprocess-md5), [`R-JSONL-PYDANTIC-REPORT`](#r-jsonl-pydantic-report)

- `preprocessors_module = "specs.pipelines.jsonlines_pydantic.transforms"` registers `validate_md5_sidecar`.
- `samples/good.jsonl` matches `samples/good.jsonl.md5` and loads into `samples`.
- `samples/bad.jsonl` fails MD5 validation against `samples/bad.jsonl.md5`; the file is skipped before line streaming, omitted from `ingested_files`, recorded in `report.txt` `load_errors` (`status: completed with errors`), and emitted as a file-level row (`line_no=None`, `raw_record=None`) in `_rejected`.

**Settings (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/settings.toml`)**

```toml
dataset_name = "jsonlines_pydantic_spec"
entity_models = "specs.pipelines.jsonlines_pydantic.sample_registry"
file_glob = "*.jsonl"
preprocessors_module = "specs.pipelines.jsonlines_pydantic.transforms"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `samples/bad.jsonl` (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/input/samples/bad.jsonl`)**

```json
{"sample_id": "s-99", "ph": 4.2, "biome": "corrupt", "tags": []}
```

**Input `samples/bad.jsonl.md5` (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/input/samples/bad.jsonl.md5`)**

```text
MD5(samples/bad.jsonl)= 00000000000000000000000000000000
```

**Input `samples/good.jsonl` (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/input/samples/good.jsonl`)**

```json
{"sample_id": "s-10", "ph": 7.1, "biome": "aquatic", "tags": ["lake"]}
```

**Input `samples/good.jsonl.md5` (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/input/samples/good.jsonl.md5`)**

```text
MD5(samples/good.jsonl)= ef54940d682eb56e8558c772e6fc76ec
```

**Expected `report.txt` Summary & Row Counts (`{'samples': 1, '_rejected': 1}`)**

```yaml
status: completed with errors
ingested_files:
- samples/good.jsonl
load_errors:
- 'File pre-processing failed for samples/bad.jsonl: MD5 mismatch for samples/bad.jsonl'
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for jsonlines_pydantic 02_md5_sidecar_preprocessing."""

import pyarrow as pa

from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from specs.pipelines.jsonlines_pydantic.sample_registry import SampleModel

SAMPLES_SCHEMA = PydanticToPyArrow().convert(SampleModel)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "samples": pa.Table.from_pylist(
        [
            {"sample_id": "s-10", "ph": 7.1, "biome": "aquatic", "tags": ["lake"]},
        ],
        schema=SAMPLES_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "samples",
                "source_file": "samples/bad.jsonl",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for samples/bad.jsonl: "
                    "expected 00000000000000000000000000000000, got d8981895f8c957b2edf853991bd4f5c4"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/expected.json`)**

```json
{
  "_rejected": [
    {
      "error_detail": "MD5 mismatch for samples/bad.jsonl: expected 00000000000000000000000000000000, got d8981895f8c957b2edf853991bd4f5c4",
      "source_file": "samples/bad.jsonl",
      "table_name": "samples"
    }
  ],
  "samples": [
    {
      "biome": "aquatic",
      "ph": 7.1,
      "sample_id": "s-10",
      "tags": [
        "lake"
      ]
    }
  ]
}
```

---

<a id="case-spec-03-table-transforms-and-output-schemas"></a>

### Table Transforms, Schema Conversion Skip, Multi-Table Fan-Out, and Output Schemas (`spec/03_table_transforms_and_output_schemas`)

**Suite:** `spec` · **Target CLI:** `jsonlines_pydantic` · **Settings Class:** `JsonlPydanticIngestSettings`  
**Directory:** `specs/pipelines/jsonlines_pydantic/spec/03_table_transforms_and_output_schemas/`  
**Rules:** [`R-JSONL-PYDANTIC-SCHEMA-SKIP`](#r-jsonl-pydantic-schema-skip), [`R-JSONL-PYDANTIC-VALIDATE-COERCE`](#r-jsonl-pydantic-validate-coerce), [`R-JSONL-PYDANTIC-TRANSFORMS`](#r-jsonl-pydantic-transforms), [`R-JSONL-PYDANTIC-REPORT`](#r-jsonl-pydantic-report)

- `entity_models`, `transforms_module`, and `output_schemas_module` all point to `specs.pipelines.jsonlines_pydantic.transforms`.
- Because `assays` has a registered `TABLE_TRANSFORMS` entry and an explicit `OUTPUT_SCHEMAS` entry, `build_output_schema_registry` skips automatic `PydanticToPyArrow` conversion for `AssayModel`.
- Validated `AssayModel` dictionaries pass through `transform_assay`, dropping `a-drop` (`exclude: true`), computing `ph_bucket` (`acidic` vs `neutral_or_basic`), and fanning out `metrics` to `assay_metrics`.

**Settings (`specs/pipelines/jsonlines_pydantic/spec/03_table_transforms_and_output_schemas/settings.toml`)**

```toml
dataset_name = "jsonlines_pydantic_spec"
entity_models = "specs.pipelines.jsonlines_pydantic.transforms"
file_glob = "*.jsonl"
transforms_module = "specs.pipelines.jsonlines_pydantic.transforms"
output_schemas_module = "specs.pipelines.jsonlines_pydantic.transforms"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `assays/assays.jsonl` (`specs/pipelines/jsonlines_pydantic/spec/03_table_transforms_and_output_schemas/input/assays/assays.jsonl`)**

```json
{"assay_id": "a-1", "ph": "5.4", "metrics": ["od600", "temp"]}
{"assay_id": "a-drop", "ph": 7.0, "exclude": true, "metrics": ["ignored"]}
{"assay_id": "a-2", "ph": 7.4, "metrics": ["dissolved_o2"]}
```

**Expected `report.txt` Summary & Row Counts (`{'assays': 2, 'assay_metrics': 3}`)**

```yaml
status: completed
ingested_files:
- assays/assays.jsonl
load_errors: []
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/jsonlines_pydantic/spec/03_table_transforms_and_output_schemas/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for jsonlines_pydantic 03_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.jsonlines_pydantic.transforms import (
    ASSAY_METRIC_SCHEMA,
    ASSAY_SUMMARY_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "assays": pa.Table.from_pylist(
        [
            {"assay_id": "A-1", "ph": 5.4, "ph_bucket": "acidic"},
            {"assay_id": "A-2", "ph": 7.4, "ph_bucket": "neutral_or_basic"},
        ],
        schema=ASSAY_SUMMARY_SCHEMA,
    ),
    "assay_metrics": pa.Table.from_pylist(
        [
            {"assay_id": "A-1", "metric": "od600"},
            {"assay_id": "A-1", "metric": "temp"},
            {"assay_id": "A-2", "metric": "dissolved_o2"},
        ],
        schema=ASSAY_METRIC_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/jsonlines_pydantic/spec/03_table_transforms_and_output_schemas/expected.json`)**

```json
{
  "assay_metrics": [
    {
      "assay_id": "A-1",
      "metric": "od600"
    },
    {
      "assay_id": "A-1",
      "metric": "temp"
    },
    {
      "assay_id": "A-2",
      "metric": "dissolved_o2"
    }
  ],
  "assays": [
    {
      "assay_id": "A-1",
      "ph": 5.4,
      "ph_bucket": "acidic"
    },
    {
      "assay_id": "A-2",
      "ph": 7.4,
      "ph_bucket": "neutral_or_basic"
    }
  ]
}
```

---

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suites and this document are both driven by it.

| Case | Target CLI | Manifest | Runs |
| :--- | :--- | :--- | :--- |
| [`spec/01_multi_entity_validation_and_rejections`](#case-spec-01-multi-entity-validation-and-rejections) | `jsonlines_pydantic` | [`case.yaml`](../specs/pipelines/jsonlines_pydantic/spec/01_multi_entity_validation_and_rejections/case.yaml) | default |
| [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing) | `jsonlines_pydantic` | [`case.yaml`](../specs/pipelines/jsonlines_pydantic/spec/02_md5_sidecar_preprocessing/case.yaml) | partial_md5_failure |
| [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas) | `jsonlines_pydantic` | [`case.yaml`](../specs/pipelines/jsonlines_pydantic/spec/03_table_transforms_and_output_schemas/case.yaml) | default |
