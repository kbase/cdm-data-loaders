<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/schema_converters/pyarrow_to_jsonschema/rules.yaml`, `specs/schema_converters/pyarrow_to_jsonschema/spec`, `specs/schema_converters/pyarrow_to_jsonschema/examples`. Regenerate with `uv run python -m scripts.gen_converter_spec`. -->

# PyArrow to JSON Schema Specification

This document specifies how a **PyArrow `pa.Schema`** determines:

1. The **Draft 2020-12 JSON Schema** document produced by `generate_json_schema` (and `PyArrowToJSONSchema`),
   including physical integer bounds, fixed-point decimal patterns, Base64 binary encodings, key/value map
   arrays, and lossless `x-pyarrow` extension metadata.
2. The **serialized JSON row dictionaries** produced by `serialize_table_for_jsonschema` and
   `serialize_record_for_jsonschema` from Arrow `pa.Table`s or Python row mappings, ready for Draft 2020-12
   JSON Schema validation.
3. The **errors** raised for Arrow schemas or data types that have no faithful Draft 2020-12 JSON Schema
   representation.

**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · [3. Errors](#3-errors-unsupported-invalid-input) · [4. Fixture Index](#4-fixture-index)

## 1. Conversion Rules

### 1.1 Top-Level Schema Resolution and Envelope

<a id="r-schema-input"></a>

- **R-SCHEMA-INPUT**: The input to `generate_json_schema` must be a concrete `pyarrow.Schema` instance; passing a bare
  `pa.DataType`, `pa.Field`, `pa.Table`, or Python mapping raises **E1**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-non-empty-root"></a>

- **R-NON-EMPTY-ROOT**: The root `pa.Schema` must contain at least one field; an empty `pa.schema([])` raises **E2**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans)

<a id="r-dialect"></a>

- **R-DIALECT**: Standalone documents always emit `"$schema": "https://json-schema.org/draft/2020-12/schema"`. When
  `destination` is supplied to `generate_json_schema`, the converted schema (requiring a non-empty
  `schema_name` or `metadata[b"title"]`) is inserted under `destination["$defs"][<name>]` with the inner
  `"$schema"` keyword stripped.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-envelope-id-title"></a>

- **R-ENVELOPE-ID-TITLE**: Top-level `"title"` resolves from explicit `schema_name` when provided, otherwise from non-empty UTF-8
  `schema.metadata[b"title"]`, and is omitted if neither is set. Top-level `"$id"` resolves from
  explicit `schema_id` when provided, otherwise from non-empty UTF-8 `schema.metadata[b"$id"]`, and is
  omitted if neither is set.

  *Cases:* [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-envelope-closed"></a>

- **R-ENVELOPE-CLOSED**: Every root schema and nested `pa.struct` emits `"type": "object"`, `"properties": {...}`, and
  `"additionalProperties": false`. Top-level keywords appear in canonical order: `$schema`, `$id`,
  `title`, `description`, `type`, `properties`, `additionalProperties`, `required`, `x-pyarrow`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [checkm2_quality_report](#case-examples-checkm2-quality-report)

### 1.2 Field Naming, Ordering, and Uniqueness

<a id="r-field-name"></a>

- **R-FIELD-NAME**: Every `pa.Field.name` in a root `pa.Schema` or nested `pa.StructType` is preserved verbatim as its
  property key in `"properties"` and (when non-nullable) in `"required"`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-field-order"></a>

- **R-FIELD-ORDER**: `"properties"` keys and `"required"` entries preserve the exact declaration order of the source
  `pa.Schema` or `pa.StructType`. When every field of an object has `nullable=True`, `"required"` is
  omitted rather than emitted as `[]`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-unique-field-names"></a>

- **R-UNIQUE-FIELD-NAMES**: Sibling fields within a `pa.Schema` or `pa.StructType` must have distinct names; duplicate sibling
  field names raise **E3**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans)

### 1.3 Structural Container Shapes

<a id="r-shape-struct"></a>

- **R-SHAPE-STRUCT**: A nested `pa.struct([...])` with at least one child field maps to a closed JSON Schema `"object"`
  (`"additionalProperties": false`) carrying `"x-pyarrow": {"arrow_type": "struct"}`. An empty nested
  `pa.struct([])` raises **E2**.

  *Cases:* [06_structs_and_lists](#case-spec-06-structs-and-lists), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-list"></a>

- **R-SHAPE-LIST**: Variable-length Arrow list types (`pa.list_`, `pa.large_list`, `pa.list_view`, `pa.large_list_view`)
  map to `{"type": "array", "items": <item schema>}` with `"x-pyarrow": {"arrow_type": "<variant>"}`
  (`"list"`, `"large_list"`, `"list_view"`, or `"large_list_view"`). Multi-dimensional lists
  (`list<list<T>>`) nest `"array"` schemas recursively.

  *Cases:* [06_structs_and_lists](#case-spec-06-structs-and-lists), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-fixed-list"></a>

- **R-SHAPE-FIXED-LIST**: Fixed-length `pa.list_(item, list_size)` (`pa.FixedSizeListType`) maps to
  `{"type": "array", "items": <item schema>, "minItems": list_size, "maxItems": list_size}` with
  `"x-pyarrow": {"arrow_type": "fixed_size_list", "list_size": list_size}`.

  *Cases:* [06_structs_and_lists](#case-spec-06-structs-and-lists), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-map"></a>

- **R-SHAPE-MAP**: `pa.map_(key_type, item_type, keys_sorted=...)` maps to an `"array"` of closed key/value entry objects
  (`"properties": {"key": <key schema>, "value": <value schema>}`, `"required": ["key", "value"]`,
  `"additionalProperties": false`), carrying
  `"description": "PyArrow map rendered as key/value pairs (JSON object keys must be strings)."` and
  `"x-pyarrow": {"arrow_type": "map", "keys_sorted": <bool>}`, supporting both string and non-string
  Arrow key types.

  *Cases:* [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-dictionary"></a>

- **R-SHAPE-DICTIONARY**: `pa.dictionary(index_type, value_type, ordered=...)` emits the JSON Schema core type and constraints
  of `value_type` while recording `"arrow_type": "dictionary"`, `"value_type"`, `"index_type"`, and
  `"ordered"` inside `"x-pyarrow"`.

  *Cases:* [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

### 1.4 Field and Element Nullability

<a id="r-null-required"></a>

- **R-NULL-REQUIRED**: A field with `pa.Field.nullable == False` is included in its enclosing object's `"required"` array and
  its property subschema is emitted directly without a null union wrapper.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [06_structs_and_lists](#case-spec-06-structs-and-lists), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-null-optional"></a>

- **R-NULL-OPTIONAL**: A field with `pa.Field.nullable == True` is omitted from `"required"` and its value subschema
  (including its type-level `"x-pyarrow"` block) is wrapped in
  `{"anyOf": [<non-null schema>, {"type": "null"}]}`. Standalone `pa.null()` columns or element types
  have no value domain and raise **E4**.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [06_structs_and_lists](#case-spec-06-structs-and-lists), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-null-list-items"></a>

- **R-NULL-LIST-ITEMS**: A list type's inner `value_field.nullable` controls element nullability independently of the outer
  list field: `value_field.nullable == False` emits `<item schema>` directly under `"items"`, whereas
  `value_field.nullable == True` wraps `"items"` in `{"anyOf": [<item schema>, {"type": "null"}]}`.

  *Cases:* [06_structs_and_lists](#case-spec-06-structs-and-lists)

<a id="r-null-map-values"></a>

- **R-NULL-MAP-VALUES**: Arrow map keys are always non-nullable (`"key"` has no null union). When `MapType.item_field.nullable`
  is `True`, the entry object's `"value"` property is wrapped in
  `{"anyOf": [<value schema>, {"type": "null"}]}` while both `"key"` and `"value"` remain in the entry
  object's `"required"` array.

  *Cases:* [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries)

### 1.5 Scalar Type Mappings

<a id="r-scalar-bool"></a>

- **R-SCALAR-BOOL**: `pa.bool_()` maps to `{"type": "boolean"}` with `"x-pyarrow": {"arrow_type": "bool"}`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-signed-int"></a>

- **R-SCALAR-SIGNED-INT**: Signed integer types (`int8`, `int16`, `int32`, `int64`) map to `"type": "integer"` with exact
  two's-complement bounds (`minimum: -2^(w-1)`, `maximum: 2^(w-1) - 1` for bit width `w`) and
  `"x-pyarrow": {"arrow_type": "int8"|"int16"|"int32"|"int64"}`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-unsigned-int"></a>

- **R-SCALAR-UNSIGNED-INT**: Unsigned integer types (`uint8`, `uint16`, `uint32`, `uint64`) map to `"type": "integer"` with
  `minimum: 0`, `maximum: 2^w - 1` for bit width `w`, and
  `"x-pyarrow": {"arrow_type": "uint8"|"uint16"|"uint32"|"uint64"}`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-float"></a>

- **R-SCALAR-FLOAT**: IEEE-754 floating-point types (`float16`, `float32`, `float64`) map to `{"type": "number"}` with
  `"x-pyarrow": {"arrow_type": "float16"|"float32"|"float64"}`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-decimal"></a>

- **R-SCALAR-DECIMAL**: `pa.decimal128(p, s)` and `pa.decimal256(p, s)` with `0 <= s <= p` map to
  `{"type": "string", "pattern": "<fixed-point regex>"}` enforcing at most `p - s` integer digits and
  `s` fractional digits, plus
  `"x-pyarrow": {"arrow_type": "decimal128"|"decimal256", "logical_type": "decimal", "precision": p, "scale": s}`.
  Decimals with `s < 0` or `s > p` raise **E5**.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-string"></a>

- **R-SCALAR-STRING**: UTF-8 string types (`string`, `large_string`, `string_view`) map to `{"type": "string"}` with
  `"x-pyarrow": {"arrow_type": "string"|"large_string"|"string_view"}`.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-binary"></a>

- **R-SCALAR-BINARY**: Variable-length binary types (`binary`, `large_binary`, `binary_view`) map to
  `{"type": "string", "contentEncoding": "base64"}` with
  `"x-pyarrow": {"arrow_type": "binary"|"large_binary"|"binary_view"}`. Fixed-width `pa.binary(n)`
  (`FixedSizeBinaryType`) maps to `{"type": "string", "contentEncoding": "base64"}` with
  `"x-pyarrow": {"arrow_type": "fixed_size_binary", "byte_width": n}`.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-date-time"></a>

- **R-SCALAR-DATE-TIME**: `date32` and `date64` map to `{"type": "string", "format": "date"}` (`arrow_type`: `"date32"` or
  `"date64"`). `time32("s"|"ms")` and `time64("us"|"ns")` map to `{"type": "string", "format": "time"}`
  with `"x-pyarrow": {"arrow_type": "time32"|"time64", "unit": "<unit>"}`. Relative durations and
  calendar intervals (`pa.duration`, `pa.month_day_nano_interval`) raise **E6**.

  *Cases:* [03_temporal_types](#case-spec-03-temporal-types), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-timestamp"></a>

- **R-SCALAR-TIMESTAMP**: Timezone-aware `pa.timestamp(unit, tz=...)` (non-empty `tz`) maps to
  `{"type": "string", "format": "date-time"}` with
  `"x-pyarrow": {"arrow_type": "timestamp", "logical_type": "timestamp-with-tz", "unit": "<unit>", "tz": "<tz>"}`.
  Timezone-naive `pa.timestamp(unit)` (`tz=None`) omits `"format"`, sets
  `"description": "ISO 8601 timestamp with no timezone attached."` on the scalar node, and records
  `"x-pyarrow": {"arrow_type": "timestamp", "logical_type": "timestamp-without-tz", "unit": "<unit>"}`.
  Arrow unions (`sparse_union`, `dense_union`) raise **E7**; `run_end_encoded` and `BaseExtensionType`
  types (`pa.uuid()`, custom `ExtensionType`) raise **E8**.

  *Cases:* [03_temporal_types](#case-spec-03-temporal-types), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

| PyArrow `DataType` | Serialized JSON value | JSON Schema (core) | `x-pyarrow` |
| :--- | :--- | :--- | :--- |
| `pa.bool_()` | `bool` | `{"type": "boolean"}` | `{"arrow_type": "bool"}` |
| `pa.int8()` | `int` | `{"type": "integer", "minimum": -128, "maximum": 127}` | `{"arrow_type": "int8"}` |
| `pa.int16()` | `int` | `{"type": "integer", "minimum": -32768, "maximum": 32767}` | `{"arrow_type": "int16"}` |
| `pa.int32()` | `int` | `{"type": "integer", "minimum": -2147483648, "maximum": 2147483647}` | `{"arrow_type": "int32"}` |
| `pa.int64()` | `int` | `{"type": "integer", "minimum": -9223372036854775808, "maximum": 9223372036854775807}` | `{"arrow_type": "int64"}` |
| `pa.uint8()` | `int` | `{"type": "integer", "minimum": 0, "maximum": 255}` | `{"arrow_type": "uint8"}` |
| `pa.uint16()` | `int` | `{"type": "integer", "minimum": 0, "maximum": 65535}` | `{"arrow_type": "uint16"}` |
| `pa.uint32()` | `int` | `{"type": "integer", "minimum": 0, "maximum": 4294967295}` | `{"arrow_type": "uint32"}` |
| `pa.uint64()` | `int` | `{"type": "integer", "minimum": 0, "maximum": 18446744073709551615}` | `{"arrow_type": "uint64"}` |
| `pa.float16()` | `float` | `{"type": "number"}` | `{"arrow_type": "float16"}` |
| `pa.float32()` | `float` | `{"type": "number"}` | `{"arrow_type": "float32"}` |
| `pa.float64()` | `float` | `{"type": "number"}` | `{"arrow_type": "float64"}` |
| `pa.decimal128(10, 2)` | `str` (fixed-point decimal) | `{"type": "string", "pattern": "^-?\\d{1,8}(\\.\\d{1,2})?$"}` | `{"arrow_type": "decimal128", "logical_type": "decimal", "precision": 10, "scale": 2}` |
| `pa.decimal128(2, 2)` | `str` (fixed-point decimal) | `{"type": "string", "pattern": "^-?0(\\.\\d{1,2})?$"}` | `{"arrow_type": "decimal128", "logical_type": "decimal", "precision": 2, "scale": 2}` |
| `pa.decimal128(10, 0)` | `str` (fixed-point integer decimal) | `{"type": "string", "pattern": "^-?\\d{1,10}$"}` | `{"arrow_type": "decimal128", "logical_type": "decimal", "precision": 10, "scale": 0}` |
| `pa.decimal256(50, 10)` | `str` (fixed-point decimal) | `{"type": "string", "pattern": "^-?\\d{1,40}(\\.\\d{1,10})?$"}` | `{"arrow_type": "decimal256", "logical_type": "decimal", "precision": 50, "scale": 10}` |
| `pa.string()` | `str` | `{"type": "string"}` | `{"arrow_type": "string"}` |
| `pa.large_string()` | `str` | `{"type": "string"}` | `{"arrow_type": "large_string"}` |
| `pa.string_view()` | `str` | `{"type": "string"}` | `{"arrow_type": "string_view"}` |
| `pa.binary()` | `str` (Base64) | `{"type": "string", "contentEncoding": "base64"}` | `{"arrow_type": "binary"}` |
| `pa.large_binary()` | `str` (Base64) | `{"type": "string", "contentEncoding": "base64"}` | `{"arrow_type": "large_binary"}` |
| `pa.binary_view()` | `str` (Base64) | `{"type": "string", "contentEncoding": "base64"}` | `{"arrow_type": "binary_view"}` |
| `pa.binary(16)` | `str` (Base64) | `{"type": "string", "contentEncoding": "base64"}` | `{"arrow_type": "fixed_size_binary", "byte_width": 16}` |
| `pa.date32()` | `str` (ISO-8601 date) | `{"type": "string", "format": "date"}` | `{"arrow_type": "date32"}` |
| `pa.date64()` | `str` (ISO-8601 date) | `{"type": "string", "format": "date"}` | `{"arrow_type": "date64"}` |
| `pa.time32("s")` | `str` (ISO-8601 time) | `{"type": "string", "format": "time"}` | `{"arrow_type": "time32", "unit": "s"}` |
| `pa.time32("ms")` | `str` (ISO-8601 time) | `{"type": "string", "format": "time"}` | `{"arrow_type": "time32", "unit": "ms"}` |
| `pa.time64("us")` | `str` (ISO-8601 time) | `{"type": "string", "format": "time"}` | `{"arrow_type": "time64", "unit": "us"}` |
| `pa.time64("ns")` | `str` (ISO-8601 time) | `{"type": "string", "format": "time"}` | `{"arrow_type": "time64", "unit": "ns"}` |
| `pa.timestamp("s", tz="UTC")` | `str` (ISO-8601 date-time) | `{"type": "string", "format": "date-time"}` | `{"arrow_type": "timestamp", "logical_type": "timestamp-with-tz", "unit": "s", "tz": "UTC"}` |
| `pa.timestamp("us", tz="UTC")` | `str` (ISO-8601 date-time) | `{"type": "string", "format": "date-time"}` | `{"arrow_type": "timestamp", "logical_type": "timestamp-with-tz", "unit": "us", "tz": "UTC"}` |
| `pa.timestamp("ns", tz="America/Los_Angeles")` | `str` (ISO-8601 date-time) | `{"type": "string", "format": "date-time"}` | `{"arrow_type": "timestamp", "logical_type": "timestamp-with-tz", "unit": "ns", "tz": "America/Los_Angeles"}` |
| `pa.timestamp("us")` | `str` (naive ISO-8601 timestamp) | `{"type": "string", "description": "ISO 8601 timestamp with no timezone attached."}` | `{"arrow_type": "timestamp", "logical_type": "timestamp-without-tz", "unit": "us"}` |
| `pa.timestamp("ns")` | `str` (naive ISO-8601 timestamp) | `{"type": "string", "description": "ISO 8601 timestamp with no timezone attached."}` | `{"arrow_type": "timestamp", "logical_type": "timestamp-without-tz", "unit": "ns"}` |
| `pa.dictionary(pa.int16(), pa.string())` | `str` | `{"type": "string"}` | `{"arrow_type": "dictionary", "value_type": "string", "index_type": "int16", "ordered": false}` |
| `pa.dictionary(pa.int8(), pa.int64(), ordered=True)` | `int` | `{"type": "integer", "minimum": -9223372036854775808, "maximum": 9223372036854775807}` | `{"arrow_type": "dictionary", "value_type": "int64", "index_type": "int8", "ordered": true}` |

### 1.6 Descriptions and `x-pyarrow` Extensions

<a id="r-meta-description"></a>

- **R-META-DESCRIPTION**: Non-empty UTF-8 `metadata[b"comment"]` on a `pa.Schema`, `pa.Field`, list `value_field`, or map
  `key_field`/`item_field` is emitted as JSON Schema `"description"` and excluded from
  `x-pyarrow.metadata`. On nullable fields (`nullable=True`), field-level `"description"` and field-level
  `"x-pyarrow"` metadata are placed on the outer property schema outside `"anyOf"`.

  *Cases:* [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [06_structs_and_lists](#case-spec-06-structs-and-lists), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-meta-x-pyarrow-type"></a>

- **R-META-X-PYARROW-TYPE**: When `include_pyarrow_metadata=True` (the default), every converted scalar, dictionary, struct, list,
  and map schema node carries a validated `"x-pyarrow"` object recording `arrow_type` and applicable
  parametric fields (`logical_type`, `precision`, `scale`, `unit`, `tz`, `byte_width`, `list_size`,
  `keys_sorted`, `index_type`, `value_type`, `ordered`).

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [06_structs_and_lists](#case-spec-06-structs-and-lists), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-meta-x-pyarrow-custom"></a>

- **R-META-X-PYARROW-CUSTOM**: Custom UTF-8 key/value entries in `pa.Schema.metadata` (excluding `comment`, `title`, `$id`),
  `pa.Field.metadata` (excluding `comment`), and list/map child field metadata are preserved under
  `"x-pyarrow": {"metadata": {...}}` when `include_pyarrow_metadata=True`.

  *Cases:* [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [06_structs_and_lists](#case-spec-06-structs-and-lists), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-meta-omit-flag"></a>

- **R-META-OMIT-FLAG**: Setting `include_pyarrow_metadata=False` omits all `"x-pyarrow"` blocks at the document, field, and
  nested node levels, yielding a pure Draft 2020-12 JSON Schema while retaining `"$id"`, `"title"`, and
  `"description"`.

  *Cases:* [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides)

### 1.7 Arrow Table and Record Serialization

<a id="r-ser-row-order"></a>

- **R-SER-ROW-ORDER**: `serialize_table_for_jsonschema` and `serialize_record_for_jsonschema` emit one JSON object per row
  with keys in `pa.Schema` declaration order; null Arrow scalars serialize as `null` (`None`), structs
  serialize as nested dicts, lists serialize as JSON arrays, and dictionary scalars unwrap to their
  decoded value.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_metadata](#case-spec-04-nullability-and-metadata), [05_metadata_omission_and_overrides](#case-spec-05-metadata-omission-and-overrides), [06_structs_and_lists](#case-spec-06-structs-and-lists), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-decimal-str"></a>

- **R-SER-DECIMAL-STR**: `decimal128` and `decimal256` scalars serialize via plain fixed-point formatting (`format(value, "f")`,
  never scientific notation) matching the emitted `pattern` regex.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-ser-base64"></a>

- **R-SER-BASE64**: `binary`, `large_binary`, `binary_view`, and `fixed_size_binary(n)` scalars serialize to ASCII Base64
  strings matching `"contentEncoding": "base64"`.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-temporal-iso"></a>

- **R-SER-TEMPORAL-ISO**: `date32` and `date64` serialize as `YYYY-MM-DD`; `time32` and `time64` serialize as
  `HH:MM:SS[.fffffffff]` (preserving sub-microsecond nanosecond digits when non-zero); timezone-aware
  `timestamp` serializes with its explicit ISO-8601 UTC/IANA offset; timezone-naive `timestamp`
  serializes without a timezone suffix.

  *Cases:* [03_temporal_types](#case-spec-03-temporal-types), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-map-entries"></a>

- **R-SER-MAP-ENTRIES**: `pa.map_` scalars serialize as `[{"key": <serialized key>, "value": <serialized value>}, ...]`
  preserving entry order and typed non-string keys.

  *Cases:* [07_maps_and_dictionaries](#case-spec-07-maps-and-dictionaries), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-finite-float"></a>

- **R-SER-FINITE-FLOAT**: Finite `float16`, `float32`, and `float64` scalars serialize as JSON `float` numbers; non-finite
  `NaN` or `±Infinity` values raise `ValueError`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans)

## 2. Cases

<a id="case-spec-01-integers-floats-and-booleans"></a>

### Booleans, Signed and Unsigned Integers, and IEEE-754 Floats (`spec/01_integers_floats_and_booleans`)

**Rules:** [R-SCHEMA-INPUT](#r-schema-input), [R-NON-EMPTY-ROOT](#r-non-empty-root), [R-DIALECT](#r-dialect), [R-ENVELOPE-CLOSED](#r-envelope-closed), [R-FIELD-NAME](#r-field-name), [R-FIELD-ORDER](#r-field-order), [R-UNIQUE-FIELD-NAMES](#r-unique-field-names), [R-NULL-REQUIRED](#r-null-required), [R-SCALAR-BOOL](#r-scalar-bool), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-FLOAT](#r-scalar-float), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-FINITE-FLOAT](#r-ser-finite-float)

Exercises exact two's-complement `minimum`/`maximum` bounds for `int8`..`int64`, unsigned `0..2^w - 1` bounds
for `uint8`..`uint64` (including full 64-bit `18446744073709551615`), `bool_`, and `float16`/`float32`/`float64`
at boundary values.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 01_integers_floats_and_booleans."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("flag", pa.bool_(), nullable=False),
        pa.field("i8", pa.int8(), nullable=False),
        pa.field("i16", pa.int16(), nullable=False),
        pa.field("i32", pa.int32(), nullable=False),
        pa.field("i64", pa.int64(), nullable=False),
        pa.field("u8", pa.uint8(), nullable=False),
        pa.field("u16", pa.uint16(), nullable=False),
        pa.field("u32", pa.uint32(), nullable=False),
        pa.field("u64", pa.uint64(), nullable=False),
        pa.field("f16", pa.float16(), nullable=False),
        pa.field("f32", pa.float32(), nullable=False),
        pa.field("f64", pa.float64(), nullable=False),
    ]
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 01_integers_floats_and_booleans."""

import numpy as np
import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "flag": True,
            "i8": -128,
            "i16": -32768,
            "i32": -2147483648,
            "i64": -9223372036854775808,
            "u8": 0,
            "u16": 0,
            "u32": 0,
            "u64": 0,
            "f16": np.float16(-1.5),
            "f32": -0.25,
            "f64": -123456.7890125,
        },
        {
            "flag": False,
            "i8": 127,
            "i16": 32767,
            "i32": 2147483647,
            "i64": 9223372036854775807,
            "u8": 255,
            "u16": 65535,
            "u32": 4294967295,
            "u64": 18446744073709551615,
            "f16": np.float16(3.5),
            "f32": 2.5,
            "f64": 0.0,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `boundary_values`

```json
[
  {
    "flag": true,
    "i8": -128,
    "i16": -32768,
    "i32": -2147483648,
    "i64": -9223372036854775808,
    "u8": 0,
    "u16": 0,
    "u32": 0,
    "u64": 0,
    "f16": -1.5,
    "f32": -0.25,
    "f64": -123456.7890125
  },
  {
    "flag": false,
    "i8": 127,
    "i16": 32767,
    "i32": 2147483647,
    "i64": 9223372036854775807,
    "u8": 255,
    "u16": 65535,
    "u32": 4294967295,
    "u64": 18446744073709551615,
    "f16": 3.5,
    "f32": 2.5,
    "f64": 0.0
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/01_integers_floats_and_booleans/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "flag": {
      "type": "boolean",
      "x-pyarrow": {
        "arrow_type": "bool"
      }
    },
    "i8": {
      "type": "integer",
      "minimum": -128,
      "maximum": 127,
      "x-pyarrow": {
        "arrow_type": "int8"
      }
    },
    "i16": {
      "type": "integer",
      "minimum": -32768,
      "maximum": 32767,
      "x-pyarrow": {
        "arrow_type": "int16"
      }
    },
    "i32": {
      "type": "integer",
      "minimum": -2147483648,
      "maximum": 2147483647,
      "x-pyarrow": {
        "arrow_type": "int32"
      }
    },
    "i64": {
      "type": "integer",
      "minimum": -9223372036854775808,
      "maximum": 9223372036854775807,
      "x-pyarrow": {
        "arrow_type": "int64"
      }
    },
    "u8": {
      "type": "integer",
      "minimum": 0,
      "maximum": 255,
      "x-pyarrow": {
        "arrow_type": "uint8"
      }
    },
    "u16": {
      "type": "integer",
      "minimum": 0,
      "maximum": 65535,
      "x-pyarrow": {
        "arrow_type": "uint16"
      }
    },
    "u32": {
      "type": "integer",
      "minimum": 0,
      "maximum": 4294967295,
      "x-pyarrow": {
        "arrow_type": "uint32"
      }
    },
    "u64": {
      "type": "integer",
      "minimum": 0,
      "maximum": 18446744073709551615,
      "x-pyarrow": {
        "arrow_type": "uint64"
      }
    },
    "f16": {
      "type": "number",
      "x-pyarrow": {
        "arrow_type": "float16"
      }
    },
    "f32": {
      "type": "number",
      "x-pyarrow": {
        "arrow_type": "float32"
      }
    },
    "f64": {
      "type": "number",
      "x-pyarrow": {
        "arrow_type": "float64"
      }
    }
  },
  "additionalProperties": false,
  "required": [
    "flag",
    "i8",
    "i16",
    "i32",
    "i64",
    "u8",
    "u16",
    "u32",
    "u64",
    "f16",
    "f32",
    "f64"
  ]
}
```

---

<a id="case-spec-02-strings-binary-and-decimals"></a>

### Strings, Binary Encodings, and Fixed-Point Decimals (`spec/02_strings_binary_and_decimals`)

**Rules:** [R-SCALAR-STRING](#r-scalar-string), [R-SCALAR-BINARY](#r-scalar-binary), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-BASE64](#r-ser-base64)

Covers UTF-8 string variants (`string`, `large_string`, `string_view`), Base64-encoded binary variants
(`binary`, `large_binary`, `binary_view`, `fixed_size_binary`), and regex-constrained fixed-point decimals
(`decimal128` with `scale < precision`, `scale == precision`, `scale == 0`, and `decimal256` including tiny
values formatted without scientific notation).

#### Schema (`schema.py`)

```python
"""PyArrow schema for 02_strings_binary_and_decimals."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("utf8_str", pa.string(), nullable=False),
        pa.field("large_str", pa.large_string(), nullable=False),
        pa.field("view_str", pa.string_view(), nullable=False),
        pa.field("raw_bin", pa.binary(), nullable=False),
        pa.field("large_bin", pa.large_binary(), nullable=False),
        pa.field("view_bin", pa.binary_view(), nullable=False),
        pa.field("fixed_bin_4", pa.binary(4), nullable=False),
        pa.field("dec_standard", pa.decimal128(10, 2), nullable=False),
        pa.field("dec_fraction_only", pa.decimal128(4, 4), nullable=False),
        pa.field("dec_integer_only", pa.decimal128(8, 0), nullable=False),
        pa.field("dec256_high_prec", pa.decimal256(50, 10), nullable=True),
    ]
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 02_strings_binary_and_decimals."""

from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "utf8_str": "contig_0001",
            "large_str": "ATGCNATGCNATGCN",
            "view_str": "short-view",
            "raw_bin": b"\x00\x01\x02\xff",
            "large_bin": b"kbase-large-payload",
            "view_bin": b"binary-view-bytes",
            "fixed_bin_4": b"\xde\xad\xbe\xef",
            "dec_standard": Decimal("-12345678.95"),
            "dec_fraction_only": Decimal("0.0625"),
            "dec_integer_only": Decimal(98765432),
            "dec256_high_prec": Decimal("0.0000000001"),
        },
        {
            "utf8_str": "",
            "large_str": "second-large-string",
            "view_str": "inlined-or-out-of-line-string-view-value",
            "raw_bin": b"",
            "large_bin": b"\x89PNG",
            "view_bin": b"1234567890abcdef",
            "fixed_bin_4": b"\x00\x00\x00\x01",
            "dec_standard": Decimal("0.00"),
            "dec_fraction_only": Decimal("-0.9999"),
            "dec_integer_only": Decimal(-42),
            "dec256_high_prec": None,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "utf8_str": "contig_0001",
    "large_str": "ATGCNATGCNATGCN",
    "view_str": "short-view",
    "raw_bin": "AAEC/w==",
    "large_bin": "a2Jhc2UtbGFyZ2UtcGF5bG9hZA==",
    "view_bin": "YmluYXJ5LXZpZXctYnl0ZXM=",
    "fixed_bin_4": "3q2+7w==",
    "dec_standard": "-12345678.95",
    "dec_fraction_only": "0.0625",
    "dec_integer_only": "98765432",
    "dec256_high_prec": "0.0000000001"
  },
  {
    "utf8_str": "",
    "large_str": "second-large-string",
    "view_str": "inlined-or-out-of-line-string-view-value",
    "raw_bin": "",
    "large_bin": "iVBORw==",
    "view_bin": "MTIzNDU2Nzg5MGFiY2RlZg==",
    "fixed_bin_4": "AAAAAQ==",
    "dec_standard": "0.00",
    "dec_fraction_only": "-0.9999",
    "dec_integer_only": "-42",
    "dec256_high_prec": null
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/02_strings_binary_and_decimals/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "utf8_str": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "string"
      }
    },
    "large_str": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "large_string"
      }
    },
    "view_str": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "string_view"
      }
    },
    "raw_bin": {
      "type": "string",
      "contentEncoding": "base64",
      "x-pyarrow": {
        "arrow_type": "binary"
      }
    },
    "large_bin": {
      "type": "string",
      "contentEncoding": "base64",
      "x-pyarrow": {
        "arrow_type": "large_binary"
      }
    },
    "view_bin": {
      "type": "string",
      "contentEncoding": "base64",
      "x-pyarrow": {
        "arrow_type": "binary_view"
      }
    },
    "fixed_bin_4": {
      "type": "string",
      "contentEncoding": "base64",
      "x-pyarrow": {
        "arrow_type": "fixed_size_binary",
        "byte_width": 4
      }
    },
    "dec_standard": {
      "type": "string",
      "pattern": "^-?\\d{1,8}(\\.\\d{1,2})?$",
      "x-pyarrow": {
        "arrow_type": "decimal128",
        "logical_type": "decimal",
        "precision": 10,
        "scale": 2
      }
    },
    "dec_fraction_only": {
      "type": "string",
      "pattern": "^-?0(\\.\\d{1,4})?$",
      "x-pyarrow": {
        "arrow_type": "decimal128",
        "logical_type": "decimal",
        "precision": 4,
        "scale": 4
      }
    },
    "dec_integer_only": {
      "type": "string",
      "pattern": "^-?\\d{1,8}$",
      "x-pyarrow": {
        "arrow_type": "decimal128",
        "logical_type": "decimal",
        "precision": 8,
        "scale": 0
      }
    },
    "dec256_high_prec": {
      "anyOf": [
        {
          "type": "string",
          "pattern": "^-?\\d{1,40}(\\.\\d{1,10})?$",
          "x-pyarrow": {
            "arrow_type": "decimal256",
            "logical_type": "decimal",
            "precision": 50,
            "scale": 10
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "additionalProperties": false,
  "required": [
    "utf8_str",
    "large_str",
    "view_str",
    "raw_bin",
    "large_bin",
    "view_bin",
    "fixed_bin_4",
    "dec_standard",
    "dec_fraction_only",
    "dec_integer_only"
  ]
}
```

---

<a id="case-spec-03-temporal-types"></a>

### Dates, Times, and Timezone-Aware vs Timezone-Naive Timestamps (`spec/03_temporal_types`)

**Rules:** [R-SCALAR-DATE-TIME](#r-scalar-date-time), [R-SCALAR-TIMESTAMP](#r-scalar-timestamp), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-TEMPORAL-ISO](#r-ser-temporal-iso)

Covers `date32`, `date64`, `time32("s"|"ms")`, `time64("us"|"ns")` (including sub-microsecond nanoseconds),
timezone-aware `timestamp` across units (`s`, `ms`, `us`, `ns`) and IANA/UTC zones (`"format": "date-time"`),
and timezone-naive `timestamp` (`logical_type: "timestamp-without-tz"` without `"format": "date-time"`).

#### Schema (`schema.py`)

```python
"""PyArrow schema for 03_temporal_types."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("d32", pa.date32(), nullable=False),
        pa.field("d64", pa.date64(), nullable=False),
        pa.field("t32_s", pa.time32("s"), nullable=False),
        pa.field("t32_ms", pa.time32("ms"), nullable=False),
        pa.field("t64_us", pa.time64("us"), nullable=False),
        pa.field("t64_ns", pa.time64("ns"), nullable=False),
        pa.field("ts_s_utc", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("ts_ms_utc", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("ts_us_la", pa.timestamp("us", tz="America/Los_Angeles"), nullable=False),
        pa.field("ts_ns_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("ts_naive_us", pa.timestamp("us"), nullable=False),
        pa.field("ts_naive_ns_opt", pa.timestamp("ns"), nullable=True),
    ]
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 03_temporal_types."""

import datetime as dt

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.table(
    {
        "d32": pa.array([dt.date(2026, 4, 10), dt.date(1970, 1, 1)], type=pa.date32()),
        "d64": pa.array([dt.date(2026, 4, 11), dt.date(2000, 2, 29)], type=pa.date64()),
        "t32_s": pa.array([dt.time(8, 30, 0), dt.time(23, 59, 59)], type=pa.time32("s")),
        "t32_ms": pa.array([dt.time(8, 30, 0, 250000), dt.time(0, 0, 0)], type=pa.time32("ms")),
        "t64_us": pa.array([dt.time(14, 15, 30, 123456), dt.time(12, 0, 0)], type=pa.time64("us")),
        "t64_ns": pa.array([52_200_123_456_789, 0], type=pa.time64("ns")),
        "ts_s_utc": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, tzinfo=dt.UTC),
                dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("s", tz="UTC"),
        ),
        "ts_ms_utc": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, 500000, tzinfo=dt.UTC),
                dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("ms", tz="UTC"),
        ),
        "ts_us_la": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, 125000, tzinfo=dt.UTC),
                dt.datetime(2025, 1, 15, 18, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("us", tz="America/Los_Angeles"),
        ),
        "ts_ns_utc": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, 750000, tzinfo=dt.UTC),
                dt.datetime(2025, 6, 1, 6, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("ns", tz="UTC"),
        ),
        "ts_naive_us": pa.array(
            [
                dt.datetime(2026, 4, 10, 9, 45, 0, tzinfo=dt.UTC).replace(tzinfo=None),
                dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.UTC).replace(tzinfo=None),
            ],
            type=pa.timestamp("us"),
        ),
        "ts_naive_ns_opt": pa.array(
            [
                dt.datetime(2026, 4, 10, 9, 45, 0, 500000, tzinfo=dt.UTC).replace(tzinfo=None),
                None,
            ],
            type=pa.timestamp("ns"),
        ),
    },
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "d32": "2026-04-10",
    "d64": "2026-04-11",
    "t32_s": "08:30:00",
    "t32_ms": "08:30:00.250000",
    "t64_us": "14:15:30.123456",
    "t64_ns": "14:30:00.123456789",
    "ts_s_utc": "2026-04-10T14:30:00+00:00",
    "ts_ms_utc": "2026-04-10T14:30:00.500000+00:00",
    "ts_us_la": "2026-04-10T07:30:00.125000-07:00",
    "ts_ns_utc": "2026-04-10T14:30:00.750000+00:00",
    "ts_naive_us": "2026-04-10T09:45:00",
    "ts_naive_ns_opt": "2026-04-10T09:45:00.500000"
  },
  {
    "d32": "1970-01-01",
    "d64": "2000-02-29",
    "t32_s": "23:59:59",
    "t32_ms": "00:00:00",
    "t64_us": "12:00:00",
    "t64_ns": "00:00:00",
    "ts_s_utc": "2025-01-01T00:00:00+00:00",
    "ts_ms_utc": "2025-01-01T00:00:00+00:00",
    "ts_us_la": "2025-01-15T10:00:00-08:00",
    "ts_ns_utc": "2025-06-01T06:00:00+00:00",
    "ts_naive_us": "2025-12-31T23:59:59",
    "ts_naive_ns_opt": null
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/03_temporal_types/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "d32": {
      "type": "string",
      "format": "date",
      "x-pyarrow": {
        "arrow_type": "date32"
      }
    },
    "d64": {
      "type": "string",
      "format": "date",
      "x-pyarrow": {
        "arrow_type": "date64"
      }
    },
    "t32_s": {
      "type": "string",
      "format": "time",
      "x-pyarrow": {
        "arrow_type": "time32",
        "unit": "s"
      }
    },
    "t32_ms": {
      "type": "string",
      "format": "time",
      "x-pyarrow": {
        "arrow_type": "time32",
        "unit": "ms"
      }
    },
    "t64_us": {
      "type": "string",
      "format": "time",
      "x-pyarrow": {
        "arrow_type": "time64",
        "unit": "us"
      }
    },
    "t64_ns": {
      "type": "string",
      "format": "time",
      "x-pyarrow": {
        "arrow_type": "time64",
        "unit": "ns"
      }
    },
    "ts_s_utc": {
      "type": "string",
      "format": "date-time",
      "x-pyarrow": {
        "arrow_type": "timestamp",
        "logical_type": "timestamp-with-tz",
        "unit": "s",
        "tz": "UTC"
      }
    },
    "ts_ms_utc": {
      "type": "string",
      "format": "date-time",
      "x-pyarrow": {
        "arrow_type": "timestamp",
        "logical_type": "timestamp-with-tz",
        "unit": "ms",
        "tz": "UTC"
      }
    },
    "ts_us_la": {
      "type": "string",
      "format": "date-time",
      "x-pyarrow": {
        "arrow_type": "timestamp",
        "logical_type": "timestamp-with-tz",
        "unit": "us",
        "tz": "America/Los_Angeles"
      }
    },
    "ts_ns_utc": {
      "type": "string",
      "format": "date-time",
      "x-pyarrow": {
        "arrow_type": "timestamp",
        "logical_type": "timestamp-with-tz",
        "unit": "ns",
        "tz": "UTC"
      }
    },
    "ts_naive_us": {
      "type": "string",
      "description": "ISO 8601 timestamp with no timezone attached.",
      "x-pyarrow": {
        "arrow_type": "timestamp",
        "logical_type": "timestamp-without-tz",
        "unit": "us"
      }
    },
    "ts_naive_ns_opt": {
      "anyOf": [
        {
          "type": "string",
          "description": "ISO 8601 timestamp with no timezone attached.",
          "x-pyarrow": {
            "arrow_type": "timestamp",
            "logical_type": "timestamp-without-tz",
            "unit": "ns"
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "additionalProperties": false,
  "required": [
    "d32",
    "d64",
    "t32_s",
    "t32_ms",
    "t64_us",
    "t64_ns",
    "ts_s_utc",
    "ts_ms_utc",
    "ts_us_la",
    "ts_ns_utc",
    "ts_naive_us"
  ]
}
```

---

<a id="case-spec-04-nullability-and-metadata"></a>

### Nullability, Envelope Metadata, Descriptions, and Custom Arrow Metadata (`spec/04_nullability_and_metadata`)

**Rules:** [R-ENVELOPE-ID-TITLE](#r-envelope-id-title), [R-ENVELOPE-CLOSED](#r-envelope-closed), [R-FIELD-NAME](#r-field-name), [R-FIELD-ORDER](#r-field-order), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-META-DESCRIPTION](#r-meta-description), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-META-X-PYARROW-CUSTOM](#r-meta-x-pyarrow-custom), [R-SER-ROW-ORDER](#r-ser-row-order)

Demonstrates resolution of `"title"`, `"$id"`, and `"description"` from `pa.Schema.metadata`, preservation of
custom schema and field metadata under `x-pyarrow.metadata`, placement of field `"description"` and
`x-pyarrow.metadata` outside `"anyOf"` on nullable fields, and omission of `"required"` on a nested struct
whose fields are all nullable.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 04_nullability_and_metadata."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "sample_id",
            pa.string(),
            nullable=False,
            metadata={
                "comment": "Unique biosample identifier.",
                "pydantic_type": "string",
                "source_column": "biosample_acc",
            },
        ),
        pa.field(
            "depth_m",
            pa.float64(),
            nullable=True,
            metadata={
                "comment": "Sampling depth in meters below surface.",
                "unit": "m",
            },
        ),
        pa.field(
            "quality_score",
            pa.int32(),
            nullable=True,
        ),
        pa.field(
            "optional_details",
            pa.struct(
                [
                    pa.field("collector", pa.string(), nullable=True, metadata={"comment": "Field collector name."}),
                    pa.field("ph", pa.float32(), nullable=True),
                ]
            ),
            nullable=True,
            metadata={"comment": "Optional environmental annotations where every child field is nullable."},
        ),
    ],
    metadata={
        "title": "EnvironmentalSample",
        "$id": "https://kbase.us/schemas/environmental_sample.json",
        "comment": "Environmental sample record with rich Arrow metadata.",
        "dataset_version": "2026.04",
        "curator": "cdm-ingest",
    },
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 04_nullability_and_metadata."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "sample_id": "SAMN00010001",
            "depth_m": 125.5,
            "quality_score": 98,
            "optional_details": {"collector": "A. Smith", "ph": 7.25},
        },
        {
            "sample_id": "SAMN00010002",
            "depth_m": None,
            "quality_score": None,
            "optional_details": None,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "sample_id": "SAMN00010001",
    "depth_m": 125.5,
    "quality_score": 98,
    "optional_details": {
      "collector": "A. Smith",
      "ph": 7.25
    }
  },
  {
    "sample_id": "SAMN00010002",
    "depth_m": null,
    "quality_score": null,
    "optional_details": null
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/04_nullability_and_metadata/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://kbase.us/schemas/environmental_sample.json",
  "title": "EnvironmentalSample",
  "description": "Environmental sample record with rich Arrow metadata.",
  "type": "object",
  "properties": {
    "sample_id": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "string",
        "metadata": {
          "pydantic_type": "string",
          "source_column": "biosample_acc"
        }
      },
      "description": "Unique biosample identifier."
    },
    "depth_m": {
      "anyOf": [
        {
          "type": "number",
          "x-pyarrow": {
            "arrow_type": "float64"
          }
        },
        {
          "type": "null"
        }
      ],
      "description": "Sampling depth in meters below surface.",
      "x-pyarrow": {
        "metadata": {
          "unit": "m"
        }
      }
    },
    "quality_score": {
      "anyOf": [
        {
          "type": "integer",
          "minimum": -2147483648,
          "maximum": 2147483647,
          "x-pyarrow": {
            "arrow_type": "int32"
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "optional_details": {
      "anyOf": [
        {
          "type": "object",
          "properties": {
            "collector": {
              "anyOf": [
                {
                  "type": "string",
                  "x-pyarrow": {
                    "arrow_type": "string"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "description": "Field collector name."
            },
            "ph": {
              "anyOf": [
                {
                  "type": "number",
                  "x-pyarrow": {
                    "arrow_type": "float32"
                  }
                },
                {
                  "type": "null"
                }
              ]
            }
          },
          "additionalProperties": false,
          "x-pyarrow": {
            "arrow_type": "struct"
          }
        },
        {
          "type": "null"
        }
      ],
      "description": "Optional environmental annotations where every child field is nullable."
    }
  },
  "additionalProperties": false,
  "required": [
    "sample_id"
  ],
  "x-pyarrow": {
    "metadata": {
      "dataset_version": "2026.04",
      "curator": "cdm-ingest"
    }
  }
}
```

---

<a id="case-spec-05-metadata-omission-and-overrides"></a>

### Explicit Envelope Overrides and Omitting `x-pyarrow` Metadata (`spec/05_metadata_omission_and_overrides`)

**Rules:** [R-DIALECT](#r-dialect), [R-ENVELOPE-ID-TITLE](#r-envelope-id-title), [R-ENVELOPE-CLOSED](#r-envelope-closed), [R-META-DESCRIPTION](#r-meta-description), [R-META-OMIT-FLAG](#r-meta-omit-flag), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SER-ROW-ORDER](#r-ser-row-order)

Verifies that explicit `schema_name` and `schema_id` parameters override `metadata[b"title"]` and
`metadata[b"$id"]`, and that `include_pyarrow_metadata=False` strips all `"x-pyarrow"` blocks while keeping
standard Draft 2020-12 keywords (`$schema`, `$id`, `title`, `description`, `minimum`, `maximum`, `pattern`,
`format`, `contentEncoding`).

#### Schema (`schema.py`)

```python
"""PyArrow schema for 05_metadata_omission_and_overrides."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"comment": "Record identifier.", "xsd_type": "xs:ID"}),
        pa.field("count", pa.uint16(), nullable=False, metadata={"unit": "reads"}),
        pa.field("gc_ratio", pa.decimal128(5, 4), nullable=True, metadata={"comment": "GC fraction."}),
        pa.field("digest", pa.binary(8), nullable=False),
        pa.field("collected_on", pa.date32(), nullable=True),
    ],
    metadata={
        "title": "OverriddenTitle",
        "$id": "urn:overridden:sample",
        "comment": "Pure Draft 2020-12 schema with x-pyarrow suppressed.",
        "internal_tag": "drop-when-omitted",
    },
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 05_metadata_omission_and_overrides."""

import datetime as dt
from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "id": "S-01",
            "count": 1024,
            "gc_ratio": Decimal("0.5125"),
            "digest": b"01234567",
            "collected_on": dt.date(2026, 3, 15),
        },
        {
            "id": "S-02",
            "count": 0,
            "gc_ratio": None,
            "digest": b"\x00\x01\x02\x03\x04\x05\x06\x07",
            "collected_on": None,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `without_pyarrow_metadata` (`schema_name=CleanSample`, `schema_id=https://kbase.us/schemas/clean_sample.json`, `include_pyarrow_metadata=False`)

```json
[
  {
    "id": "S-01",
    "count": 1024,
    "gc_ratio": "0.5125",
    "digest": "MDEyMzQ1Njc=",
    "collected_on": "2026-03-15"
  },
  {
    "id": "S-02",
    "count": 0,
    "gc_ratio": null,
    "digest": "AAECAwQFBgc=",
    "collected_on": null
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/05_metadata_omission_and_overrides/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://kbase.us/schemas/clean_sample.json",
  "title": "CleanSample",
  "description": "Pure Draft 2020-12 schema with x-pyarrow suppressed.",
  "type": "object",
  "properties": {
    "id": {
      "type": "string",
      "description": "Record identifier."
    },
    "count": {
      "type": "integer",
      "minimum": 0,
      "maximum": 65535
    },
    "gc_ratio": {
      "anyOf": [
        {
          "type": "string",
          "pattern": "^-?\\d{1,1}(\\.\\d{1,4})?$"
        },
        {
          "type": "null"
        }
      ],
      "description": "GC fraction."
    },
    "digest": {
      "type": "string",
      "contentEncoding": "base64"
    },
    "collected_on": {
      "anyOf": [
        {
          "type": "string",
          "format": "date"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "additionalProperties": false,
  "required": [
    "id",
    "count",
    "digest"
  ]
}
```

---

<a id="case-spec-06-structs-and-lists"></a>

### Nested Structs, Variable/View Lists, Fixed-Size Lists, and 2D Lists (`spec/06_structs_and_lists`)

**Rules:** [R-SHAPE-STRUCT](#r-shape-struct), [R-SHAPE-LIST](#r-shape-list), [R-SHAPE-FIXED-LIST](#r-shape-fixed-list), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-NULL-LIST-ITEMS](#r-null-list-items), [R-META-DESCRIPTION](#r-meta-description), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-META-X-PYARROW-CUSTOM](#r-meta-x-pyarrow-custom), [R-SER-ROW-ORDER](#r-ser-row-order)

Exercises nested `pa.struct`, `pa.list_`, `pa.large_list`, `pa.list_view`, `pa.large_list_view`,
`pa.list_(item, 3)` (`minItems: 3, maxItems: 3`), 2D `list<list<int32>>`, nullable vs non-nullable list
elements, and element-level `comment` + custom metadata on inner `pa.field` items.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 06_structs_and_lists."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("feature_id", pa.string(), nullable=False),
        pa.field(
            "location",
            pa.struct(
                [
                    pa.field("contig", pa.string(), nullable=False),
                    pa.field("start", pa.int64(), nullable=False),
                    pa.field("end", pa.int64(), nullable=False),
                    pa.field("strand", pa.string(), nullable=True),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "go_terms",
            pa.list_(
                pa.field(
                    "item",
                    pa.string(),
                    nullable=False,
                    metadata={"comment": "Gene Ontology term accession.", "ontology": "GO"},
                )
            ),
            nullable=False,
        ),
        pa.field(
            "nullable_scores",
            pa.large_list(pa.field("item", pa.int32(), nullable=True)),
            nullable=True,
        ),
        pa.field(
            "aliases_view",
            pa.list_view(pa.field("item", pa.string(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "notes_large_view",
            pa.large_list_view(pa.field("item", pa.string(), nullable=True)),
            nullable=True,
        ),
        pa.field(
            "rgb_triplet",
            pa.list_(pa.field("item", pa.uint8(), nullable=False), 3),
            nullable=False,
        ),
        pa.field(
            "score_matrix",
            pa.list_(
                pa.field(
                    "row",
                    pa.list_(pa.field("cell", pa.int32(), nullable=False)),
                    nullable=False,
                )
            ),
            nullable=False,
        ),
    ]
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 06_structs_and_lists."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "feature_id": "CDS_0001",
            "location": {"contig": "chr1", "start": 100, "end": 950, "strand": "+"},
            "go_terms": ["GO:0003677", "GO:0006355"],
            "nullable_scores": [10, None, 42],
            "aliases_view": ["dnaA", "b3702"],
            "notes_large_view": ["verified", None],
            "rgb_triplet": [255, 128, 0],
            "score_matrix": [[1, 2], [3, 4, 5]],
        },
        {
            "feature_id": "CDS_0002",
            "location": {"contig": "chr2", "start": 2050, "end": 3100, "strand": None},
            "go_terms": [],
            "nullable_scores": None,
            "aliases_view": [],
            "notes_large_view": None,
            "rgb_triplet": [0, 64, 128],
            "score_matrix": [[]],
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "feature_id": "CDS_0001",
    "location": {
      "contig": "chr1",
      "start": 100,
      "end": 950,
      "strand": "+"
    },
    "go_terms": [
      "GO:0003677",
      "GO:0006355"
    ],
    "nullable_scores": [
      10,
      null,
      42
    ],
    "aliases_view": [
      "dnaA",
      "b3702"
    ],
    "notes_large_view": [
      "verified",
      null
    ],
    "rgb_triplet": [
      255,
      128,
      0
    ],
    "score_matrix": [
      [
        1,
        2
      ],
      [
        3,
        4,
        5
      ]
    ]
  },
  {
    "feature_id": "CDS_0002",
    "location": {
      "contig": "chr2",
      "start": 2050,
      "end": 3100,
      "strand": null
    },
    "go_terms": [],
    "nullable_scores": null,
    "aliases_view": [],
    "notes_large_view": null,
    "rgb_triplet": [
      0,
      64,
      128
    ],
    "score_matrix": [
      []
    ]
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/06_structs_and_lists/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "feature_id": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "string"
      }
    },
    "location": {
      "type": "object",
      "properties": {
        "contig": {
          "type": "string",
          "x-pyarrow": {
            "arrow_type": "string"
          }
        },
        "start": {
          "type": "integer",
          "minimum": -9223372036854775808,
          "maximum": 9223372036854775807,
          "x-pyarrow": {
            "arrow_type": "int64"
          }
        },
        "end": {
          "type": "integer",
          "minimum": -9223372036854775808,
          "maximum": 9223372036854775807,
          "x-pyarrow": {
            "arrow_type": "int64"
          }
        },
        "strand": {
          "anyOf": [
            {
              "type": "string",
              "x-pyarrow": {
                "arrow_type": "string"
              }
            },
            {
              "type": "null"
            }
          ]
        }
      },
      "additionalProperties": false,
      "required": [
        "contig",
        "start",
        "end"
      ],
      "x-pyarrow": {
        "arrow_type": "struct"
      }
    },
    "go_terms": {
      "type": "array",
      "items": {
        "type": "string",
        "description": "Gene Ontology term accession.",
        "x-pyarrow": {
          "arrow_type": "string",
          "metadata": {
            "ontology": "GO"
          }
        }
      },
      "x-pyarrow": {
        "arrow_type": "list"
      }
    },
    "nullable_scores": {
      "anyOf": [
        {
          "type": "array",
          "items": {
            "anyOf": [
              {
                "type": "integer",
                "minimum": -2147483648,
                "maximum": 2147483647,
                "x-pyarrow": {
                  "arrow_type": "int32"
                }
              },
              {
                "type": "null"
              }
            ]
          },
          "x-pyarrow": {
            "arrow_type": "large_list"
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "aliases_view": {
      "type": "array",
      "items": {
        "type": "string",
        "x-pyarrow": {
          "arrow_type": "string"
        }
      },
      "x-pyarrow": {
        "arrow_type": "list_view"
      }
    },
    "notes_large_view": {
      "anyOf": [
        {
          "type": "array",
          "items": {
            "anyOf": [
              {
                "type": "string",
                "x-pyarrow": {
                  "arrow_type": "string"
                }
              },
              {
                "type": "null"
              }
            ]
          },
          "x-pyarrow": {
            "arrow_type": "large_list_view"
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "rgb_triplet": {
      "type": "array",
      "items": {
        "type": "integer",
        "minimum": 0,
        "maximum": 255,
        "x-pyarrow": {
          "arrow_type": "uint8"
        }
      },
      "minItems": 3,
      "maxItems": 3,
      "x-pyarrow": {
        "arrow_type": "fixed_size_list",
        "list_size": 3
      }
    },
    "score_matrix": {
      "type": "array",
      "items": {
        "type": "array",
        "items": {
          "type": "integer",
          "minimum": -2147483648,
          "maximum": 2147483647,
          "x-pyarrow": {
            "arrow_type": "int32"
          }
        },
        "x-pyarrow": {
          "arrow_type": "list"
        }
      },
      "x-pyarrow": {
        "arrow_type": "list"
      }
    }
  },
  "additionalProperties": false,
  "required": [
    "feature_id",
    "location",
    "go_terms",
    "aliases_view",
    "rgb_triplet",
    "score_matrix"
  ]
}
```

---

<a id="case-spec-07-maps-and-dictionaries"></a>

### String-Keyed and Non-String-Keyed Maps and Dictionary-Encoded Columns (`spec/07_maps_and_dictionaries`)

**Rules:** [R-SHAPE-MAP](#r-shape-map), [R-SHAPE-DICTIONARY](#r-shape-dictionary), [R-SHAPE-STRUCT](#r-shape-struct), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-NULL-MAP-VALUES](#r-null-map-values), [R-META-DESCRIPTION](#r-meta-description), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-MAP-ENTRIES](#r-ser-map-entries), [R-SER-DECIMAL-STR](#r-ser-decimal-str)

Exercises `pa.map_` with string keys and nullable struct values, `pa.map_` with sorted `int32` keys and
non-nullable `float64` values, and `pa.dictionary` columns wrapping `string`, `int64` (`ordered=True`), and
`decimal128(8, 2)`.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 07_maps_and_dictionaries."""

import pyarrow as pa

ANNOTATION_VALUE_STRUCT = pa.struct(
    [
        pa.field("db", pa.string(), nullable=False),
        pa.field("evalue", pa.float64(), nullable=True),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("entry_id", pa.string(), nullable=False),
        pa.field(
            "biotype",
            pa.dictionary(pa.int16(), pa.string(), ordered=False),
            nullable=False,
            metadata={"comment": "Controlled biotype term."},
        ),
        pa.field(
            "priority_tier",
            pa.dictionary(pa.int8(), pa.int64(), ordered=True),
            nullable=True,
        ),
        pa.field(
            "rate_bucket",
            pa.dictionary(pa.int8(), pa.decimal128(8, 2), ordered=False),
            nullable=False,
        ),
        pa.field(
            "xref_hits",
            pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={"comment": "External database key."}),
                pa.field("value", ANNOTATION_VALUE_STRUCT, nullable=True),
            ),
            nullable=False,
        ),
        pa.field(
            "coverage_by_bin",
            pa.map_(
                pa.int32(),
                pa.field("value", pa.float64(), nullable=False),
                keys_sorted=True,
            ),
            nullable=True,
        ),
    ]
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 07_maps_and_dictionaries."""

from decimal import Decimal

import pyarrow as pa
from schema import ANNOTATION_VALUE_STRUCT, SCHEMA

TABLE = pa.Table.from_arrays(
    [
        pa.array(["PROT-001", "PROT-002"], type=pa.string()),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int16()),
            pa.array(["protein_coding", "rRNA"], type=pa.string()),
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, None], type=pa.int8()),
            pa.array([1], type=pa.int64()),
            ordered=True,
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int8()),
            pa.array([Decimal("12.50"), Decimal("0.75")], type=pa.decimal128(8, 2)),
        ),
        pa.array(
            [
                [("PF00001", {"db": "Pfam", "evalue": 1.25e-12}), ("IPR000276", None)],
                [],
            ],
            type=pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={"comment": "External database key."}),
                pa.field("value", ANNOTATION_VALUE_STRUCT, nullable=True),
            ),
        ),
        pa.array(
            [[(1, 45.5), (2, 88.25)], None],
            type=pa.map_(pa.int32(), pa.field("value", pa.float64(), nullable=False), keys_sorted=True),
        ),
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "entry_id": "PROT-001",
    "biotype": "protein_coding",
    "priority_tier": 1,
    "rate_bucket": "12.50",
    "xref_hits": [
      {
        "key": "PF00001",
        "value": {
          "db": "Pfam",
          "evalue": 1.25e-12
        }
      },
      {
        "key": "IPR000276",
        "value": null
      }
    ],
    "coverage_by_bin": [
      {
        "key": 1,
        "value": 45.5
      },
      {
        "key": 2,
        "value": 88.25
      }
    ]
  },
  {
    "entry_id": "PROT-002",
    "biotype": "rRNA",
    "priority_tier": null,
    "rate_bucket": "0.75",
    "xref_hits": [],
    "coverage_by_bin": null
  }
]
```

#### JSON Schema ([`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/07_maps_and_dictionaries/expected_schema.json))

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "entry_id": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "string"
      }
    },
    "biotype": {
      "type": "string",
      "x-pyarrow": {
        "arrow_type": "dictionary",
        "value_type": "string",
        "index_type": "int16",
        "ordered": false
      },
      "description": "Controlled biotype term."
    },
    "priority_tier": {
      "anyOf": [
        {
          "type": "integer",
          "minimum": -9223372036854775808,
          "maximum": 9223372036854775807,
          "x-pyarrow": {
            "arrow_type": "dictionary",
            "value_type": "int64",
            "index_type": "int8",
            "ordered": true
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "rate_bucket": {
      "type": "string",
      "pattern": "^-?\\d{1,6}(\\.\\d{1,2})?$",
      "x-pyarrow": {
        "arrow_type": "dictionary",
        "logical_type": "decimal",
        "precision": 8,
        "scale": 2,
        "value_type": "decimal128",
        "index_type": "int8",
        "ordered": false
      }
    },
    "xref_hits": {
      "type": "array",
      "description": "PyArrow map rendered as key/value pairs (JSON object keys must be strings).",
      "items": {
        "type": "object",
        "properties": {
          "key": {
            "type": "string",
            "description": "External database key.",
            "x-pyarrow": {
              "arrow_type": "string"
            }
          },
          "value": {
            "anyOf": [
              {
                "type": "object",
                "properties": {
                  "db": {
                    "type": "string",
                    "x-pyarrow": {
                      "arrow_type": "string"
                    }
                  },
                  "evalue": {
                    "anyOf": [
                      {
                        "type": "number",
                        "x-pyarrow": {
                          "arrow_type": "float64"
                        }
                      },
                      {
                        "type": "null"
                      }
                    ]
                  }
                },
                "additionalProperties": false,
                "required": [
                  "db"
                ],
                "x-pyarrow": {
                  "arrow_type": "struct"
                }
              },
              {
                "type": "null"
              }
            ]
          }
        },
        "required": [
          "key",
          "value"
        ],
        "additionalProperties": false
      },
      "x-pyarrow": {
        "arrow_type": "map",
        "keys_sorted": false
      }
    },
    "coverage_by_bin": {
      "anyOf": [
        {
          "type": "array",
          "description": "PyArrow map rendered as key/value pairs (JSON object keys must be strings).",
          "items": {
            "type": "object",
            "properties": {
              "key": {
                "type": "integer",
                "minimum": -2147483648,
                "maximum": 2147483647,
                "x-pyarrow": {
                  "arrow_type": "int32"
                }
              },
              "value": {
                "type": "number",
                "x-pyarrow": {
                  "arrow_type": "float64"
                }
              }
            },
            "required": [
              "key",
              "value"
            ],
            "additionalProperties": false
          },
          "x-pyarrow": {
            "arrow_type": "map",
            "keys_sorted": true
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "additionalProperties": false,
  "required": [
    "entry_id",
    "biotype",
    "rate_bucket",
    "xref_hits"
  ]
}
```

---

<a id="case-examples-checkm2-quality-report"></a>

### CheckM2 Genome Quality and Contig Statistics Table (`examples/checkm2_quality_report`)

**Rules:** [R-SCHEMA-INPUT](#r-schema-input), [R-DIALECT](#r-dialect), [R-ENVELOPE-ID-TITLE](#r-envelope-id-title), [R-ENVELOPE-CLOSED](#r-envelope-closed), [R-FIELD-NAME](#r-field-name), [R-FIELD-ORDER](#r-field-order), [R-SHAPE-STRUCT](#r-shape-struct), [R-SHAPE-LIST](#r-shape-list), [R-SHAPE-MAP](#r-shape-map), [R-SHAPE-DICTIONARY](#r-shape-dictionary), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-FLOAT](#r-scalar-float), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-SCALAR-STRING](#r-scalar-string), [R-META-DESCRIPTION](#r-meta-description), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-MAP-ENTRIES](#r-ser-map-entries)

**Schema:** [`specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/schema.py)

- Run `default`: [`table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/table.py), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/expected.json)

**Expected JSON Schema:** [`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/expected_schema.json)

---

<a id="case-examples-ncbi-assembly-summary"></a>

### NCBI Datasets Assembly Summary Table (`examples/ncbi_assembly_summary`)

**Rules:** [R-ENVELOPE-ID-TITLE](#r-envelope-id-title), [R-SHAPE-STRUCT](#r-shape-struct), [R-SHAPE-LIST](#r-shape-list), [R-SHAPE-DICTIONARY](#r-shape-dictionary), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-BOOL](#r-scalar-bool), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-SCALAR-STRING](#r-scalar-string), [R-SCALAR-BINARY](#r-scalar-binary), [R-SCALAR-DATE-TIME](#r-scalar-date-time), [R-SCALAR-TIMESTAMP](#r-scalar-timestamp), [R-META-DESCRIPTION](#r-meta-description), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-BASE64](#r-ser-base64), [R-SER-TEMPORAL-ISO](#r-ser-temporal-iso)

**Schema:** [`specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/schema.py)

- Run `default`: [`table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/table.py), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/expected.json)

**Expected JSON Schema:** [`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/expected_schema.json)

---

<a id="case-examples-uniref-cluster-entry"></a>

### UniRef Sequence Cluster Inventory Table (`examples/uniref_cluster_entry`)

**Rules:** [R-ENVELOPE-ID-TITLE](#r-envelope-id-title), [R-SHAPE-STRUCT](#r-shape-struct), [R-SHAPE-LIST](#r-shape-list), [R-SHAPE-FIXED-LIST](#r-shape-fixed-list), [R-SHAPE-MAP](#r-shape-map), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-FLOAT](#r-scalar-float), [R-SCALAR-STRING](#r-scalar-string), [R-SCALAR-BINARY](#r-scalar-binary), [R-SCALAR-TIMESTAMP](#r-scalar-timestamp), [R-META-DESCRIPTION](#r-meta-description), [R-META-X-PYARROW-TYPE](#r-meta-x-pyarrow-type), [R-META-X-PYARROW-CUSTOM](#r-meta-x-pyarrow-custom), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-BASE64](#r-ser-base64), [R-SER-TEMPORAL-ISO](#r-ser-temporal-iso), [R-SER-MAP-ENTRIES](#r-ser-map-entries)

**Schema:** [`specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/schema.py)

- Run `default`: [`table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/table.py), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/expected.json)

**Expected JSON Schema:** [`expected_schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/expected_schema.json)

---

## 3. Errors (Unsupported / Invalid Input)

Schema generation raises a subclass of `PyArrowToJSONSchemaError` (itself a `ValueError`).

| Code | Exception | Condition | Reason | Fixtures |
| :--- | :--- | :--- | :--- | :--- |
| **E1** | `InvalidPyArrowSchemaError` | Input is not a `pa.Schema` instance | Schema generation requires a concrete PyArrow `pa.Schema` object; bare `pa.DataType`, `pa.Field`, or Python mappings are not table schemas. | [`e01_non_schema_input.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e01_non_schema_input.py): Input is a `pa.StructType` rather than a `pa.Schema`. |
| **E2** | `EmptyStructError` | Empty root schema or nested `pa.struct([])` (zero fields) | Arrow tables and Parquet struct columns require at least one named child field. | [`e02a_empty_root_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e02a_empty_root_schema.py): Root `pa.schema([])` declares zero fields.<br>[`e02b_empty_nested_struct.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e02b_empty_nested_struct.py): Nested `pa.struct([])` column declares zero child fields. |
| **E3** | `DuplicateFieldNameError` | Duplicate field name in schema or `pa.struct` | JSON Schema `properties` is an object mapping keyed by property name and cannot represent duplicate sibling field names. | [`e03a_duplicate_root_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e03a_duplicate_root_field.py): Root `pa.Schema` declares two fields named `'id'`.<br>[`e03b_duplicate_struct_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e03b_duplicate_struct_field.py): Nested `pa.struct` declares duplicate child fields named `'tag'`. |
| **E4** | `NullOnlyTypeError` | Standalone `pa.null()` field or element type | A column or container element typed solely as `pa.null()` carries no value domain; nullable columns must wrap a concrete Arrow value type with `nullable=True`. | [`e04_null_only_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e04_null_only_field.py): Column uses standalone `pa.null()` with no value type. |
| **E5** | `InvalidDecimalTypeError` | Decimal type with negative scale or `scale > precision` | Fixed-point decimal string patterns require `0 <= scale <= precision`; negative scales or `scale > precision` have no plain fixed-point string regex. | [`e05_negative_scale_decimal.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e05_negative_scale_decimal.py): Column uses `pa.decimal128(6, -2)` with negative scale. |
| **E6** | `UnsupportedTemporalIntervalError` | Duration or calendar interval type (`pa.duration`, `pa.month_day_nano_interval`) | Relative durations and `month_day_nano` intervals have no lossless canonical scalar representation across JSON Schema validators without losing unit or sign-per-component semantics. | [`e06a_duration_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e06a_duration_field.py): Column uses `pa.duration("us")`.<br>[`e06b_interval_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e06b_interval_field.py): Column uses `pa.month_day_nano_interval()`. |
| **E7** | `UnsupportedUnionTypeError` | Arrow union type (`pa.sparse_union` or `pa.dense_union`) | Arrow type-code unions lack stable JSON property discriminators and Parquet storage support; union columns must be normalized before schema export. | [`e07a_sparse_union_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e07a_sparse_union_field.py): Column uses `pa.sparse_union(...)`.<br>[`e07b_dense_union_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e07b_dense_union_field.py): Column uses `pa.dense_union(...)`. |
| **E8** | `UnsupportedArrowTypeError` | Unsupported Arrow physical or extension type (`run_end_encoded`, `ExtensionType`, etc.) | Specialized physical encodings such as `pa.run_end_encoded` and custom `pa.ExtensionType` subclasses have no general target-independent JSON Schema contract. | [`e08a_run_end_encoded_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e08a_run_end_encoded_field.py): Column uses `pa.run_end_encoded(pa.int32(), pa.string())`.<br>[`e08b_extension_type_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/errors/e08b_extension_type_field.py): Column uses Arrow extension type `pa.uuid()`. |

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suite and this document are both driven by it.

| Case | Schema | Runs |
| :--- | :--- | :--- |
| [`spec/01_integers_floats_and_booleans`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/01_integers_floats_and_booleans/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/01_integers_floats_and_booleans/schema.py) | boundary_values |
| [`spec/02_strings_binary_and_decimals`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/02_strings_binary_and_decimals/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/02_strings_binary_and_decimals/schema.py) | default |
| [`spec/03_temporal_types`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/03_temporal_types/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/03_temporal_types/schema.py) | default |
| [`spec/04_nullability_and_metadata`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/04_nullability_and_metadata/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/04_nullability_and_metadata/schema.py) | default |
| [`spec/05_metadata_omission_and_overrides`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/05_metadata_omission_and_overrides/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/05_metadata_omission_and_overrides/schema.py) | without_pyarrow_metadata |
| [`spec/06_structs_and_lists`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/06_structs_and_lists/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/06_structs_and_lists/schema.py) | default |
| [`spec/07_maps_and_dictionaries`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/07_maps_and_dictionaries/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/spec/07_maps_and_dictionaries/schema.py) | default |
| [`examples/checkm2_quality_report`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/checkm2_quality_report/schema.py) | default |
| [`examples/ncbi_assembly_summary`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/ncbi_assembly_summary/schema.py) | default |
| [`examples/uniref_cluster_entry`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_jsonschema/examples/uniref_cluster_entry/schema.py) | default |
