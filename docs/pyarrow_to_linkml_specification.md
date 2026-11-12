<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/schema_converters/pyarrow_to_linkml/rules.yaml`, `specs/schema_converters/pyarrow_to_linkml/spec`, `specs/schema_converters/pyarrow_to_linkml/examples`. Regenerate with `uv run python -m scripts.gen_converter_spec`. -->

# PyArrow to LinkML Specification

This document specifies how a **PyArrow `pa.Schema`** determines:

1. The **LinkML `SchemaDefinition`** (and YAML schema string) produced by `generate_linkml_schema`,
   `generate_linkml_yaml`, and `PyArrowToLinkML`, including `linkml:types` scalar ranges, integer bit-width
   bounds (`minimum_value` / `maximum_value`), fixed-size list cardinalities, synthesized nested `ClassDefinition`s
   for Arrow `struct` and `map` columns, and collision-free identifier sanitization.
2. The **serialized JSON-compatible row dictionaries** produced by `serialize_table_for_linkml` and
   `serialize_record_for_linkml`, ready for LinkML instance validation.
3. The **errors** raised for Arrow schemas or data types that have no faithful LinkML representation.

**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · [3. Errors](#3-errors-unsupported-invalid-input) · [4. Fixture Index](#4-fixture-index)

## 1. Conversion Rules

### 1.1 Top-Level Schema Resolution and Envelope

<a id="r-schema-input"></a>

- **R-SCHEMA-INPUT**: The input to `generate_linkml_schema` and `generate_linkml_yaml` must be a concrete `pyarrow.Schema`
  instance; passing a bare `pa.DataType`, `pa.Field`, `pa.Table`, or Python mapping raises **E1**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-non-empty-root"></a>

- **R-NON-EMPTY-ROOT**: The root `pa.Schema` must contain at least one field; an empty `pa.schema([])` raises **E2**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans)

<a id="r-schema-identity"></a>

- **R-SCHEMA-IDENTITY**: The candidate name resolves from explicit `schema_name` when provided, otherwise from non-empty UTF-8
  `schema.metadata[b"title"]`. `SchemaDefinition.name` is the sanitized identifier of that candidate
  name (defaulting to `"schema"` when absent), and the root `ClassDefinition.name` is the sanitized
  identifier of that candidate name (defaulting to `"Root"` when absent). `SchemaDefinition.id`
  resolves from explicit `schema_id` when provided, otherwise from non-empty UTF-8
  `schema.metadata[b"$id"]`, defaulting to `"urn:linkml:<SchemaDefinition.name>"`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-imports-prefixes"></a>

- **R-IMPORTS-PREFIXES**: Every standalone `SchemaDefinition` imports `"linkml:types"`, sets `default_prefix` to
  `SchemaDefinition.name`, and declares two prefixes: `<SchemaDefinition.name>: "<id>#"` and
  `linkml: "https://w3id.org/linkml/"`. When `destination` is supplied to `generate_linkml_schema`,
  generated classes are appended into `destination.classes` without overwriting prior definitions.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

### 1.2 Slot Naming, Identifier Sanitization, and Collision Disambiguation

<a id="r-slot-name"></a>

- **R-SLOT-NAME**: Every `pa.Field.name` must contain at least one non-whitespace character and is preserved verbatim as
  its `SlotDefinition` key under `ClassDefinition.attributes`. An empty or whitespace-only field name
  (`""` or `"   "`) raises **E9**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-slot-order"></a>

- **R-SLOT-ORDER**: `ClassDefinition.attributes` preserves the exact field declaration order of the source `pa.Schema` or
  `pa.StructType`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [checkm2_quality_report](#case-examples-checkm2-quality-report)

<a id="r-unique-field-names"></a>

- **R-UNIQUE-FIELD-NAMES**: Sibling fields within a `pa.Schema` or `pa.StructType` must have distinct names; duplicate sibling
  field names raise **E3**.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans)

<a id="r-class-identifier"></a>

- **R-CLASS-IDENTIFIER**: LinkML schema and class identifiers replace characters outside `[A-Za-z0-9_]` with `"_"`, strip
  leading and trailing `"_"`, fall back to `"schema"` when no identifier characters remain, and prepend
  `"_"` when the first character is an ASCII digit (`0`..`9`).

  *Cases:* [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions)

<a id="r-class-collision"></a>

- **R-CLASS-COLLISION**: Derived nested struct class names (`<OwnerClass>_<field_name>`) and map entry class names
  (`<OwnerClass>_<field_name>_map`) are deduplicated across the `SchemaDefinition` by appending `_2`,
  `_3`, ... whenever two sanitized class names collide.

  *Cases:* [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions)

### 1.3 Structural Container Shapes

<a id="r-shape-struct-class"></a>

- **R-SHAPE-STRUCT-CLASS**: A nested `pa.struct([...])` with at least one child field emits a `ClassDefinition` named
  `<OwnerClass>_<field_name>` (sanitized and deduplicated) and sets the parent slot's `range` to that
  class name. An empty nested `pa.struct([])` raises **E2**.

  *Cases:* [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists), [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-list-slot"></a>

- **R-SHAPE-LIST-SLOT**: Variable-length Arrow list types (`pa.list_`, `pa.large_list`, `pa.list_view`, `pa.large_list_view`)
  emit a single `SlotDefinition` with `multivalued: true` and the `range` of the inner element type
  (omitting scalar `minimum_value`/`maximum_value` so LinkML's Pydantic codegen does not apply numeric
  bounds to the outer list container), generating an element `ClassDefinition` when the element is a
  `pa.struct`.

  *Cases:* [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-fixed-list-cardinality"></a>

- **R-SHAPE-FIXED-LIST-CARDINALITY**: Fixed-length `pa.list_(item, list_size)` (`pa.FixedSizeListType`) emits a multivalued `SlotDefinition`
  (`multivalued: true`) with `minimum_cardinality: list_size` and `maximum_cardinality: list_size`.

  *Cases:* [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-map-class"></a>

- **R-SHAPE-MAP-CLASS**: `pa.map_(key_type, item_type)` emits an entry `ClassDefinition` named `<OwnerClass>_<field_name>_map`
  with attributes `key` and `value`, and sets the map slot to `range: <OwnerClass>_<field_name>_map` and
  `multivalued: true`.

  *Cases:* [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-shape-dictionary"></a>

- **R-SHAPE-DICTIONARY**: `pa.dictionary(index_type, value_type, ordered=...)` unwraps to the LinkML `range` and integer bounds
  of `value_type`.

  *Cases:* [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-reject-nested-multivalued"></a>

- **R-REJECT-NESTED-MULTIVALUED**: Because LinkML represents list multiplicity via a flat `multivalued: true` flag on a slot rather than
  nested array containers, directly nested lists (`list<list<...>>`) and lists of maps (`list<map<...>>`)
  raise **E10**. Nested collections wrapped inside an intermediate `pa.struct` (`list<struct<...>>`) are
  supported via the struct's `ClassDefinition`.

  *Cases:* [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists)

### 1.4 Requiredness and List Element Constraints

<a id="r-null-required"></a>

- **R-NULL-REQUIRED**: A field with `pa.Field.nullable == False` sets `required: true` on its `SlotDefinition`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists), [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-null-optional"></a>

- **R-NULL-OPTIONAL**: A field with `pa.Field.nullable == True` omits `required` (leaving the LinkML slot optional).
  Standalone `pa.null()` columns or element types have no value domain and raise **E4**.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists), [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-null-map-value"></a>

- **R-NULL-MAP-VALUE**: Inside a generated `<OwnerClass>_<field_name>_map` entry class, `key` always carries `required: true`;
  `value` carries `required: true` when `MapType.item_field.nullable == False` and omits `required` when
  `MapType.item_field.nullable == True`.

  *Cases:* [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-list-non-null-items"></a>

- **R-LIST-NON-NULL-ITEMS**: LinkML multivalued slots represent lists of non-null domain values: `serialize_table_for_linkml` and
  `serialize_record_for_linkml` reject `null` elements inside populated list values with `ValueError`
  while permitting `null` (`None`) on optional outer list slots.

  *Cases:* [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists)

### 1.5 Scalar Ranges and Integer Bounds

<a id="r-scalar-bool"></a>

- **R-SCALAR-BOOL**: `pa.bool_()` maps to `range: boolean`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-signed-int"></a>

- **R-SCALAR-SIGNED-INT**: Single-valued signed integer slots (`int8`, `int16`, `int32`, `int64`) map to `range: integer` with
  `minimum_value: -2^(w-1)` and `maximum_value: 2^(w-1) - 1` for bit width `w` (multivalued integer
  list slots omit `minimum_value`/`maximum_value`; see [R-SHAPE-LIST-SLOT](#r-shape-list-slot)).

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-unsigned-int"></a>

- **R-SCALAR-UNSIGNED-INT**: Single-valued unsigned integer slots (`uint8`, `uint16`, `uint32`, `uint64`) map to `range: integer`
  with `minimum_value: 0` and `maximum_value: 2^w - 1` for bit width `w` (multivalued integer list
  slots omit `minimum_value`/`maximum_value`; see [R-SHAPE-LIST-SLOT](#r-shape-list-slot)).

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-float"></a>

- **R-SCALAR-FLOAT**: IEEE-754 floating-point types (`float16`, `float32`, `float64`) map to `range: float`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-decimal"></a>

- **R-SCALAR-DECIMAL**: `pa.decimal128(p, s)` and `pa.decimal256(p, s)` with `0 <= s <= p` map to `range: decimal`. Decimals
  with `s < 0` or `s > p` raise **E5**.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-scalar-string-binary"></a>

- **R-SCALAR-STRING-BINARY**: UTF-8 string types (`string`, `large_string`, `string_view`) and Base64-serialized binary types
  (`binary`, `large_binary`, `binary_view`, `fixed_size_binary(n)`) map to `range: string`.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-scalar-temporal"></a>

- **R-SCALAR-TEMPORAL**: `date32` and `date64` map to `range: date`; `time32` and `time64` map to `range: time`; both
  timezone-aware and timezone-naive `pa.timestamp` types map to `range: datetime`. Relative durations
  and calendar intervals (`pa.duration`, `pa.month_day_nano_interval`) raise **E6**; Arrow unions raise
  **E7**; `run_end_encoded` and `BaseExtensionType` types (`pa.uuid()`, custom `ExtensionType`) raise
  **E8**.

  *Cases:* [03_temporal_types](#case-spec-03-temporal-types), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

| PyArrow `DataType` | Serialized JSON value | LinkML `range` | Slot bounds (`minimum_value` .. `maximum_value`) |
| :--- | :--- | :--- | :--- |
| `pa.bool_()` | `bool` | `boolean` | — |
| `pa.int8()` | `int` | `integer` | `-128` .. `127` |
| `pa.int16()` | `int` | `integer` | `-32768` .. `32767` |
| `pa.int32()` | `int` | `integer` | `-2147483648` .. `2147483647` |
| `pa.int64()` | `int` | `integer` | `-9223372036854775808` .. `9223372036854775807` |
| `pa.uint8()` | `int` | `integer` | `0` .. `255` |
| `pa.uint16()` | `int` | `integer` | `0` .. `65535` |
| `pa.uint32()` | `int` | `integer` | `0` .. `4294967295` |
| `pa.uint64()` | `int` | `integer` | `0` .. `18446744073709551615` |
| `pa.float16()` | `float` | `float` | — |
| `pa.float32()` | `float` | `float` | — |
| `pa.float64()` | `float` | `float` | — |
| `pa.decimal128(10, 2)` | `str` (fixed-point decimal) | `decimal` | — |
| `pa.decimal128(2, 2)` | `str` (fixed-point decimal) | `decimal` | — |
| `pa.decimal128(10, 0)` | `str` (fixed-point integer decimal) | `decimal` | — |
| `pa.decimal256(50, 10)` | `str` (fixed-point decimal) | `decimal` | — |
| `pa.string()` | `str` | `string` | — |
| `pa.large_string()` | `str` | `string` | — |
| `pa.string_view()` | `str` | `string` | — |
| `pa.binary()` | `str` (Base64) | `string` | — |
| `pa.large_binary()` | `str` (Base64) | `string` | — |
| `pa.binary_view()` | `str` (Base64) | `string` | — |
| `pa.binary(16)` | `str` (Base64) | `string` | — |
| `pa.date32()` | `str` (ISO-8601 date) | `date` | — |
| `pa.date64()` | `str` (ISO-8601 date) | `date` | — |
| `pa.time32("s")` | `str` (ISO-8601 time) | `time` | — |
| `pa.time32("ms")` | `str` (ISO-8601 time) | `time` | — |
| `pa.time64("us")` | `str` (ISO-8601 time) | `time` | — |
| `pa.time64("ns")` | `str` (ISO-8601 time) | `time` | — |
| `pa.timestamp("s", tz="UTC")` | `str` (ISO-8601 date-time) | `datetime` | — |
| `pa.timestamp("us", tz="UTC")` | `str` (ISO-8601 date-time) | `datetime` | — |
| `pa.timestamp("ns", tz="America/Los_Angeles")` | `str` (ISO-8601 date-time) | `datetime` | — |
| `pa.timestamp("us")` | `str` (naive ISO-8601 timestamp) | `datetime` | — |
| `pa.timestamp("ns")` | `str` (naive ISO-8601 timestamp) | `datetime` | — |
| `pa.dictionary(pa.int16(), pa.string())` | `str` | `string` | — |
| `pa.dictionary(pa.int8(), pa.int64(), ordered=True)` | `int` | `integer` | `-9223372036854775808` .. `9223372036854775807` |

### 1.6 Schema and Slot Descriptions

<a id="r-meta-schema-desc"></a>

- **R-META-SCHEMA-DESC**: Non-empty UTF-8 `schema.metadata[b"comment"]` on a `pa.Schema` is emitted as
  `SchemaDefinition.description` (and as the root `ClassDefinition.description` when appended into an
  existing schema via `destination`).

  *Cases:* [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-meta-slot-desc"></a>

- **R-META-SLOT-DESC**: Non-empty UTF-8 `pa.Field.metadata[b"comment"]` on a root or struct field is emitted as
  `SlotDefinition.description` (falling back to inner list element `comment` when the outer field has no
  `comment`).

  *Cases:* [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

### 1.7 Arrow Table and Record Serialization

<a id="r-ser-row-order"></a>

- **R-SER-ROW-ORDER**: `serialize_table_for_linkml` and `serialize_record_for_linkml` emit one JSON-compatible dictionary per
  row with keys in `pa.Schema` declaration order; null Arrow scalars on optional slots serialize as
  `null` (`None`), structs serialize as nested dicts, lists serialize as JSON arrays, and dictionary
  scalars unwrap to their decoded value.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans), [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [03_temporal_types](#case-spec-03-temporal-types), [04_nullability_and_descriptions](#case-spec-04-nullability-and-descriptions), [05_nested_structs_and_lists](#case-spec-05-nested-structs-and-lists), [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-decimal-str"></a>

- **R-SER-DECIMAL-STR**: `decimal128` and `decimal256` scalars serialize via plain fixed-point formatting (`format(value, "f")`,
  never scientific notation) matching LinkML's `decimal` scalar type.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [checkm2_quality_report](#case-examples-checkm2-quality-report), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary)

<a id="r-ser-base64"></a>

- **R-SER-BASE64**: `binary`, `large_binary`, `binary_view`, and `fixed_size_binary(n)` scalars serialize to ASCII Base64
  strings.

  *Cases:* [02_strings_binary_and_decimals](#case-spec-02-strings-binary-and-decimals), [07_dictionaries_and_views](#case-spec-07-dictionaries-and-views), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-temporal-iso"></a>

- **R-SER-TEMPORAL-ISO**: `date32` and `date64` serialize as `YYYY-MM-DD`; `time32` and `time64` serialize as
  `HH:MM:SS[.fffffffff]`; timezone-aware and timezone-naive `timestamp` scalars serialize via ISO-8601
  `isoformat()`.

  *Cases:* [03_temporal_types](#case-spec-03-temporal-types), [ncbi_assembly_summary](#case-examples-ncbi-assembly-summary), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-map-entries"></a>

- **R-SER-MAP-ENTRIES**: `pa.map_` scalars serialize as `[{"key": <serialized key>, "value": <serialized value>}, ...]`
  matching the generated `<OwnerClass>_<field_name>_map` entry class attributes.

  *Cases:* [06_maps_and_entry_classes](#case-spec-06-maps-and-entry-classes), [08_identifier_sanitization_and_collisions](#case-spec-08-identifier-sanitization-and-collisions), [checkm2_quality_report](#case-examples-checkm2-quality-report), [uniref_cluster_entry](#case-examples-uniref-cluster-entry)

<a id="r-ser-finite-float"></a>

- **R-SER-FINITE-FLOAT**: Finite `float16`, `float32`, and `float64` scalars serialize as JSON `float` numbers; non-finite
  `NaN` or `±Infinity` values raise `ValueError`.

  *Cases:* [01_integers_floats_and_booleans](#case-spec-01-integers-floats-and-booleans)

## 2. Cases

<a id="case-spec-01-integers-floats-and-booleans"></a>

### Booleans, Signed and Unsigned Integers, and IEEE-754 Floats (`spec/01_integers_floats_and_booleans`)

**Rules:** [R-SCHEMA-INPUT](#r-schema-input), [R-NON-EMPTY-ROOT](#r-non-empty-root), [R-SCHEMA-IDENTITY](#r-schema-identity), [R-IMPORTS-PREFIXES](#r-imports-prefixes), [R-SLOT-NAME](#r-slot-name), [R-SLOT-ORDER](#r-slot-order), [R-UNIQUE-FIELD-NAMES](#r-unique-field-names), [R-NULL-REQUIRED](#r-null-required), [R-SCALAR-BOOL](#r-scalar-bool), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-FLOAT](#r-scalar-float), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-FINITE-FLOAT](#r-ser-finite-float)

Exercises default unnamed schema identity (`name: schema`, `id: urn:linkml:schema`, root class `Root`),
two's-complement `minimum_value`/`maximum_value` bounds on `int8`..`int64`, unsigned `0..2^w - 1` bounds on
`uint8`..`uint64`, `boolean`, and `float` ranges across `float16`/`float32`/`float64`.

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

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/01_integers_floats_and_booleans/expected_schema.yaml))

```yaml
name: schema
id: urn:linkml:schema
imports:
- linkml:types
prefixes:
  schema:
    prefix_prefix: schema
    prefix_reference: urn:linkml:schema#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: schema
classes:
  Root:
    name: Root
    attributes:
      flag:
        name: flag
        range: boolean
        required: true
      i8:
        name: i8
        range: integer
        required: true
        minimum_value: -128
        maximum_value: 127
      i16:
        name: i16
        range: integer
        required: true
        minimum_value: -32768
        maximum_value: 32767
      i32:
        name: i32
        range: integer
        required: true
        minimum_value: -2147483648
        maximum_value: 2147483647
      i64:
        name: i64
        range: integer
        required: true
        minimum_value: -9223372036854775808
        maximum_value: 9223372036854775807
      u8:
        name: u8
        range: integer
        required: true
        minimum_value: 0
        maximum_value: 255
      u16:
        name: u16
        range: integer
        required: true
        minimum_value: 0
        maximum_value: 65535
      u32:
        name: u32
        range: integer
        required: true
        minimum_value: 0
        maximum_value: 4294967295
      u64:
        name: u64
        range: integer
        required: true
        minimum_value: 0
        maximum_value: 18446744073709551615
      f16:
        name: f16
        range: float
        required: true
      f32:
        name: f32
        range: float
        required: true
      f64:
        name: f64
        range: float
        required: true
```

---

<a id="case-spec-02-strings-binary-and-decimals"></a>

### Strings, Base64 Binary Columns, and Fixed-Point Decimals (`spec/02_strings_binary_and_decimals`)

**Rules:** [R-SCALAR-STRING-BINARY](#r-scalar-string-binary), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-BASE64](#r-ser-base64)

Verifies that `string`, `large_string`, `binary`, `large_binary`, and `fixed_size_binary` map to `range: string`
while `decimal128` and `decimal256` map to `range: decimal` with plain fixed-point string serialization.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 02_strings_binary_and_decimals."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("utf8_str", pa.string(), nullable=False),
        pa.field("large_str", pa.large_string(), nullable=False),
        pa.field("raw_bin", pa.binary(), nullable=False),
        pa.field("large_bin", pa.large_binary(), nullable=False),
        pa.field("fixed_bin_4", pa.binary(4), nullable=False),
        pa.field("dec_standard", pa.decimal128(10, 2), nullable=False),
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
            "raw_bin": b"\x00\x01\x02\xff",
            "large_bin": b"kbase-large-payload",
            "fixed_bin_4": b"\xde\xad\xbe\xef",
            "dec_standard": Decimal("-12345678.95"),
            "dec_integer_only": Decimal(98765432),
            "dec256_high_prec": Decimal("0.0000000001"),
        },
        {
            "utf8_str": "",
            "large_str": "second-large-string",
            "raw_bin": b"",
            "large_bin": b"\x89PNG",
            "fixed_bin_4": b"\x00\x00\x00\x01",
            "dec_standard": Decimal("0.00"),
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
    "raw_bin": "AAEC/w==",
    "large_bin": "a2Jhc2UtbGFyZ2UtcGF5bG9hZA==",
    "fixed_bin_4": "3q2+7w==",
    "dec_standard": "-12345678.95",
    "dec_integer_only": "98765432",
    "dec256_high_prec": "0.0000000001"
  },
  {
    "utf8_str": "",
    "large_str": "second-large-string",
    "raw_bin": "",
    "large_bin": "iVBORw==",
    "fixed_bin_4": "AAAAAQ==",
    "dec_standard": "0.00",
    "dec_integer_only": "-42",
    "dec256_high_prec": null
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/02_strings_binary_and_decimals/expected_schema.yaml))

```yaml
name: schema
id: urn:linkml:schema
imports:
- linkml:types
prefixes:
  schema:
    prefix_prefix: schema
    prefix_reference: urn:linkml:schema#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: schema
classes:
  Root:
    name: Root
    attributes:
      utf8_str:
        name: utf8_str
        range: string
        required: true
      large_str:
        name: large_str
        range: string
        required: true
      raw_bin:
        name: raw_bin
        range: string
        required: true
      large_bin:
        name: large_bin
        range: string
        required: true
      fixed_bin_4:
        name: fixed_bin_4
        range: string
        required: true
      dec_standard:
        name: dec_standard
        range: decimal
        required: true
      dec_integer_only:
        name: dec_integer_only
        range: decimal
        required: true
      dec256_high_prec:
        name: dec256_high_prec
        range: decimal
```

---

<a id="case-spec-03-temporal-types"></a>

### Dates, Times, and Timezone-Aware and Timezone-Naive Timestamps (`spec/03_temporal_types`)

**Rules:** [R-SCALAR-TEMPORAL](#r-scalar-temporal), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-TEMPORAL-ISO](#r-ser-temporal-iso)

Maps `date32`/`date64` to `range: date`, `time32`/`time64` to `range: time`, and both timezone-aware and
timezone-naive `timestamp` types across units (`s`, `ms`, `us`, `ns`) to `range: datetime`.

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
        "t64_ns": pa.array([52_200_123_456_000, 0], type=pa.time64("ns")),
        "ts_s_utc": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, tzinfo=dt.UTC),
                dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("s", tz="UTC"),
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
    "t64_ns": "14:30:00.123456",
    "ts_s_utc": "2026-04-10T14:30:00+00:00",
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
    "ts_us_la": "2025-01-15T10:00:00-08:00",
    "ts_ns_utc": "2025-06-01T06:00:00+00:00",
    "ts_naive_us": "2025-12-31T23:59:59",
    "ts_naive_ns_opt": null
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/03_temporal_types/expected_schema.yaml))

```yaml
name: schema
id: urn:linkml:schema
imports:
- linkml:types
prefixes:
  schema:
    prefix_prefix: schema
    prefix_reference: urn:linkml:schema#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: schema
classes:
  Root:
    name: Root
    attributes:
      d32:
        name: d32
        range: date
        required: true
      d64:
        name: d64
        range: date
        required: true
      t32_s:
        name: t32_s
        range: time
        required: true
      t32_ms:
        name: t32_ms
        range: time
        required: true
      t64_us:
        name: t64_us
        range: time
        required: true
      t64_ns:
        name: t64_ns
        range: time
        required: true
      ts_s_utc:
        name: ts_s_utc
        range: datetime
        required: true
      ts_us_la:
        name: ts_us_la
        range: datetime
        required: true
      ts_ns_utc:
        name: ts_ns_utc
        range: datetime
        required: true
      ts_naive_us:
        name: ts_naive_us
        range: datetime
        required: true
      ts_naive_ns_opt:
        name: ts_naive_ns_opt
        range: datetime
```

---

<a id="case-spec-04-nullability-and-descriptions"></a>

### Nullability, Schema Identity Metadata, and Slot Descriptions (`spec/04_nullability_and_descriptions`)

**Rules:** [R-SCHEMA-IDENTITY](#r-schema-identity), [R-IMPORTS-PREFIXES](#r-imports-prefixes), [R-SLOT-NAME](#r-slot-name), [R-SLOT-ORDER](#r-slot-order), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-META-SCHEMA-DESC](#r-meta-schema-desc), [R-META-SLOT-DESC](#r-meta-slot-desc), [R-SER-ROW-ORDER](#r-ser-row-order)

Demonstrates resolution of `SchemaDefinition.name`, root `ClassDefinition.name`, `id`, `prefixes`, and
`description` from `pa.Schema.metadata` (`title`, `$id`, `comment`), alongside required vs optional slots and
slot-level `description` extraction (including fallback to list item `comment`).

#### Schema (`schema.py`)

```python
"""PyArrow schema for 04_nullability_and_descriptions."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("assay_id", pa.string(), nullable=False, metadata={"comment": "Primary assay identifier."}),
        pa.field("od600", pa.float64(), nullable=True, metadata={"comment": "Optical density at 600 nm."}),
        pa.field("replicate_num", pa.int16(), nullable=False),
        pa.field("notes", pa.string(), nullable=True),
        pa.field(
            "reagents",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"comment": "Reagent catalog code."})),
            nullable=True,
        ),
    ],
    metadata={
        "title": "AssayMeasurement",
        "$id": "https://kbase.us/linkml/assay_measurement",
        "comment": "High-throughput growth assay measurement records.",
    },
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 04_nullability_and_descriptions."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "assay_id": "ASY-1001",
            "od600": 0.825,
            "replicate_num": 1,
            "notes": "Exponential phase",
            "reagents": ["R-10", "R-22"],
        },
        {
            "assay_id": "ASY-1002",
            "od600": None,
            "replicate_num": 2,
            "notes": None,
            "reagents": None,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "assay_id": "ASY-1001",
    "od600": 0.825,
    "replicate_num": 1,
    "notes": "Exponential phase",
    "reagents": [
      "R-10",
      "R-22"
    ]
  },
  {
    "assay_id": "ASY-1002",
    "od600": null,
    "replicate_num": 2,
    "notes": null,
    "reagents": null
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/04_nullability_and_descriptions/expected_schema.yaml))

```yaml
name: AssayMeasurement
description: High-throughput growth assay measurement records.
id: https://kbase.us/linkml/assay_measurement
imports:
- linkml:types
prefixes:
  AssayMeasurement:
    prefix_prefix: AssayMeasurement
    prefix_reference: https://kbase.us/linkml/assay_measurement#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: AssayMeasurement
classes:
  AssayMeasurement:
    name: AssayMeasurement
    attributes:
      assay_id:
        name: assay_id
        description: Primary assay identifier.
        range: string
        required: true
      od600:
        name: od600
        description: Optical density at 600 nm.
        range: float
      replicate_num:
        name: replicate_num
        range: integer
        required: true
        minimum_value: -32768
        maximum_value: 32767
      notes:
        name: notes
        range: string
      reagents:
        name: reagents
        range: string
        multivalued: true
```

---

<a id="case-spec-05-nested-structs-and-lists"></a>

### Nested Struct Classes, Multivalued Lists, and Fixed-Size List Cardinality (`spec/05_nested_structs_and_lists`)

**Rules:** [R-SHAPE-STRUCT-CLASS](#r-shape-struct-class), [R-SHAPE-LIST-SLOT](#r-shape-list-slot), [R-SHAPE-FIXED-LIST-CARDINALITY](#r-shape-fixed-list-cardinality), [R-REJECT-NESTED-MULTIVALUED](#r-reject-nested-multivalued), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-LIST-NON-NULL-ITEMS](#r-list-non-null-items), [R-SER-ROW-ORDER](#r-ser-row-order)

Demonstrates derived `<OwnerClass>_<field_name>` classes for nested `pa.struct` columns and struct-typed list
items (including a nested `list<string>` inside a struct element class), omission of scalar numeric bounds on
multivalued list slots for LinkML Pydantic compatibility, and `minimum_cardinality` / `maximum_cardinality`
on `pa.FixedSizeListType`.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 05_nested_structs_and_lists."""

import pyarrow as pa

DOMAIN_HIT_STRUCT = pa.struct(
    [
        pa.field("domain_acc", pa.string(), nullable=False),
        pa.field("start_aa", pa.uint32(), nullable=False),
        pa.field("end_aa", pa.uint32(), nullable=False),
        pa.field("evidence_codes", pa.list_(pa.field("item", pa.string(), nullable=False)), nullable=False),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("protein_id", pa.string(), nullable=False),
        pa.field(
            "source_locus",
            pa.struct(
                [
                    pa.field("contig", pa.string(), nullable=False),
                    pa.field("strand", pa.string(), nullable=True),
                ]
            ),
            nullable=False,
        ),
        pa.field("ec_numbers", pa.list_(pa.field("item", pa.string(), nullable=False)), nullable=False),
        pa.field("quality_flags", pa.large_list(pa.field("item", pa.uint8(), nullable=False)), nullable=True),
        pa.field("coords_3d", pa.list_(pa.field("item", pa.float32(), nullable=False), 3), nullable=False),
        pa.field("domain_hits", pa.list_(pa.field("item", DOMAIN_HIT_STRUCT, nullable=False)), nullable=False),
    ],
    metadata={"title": "ProteinAnnotation"},
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 05_nested_structs_and_lists."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "protein_id": "WP_000001.1",
            "source_locus": {"contig": "NC_000913.3", "strand": "+"},
            "ec_numbers": ["2.7.7.7", "3.1.11.2"],
            "quality_flags": [1, 4, 255],
            "coords_3d": [1.25, -0.5, 4.0],
            "domain_hits": [
                {
                    "domain_acc": "PF00476",
                    "start_aa": 12,
                    "end_aa": 298,
                    "evidence_codes": ["ECO:0000259", "ECO:0000313"],
                }
            ],
        },
        {
            "protein_id": "WP_000002.1",
            "source_locus": {"contig": "NC_000913.3", "strand": None},
            "ec_numbers": [],
            "quality_flags": None,
            "coords_3d": [0.0, 0.0, 0.0],
            "domain_hits": [],
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "protein_id": "WP_000001.1",
    "source_locus": {
      "contig": "NC_000913.3",
      "strand": "+"
    },
    "ec_numbers": [
      "2.7.7.7",
      "3.1.11.2"
    ],
    "quality_flags": [
      1,
      4,
      255
    ],
    "coords_3d": [
      1.25,
      -0.5,
      4.0
    ],
    "domain_hits": [
      {
        "domain_acc": "PF00476",
        "start_aa": 12,
        "end_aa": 298,
        "evidence_codes": [
          "ECO:0000259",
          "ECO:0000313"
        ]
      }
    ]
  },
  {
    "protein_id": "WP_000002.1",
    "source_locus": {
      "contig": "NC_000913.3",
      "strand": null
    },
    "ec_numbers": [],
    "quality_flags": null,
    "coords_3d": [
      0.0,
      0.0,
      0.0
    ],
    "domain_hits": []
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/05_nested_structs_and_lists/expected_schema.yaml))

```yaml
name: ProteinAnnotation
id: urn:linkml:ProteinAnnotation
imports:
- linkml:types
prefixes:
  ProteinAnnotation:
    prefix_prefix: ProteinAnnotation
    prefix_reference: urn:linkml:ProteinAnnotation#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: ProteinAnnotation
classes:
  ProteinAnnotation_source_locus:
    name: ProteinAnnotation_source_locus
    attributes:
      contig:
        name: contig
        range: string
        required: true
      strand:
        name: strand
        range: string
  ProteinAnnotation_domain_hits:
    name: ProteinAnnotation_domain_hits
    attributes:
      domain_acc:
        name: domain_acc
        range: string
        required: true
      start_aa:
        name: start_aa
        range: integer
        required: true
        minimum_value: 0
        maximum_value: 4294967295
      end_aa:
        name: end_aa
        range: integer
        required: true
        minimum_value: 0
        maximum_value: 4294967295
      evidence_codes:
        name: evidence_codes
        range: string
        required: true
        multivalued: true
  ProteinAnnotation:
    name: ProteinAnnotation
    attributes:
      protein_id:
        name: protein_id
        range: string
        required: true
      source_locus:
        name: source_locus
        range: ProteinAnnotation_source_locus
        required: true
      ec_numbers:
        name: ec_numbers
        range: string
        required: true
        multivalued: true
      quality_flags:
        name: quality_flags
        range: integer
        multivalued: true
      coords_3d:
        name: coords_3d
        range: float
        required: true
        multivalued: true
        minimum_cardinality: 3
        maximum_cardinality: 3
      domain_hits:
        name: domain_hits
        range: ProteinAnnotation_domain_hits
        required: true
        multivalued: true
```

---

<a id="case-spec-06-maps-and-entry-classes"></a>

### Map Entry Classes with Required vs Optional Values and Non-String Keys (`spec/06_maps_and_entry_classes`)

**Rules:** [R-SHAPE-MAP-CLASS](#r-shape-map-class), [R-SHAPE-STRUCT-CLASS](#r-shape-struct-class), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-NULL-MAP-VALUE](#r-null-map-value), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-MAP-ENTRIES](#r-ser-map-entries)

Verifies that `pa.map_` synthesizes `<OwnerClass>_<field_name>_map` entry classes where `key` is always
`required: true`, `value` carries `required: true` iff `item_field.nullable == False`, and struct map values
produce a nested `<OwnerClass>_<field_name>_map_value` class.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 06_maps_and_entry_classes."""

import pyarrow as pa

HIT_DETAIL_STRUCT = pa.struct(
    [
        pa.field("score", pa.float64(), nullable=False),
        pa.field("note", pa.string(), nullable=True),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field(
            "read_counts",
            pa.map_(pa.string(), pa.field("value", pa.int64(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "taxid_hits",
            pa.map_(pa.int32(), pa.field("value", HIT_DETAIL_STRUCT, nullable=True), keys_sorted=True),
            nullable=True,
        ),
    ],
    metadata={"title": "SampleReadMap"},
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 06_maps_and_entry_classes."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "sample_id": "SMP-01",
            "read_counts": [("raw", 150000), ("trimmed", 142500)],
            "taxid_hits": [
                (562, {"score": 99.5, "note": "Escherichia coli"}),
                (1280, None),
            ],
        },
        {
            "sample_id": "SMP-02",
            "read_counts": [],
            "taxid_hits": None,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "sample_id": "SMP-01",
    "read_counts": [
      {
        "key": "raw",
        "value": 150000
      },
      {
        "key": "trimmed",
        "value": 142500
      }
    ],
    "taxid_hits": [
      {
        "key": 562,
        "value": {
          "score": 99.5,
          "note": "Escherichia coli"
        }
      },
      {
        "key": 1280,
        "value": null
      }
    ]
  },
  {
    "sample_id": "SMP-02",
    "read_counts": [],
    "taxid_hits": null
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/06_maps_and_entry_classes/expected_schema.yaml))

```yaml
name: SampleReadMap
id: urn:linkml:SampleReadMap
imports:
- linkml:types
prefixes:
  SampleReadMap:
    prefix_prefix: SampleReadMap
    prefix_reference: urn:linkml:SampleReadMap#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: SampleReadMap
classes:
  SampleReadMap_read_counts_map:
    name: SampleReadMap_read_counts_map
    attributes:
      key:
        name: key
        range: string
        required: true
      value:
        name: value
        range: integer
        required: true
        minimum_value: -9223372036854775808
        maximum_value: 9223372036854775807
  SampleReadMap_taxid_hits_map_value:
    name: SampleReadMap_taxid_hits_map_value
    attributes:
      score:
        name: score
        range: float
        required: true
      note:
        name: note
        range: string
  SampleReadMap_taxid_hits_map:
    name: SampleReadMap_taxid_hits_map
    attributes:
      key:
        name: key
        range: integer
        required: true
        minimum_value: -2147483648
        maximum_value: 2147483647
      value:
        name: value
        range: SampleReadMap_taxid_hits_map_value
  SampleReadMap:
    name: SampleReadMap
    attributes:
      sample_id:
        name: sample_id
        range: string
        required: true
      read_counts:
        name: read_counts
        range: SampleReadMap_read_counts_map
        required: true
        multivalued: true
      taxid_hits:
        name: taxid_hits
        range: SampleReadMap_taxid_hits_map
        multivalued: true
```

---

<a id="case-spec-07-dictionaries-and-views"></a>

### Dictionary-Encoded Columns and String/Binary/List View Variants (`spec/07_dictionaries_and_views`)

**Rules:** [R-SHAPE-DICTIONARY](#r-shape-dictionary), [R-SHAPE-LIST-SLOT](#r-shape-list-slot), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-SCALAR-STRING-BINARY](#r-scalar-string-binary), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-BASE64](#r-ser-base64)

Verifies that `pa.dictionary` unwraps to the underlying value range and integer bounds (`uint16` ->
`0..65535`), and that Arrow view types (`string_view`, `binary_view`, `list_view`, `large_list_view`) map to
standard LinkML string and multivalued slots.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 07_dictionaries_and_views."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string_view(), nullable=False),
        pa.field("payload_view", pa.binary_view(), nullable=False),
        pa.field("category", pa.dictionary(pa.int16(), pa.string()), nullable=False),
        pa.field("rank_code", pa.dictionary(pa.int8(), pa.uint16(), ordered=True), nullable=True),
        pa.field("weight_tier", pa.dictionary(pa.int8(), pa.decimal128(6, 2)), nullable=False),
        pa.field("tags_view", pa.list_view(pa.field("item", pa.string(), nullable=False)), nullable=False),
        pa.field("scores_large_view", pa.large_list_view(pa.field("item", pa.uint8(), nullable=False)), nullable=True),
    ],
    metadata={"title": "CatalogViewRecord"},
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 07_dictionaries_and_views."""

from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_arrays(
    [
        pa.array(["VIEW-01", "VIEW-02"], type=pa.string_view()),
        pa.array([b"\xca\xfe\xba\xbe", b"plain-bytes"], type=pa.binary_view()),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int16()),
            pa.array(["metagenome", "isolate"], type=pa.string()),
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, None], type=pa.int8()),
            pa.array([1024], type=pa.uint16()),
            ordered=True,
        ),
        pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int8()),
            pa.array([Decimal("15.25"), Decimal("1.00")], type=pa.decimal128(6, 2)),
        ),
        pa.array([["marine", "pelagic"], []], type=pa.list_view(pa.field("item", pa.string(), nullable=False))),
        pa.array([[10, 200], None], type=pa.large_list_view(pa.field("item", pa.uint8(), nullable=False))),
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `default`

```json
[
  {
    "id": "VIEW-01",
    "payload_view": "yv66vg==",
    "category": "metagenome",
    "rank_code": 1024,
    "weight_tier": "15.25",
    "tags_view": [
      "marine",
      "pelagic"
    ],
    "scores_large_view": [
      10,
      200
    ]
  },
  {
    "id": "VIEW-02",
    "payload_view": "cGxhaW4tYnl0ZXM=",
    "category": "isolate",
    "rank_code": null,
    "weight_tier": "1.00",
    "tags_view": [],
    "scores_large_view": null
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/07_dictionaries_and_views/expected_schema.yaml))

```yaml
name: CatalogViewRecord
id: urn:linkml:CatalogViewRecord
imports:
- linkml:types
prefixes:
  CatalogViewRecord:
    prefix_prefix: CatalogViewRecord
    prefix_reference: urn:linkml:CatalogViewRecord#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: CatalogViewRecord
classes:
  CatalogViewRecord:
    name: CatalogViewRecord
    attributes:
      id:
        name: id
        range: string
        required: true
      payload_view:
        name: payload_view
        range: string
        required: true
      category:
        name: category
        range: string
        required: true
      rank_code:
        name: rank_code
        range: integer
        minimum_value: 0
        maximum_value: 65535
      weight_tier:
        name: weight_tier
        range: decimal
        required: true
      tags_view:
        name: tags_view
        range: string
        required: true
        multivalued: true
      scores_large_view:
        name: scores_large_view
        range: integer
        multivalued: true
```

---

<a id="case-spec-08-identifier-sanitization-and-collisions"></a>

### Identifier Sanitization, Explicit Overrides, and Class Name Collision Disambiguation (`spec/08_identifier_sanitization_and_collisions`)

**Rules:** [R-SCHEMA-IDENTITY](#r-schema-identity), [R-IMPORTS-PREFIXES](#r-imports-prefixes), [R-CLASS-IDENTIFIER](#r-class-identifier), [R-CLASS-COLLISION](#r-class-collision), [R-SHAPE-STRUCT-CLASS](#r-shape-struct-class), [R-SHAPE-MAP-CLASS](#r-shape-map-class), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-MAP-ENTRIES](#r-ser-map-entries)

Exercises explicit `schema_name` and `schema_id` overrides with punctuation sanitization
(`"--genome-report.v1--"` -> `genome_report_v1`), plus deterministic `_2` suffix disambiguation when
`<OwnerClass>_meta_info` collides with `<OwnerClass>_meta`'s child `info` struct.

#### Schema (`schema.py`)

```python
"""PyArrow schema for 08_identifier_sanitization_and_collisions."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "meta_info",
            pa.struct([pa.field("source", pa.string(), nullable=False)]),
            nullable=False,
        ),
        pa.field(
            "meta",
            pa.struct(
                [
                    pa.field(
                        "info",
                        pa.struct([pa.field("version", pa.int32(), nullable=False)]),
                        nullable=False,
                    )
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "labels",
            pa.map_(pa.string(), pa.field("value", pa.string(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "labels_map",
            pa.struct([pa.field("note", pa.string(), nullable=True)]),
            nullable=True,
        ),
    ],
    metadata={
        "title": "OverriddenTitle",
        "$id": "urn:overridden",
    },
)
```

#### Input (`table.py`)

```python
"""Sample PyArrow table for 08_identifier_sanitization_and_collisions."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "meta_info": {"source": "IMG/M"},
            "meta": {"info": {"version": 4}},
            "labels": [("env", "soil")],
            "labels_map": {"note": "collides with labels map class"},
        },
        {
            "meta_info": {"source": "GTDB"},
            "meta": {"info": {"version": 220}},
            "labels": [],
            "labels_map": None,
        },
    ],
    schema=SCHEMA,
)
```

#### Serialized records: run `sanitized_override` (`schema_name=--genome-report.v1--`, `schema_id=https://kbase.us/linkml/genome-report-v1`)

```json
[
  {
    "meta_info": {
      "source": "IMG/M"
    },
    "meta": {
      "info": {
        "version": 4
      }
    },
    "labels": [
      {
        "key": "env",
        "value": "soil"
      }
    ],
    "labels_map": {
      "note": "collides with labels map class"
    }
  },
  {
    "meta_info": {
      "source": "GTDB"
    },
    "meta": {
      "info": {
        "version": 220
      }
    },
    "labels": [],
    "labels_map": null
  }
]
```

#### LinkML schema ([`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/08_identifier_sanitization_and_collisions/expected_schema.yaml))

```yaml
name: genome_report_v1
id: https://kbase.us/linkml/genome-report-v1
imports:
- linkml:types
prefixes:
  genome_report_v1:
    prefix_prefix: genome_report_v1
    prefix_reference: https://kbase.us/linkml/genome-report-v1#
  linkml:
    prefix_prefix: linkml
    prefix_reference: https://w3id.org/linkml/
default_prefix: genome_report_v1
classes:
  genome_report_v1_meta_info:
    name: genome_report_v1_meta_info
    attributes:
      source:
        name: source
        range: string
        required: true
  genome_report_v1_meta_info_2:
    name: genome_report_v1_meta_info_2
    attributes:
      version:
        name: version
        range: integer
        required: true
        minimum_value: -2147483648
        maximum_value: 2147483647
  genome_report_v1_meta:
    name: genome_report_v1_meta
    attributes:
      info:
        name: info
        range: genome_report_v1_meta_info_2
        required: true
  genome_report_v1_labels_map:
    name: genome_report_v1_labels_map
    attributes:
      key:
        name: key
        range: string
        required: true
      value:
        name: value
        range: string
        required: true
  genome_report_v1_labels_map_2:
    name: genome_report_v1_labels_map_2
    attributes:
      note:
        name: note
        range: string
  genome_report_v1:
    name: genome_report_v1
    attributes:
      meta_info:
        name: meta_info
        range: genome_report_v1_meta_info
        required: true
      meta:
        name: meta
        range: genome_report_v1_meta
        required: true
      labels:
        name: labels
        range: genome_report_v1_labels_map
        required: true
        multivalued: true
      labels_map:
        name: labels_map
        range: genome_report_v1_labels_map_2
```

---

<a id="case-examples-checkm2-quality-report"></a>

### CheckM2 Genome Quality and Contig Statistics Table (`examples/checkm2_quality_report`)

**Rules:** [R-SCHEMA-INPUT](#r-schema-input), [R-SCHEMA-IDENTITY](#r-schema-identity), [R-IMPORTS-PREFIXES](#r-imports-prefixes), [R-SLOT-NAME](#r-slot-name), [R-SLOT-ORDER](#r-slot-order), [R-SHAPE-STRUCT-CLASS](#r-shape-struct-class), [R-SHAPE-LIST-SLOT](#r-shape-list-slot), [R-SHAPE-MAP-CLASS](#r-shape-map-class), [R-SHAPE-DICTIONARY](#r-shape-dictionary), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-NULL-MAP-VALUE](#r-null-map-value), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-FLOAT](#r-scalar-float), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-SCALAR-STRING-BINARY](#r-scalar-string-binary), [R-META-SCHEMA-DESC](#r-meta-schema-desc), [R-META-SLOT-DESC](#r-meta-slot-desc), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-MAP-ENTRIES](#r-ser-map-entries)

**Schema:** [`specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/schema.py)

- Run `default`: [`table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/table.py), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/expected.json)

**Expected LinkML schema:** [`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/expected_schema.yaml)

---

<a id="case-examples-ncbi-assembly-summary"></a>

### NCBI Datasets Assembly Summary Table (`examples/ncbi_assembly_summary`)

**Rules:** [R-SCHEMA-IDENTITY](#r-schema-identity), [R-IMPORTS-PREFIXES](#r-imports-prefixes), [R-SHAPE-STRUCT-CLASS](#r-shape-struct-class), [R-SHAPE-LIST-SLOT](#r-shape-list-slot), [R-SHAPE-DICTIONARY](#r-shape-dictionary), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-BOOL](#r-scalar-bool), [R-SCALAR-SIGNED-INT](#r-scalar-signed-int), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-SCALAR-STRING-BINARY](#r-scalar-string-binary), [R-SCALAR-TEMPORAL](#r-scalar-temporal), [R-META-SCHEMA-DESC](#r-meta-schema-desc), [R-META-SLOT-DESC](#r-meta-slot-desc), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-DECIMAL-STR](#r-ser-decimal-str), [R-SER-BASE64](#r-ser-base64), [R-SER-TEMPORAL-ISO](#r-ser-temporal-iso)

**Schema:** [`specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/schema.py)

- Run `default`: [`table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/table.py), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/expected.json)

**Expected LinkML schema:** [`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/expected_schema.yaml)

---

<a id="case-examples-uniref-cluster-entry"></a>

### UniRef Sequence Cluster Inventory Table (`examples/uniref_cluster_entry`)

**Rules:** [R-SCHEMA-IDENTITY](#r-schema-identity), [R-IMPORTS-PREFIXES](#r-imports-prefixes), [R-SHAPE-STRUCT-CLASS](#r-shape-struct-class), [R-SHAPE-LIST-SLOT](#r-shape-list-slot), [R-SHAPE-FIXED-LIST-CARDINALITY](#r-shape-fixed-list-cardinality), [R-SHAPE-MAP-CLASS](#r-shape-map-class), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-NULL-MAP-VALUE](#r-null-map-value), [R-SCALAR-UNSIGNED-INT](#r-scalar-unsigned-int), [R-SCALAR-FLOAT](#r-scalar-float), [R-SCALAR-STRING-BINARY](#r-scalar-string-binary), [R-SCALAR-TEMPORAL](#r-scalar-temporal), [R-META-SCHEMA-DESC](#r-meta-schema-desc), [R-META-SLOT-DESC](#r-meta-slot-desc), [R-SER-ROW-ORDER](#r-ser-row-order), [R-SER-BASE64](#r-ser-base64), [R-SER-TEMPORAL-ISO](#r-ser-temporal-iso), [R-SER-MAP-ENTRIES](#r-ser-map-entries)

**Schema:** [`specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/schema.py)

- Run `default`: [`table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/table.py), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/expected.json)

**Expected LinkML schema:** [`expected_schema.yaml`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/expected_schema.yaml)

---

## 3. Errors (Unsupported / Invalid Input)

Schema generation raises a subclass of `PyArrowToLinkMLError` (itself a `ValueError`).

| Code | Exception | Condition | Reason | Fixtures |
| :--- | :--- | :--- | :--- | :--- |
| **E1** | `InvalidPyArrowSchemaError` | Input is not a `pa.Schema` instance | Schema generation requires a concrete PyArrow `pa.Schema` object; bare `pa.DataType`, `pa.Field`, or Python mappings are not table schemas. | [`e01_non_schema_input.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e01_non_schema_input.py): Input is a `pa.Field` rather than a `pa.Schema`. |
| **E2** | `EmptyStructError` | Empty root schema or nested `pa.struct([])` (zero fields) | Arrow tables and LinkML record classes generated from Arrow structs require at least one named attribute. | [`e02a_empty_root_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e02a_empty_root_schema.py): Root `pa.schema([])` declares zero fields.<br>[`e02b_empty_nested_struct.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e02b_empty_nested_struct.py): Nested `pa.struct([])` column declares zero child fields. |
| **E3** | `DuplicateFieldNameError` | Duplicate field name in schema or `pa.struct` | LinkML class `attributes` is keyed by slot name and cannot represent duplicate sibling field names. | [`e03a_duplicate_root_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e03a_duplicate_root_field.py): Root `pa.Schema` declares two fields named `'id'`.<br>[`e03b_duplicate_struct_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e03b_duplicate_struct_field.py): Nested `pa.struct` declares duplicate child fields named `'tag'`. |
| **E4** | `NullOnlyTypeError` | Standalone `pa.null()` field or element type | A column or container element typed solely as `pa.null()` carries no value domain; nullable columns must wrap a concrete Arrow value type with `nullable=True`. | [`e04_null_only_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e04_null_only_field.py): Column uses standalone `pa.null()` with no value type. |
| **E5** | `InvalidDecimalTypeError` | Decimal type with negative scale or `scale > precision` | Fixed-point decimal representations require `0 <= scale <= precision`; negative scales or `scale > precision` have no plain decimal slot representation. | [`e05_negative_scale_decimal.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e05_negative_scale_decimal.py): Column uses `pa.decimal128(6, -2)` with negative scale. |
| **E6** | `UnsupportedTemporalIntervalError` | Duration or calendar interval type (`pa.duration`, `pa.month_day_nano_interval`) | Relative durations and `month_day_nano` intervals have no lossless built-in `linkml:types` scalar range. | [`e06a_duration_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e06a_duration_field.py): Column uses `pa.duration("us")`.<br>[`e06b_interval_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e06b_interval_field.py): Column uses `pa.month_day_nano_interval()`. |
| **E7** | `UnsupportedUnionTypeError` | Arrow union type (`pa.sparse_union` or `pa.dense_union`) | Arrow type-code unions cannot be represented as typed LinkML slots without class/union redesign. | [`e07a_sparse_union_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e07a_sparse_union_field.py): Column uses `pa.sparse_union(...)`.<br>[`e07b_dense_union_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e07b_dense_union_field.py): Column uses `pa.dense_union(...)`. |
| **E8** | `UnsupportedArrowTypeError` | Unsupported Arrow physical or extension type (`run_end_encoded`, `ExtensionType`, etc.) | Specialized physical encodings such as `pa.run_end_encoded` and custom `pa.ExtensionType` subclasses have no general LinkML mapping. | [`e08a_run_end_encoded_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e08a_run_end_encoded_field.py): Column uses `pa.run_end_encoded(pa.int32(), pa.string())`.<br>[`e08b_extension_type_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e08b_extension_type_field.py): Column uses Arrow extension type `pa.uuid()`. |
| **E9** | `EmptyFieldNameError` | Blank or empty field name | LinkML `SlotDefinition` identifiers require a non-empty attribute name; blank Arrow field names cannot form valid LinkML slots. | [`e09a_empty_field_name.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e09a_empty_field_name.py): Column declares an empty field name `""`.<br>[`e09b_whitespace_field_name.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e09b_whitespace_field_name.py): Column declares a whitespace-only field name `"   "`. |
| **E10** | `NestedMultivaluedSlotError` | Directly nested list or map inside a list (`list<list<...>>` or `list<map<...>>`) | LinkML slots use a flat `multivalued: true` flag on a single `range` and cannot represent multidimensional lists or lists of map-entry lists without intermediate wrapper classes. | [`e10a_nested_list_in_list.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e10a_nested_list_in_list.py): Column uses a 2D nested list `pa.list_(pa.list_(pa.int64()))`.<br>[`e10b_nested_map_in_list.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/errors/e10b_nested_map_in_list.py): Column uses a list of maps `pa.list_(pa.map_(pa.string(), pa.int64()))`. |

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suite and this document are both driven by it.

| Case | Schema | Runs |
| :--- | :--- | :--- |
| [`spec/01_integers_floats_and_booleans`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/01_integers_floats_and_booleans/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/01_integers_floats_and_booleans/schema.py) | boundary_values |
| [`spec/02_strings_binary_and_decimals`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/02_strings_binary_and_decimals/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/02_strings_binary_and_decimals/schema.py) | default |
| [`spec/03_temporal_types`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/03_temporal_types/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/03_temporal_types/schema.py) | default |
| [`spec/04_nullability_and_descriptions`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/04_nullability_and_descriptions/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/04_nullability_and_descriptions/schema.py) | default |
| [`spec/05_nested_structs_and_lists`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/05_nested_structs_and_lists/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/05_nested_structs_and_lists/schema.py) | default |
| [`spec/06_maps_and_entry_classes`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/06_maps_and_entry_classes/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/06_maps_and_entry_classes/schema.py) | default |
| [`spec/07_dictionaries_and_views`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/07_dictionaries_and_views/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/07_dictionaries_and_views/schema.py) | default |
| [`spec/08_identifier_sanitization_and_collisions`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/08_identifier_sanitization_and_collisions/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/spec/08_identifier_sanitization_and_collisions/schema.py) | sanitized_override |
| [`examples/checkm2_quality_report`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/checkm2_quality_report/schema.py) | default |
| [`examples/ncbi_assembly_summary`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/ncbi_assembly_summary/schema.py) | default |
| [`examples/uniref_cluster_entry`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pyarrow_to_linkml/examples/uniref_cluster_entry/schema.py) | default |
