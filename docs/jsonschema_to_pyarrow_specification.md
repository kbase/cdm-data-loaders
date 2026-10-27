<!-- GENERATED FILE - DO NOT EDIT. Source: `tests/data/jsonschema/rules.yaml`, `tests/data/jsonschema/spec`, `tests/data/jsonschema/examples`. Regenerate with `uv run python -m scripts.gen_converter_spec`. -->

# JSON Schema (Draft 2020-12) to PyArrow Specification

This document specifies how a **JSON Schema Draft 2020-12** document determines:

1. The **PyArrow `pa.Schema`** produced by `generate_pyarrow_schema`.
2. The **Python row dictionaries** produced by `coerce_record_for_pyarrow` from schema-valid JSON records,
   ready for `pa.Table.from_pylist(rows, schema=pa_schema)` and Parquet serialization.
3. The **errors** raised for constructs that cannot be represented as a static columnar schema.

**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · [3. Errors](#3-errors-unsupported--invalid-input) · [4. Fixture Index](#4-fixture-index)

## 1. Conversion Rules

### 1.1 Top-Level Resolution and Unwrapping

<a id="r-dialect"></a>

- **R-DIALECT**: The root must declare `"$schema": "https://json-schema.org/draft/2020-12/schema"` or the KBase
  XSV metaschema `"https://json-schema.kbase.us/draft/2020-12-xsv/schema"`; anything else raises **E1**.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats)

<a id="r-root-ref"></a>

- **R-ROOT-REF**: A root consisting of `"$ref": "#/$defs/<Name>"` (without its own `properties`) is replaced by the
  referenced definition before classification. That definition counts as one expansion for recursion.

  *Cases:* [05_defs_and_refs](#case-spec-05-defs-and-refs), [ncbi_annotation_report](#case-examples-ncbi-annotation-report)

<a id="r-unwrap"></a>

- **R-UNWRAP**: The resolved root must be a fixed-property object producing at least one field. Its fields become
  the top-level columns. A scalar, array or pure-map root raises **E2**; an object with no fields raises
  **E3**.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats)

### 1.2 Naming and Field Ordering

<a id="r-name-property"></a>

- **R-NAME-PROPERTY**: Property keys are used verbatim as field names.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats)

<a id="r-order"></a>

- **R-ORDER**: Struct fields are emitted in this order: properties from each `allOf` subschema in list order, then
  direct `properties` in declaration order, then properties contributed by `anyOf`, `oneOf`,
  `dependentSchemas`, `then` and `else`. A property seen more than once keeps the position of its
  first occurrence.

  *Cases:* [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof), [08_conditional_and_dependent_schemas](#case-spec-08-conditional-and-dependent-schemas), [10_combinations](#case-spec-10-combinations)

<a id="r-merge-duplicate"></a>

- **R-MERGE-DUPLICATE**: When a property is declared more than once (across `allOf`, `anyOf`/`oneOf`, `dependentSchemas` or
  `then`/`else`), identical declarations collapse to one; differing declarations are combined as an
  `anyOf` of the declarations and resolved with the union rules ([R-UNION-SCALAR](#r-union-scalar),
  [R-UNION-OBJECT](#r-union-object)). The first declared `description` is kept.

  *Cases:* [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof)

### 1.3 Construct Classification (Shape)

<a id="r-shape-object"></a>

- **R-SHAPE-OBJECT**: An object with `properties` (or object combinators) maps to `pa.struct([...])`. Boolean
  `additionalProperties` (`true`, `false` or omitted) on such an object is ignored.

  *Cases:* [04_objects_and_maps](#case-spec-04-objects-and-maps)

<a id="r-shape-array"></a>

- **R-SHAPE-ARRAY**: `type: "array"` with `items: <schema>` maps to `pa.list_(pa.field("item", <item type>))`; nested
  arrays nest lists.

  *Cases:* [03_arrays_and_tuples](#case-spec-03-arrays-and-tuples), [cts_jobs](#case-examples-cts-jobs), [ncbi_annotation_report](#case-examples-ncbi-annotation-report)

<a id="r-shape-tuple"></a>

- **R-SHAPE-TUPLE**: A closed tuple (`prefixItems: [S_0, ..., S_k-1]` with `items` `false` or omitted) maps to
  `pa.struct` with fields `item_0` ... `item_k-1`. Any other `items` value raises **E5**.

  *Cases:* [03_arrays_and_tuples](#case-spec-03-arrays-and-tuples), [10_combinations](#case-spec-10-combinations)

<a id="r-shape-map"></a>

- **R-SHAPE-MAP**: An object with schema-valued `additionalProperties` and/or `patternProperties` and no fixed
  properties maps to `pa.map_(pa.string(), pa.field("value", <value type>))`. Several value schemas are
  combined with the union rules. Mixing fixed properties with a map schema raises **E6**.

  *Cases:* [04_objects_and_maps](#case-spec-04-objects-and-maps), [10_combinations](#case-spec-10-combinations), [cts_job](#case-examples-cts-job), [uniprot_search](#case-examples-uniprot-search)

<a id="r-shape-ref"></a>

- **R-SHAPE-REF**: `"$ref": "#/$defs/<Name>"` is replaced by the definition; sibling keywords on the referencing node
  are overlaid on the definition. Only local `#/$defs/` pointers are supported (**E9**).

  *Cases:* [05_defs_and_refs](#case-spec-05-defs-and-refs), [09_context_dependent_shared_defs](#case-spec-09-context-dependent-shared-defs), [cts_job](#case-examples-cts-job), [cts_jobs](#case-examples-cts-jobs), [ncbi_annotation_report](#case-examples-ncbi-annotation-report)

<a id="r-recursion"></a>

- **R-RECURSION**: Each `$defs` target may be expanded at most `max_recursion_depth` times (default `2`) along a path.
  A deeper reference becomes `pa.string()` (or `pa.list_(pa.string())` inside an array), carries no
  `jsonschema_type`, and its runtime value is serialized with `json.dumps`. Applies to self-recursion
  and mutual recursion.

  *Cases:* [06_recursion_depth](#case-spec-06-recursion-depth), [10_combinations](#case-spec-10-combinations), [uniprot_search](#case-examples-uniprot-search)

### 1.4 Field Nullability

<a id="r-null-required"></a>

- **R-NULL-REQUIRED**: A property field is `nullable=False` only when it is in the enclosing object's effective `required`
  set and its schema does not allow `null`. The effective `required` set is the union of `required`
  across the object and its `allOf` subschemas, plus the intersection of `required` across
  `anyOf`/`oneOf` object branches.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [02_nullability_and_required](#case-spec-02-nullability-and-required), [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof), [09_context_dependent_shared_defs](#case-spec-09-context-dependent-shared-defs)

<a id="r-null-allows-null"></a>

- **R-NULL-ALLOWS-NULL**: A schema allows `null` when `"null"` is in `type`, `null` is in `enum` or is the `const`, or any
  `anyOf`/`oneOf`/`allOf` branch (or `$ref` target) allows `null`. Such fields are `nullable=True` even
  when required.

  *Cases:* [02_nullability_and_required](#case-spec-02-nullability-and-required), [09_context_dependent_shared_defs](#case-spec-09-context-dependent-shared-defs), [cts_job](#case-examples-cts-job)

<a id="r-null-conditional"></a>

- **R-NULL-CONDITIONAL**: Properties introduced only by `dependentSchemas`, `then` or `else` are `nullable=True`;
  `dependentRequired` does not affect nullability.

  *Cases:* [08_conditional_and_dependent_schemas](#case-spec-08-conditional-and-dependent-schemas), [10_combinations](#case-spec-10-combinations)

<a id="r-null-default"></a>

- **R-NULL-DEFAULT**: `default` is an annotation only: it neither changes nullability nor fills in missing values.

  *Cases:* [02_nullability_and_required](#case-spec-02-nullability-and-required)

<a id="r-null-items"></a>

- **R-NULL-ITEMS**: A list `item` field and a map `value` field are `nullable=False` unless the item/value schema allows
  `null`. A tuple field `item_i` is `nullable=False` only when `minItems >= i + 1` and `prefixItems[i]`
  does not allow `null`.

  *Cases:* [02_nullability_and_required](#case-spec-02-nullability-and-required), [03_arrays_and_tuples](#case-spec-03-arrays-and-tuples), [04_objects_and_maps](#case-spec-04-objects-and-maps), [09_context_dependent_shared_defs](#case-spec-09-context-dependent-shared-defs)

### 1.5 Scalar Types and Unions

<a id="r-scalar-type"></a>

- **R-SCALAR-TYPE**: Scalar `type`, `format` and `contentEncoding` map to Arrow types per the table below. Numeric
  validation keywords (`minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum`, `multipleOf`) do
  not change the type. Only `format: "date"` becomes a temporal type; other formats stay `pa.string()`.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [10_combinations](#case-spec-10-combinations), [uniprot_search](#case-examples-uniprot-search)

<a id="r-scalar-enum-const"></a>

- **R-SCALAR-ENUM-CONST**: With an explicit `type`, `enum`/`const` use that type. Without one, each non-null literal's type is
  inferred (`bool` -> boolean, `int` -> integer, `float` -> number, `str` -> string) and combined with
  the union rules.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [02_nullability_and_required](#case-spec-02-nullability-and-required), [10_combinations](#case-spec-10-combinations), [ncbi_annotation_report](#case-examples-ncbi-annotation-report)

<a id="r-union-scalar"></a>

- **R-UNION-SCALAR**: For `type: [...]` and scalar-only `anyOf`/`oneOf`, `null` members are dropped (making the field
  nullable). If the rest share one Arrow type it is used; if they are only `integer` and `number` the
  type is `pa.float64()`; otherwise `pa.string()`. `jsonschema_type` joins the distinct member labels
  with `" | "` in declaration order. `object` or `array` in a `type` list raises **E8**.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof), [10_combinations](#case-spec-10-combinations), [cts_job](#case-examples-cts-job)

<a id="r-union-object"></a>

- **R-UNION-OBJECT**: `anyOf`/`oneOf` whose non-null branches are all objects merge their properties into one struct.
  Branches of other structural shapes (array, map) raise **E8**.

  *Cases:* [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof), [10_combinations](#case-spec-10-combinations), [uniprot_search](#case-examples-uniprot-search)

<a id="r-union-scalar-object"></a>

- **R-UNION-SCALAR-OBJECT**: `anyOf`/`oneOf` mixing object and scalar branches yields a struct of the merged object properties,
  all `nullable=True`, followed by a synthesized `_value` field holding the unified scalar type. A
  declared `_value` property raises **E12**.

  *Cases:* [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof), [uniprot_search](#case-examples-uniprot-search)

| JSON Schema | Python type (coerced) | PyArrow `DataType` | `metadata["jsonschema_type"]` |
| :--- | :--- | :--- | :--- |
| `{"type": "boolean"}` | `bool` | `pa.bool_()` | `boolean` |
| `{"type": "integer"}` | `int` | `pa.int64()` | `integer` |
| `{"type": "number"}` | `float` | `pa.float64()` | `number` |
| `{"type": "string"}` | `str` | `pa.string()` | `string` |
| `{"type": "string", "format": "date"}` | `datetime.date` | `pa.date32()` | `string (date)` |
| `{"type": "string", "format": "date-time"}` | `str` | `pa.string()` | `string (date-time)` |
| `{"type": "string", "format": "time"}` | `str` | `pa.string()` | `string (time)` |
| `{"type": "string", "format": "duration"}` | `str` | `pa.string()` | `string (duration)` |
| `{"type": "string", "format": "uuid"}` | `str` | `pa.string()` | `string (uuid)` |
| `{"type": "string", "format": "uri"}` | `str` | `pa.string()` | `string (uri)` |
| `{"type": "string", "format": "email"}` | `str` | `pa.string()` | `string (email)` |
| `{"type": "string", "contentEncoding": "base64"}` | `bytes` | `pa.binary()` | `string (base64)` |
| `{"type": "string", "contentEncoding": "base16"}` | `bytes` | `pa.binary()` | `string (base16)` |
| `{"enum": [1, 2]}` | `int` | `pa.int64()` | `integer` |
| `{"enum": [1, 2.5]}` | `float` | `pa.float64()` | `integer \| number` |
| `{"enum": [1, "two", true]}` | `str` | `pa.string()` | `integer \| string \| boolean` |
| `{"type": ["integer", "number"]}` | `float` | `pa.float64()` | `integer \| number` |
| `{"type": ["integer", "string"]}` | `str` | `pa.string()` | `integer \| string` |
| `{"type": ["string", "null"]}` | `str` | `pa.string()` | `string` |

### 1.6 Field Metadata

<a id="r-meta-jsonschema-type"></a>

- **R-META-JSONSCHEMA-TYPE**: Every scalar field (including scalar list `item`, map `value` and tuple `item_i` fields) carries
  `metadata["jsonschema_type"]`. Struct, list, map and recursion-truncated fields do not.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats)

<a id="r-meta-comment"></a>

- **R-META-COMMENT**: A non-empty `description` is stored in `metadata["comment"]`. If the node has none, it is inherited
  from its `$ref` target, or from the only non-null `$ref` branch of an `anyOf`/`oneOf`.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [03_arrays_and_tuples](#case-spec-03-arrays-and-tuples), [05_defs_and_refs](#case-spec-05-defs-and-refs), [09_context_dependent_shared_defs](#case-spec-09-context-dependent-shared-defs)

### 1.7 Record Coercion

<a id="r-coerce-scalar"></a>

- **R-COERCE-SCALAR**: Strings for `pa.date32()` are parsed with `datetime.date.fromisoformat`; `pa.binary()` strings are
  decoded as Base16 (`bytes.fromhex`) when `jsonschema_type` mentions `base16`, otherwise Base64. Ints
  become floats for float fields, and integral floats become ints for integer fields.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [10_combinations](#case-spec-10-combinations)

<a id="r-coerce-string-fallback"></a>

- **R-COERCE-STRING-FALLBACK**: Non-string values written to a `pa.string()` field are stringified: booleans, objects and arrays with
  `json.dumps` (`true`/`false`), numbers with `str()`. This covers heterogeneous scalar unions and
  recursion-truncated subtrees.

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats), [06_recursion_depth](#case-spec-06-recursion-depth)

<a id="r-coerce-tuple"></a>

- **R-COERCE-TUPLE**: A JSON array for a closed tuple becomes `{"item_0": v_0, ...}`; positions that are missing or `null`
  are omitted.

  *Cases:* [03_arrays_and_tuples](#case-spec-03-arrays-and-tuples)

<a id="r-coerce-value"></a>

- **R-COERCE-VALUE**: A scalar written to a scalar + object union struct becomes `{"_value": <coerced scalar>}`.

  *Cases:* [07_combinators_allof_anyof_oneof](#case-spec-07-combinators-allof-anyof-oneof)

<a id="r-coerce-sparse"></a>

- **R-COERCE-SPARSE**: Only keys declared in the schema are kept; absent keys stay absent (they become null in the table).

  *Cases:* [01_scalars_and_formats](#case-spec-01-scalars-and-formats)

## 2. Cases

<a id="case-spec-01-scalars-and-formats"></a>

### Scalars, Formats, Binary Encodings, Enums and Scalar Unions (`spec/01_scalars_and_formats`)

**Rules:** [R-DIALECT](#r-dialect), [R-UNWRAP](#r-unwrap), [R-NAME-PROPERTY](#r-name-property), [R-SCALAR-TYPE](#r-scalar-type), [R-SCALAR-ENUM-CONST](#r-scalar-enum-const), [R-UNION-SCALAR](#r-union-scalar), [R-META-JSONSCHEMA-TYPE](#r-meta-jsonschema-type), [R-META-COMMENT](#r-meta-comment), [R-NULL-REQUIRED](#r-null-required), [R-COERCE-SCALAR](#r-coerce-scalar), [R-COERCE-STRING-FALLBACK](#r-coerce-string-fallback), [R-COERCE-SPARSE](#r-coerce-sparse)

- `untyped_mixed_enum` (`enum: [1, "two", true]`) falls back to `pa.string()`; the value `1` is coerced to `"1"`.
- `numeric_union` (`["integer", "number"]`) promotes to `pa.float64()`; `scalar_union` (`["integer", "string"]`) falls back to `pa.string()`.
- `date_or_year_union` mixes `string (date)` (`pa.date32()`) and `string`, so it falls back to `pa.string()`.
- The second record omits every optional key; they stay absent in the coerced row.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/01_scalars_and_formats.schema.json",
  "title": "ScalarsAndFormatsRecord",
  "type": "object",
  "required": [
    "id",
    "status",
    "version",
    "flag"
  ],
  "properties": {
    "id": {
      "type": "string",
      "description": "Unique record identifier."
    },
    "status": {
      "type": "string",
      "enum": [
        "active",
        "archived",
        "pending"
      ],
      "description": "Record lifecycle status."
    },
    "version": {
      "type": "integer",
      "minimum": 1,
      "maximum": 32767
    },
    "flag": {
      "type": "boolean",
      "description": "Primary boolean flag."
    },
    "bounded_int": {
      "type": "integer",
      "exclusiveMinimum": -1000,
      "exclusiveMaximum": 1000
    },
    "big_int": {
      "type": "integer"
    },
    "float_val": {
      "type": "number"
    },
    "decimal_multiple": {
      "type": "number",
      "multipleOf": 0.01
    },
    "created_date": {
      "type": "string",
      "format": "date"
    },
    "updated_at": {
      "type": "string",
      "format": "date-time"
    },
    "event_time": {
      "type": "string",
      "format": "time"
    },
    "elapsed": {
      "type": "string",
      "format": "duration"
    },
    "record_uuid": {
      "type": "string",
      "format": "uuid"
    },
    "homepage": {
      "type": "string",
      "format": "uri"
    },
    "const_tag": {
      "const": "v1"
    },
    "untyped_int_enum": {
      "enum": [
        10,
        20,
        30
      ]
    },
    "untyped_mixed_enum": {
      "enum": [
        1,
        "two",
        true
      ]
    },
    "numeric_union": {
      "type": [
        "integer",
        "number"
      ]
    },
    "scalar_union": {
      "type": [
        "integer",
        "string"
      ]
    },
    "date_or_year_union": {
      "anyOf": [
        {
          "type": "string",
          "format": "date"
        },
        {
          "type": "string",
          "pattern": "^[0-9]{4}$"
        }
      ]
    },
    "raw_b64": {
      "type": "string",
      "contentEncoding": "base64"
    },
    "raw_hex": {
      "type": "string",
      "contentEncoding": "base16"
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "id": "rec1",
        "status": "active",
        "version": 2,
        "flag": true,
        "bounded_int": -42,
        "big_int": 999999999999,
        "float_val": 2.718281828,
        "decimal_multiple": 3.14,
        "created_date": "2025-06-15",
        "updated_at": "2025-06-15T12:30:00Z",
        "event_time": "12:30:00Z",
        "elapsed": "P3DT4H",
        "record_uuid": "550e8400-e29b-41d4-a716-446655440000",
        "homepage": "https://example.org/rec1",
        "const_tag": "v1",
        "untyped_int_enum": 20,
        "untyped_mixed_enum": 1,
        "numeric_union": 42,
        "scalar_union": 100,
        "date_or_year_union": "2025-06-15",
        "raw_b64": "SGVsbG8=",
        "raw_hex": "48656c6c6f"
    },
    {
        "id": "rec2",
        "status": "archived",
        "version": 1,
        "flag": false
    },
    {
        "id": "rec3",
        "status": "pending",
        "version": 32767,
        "flag": true,
        "bounded_int": 999,
        "big_int": -999999999999,
        "float_val": -0.5,
        "decimal_multiple": 42.0,
        "created_date": "1999-12-31",
        "updated_at": "2025-06-15T12:30:00.123+02:00",
        "event_time": "23:59:59+02:00",
        "elapsed": "PT15M",
        "record_uuid": "123e4567-e89b-12d3-a456-426614174000",
        "homepage": "https://example.org/rec3",
        "const_tag": "v1",
        "untyped_int_enum": 30,
        "untyped_mixed_enum": true,
        "numeric_union": 3.5,
        "scalar_union": "hundred",
        "date_or_year_union": "1999",
        "raw_b64": "V29ybGQ=",
        "raw_hex": "576f726c64"
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "id": "rec1",
        "status": "active",
        "version": 2,
        "flag": true,
        "bounded_int": -42,
        "big_int": 999999999999,
        "float_val": 2.718281828,
        "decimal_multiple": 3.14,
        "created_date": "2025-06-15",
        "updated_at": "2025-06-15T12:30:00Z",
        "event_time": "12:30:00Z",
        "elapsed": "P3DT4H",
        "record_uuid": "550e8400-e29b-41d4-a716-446655440000",
        "homepage": "https://example.org/rec1",
        "const_tag": "v1",
        "untyped_int_enum": 20,
        "untyped_mixed_enum": "1",
        "numeric_union": 42.0,
        "scalar_union": "100",
        "date_or_year_union": "2025-06-15",
        "raw_b64": "Hello",
        "raw_hex": "Hello"
    },
    {
        "id": "rec2",
        "status": "archived",
        "version": 1,
        "flag": false
    },
    {
        "id": "rec3",
        "status": "pending",
        "version": 32767,
        "flag": true,
        "bounded_int": 999,
        "big_int": -999999999999,
        "float_val": -0.5,
        "decimal_multiple": 42.0,
        "created_date": "1999-12-31",
        "updated_at": "2025-06-15T12:30:00.123+02:00",
        "event_time": "23:59:59+02:00",
        "elapsed": "PT15M",
        "record_uuid": "123e4567-e89b-12d3-a456-426614174000",
        "homepage": "https://example.org/rec3",
        "const_tag": "v1",
        "untyped_int_enum": 30,
        "untyped_mixed_enum": "true",
        "numeric_union": 3.5,
        "scalar_union": "hundred",
        "date_or_year_union": "1999",
        "raw_b64": "World",
        "raw_hex": "World"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/01_scalars_and_formats/expected_schema.py))

```python
"""Expected PyArrow schema for 01_scalars_and_formats."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Unique record identifier.", "jsonschema_type": "string"},
        ),
        pa.field(
            "status",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record lifecycle status.", "jsonschema_type": "string"},
        ),
        pa.field("version", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "flag",
            pa.bool_(),
            nullable=False,
            metadata={"comment": "Primary boolean flag.", "jsonschema_type": "boolean"},
        ),
        pa.field("bounded_int", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("big_int", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("float_val", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("decimal_multiple", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("created_date", pa.date32(), nullable=True, metadata={"jsonschema_type": "string (date)"}),
        pa.field("updated_at", pa.string(), nullable=True, metadata={"jsonschema_type": "string (date-time)"}),
        pa.field("event_time", pa.string(), nullable=True, metadata={"jsonschema_type": "string (time)"}),
        pa.field("elapsed", pa.string(), nullable=True, metadata={"jsonschema_type": "string (duration)"}),
        pa.field("record_uuid", pa.string(), nullable=True, metadata={"jsonschema_type": "string (uuid)"}),
        pa.field("homepage", pa.string(), nullable=True, metadata={"jsonschema_type": "string (uri)"}),
        pa.field("const_tag", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("untyped_int_enum", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "untyped_mixed_enum",
            pa.string(),
            nullable=True,
            metadata={"jsonschema_type": "integer | string | boolean"},
        ),
        pa.field("numeric_union", pa.float64(), nullable=True, metadata={"jsonschema_type": "integer | number"}),
        pa.field("scalar_union", pa.string(), nullable=True, metadata={"jsonschema_type": "integer | string"}),
        pa.field(
            "date_or_year_union",
            pa.string(),
            nullable=True,
            metadata={"jsonschema_type": "string (date) | string"},
        ),
        pa.field("raw_b64", pa.binary(), nullable=True, metadata={"jsonschema_type": "string (base64)"}),
        pa.field("raw_hex", pa.binary(), nullable=True, metadata={"jsonschema_type": "string (base16)"}),
    ]
)
```

---

<a id="case-spec-02-nullability-and-required"></a>

### Nullability, Required Fields and Array Item Nullability (`spec/02_nullability_and_required`)

**Rules:** [R-NULL-REQUIRED](#r-null-required), [R-NULL-ALLOWS-NULL](#r-null-allows-null), [R-NULL-DEFAULT](#r-null-default), [R-NULL-ITEMS](#r-null-items), [R-SCALAR-ENUM-CONST](#r-scalar-enum-const)

- `required_nullable_str`, `required_anyof_nullable` and `required_enum_nullable` are required but allow `null`, so they are nullable.
- `optional_with_default` has a `default` and stays nullable.
- `strict_int_list` items are non-nullable; `nullable_int_list` items (`["integer", "null"]`) are nullable.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/02_nullability_and_required.schema.json",
  "title": "NullabilityAndRequiredRecord",
  "type": "object",
  "required": [
    "required_str",
    "required_int",
    "required_nullable_str",
    "required_anyof_nullable",
    "required_enum_nullable",
    "strict_int_list",
    "nullable_int_list"
  ],
  "properties": {
    "required_str": {
      "type": "string",
      "description": "Strictly required non-null string."
    },
    "required_int": {
      "type": "integer"
    },
    "required_nullable_str": {
      "type": [
        "string",
        "null"
      ],
      "description": "Required key whose value may be null."
    },
    "required_anyof_nullable": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ]
    },
    "required_enum_nullable": {
      "enum": [
        "low",
        "medium",
        "high",
        null
      ]
    },
    "optional_str": {
      "type": "string"
    },
    "optional_with_default": {
      "type": "string",
      "default": "fallback"
    },
    "optional_nullable_number": {
      "type": [
        "number",
        "null"
      ]
    },
    "strict_int_list": {
      "type": "array",
      "items": {
        "type": "integer"
      }
    },
    "nullable_int_list": {
      "type": "array",
      "items": {
        "type": [
          "integer",
          "null"
        ]
      }
    },
    "optional_str_list": {
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "required_str": "alpha",
        "required_int": 10,
        "required_nullable_str": "present",
        "required_anyof_nullable": 99,
        "required_enum_nullable": "high",
        "optional_str": "opt_value",
        "optional_with_default": "explicit",
        "optional_nullable_number": 1.25,
        "strict_int_list": [
            1,
            2,
            3
        ],
        "nullable_int_list": [
            10,
            null,
            30
        ],
        "optional_str_list": [
            "a",
            "b"
        ]
    },
    {
        "required_str": "beta",
        "required_int": 0,
        "required_nullable_str": null,
        "required_anyof_nullable": null,
        "required_enum_nullable": null,
        "optional_nullable_number": null,
        "strict_int_list": [],
        "nullable_int_list": [
            null
        ]
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "required_str": "alpha",
        "required_int": 10,
        "required_nullable_str": "present",
        "required_anyof_nullable": 99,
        "required_enum_nullable": "high",
        "optional_str": "opt_value",
        "optional_with_default": "explicit",
        "optional_nullable_number": 1.25,
        "strict_int_list": [
            1,
            2,
            3
        ],
        "nullable_int_list": [
            10,
            null,
            30
        ],
        "optional_str_list": [
            "a",
            "b"
        ]
    },
    {
        "required_str": "beta",
        "required_int": 0,
        "required_nullable_str": null,
        "required_anyof_nullable": null,
        "required_enum_nullable": null,
        "optional_nullable_number": null,
        "strict_int_list": [],
        "nullable_int_list": [
            null
        ]
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/02_nullability_and_required/expected_schema.py))

```python
"""Expected PyArrow schema for 02_nullability_and_required."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "required_str",
            pa.string(),
            nullable=False,
            metadata={"comment": "Strictly required non-null string.", "jsonschema_type": "string"},
        ),
        pa.field("required_int", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "required_nullable_str",
            pa.string(),
            nullable=True,
            metadata={"comment": "Required key whose value may be null.", "jsonschema_type": "string"},
        ),
        pa.field("required_anyof_nullable", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("required_enum_nullable", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("optional_str", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("optional_with_default", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("optional_nullable_number", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field(
            "strict_int_list",
            pa.list_(
                pa.field("item", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "nullable_int_list",
            pa.list_(
                pa.field("item", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "optional_str_list",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-03-arrays-and-tuples"></a>

### Homogeneous Arrays, 2D Arrays and Closed Tuples (`spec/03_arrays_and_tuples`)

**Rules:** [R-SHAPE-ARRAY](#r-shape-array), [R-SHAPE-TUPLE](#r-shape-tuple), [R-NULL-ITEMS](#r-null-items), [R-COERCE-TUPLE](#r-coerce-tuple), [R-META-COMMENT](#r-meta-comment)

`coord_3d` has `minItems: 2`, so `item_0` and `item_1` are non-nullable and `item_2` (`["number", "null"]`) is
nullable. `[10.5, 20.25, 30.0]` becomes `{"item_0": 10.5, "item_1": 20.25, "item_2": 30.0}`.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/03_arrays_and_tuples.schema.json",
  "title": "ArraysAndTuplesRecord",
  "type": "object",
  "required": [
    "id",
    "tags",
    "coord_3d"
  ],
  "properties": {
    "id": {
      "type": "string"
    },
    "tags": {
      "type": "array",
      "description": "Ordered list of string tags.",
      "items": {
        "type": "string"
      }
    },
    "measurements": {
      "type": "array",
      "items": {
        "type": "object",
        "required": [
          "label",
          "value"
        ],
        "properties": {
          "label": {
            "type": "string"
          },
          "value": {
            "type": "number"
          },
          "unit": {
            "type": "string"
          }
        }
      }
    },
    "matrix_2d": {
      "type": "array",
      "description": "Two-dimensional numeric matrix.",
      "items": {
        "type": "array",
        "items": {
          "type": "number"
        }
      }
    },
    "coord_3d": {
      "type": "array",
      "description": "Closed 3D coordinate tuple (x, y, optional z).",
      "minItems": 2,
      "prefixItems": [
        {
          "type": "number",
          "description": "X coordinate."
        },
        {
          "type": "number",
          "description": "Y coordinate."
        },
        {
          "type": [
            "number",
            "null"
          ],
          "description": "Optional Z coordinate."
        }
      ],
      "items": false
    },
    "pair_tuple_list": {
      "type": "array",
      "items": {
        "type": "array",
        "minItems": 2,
        "prefixItems": [
          {
            "type": "string"
          },
          {
            "type": "integer"
          }
        ],
        "items": false
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "id": "arr1",
        "tags": [
            "genomics",
            "assembly"
        ],
        "measurements": [
            {
                "label": "gc_content",
                "value": 0.512,
                "unit": "ratio"
            },
            {
                "label": "n50",
                "value": 4500000.0
            }
        ],
        "matrix_2d": [
            [
                1.0,
                2.0
            ],
            [
                3.5,
                4.5
            ]
        ],
        "coord_3d": [
            10.5,
            20.25,
            30.0
        ],
        "pair_tuple_list": [
            [
                "chr1",
                248956422
            ],
            [
                "chr2",
                242193529
            ]
        ]
    },
    {
        "id": "arr2",
        "tags": [],
        "coord_3d": [
            -1.0,
            2.0
        ]
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "id": "arr1",
        "tags": [
            "genomics",
            "assembly"
        ],
        "measurements": [
            {
                "label": "gc_content",
                "value": 0.512,
                "unit": "ratio"
            },
            {
                "label": "n50",
                "value": 4500000.0
            }
        ],
        "matrix_2d": [
            [
                1.0,
                2.0
            ],
            [
                3.5,
                4.5
            ]
        ],
        "coord_3d": {
            "item_0": 10.5,
            "item_1": 20.25,
            "item_2": 30.0
        },
        "pair_tuple_list": [
            {
                "item_0": "chr1",
                "item_1": 248956422
            },
            {
                "item_0": "chr2",
                "item_1": 242193529
            }
        ]
    },
    {
        "id": "arr2",
        "tags": [],
        "coord_3d": {
            "item_0": -1.0,
            "item_1": 2.0
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/03_arrays_and_tuples/expected_schema.py))

```python
"""Expected PyArrow schema for 03_arrays_and_tuples."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=False,
            metadata={"comment": "Ordered list of string tags."},
        ),
        pa.field(
            "measurements",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("label", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                            pa.field("value", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
                            pa.field("unit", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "matrix_2d",
            pa.list_(
                pa.field(
                    "item",
                    pa.list_(
                        pa.field("item", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
            metadata={"comment": "Two-dimensional numeric matrix."},
        ),
        pa.field(
            "coord_3d",
            pa.struct(
                [
                    pa.field(
                        "item_0",
                        pa.float64(),
                        nullable=False,
                        metadata={"comment": "X coordinate.", "jsonschema_type": "number"},
                    ),
                    pa.field(
                        "item_1",
                        pa.float64(),
                        nullable=False,
                        metadata={"comment": "Y coordinate.", "jsonschema_type": "number"},
                    ),
                    pa.field(
                        "item_2",
                        pa.float64(),
                        nullable=True,
                        metadata={"comment": "Optional Z coordinate.", "jsonschema_type": "number"},
                    ),
                ]
            ),
            nullable=False,
            metadata={"comment": "Closed 3D coordinate tuple (x, y, optional z)."},
        ),
        pa.field(
            "pair_tuple_list",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("item_0", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                            pa.field("item_1", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-04-objects-and-maps"></a>

### Nested Fixed Objects and Dynamic Maps (`spec/04_objects_and_maps`)

**Rules:** [R-SHAPE-OBJECT](#r-shape-object), [R-SHAPE-MAP](#r-shape-map), [R-NULL-ITEMS](#r-null-items)

`closed_meta` (`additionalProperties: false`) and `open_meta` (`additionalProperties: true`) are both structs.
`string_to_int_map`, `nullable_score_map` and `struct_map` use `additionalProperties`; `pattern_metric_map`
uses `patternProperties`.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/04_objects_and_maps.schema.json",
  "title": "ObjectsAndMapsRecord",
  "type": "object",
  "required": [
    "id",
    "closed_meta"
  ],
  "properties": {
    "id": {
      "type": "string"
    },
    "closed_meta": {
      "type": "object",
      "description": "Strict object with additionalProperties set to false.",
      "required": [
        "owner"
      ],
      "additionalProperties": false,
      "properties": {
        "owner": {
          "type": "string"
        },
        "priority": {
          "type": "integer"
        }
      }
    },
    "open_meta": {
      "type": "object",
      "description": "Fixed-property object with boolean additionalProperties true.",
      "additionalProperties": true,
      "properties": {
        "source": {
          "type": "string"
        }
      }
    },
    "string_to_int_map": {
      "type": "object",
      "description": "Dynamic map from string keys to non-null integer counts.",
      "additionalProperties": {
        "type": "integer"
      }
    },
    "nullable_score_map": {
      "type": "object",
      "additionalProperties": {
        "type": [
          "number",
          "null"
        ]
      }
    },
    "pattern_metric_map": {
      "type": "object",
      "patternProperties": {
        "^metric_[0-9]+$": {
          "type": "number"
        }
      },
      "additionalProperties": false
    },
    "struct_map": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "required": [
          "count"
        ],
        "properties": {
          "count": {
            "type": "integer"
          },
          "note": {
            "type": "string"
          }
        }
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "id": "obj1",
        "closed_meta": {
            "owner": "alice",
            "priority": 1
        },
        "open_meta": {
            "source": "ncbi"
        },
        "string_to_int_map": {
            "reads": 1500,
            "contigs": 12
        },
        "nullable_score_map": {
            "completeness": 99.4,
            "contamination": null
        },
        "pattern_metric_map": {
            "metric_1": 0.95,
            "metric_2": 0.88
        },
        "struct_map": {
            "kmer_31": {
                "count": 42000,
                "note": "canonical"
            },
            "kmer_21": {
                "count": 38000
            }
        }
    },
    {
        "id": "obj2",
        "closed_meta": {
            "owner": "bob"
        }
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "id": "obj1",
        "closed_meta": {
            "owner": "alice",
            "priority": 1
        },
        "open_meta": {
            "source": "ncbi"
        },
        "string_to_int_map": {
            "reads": 1500,
            "contigs": 12
        },
        "nullable_score_map": {
            "completeness": 99.4,
            "contamination": null
        },
        "pattern_metric_map": {
            "metric_1": 0.95,
            "metric_2": 0.88
        },
        "struct_map": {
            "kmer_31": {
                "count": 42000,
                "note": "canonical"
            },
            "kmer_21": {
                "count": 38000
            }
        }
    },
    {
        "id": "obj2",
        "closed_meta": {
            "owner": "bob"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/04_objects_and_maps/expected_schema.py))

```python
"""Expected PyArrow schema for 04_objects_and_maps."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "closed_meta",
            pa.struct(
                [
                    pa.field("owner", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("priority", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Strict object with additionalProperties set to false."},
        ),
        pa.field(
            "open_meta",
            pa.struct(
                [
                    pa.field("source", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
            metadata={"comment": "Fixed-property object with boolean additionalProperties true."},
        ),
        pa.field(
            "string_to_int_map",
            pa.map_(
                pa.string(),
                pa.field("value", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
            ),
            nullable=True,
            metadata={"comment": "Dynamic map from string keys to non-null integer counts."},
        ),
        pa.field(
            "nullable_score_map",
            pa.map_(
                pa.string(),
                pa.field("value", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
            ),
            nullable=True,
        ),
        pa.field(
            "pattern_metric_map",
            pa.map_(
                pa.string(),
                pa.field("value", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
            ),
            nullable=True,
        ),
        pa.field(
            "struct_map",
            pa.map_(
                pa.string(),
                pa.field(
                    "value",
                    pa.struct(
                        [
                            pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                            pa.field("note", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-05-defs-and-refs"></a>

### Shared Definitions, `$ref` Reuse and Root `$ref` (`spec/05_defs_and_refs`)

**Rules:** [R-ROOT-REF](#r-root-ref), [R-SHAPE-REF](#r-shape-ref), [R-META-COMMENT](#r-meta-comment)

The root is `"$ref": "#/$defs/Document"`. `lead_author` declares its own `description`, overriding the
`Author` definition's; `reviewer` and `co_authors` items inherit it.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/05_defs_and_refs.schema.json",
  "title": "DefsAndRefsRoot",
  "$ref": "#/$defs/Document",
  "$defs": {
    "IsoDate": {
      "type": "string",
      "format": "date",
      "description": "ISO-8601 calendar date."
    },
    "Author": {
      "type": "object",
      "description": "Author contact record.",
      "required": [
        "name"
      ],
      "properties": {
        "name": {
          "type": "string"
        },
        "orcid": {
          "type": "string"
        }
      }
    },
    "Document": {
      "type": "object",
      "required": [
        "doc_id",
        "lead_author"
      ],
      "properties": {
        "doc_id": {
          "type": "string"
        },
        "published_on": {
          "$ref": "#/$defs/IsoDate"
        },
        "lead_author": {
          "$ref": "#/$defs/Author",
          "description": "Primary corresponding author."
        },
        "reviewer": {
          "$ref": "#/$defs/Author"
        },
        "co_authors": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/Author"
          }
        }
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "doc_id": "doc1",
        "published_on": "2025-07-01",
        "lead_author": {
            "name": "Ada Lovelace",
            "orcid": "0000-0001-2345-6789"
        },
        "reviewer": {
            "name": "Grace Hopper"
        },
        "co_authors": [
            {
                "name": "Alan Turing",
                "orcid": "0000-0002-3456-7890"
            }
        ]
    },
    {
        "doc_id": "doc2",
        "lead_author": {
            "name": "Barbara Liskov"
        }
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "doc_id": "doc1",
        "published_on": "2025-07-01",
        "lead_author": {
            "name": "Ada Lovelace",
            "orcid": "0000-0001-2345-6789"
        },
        "reviewer": {
            "name": "Grace Hopper"
        },
        "co_authors": [
            {
                "name": "Alan Turing",
                "orcid": "0000-0002-3456-7890"
            }
        ]
    },
    {
        "doc_id": "doc2",
        "lead_author": {
            "name": "Barbara Liskov"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/05_defs_and_refs/expected_schema.py))

```python
"""Expected PyArrow schema for 05_defs_and_refs."""

# ruff: noqa: INP001

import pyarrow as pa

_AUTHOR_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("orcid", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("doc_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "published_on",
            pa.date32(),
            nullable=True,
            metadata={"comment": "ISO-8601 calendar date.", "jsonschema_type": "string (date)"},
        ),
        pa.field(
            "lead_author",
            _AUTHOR_STRUCT,
            nullable=False,
            metadata={"comment": "Primary corresponding author."},
        ),
        pa.field(
            "reviewer",
            _AUTHOR_STRUCT,
            nullable=True,
            metadata={"comment": "Author contact record."},
        ),
        pa.field(
            "co_authors",
            pa.list_(
                pa.field(
                    "item",
                    _AUTHOR_STRUCT,
                    nullable=False,
                    metadata={"comment": "Author contact record."},
                ),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-06-recursion-depth"></a>

### Structural Recursion and `max_recursion_depth` (`spec/06_recursion_depth`)

**Rules:** [R-RECURSION](#r-recursion), [R-COERCE-STRING-FALLBACK](#r-coerce-string-fallback)

`TreeNode` is self-recursive (`branch: array of TreeNode`); `Section` and `Subsection` are mutually recursive.
At depth 2 the third `TreeNode` level becomes JSON strings; at depth 1 the second level already does.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/06_recursion_depth.schema.json",
  "title": "RecursionDepthRecord",
  "type": "object",
  "required": [
    "id"
  ],
  "properties": {
    "id": {
      "type": "string"
    },
    "tree": {
      "$ref": "#/$defs/TreeNode"
    },
    "section": {
      "$ref": "#/$defs/Section"
    }
  },
  "$defs": {
    "TreeNode": {
      "type": "object",
      "properties": {
        "label": {
          "type": "string"
        },
        "weight": {
          "type": "integer"
        },
        "branch": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/TreeNode"
          }
        }
      }
    },
    "Section": {
      "type": "object",
      "required": [
        "section_title"
      ],
      "properties": {
        "section_title": {
          "type": "string"
        },
        "subsection": {
          "$ref": "#/$defs/Subsection"
        }
      }
    },
    "Subsection": {
      "type": "object",
      "required": [
        "subsection_title"
      ],
      "properties": {
        "subsection_title": {
          "type": "string"
        },
        "nested_sections": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/Section"
          }
        }
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "id": "r1",
        "tree": {
            "label": "L1",
            "weight": 10,
            "branch": [
                {
                    "label": "L2",
                    "branch": [
                        {
                            "label": "L3",
                            "weight": 30,
                            "branch": [
                                {
                                    "label": "L4"
                                }
                            ]
                        },
                        {
                            "label": "L3_sibling"
                        }
                    ]
                }
            ]
        },
        "section": {
            "section_title": "Ch 1",
            "subsection": {
                "subsection_title": "Sub 1.a",
                "nested_sections": [
                    {
                        "section_title": "Sec 1.1",
                        "subsection": {
                            "subsection_title": "Sub 1.1.a",
                            "nested_sections": [
                                {
                                    "section_title": "Subsec 1.1.1"
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
]
```

#### Converted rows: run `depth-2` (`max_recursion_depth=2`)

```json
[
    {
        "id": "r1",
        "tree": {
            "label": "L1",
            "weight": 10,
            "branch": [
                {
                    "label": "L2",
                    "branch": [
                        "{\"label\": \"L3\", \"weight\": 30, \"branch\": [{\"label\": \"L4\"}]}",
                        "{\"label\": \"L3_sibling\"}"
                    ]
                }
            ]
        },
        "section": {
            "section_title": "Ch 1",
            "subsection": {
                "subsection_title": "Sub 1.a",
                "nested_sections": [
                    {
                        "section_title": "Sec 1.1",
                        "subsection": {
                            "subsection_title": "Sub 1.1.a",
                            "nested_sections": [
                                "{\"section_title\": \"Subsec 1.1.1\"}"
                            ]
                        }
                    }
                ]
            }
        }
    }
]
```

#### Converted rows: run `depth-1` (`max_recursion_depth=1`)

```json
[
    {
        "id": "r1",
        "tree": {
            "label": "L1",
            "weight": 10,
            "branch": [
                "{\"label\": \"L2\", \"branch\": [{\"label\": \"L3\", \"weight\": 30, \"branch\": [{\"label\": \"L4\"}]}, {\"label\": \"L3_sibling\"}]}"
            ]
        },
        "section": {
            "section_title": "Ch 1",
            "subsection": {
                "subsection_title": "Sub 1.a",
                "nested_sections": [
                    "{\"section_title\": \"Sec 1.1\", \"subsection\": {\"subsection_title\": \"Sub 1.1.a\", \"nested_sections\": [{\"section_title\": \"Subsec 1.1.1\"}]}}"
                ]
            }
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/06_recursion_depth/expected_schema.py))

```python
"""Expected PyArrow schema for 06_recursion_depth."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("weight", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "label",
                                            pa.string(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "weight",
                                            pa.int64(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "integer"},
                                        ),
                                        pa.field(
                                            "branch",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=False),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "section",
            pa.struct(
                [
                    pa.field("section_title", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field(
                        "subsection",
                        pa.struct(
                            [
                                pa.field(
                                    "subsection_title",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"jsonschema_type": "string"},
                                ),
                                pa.field(
                                    "nested_sections",
                                    pa.list_(
                                        pa.field(
                                            "item",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "section_title",
                                                        pa.string(),
                                                        nullable=False,
                                                        metadata={"jsonschema_type": "string"},
                                                    ),
                                                    pa.field(
                                                        "subsection",
                                                        pa.struct(
                                                            [
                                                                pa.field(
                                                                    "subsection_title",
                                                                    pa.string(),
                                                                    nullable=False,
                                                                    metadata={"jsonschema_type": "string"},
                                                                ),
                                                                pa.field(
                                                                    "nested_sections",
                                                                    pa.list_(
                                                                        pa.field("item", pa.string(), nullable=False),
                                                                    ),
                                                                    nullable=True,
                                                                ),
                                                            ]
                                                        ),
                                                        nullable=True,
                                                    ),
                                                ]
                                            ),
                                            nullable=False,
                                        ),
                                    ),
                                    nullable=True,
                                ),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)

EXPECTED_SCHEMA_DEPTH_1 = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("weight", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=False),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "section",
            pa.struct(
                [
                    pa.field("section_title", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field(
                        "subsection",
                        pa.struct(
                            [
                                pa.field(
                                    "subsection_title",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"jsonschema_type": "string"},
                                ),
                                pa.field(
                                    "nested_sections",
                                    pa.list_(
                                        pa.field("item", pa.string(), nullable=False),
                                    ),
                                    nullable=True,
                                ),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-07-combinators-allof-anyof-oneof"></a>

### Combinators — `allOf`, `anyOf` and `oneOf` (`spec/07_combinators_allof_anyof_oneof`)

**Rules:** [R-ORDER](#r-order), [R-MERGE-DUPLICATE](#r-merge-duplicate), [R-UNION-OBJECT](#r-union-object), [R-UNION-SCALAR](#r-union-scalar), [R-UNION-SCALAR-OBJECT](#r-union-scalar-object), [R-NULL-REQUIRED](#r-null-required), [R-COERCE-VALUE](#r-coerce-value)

- `allOf: [BaseEntity]` puts `entity_id` and `created_by` before the direct properties.
- `location` merges two `oneOf` object branches. `storage_kind` is required in both, so it is non-nullable; it is
  declared with different `const` values, which merge to `string`.
- `note` is `string | object`; the plain string `"plain text note"` becomes `{"_value": "plain text note"}`.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/07_combinators_allof_anyof_oneof.schema.json",
  "title": "CombinatorsRecord",
  "type": "object",
  "allOf": [
    {
      "$ref": "#/$defs/BaseEntity"
    }
  ],
  "required": [
    "sample_name",
    "location"
  ],
  "properties": {
    "sample_name": {
      "type": "string"
    },
    "location": {
      "description": "Storage location choice across S3 or HTTP.",
      "oneOf": [
        {
          "type": "object",
          "required": [
            "storage_kind",
            "bucket",
            "key"
          ],
          "properties": {
            "storage_kind": {
              "type": "string",
              "const": "s3"
            },
            "bucket": {
              "type": "string"
            },
            "key": {
              "type": "string"
            }
          }
        },
        {
          "type": "object",
          "required": [
            "storage_kind",
            "url"
          ],
          "properties": {
            "storage_kind": {
              "type": "string",
              "const": "http"
            },
            "url": {
              "type": "string",
              "format": "uri"
            },
            "checksum": {
              "type": "string"
            }
          }
        }
      ]
    },
    "pagination": {
      "anyOf": [
        {
          "type": "object",
          "required": [
            "start_page"
          ],
          "properties": {
            "start_page": {
              "type": "string"
            },
            "end_page": {
              "type": "string"
            }
          }
        },
        {
          "type": "object",
          "required": [
            "medline_pgn"
          ],
          "properties": {
            "medline_pgn": {
              "type": "string"
            }
          }
        }
      ]
    },
    "identifier_union": {
      "oneOf": [
        {
          "type": "integer"
        },
        {
          "type": "string"
        }
      ]
    },
    "note": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "object",
          "required": [
            "text"
          ],
          "properties": {
            "text": {
              "type": "string"
            },
            "author": {
              "type": "string"
            }
          }
        }
      ]
    }
  },
  "$defs": {
    "BaseEntity": {
      "type": "object",
      "required": [
        "entity_id"
      ],
      "properties": {
        "entity_id": {
          "type": "string",
          "description": "Inherited base entity identifier."
        },
        "created_by": {
          "type": "string"
        }
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "entity_id": "ent1",
        "created_by": "pipeline",
        "sample_name": "E. coli K-12",
        "location": {
            "storage_kind": "s3",
            "bucket": "cdm-lake",
            "key": "genomes/ecoli.fna.gz"
        },
        "pagination": {
            "start_page": "101",
            "end_page": "115"
        },
        "identifier_union": 12345,
        "note": {
            "text": "Structured note",
            "author": "curator"
        }
    },
    {
        "entity_id": "ent2",
        "sample_name": "B. subtilis",
        "location": {
            "storage_kind": "http",
            "url": "https://example.org/bsub.fna.gz",
            "checksum": "sha256:abc123"
        },
        "pagination": {
            "medline_pgn": "e10042"
        },
        "identifier_union": "GCF_000009045.1",
        "note": "Plain text note"
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "entity_id": "ent1",
        "created_by": "pipeline",
        "sample_name": "E. coli K-12",
        "location": {
            "storage_kind": "s3",
            "bucket": "cdm-lake",
            "key": "genomes/ecoli.fna.gz"
        },
        "pagination": {
            "start_page": "101",
            "end_page": "115"
        },
        "identifier_union": "12345",
        "note": {
            "text": "Structured note",
            "author": "curator"
        }
    },
    {
        "entity_id": "ent2",
        "sample_name": "B. subtilis",
        "location": {
            "storage_kind": "http",
            "url": "https://example.org/bsub.fna.gz",
            "checksum": "sha256:abc123"
        },
        "pagination": {
            "medline_pgn": "e10042"
        },
        "identifier_union": "GCF_000009045.1",
        "note": {
            "_value": "Plain text note"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/07_combinators_allof_anyof_oneof/expected_schema.py))

```python
"""Expected PyArrow schema for 07_combinators_allof_anyof_oneof."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "entity_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Inherited base entity identifier.", "jsonschema_type": "string"},
        ),
        pa.field("created_by", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sample_name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "location",
            pa.struct(
                [
                    pa.field("storage_kind", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("bucket", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("key", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("url", pa.string(), nullable=True, metadata={"jsonschema_type": "string (uri)"}),
                    pa.field("checksum", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Storage location choice across S3 or HTTP."},
        ),
        pa.field(
            "pagination",
            pa.struct(
                [
                    pa.field("start_page", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("end_page", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("medline_pgn", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field("identifier_union", pa.string(), nullable=True, metadata={"jsonschema_type": "integer | string"}),
        pa.field(
            "note",
            pa.struct(
                [
                    pa.field("text", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("author", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("_value", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-08-conditional-and-dependent-schemas"></a>

### Conditional (`if`/`then`/`else`) and Dependent Schemas (`spec/08_conditional_and_dependent_schemas`)

**Rules:** [R-NULL-CONDITIONAL](#r-null-conditional), [R-ORDER](#r-order)

`strain_name` (from `then`), `biome` (from `else`) and `platform` (from `dependentSchemas`) are nullable.
`depth_unit` appears only in `dependentRequired` and stays nullable.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/08_conditional_and_dependent_schemas.schema.json",
  "title": "ConditionalAndDependentRecord",
  "type": "object",
  "required": [
    "id",
    "specimen_type"
  ],
  "properties": {
    "id": {
      "type": "string"
    },
    "specimen_type": {
      "type": "string",
      "enum": [
        "isolate",
        "metagenome"
      ]
    },
    "sequencing_depth": {
      "type": "number"
    },
    "depth_unit": {
      "type": "string"
    }
  },
  "dependentRequired": {
    "sequencing_depth": [
      "depth_unit"
    ]
  },
  "dependentSchemas": {
    "sequencing_depth": {
      "properties": {
        "platform": {
          "type": "string",
          "description": "Sequencing platform when depth is reported."
        }
      }
    }
  },
  "if": {
    "properties": {
      "specimen_type": {
        "const": "isolate"
      }
    }
  },
  "then": {
    "required": [
      "strain_name"
    ],
    "properties": {
      "strain_name": {
        "type": "string",
        "description": "Cultured isolate strain identifier."
      }
    }
  },
  "else": {
    "required": [
      "biome"
    ],
    "properties": {
      "biome": {
        "type": "string",
        "description": "Environmental biome for metagenome samples."
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "id": "cond1",
        "specimen_type": "isolate",
        "sequencing_depth": 120.5,
        "depth_unit": "x",
        "platform": "Illumina NovaSeq",
        "strain_name": "K-12 MG1655"
    },
    {
        "id": "cond2",
        "specimen_type": "metagenome",
        "biome": "marine pelagic"
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "id": "cond1",
        "specimen_type": "isolate",
        "sequencing_depth": 120.5,
        "depth_unit": "x",
        "platform": "Illumina NovaSeq",
        "strain_name": "K-12 MG1655"
    },
    {
        "id": "cond2",
        "specimen_type": "metagenome",
        "biome": "marine pelagic"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/08_conditional_and_dependent_schemas/expected_schema.py))

```python
"""Expected PyArrow schema for 08_conditional_and_dependent_schemas."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("specimen_type", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("sequencing_depth", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("depth_unit", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "platform",
            pa.string(),
            nullable=True,
            metadata={"comment": "Sequencing platform when depth is reported.", "jsonschema_type": "string"},
        ),
        pa.field(
            "strain_name",
            pa.string(),
            nullable=True,
            metadata={"comment": "Cultured isolate strain identifier.", "jsonschema_type": "string"},
        ),
        pa.field(
            "biome",
            pa.string(),
            nullable=True,
            metadata={"comment": "Environmental biome for metagenome samples.", "jsonschema_type": "string"},
        ),
    ]
)
```

---

<a id="case-spec-09-context-dependent-shared-defs"></a>

### Context-Dependent Shared Definitions (`spec/09_context_dependent_shared_defs`)

**Rules:** [R-SHAPE-REF](#r-shape-ref), [R-NULL-REQUIRED](#r-null-required), [R-NULL-ALLOWS-NULL](#r-null-allows-null), [R-NULL-ITEMS](#r-null-items), [R-META-COMMENT](#r-meta-comment)

One `#/$defs/Tag` used as a required property (`primary_tag`), an optional property (`secondary_tag`), a
required-but-nullable `anyOf` property (`fallback_tag`), an array item (`tag_list`) and a map value
(`tag_lookup`).

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/09_context_dependent_shared_defs.schema.json",
  "title": "ContextDependentSharedDefsRecord",
  "type": "object",
  "required": [
    "id",
    "primary_tag",
    "fallback_tag"
  ],
  "properties": {
    "id": {
      "type": "string"
    },
    "primary_tag": {
      "$ref": "#/$defs/Tag"
    },
    "secondary_tag": {
      "$ref": "#/$defs/Tag"
    },
    "fallback_tag": {
      "anyOf": [
        {
          "$ref": "#/$defs/Tag"
        },
        {
          "type": "null"
        }
      ]
    },
    "tag_list": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/Tag"
      }
    },
    "tag_lookup": {
      "type": "object",
      "additionalProperties": {
        "$ref": "#/$defs/Tag"
      }
    }
  },
  "$defs": {
    "Tag": {
      "type": "object",
      "description": "Reusable key-value tag structure.",
      "required": [
        "key"
      ],
      "properties": {
        "key": {
          "type": "string"
        },
        "value": {
          "type": "string"
        }
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "id": "ctx1",
        "primary_tag": {
            "key": "domain",
            "value": "Bacteria"
        },
        "secondary_tag": {
            "key": "phylum",
            "value": "Proteobacteria"
        },
        "fallback_tag": {
            "key": "status",
            "value": "curated"
        },
        "tag_list": [
            {
                "key": "gram",
                "value": "negative"
            },
            {
                "key": "motile"
            }
        ],
        "tag_lookup": {
            "ncbi": {
                "key": "taxid",
                "value": "562"
            }
        }
    },
    {
        "id": "ctx2",
        "primary_tag": {
            "key": "domain",
            "value": "Archaea"
        },
        "fallback_tag": null
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "id": "ctx1",
        "primary_tag": {
            "key": "domain",
            "value": "Bacteria"
        },
        "secondary_tag": {
            "key": "phylum",
            "value": "Proteobacteria"
        },
        "fallback_tag": {
            "key": "status",
            "value": "curated"
        },
        "tag_list": [
            {
                "key": "gram",
                "value": "negative"
            },
            {
                "key": "motile"
            }
        ],
        "tag_lookup": {
            "ncbi": {
                "key": "taxid",
                "value": "562"
            }
        }
    },
    {
        "id": "ctx2",
        "primary_tag": {
            "key": "domain",
            "value": "Archaea"
        },
        "fallback_tag": null
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/09_context_dependent_shared_defs/expected_schema.py))

```python
"""Expected PyArrow schema for 09_context_dependent_shared_defs."""

# ruff: noqa: INP001

import pyarrow as pa

_TAG_STRUCT = pa.struct(
    [
        pa.field("key", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("value", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "primary_tag",
            _TAG_STRUCT,
            nullable=False,
            metadata={"comment": "Reusable key-value tag structure."},
        ),
        pa.field(
            "secondary_tag",
            _TAG_STRUCT,
            nullable=True,
            metadata={"comment": "Reusable key-value tag structure."},
        ),
        pa.field(
            "fallback_tag",
            _TAG_STRUCT,
            nullable=True,
            metadata={"comment": "Reusable key-value tag structure."},
        ),
        pa.field(
            "tag_list",
            pa.list_(
                pa.field(
                    "item",
                    _TAG_STRUCT,
                    nullable=False,
                    metadata={"comment": "Reusable key-value tag structure."},
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "tag_lookup",
            pa.map_(
                pa.string(),
                pa.field(
                    "value",
                    _TAG_STRUCT,
                    nullable=False,
                    metadata={"comment": "Reusable key-value tag structure."},
                ),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-10-combinations"></a>

### Combinations (`spec/10_combinations`)

**Rules:** [R-ORDER](#r-order), [R-SCALAR-TYPE](#r-scalar-type), [R-UNION-SCALAR](#r-union-scalar), [R-UNION-OBJECT](#r-union-object), [R-SHAPE-TUPLE](#r-shape-tuple), [R-SHAPE-MAP](#r-shape-map), [R-RECURSION](#r-recursion), [R-NULL-CONDITIONAL](#r-null-conditional), [R-SCALAR-ENUM-CONST](#r-scalar-enum-const), [R-COERCE-SCALAR](#r-coerce-scalar)

Stress test combining `allOf` inheritance, `const`, date / date-time / Base64 scalars, a nullable
`["number", "string", "null"]` union, a closed tuple, a string map, a `oneOf` object choice, a recursive
`TaxNode` and a `dependentSchemas` property.

#### Schema (`schema.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.org/spec/10_combinations.schema.json",
  "title": "ComprehensiveCombinationsRecord",
  "type": "object",
  "allOf": [
    {
      "$ref": "#/$defs/AuditHeader"
    }
  ],
  "required": [
    "record_id",
    "active",
    "count",
    "ratio",
    "release_date",
    "range_tuple"
  ],
  "properties": {
    "record_id": {
      "type": "string",
      "description": "Primary record identifier."
    },
    "active": {
      "type": "boolean"
    },
    "count": {
      "type": "integer",
      "minimum": 0
    },
    "ratio": {
      "type": "number",
      "multipleOf": 0.001
    },
    "release_date": {
      "type": "string",
      "format": "date"
    },
    "timestamp_str": {
      "type": "string",
      "format": "date-time"
    },
    "payload_b64": {
      "type": "string",
      "contentEncoding": "base64"
    },
    "score_or_label": {
      "type": [
        "number",
        "string",
        "null"
      ]
    },
    "range_tuple": {
      "type": "array",
      "minItems": 2,
      "prefixItems": [
        {
          "type": "integer"
        },
        {
          "type": "integer"
        },
        {
          "type": "string"
        }
      ],
      "items": false
    },
    "tags": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "annotations": {
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "source_choice": {
      "oneOf": [
        {
          "type": "object",
          "required": [
            "kind",
            "accession"
          ],
          "properties": {
            "kind": {
              "type": "string",
              "const": "genbank"
            },
            "accession": {
              "type": "string"
            }
          }
        },
        {
          "type": "object",
          "required": [
            "kind",
            "doi"
          ],
          "properties": {
            "kind": {
              "type": "string",
              "const": "publication"
            },
            "doi": {
              "type": "string"
            }
          }
        }
      ]
    },
    "lineage_tree": {
      "$ref": "#/$defs/TaxNode"
    }
  },
  "dependentSchemas": {
    "payload_b64": {
      "properties": {
        "payload_sha256": {
          "type": "string"
        }
      }
    }
  },
  "$defs": {
    "AuditHeader": {
      "type": "object",
      "required": [
        "schema_version"
      ],
      "properties": {
        "schema_version": {
          "const": 1
        }
      }
    },
    "TaxNode": {
      "type": "object",
      "required": [
        "taxon_name"
      ],
      "properties": {
        "taxon_name": {
          "type": "string"
        },
        "rank": {
          "type": "string"
        },
        "children": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/TaxNode"
          }
        }
      }
    }
  }
}
```

#### Input (`data.json`)

```json
[
    {
        "schema_version": 1,
        "record_id": "comb1",
        "active": true,
        "count": 42,
        "ratio": 0.875,
        "release_date": "2025-08-15",
        "timestamp_str": "2025-08-15T09:00:00Z",
        "payload_b64": "Q0RN",
        "payload_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "score_or_label": 98.5,
        "range_tuple": [
            100,
            500,
            "+"
        ],
        "tags": [
            "complete",
            "reference"
        ],
        "annotations": {
            "bioproject": "PRJNA225",
            "biosample": "SAMN02604091"
        },
        "source_choice": {
            "kind": "genbank",
            "accession": "GCF_000005845.2"
        },
        "lineage_tree": {
            "taxon_name": "Bacteria",
            "rank": "domain",
            "children": [
                {
                    "taxon_name": "Proteobacteria",
                    "rank": "phylum",
                    "children": [
                        {
                            "taxon_name": "Gammaproteobacteria",
                            "rank": "class"
                        }
                    ]
                }
            ]
        }
    },
    {
        "schema_version": 1,
        "record_id": "comb2",
        "active": false,
        "count": 0,
        "ratio": 0.0,
        "release_date": "2025-01-01",
        "score_or_label": null,
        "range_tuple": [
            1,
            10
        ],
        "source_choice": {
            "kind": "publication",
            "doi": "10.1038/s41586-025-00001-x"
        }
    }
]
```

#### Converted rows: run `default`

```json
[
    {
        "schema_version": 1,
        "record_id": "comb1",
        "active": true,
        "count": 42,
        "ratio": 0.875,
        "release_date": "2025-08-15",
        "timestamp_str": "2025-08-15T09:00:00Z",
        "payload_b64": "CDM",
        "payload_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "score_or_label": "98.5",
        "range_tuple": {
            "item_0": 100,
            "item_1": 500,
            "item_2": "+"
        },
        "tags": [
            "complete",
            "reference"
        ],
        "annotations": {
            "bioproject": "PRJNA225",
            "biosample": "SAMN02604091"
        },
        "source_choice": {
            "kind": "genbank",
            "accession": "GCF_000005845.2"
        },
        "lineage_tree": {
            "taxon_name": "Bacteria",
            "rank": "domain",
            "children": [
                {
                    "taxon_name": "Proteobacteria",
                    "rank": "phylum",
                    "children": [
                        "{\"taxon_name\": \"Gammaproteobacteria\", \"rank\": \"class\"}"
                    ]
                }
            ]
        }
    },
    {
        "schema_version": 1,
        "record_id": "comb2",
        "active": false,
        "count": 0,
        "ratio": 0.0,
        "release_date": "2025-01-01",
        "score_or_label": null,
        "range_tuple": {
            "item_0": 1,
            "item_1": 10
        },
        "source_choice": {
            "kind": "publication",
            "doi": "10.1038/s41586-025-00001-x"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/jsonschema/spec/10_combinations/expected_schema.py))

```python
"""Expected PyArrow schema for 10_combinations."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "record_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Primary record identifier.", "jsonschema_type": "string"},
        ),
        pa.field("active", pa.bool_(), nullable=False, metadata={"jsonschema_type": "boolean"}),
        pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("ratio", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("release_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
        pa.field("timestamp_str", pa.string(), nullable=True, metadata={"jsonschema_type": "string (date-time)"}),
        pa.field("payload_b64", pa.binary(), nullable=True, metadata={"jsonschema_type": "string (base64)"}),
        pa.field("score_or_label", pa.string(), nullable=True, metadata={"jsonschema_type": "number | string"}),
        pa.field(
            "range_tuple",
            pa.struct(
                [
                    pa.field("item_0", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                    pa.field("item_1", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                    pa.field("item_2", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "annotations",
            pa.map_(
                pa.string(),
                pa.field("value", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "source_choice",
            pa.struct(
                [
                    pa.field("kind", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("doi", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "lineage_tree",
            pa.struct(
                [
                    pa.field("taxon_name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("rank", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field(
                        "children",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "taxon_name",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "rank",
                                            pa.string(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "children",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=False),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field("payload_sha256", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)
```

---

<a id="case-examples-cts-job"></a>

### KBase CTS `Job` (`examples/cts_job`)

**Rules:** [R-UNION-SCALAR](#r-union-scalar), [R-NULL-ALLOWS-NULL](#r-null-allows-null), [R-SHAPE-MAP](#r-shape-map), [R-SHAPE-REF](#r-shape-ref)

`/jobs/{job_id}` response schema from `not_for_github/cts/openapi.json` (`#/components/schemas/Job`), with nested
`JobInput`, `JobImage`, `RefData` / `S3FileWithDataID`, a dynamic `params` map and `anyOf` nullability.

**Schema:** [`tests/data/jsonschema/examples/cts_job/schema.json`](../tests/data/jsonschema/examples/cts_job/schema.json)

- Run `minimal`: [`minimal.json`](../tests/data/jsonschema/examples/cts_job/minimal.json), [`minimal_expected.json`](../tests/data/jsonschema/examples/cts_job/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/jsonschema/examples/cts_job/minimal_expected_table.py)
- Run `maximal`: [`maximal.json`](../tests/data/jsonschema/examples/cts_job/maximal.json), [`maximal_expected.json`](../tests/data/jsonschema/examples/cts_job/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/jsonschema/examples/cts_job/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/jsonschema/examples/cts_job/expected_schema.py)

---

<a id="case-examples-cts-jobs"></a>

### KBase CTS `ListJobsResponse` (`examples/cts_jobs`)

**Rules:** [R-SHAPE-ARRAY](#r-shape-array), [R-SHAPE-REF](#r-shape-ref)

`/jobs` response schema from `not_for_github/cts/openapi.json` (`#/components/schemas/ListJobsResponse`): an
array of `JobPreview` structs with `JobInputPreview`, `JobImage` and state transition timestamps.

**Schema:** [`tests/data/jsonschema/examples/cts_jobs/schema.json`](../tests/data/jsonschema/examples/cts_jobs/schema.json)

- Run `minimal`: [`minimal.json`](../tests/data/jsonschema/examples/cts_jobs/minimal.json), [`minimal_expected.json`](../tests/data/jsonschema/examples/cts_jobs/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/jsonschema/examples/cts_jobs/minimal_expected_table.py)
- Run `maximal`: [`maximal.json`](../tests/data/jsonschema/examples/cts_jobs/maximal.json), [`maximal_expected.json`](../tests/data/jsonschema/examples/cts_jobs/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/jsonschema/examples/cts_jobs/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/jsonschema/examples/cts_jobs/expected_schema.py)

---

<a id="case-examples-ncbi-annotation-report"></a>

### NCBI Datasets `AnnotationReportResponse` (`examples/ncbi_annotation_report`)

**Rules:** [R-ROOT-REF](#r-root-ref), [R-SHAPE-REF](#r-shape-ref), [R-SHAPE-ARRAY](#r-shape-array), [R-SCALAR-ENUM-CONST](#r-scalar-enum-const)

Deeply nested NCBI Datasets v2 schema (`parsers/ncbi/datasets_api/AnnotationReportResponse.schema.json`) with a
root `$ref`, many `$defs`, arrays of structs, enums, integers and booleans.

**Schema:** [`tests/data/jsonschema/examples/ncbi_annotation_report/schema.json`](../tests/data/jsonschema/examples/ncbi_annotation_report/schema.json)

- Run `minimal`: [`minimal.json`](../tests/data/jsonschema/examples/ncbi_annotation_report/minimal.json), [`minimal_expected.json`](../tests/data/jsonschema/examples/ncbi_annotation_report/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/jsonschema/examples/ncbi_annotation_report/minimal_expected_table.py)
- Run `maximal`: [`maximal.json`](../tests/data/jsonschema/examples/ncbi_annotation_report/maximal.json), [`maximal_expected.json`](../tests/data/jsonschema/examples/ncbi_annotation_report/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/jsonschema/examples/ncbi_annotation_report/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/jsonschema/examples/ncbi_annotation_report/expected_schema.py)

---

<a id="case-examples-uniprot-search"></a>

### UniProtKB `SearchResult` (`examples/uniprot_search`)

**Rules:** [R-UNION-OBJECT](#r-union-object), [R-UNION-SCALAR-OBJECT](#r-union-scalar-object), [R-RECURSION](#r-recursion), [R-SHAPE-MAP](#r-shape-map), [R-SCALAR-TYPE](#r-scalar-type)

`/uniprotkb/search` response schema from `not_for_github/uniprot/api/openapi-3.1.json`
(`#/components/schemas/SearchResult`): a 24-branch polymorphic `Comment` `anyOf` (including a string + object
`note` with a synthesized `_value`), recursive `ProteinSection`, `extraAttributes` maps and `pa.date32()` dates.

**Schema:** [`tests/data/jsonschema/examples/uniprot_search/schema.json`](../tests/data/jsonschema/examples/uniprot_search/schema.json)

- Run `minimal`: [`minimal.json`](../tests/data/jsonschema/examples/uniprot_search/minimal.json), [`minimal_expected.json`](../tests/data/jsonschema/examples/uniprot_search/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/jsonschema/examples/uniprot_search/minimal_expected_table.py)
- Run `maximal`: [`maximal.json`](../tests/data/jsonschema/examples/uniprot_search/maximal.json), [`maximal_expected.json`](../tests/data/jsonschema/examples/uniprot_search/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/jsonschema/examples/uniprot_search/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/jsonschema/examples/uniprot_search/expected_schema.py)

---

## 3. Errors (Unsupported / Invalid Input)

Schema generation raises a subclass of `JSONSchemaToPyArrowError` (itself a `ValueError`).

| Code | Exception | Condition | Reason | Fixtures |
| :--- | :--- | :--- | :--- | :--- |
| **E1** | `UnsupportedDialectError` | Missing or unsupported `$schema` dialect | Only JSON Schema Draft 2020-12 and the KBase Draft 2020-12 XSV metaschema are supported. | [`missing_schema_dialect.json`](../tests/data/jsonschema/spec/errors/missing_schema_dialect.json): Declares the Draft-07 dialect URI.<br>[`missing_schema_keyword.json`](../tests/data/jsonschema/spec/errors/missing_schema_keyword.json): No `$schema` keyword. |
| **E2** | `NonStructRootError` | Non-struct top-level schema | A `pa.Schema` needs top-level columns, so the root must resolve to a fixed-property object. | [`scalar_top_level.json`](../tests/data/jsonschema/spec/errors/scalar_top_level.json): Root is `{"type": "string"}`.<br>[`array_top_level.json`](../tests/data/jsonschema/spec/errors/array_top_level.json): Root is `{"type": "array", "items": {"type": "integer"}}`.<br>[`map_top_level.json`](../tests/data/jsonschema/spec/errors/map_top_level.json): Root is a pure map (`additionalProperties: {"type": "string"}`). |
| **E3** | `EmptyObjectError` | Empty object or tuple (zero fields) | Parquet cannot store a `pa.struct([])` with no child fields. | [`empty_object_top_level.json`](../tests/data/jsonschema/spec/errors/empty_object_top_level.json): Root object has `properties: {}`.<br>[`empty_object_nested.json`](../tests/data/jsonschema/spec/errors/empty_object_nested.json): Nested property has `properties: {}`. |
| **E4** | `UnconstrainedSchemaError` | Unconstrained or boolean schema | An untyped JSON value cannot be assigned a concrete Arrow `DataType`. | [`unconstrained_property.json`](../tests/data/jsonschema/spec/errors/unconstrained_property.json): Property schema is `{}`.<br>[`boolean_property_schema.json`](../tests/data/jsonschema/spec/errors/boolean_property_schema.json): Property schema is `true`. |
| **E5** | `OpenTupleError` | Open tuple (`prefixItems` with schema-valued `items`) | A field cannot be both a positional `pa.struct` and an unbounded `pa.list_`. | [`open_tuple_prefix_items.json`](../tests/data/jsonschema/spec/errors/open_tuple_prefix_items.json): `prefixItems` with `items: {"type": "boolean"}`. |
| **E6** | `MixedPropertiesAndMapError` | Fixed `properties` mixed with a dynamic map | An object cannot be both a fixed-field `pa.struct` and a dynamic `pa.map_`. | [`mixed_properties_and_map.json`](../tests/data/jsonschema/spec/errors/mixed_properties_and_map.json): `properties` together with `additionalProperties: {"type": "integer"}`. |
| **E7** | `NegationError` | Negation (`not`) | A complement type does not define a positive storage `DataType`. | [`negation_not_keyword.json`](../tests/data/jsonschema/spec/errors/negation_not_keyword.json): Property uses `"not": {"type": "string"}`. |
| **E8** | `IncompatibleBranchShapesError` | Incompatible structural union branches | Branches with different structural shapes (object vs array, array vs scalar, object vs map) cannot share one column. | [`incompatible_branch_shapes.json`](../tests/data/jsonschema/spec/errors/incompatible_branch_shapes.json): `oneOf` with an object branch and an array branch. |
| **E9** | `UnsupportedRefError` | Unresolvable or unsupported reference | Every `$ref` must resolve to a local `#/$defs/...` definition; `$dynamicRef` and `$recursiveRef` are not supported. | [`unresolvable_ref.json`](../tests/data/jsonschema/spec/errors/unresolvable_ref.json): `$ref` to the missing `#/$defs/DoesNotExist`.<br>[`dynamic_ref.json`](../tests/data/jsonschema/spec/errors/dynamic_ref.json): Property uses `$dynamicRef`. |
| **E10** | `RecursiveNonJsonTypeError` | Non-JSON-serializable type in a recursive `$ref` cycle | Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`. | [`recursive_date_type.json`](../tests/data/jsonschema/spec/errors/recursive_date_type.json): Recursive `DatedNode` has a `format: "date"` property.<br>[`recursive_binary_type.json`](../tests/data/jsonschema/spec/errors/recursive_binary_type.json): Recursive `BlobNode` has a `contentEncoding: "base64"` property. |
| **E11** | `NullOnlySchemaError` | Null-only schema | Parquet has no physical storage type for a `pa.null()` column. | [`null_only_field.json`](../tests/data/jsonschema/spec/errors/null_only_field.json): Property is `{"type": "null"}`.<br>[`const_null_field.json`](../tests/data/jsonschema/spec/errors/const_null_field.json): Property is `{"const": null}`.<br>[`enum_null_field.json`](../tests/data/jsonschema/spec/errors/enum_null_field.json): Property is `{"enum": [null]}`. |
| **E12** | `ValueFieldCollisionError` | `_value` collision in a scalar + object union | The synthesized `_value` field of a scalar + object union would collide with a declared `_value` property. | [`value_field_collision.json`](../tests/data/jsonschema/spec/errors/value_field_collision.json): Object branch of a string + object `anyOf` declares `_value`. |

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suite and this document are both driven by it.

| Case | Schema | Runs |
| :--- | :--- | :--- |
| [`spec/01_scalars_and_formats`](../tests/data/jsonschema/spec/01_scalars_and_formats/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/01_scalars_and_formats/schema.json) | default |
| [`spec/02_nullability_and_required`](../tests/data/jsonschema/spec/02_nullability_and_required/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/02_nullability_and_required/schema.json) | default |
| [`spec/03_arrays_and_tuples`](../tests/data/jsonschema/spec/03_arrays_and_tuples/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/03_arrays_and_tuples/schema.json) | default |
| [`spec/04_objects_and_maps`](../tests/data/jsonschema/spec/04_objects_and_maps/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/04_objects_and_maps/schema.json) | default |
| [`spec/05_defs_and_refs`](../tests/data/jsonschema/spec/05_defs_and_refs/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/05_defs_and_refs/schema.json) | default |
| [`spec/06_recursion_depth`](../tests/data/jsonschema/spec/06_recursion_depth/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/06_recursion_depth/schema.json) | depth-2, depth-1 |
| [`spec/07_combinators_allof_anyof_oneof`](../tests/data/jsonschema/spec/07_combinators_allof_anyof_oneof/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/07_combinators_allof_anyof_oneof/schema.json) | default |
| [`spec/08_conditional_and_dependent_schemas`](../tests/data/jsonschema/spec/08_conditional_and_dependent_schemas/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/08_conditional_and_dependent_schemas/schema.json) | default |
| [`spec/09_context_dependent_shared_defs`](../tests/data/jsonschema/spec/09_context_dependent_shared_defs/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/09_context_dependent_shared_defs/schema.json) | default |
| [`spec/10_combinations`](../tests/data/jsonschema/spec/10_combinations/case.yaml) | [`schema.json`](../tests/data/jsonschema/spec/10_combinations/schema.json) | default |
| [`examples/cts_job`](../tests/data/jsonschema/examples/cts_job/case.yaml) | [`schema.json`](../tests/data/jsonschema/examples/cts_job/schema.json) | minimal, maximal |
| [`examples/cts_jobs`](../tests/data/jsonschema/examples/cts_jobs/case.yaml) | [`schema.json`](../tests/data/jsonschema/examples/cts_jobs/schema.json) | minimal, maximal |
| [`examples/ncbi_annotation_report`](../tests/data/jsonschema/examples/ncbi_annotation_report/case.yaml) | [`schema.json`](../tests/data/jsonschema/examples/ncbi_annotation_report/schema.json) | minimal, maximal |
| [`examples/uniprot_search`](../tests/data/jsonschema/examples/uniprot_search/case.yaml) | [`schema.json`](../tests/data/jsonschema/examples/uniprot_search/schema.json) | minimal, maximal |
