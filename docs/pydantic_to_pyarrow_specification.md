<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/schema_converters/pydantic_to_pyarrow/rules.yaml`, `specs/schema_converters/pydantic_to_pyarrow/spec`, `specs/schema_converters/pydantic_to_pyarrow/examples`. Regenerate with `uv run python -m scripts.gen_converter_spec`. -->

# Pydantic v2 to PyArrow Specification

This document specifies how a **Pydantic v2 `BaseModel`** class determines:

1. The **PyArrow `pa.Schema`** produced by `generate_pyarrow_schema`.
2. The **Python row dictionaries** produced by `coerce_record_for_pyarrow` (and `coerce_model_for_pyarrow`)
   from validated `BaseModel` instances or JSON-compatible dictionaries, ready for
   `pa.Table.from_pylist(rows, schema=pa_schema)` and Parquet serialization.
3. The **errors** raised for constructs that cannot be represented as a static columnar schema.

**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · [3. Errors](#3-errors-unsupported-invalid-input) · [4. Fixture Index](#4-fixture-index)

## 1. Conversion Rules

### 1.1 Top-Level Model Resolution and Unwrapping

<a id="r-model-input"></a>

- **R-MODEL-INPUT**: The input to `generate_pyarrow_schema` must be a concrete `pydantic.BaseModel` subclass (not an
  instance, stdlib dataclass, `TypedDict`, scalar type, or unparameterized generic with unresolved
  `TypeVar`s); anything else raises **E1**.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-root-model-unwrap"></a>

- **R-ROOT-MODEL-UNWRAP**: A `RootModel[T]` whose inner `root` type `T` is a `BaseModel` (or union of `BaseModel`s) unwraps to
  `T` before root classification. A `RootModel` wrapping a scalar, sequence, or `dict` raises **E2**.

  *Cases:* [05_inheritance_generics_and_root_model](#case-spec-05-inheritance-generics-and-root-model)

<a id="r-unwrap"></a>

- **R-UNWRAP**: The resolved root model must produce at least one column (from `model_fields` or
  `model_computed_fields`). A model with zero fields raises **E3**.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-model-inheritance"></a>

- **R-MODEL-INHERITANCE**: Subclasses inherit fields from base `BaseModel` classes in MRO / `model_fields` order; overridden
  fields take the subclass's annotation and metadata.

  *Cases:* [05_inheritance_generics_and_root_model](#case-spec-05-inheritance-generics-and-root-model)

<a id="r-generic-model"></a>

- **R-GENERIC-MODEL**: Parameterized generic models (`class Envelope(BaseModel, Generic[T]): ...` instantiated as
  `Envelope[Payload]`) resolve concrete type arguments; an unparameterized `TypeVar` raises **E1**.

  *Cases:* [05_inheritance_generics_and_root_model](#case-spec-05-inheritance-generics-and-root-model)

### 1.2 Naming, Ordering, and Aliases

<a id="r-name-field"></a>

- **R-NAME-FIELD**: By default (`by_alias=False`), Python field attribute names are used verbatim. When `by_alias=True`,
  `FieldInfo.alias` (or `serialization_alias` if set) is used. String `validation_alias` values are
  preserved as metadata when applicable; non-string `AliasPath`/`AliasChoices` do not rename the Arrow
  column.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [08_aliases_and_field_names](#case-spec-08-aliases-and-field-names), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-order"></a>

- **R-ORDER**: Struct fields are emitted in `model.model_fields` declaration order, followed by
  `model.model_computed_fields` in declaration order, followed by fields contributed by union branches
  in branch declaration order. A field seen more than once keeps the position of its first occurrence.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [05_inheritance_generics_and_root_model](#case-spec-05-inheritance-generics-and-root-model), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-computed-fields"></a>

- **R-COMPUTED-FIELDS**: `@computed_field` properties with an explicit return type annotation are included in the schema
  using their return type; a `@computed_field` without a return type annotation raises **E7**.

  *Cases:* [02_nullability_defaults_and_computed](#case-spec-02-nullability-defaults-and-computed), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap)

<a id="r-merge-duplicate"></a>

- **R-MERGE-DUPLICATE**: When a field name appears across multiple `BaseModel` branches of a `Union` (`A | B`), identical
  types collapse to one; differing types are combined using the union rules
  ([R-UNION-SCALAR](#r-union-scalar), [R-UNION-MODEL](#r-union-model)). The first non-empty
  `description` is kept.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

### 1.3 Construct Classification (Shape)

<a id="r-shape-model"></a>

- **R-SHAPE-MODEL**: A nested `BaseModel`, `TypedDict`, or stdlib `@dataclass` maps to `pa.struct([...])`.
  `model_config["extra"] = "ignore"` or `"forbid"` is accepted; `model_config["extra"] = "allow"` on a
  model with fixed fields raises **E6**.

  *Cases:* [04_nested_models_and_maps](#case-spec-04-nested-models-and-maps), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-shape-sequence"></a>

- **R-SHAPE-SEQUENCE**: `list[T]`, `Sequence[T]`, `set[T]`, `frozenset[T]`, `deque[T]`, and homogeneous variadic
  `tuple[T, ...]` map to `pa.list_(pa.field("item", <item type>))`; nested sequences nest lists.
  Unparameterized `list`, `set`, or `tuple` raises **E7**.

  *Cases:* [03_sequences_sets_and_tuples](#case-spec-03-sequences-sets-and-tuples), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-shape-tuple"></a>

- **R-SHAPE-TUPLE**: A fixed-length positional tuple `tuple[T_0, T_1, ..., T_{k-1}]` (without `...`) maps to `pa.struct`
  with fields `item_0` ... `item_{k-1}`. Empty `tuple[()]` raises **E3**; mixed prefix + variadic
  `tuple[int, *tuple[str, ...]]` raises **E5**.

  *Cases:* [03_sequences_sets_and_tuples](#case-spec-03-sequences-sets-and-tuples)

<a id="r-shape-map"></a>

- **R-SHAPE-MAP**: `dict[K, V]` and `Mapping[K, V]` where `K` is `str` (or a `StrEnum` / string subclass) map to
  `pa.map_(pa.string(), pa.field("value", <value type>))`. Non-string map keys `K` (e.g. `dict[int, V]`)
  raise **E6**; unparameterized `dict` raises **E7**.

  *Cases:* [04_nested_models_and_maps](#case-spec-04-nested-models-and-maps), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap)

<a id="r-recursion"></a>

- **R-RECURSION**: Each `BaseModel` class may be expanded at most `max_recursion_depth` times (default `2`) along an
  ancestor path. A deeper reference collapses to `pa.string()` (or `pa.list_(pa.string())` inside a
  list), carries no `pydantic_type`, and its runtime value is serialized with `json.dumps`. Applies to
  self-recursion and mutual recursion (`A -> B -> A`).

  *Cases:* [10_bounded_recursion](#case-spec-10-bounded-recursion)

### 1.4 Field Nullability

<a id="r-null-required"></a>

- **R-NULL-REQUIRED**: A field is `nullable=False` when its annotation does not allow `None` (even if it has a non-`None`
  `default` or `default_factory`) and, for union-of-models, it is present and non-nullable in every
  union branch.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [02_nullability_defaults_and_computed](#case-spec-02-nullability-defaults-and-computed), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-null-optional"></a>

- **R-NULL-OPTIONAL**: A field is `nullable=True` when `None` is in its type union (`T | None` or `Optional[T]`), when its
  `default` is `None`, or when its type is `Literal[None]` (**E7** if `None` is the only type).

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [02_nullability_defaults_and_computed](#case-spec-02-nullability-defaults-and-computed), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-null-default"></a>

- **R-NULL-DEFAULT**: A non-`None` `default` or `default_factory` (e.g. `count: int = 0` or `tags: list[str] = Field(default_factory=list)`)
  leaves the field `nullable=False` because Pydantic guarantees a non-`None` value on validated
  instances.

  *Cases:* [02_nullability_defaults_and_computed](#case-spec-02-nullability-defaults-and-computed)

<a id="r-null-union-branch"></a>

- **R-NULL-UNION-BRANCH**: Fields present in only a subset of `BaseModel` branches of a `Union[M1, M2]` (or discriminated union)
  are `nullable=True`.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

<a id="r-null-items"></a>

- **R-NULL-ITEMS**: A list `item` field and a map `value` field are `nullable=False` unless the element/value type is
  `T | None`. A fixed-tuple field `item_i` is `nullable=False` unless `T_i` is `U | None`.

  *Cases:* [03_sequences_sets_and_tuples](#case-spec-03-sequences-sets-and-tuples), [04_nested_models_and_maps](#case-spec-04-nested-models-and-maps)

### 1.5 Scalar Types, Annotations, Enums, and Unions

<a id="r-scalar-type"></a>

- **R-SCALAR-TYPE**: Built-in Python scalars and Pydantic network/string/path/UUID types map to Arrow types per the table
  below. Validation constraints (`gt`, `ge`, `lt`, `le`, `multiple_of`, `min_length`, `max_length`,
  `pattern`, `StrictInt`, `PositiveInt`, etc.) do not change the physical Arrow type (except `Decimal`
  precision/scale).

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [09_annotated_and_custom_types](#case-spec-09-annotated-and-custom-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-scalar-decimal"></a>

- **R-SCALAR-DECIMAL**: `Decimal` with explicit `Field(max_digits=p, decimal_places=s)` (or `condecimal`) where
  `1 <= p <= 38` and `0 <= s <= p` maps to `pa.decimal128(p, s)` (`pydantic_type="decimal"`). An
  unconstrained `Decimal` (or one with `max_digits` without `decimal_places`) maps to `pa.float64()`
  (`pydantic_type="number"`). Precision `p > 38` or `s > p` raises **E11**.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [09_annotated_and_custom_types](#case-spec-09-annotated-and-custom-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap)

<a id="r-scalar-temporal"></a>

- **R-SCALAR-TEMPORAL**: `datetime.date` (`PastDate`, `FutureDate`) maps to `pa.date32()`. `datetime.datetime`
  (`AwareDatetime`, `PastDatetime`, `FutureDatetime`) maps to `pa.timestamp("us", tz="UTC")`.
  `NaiveDatetime` maps to timezone-naive `pa.timestamp("us")`. `datetime.time` maps to
  `pa.time64("us")`. `datetime.timedelta` maps to `pa.string()` (ISO-8601 duration string).

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-scalar-binary"></a>

- **R-SCALAR-BINARY**: `bytes`, `bytearray`, `SecretBytes`, and `Base64Bytes` map to `pa.binary()`. `Base64Str` maps to
  `pa.string()`.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types)

<a id="r-scalar-annotated"></a>

- **R-SCALAR-ANNOTATED**: `Annotated[T, ...]` and `type Alias = ...` (`TypeAliasType`) unwrap to `T` while merging any
  `FieldInfo` or `annotated_types` metadata attached in the `Annotated` arguments (outermost metadata
  wins on conflict).

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [09_annotated_and_custom_types](#case-spec-09-annotated-and-custom-types)

<a id="r-scalar-enum-literal"></a>

- **R-SCALAR-ENUM-LITERAL**: `StrEnum` maps to `pa.string()`; `IntEnum` maps to `pa.int64()`. General `Enum` subclasses infer the
  Arrow type from the types of all member `.value`s using the scalar union rules. `Literal[v1, v2, ...]`
  infers the type of each non-`None` literal (`bool`, `int`, `float`, `str`, `bytes`, `Enum`) and
  combines them with the scalar union rules.

  *Cases:* [06_enums_and_literals](#case-spec-06-enums-and-literals), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-union-scalar"></a>

- **R-UNION-SCALAR**: For `Union[T1, T2, ...]` (`T1 | T2`) of scalar types, `NoneType` is stripped (setting
  `nullable=True`). If the remaining types share one Arrow type, that type is used; if they are a mix
  of `int` and `float` (or unconstrained `Decimal`), the type is `pa.float64()`; otherwise
  `pa.string()`. `metadata["pydantic_type"]` joins distinct member labels with `" | "` in declaration
  order.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

<a id="r-union-model"></a>

- **R-UNION-MODEL**: A union whose non-`None` branches are all `BaseModel`s (or `TypedDict` / `@dataclass`) merges their
  fields into a single `pa.struct` ([R-ORDER](#r-order), [R-MERGE-DUPLICATE](#r-merge-duplicate),
  [R-NULL-UNION-BRANCH](#r-null-union-branch)). Unions mixing lists, maps, or incompatible container
  shapes raise **E8**.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

<a id="r-union-discriminated"></a>

- **R-UNION-DISCRIMINATED**: A discriminated union (`Annotated[A | B, Field(discriminator="kind")]`) merges all variant models
  into a single `pa.struct`; the discriminator field ( typically `Literal[...]` on each branch) is merged
  across all branches and is `nullable=False` when required in every branch.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

<a id="r-union-scalar-model"></a>

- **R-UNION-SCALAR-MODEL**: A union mixing `BaseModel` branch(es) and scalar branch(es) produces a `pa.struct` of the merged
  model fields (all `nullable=True`) followed by a synthesized `_value` field holding the unified
  scalar type (`nullable=True`). If any model branch declares a field named `_value`, raises **E12**.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

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

### 1.6 Field Metadata

<a id="r-meta-pydantic-type"></a>

- **R-META-PYDANTIC-TYPE**: Every scalar field (including scalar list `item`, map `value`, tuple `item_i`, and synthesized
  `_value` fields) carries `metadata["pydantic_type"]`. Struct, list, map, and recursion-truncated
  `pa.string()` fields do not.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-meta-comment"></a>

- **R-META-COMMENT**: A non-empty `Field(description="...")` is stored in `metadata["comment"]`. If the field has no
  `description` and its type is a `BaseModel` or `Enum` with a class docstring (not inherited from
  `BaseModel`), the cleaned docstring is used as `metadata["comment"]`.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [04_nested_models_and_maps](#case-spec-04-nested-models-and-maps), [06_enums_and_literals](#case-spec-06-enums-and-literals), [09_annotated_and_custom_types](#case-spec-09-annotated-and-custom-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

### 1.7 Record / Model Coercion

<a id="r-coerce-input"></a>

- **R-COERCE-INPUT**: `coerce_record_for_pyarrow` accepts either a `BaseModel` instance or a JSON/Python `Mapping[str, Any]`
  (optionally validated through `model.model_validate` when `model` is passed). Both pathways produce
  identical row dictionaries.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [08_aliases_and_field_names](#case-spec-08-aliases-and-field-names), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-coerce-scalar"></a>

- **R-COERCE-SCALAR**: Temporal strings or objects are coerced to `datetime.date` (`pa.date32()`), UTC-aware
  `datetime.datetime` (`pa.timestamp("us", tz="UTC")`, converting aware datetimes via
  `.astimezone(timezone.utc)` and attaching UTC to naive inputs), naive `datetime.datetime`
  (`pa.timestamp("us")`), or `datetime.time` (`pa.time64("us")`). `timedelta` becomes an ISO-8601
  duration string (`P...T...`). `UUID`, `Path`, `IPv*Address`, `AnyUrl`, and `SecretStr`
  (`.get_secret_value()`) become `str`. `SecretBytes` (`.get_secret_value()`), `bytearray`, and
  `Base64Bytes` become `bytes`. `Enum` instances unwrap to `.value`. `Decimal` values are quantized to
  the target scale for `pa.decimal128(p, s)` or converted to `float` for `pa.float64()`.

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

<a id="r-coerce-string-fallback"></a>

- **R-COERCE-STRING-FALLBACK**: Non-string values written to a `pa.string()` field (e.g. heterogeneous scalar unions or
  recursion-truncated subtrees) are stringified: booleans, dicts, lists, and `BaseModel`s with
  `json.dumps` (`true`/`false`), numbers with `str()`.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators), [10_bounded_recursion](#case-spec-10-bounded-recursion)

<a id="r-coerce-tuple"></a>

- **R-COERCE-TUPLE**: A Python `tuple` or JSON `list` for a fixed-length tuple becomes `{"item_0": v_0, ...}`; positions
  that are missing or `None` are omitted.

  *Cases:* [03_sequences_sets_and_tuples](#case-spec-03-sequences-sets-and-tuples)

<a id="r-coerce-set"></a>

- **R-COERCE-SET**: A `set` or `frozenset` for a `pa.list_` field is converted to a deterministically sorted `list` (when
  elements are comparable) so Parquet output is reproducible across runs.

  *Cases:* [03_sequences_sets_and_tuples](#case-spec-03-sequences-sets-and-tuples)

<a id="r-coerce-value"></a>

- **R-COERCE-VALUE**: A scalar value written to a scalar + model union struct becomes `{"_value": <coerced scalar>}`.

  *Cases:* [07_unions_and_discriminators](#case-spec-07-unions-and-discriminators)

<a id="r-coerce-sparse"></a>

- **R-COERCE-SPARSE**: Only keys declared in the target `pa.Schema` are kept; `None` values and unset optional keys are
  omitted from the coerced dict (becoming `null` in the Arrow table).

  *Cases:* [01_scalars_and_special_types](#case-spec-01-scalars-and-special-types), [02_nullability_defaults_and_computed](#case-spec-02-nullability-defaults-and-computed), [checkm2_and_bbmap](#case-examples-checkm2-and-bbmap), [ncbi_annotation_report](#case-examples-ncbi-annotation-report), [ncbi_assembly_report](#case-examples-ncbi-assembly-report)

## 2. Cases

<a id="case-spec-01-scalars-and-special-types"></a>

### Scalars, Network/Path Types, Temporal Types, Binary, and Decimals (`spec/01_scalars_and_special_types`)

**Rules:** [R-MODEL-INPUT](#r-model-input), [R-UNWRAP](#r-unwrap), [R-NAME-FIELD](#r-name-field), [R-ORDER](#r-order), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-TYPE](#r-scalar-type), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-SCALAR-TEMPORAL](#r-scalar-temporal), [R-SCALAR-BINARY](#r-scalar-binary), [R-SCALAR-ANNOTATED](#r-scalar-annotated), [R-META-PYDANTIC-TYPE](#r-meta-pydantic-type), [R-META-COMMENT](#r-meta-comment), [R-COERCE-INPUT](#r-coerce-input), [R-COERCE-SCALAR](#r-coerce-scalar), [R-COERCE-SPARSE](#r-coerce-sparse)

#### Schema (`schema.py`)

```python
"""Spec 01: Built-in scalars, Pydantic special types, temporal types, binary, and Decimals."""

import datetime as dt
from decimal import Decimal
from ipaddress import IPv4Address
from pathlib import Path
from typing import Annotated
from uuid import UUID

from pydantic import (
    AnyUrl,
    Base64Bytes,
    Base64Str,
    BaseModel,
    EmailStr,
    Field,
    NaiveDatetime,
    PositiveInt,
    SecretBytes,
    SecretStr,
)


class ScalarShowcaseRecord(BaseModel):
    """Comprehensive scalar type mapping showcase."""

    id: Annotated[str, Field(description="Primary record identifier")]
    active: bool
    count: PositiveInt
    ratio: float
    price: Annotated[Decimal, Field(max_digits=10, decimal_places=2, description="Fixed-scale price")]
    unconstrained_dec: Decimal
    release_date: dt.date
    created_at: dt.datetime
    local_ts: NaiveDatetime
    alarm_time: dt.time
    elapsed: dt.timedelta
    item_uuid: UUID
    config_path: Path
    host_ip: IPv4Address
    homepage: AnyUrl
    contact_email: EmailStr
    api_token: SecretStr
    raw_bytes: bytes
    secret_blob: SecretBytes
    encoded_payload: Base64Bytes
    encoded_text: Base64Str
    optional_note: Annotated[str | None, Field(description="Optional free-text note")] = None


MODEL = ScalarShowcaseRecord
```

#### Input (`data.json`)

```json
[
  {
    "id": "rec-001",
    "active": true,
    "count": 42,
    "ratio": 0.875,
    "price": "199.95",
    "unconstrained_dec": "3.14159",
    "release_date": "2026-04-10",
    "created_at": "2026-04-10T14:30:00+02:00",
    "local_ts": "2026-04-10T12:30:00",
    "alarm_time": "08:15:30",
    "elapsed": "PT1H30M",
    "item_uuid": "12345678-1234-5678-1234-567812345678",
    "config_path": "/etc/cdm/loader.yaml",
    "host_ip": "192.168.1.10",
    "homepage": "https://kbase.us/data",
    "contact_email": "info@kbase.us",
    "api_token": "top-secret-token",
    "raw_bytes": "hello",
    "secret_blob": "key",
    "encoded_payload": "cHl0aG9u",
    "encoded_text": "dGV4dA==",
    "optional_note": "Full scalar record"
  },
  {
    "id": "rec-002",
    "active": false,
    "count": 1,
    "ratio": 0.0,
    "price": "0.50",
    "unconstrained_dec": "2.5",
    "release_date": "2025-12-31",
    "created_at": "2025-12-31T23:59:59Z",
    "local_ts": "2025-12-31T23:59:59",
    "alarm_time": "00:00:00",
    "elapsed": "PT45S",
    "item_uuid": "87654321-4321-8765-4321-876543218765",
    "config_path": "/var/data/input.json",
    "host_ip": "10.0.0.1",
    "homepage": "https://example.org/",
    "contact_email": "admin@example.org",
    "api_token": "secondary-token",
    "raw_bytes": "world",
    "secret_blob": "sec",
    "encoded_payload": "YXJyb3c=",
    "encoded_text": "Y2Rt",
    "optional_note": null
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "rec-001",
        "active": true,
        "count": 42,
        "ratio": 0.875,
        "price": "199.95",
        "unconstrained_dec": 3.14159,
        "release_date": "2026-04-10",
        "created_at": "2026-04-10T12:30:00+00:00",
        "local_ts": "2026-04-10T12:30:00",
        "alarm_time": "08:15:30",
        "elapsed": "PT1H30M",
        "item_uuid": "12345678-1234-5678-1234-567812345678",
        "config_path": "/etc/cdm/loader.yaml",
        "host_ip": "192.168.1.10",
        "homepage": "https://kbase.us/data",
        "contact_email": "info@kbase.us",
        "api_token": "top-secret-token",
        "raw_bytes": "hello",
        "secret_blob": "key",
        "encoded_payload": "python",
        "encoded_text": "text",
        "optional_note": "Full scalar record"
    },
    {
        "id": "rec-002",
        "active": false,
        "count": 1,
        "ratio": 0.0,
        "price": "0.50",
        "unconstrained_dec": 2.5,
        "release_date": "2025-12-31",
        "created_at": "2025-12-31T23:59:59+00:00",
        "local_ts": "2025-12-31T23:59:59",
        "alarm_time": "00:00:00",
        "elapsed": "PT45S",
        "item_uuid": "87654321-4321-8765-4321-876543218765",
        "config_path": "/var/data/input.json",
        "host_ip": "10.0.0.1",
        "homepage": "https://example.org/",
        "contact_email": "admin@example.org",
        "api_token": "secondary-token",
        "raw_bytes": "world",
        "secret_blob": "sec",
        "encoded_payload": "arrow",
        "encoded_text": "cdm",
        "optional_note": null
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/01_scalars_and_special_types/expected_schema.py))

```python
"""Expected PyArrow schema for 01_scalars_and_special_types."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Primary record identifier", "pydantic_type": "string"},
        ),
        pa.field("active", pa.bool_(), nullable=False, metadata={"pydantic_type": "boolean"}),
        pa.field("count", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field("ratio", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
        pa.field(
            "price",
            pa.decimal128(10, 2),
            nullable=False,
            metadata={"comment": "Fixed-scale price", "pydantic_type": "decimal"},
        ),
        pa.field("unconstrained_dec", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
        pa.field("release_date", pa.date32(), nullable=False, metadata={"pydantic_type": "date"}),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False, metadata={"pydantic_type": "date-time"}),
        pa.field("local_ts", pa.timestamp("us"), nullable=False, metadata={"pydantic_type": "date-time-naive"}),
        pa.field("alarm_time", pa.time64("us"), nullable=False, metadata={"pydantic_type": "time"}),
        pa.field("elapsed", pa.string(), nullable=False, metadata={"pydantic_type": "duration"}),
        pa.field("item_uuid", pa.string(), nullable=False, metadata={"pydantic_type": "uuid"}),
        pa.field("config_path", pa.string(), nullable=False, metadata={"pydantic_type": "path"}),
        pa.field("host_ip", pa.string(), nullable=False, metadata={"pydantic_type": "ip"}),
        pa.field("homepage", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("contact_email", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("api_token", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("raw_bytes", pa.binary(), nullable=False, metadata={"pydantic_type": "binary"}),
        pa.field("secret_blob", pa.binary(), nullable=False, metadata={"pydantic_type": "binary"}),
        pa.field("encoded_payload", pa.binary(), nullable=False, metadata={"pydantic_type": "binary (base64)"}),
        pa.field("encoded_text", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "optional_note",
            pa.string(),
            nullable=True,
            metadata={"comment": "Optional free-text note", "pydantic_type": "string"},
        ),
    ]
)
```

---

<a id="case-spec-02-nullability-defaults-and-computed"></a>

### Nullability, Non-None Defaults, and Computed Fields (`spec/02_nullability_defaults_and_computed`)

**Rules:** [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-NULL-DEFAULT](#r-null-default), [R-COMPUTED-FIELDS](#r-computed-fields), [R-COERCE-SPARSE](#r-coerce-sparse)

#### Schema (`schema.py`)

```python
"""Spec 02: Field nullability, non-None defaults, default_factory, and @computed_field."""

from functools import partial

from pydantic import BaseModel, Field, computed_field


class NullabilityAndDefaultsRecord(BaseModel):
    id: str
    required_score: int
    defaulted_retries: int = 3
    default_tags: list[str] = Field(default_factory=partial(list, ("general",)))
    optional_alias: str | None = None
    explicit_nullable: int | None

    @computed_field(description="Synthesized identifier and retry label")
    @property
    def summary_label(self) -> str:
        return f"{self.id}:r{self.defaulted_retries}"


MODEL = NullabilityAndDefaultsRecord
```

#### Input (`data.json`)

```json
[
  {
    "id": "job-10",
    "required_score": 95,
    "defaulted_retries": 5,
    "default_tags": ["urgent", "batch"],
    "optional_alias": "primary-job",
    "explicit_nullable": 12
  },
  {
    "id": "job-20",
    "required_score": 60,
    "explicit_nullable": null
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "job-10",
        "required_score": 95,
        "defaulted_retries": 5,
        "default_tags": [
            "urgent",
            "batch"
        ],
        "optional_alias": "primary-job",
        "explicit_nullable": 12,
        "summary_label": "job-10:r5"
    },
    {
        "id": "job-20",
        "required_score": 60,
        "defaulted_retries": 3,
        "default_tags": [
            "general"
        ],
        "explicit_nullable": null,
        "summary_label": "job-20:r3"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/02_nullability_defaults_and_computed/expected_schema.py))

```python
"""Expected PyArrow schema for 02_nullability_defaults_and_computed."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("required_score", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field("defaulted_retries", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "default_tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field("optional_alias", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
        pa.field("explicit_nullable", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
        pa.field(
            "summary_label",
            pa.string(),
            nullable=False,
            metadata={"comment": "Synthesized identifier and retry label", "pydantic_type": "string"},
        ),
    ]
)
```

---

<a id="case-spec-03-sequences-sets-and-tuples"></a>

### Sequences, Sets, Variadic Tuples, and Fixed-Length Positional Tuples (`spec/03_sequences_sets_and_tuples`)

**Rules:** [R-SHAPE-SEQUENCE](#r-shape-sequence), [R-SHAPE-TUPLE](#r-shape-tuple), [R-NULL-ITEMS](#r-null-items), [R-COERCE-TUPLE](#r-coerce-tuple), [R-COERCE-SET](#r-coerce-set)

#### Schema (`schema.py`)

```python
"""Spec 03: Lists, sets, frozensets, variadic tuples, and fixed-length tuples."""

from pydantic import BaseModel


class SequenceAndTupleRecord(BaseModel):
    id: str
    tags: list[str]
    nullable_scores: list[int | None]
    matrix: list[list[float]]
    unique_codes: set[str]
    frozen_flags: frozenset[int]
    variadic_labels: tuple[str, ...]
    point_3d: tuple[str, int, float | None]
    optional_pair: tuple[int, str] | None = None


MODEL = SequenceAndTupleRecord
```

#### Input (`data.json`)

```json
[
  {
    "id": "seq-1",
    "tags": ["alpha", "beta"],
    "nullable_scores": [10, null, 30],
    "matrix": [[1.0, 2.0], [3.5, 4.5]],
    "unique_codes": ["c2", "c1", "c3"],
    "frozen_flags": [2, 1],
    "variadic_labels": ["x", "y", "z"],
    "point_3d": ["origin", 0, 1.25],
    "optional_pair": [7, "seven"]
  },
  {
    "id": "seq-2",
    "tags": [],
    "nullable_scores": [5, 15],
    "matrix": [[0.0]],
    "unique_codes": ["only"],
    "frozen_flags": [9],
    "variadic_labels": ["single"],
    "point_3d": ["axis", 4, null],
    "optional_pair": null
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "seq-1",
        "tags": [
            "alpha",
            "beta"
        ],
        "nullable_scores": [
            10,
            null,
            30
        ],
        "matrix": [
            [
                1.0,
                2.0
            ],
            [
                3.5,
                4.5
            ]
        ],
        "unique_codes": [
            "c1",
            "c2",
            "c3"
        ],
        "frozen_flags": [
            1,
            2
        ],
        "variadic_labels": [
            "x",
            "y",
            "z"
        ],
        "point_3d": {
            "item_0": "origin",
            "item_1": 0,
            "item_2": 1.25
        },
        "optional_pair": {
            "item_0": 7,
            "item_1": "seven"
        }
    },
    {
        "id": "seq-2",
        "tags": [],
        "nullable_scores": [
            5,
            15
        ],
        "matrix": [
            [
                0.0
            ]
        ],
        "unique_codes": [
            "only"
        ],
        "frozen_flags": [
            9
        ],
        "variadic_labels": [
            "single"
        ],
        "point_3d": {
            "item_0": "axis",
            "item_1": 4
        },
        "optional_pair": null
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/03_sequences_sets_and_tuples/expected_schema.py))

```python
"""Expected PyArrow schema for 03_sequences_sets_and_tuples."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "nullable_scores",
            pa.list_(
                pa.field("item", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "matrix",
            pa.list_(
                pa.field(
                    "item",
                    pa.list_(
                        pa.field("item", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                    ),
                    nullable=False,
                ),
            ),
            nullable=False,
        ),
        pa.field(
            "unique_codes",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "frozen_flags",
            pa.list_(
                pa.field("item", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "variadic_labels",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "point_3d",
            pa.struct(
                [
                    pa.field("item_0", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("item_1", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("item_2", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "optional_pair",
            pa.struct(
                [
                    pa.field("item_0", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("item_1", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-04-nested-models-and-maps"></a>

### Nested Models, TypedDict, Dataclasses, and String-Keyed Maps (`spec/04_nested_models_and_maps`)

**Rules:** [R-SHAPE-MODEL](#r-shape-model), [R-SHAPE-MAP](#r-shape-map), [R-NULL-ITEMS](#r-null-items), [R-META-COMMENT](#r-meta-comment)

#### Schema (`schema.py`)

```python
"""Spec 04: Nested BaseModel, TypedDict, stdlib dataclass, and dict[str, V] maps."""

from dataclasses import dataclass
from typing import TypedDict

from pydantic import BaseModel


class OrganismInfo(BaseModel):
    """Taxonomic organism metadata."""

    taxon_id: int
    scientific_name: str
    strain: str | None = None


class AssemblyStatsDict(TypedDict):
    contigs: int
    gc_percent: float


@dataclass
class SubmitterContact:
    """Submitting institution contact."""

    institution: str
    department: str | None = None


class NestedModelsAndMapsRecord(BaseModel):
    id: str
    organism: OrganismInfo
    stats: AssemblyStatsDict
    submitter: SubmitterContact | None = None
    feature_counts: dict[str, int]
    optional_metrics: dict[str, float | None]
    extra_organisms: dict[str, OrganismInfo]


MODEL = NestedModelsAndMapsRecord
```

#### Input (`data.json`)

```json
[
  {
    "id": "asm-100",
    "organism": {
      "taxon_id": 562,
      "scientific_name": "Escherichia coli",
      "strain": "K-12"
    },
    "stats": {
      "contigs": 1,
      "gc_percent": 50.8
    },
    "submitter": {
      "institution": "Lawrence Berkeley National Laboratory",
      "department": "Biosciences"
    },
    "feature_counts": {
      "cds": 4300,
      "trna": 86
    },
    "optional_metrics": {
      "completeness": 99.9,
      "contamination": null
    },
    "extra_organisms": {
      "host": {
        "taxon_id": 9606,
        "scientific_name": "Homo sapiens"
      }
    }
  },
  {
    "id": "asm-200",
    "organism": {
      "taxon_id": 1423,
      "scientific_name": "Bacillus subtilis"
    },
    "stats": {
      "contigs": 4,
      "gc_percent": 43.5
    },
    "submitter": null,
    "feature_counts": {
      "cds": 4100
    },
    "optional_metrics": {
      "completeness": 98.2
    },
    "extra_organisms": {}
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "asm-100",
        "organism": {
            "taxon_id": 562,
            "scientific_name": "Escherichia coli",
            "strain": "K-12"
        },
        "stats": {
            "contigs": 1,
            "gc_percent": 50.8
        },
        "submitter": {
            "institution": "Lawrence Berkeley National Laboratory",
            "department": "Biosciences"
        },
        "feature_counts": {
            "cds": 4300,
            "trna": 86
        },
        "optional_metrics": {
            "completeness": 99.9,
            "contamination": null
        },
        "extra_organisms": {
            "host": {
                "taxon_id": 9606,
                "scientific_name": "Homo sapiens"
            }
        }
    },
    {
        "id": "asm-200",
        "organism": {
            "taxon_id": 1423,
            "scientific_name": "Bacillus subtilis"
        },
        "stats": {
            "contigs": 4,
            "gc_percent": 43.5
        },
        "submitter": null,
        "feature_counts": {
            "cds": 4100
        },
        "optional_metrics": {
            "completeness": 98.2
        },
        "extra_organisms": {}
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/04_nested_models_and_maps/expected_schema.py))

```python
"""Expected PyArrow schema for 04_nested_models_and_maps."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field("taxon_id", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("scientific_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("strain", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Taxonomic organism metadata."},
        ),
        pa.field(
            "stats",
            pa.struct(
                [
                    pa.field("contigs", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("gc_percent", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "submitter",
            pa.struct(
                [
                    pa.field("institution", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("department", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=True,
            metadata={"comment": "Submitting institution contact."},
        ),
        pa.field(
            "feature_counts",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "optional_metrics",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
            ),
            nullable=False,
        ),
        pa.field(
            "extra_organisms",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field(
                    "value",
                    pa.struct(
                        [
                            pa.field("taxon_id", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                            pa.field(
                                "scientific_name",
                                pa.string(),
                                nullable=False,
                                metadata={"pydantic_type": "string"},
                            ),
                            pa.field("strain", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                        ]
                    ),
                    nullable=False,
                    metadata={"comment": "Taxonomic organism metadata."},
                ),
            ),
            nullable=False,
        ),
    ]
)
```

---

<a id="case-spec-05-inheritance-generics-and-root-model"></a>

### Model Inheritance, Parameterized Generics, and RootModel Unwrapping (`spec/05_inheritance_generics_and_root_model`)

**Rules:** [R-ROOT-MODEL-UNWRAP](#r-root-model-unwrap), [R-MODEL-INHERITANCE](#r-model-inheritance), [R-GENERIC-MODEL](#r-generic-model), [R-ORDER](#r-order)

#### Schema (`schema.py`)

```python
"""Spec 05: Model inheritance, Generic[T] parameterization, and RootModel[T] unwrapping."""

from pydantic import BaseModel, Field, RootModel


class BaseEntity(BaseModel):
    id: str
    version: int | str = 1


class AuditedEntity(BaseEntity):
    version: int = Field(default=1, description="Overridden integer version")
    updated_by: str


class SamplePayload(BaseModel):
    sample_name: str
    depth_m: float


class GenericEnvelope[PayloadT](AuditedEntity):
    payload: PayloadT
    secondary_payload: PayloadT | None = None


class RootEnvelopeModel(RootModel[GenericEnvelope[SamplePayload]]):
    pass


MODEL = RootEnvelopeModel
```

#### Input (`data.json`)

```json
[
  {
    "id": "env-01",
    "version": 2,
    "updated_by": "curator_a",
    "payload": {
      "sample_name": "surface_water",
      "depth_m": 5.0
    },
    "secondary_payload": {
      "sample_name": "deep_water",
      "depth_m": 120.5
    }
  },
  {
    "id": "env-02",
    "version": 1,
    "updated_by": "curator_b",
    "payload": {
      "sample_name": "sediment",
      "depth_m": 250.0
    },
    "secondary_payload": null
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "env-01",
        "version": 2,
        "updated_by": "curator_a",
        "payload": {
            "sample_name": "surface_water",
            "depth_m": 5.0
        },
        "secondary_payload": {
            "sample_name": "deep_water",
            "depth_m": 120.5
        }
    },
    {
        "id": "env-02",
        "version": 1,
        "updated_by": "curator_b",
        "payload": {
            "sample_name": "sediment",
            "depth_m": 250.0
        },
        "secondary_payload": null
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/05_inheritance_generics_and_root_model/expected_schema.py))

```python
"""Expected PyArrow schema for 05_inheritance_generics_and_root_model."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "version",
            pa.int64(),
            nullable=False,
            metadata={"comment": "Overridden integer version", "pydantic_type": "integer"},
        ),
        pa.field("updated_by", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "payload",
            pa.struct(
                [
                    pa.field("sample_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("depth_m", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "secondary_payload",
            pa.struct(
                [
                    pa.field("sample_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("depth_m", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-06-enums-and-literals"></a>

### Enums (StrEnum, IntEnum, General Enum) and Literal Types (`spec/06_enums_and_literals`)

**Rules:** [R-SCALAR-ENUM-LITERAL](#r-scalar-enum-literal), [R-META-COMMENT](#r-meta-comment)

#### Schema (`schema.py`)

```python
"""Spec 06: StrEnum, IntEnum, general Enum with docstring, and Literal types."""

from enum import Enum, IntEnum, StrEnum
from typing import Literal

from pydantic import BaseModel


class Strand(StrEnum):
    """Genomic strand orientation."""

    PLUS = "+"
    MINUS = "-"


class PriorityLevel(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class MixedCodeEnum(Enum):
    """Mixed integer and string status codes."""

    OK = 200
    CUSTOM = "custom-ok"


class EnumAndLiteralRecord(BaseModel):
    id: str
    strand: Strand
    priority: PriorityLevel
    mixed_code: MixedCodeEnum
    string_literal: Literal["draft", "complete", "archived"]
    int_literal: Literal[10, 20, 30]
    bool_literal: Literal[True]
    num_widened_literal: Literal[1, 2.5]
    heterogeneous_literal: Literal[1, "two", True]
    nullable_literal: Literal["active", None] = None


MODEL = EnumAndLiteralRecord
```

#### Input (`data.json`)

```json
[
  {
    "id": "enum-1",
    "strand": "+",
    "priority": 3,
    "mixed_code": 200,
    "string_literal": "complete",
    "int_literal": 20,
    "bool_literal": true,
    "num_widened_literal": 1,
    "heterogeneous_literal": "two",
    "nullable_literal": "active"
  },
  {
    "id": "enum-2",
    "strand": "-",
    "priority": 1,
    "mixed_code": "custom-ok",
    "string_literal": "draft",
    "int_literal": 10,
    "bool_literal": true,
    "num_widened_literal": 2.5,
    "heterogeneous_literal": true,
    "nullable_literal": null
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "enum-1",
        "strand": "+",
        "priority": 3,
        "mixed_code": "200",
        "string_literal": "complete",
        "int_literal": 20,
        "bool_literal": true,
        "num_widened_literal": 1.0,
        "heterogeneous_literal": "two",
        "nullable_literal": "active"
    },
    {
        "id": "enum-2",
        "strand": "-",
        "priority": 1,
        "mixed_code": "custom-ok",
        "string_literal": "draft",
        "int_literal": 10,
        "bool_literal": true,
        "num_widened_literal": 2.5,
        "heterogeneous_literal": "true",
        "nullable_literal": null
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/06_enums_and_literals/expected_schema.py))

```python
"""Expected PyArrow schema for 06_enums_and_literals."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "strand",
            pa.string(),
            nullable=False,
            metadata={"comment": "Genomic strand orientation.", "pydantic_type": "string"},
        ),
        pa.field("priority", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "mixed_code",
            pa.string(),
            nullable=False,
            metadata={"comment": "Mixed integer and string status codes.", "pydantic_type": "integer | string"},
        ),
        pa.field("string_literal", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("int_literal", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field("bool_literal", pa.bool_(), nullable=False, metadata={"pydantic_type": "boolean"}),
        pa.field("num_widened_literal", pa.float64(), nullable=False, metadata={"pydantic_type": "integer | number"}),
        pa.field(
            "heterogeneous_literal",
            pa.string(),
            nullable=False,
            metadata={"pydantic_type": "integer | string | boolean"},
        ),
        pa.field("nullable_literal", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
    ]
)
```

---

<a id="case-spec-07-unions-and-discriminators"></a>

### Scalar Unions, Model Unions, Discriminated Unions, and Scalar + Model Unions (`spec/07_unions_and_discriminators`)

**Rules:** [R-UNION-SCALAR](#r-union-scalar), [R-UNION-MODEL](#r-union-model), [R-UNION-DISCRIMINATED](#r-union-discriminated), [R-UNION-SCALAR-MODEL](#r-union-scalar-model), [R-MERGE-DUPLICATE](#r-merge-duplicate), [R-NULL-UNION-BRANCH](#r-null-union-branch), [R-COERCE-STRING-FALLBACK](#r-coerce-string-fallback), [R-COERCE-VALUE](#r-coerce-value)

#### Schema (`schema.py`)

```python
"""Spec 07: Scalar unions, BaseModel unions, discriminated unions, and scalar + model unions."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class ShortReadSource(BaseModel):
    shared_id: str = Field(description="Shared source identifier")
    platform: str
    read_length: int
    metric: int


class LongReadSource(BaseModel):
    shared_id: str = Field(description="Ignored second description")
    chemistry: str
    metric: float


class BlastHit(BaseModel):
    hit_type: Literal["blast"]
    accession: str
    evalue: float


class HmmHit(BaseModel):
    hit_type: Literal["hmm"]
    accession: str
    bit_score: float


class StructuredCitation(BaseModel):
    pmid: int
    title: str


class UnionShowcaseRecord(BaseModel):
    id: str
    numeric_union: int | float
    scalar_fallback_union: int | str | bool
    source: ShortReadSource | LongReadSource
    top_hit: Annotated[BlastHit | HmmHit, Field(discriminator="hit_type")]
    citation: StructuredCitation | str


MODEL = UnionShowcaseRecord
```

#### Input (`data.json`)

```json
[
  {
    "id": "u-1",
    "numeric_union": 10,
    "scalar_fallback_union": 42,
    "source": {
      "shared_id": "src-illumina",
      "platform": "Illumina NovaSeq",
      "read_length": 150,
      "metric": 99
    },
    "top_hit": {
      "hit_type": "blast",
      "accession": "NP_414542.1",
      "evalue": 1e-50
    },
    "citation": {
      "pmid": 12345678,
      "title": "Genome sequence of E. coli"
    }
  },
  {
    "id": "u-2",
    "numeric_union": 3.5,
    "scalar_fallback_union": true,
    "source": {
      "shared_id": "src-nanopore",
      "chemistry": "R10.4.1",
      "metric": 14.25
    },
    "top_hit": {
      "hit_type": "hmm",
      "accession": "PF00001.24",
      "bit_score": 210.4
    },
    "citation": "Unpublished observation"
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "id": "u-1",
        "numeric_union": 10.0,
        "scalar_fallback_union": "42",
        "source": {
            "shared_id": "src-illumina",
            "platform": "Illumina NovaSeq",
            "read_length": 150,
            "metric": 99.0
        },
        "top_hit": {
            "hit_type": "blast",
            "accession": "NP_414542.1",
            "evalue": 1e-50
        },
        "citation": {
            "pmid": 12345678,
            "title": "Genome sequence of E. coli"
        }
    },
    {
        "id": "u-2",
        "numeric_union": 3.5,
        "scalar_fallback_union": "true",
        "source": {
            "shared_id": "src-nanopore",
            "metric": 14.25,
            "chemistry": "R10.4.1"
        },
        "top_hit": {
            "hit_type": "hmm",
            "accession": "PF00001.24",
            "bit_score": 210.4
        },
        "citation": {
            "_value": "Unpublished observation"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/07_unions_and_discriminators/expected_schema.py))

```python
"""Expected PyArrow schema for 07_unions_and_discriminators."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("numeric_union", pa.float64(), nullable=False, metadata={"pydantic_type": "integer | number"}),
        pa.field(
            "scalar_fallback_union",
            pa.string(),
            nullable=False,
            metadata={"pydantic_type": "integer | string | boolean"},
        ),
        pa.field(
            "source",
            pa.struct(
                [
                    pa.field(
                        "shared_id",
                        pa.string(),
                        nullable=False,
                        metadata={"comment": "Shared source identifier", "pydantic_type": "string"},
                    ),
                    pa.field("platform", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                    pa.field("read_length", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
                    pa.field("metric", pa.float64(), nullable=False, metadata={"pydantic_type": "integer | number"}),
                    pa.field("chemistry", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "top_hit",
            pa.struct(
                [
                    pa.field("hit_type", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("accession", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("evalue", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                    pa.field("bit_score", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "citation",
            pa.struct(
                [
                    pa.field("pmid", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
                    pa.field("title", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                    pa.field("_value", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
        ),
    ]
)
```

---

<a id="case-spec-08-aliases-and-field-names"></a>

### Field Naming, `alias`, `serialization_alias`, and `by_alias` Mode (`spec/08_aliases_and_field_names`)

**Rules:** [R-NAME-FIELD](#r-name-field), [R-COERCE-INPUT](#r-coerce-input)

#### Schema (`schema.py`)

```python
"""Spec 08: Python field names vs. Field(alias=...) and Field(serialization_alias=...)."""

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class AliasedChild(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    internal_code: str = Field(alias="code")
    score_val: float = Field(serialization_alias="serialized_score")


class AliasedRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    record_id: str = Field(alias="recordId", description="Record identifier")
    display_name: str = Field(serialization_alias="displayName")
    alt_input: int = Field(validation_alias=AliasChoices("alt_input", "altInput"))
    child: AliasedChild


MODEL = AliasedRecord
```

#### Input (`data.json`)

```json
[
  {
    "record_id": "rec-a",
    "display_name": "Alpha",
    "alt_input": 100,
    "child": {
      "internal_code": "c-1",
      "score_val": 9.5
    }
  },
  {
    "recordId": "rec-b",
    "display_name": "Beta",
    "altInput": 200,
    "child": {
      "code": "c-2",
      "score_val": 4.25
    }
  }
]
```

#### Converted rows: run `field-names` (`by_alias=False`)

```json
[
    {
        "record_id": "rec-a",
        "display_name": "Alpha",
        "alt_input": 100,
        "child": {
            "internal_code": "c-1",
            "score_val": 9.5
        }
    },
    {
        "record_id": "rec-b",
        "display_name": "Beta",
        "alt_input": 200,
        "child": {
            "internal_code": "c-2",
            "score_val": 4.25
        }
    }
]
```

#### Converted rows: run `by-alias` (`by_alias=True`)

```json
[
    {
        "recordId": "rec-a",
        "displayName": "Alpha",
        "alt_input": 100,
        "child": {
            "code": "c-1",
            "serialized_score": 9.5
        }
    },
    {
        "recordId": "rec-b",
        "displayName": "Beta",
        "alt_input": 200,
        "child": {
            "code": "c-2",
            "serialized_score": 4.25
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/08_aliases_and_field_names/expected_schema.py))

```python
"""Expected PyArrow schema for 08_aliases_and_field_names."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "record_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record identifier", "pydantic_type": "string"},
        ),
        pa.field("display_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("alt_input", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "child",
            pa.struct(
                [
                    pa.field("internal_code", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("score_val", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
    ]
)

EXPECTED_SCHEMA_BY_ALIAS = pa.schema(
    [
        pa.field(
            "recordId",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record identifier", "pydantic_type": "string"},
        ),
        pa.field("displayName", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("alt_input", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "child",
            pa.struct(
                [
                    pa.field("code", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("serialized_score", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
    ]
)
```

---

<a id="case-spec-09-annotated-and-custom-types"></a>

### Annotated Type Aliases (`TypeAliasType`), Functional Validators, and Nested Metadata (`spec/09_annotated_and_custom_types`)

**Rules:** [R-SCALAR-ANNOTATED](#r-scalar-annotated), [R-SCALAR-TYPE](#r-scalar-type), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-META-COMMENT](#r-meta-comment)

#### Schema (`schema.py`)

```python
"""Spec 09: PEP 695 type aliases, nested Annotated metadata precedence, and functional validators."""

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
```

#### Input (`data.json`)

```json
[
  {
    "accession": "GCF_000005845.2",
    "overridden_accession": "GCA_000005845.2",
    "coverage": "125.375",
    "quality_phred": 40
  },
  {
    "accession": "GCF_000009045.1",
    "overridden_accession": "GCA_000009045.1",
    "coverage": "50.000",
    "quality_phred": 35
  }
]
```

#### Converted rows: run `maximal`

```json
[
    {
        "accession": "GCF_000005845.2",
        "overridden_accession": "GCA_000005845.2",
        "coverage": "125.375",
        "quality_phred": 40
    },
    {
        "accession": "GCF_000009045.1",
        "overridden_accession": "GCA_000009045.1",
        "coverage": "50.000",
        "quality_phred": 35
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/09_annotated_and_custom_types/expected_schema.py))

```python
"""Expected PyArrow schema for 09_annotated_and_custom_types."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "accession",
            pa.string(),
            nullable=False,
            metadata={"comment": "Base accession string", "pydantic_type": "string"},
        ),
        pa.field(
            "overridden_accession",
            pa.string(),
            nullable=False,
            metadata={"comment": "Overridden accession description", "pydantic_type": "string"},
        ),
        pa.field(
            "coverage",
            pa.decimal128(8, 3),
            nullable=False,
            metadata={"comment": "Sequencing depth coverage", "pydantic_type": "decimal"},
        ),
        pa.field(
            "quality_phred",
            pa.int64(),
            nullable=False,
            metadata={"comment": "Phred quality score", "pydantic_type": "integer"},
        ),
    ]
)
```

---

<a id="case-spec-10-bounded-recursion"></a>

### Self-Recursive and Mutually Recursive Models with `max_recursion_depth` (`spec/10_bounded_recursion`)

**Rules:** [R-RECURSION](#r-recursion), [R-COERCE-STRING-FALLBACK](#r-coerce-string-fallback)

`TaxonNode` is self-recursive (`children: list[TaxonNode]`); `PathwayStep` and `ReactionNode` are mutually
recursive. At `max_recursion_depth=2` the third level collapses to `pa.string()` serialized with `json.dumps`;
at `max_recursion_depth=1` the second level already collapses to `pa.string()`.

#### Schema (`schema.py`)

```python
"""Spec 10: Self-recursive and mutually recursive BaseModels."""

from pydantic import BaseModel, Field


class TaxonNode(BaseModel):
    rank: str
    name: str
    children: list["TaxonNode"] = Field(default_factory=list)


class PathwayStep(BaseModel):
    step_id: str
    reaction: "ReactionNode | None" = None


class ReactionNode(BaseModel):
    ec_number: str
    next_step: PathwayStep | None = None


class RecursiveCladeRecord(BaseModel):
    root_taxon: TaxonNode
    entry_step: PathwayStep


MODEL = RecursiveCladeRecord
```

#### Input (`data.json`)

```json
[
  {
    "root_taxon": {
      "rank": "domain",
      "name": "Bacteria",
      "children": [
        {
          "rank": "phylum",
          "name": "Pseudomonadota",
          "children": [
            {
              "rank": "class",
              "name": "Gammaproteobacteria",
              "children": []
            }
          ]
        }
      ]
    },
    "entry_step": {
      "step_id": "step-1",
      "reaction": {
        "ec_number": "2.7.1.1",
        "next_step": {
          "step_id": "step-2",
          "reaction": {
            "ec_number": "5.3.1.9",
            "next_step": null
          }
        }
      }
    }
  }
]
```

#### Converted rows: run `depth-2` (`max_recursion_depth=2`)

```json
[
    {
        "root_taxon": {
            "rank": "domain",
            "name": "Bacteria",
            "children": [
                {
                    "rank": "phylum",
                    "name": "Pseudomonadota",
                    "children": [
                        "{\"rank\": \"class\", \"name\": \"Gammaproteobacteria\", \"children\": []}"
                    ]
                }
            ]
        },
        "entry_step": {
            "step_id": "step-1",
            "reaction": {
                "ec_number": "2.7.1.1",
                "next_step": {
                    "step_id": "step-2",
                    "reaction": {
                        "ec_number": "5.3.1.9",
                        "next_step": null
                    }
                }
            }
        }
    }
]
```

#### Converted rows: run `depth-1` (`max_recursion_depth=1`)

```json
[
    {
        "root_taxon": {
            "rank": "domain",
            "name": "Bacteria",
            "children": [
                "{\"rank\": \"phylum\", \"name\": \"Pseudomonadota\", \"children\": [{\"rank\": \"class\", \"name\": \"Gammaproteobacteria\", \"children\": []}]}"
            ]
        },
        "entry_step": {
            "step_id": "step-1",
            "reaction": {
                "ec_number": "2.7.1.1",
                "next_step": "{\"step_id\": \"step-2\", \"reaction\": {\"ec_number\": \"5.3.1.9\", \"next_step\": null}}"
            }
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/10_bounded_recursion/expected_schema.py))

```python
"""Expected PyArrow schema for 10_bounded_recursion."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "root_taxon",
            pa.struct(
                [
                    pa.field("rank", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "children",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "rank",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"pydantic_type": "string"},
                                        ),
                                        pa.field(
                                            "name",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"pydantic_type": "string"},
                                        ),
                                        pa.field(
                                            "children",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=False),
                                            ),
                                            nullable=False,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "entry_step",
            pa.struct(
                [
                    pa.field("step_id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "reaction",
                        pa.struct(
                            [
                                pa.field(
                                    "ec_number",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"pydantic_type": "string"},
                                ),
                                pa.field(
                                    "next_step",
                                    pa.struct(
                                        [
                                            pa.field(
                                                "step_id",
                                                pa.string(),
                                                nullable=False,
                                                metadata={"pydantic_type": "string"},
                                            ),
                                            pa.field(
                                                "reaction",
                                                pa.struct(
                                                    [
                                                        pa.field(
                                                            "ec_number",
                                                            pa.string(),
                                                            nullable=False,
                                                            metadata={"pydantic_type": "string"},
                                                        ),
                                                        pa.field("next_step", pa.string(), nullable=True),
                                                    ]
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
            ),
            nullable=False,
        ),
    ]
)

EXPECTED_SCHEMA_DEPTH_1 = pa.schema(
    [
        pa.field(
            "root_taxon",
            pa.struct(
                [
                    pa.field("rank", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "children",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=False),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "entry_step",
            pa.struct(
                [
                    pa.field("step_id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "reaction",
                        pa.struct(
                            [
                                pa.field(
                                    "ec_number",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"pydantic_type": "string"},
                                ),
                                pa.field("next_step", pa.string(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)
```

---

<a id="case-examples-checkm2-and-bbmap"></a>

### CheckM2 Genome Quality and BBMap Contigset Statistics (`examples/checkm2_and_bbmap`)

**Rules:** [R-MODEL-INPUT](#r-model-input), [R-UNWRAP](#r-unwrap), [R-NAME-FIELD](#r-name-field), [R-ORDER](#r-order), [R-COMPUTED-FIELDS](#r-computed-fields), [R-SHAPE-MODEL](#r-shape-model), [R-SHAPE-MAP](#r-shape-map), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-TYPE](#r-scalar-type), [R-SCALAR-DECIMAL](#r-scalar-decimal), [R-META-PYDANTIC-TYPE](#r-meta-pydantic-type), [R-META-COMMENT](#r-meta-comment), [R-COERCE-INPUT](#r-coerce-input), [R-COERCE-SCALAR](#r-coerce-scalar), [R-COERCE-SPARSE](#r-coerce-sparse)

**Schema:** [`specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/schema.py)

- Run `maximal`: [`data.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/data.json), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/expected.json), [`expected_table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/expected_schema.py)

---

<a id="case-examples-ncbi-annotation-report"></a>

### NCBI Datasets API — Genome Annotation Report (`V2GenomeAnnotationTableSummaryReply` + Feature Report) (`examples/ncbi_annotation_report`)

**Rules:** [R-MODEL-INPUT](#r-model-input), [R-UNWRAP](#r-unwrap), [R-NAME-FIELD](#r-name-field), [R-ORDER](#r-order), [R-SHAPE-MODEL](#r-shape-model), [R-SHAPE-SEQUENCE](#r-shape-sequence), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-TYPE](#r-scalar-type), [R-SCALAR-ENUM-LITERAL](#r-scalar-enum-literal), [R-META-PYDANTIC-TYPE](#r-meta-pydantic-type), [R-META-COMMENT](#r-meta-comment), [R-COERCE-INPUT](#r-coerce-input), [R-COERCE-SCALAR](#r-coerce-scalar), [R-COERCE-SPARSE](#r-coerce-sparse)

**Schema:** [`specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/schema.py)

- Run `maximal`: [`data.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/data.json), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/expected.json), [`expected_table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/expected_schema.py)

---

<a id="case-examples-ncbi-assembly-report"></a>

### NCBI Datasets API — Assembly Dataset Report (`V2reportsAssemblyDataReport` Subset) (`examples/ncbi_assembly_report`)

**Rules:** [R-MODEL-INPUT](#r-model-input), [R-UNWRAP](#r-unwrap), [R-NAME-FIELD](#r-name-field), [R-ORDER](#r-order), [R-SHAPE-MODEL](#r-shape-model), [R-SHAPE-SEQUENCE](#r-shape-sequence), [R-NULL-REQUIRED](#r-null-required), [R-NULL-OPTIONAL](#r-null-optional), [R-SCALAR-TYPE](#r-scalar-type), [R-SCALAR-TEMPORAL](#r-scalar-temporal), [R-SCALAR-ENUM-LITERAL](#r-scalar-enum-literal), [R-META-PYDANTIC-TYPE](#r-meta-pydantic-type), [R-META-COMMENT](#r-meta-comment), [R-COERCE-INPUT](#r-coerce-input), [R-COERCE-SCALAR](#r-coerce-scalar), [R-COERCE-SPARSE](#r-coerce-sparse)

**Schema:** [`specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/schema.py)

- Run `maximal`: [`data.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/data.json), [`expected.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/expected.json), [`expected_table.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/expected_schema.py)

---

## 3. Errors (Unsupported / Invalid Input)

Schema generation raises a subclass of `PydanticToPyArrowError` (itself a `ValueError`).

| Code | Exception | Condition | Reason | Fixtures |
| :--- | :--- | :--- | :--- | :--- |
| **E1** | `InvalidPydanticModelError` | Input is not a concrete Pydantic `BaseModel` subclass | Converter requires a concrete Pydantic `BaseModel` subclass as input. | [`e01_invalid_model_input.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e01_invalid_model_input.py): Input is a stdlib `@dataclass` rather than a `BaseModel` subclass. |
| **E2** | `NonStructRootError` | Non-struct `RootModel` | Root of a PyArrow schema must be a `struct` (a table has named columns, not a bare scalar, list, or map). | [`e02_non_object_root_model.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e02_non_object_root_model.py): `RootModel[list[int]]` wraps a sequence rather than a `BaseModel`. |
| **E3** | `EmptyModelError` | Empty model or tuple (zero fields) | Parquet cannot persist a `struct` with zero child fields. | [`e03_empty_model.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e03_empty_model.py): Root `BaseModel` has no declared or computed fields. |
| **E4** | `UnconstrainedFieldError` | Unconstrained field (`Any`, `object`, untyped container, or unbound `TypeVar`) | PyArrow requires a concrete static column type and has no dynamic `any` or untyped container column. | [`e04_unconstrained_any_field.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e04_unconstrained_any_field.py): Field annotation is `Any`. |
| **E5** | `OpenTupleError` | Open tuple mixing positional and variadic elements | Positional tuple structs require a fixed element count so every position maps to a deterministic field. | [`e05_open_tuple.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e05_open_tuple.py): Field uses a mixed prefix + variadic tuple `tuple[int, *tuple[str, ...]]`. |
| **E6** | `NonStringMapKeyError` | Open `extra='allow'` model or non-string mapping key type | Fixed-field structs cannot mix open `extra='allow'` maps, and Arrow maps require string keys for JSONL round-tripping. | [`e06_mixed_extra_allow_or_non_string_map_key.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e06_mixed_extra_allow_or_non_string_map_key.py): Model declares `model_config = ConfigDict(extra="allow")` with fixed fields. |
| **E7** | `UnsupportedFieldTypeError` | Unsupported field type or null-only field | Non-serializable Python runtime types and null-only fields have no valid Arrow column representation. | [`e07_unsupported_field_type.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e07_unsupported_field_type.py): Field annotation is `Callable[[int], str]`. |
| **E8** | `IncompatibleUnionBranchesError` | Incompatible structural union branches | PyArrow cannot merge lists, maps, and structs into a single column without an opaque union type. | [`e08_incompatible_union_shapes.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e08_incompatible_union_shapes.py): Field annotation mixes a `BaseModel` branch with a `list[int]` branch. |
| **E9** | `UnresolvedForwardRefError` | Unresolvable forward reference | Schema generation requires all forward type annotations to be resolved to concrete types. | [`e09_unresolved_forward_ref.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e09_unresolved_forward_ref.py): Field references a forward annotation `"UndefinedChild"` that cannot be resolved. |
| **E10** | `RecursiveNonJsonTypeError` | Non-JSON-serializable type in a recursive model cycle or cyclic type alias | Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`; cyclic type aliases without a `BaseModel` boundary cannot be finitely unrolled. | [`e10_recursive_non_json_type.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e10_recursive_non_json_type.py): Recursive `BaseModel` contains a `datetime.date` field. |
| **E11** | `InvalidDecimalConstraintsError` | Invalid or out-of-range `Decimal` precision/scale constraints | Arrow `decimal128` requires `1 <= max_digits <= 38` and `0 <= decimal_places <= max_digits`. | [`e11_invalid_decimal_constraints.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e11_invalid_decimal_constraints.py): Field declares `Decimal` with `max_digits=50` (> 38). |
| **E12** | `ValueFieldCollisionError` | `_value` collision in a scalar + model union | The synthesized `_value` field for scalar union branches would collide with the model's declared `_value` field. | [`e12_value_field_collision.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/errors/e12_value_field_collision.py): `BaseModel` branch of a scalar + model union declares a `_value` field. |

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suite and this document are both driven by it.

| Case | Schema | Runs |
| :--- | :--- | :--- |
| [`spec/01_scalars_and_special_types`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/01_scalars_and_special_types/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/01_scalars_and_special_types/schema.py) | maximal |
| [`spec/02_nullability_defaults_and_computed`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/02_nullability_defaults_and_computed/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/02_nullability_defaults_and_computed/schema.py) | maximal |
| [`spec/03_sequences_sets_and_tuples`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/03_sequences_sets_and_tuples/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/03_sequences_sets_and_tuples/schema.py) | maximal |
| [`spec/04_nested_models_and_maps`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/04_nested_models_and_maps/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/04_nested_models_and_maps/schema.py) | maximal |
| [`spec/05_inheritance_generics_and_root_model`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/05_inheritance_generics_and_root_model/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/05_inheritance_generics_and_root_model/schema.py) | maximal |
| [`spec/06_enums_and_literals`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/06_enums_and_literals/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/06_enums_and_literals/schema.py) | maximal |
| [`spec/07_unions_and_discriminators`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/07_unions_and_discriminators/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/07_unions_and_discriminators/schema.py) | maximal |
| [`spec/08_aliases_and_field_names`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/08_aliases_and_field_names/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/08_aliases_and_field_names/schema.py) | field-names, by-alias |
| [`spec/09_annotated_and_custom_types`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/09_annotated_and_custom_types/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/09_annotated_and_custom_types/schema.py) | maximal |
| [`spec/10_bounded_recursion`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/10_bounded_recursion/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/spec/10_bounded_recursion/schema.py) | depth-2, depth-1 |
| [`examples/checkm2_and_bbmap`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/checkm2_and_bbmap/schema.py) | maximal |
| [`examples/ncbi_annotation_report`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_annotation_report/schema.py) | maximal |
| [`examples/ncbi_assembly_report`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/case.yaml) | [`schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs/schema_converters/pydantic_to_pyarrow/examples/ncbi_assembly_report/schema.py) | maximal |
