# Converter IR and Reader Contract

Phase 3 adds immutable source readers and a structural intermediate representation (IR).
No emitter is implemented and none of the four converter facades is rewired.
The existing converters remain the compatibility reference, not the definition of source truth.

## Public Modules

All paths below are under `src/cdm_data_loaders/converters/`.

| Module | Public API |
| --- | --- |
| `ir.py` | `NodeType`, `Source`, `Provenance`, `NodeHints`, `TypedNode`, `Field`, `SchemaDocument`, `Reader`, `Emitter` |
| `ir_values.py` | `Value`, `freeze_value`, `freeze_mapping`, `mutable_value` |
| `extensions.py` | `ExtensionError`, `ExtensionSpec`, `ExtensionRegistry`, `Extensions`, `DEFAULT_EXTENSIONS`, `validated_extensions` |
| `readers/json_schema.py` | `JsonSchemaReader` |
| `readers/dlt.py` | `DltReader` |
| `readers/iceberg.py` | `IcebergReader`, `iceberg_value` |

The IR, value helpers and extension registry import no dlt, PyIceberg or PySpark modules.
The XSV contract imports only the existing custom metaschema module, not Spark readers.
Models are frozen, slotted dataclasses. Constructors validate and recursively copy mappings
into `frozendict` and sequences into tuples. There is no unchecked construction path.

## Models

The following are complete constructor fields. `Mapping` preserves insertion order.
All metadata mappings default to empty immutable mappings; all fields not marked optional
are required constructor arguments. Metadata values retain absent keys separately from null.

```python
type Value = None | bool | int | float | Decimal | str | tuple[Value, ...] | Mapping[str, Value]
type Source = Literal["json-schema", "dlt", "iceberg"]
type NodeType = Literal[
    "object", "map", "array", "string", "integer", "number", "boolean",
    "null", "any", "never", "unknown",
]

Provenance(
    source: Source,
    path: tuple[str | int, ...] = (),
    metadata: Mapping[str, Value] = {},
)

NodeHints(
    logical_type: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
    bit_width: int | None = None,
    timezone: bool | None = None,
    length: int | None = None,
)

TypedNode(
    type: NodeType,
    nullable: bool | None = None,
    declared_type: str | tuple[str, ...] | None = None,
    inferred_type: str | None = None,
    hints: NodeHints = NodeHints(),
    properties: tuple[Field, ...] = (),
    required_names: tuple[str, ...] | None = None,
    items: TypedNode | tuple[TypedNode, ...] | None = None,
    prefix_items: tuple[TypedNode, ...] | None = None,
    pattern_properties: Mapping[str, TypedNode] = {},
    additional_properties: TypedNode | None = None,
    any_of: tuple[TypedNode, ...] | None = None,
    one_of: tuple[TypedNode, ...] | None = None,
    schema_keywords: Mapping[str, TypedNode] = {},
    schema_maps: Mapping[str, Mapping[str, TypedNode]] = {},
    key_type: TypedNode | None = None,
    value_type: TypedNode | None = None,
    constraints: Mapping[str, Value] = {},
    annotations: Mapping[str, Value] = {},
    extensions: Mapping[str, Value] = {},
    provenance: Provenance | None = None,
    source_keywords: tuple[str, ...] = (),
)

Field(
    name: str,
    node: TypedNode,
    required: bool = False,
    annotations: Mapping[str, Value] = {},
    extensions: Mapping[str, Value] = {},
    provenance: Provenance | None = None,
)

SchemaDocument(
    root: TypedNode,
    name: str | None = None,
    dialect: str | None = None,
    identifier: str | None = None,
    annotations: Mapping[str, Value] = {},
    extensions: Mapping[str, Value] = {},
    provenance: Provenance | None = None,
)
```

`Field.required` describes property presence, not whether its value accepts null.
`TypedNode.nullable=None` means unresolved. JSON unions are not flattened or selected.
`declared_type=None` means absent; a scalar string and a tuple union declaration remain
distinct, including a one-element union. `inferred_type` records the shared keyword inference
without replacing the declaration or choosing a combiner branch. Enum/const values remain
in `constraints`; inference from them is an emitter choice. `unknown` is not `string`.
Boolean schema `true` is `any`; `false` is `never`; explicit `type: null` is `null`.

`required_names` retains original order and names without declared properties. For dlt it
also retains legacy required-list order, including duplicate names when a child overwrites
a same-named column. `source_keywords` retains JSON keyword presence and order, distinguishing
an omitted `properties` or `type` from an explicit empty declaration. Boolean schemas have
no keywords. Target dispatch must consider these facts, not just `node.type`.

Schema structure is separate from literal values:

- `properties`, `pattern_properties`, `items`, `prefix_items`, `additional_properties`,
  `any_of` and `one_of` hold typed children, preserving declaration order.
- `schema_keywords` holds `additionalItems`, `unevaluatedItems`, `contains`,
  `unevaluatedProperties`, `propertyNames`, `not`, `if`, `then`, `else`, `contentSchema`.
- `schema_maps` holds `$defs`, `definitions`, `dependentSchemas`, and schema-valued
  `dependencies`. Array-valued `dependencies` stays in `constraints`.
- `constraints` retains all remaining non-extension keywords, including exact `multipleOf`,
  min/max and exclusive bounds, enum, const, pattern, format, content encoding/media type,
  lengths, item counts, uniqueness, and unknown readable keywords.
- `annotations` holds `$schema`, `$id`, `$anchor`, `$dynamicAnchor`, `$comment`, `$vocabulary`,
  title, description, default, examples, readOnly, writeOnly and deprecated.

Decimals remain finite `Decimal` objects, with no conversion through binary floats.
`freeze_value(value: object) -> Value` and
`freeze_mapping(value: Mapping[str, object]) -> Mapping[str, Value]` reject arbitrary objects,
non-string mapping keys and non-finite numbers. `mutable_value(value: Value) -> object`
returns independent dict/list containers and preserves Decimal. This is not a JSON encoder:
future serializers must encode Decimal deliberately and must not convert it to float.

## Extension Contracts

```python
ExtensionSpec(namespace: str, schema: Mapping[str, Value] | bool)
ExtensionRegistry(specs: tuple[ExtensionSpec, ...] = ())
ExtensionRegistry.register(spec: ExtensionSpec, *, replace: bool = False) -> ExtensionRegistry
ExtensionRegistry.extend_payload(namespace: str, properties: Mapping[str, object]) -> ExtensionRegistry
ExtensionRegistry.validate(values: Mapping[str, object]) -> Mapping[str, Value]
Extensions(payload: Mapping[str, Value] = {}, registry: ExtensionRegistry = DEFAULT_EXTENSIONS)
validated_extensions(values: Mapping[str, Value]) -> Extensions
```

Only top-level namespace names require a nonempty `x-` prefix. Internal names such as
`data_type` and `field_id` are ordinary keys. Key spelling is never rewritten with `replace`.
Each namespace has an explicit draft 2020-12 validation schema. Arrays and null are valid
when that schema permits them. Normal IR construction wraps plain mappings in `Extensions`
using `DEFAULT_EXTENSIONS`; a prevalidated `Extensions` instance retains its custom registry.
Schemas, additional property contracts and payloads are recursively copied and immutable.
Registration returns a new registry. Duplicate namespaces need `replace=True`; extending
a payload rejects key collisions. Ordinary payload objects reject unknown keys.

Builtins:

- `x-dlt`: name, data_type, nullable, description, precision, scale, primary_key, unique,
  foreign_key, sort, cluster, partition, merge_key, row_key, root_key, variant and timezone.
  Boolean hints permit null to preserve observed metadata. Precision/scale are nonnegative
  integers or null. Data type names are strings or null so unknown source types remain readable.
  Custom hints, including internal `x-*` hints, require `extend_payload("x-dlt", {...})`.
- `x-iceberg`: field_id, required, initial_default, write_default, logical_type, precision,
  scale, length, element_id, element_required, key_id, value_id, value_required, identifier,
  schema_id, format_version, current_snapshot_id, location, properties, partition_spec,
  identifier_field_ids and generated_at. Defaults allow arbitrary IR values; other keys
  have explicit scalar, array or closed-object contracts.
- `x-xsv-config`: exactly `X_XSV_CONFIG_SCHEMA` from the existing custom metaschema,
  including its nonempty recognized-key requirement and closed property set. This does
  not impose a nonempty root JSON Schema `required` array.
- `x-file-glob`, `x-dlt-prefix`, `x-dlt-split`: strings; `x-delimiter`: one character;
  `x-pii`: boolean. These match observed repository tests.

Unregistered extensions raise `ExtensionError(ConversionError)` by default. This is a
deliberately stricter new-IR contract than the unchanged legacy facades, notably PySpark's
arbitrary `extra_metadata_keywords={"x-custom"}` acceptance. Phase 5 composition must supply
explicit application schemas for those namespaces; do not silently register unchecked data.
Contract violations raise errors. Unknown dlt types log through the reader module logger;
there is no logging callback.

## Readers

```python
JsonSchemaReader(extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS)
JsonSchemaReader.read(source: Mapping[str, Any]) -> SchemaDocument

DltReader(
    include_dlt_columns: bool = False,
    include_variant_columns: bool = False,
    child_table_mode: Literal["object", "array"] = "object",
    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS,
)
DltReader.read(source: Mapping[str, Any]) -> dict[str, SchemaDocument]

IcebergReader(extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS)
IcebergReader.read(source: Schema) -> SchemaDocument
IcebergReader.read_table(table: Table, identifier: tuple[str, ...]) -> SchemaDocument
iceberg_value(value: object) -> Value

class Reader[Input, Output](Protocol):
    def read(self, source: Input) -> Output: ...

class Emitter[Output](Protocol):
    def emit(self, document: SchemaDocument) -> Output: ...
```

### JSON Schema

Input is already validated and dereferenced. The reader requires `$schema` and an
object-compatible root: absent type, `object`, or a type list containing `object`.
It rejects `$ref`, unmerged `allOf`, `$dynamicRef`, and `$recursiveRef` only at schema
positions. Literal enum/const/default/example and extension data are never scanned as
schemas. It does not run a generic root metaschema validation or perform dereferencing.
Nested boolean schemas and both tuple forms are retained. No `treat_unknown_as_string`
reader setting exists. Root annotations/extensions are mirrored into the document envelope;
emitters emit them once, while retaining root type constraints.

### dlt

`unflatten_tables` owns graph validation and reconstruction, including multiple roots,
explicit parents, cycles and ordering. Empty tables are rejected. Each root becomes a
document; document provenance contains the stored-schema envelope and reader policy.
Table and column provenance contain immutable source metadata, including filtered columns.
Emitters must traverse actual nodes, not re-run an old converter over this metadata.

Filtering uses `_dlt_` and `__v_` names, matching the legacy converter, before scalar
classification and required propagation. A child with exactly one visible `value` column
and no children becomes an array of that typed value. Other children use the configured
object/array mode, recorded in document provenance. A child is required when at least one
direct visible column is nonnullable; grandchildren alone do not make it required.

Incomplete and `json` columns become `any`, retaining their nullable fact. Unknown type names
become `unknown` and log, never explicit strings. Decimal is numeric with precision/scale;
decimal-as-string and decimal patterns belong to the JSON emitter. `bigint.precision`
is retained as bit width. A missing timestamp timezone hint remains unknown, not naive.
All source column keys are validated under `x-dlt`, not silently discarded.

### Iceberg

Primitive widths, logical types, decimal precision/scale, fixed length, field IDs and
list/map element IDs are retained. Maps have distinct `key_type` and `value_type` nodes;
they are not JSON dynamic objects or prematurely rendered key/value arrays.
Timestamp and timestamptz are `timestamp-without-tz`/`False` and
`timestamp-with-tz`/`True`. Binary is readable even though the old JSON converter rejects it.
Unmapped types, including current nanosecond/unknown types, raise `NotImplementedError`.

Field descriptions/defaults/IDs live on `Field`; type facts live on `TypedNode`; table
metadata lives on `SchemaDocument`. This separation supports legacy nullable wrapper
placement. `read_table` reads only loaded table accessors, capturing snapshot ID, partition
spec, properties, location, format version, schema ID and identifier IDs without I/O.

`iceberg_value` explicitly converts UUID to text, temporal objects to ISO text and bytes to
base64; finite Decimal stays exact. Other unsupported Python objects fail. PyIceberg's
`NestedField.__init__` currently populates both defaults and doc even when omitted. The
reader preserves those observable null values and cannot recover presence already lost
by PyIceberg. JSON and dlt metadata preserve absent versus null directly.

## Phase 4 Emitter Rules

Consume `document.root`, fields, logical hints, constraints and scoped extensions directly.
Do not reconstruct source schemas as an intermediate call into old monoliths.

1. JSON-to-dlt: implement dlt's existing dispatch order from declared/inferred/enum facts,
   with combiners overriding declared types where the legacy converter does so. Both
   forward converters prefer `oneOf` over `anyOf` and approximate the first branch.
   Keep nesting limits, scalar flattening and JSON fallback in the emitter.
2. JSON-to-PySpark: recognized declared or inferred types precede combiners. Retain field
   presence separately from nullable values; apply strict/lenient unknown handling there.
   Use constraints and annotations for formats, numeric widths and field metadata.
3. dlt-to-JSON: apply existing logical-type templates, decimal string pattern, precision/
   scale and hint-selection policy. Put column descriptions and `x-dlt` inside nullable
   wrappers. Empty `any` nodes without emitted metadata must stay `{}`. Use `required_names`
   for exact legacy required-list order. Root titles/IDs are derived from document names;
   table descriptions are retained on root/child nodes.
4. Iceberg-to-JSON: derive integer bounds, formats, decimal string patterns and map-as-pairs
   encoding from logical hints. Type metadata goes inside optional `anyOf`; field metadata
   and field descriptions go outside it. Nullable list elements and map values use their
   own wrappers. Legacy mode omits null defaults, suppresses element IDs it never emitted,
   and preserves its unsupported-type errors even where IR can represent more.

Identity claims are limited to supported behavior. JSON constraints preserved by the reader
need not survive a legacy target conversion. dlt child-table ambiguity is irrecoverable.
An IR-aware future mode may improve output, but compatibility mode must match captured
structured outputs, not claim arbitrary JSON/YAML byte identity.

## Tests and Limits

`tests/data/converters/ir/legacy_outputs.json` contains seven fixed cases with inputs,
options and mechanically captured outputs from all four unchanged converter routes.
Spark outputs use `StructType.jsonValue()` without a Spark session. Other outputs are
stored dlt dictionaries or JSON Schema documents. Cases cover nested trees, scalar arrays,
unions, precedence, dynamic objects, metadata, multi-root dlt, and Iceberg maps/defaults.
The existing precise converter tests remain primary; the corpus is not exhaustive.

Tests are under `tests/cdm_data_loaders/converters/readers/`, plus `test_ir.py` and
`test_extensions.py`. No internal mocks, external services or Spark sessions are used.

Verified phase-3 results: 783 converter tests passed, including 136 new tests. Focused
coverage of the new production modules reached 100% statements and 99% combined
statement/branch coverage; the remaining branch is absent PyIceberg field-default metadata.
Targeted Ruff checks and formatting passed, and editor diagnostics reported no errors.
An isolated interpreter import confirmed the IR loads no dlt, PyIceberg or PySpark modules.

Commands run from the repository root:

```sh
uv run pytest tests/cdm_data_loaders/converters -m 'not requires_spark and not requires_ceph and not external_request'
uv run pytest tests/cdm_data_loaders/converters/test_ir.py tests/cdm_data_loaders/converters/test_extensions.py tests/cdm_data_loaders/converters/readers --cov=cdm_data_loaders.converters.ir --cov=cdm_data_loaders.converters.ir_values --cov=cdm_data_loaders.converters.extensions --cov=cdm_data_loaders.converters.readers --cov-branch --cov-report=term-missing
uv run ruff check src/cdm_data_loaders/converters/ir.py src/cdm_data_loaders/converters/ir_values.py src/cdm_data_loaders/converters/extensions.py src/cdm_data_loaders/converters/readers tests/cdm_data_loaders/converters/test_ir.py tests/cdm_data_loaders/converters/test_extensions.py tests/cdm_data_loaders/converters/readers
uv run ruff format src/cdm_data_loaders/converters/ir.py src/cdm_data_loaders/converters/ir_values.py src/cdm_data_loaders/converters/extensions.py src/cdm_data_loaders/converters/readers tests/cdm_data_loaders/converters/test_ir.py tests/cdm_data_loaders/converters/test_extensions.py tests/cdm_data_loaders/converters/readers
```

Readers recurse over typed children; unlike the iterative dlt normalizer, they do not
promise support beyond Python's recursion limit. JSON input validation is upstream.
Extension validation uses registered JSON Schemas; caller-provided schemas are trusted
configuration and should be self-contained. No public Decimal JSON serializer or emitter
exists in this phase. These are implementation boundaries, not output-equivalence claims.