# Converters: prep-normalization and IR refactor plan

This is the implementation plan for restructuring `src/cdm_data_loaders/converters/` around a
canonical intermediate representation (IR) with reader and emitter components, preceded by
extracting the shared JSON-Schema core and a standalone dlt unflatten step.

Background and rationale are in the converter analysis: the four direction-specific converters
(`jsonschema_to_dlt`, `jsonschema_to_pyspark`, `dlt_to_jsonschema`, `pyiceberg_to_jsonschema`)
duplicate the same logic (union/enum resolution, implicit-type inference, dereferencing guards,
string/file IO, nullable conventions) and are edges of one graph rather than four unrelated
programs.

## Target architecture

Four monolithic converters become three readers, three emitters, and a typed node tree in
between. Each current converter is a reader+emitter pair; new conversions (e.g. dlt -> PySpark,
or a future PyArrow emitter) become small compositions instead of new monoliths.

```
readers (X -> IR)                    IR                      emitters (IR -> Y)
  JsonSchemaReader      dereferenced JSON Schema  ->   \\  //->  JsonSchemaEmitter   draft 2020-12 doc
  DltReader             TStoredSchema             ->   TypedNode tree  ->  DltEmitter       TStoredSchema
  IcebergReader         pyiceberg Schema.fields   ->   /            \\->  PySparkEmitter   StructType
```

Package layout after the refactor (existing direction subpackages remain as public facades):

```
src/cdm_data_loaders/converters/
  core/                         shared, direction-agnostic logic (phase 1)
    errors.py                   ConversionError base; subclasses stay direction-specific
    guards.py                   $schema / $ref / allOf / root-type assertions
    inference.py                implicit-type inference, JSON enum inference, decimal scale
    io.py                       parse/serialize helpers for str/file entry points
  ir.py                         TypedNode model (phase 3)
  dlt_normalization.py          unflatten + parents-first flatten + shared child-path
                                primitives (phase 2; also consumed by test-side
                                row-level reconstruction)
  readers/
    json_schema.py              dereferenced JSON Schema -> IR
    dlt.py                      TStoredSchema -> IR (uses dlt_normalization)
    iceberg.py                  pyiceberg Schema -> IR
  emitters/
    json_schema.py              IR -> draft 2020-12 JSON Schema document
    dlt.py                      IR -> TStoredSchema (uses dlt_normalization)
    pyspark.py                  IR -> StructType
  jsonschema_to_dlt/            facade: JSONSchemaToDlt = JsonSchemaReader + DltEmitter
  jsonschema_to_pyspark/        facade: JSONSchemaToPySpark = JsonSchemaReader + PySparkEmitter
  dlt_to_jsonschema/            facade: DltToJSONSchema = DltReader + JsonSchemaEmitter
  pyiceberg_to_jsonschema/      facade: module functions over IcebergReader + JsonSchemaEmitter
  jsonschema_to_pyspark/dereferencer.py   unchanged (already a standalone prep step)
```

## The IR

A single pydantic model, frozen, mirroring the repo's existing conventions. The `hints` slot
formalizes what the current code smuggles through `x-dlt` / `x-iceberg` blocks.

```python
NodeType = Literal["object", "map", "array", "string", "integer", "number",
                   "boolean", "null", "any", "never"]

class NodeHints(BaseModel):
    format: str | None = None          # date, date-time, time, uuid, ...
    pattern: str | None = None         # decimal pattern from precision/scale
    enum: tuple[Any, ...] | None = None
    precision: int | None = None
    scale: int | None = None
    logical_type: str | None = None    # dlt 'timestamp'/'decimal'/'wei', iceberg 'fixed', ...
    extensions: dict[str, Any] = {}    # validated x-* keyed vendor data (see below)

class TypedNode(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    type: NodeType
    nullable: bool = True
    required: bool = False
    description: str | None = None
    hints: NodeHints = Field(default_factory=NodeHints)
    children: tuple[TypedNode, ...] = ()   # object properties, declaration order
    items: TypedNode | None = None         # array element type
    key_type: TypedNode | None = None      # map key (iceberg MapType)
    value_type: TypedNode | None = None    # map value (iceberg MapType / PySpark MapType)
```

Type vocabulary decisions:

- Base types are JSON-Schema-ish (`string`, `integer`, `number`, `boolean`) because two of the
  three emitters are JSON-Schema-descended; dlt/iceberg refinements live in `hints.logical_type`.
- `any` and `never` are first-class, corresponding to JSON Schema's boolean `true`/`false`
  schemas. Emitters decide their own fallback (`treat_unknown_as_string` stays a PySpark-emitter
  option); the IR no longer pretends they are strings.
- `map` is separate from `object`: PySpark MapType and iceberg MapType are dynamic-key
  structures that the JSON-Schema emitter renders as arrays of key/value pairs (the existing
  `pyiceberg_to_jsonschema` convention).
- Keeping `enum` values in the IR is a deliberate improvement: both current converters use enum
  values only for type inference and then discard them. Emitters may now re-emit them; default
  behavior stays unchanged until the emitters opt in.

Known lossiness (recorded here so tests encode it rather than discover it):

- dlt's stored schema cannot distinguish a nested dict from a list of objects; both flatten to
  child tables. `DltReader` records `logical_type`-adjacent evidence in `hints.extensions`
  (e.g. `{"x-dlt": {"child_table": true}}`) and the JSON-Schema emitter's `child_table_mode`
  option decides object-vs-array, exactly as today. No IR can recover information the source
  format never recorded.
- dlt `timestamp` is timezone-naive; iceberg `Timestamptz` is aware. `hints.logical_type`
  distinguishes them; the draft 2020-12 emitter renders both as `format: date-time` strings
  (current behavior).
- dlt has one integer type (`bigint`); int32-vs-int64 width hints from the PySpark side are not
  representable and are dropped (current behavior).
- `max_nesting`, `skip_nested_types`, `flatten_scalars` (JSONSchemaToDlt) become DltEmitter
  options with identical defaults.

Extension-key validation (decision settled): `NodeHints.extensions` is a validated `x-*` keyed
mapping, reusing the repo's existing validated-extension idiom:

- `readers/jsonschema_xsv/xsv_validator/custom_metaschema.py` defines the pattern: a typed
  sub-schema for the extension block with `additionalProperties: False`, per-key typed
  properties, and a required-key `anyOf`. The IR's `extensions` gets the equivalent treatment:
  keys must start with `x-`, and values are either scalars or nested `x-*`-keyed mappings,
  mirroring the `{"x-dlt": {...}}` / `{"x-iceberg": {...}}` blocks the current converters emit.
- `readers/jsonschema_xsv/xsv_validator/schema_utils.py` provides the consumer-side key
  transformation to reuse: `k.replace("x-", "").replace("-", "_")`, the same convention
  `get_schema_parsing_metadata` uses to turn `x-has-header` into `has_header`.
- A pydantic field validator on `NodeHints.extensions` enforces the key pattern and rejects
  values colliding with first-class `NodeHints` fields, so malformed vendor data fails loudly at
  reader construction rather than silently at emit time.

## Phase 0: inventory existing code for reusable logic

Goal: before writing new modules, survey the repo for existing implementations of the same
primitives (reconstruction, output reading, name mapping) so the new code reuses rather than
re-implements.

Known starting points (found in the initial survey; the inventory phase extends this table):

| Location                                                         | What it does                                                                                                                                                       | Reuse relevance                                                                                                                                           |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration/pipelines/xml/xmltodict_reference_helpers.py` | `reconstruct_entries` rebuilds nested dicts from flattened output tables, following `_dlt_parent_id` / `_dlt_list_idx` row linkage rather than table-name prefixes | Data-driven twin of the schema-driven `unflatten_tables` (phase 2); the row-linkage walk is the ground-truth reconstruction semantics my dlt notes record |
| `tests/integration/pipelines/helpers.py`                         | imports `reconstruct_entries` for `assert_dataset_matches_reference`                                                                                               | Already shared beyond xmltodict; a move to shared code must keep this import working                                                                      |
| `scripts/output_tests_core.py`                                   | Diagnostic reading of parquet/jsonl pipeline output (rglob, magic-byte gzip detection, JSON-string parsing of nested content)                                      | Fragment-level overlap with the test-side readers; candidate for consolidation only if it is cheap                                                        |
| `scripts/output_tests_{iceberg,parquet,pyarrow_*}.py`            | Variants of the same output-inspection tooling                                                                                                                     | Same as above; survey for duplication breadth                                                                                                             |

Inventory method: grep for the primitive names (`reconstruct`, `parent_id`, `list_idx`,
`set_nested`, `read_parquet`, `rglob`) across `src/`, `scripts/`, and `tests/`; for each hit,
decide reuse / consolidate / leave, and record the decision. Any helper promoted from
`tests/` or `scripts/` into `src/cdm_data_loaders/converters/` moves with its tests
(see phase 2).

## Phase 0 (completed 2026-09): inventory results

The inventory is complete. Recorded decisions:

| Primitive | Locations found | Decision |
| --- | --- | --- |
| Row-level reconstruction (`reconstruct_entries`, `_attach_nested_rows`, `set_nested`, `_child_table_path`) | `tests/integration/pipelines/xml/xmltodict_reference_helpers.py`; consumed by `tests/integration/pipelines/helpers.py` and the xmltodict reference tests | Phase 2 extracts the schema-independent primitives (`child_table_path`, `set_nested`) into `dlt_normalization.py`; the row-linkage walk stays test-side |
| Child-table naming (`NESTED_TABLE_SEPARATOR`, `parent__key`) | `dlt_to_jsonschema._child_key`, `jsonschema_to_dlt` inline naming (3 sites), `xmltodict_reference_helpers._child_table_path` | Consolidate into `dlt_normalization.py` as one `child_key(parent, child)` helper (phase 2) |
| Parents-first table ordering | `jsonschema_to_dlt._tables_parents_first` | Moves to `dlt_normalization.py` (phase 2) |
| Enum narrowest-type walk | `jsonschema_to_dlt._data_type_from_enum` (tested), `jsonschema_to_pyspark._infer_type_from_enum` (tested) | Consolidate into `core/inference.py` (phase 1), keeping both module paths importable for their existing tests |
| Implicit-type inference | `jsonschema_to_pyspark._infer_implicit_type` + keyword sets; imported by `jsonschema_to_dlt` | Move to `core/inference.py` (phase 1), re-export from `jsonschema_to_pyspark.converter` |
| Decimal helper | `jsonschema_to_dlt._decimal_places`, `jsonschema_to_pyspark._decimal_places` (identical) | Consolidate into `core/inference.py` (phase 1) |
| Gzip magic-byte detection, JSON-string parsing | `xmltodict_reference_helpers` only (single site) | Leave: one site, no duplication |
| Output-inspection diagnostics (rglob parquet, print tooling) | `scripts/output_tests_*.py` (5 files) | Leave: diagnostic scripts, not library code |

## Phase 1: extract the shared JSON-Schema core

Goal: remove the verbatim duplication between `jsonschema_to_dlt` and `jsonschema_to_pyspark`
without changing any public behavior.

New files:

- `converters/core/errors.py`: `ConversionError(ValueError)`; direction-specific errors inherit
  this base while retaining their definitions and identities in their existing modules.
- `converters/core/guards.py`:
  - `require_schema_keyword(schema)` -- rejects missing top-level `$schema` (message text kept
    identical; both tests match on it).
  - `require_object_root(schema)` -- `type not in (None, "object")`.
  - `reject_unresolved_references(schema)` -- the current `$ref`/`allOf` check, one copy.
- `converters/core/inference.py`:
  - `_infer_implicit_type` and the `IMPLICIT_*_KEYWORDS` sets, moved out of
    `jsonschema_to_pyspark.converter` (ownership fix: these are JSON-Schema-generic).
  - `json_type_from_enum(values)` -- the bool -> int -> float -> string walk, returning JSON
    type names. Target-specific wrappers map these names to dlt and PySpark types.
  - `decimal_places(value) -> int`.
- `converters/core/io.py`: `load_schema_text` defaults to JSON-only parsing. Reverse conversion
  explicitly enables YAML fallback. `load_schema_file` uses JSON for `.json` and YAML otherwise.

Type dispatch remains direction-specific. dlt lets combiners override declared types, but not
enum or implicit inference; PySpark dispatches recognized declared or inferred types first.
dlt falls back to text for unknown constructs and boolean combiner branches. PySpark can reject
these with `treat_unknown_as_string=False`. Both prefer `oneOf` over `anyOf` and use the first
branch. Warnings remain on each converter's module logger; no logging callback is injected.
Core imports neither dlt nor PySpark.

Changed files: both forward converters and `dlt_to_jsonschema/converter.py` use core helpers.
Private helper imports and PySpark's implicit keyword exports remain available as aliases or
target-specific wrappers. Reverse conversion deep-copies `TYPE_MAP` entries to isolate nested
`x-dlt` metadata across columns and conversion calls.

Tests: new `tests/cdm_data_loaders/converters/core/` tests cover each helper, error identity,
private exports, parsing policies, combiner precedence, and strict fallback. A reverse-converter
regression covers nested metadata isolation. Tests are typed and parametrized with readable ids.

Exit criteria: `uv run pytest tests/cdm_data_loaders/converters -m "not requires_spark and not
requires_ceph"` green; `uv run ruff check src tests && uv run ruff format src tests` clean.

## Phase 2: dlt normalization as a standalone module

Goal: split `dlt_to_jsonschema`'s folding logic (and `jsonschema_to_dlt._tables_parents_first`)
into independently testable pure functions over `TStoredSchema`.

New file `converters/dlt_normalization.py`:

- `unflatten_tables(tables) -> dict[str, Any]` -- folds `parent`-linked child tables into a
  nested structure. Moves in: `_validate_parents`, `_child_key`, `_is_scalar_value_table`
  detection, and the `_convert_child` recursion from `dlt_to_jsonschema`. Scalar `value` child
  tables become `{"type": "array", "items": <col>}` nodes in the nested form.
- `flatten_nodes(root_name, nested) -> dict[str, TTableSchema]` -- the inverse, parents-first.
  Moves in `_tables_parents_first` from `jsonschema_to_dlt` plus child-table naming
  (`parent__key` via `NESTED_TABLE_SEPARATOR`).
- Shared child-path primitives, extracted so the row-level reconstruction in
  `xmltodict_reference_helpers.reconstruct_entries` can consume the same functions instead of
  maintaining its own copies: `child_table_path(table_name, top_table)` (the
  `_child_table_path` fragment-stripping walk) and `set_nested(target, path, value)`. The
  test-side helper differs in linkage semantics (`_dlt_parent_id`/`_dlt_list_idx` vs schema
  `parent` entries) and name unmangling, so it keeps its own walk but calls these primitives.
- `DltNormalizationError(ValueError)` for dangling parents and cycles.

`dlt_to_jsonschema` is then rewired: `convert()` = `unflatten_tables` -> walk the nested tree.
Its remaining unique part is the column-to-JSON-Schema mapping (`TYPE_MAP`, nullable `anyOf`
wrapping, hint collection), which stays in place until phase 4.

Tests: `tests/cdm_data_loaders/converters/test_dlt_normalization.py` -- unit tests for
`unflatten_tables` (happy path, dangling parent, scalar-value table, deep nesting) and
`flatten_nodes` (parents-first ordering, max depth); a round-trip test
`unflatten -> flatten == original` on fixture schemas. Data fixtures under
`tests/data/converters/` rather than inline dicts where they get large. If the test-side
reconstruction helpers move into shared code, their existing consumers
(`tests/integration/pipelines/helpers.py`, `test_xmltodict_reference_tests.py`) keep working via
re-export from the old import path.

Exit criteria: same as phase 1, plus `dlt_to_jsonschema` tests green against the rewired path.

## Phase 3: the IR and readers

Goal: introduce `ir.py` and convert three sources into it.

- `converters/ir.py`: `NodeType`, `NodeHints`, `TypedNode` as specified above.
- `converters/readers/json_schema.py`: `JsonSchemaReader(treat_unknown_as_string=...)`. Input is
  a dereferenced document (the dereferencer stays the prep step; the reader runs the phase-1
  guards defensively). Walks properties/required, recurses into objects and arrays-of-objects,
  produces `TypedNode` trees. `true` -> `type="any"`, `false` -> `type="never"`.
- `converters/readers/dlt.py`: `DltReader(include_dlt_columns=..., include_variant_columns=...)`.
  Filters `_dlt_*` and variant columns (moved from `DltToJSONSchema._skip_column`), then maps
  `TYPE_MAP` dtypes to IR types + hints (`decimal` -> `string` + precision/scale + pattern,
  `timestamp` -> `string` + `logical_type: timestamp`, `binary` -> `string` + base64 hint, `wei`
  -> `integer` + `logical_type: wei`).
- `converters/readers/iceberg.py`: `IcebergReader()`. Moves the iceberg-type-to-node mapping
  from `pyiceberg_to_jsonschema` (`TYPE_CONVERTER` walk), preserving precision/scale pattern
  generation and the timestamp-without-tz logical type.

Tests: per-reader unit tests plus an equivalence test per existing converter: for a corpus of
fixture schemas, `reader(x)` must reproduce the same decisions the current monolith makes
(type, nullability, child structure). Fixtures: the existing test data for the three converters,
extended where coverage is thin (map types, scalar-value child tables, boolean schemas).

Exit criteria: equivalence tests green; no emitter changes yet (the monoliths still produce the
outputs).

## Phase 4: emitters

- `converters/emitters/json_schema.py`: `JsonSchemaEmitter(child_table_mode=...,
  preserve_unknown_hints=...)`. Renders `TypedNode` trees to draft 2020-12: nullable ->
  `anyOf: [<type>, {"type": "null"}]`, hints -> `x-dlt`/`x-iceberg` blocks, root-level `$schema`
  / `$id` / `title`. Consumed by both `dlt_to_jsonschema` and `pyiceberg_to_jsonschema` facades.
- `converters/emitters/dlt.py`: `DltEmitter(schema_name=..., skip_nested_types=..., max_nesting=...,
  write_disposition=..., flatten_scalars=...)`. Uses `flatten_nodes`; produces `TStoredSchema`
  and the live-`Schema` merge via the existing parents-first `update_table` flow.
- `converters/emitters/pyspark.py`: `PySparkEmitter(format_map=..., treat_unknown_as_string=...,
  extra_metadata_keywords=...)`. Renders StructType/StructField metadata exactly as
  `_build_metadata` does today (title under `jsonschema`, description as `comment`).

Tests: golden-output equivalence against the current monoliths' outputs for the full fixture
corpus (write goldens from current behavior in the phase-3 PR so drift is reviewable). Round-trip
invariants: `dlt -> IR -> dlt` stable modulo the recorded lossiness; `json -> IR -> json` stable
for schemas that use no unsupported constructs.

Exit criteria: all four facades produce byte-identical JSON/YAML/`TStoredSchema`/`StructType`
outputs to the pre-refactor code on the fixture corpus.

## Phase 5: facades and cleanup

- Rewrite the four public classes as thin compositions (reader -> IR -> emitter), keeping their
  names, constructor fields, and module paths. `convert_from_string` / `convert_from_file` come
  from `core.io`. `pyiceberg_to_jsonschema` keeps its module-function API
  (`table_to_json_schema`) delegating to `IcebergReader` + `JsonSchemaEmitter`; the existing
  `dump_catalog_schemas` post-conversion `x-iceberg` stamping is unaffected because the emitter
  still produces the `x-iceberg` block at the root.
- Delete the now-dead private methods from the monoliths; keep `_infer_implicit_type` re-exported
  from `jsonschema_to_pyspark.converter` for the one known importer.
- No deprecation shims: the public names never change, so there is nothing to deprecate. A later
  PR may add new compositions (e.g. `dlt_to_pyspark`) now that they are one-liners.

## Sequencing, scope, and non-goals

Each phase is an independently mergeable PR with the suite green at its boundary. Phase 0 is a
read-only survey whose output is the reuse-decision table; phases 1 and 2 are safe, immediate
deduplication and could ship on their own even if the IR work stalls; phases 3-5 are the
architectural payoff. Non-goals: no new target formats, no output changes, no
dlt/iceberg dependency upgrades, no changes to the dereferencer's behavior.

## Conventions and constraints

- Python 3.13+, `uv run` from repo root, ruff (line length 120, full `PL` ruleset). Long dispatch
  functions will need `# noqa: PLR0911` as the current ones do; no function-level imports
  (`PLC0415`) except the existing live-`Schema` lazy import in the dlt emitter, which keeps its
  exemption.
- Tests: module-based typed `def test_*() -> None` functions, pytest fixtures in `conftest.py`,
  `pytest.mark.parametrize` with human-readable ids, no external-network dependencies, no mocking
  of internal code. Run with `uv run pytest tests/cdm_data_loaders/converters` (the repo has
  pre-existing collection errors elsewhere; run the converters subtree, not the full suite).

## Open decisions

Settled (2026-09):

- `NodeHints.extensions` is a validated `x-*` keyed mapping, not a pass-through dict. The
  validation approach and the reused `custom_metaschema.py` / `schema_utils.py` patterns are
  specified under "Extension-key validation" above.
- Warning behavior stays on module-level `logging` rather than an injectable callback:
  logging is for communicating with the user, the current converters already emit these messages
  through `logger.warning`, and no current consumer needs to intercept them. An injectable would
  add a parameter to a hot path for no present use.
