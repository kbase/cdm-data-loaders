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
    paths.py                    generic set_nested dictionary assignment (phase 2)
  ir.py                         TypedNode model (phase 3)
  dlt_normalization.py          typed raw-table tree, unflatten, parents-first flatten,
                                schema child keys and name joining (phase 2)
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

Phase 3's implemented specification and complete public signatures are in
[converters_ir.md](converters_ir.md). Frozen dataclasses separate `SchemaDocument`,
`TypedNode` and `Field`; all metadata is deeply owned and immutable. Presence and value
nullability are independent. Declared types, union lists, inference, ordered branches,
schema-valued children and literal constraints remain available without choosing target policy.

`any`, `never`, `null` and `unknown` are distinct. Iceberg maps differ from JSON dynamic
objects. Decimal precision/scale are numeric source facts; string patterns belong to emitters.
dlt timestamps are not inherently timezone-naive, and bigint precision can represent width.
The old converters' discarded hints are legacy lossiness, not source-format limitations.
dlt child-table object/array ambiguity is resolved and recorded at the reader boundary.

Extensions use an immutable registry of exact namespace names and JSON validation schemas.
Only namespace names require `x-`; internal keys are ordinary names, arrays/null are allowed
by their schemas, and key spelling is never transformed. Unknown namespaces and payload
keys fail unless explicitly registered. `x-xsv-config` reuses the exact existing schema.
This is deliberately stricter than unchanged pre-phase-5 facades. Logging uses module loggers.

## Phase 0: inventory existing code for reusable logic

Goal: before writing new modules, survey the repo for existing implementations of the same
primitives (reconstruction, output reading, name mapping) so the new code reuses rather than
re-implements.

Known starting points (found in the initial survey; the inventory phase extends this table):

| Location                                                         | What it does                                                                                                                                                       | Reuse relevance                                                                                                                                           |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration/pipelines/xml/xmltodict_reference_helpers.py` | `reconstruct_entries` attaches rows using `_dlt_parent_id` / `_dlt_list_idx`; XML property paths denormalize names and strip repeated root fragments | Schema normalization instead follows explicit table `parent` entries; only generic dictionary assignment is shared |
| `tests/integration/pipelines/helpers.py`                         | imports `reconstruct_entries` for `assert_dataset_matches_reference`                                                                                               | Already shared beyond xmltodict; a move to shared code must keep this import working                                                                      |
| `scripts/output_tests_core.py`                                   | Diagnostic inspection of Iceberg/Parquet output, parquet rglob, and JSON-string parsing of nested content; no gzip implementation | Distinct from the test-side JSONL reader and its gzip magic-byte detection; leave as diagnostic tooling |
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
| Row-level reconstruction (`reconstruct_entries`, `_attach_nested_rows`, `set_nested`, `_child_table_path`) | `tests/integration/pipelines/xml/xmltodict_reference_helpers.py`; consumed by `tests/integration/pipelines/helpers.py` and the xmltodict reference tests | Phase 2 extracts only `set_nested` into `core/paths.py`, imported at the old helper path. Row linkage and XML-specific `_child_table_path` stay test-side unchanged |
| Schema child-table naming (`NESTED_TABLE_SEPARATOR`, `parent__key`) | `dlt_to_jsonschema._child_key`, `jsonschema_to_dlt` inline naming (3 sites) | Phase 2 shares `child_key(parent, child)` and `child_table_name(parent, key)`. Explicit `parent` fields determine schema hierarchy; remove exactly one declared-parent prefix, with no XML denormalization |
| Parents-first table ordering | `jsonschema_to_dlt._tables_parents_first` | Moves to `dlt_normalization.py` (phase 2) |
| Enum narrowest-type walk | `jsonschema_to_dlt._data_type_from_enum` (tested), `jsonschema_to_pyspark._infer_type_from_enum` (tested) | Consolidate into `core/inference.py` (phase 1), keeping both module paths importable for their existing tests |
| Implicit-type inference | `jsonschema_to_pyspark._infer_implicit_type` + keyword sets; imported by `jsonschema_to_dlt` | Move to `core/inference.py` (phase 1), re-export from `jsonschema_to_pyspark.converter` |
| Decimal helper | `jsonschema_to_dlt._decimal_places`, `jsonschema_to_pyspark._decimal_places` (identical) | Consolidate into `core/inference.py` (phase 1) |
| Gzip magic-byte detection | `xmltodict_reference_helpers.read_pipeline_tables` | Leave: the output-inspection scripts have no gzip implementation |
| JSON-string parsing | `xmltodict_reference_helpers.parse_json_string` and `scripts/output_tests_core.py` inspection functions | Leave: row reconstruction and diagnostic printing have different contracts |
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

## Phase 2: completed - standalone dlt normalization

`converters/dlt_normalization.py` provides a neutral, lossless table forest. It operates on
the `tables` mapping, not the surrounding stored-schema envelope. Public API:

```python
@dataclass(frozen=True, slots=True)
class DltTableNode:
    name: str
    parent: str | None
    key: str
    table: TTableSchema
    children: dict[str, "DltTableNode"] = field(default_factory=dict)

def unflatten_tables(tables: Mapping[str, TTableSchema]) -> dict[str, DltTableNode]: ...
def flatten_nodes(roots: Mapping[str, DltTableNode]) -> dict[str, TTableSchema]: ...
def tables_parents_first(tables: Mapping[str, TTableSchema]) -> list[TTableSchema]: ...
def child_key(parent_name: str, child_name: str) -> str: ...
def child_table_name(parent_name: str, key: str) -> str: ...
```

- Table mapping keys identify nodes. Raw `name` metadata is preserved even when it differs.
  Roots use their full mapping name as their property key. Explicit `parent` entries alone
  determine hierarchy; names containing `__` do not imply a parent.
- `child_key` removes exactly one `<declared-parent>__` prefix, or retains the full child
  name when absent. It does not split paths, decode XML names, or remove repeated roots.
  `child_table_name` joins two literal fragments with `__`, including empty fragments.
- Root and sibling order follow input declaration order. Flattening uses depth-first,
  parents-first order and preserves all table/column metadata, including missing fields,
  internal columns, variants, and scalar `value` columns. Empty input returns empty output.
  Unflatten/flatten round trips preserve definitions, not arbitrary child-before-parent order.
- Input, independent calls, each table's payload, and flattened copies are isolated through
  deep copies. Node identity fields are frozen; owned table and children mappings are mutable.
  Traversals are iterative and tested beyond Python's recursion limit.
- `DltNormalizationError(ConversionError)` rejects malformed structural fields, dangling
  parents, all cycles (including disconnected components), and duplicate sibling keys.
  Flattening also rejects inconsistent node keys/parents, duplicate names, and node cycles.
  Rootless cycles retain `No root tables found` in the diagnostic.

`DltToJSONSchema.convert()` now normalizes once and traverses node children without scanning
the full tables mapping per node. It translates normalization errors to `DltToJSONSchemaError`.
Scalar-value classification, internal/variant filtering, object-vs-array policy, required
fields, nullable wrapping, and column hint mapping remain converter policy.
`JSONSchemaToDlt.to_schema()` calls shared parents-first ordering; all three child-name sites
use the join helper. Unused private graph helpers were removed.

Only generic dictionary assignment is shared with XML row reconstruction:

```python
def set_nested(target: dict[str, Any], path: Sequence[str], value: object) -> None: ...
```

It lives in `core/paths.py` and remains importable from `xmltodict_reference_helpers`.
Paths must be nonempty sequences of string keys, not bare strings. Missing dicts are created;
non-dict intermediate collisions raise `NestedPathError(ConversionError)` without mutation.
Existing leaves are replaced; empty string keys remain valid; assigned values are not copied.
The row helper's `_child_table_path` retains XML denormalization and repeated-root stripping.
Its `_dlt_parent_id`/`_dlt_list_idx` walk remains distinct from schema `parent` handling.

Tests cover precise forests and round trips, missing metadata, multiple roots, malformed
tables, disconnected cycles, naming exceptions, isolation, deep graphs, and generic path
errors. The raw fixture is `tests/data/converters/dlt_normalization/tables.json`. Pure XML
helper tests live in `tests/integration/pipelines/xml/test_reference_helpers.py` and require
no Spark, CEPH, network, or pipeline fixtures. Cycle/malformed-table and invalid-path
regressions were run failing before their fixes.

Validation commands:

```sh
uv run pytest tests/cdm_data_loaders/converters -m 'not requires_spark and not requires_ceph and not external_request'
uv run pytest tests/cdm_data_loaders/converters/test_dlt_normalization.py tests/cdm_data_loaders/converters/core/test_paths.py tests/integration/pipelines/xml/test_reference_helpers.py -m 'not requires_spark and not requires_ceph and not external_request'
```

Verified: 647 converter tests passed; 69 direct helper tests passed. Both new production
modules have 100% statement and branch coverage in focused helper tests. Targeted Ruff check,
separate formatting, editor diagnostics, and diff whitespace checks passed. No extension
validation or remaining IR-phase design changes are part of Phase 2.

## Phase 3: completed - IR, extension contracts and readers

Added `ir.py`, `ir_values.py`, `extensions.py` and three reader modules. JSON reading is
schema-aware, not a generic scan of literal data. dlt uses `unflatten_tables` and retains
source metadata alongside real typed children. Iceberg supports Schema and loaded Table
envelopes with separate field/type/root metadata scopes and exact Decimal handling.

Seven legacy cases in `tests/data/converters/ir/legacy_outputs.json` capture all four routes
before rewiring: stored dlt dictionaries, Spark `jsonValue()` and JSON Schema documents.
The corpus tests exact structured legacy outputs independently of reader facts. New tests
exercise frozen models, extension contracts, branches, tuples, filtering, cycles and metadata.
Facades, emitters and row reconstruction remain unchanged. See the implemented specification
for validation results, limitations and direct-IR emitter guidance.

## Phase 4: emitters

- `converters/emitters/json_schema.py`: `JsonSchemaEmitter(preserve_unknown_hints=...)`.
  Child-table mode is resolved by the reader. Renders `TypedNode` trees to draft 2020-12: nullable ->
  `anyOf: [<type>, {"type": "null"}]`, hints -> `x-dlt`/`x-iceberg` blocks, root-level `$schema`
  / `$id` / `title`. Consumed by both `dlt_to_jsonschema` and `pyiceberg_to_jsonschema` facades.
- `converters/emitters/dlt.py`: `DltEmitter(schema_name=..., skip_nested_types=..., max_nesting=...,
  write_disposition=..., flatten_scalars=...)`. Uses `flatten_nodes`; produces `TStoredSchema`
  and the live-`Schema` merge via the existing parents-first `update_table` flow.
- `converters/emitters/pyspark.py`: `PySparkEmitter(format_map=..., treat_unknown_as_string=...,
  extra_metadata_keywords=...)`. Renders StructType/StructField metadata exactly as
  `_build_metadata` does today (title under `jsonschema`, description as `comment`).

Tests: compare emitter outputs to the fixed phase-3 corpus and existing precise tests.
Round-trip invariants apply only modulo explicitly supported behavior and source lossiness;
reader preservation does not imply unsupported JSON keywords survive target conversion.

Exit criteria: direct reader/emitter compositions match legacy structured outputs on the
corpus. Facades remain unchanged until phase 5. Do not claim general JSON/YAML byte identity.

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

- Extensions on nodes, fields and documents are schema-validated immutable mappings.
  Namespaces and payload keys have distinct contracts; no `schema_utils.py` key rewriting
  is used. Custom namespaces need explicit schemas in the reader's registry.
- Warning behavior stays on module-level `logging` rather than an injectable callback:
  logging is for communicating with the user, the current converters already emit these messages
  through `logger.warning`, and no current consumer needs to intercept them. An injectable would
  add a parameter to a hot path for no present use.
