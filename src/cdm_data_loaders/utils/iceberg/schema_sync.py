"""Recursive PyArrow-to-Iceberg table comment and field ``doc`` synchronization."""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Final

import pyarrow as pa
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StructType

logger: Logger = getLogger(__name__)

TABLE_COMMENT_PROPERTY: Final[str] = "comment"
DEFAULT_DEFINITION_METADATA_KEYS: Final[tuple[bytes, ...]] = (
    b"doc",
    b"comment",
    b"description",
)
SNAKE_CASE_NAMING: Final[SnakeCaseNamingConvention] = SnakeCaseNamingConvention()


def _normalize_metadata_key(key: bytes | str) -> bytes:
    """Convert a metadata key to UTF-8 bytes when supplied as ``str``.

    :param key: metadata key as ``bytes`` or ``str``
    :type key: bytes | str
    :return: UTF-8 encoded metadata key bytes
    :rtype: bytes
    :raises ValueError: if the encoded key is empty
    """
    encoded = key if isinstance(key, bytes) else key.encode("utf-8")
    if not encoded.strip():
        msg = "Metadata key must not be empty"
        raise ValueError(msg)
    return encoded


class DefinitionSyncOptions(BaseModel):
    """Options controlling how PyArrow definitions map onto an Iceberg table."""

    model_config = ConfigDict(frozen=True)

    table_description: str | None = None
    metadata_keys: tuple[bytes, ...] = DEFAULT_DEFINITION_METADATA_KEYS
    normalize_root_names: bool = True
    strict: bool = False
    dry_run: bool = False

    @field_validator("metadata_keys", mode="before")
    @classmethod
    def _coerce_metadata_keys(cls, value: object) -> object:
        """Coerce sequences of ``str`` or ``bytes`` metadata keys into ``tuple[bytes, ...]``.

        :param value: raw ``metadata_keys`` input
        :type value: object
        :return: tuple of non-empty UTF-8 ``bytes`` keys, or raw ``value`` when not a sequence
        :rtype: object
        :raises ValueError: if ``metadata_keys`` is empty
        """
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            return value
        keys = tuple(_normalize_metadata_key(item) for item in value)
        if not keys:
            msg = "metadata_keys must contain at least one key"
            raise ValueError(msg)
        return keys


class SchemaDefinitionsPlan(BaseModel):
    """Resolved table comment and per-field documentation staged from a PyArrow schema."""

    model_config = ConfigDict(frozen=True)

    table_comment: str | None = None
    field_docs: dict[tuple[str, ...], str] = Field(default_factory=dict)
    unmatched_arrow_fields: tuple[str, ...] = ()
    skipped_non_struct_fields: dict[str, str] = Field(default_factory=dict)


class SchemaDefinitionsReport(BaseModel):
    """Outcome of applying PyArrow table and field definitions to an Iceberg table."""

    model_config = ConfigDict(frozen=True)

    table_identifier: tuple[str, ...]
    dry_run: bool = False
    table_comment_applied: str | None = None
    table_comment_unchanged: bool = False
    updated_fields: dict[str, str] = Field(default_factory=dict)
    unchanged_fields: tuple[str, ...] = ()
    unmatched_arrow_fields: tuple[str, ...] = ()
    skipped_non_struct_fields: dict[str, str] = Field(default_factory=dict)


@dataclass(slots=True)
class _PlannerState:
    """Mutable accumulator used while walking a PyArrow schema alongside an Iceberg schema."""

    options: DefinitionSyncOptions
    field_docs: dict[tuple[str, ...], str]
    unmatched_arrow_fields: list[str]
    skipped_non_struct_fields: dict[str, str]


def extract_metadata_definition(
    metadata: Mapping[bytes, bytes] | None,
    metadata_keys: Sequence[bytes | str] = DEFAULT_DEFINITION_METADATA_KEYS,
) -> str | None:
    """Return the first non-blank UTF-8 definition string found in ``metadata``.

    :param metadata: PyArrow schema or field metadata mapping
    :type metadata: Mapping[bytes, bytes] | None
    :param metadata_keys: ordered candidate metadata keys to inspect
    :type metadata_keys: Sequence[bytes | str]
    :return: decoded definition text, or ``None`` when no candidate key has non-blank text
    :rtype: str | None
    """
    if not metadata:
        return None
    for raw_key in metadata_keys:
        key_bytes = _normalize_metadata_key(raw_key)
        raw_val = metadata.get(key_bytes)
        if raw_val is not None:
            decoded = raw_val.decode("utf-8")
            if decoded.strip():
                return decoded
    return None


def _unwrap_dictionary_type(pa_type: pa.DataType) -> pa.DataType:
    """Unwrap ``pa.DictionaryType`` to its underlying ``value_type``.

    :param pa_type: input PyArrow data type
    :type pa_type: pa.DataType
    :return: underlying value type when dictionary-encoded, else ``pa_type``
    :rtype: pa.DataType
    """
    if pa.types.is_dictionary(pa_type):
        return pa_type.value_type
    return pa_type


def _is_arrow_list_type(pa_type: pa.DataType) -> bool:
    """Return ``True`` if ``pa_type`` is a list-like Arrow container (excluding ``MapType``).

    :param pa_type: input PyArrow data type
    :type pa_type: pa.DataType
    :return: ``True`` for ``list``, ``large_list``, ``fixed_size_list``, ``list_view``, or ``large_list_view``
    :rtype: bool
    """
    if pa.types.is_map(pa_type):
        return False
    return bool(
        pa.types.is_list(pa_type)
        or pa.types.is_large_list(pa_type)
        or pa.types.is_fixed_size_list(pa_type)
        or pa.types.is_list_view(pa_type)
        or pa.types.is_large_list_view(pa_type)
    )


def _build_root_normalized_lookup(
    pa_fields: Sequence[pa.Field],
    by_name: Mapping[str, NestedField],
) -> dict[str, NestedField]:
    """Map unmatched root Arrow field names to unambiguous ``dlt`` ``snake_case`` Iceberg fields.

    :param pa_fields: root PyArrow schema fields
    :type pa_fields: Sequence[pa.Field]
    :param by_name: root Iceberg fields indexed by exact column name
    :type by_name: Mapping[str, NestedField]
    :return: mapping from unmatched root Arrow field name to matched Iceberg ``NestedField``
    :rtype: dict[str, NestedField]
    """
    exact_claimed = {field.name for field in pa_fields if field.name in by_name}
    unmatched = [field.name for field in pa_fields if field.name not in by_name]
    normalized_by_arrow = {name: SNAKE_CASE_NAMING.normalize_identifier(name) for name in unmatched}
    counts = Counter(normalized_by_arrow.values())

    fallback: dict[str, NestedField] = {}
    for arrow_name, norm_name in normalized_by_arrow.items():
        if counts[norm_name] == 1 and norm_name not in exact_claimed and norm_name in by_name:
            fallback[arrow_name] = by_name[norm_name]
    return fallback


def _collect_skipped_subtree_definitions(
    pa_field: pa.Field,
    parent_arrow_path: tuple[str, ...],
    metadata_keys: Sequence[bytes],
    target: dict[str, str],
) -> None:
    """Record definitions on an Arrow field and its descendants that Iceberg cannot annotate.

    :param pa_field: Arrow field whose Iceberg counterpart cannot store ``doc``
    :type pa_field: pa.Field
    :param parent_arrow_path: ancestor Arrow path segments
    :type parent_arrow_path: tuple[str, ...]
    :param metadata_keys: ordered metadata keys inspected for definitions
    :type metadata_keys: Sequence[bytes]
    :param target: output dictionary mapping dot-joined Arrow paths to skipped definitions
    :type target: dict[str, str]
    """
    field_path = (*parent_arrow_path, pa_field.name)
    if definition := extract_metadata_definition(pa_field.metadata, metadata_keys):
        target[".".join(field_path)] = definition

    pa_type = _unwrap_dictionary_type(pa_field.type)
    if pa.types.is_struct(pa_type):
        for idx in range(pa_type.num_fields):
            _collect_skipped_subtree_definitions(pa_type.field(idx), field_path, metadata_keys, target)
    elif pa.types.is_map(pa_type):
        _collect_skipped_subtree_definitions(pa_type.key_field, field_path, metadata_keys, target)
        _collect_skipped_subtree_definitions(pa_type.item_field, field_path, metadata_keys, target)
    elif _is_arrow_list_type(pa_type):
        _collect_skipped_subtree_definitions(pa_type.value_field, field_path, metadata_keys, target)


def _collect_unmatched_arrow_fields(
    pa_type: pa.DataType,
    arrow_path: tuple[str, ...],
    unmatched: list[str],
) -> None:
    """Record nested Arrow struct field paths when the target Iceberg column has a non-matching type.

    :param pa_type: nested PyArrow container type
    :type pa_type: pa.DataType
    :param arrow_path: ancestor Arrow path segments
    :type arrow_path: tuple[str, ...]
    :param unmatched: accumulator of dot-joined unmatched Arrow field paths
    :type unmatched: list[str]
    """
    resolved = _unwrap_dictionary_type(pa_type)
    if pa.types.is_struct(resolved):
        unmatched.extend(".".join((*arrow_path, resolved.field(idx).name)) for idx in range(resolved.num_fields))
    elif pa.types.is_map(resolved):
        _collect_unmatched_arrow_fields(resolved.item_field.type, (*arrow_path, resolved.item_field.name), unmatched)
    elif _is_arrow_list_type(resolved):
        _collect_unmatched_arrow_fields(resolved.value_field.type, (*arrow_path, resolved.value_field.name), unmatched)


def _walk_field_types(
    pa_type: pa.DataType,
    iceberg_type: IcebergType,
    arrow_path: tuple[str, ...],
    iceberg_path: tuple[str, ...],
    state: _PlannerState,
) -> None:
    """Recurse into paired Arrow and Iceberg container types to collect nested field definitions.

    :param pa_type: PyArrow field type
    :type pa_type: pa.DataType
    :param iceberg_type: corresponding Iceberg field type
    :type iceberg_type: IcebergType
    :param arrow_path: ancestor Arrow field path segments
    :type arrow_path: tuple[str, ...]
    :param iceberg_path: ancestor Iceberg field path segments
    :type iceberg_path: tuple[str, ...]
    :param state: mutable planning state
    :type state: _PlannerState
    """
    resolved_pa = _unwrap_dictionary_type(pa_type)
    if pa.types.is_struct(resolved_pa):
        if isinstance(iceberg_type, StructType):
            child_fields = [resolved_pa.field(idx) for idx in range(resolved_pa.num_fields)]
            _walk_struct_fields(child_fields, iceberg_type.fields, arrow_path, iceberg_path, state)
        else:
            _collect_unmatched_arrow_fields(resolved_pa, arrow_path, state.unmatched_arrow_fields)
        return

    if pa.types.is_map(resolved_pa):
        # iceberg map type forbids altering map keys and stores no doc on key or value slots
        _collect_skipped_subtree_definitions(
            resolved_pa.key_field,
            arrow_path,
            state.options.metadata_keys,
            state.skipped_non_struct_fields,
        )
        item_arrow_path = (*arrow_path, resolved_pa.item_field.name)
        if isinstance(iceberg_type, MapType):
            if val_def := extract_metadata_definition(resolved_pa.item_field.metadata, state.options.metadata_keys):
                state.skipped_non_struct_fields[".".join(item_arrow_path)] = val_def
            _walk_field_types(
                resolved_pa.item_field.type,
                iceberg_type.value_type,
                item_arrow_path,
                (*iceberg_path, iceberg_type.value_field.name),
                state,
            )
        else:
            _collect_unmatched_arrow_fields(resolved_pa.item_field.type, item_arrow_path, state.unmatched_arrow_fields)
        return

    if _is_arrow_list_type(resolved_pa):
        elem_arrow_path = (*arrow_path, resolved_pa.value_field.name)
        if isinstance(iceberg_type, ListType):
            if elem_def := extract_metadata_definition(resolved_pa.value_field.metadata, state.options.metadata_keys):
                state.skipped_non_struct_fields[".".join(elem_arrow_path)] = elem_def
            _walk_field_types(
                resolved_pa.value_field.type,
                iceberg_type.element_type,
                elem_arrow_path,
                (*iceberg_path, iceberg_type.element_field.name),
                state,
            )
        else:
            _collect_unmatched_arrow_fields(resolved_pa.value_field.type, elem_arrow_path, state.unmatched_arrow_fields)


def _walk_struct_fields(
    pa_fields: Sequence[pa.Field],
    iceberg_fields: Sequence[NestedField],
    arrow_path: tuple[str, ...],
    iceberg_path: tuple[str, ...],
    state: _PlannerState,
) -> None:
    """Match ``pa_fields`` against ``iceberg_fields`` and recurse into child types.

    :param pa_fields: PyArrow fields at the current struct level
    :type pa_fields: Sequence[pa.Field]
    :param iceberg_fields: Iceberg ``NestedField`` entries at the current struct level
    :type iceberg_fields: Sequence[NestedField]
    :param arrow_path: ancestor Arrow path segments (empty at schema root)
    :type arrow_path: tuple[str, ...]
    :param iceberg_path: ancestor Iceberg path segments (empty at schema root)
    :type iceberg_path: tuple[str, ...]
    :param state: mutable planning state
    :type state: _PlannerState
    """
    by_name: dict[str, NestedField] = {field.name: field for field in iceberg_fields}
    root_fallback = (
        _build_root_normalized_lookup(pa_fields, by_name)
        if not arrow_path and state.options.normalize_root_names
        else {}
    )

    for pa_field in pa_fields:
        iceberg_field = by_name.get(pa_field.name) or root_fallback.get(pa_field.name)
        next_arrow_path = (*arrow_path, pa_field.name)
        if iceberg_field is None:
            state.unmatched_arrow_fields.append(".".join(next_arrow_path))
            continue

        next_iceberg_path = (*iceberg_path, iceberg_field.name)
        if definition := extract_metadata_definition(pa_field.metadata, state.options.metadata_keys):
            state.field_docs[next_iceberg_path] = definition

        _walk_field_types(
            pa_field.type,
            iceberg_field.field_type,
            next_arrow_path,
            next_iceberg_path,
            state,
        )


def plan_schema_definitions(
    arrow_schema: pa.Schema,
    iceberg_schema: Schema,
    options: DefinitionSyncOptions | None = None,
) -> SchemaDefinitionsPlan:
    """Build a :class:`SchemaDefinitionsPlan` mapping ``arrow_schema`` definitions onto ``iceberg_schema``.

    :param arrow_schema: input PyArrow schema carrying table and field metadata
    :type arrow_schema: pa.Schema
    :param iceberg_schema: target Iceberg table schema
    :type iceberg_schema: Schema
    :param options: optional matching and metadata key settings
    :type options: DefinitionSyncOptions | None
    :return: planned table comment, matched field docs, unmatched paths, and skipped slot definitions
    :rtype: SchemaDefinitionsPlan
    """
    resolved_options = options or DefinitionSyncOptions()
    state = _PlannerState(
        options=resolved_options,
        field_docs={},
        unmatched_arrow_fields=[],
        skipped_non_struct_fields={},
    )

    _walk_struct_fields(list(arrow_schema), iceberg_schema.fields, (), (), state)

    table_comment = (
        resolved_options.table_description
        if resolved_options.table_description is not None
        else extract_metadata_definition(arrow_schema.metadata, resolved_options.metadata_keys)
    )
    return SchemaDefinitionsPlan(
        table_comment=table_comment,
        field_docs=state.field_docs,
        unmatched_arrow_fields=tuple(state.unmatched_arrow_fields),
        skipped_non_struct_fields=state.skipped_non_struct_fields,
    )


def _partition_field_updates(
    iceberg_schema: Schema,
    field_docs: Mapping[tuple[str, ...], str],
) -> tuple[dict[tuple[str, ...], str], dict[str, str], tuple[str, ...]]:
    """Split planned field documentation into pending updates and unchanged column names.

    :param iceberg_schema: current Iceberg table schema
    :type iceberg_schema: Schema
    :param field_docs: planned ``{field_path_tuple: doc}`` mapping
    :type field_docs: Mapping[tuple[str, ...], str]
    :return: tuple of ``(fields_to_update, updated_by_canonical_name, unchanged_canonical_names)``
    :rtype: tuple[dict[tuple[str, ...], str], dict[str, str], tuple[str, ...]]
    """
    fields_to_update: dict[tuple[str, ...], str] = {}
    updated_fields: dict[str, str] = {}
    unchanged_fields: list[str] = []
    for field_path, resolved_doc in field_docs.items():
        canonical_name = ".".join(field_path)
        if iceberg_schema.find_field(canonical_name).doc == resolved_doc:
            unchanged_fields.append(canonical_name)
        else:
            fields_to_update[field_path] = resolved_doc
            updated_fields[canonical_name] = resolved_doc
    return fields_to_update, updated_fields, tuple(unchanged_fields)


def _commit_table_definitions(
    table: Table,
    table_comment_applied: str | None,
    fields_to_update: Mapping[tuple[str, ...], str],
) -> None:
    """Commit table ``comment`` property and column ``doc`` updates in a single Iceberg transaction.

    :param table: target PyIceberg table handle
    :type table: Table
    :param table_comment_applied: new table comment to set, or ``None`` if unchanged
    :type table_comment_applied: str | None
    :param fields_to_update: mapping of field path tuples to new ``doc`` strings
    :type fields_to_update: Mapping[tuple[str, ...], str]
    """
    if table_comment_applied is None and not fields_to_update:
        return
    with table.transaction() as tx:
        if table_comment_applied is not None:
            tx.set_properties({TABLE_COMMENT_PROPERTY: table_comment_applied})
        if fields_to_update:
            with tx.update_schema() as update:
                for field_path, doc in fields_to_update.items():
                    update.update_column(field_path, doc=doc)


def apply_schema_definitions_to_table(
    table: Table,
    schema: pa.Schema,
    options: DefinitionSyncOptions | None = None,
) -> SchemaDefinitionsReport:
    """Apply table and recursive field definitions from ``schema`` to a loaded Iceberg ``table``.

    :param table: target PyIceberg table handle
    :type table: Table
    :param schema: input PyArrow schema with table and field definitions in metadata
    :type schema: pa.Schema
    :param options: optional matching, strictness, and dry-run settings
    :type options: DefinitionSyncOptions | None
    :return: report summarizing applied, unchanged, unmatched, and skipped definitions
    :rtype: SchemaDefinitionsReport
    :raises ValueError: if ``options.strict`` is ``True`` and ``schema`` has unmatched Arrow fields
    """
    resolved_options = options or DefinitionSyncOptions()
    iceberg_schema = table.schema()
    plan = plan_schema_definitions(schema, iceberg_schema, resolved_options)
    table_id = table.name()

    if resolved_options.strict and plan.unmatched_arrow_fields:
        joined_unmatched = ", ".join(plan.unmatched_arrow_fields)
        msg = f"Unmatched PyArrow field(s) for table {'.'.join(table_id)}: {joined_unmatched}"
        raise ValueError(msg)

    existing_comment = table.properties.get(TABLE_COMMENT_PROPERTY)
    table_comment_unchanged = plan.table_comment is not None and existing_comment == plan.table_comment
    table_comment_applied = None if table_comment_unchanged else plan.table_comment

    fields_to_update, updated_fields, unchanged_fields = _partition_field_updates(iceberg_schema, plan.field_docs)
    if not resolved_options.dry_run:
        _commit_table_definitions(table, table_comment_applied, fields_to_update)

    if plan.unmatched_arrow_fields:
        logger.warning(
            "Skipped %d unmatched PyArrow field(s) on %s: %s",
            len(plan.unmatched_arrow_fields),
            ".".join(table_id),
            ", ".join(plan.unmatched_arrow_fields),
        )
    if plan.skipped_non_struct_fields:
        logger.info(
            "Skipped %d list/map slot definition(s) unsupported by Iceberg on %s: %s",
            len(plan.skipped_non_struct_fields),
            ".".join(table_id),
            ", ".join(sorted(plan.skipped_non_struct_fields)),
        )

    return SchemaDefinitionsReport(
        table_identifier=table_id,
        dry_run=resolved_options.dry_run,
        table_comment_applied=table_comment_applied,
        table_comment_unchanged=table_comment_unchanged,
        updated_fields=updated_fields,
        unchanged_fields=unchanged_fields,
        unmatched_arrow_fields=plan.unmatched_arrow_fields,
        skipped_non_struct_fields=plan.skipped_non_struct_fields,
    )


def apply_schema_definitions(
    catalog: Catalog,
    table_identifier: str | tuple[str, ...],
    schema: pa.Schema,
    options: DefinitionSyncOptions | None = None,
) -> SchemaDefinitionsReport:
    """Load ``table_identifier`` from ``catalog`` and apply definitions from ``schema``.

    :param catalog: open PyIceberg catalog connection
    :type catalog: Catalog
    :param table_identifier: dot-separated table name or ``(namespace, ..., table)`` tuple
    :type table_identifier: str | tuple[str, ...]
    :param schema: input PyArrow schema with table and field definitions in metadata
    :type schema: pa.Schema
    :param options: optional matching, strictness, and dry-run settings
    :type options: DefinitionSyncOptions | None
    :return: report summarizing applied, unchanged, unmatched, and skipped definitions
    :rtype: SchemaDefinitionsReport
    """
    table = catalog.load_table(table_identifier)
    return apply_schema_definitions_to_table(table, schema, options=options)
