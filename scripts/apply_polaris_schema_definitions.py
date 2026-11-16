r"""Recursively apply table and field definitions from a PyArrow schema to an Iceberg table in Polaris.

Usage::

    uv run python -m scripts.apply_polaris_schema_definitions \
        --catalog polaris \
        --table my_namespace.id_mapping \
        --schema-source cdm_data_loaders.schemas.uniprot.idmapping.schema:ID_MAPPING_SCHEMA
"""

import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from logging import Logger, getLogger
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, Final

import pyarrow as pa
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import CliApp, SettingsConfigDict
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StructType

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import JSONSchemaToPyArrow
from cdm_data_loaders.core.fields import NonEmptyStr
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.utils.cdm_logger import init_logger
from cdm_data_loaders.utils.iceberg_cleanup import install_iceberg_catalog_cleanup

logger: Logger = getLogger(__name__)

SCRIPT_NAME: Final[str] = "apply_polaris_schema_definitions"
DEFAULT_CATALOG_NAME: Final[str] = "polaris"
TABLE_COMMENT_PROPERTY: Final[str] = "comment"
DEFAULT_DEFINITION_METADATA_KEYS: Final[tuple[bytes, ...]] = (
    b"doc",
    b"comment",
    b"description",
)
CLI_SHORTCUTS: Final[dict[str, str]] = {
    "catalog": "c",
    "table": "t",
    "schema_source": "s",
    "schema_key": "k",
}
SNAKE_CASE_NAMING: Final[SnakeCaseNamingConvention] = SnakeCaseNamingConvention()


def _normalize_metadata_key(key: bytes | str) -> bytes:
    """Convert a metadata key to UTF-8 bytes when supplied as ``str``.

    :param key: Metadata key as ``bytes`` or ``str``.
    :type key: bytes | str
    :return: UTF-8 encoded metadata key bytes.
    :rtype: bytes
    :raises ValueError: If the encoded key is empty.
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

        :param value: Raw ``metadata_keys`` input.
        :type value: object
        :return: Tuple of non-empty UTF-8 ``bytes`` keys, or raw ``value`` when not a sequence.
        :rtype: object
        :raises ValueError: If ``metadata_keys`` is empty.
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
    """Outcome of applying PyArrow table and field definitions to a Polaris Iceberg table."""

    model_config = ConfigDict(frozen=True)

    table_identifier: tuple[str, ...]
    dry_run: bool = False
    table_comment_applied: str | None = None
    table_comment_unchanged: bool = False
    updated_fields: dict[str, str] = Field(default_factory=dict)
    unchanged_fields: tuple[str, ...] = ()
    unmatched_arrow_fields: tuple[str, ...] = ()
    skipped_non_struct_fields: dict[str, str] = Field(default_factory=dict)


class PolarisCatalogConfig(BaseModel):
    """Connection parameters for loading a Polaris REST catalog via PyIceberg."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    catalog: NonEmptyStr = DEFAULT_CATALOG_NAME
    uri: NonEmptyStr | None = None
    warehouse: NonEmptyStr | None = None
    credential: NonEmptyStr | None = None
    token: NonEmptyStr | None = None
    scope: NonEmptyStr | None = None
    properties: dict[str, str] = Field(default_factory=dict)

    def to_load_catalog_kwargs(self) -> dict[str, str]:
        """Build keyword overrides for :func:`pyiceberg.catalog.load_catalog`.

        :return: Non-empty catalog property overrides.
        :rtype: dict[str, str]
        """
        explicit: dict[str, str | None] = {
            "uri": self.uri,
            "warehouse": self.warehouse,
            "credential": self.credential,
            "token": self.token,
            "scope": self.scope,
        }
        merged: dict[str, str] = {
            **self.properties,
            **{key: val for key, val in explicit.items() if val is not None},
        }
        if self.uri is not None and self.uri.startswith(("http://", "https://")) and "type" not in merged:
            merged["type"] = "rest"
        return merged


class PolarisSchemaDefinitionsSettings(LoggerSettings):
    """CLI settings for applying PyArrow table and field definitions to a Polaris table."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=SCRIPT_NAME,
        cli_shortcuts=CLI_SHORTCUTS,
    )

    catalog: Annotated[
        NonEmptyStr,
        Field(
            default=DEFAULT_CATALOG_NAME,
            description="Catalog name in ~/.pyiceberg.yaml or Polaris catalog identifier",
        ),
    ]
    table: Annotated[
        NonEmptyStr,
        Field(description="Dot-separated table identifier in Polaris (for example, 'my_namespace.my_table')"),
    ]
    schema_source: Annotated[
        NonEmptyStr,
        Field(
            description=(
                "PyArrow schema source: 'module.path[:ATTR]', 'path/to/schema.py[:ATTR]', or 'path/to/schema.json'"
            )
        ),
    ]
    schema_key: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Key in a schema mapping or ArrowSchemaRegistry when schema_source exports multiple schemas",
        ),
    ]
    table_description: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="Explicit table comment overriding schema-level metadata"),
    ]
    uri: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Polaris REST catalog URI override (for example, 'http://polaris:8181/api/catalog')",
        ),
    ]
    warehouse: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="Polaris warehouse name override"),
    ]
    credential: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="OAuth2 'client_id:client_secret' credential override"),
    ]
    token: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="Bearer token override"),
    ]
    scope: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="OAuth2 scope override (for example, 'PRINCIPAL_ROLE:ALL')"),
    ]
    metadata_keys: Annotated[
        tuple[NonEmptyStr, ...],
        Field(
            default=("doc", "comment", "description"),
            description="Ordered Arrow metadata keys inspected for table and field definitions",
        ),
    ]
    normalize_root_names: Annotated[
        bool,
        Field(
            default=True,
            description="Fall back to dlt snake_case root column names when exact Arrow names are absent",
        ),
    ]
    strict: Annotated[
        bool,
        Field(
            default=False,
            description="Raise an error if any PyArrow field has no match in the target Polaris table",
        ),
    ]
    dry_run: Annotated[
        bool,
        Field(
            default=False,
            description="Compute table and field definition updates without committing to Polaris",
        ),
    ]


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

    :param metadata: PyArrow schema or field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :param metadata_keys: Ordered candidate metadata keys to inspect.
    :type metadata_keys: Sequence[bytes | str]
    :return: Decoded definition text, or ``None`` when no candidate key has non-blank text.
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

    :param pa_type: Input PyArrow data type.
    :type pa_type: pa.DataType
    :return: Underlying value type when dictionary-encoded, else ``pa_type``.
    :rtype: pa.DataType
    """
    if pa.types.is_dictionary(pa_type):
        return pa_type.value_type
    return pa_type


def _is_arrow_list_type(pa_type: pa.DataType) -> bool:
    """Return ``True`` if ``pa_type`` is a list-like Arrow container (excluding ``MapType``).

    :param pa_type: Input PyArrow data type.
    :type pa_type: pa.DataType
    :return: ``True`` for ``list``, ``large_list``, ``fixed_size_list``, ``list_view``, or ``large_list_view``.
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

    :param pa_fields: Root PyArrow schema fields.
    :type pa_fields: Sequence[pa.Field]
    :param by_name: Root Iceberg fields indexed by exact column name.
    :type by_name: Mapping[str, NestedField]
    :return: Mapping from unmatched root Arrow field name to matched Iceberg ``NestedField``.
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

    :param pa_field: Arrow field (such as a map key field) whose Iceberg counterpart cannot store ``doc``.
    :type pa_field: pa.Field
    :param parent_arrow_path: Ancestor Arrow path segments.
    :type parent_arrow_path: tuple[str, ...]
    :param metadata_keys: Ordered metadata keys inspected for definitions.
    :type metadata_keys: Sequence[bytes]
    :param target: Output dictionary mapping dot-joined Arrow paths to skipped definitions.
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

    :param pa_type: Nested PyArrow container type.
    :type pa_type: pa.DataType
    :param arrow_path: Ancestor Arrow path segments.
    :type arrow_path: tuple[str, ...]
    :param unmatched: Accumulator of dot-joined unmatched Arrow field paths.
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

    :param pa_type: PyArrow field type.
    :type pa_type: pa.DataType
    :param iceberg_type: Corresponding Iceberg field type.
    :type iceberg_type: IcebergType
    :param arrow_path: Ancestor Arrow field path segments.
    :type arrow_path: tuple[str, ...]
    :param iceberg_path: Ancestor Iceberg field path segments.
    :type iceberg_path: tuple[str, ...]
    :param state: Mutable planning state.
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
        # Iceberg MapType forbids altering map keys and stores no doc on key or value slots
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

    :param pa_fields: PyArrow fields at the current struct level.
    :type pa_fields: Sequence[pa.Field]
    :param iceberg_fields: Iceberg ``NestedField`` entries at the current struct level.
    :type iceberg_fields: Sequence[NestedField]
    :param arrow_path: Ancestor Arrow path segments (empty at schema root).
    :type arrow_path: tuple[str, ...]
    :param iceberg_path: Ancestor Iceberg path segments (empty at schema root).
    :type iceberg_path: tuple[str, ...]
    :param state: Mutable planning state.
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

    :param arrow_schema: Input PyArrow schema carrying table and field metadata.
    :type arrow_schema: pa.Schema
    :param iceberg_schema: Target Iceberg table schema.
    :type iceberg_schema: Schema
    :param options: Optional matching and metadata key settings.
    :type options: DefinitionSyncOptions | None
    :return: Planned table comment, matched field docs, unmatched paths, and skipped slot definitions.
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

    :param iceberg_schema: Current Iceberg table schema.
    :type iceberg_schema: Schema
    :param field_docs: Planned ``{field_path_tuple: doc}`` mapping.
    :type field_docs: Mapping[tuple[str, ...], str]
    :return: Tuple of ``(fields_to_update, updated_by_canonical_name, unchanged_canonical_names)``.
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

    :param table: Target PyIceberg table handle.
    :type table: Table
    :param table_comment_applied: New table comment to set, or ``None`` if unchanged.
    :type table_comment_applied: str | None
    :param fields_to_update: Mapping of field path tuples to new ``doc`` strings.
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

    :param table: Target PyIceberg table handle (for example, loaded from a Polaris ``RestCatalog``).
    :type table: Table
    :param schema: Input PyArrow schema with table and field definitions in metadata.
    :type schema: pa.Schema
    :param options: Optional matching, strictness, and dry-run settings.
    :type options: DefinitionSyncOptions | None
    :return: Report summarizing applied, unchanged, unmatched, and skipped definitions.
    :rtype: SchemaDefinitionsReport
    :raises ValueError: If ``options.strict`` is ``True`` and ``schema`` has unmatched Arrow fields.
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

    :param catalog: Open PyIceberg catalog connection (such as a Polaris ``RestCatalog``).
    :type catalog: Catalog
    :param table_identifier: Dot-separated table name or ``(namespace, ..., table)`` tuple.
    :type table_identifier: str | tuple[str, ...]
    :param schema: Input PyArrow schema with table and field definitions in metadata.
    :type schema: pa.Schema
    :param options: Optional matching, strictness, and dry-run settings.
    :type options: DefinitionSyncOptions | None
    :return: Report summarizing applied, unchanged, unmatched, and skipped definitions.
    :rtype: SchemaDefinitionsReport
    """
    table = catalog.load_table(table_identifier)
    return apply_schema_definitions_to_table(table, schema, options=options)


def connect_polaris_catalog(config: PolarisCatalogConfig) -> Catalog:
    """Open a PyIceberg catalog connection using ``config`` merged with ``~/.pyiceberg.yaml``.

    Always use the returned catalog as a context manager (``with connect_polaris_catalog(cfg) as catalog:``).

    :param config: Polaris catalog connection settings.
    :type config: PolarisCatalogConfig
    :return: Initialized PyIceberg ``Catalog`` instance.
    :rtype: Catalog
    """
    install_iceberg_catalog_cleanup()
    return load_catalog(config.catalog, **config.to_load_catalog_kwargs())


def _split_schema_source(schema_source: str) -> tuple[str, str | None]:
    """Split ``schema_source`` into ``(module_or_path, optional_attribute_name)``.

    :param schema_source: Raw schema source string (for example, ``pkg.mod:SCHEMA`` or ``path/to/file.py:SCHEMA``).
    :type schema_source: str
    :return: Tuple of ``(target_spec, attr_name_or_none)``.
    :rtype: tuple[str, str | None]
    """
    head, sep, tail = schema_source.rpartition(":")
    if not sep or not head or not tail:
        return schema_source, None
    # Preserve Windows drive-letter prefixes such as 'C:\path\to\schema.py'
    if (len(head) == 1 and head.isalpha()) or "/" in tail or "\\" in tail:
        return schema_source, None
    return head, tail


def _load_python_module(target: str) -> ModuleType:
    """Import ``target`` either from a ``.py`` file path or as a dotted Python module path.

    :param target: Python file path or dotted module import path.
    :type target: str
    :return: Loaded Python module object.
    :rtype: ModuleType
    :raises ValueError: If a file path cannot be loaded as a Python module.
    """
    candidate_path = Path(target)
    if candidate_path.suffix == ".py" or candidate_path.is_file():
        resolved = candidate_path.resolve()
        spec = spec_from_file_location(f"_dynamic_schema_{resolved.stem}", resolved)
        if spec is None or spec.loader is None:
            msg = f"Cannot load Python module from path: {resolved}"
            raise ValueError(msg)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return import_module(target)


def _select_schema_from_object(
    obj: Any,  # noqa: ANN401
    source_label: str,
    *,
    schema_key: str | None = None,
) -> pa.Schema:
    """Extract a ``pa.Schema`` from a direct schema, mapping, or ``ArrowSchemaRegistry``.

    :param obj: Candidate object exported by a schema module.
    :type obj: Any
    :param source_label: Human-readable label for error messages.
    :type source_label: str
    :param schema_key: Optional key within a schema mapping or ``ArrowSchemaRegistry``.
    :type schema_key: str | None
    :return: Resolved PyArrow schema.
    :rtype: pa.Schema
    :raises TypeError: If the resolved object is not a ``pa.Schema`` or mapping of schemas.
    :raises ValueError: If ``schema_key`` is missing, unexpected, or not found.
    """
    if isinstance(obj, pa.Schema):
        if schema_key is not None:
            msg = f"schema_key={schema_key!r} cannot be used because {source_label} is already a single pyarrow.Schema"
            raise ValueError(msg)
        return obj

    mapping: Mapping[str, Any] | None = None
    if isinstance(obj, ArrowSchemaRegistry):
        mapping = obj.registry
    elif isinstance(obj, Mapping):
        mapping = obj

    if mapping is None:
        msg = f"{source_label} must be a pyarrow.Schema, ArrowSchemaRegistry, or Mapping[str, pyarrow.Schema], got {type(obj).__name__}"
        raise TypeError(msg)

    if schema_key is None:
        available = ", ".join(sorted(str(k) for k in mapping))
        msg = f"{source_label} contains multiple schemas ({available}); pass schema_key / --schema-key to select one"
        raise ValueError(msg)

    if schema_key not in mapping:
        available = ", ".join(sorted(str(k) for k in mapping))
        msg = f"Schema key {schema_key!r} not found in {source_label}; available keys: {available}"
        raise ValueError(msg)

    selected = mapping[schema_key]
    if not isinstance(selected, pa.Schema):
        msg = f"{source_label}[{schema_key!r}] must be a pyarrow.Schema, got {type(selected).__name__}"
        raise TypeError(msg)
    return selected


def _load_json_schema_as_pyarrow(json_path: Path, *, schema_key: str | None = None) -> pa.Schema:
    """Convert a JSON Schema file on disk into a ``pa.Schema``, preserving top-level ``description``.

    :param json_path: Path to a Draft 2020-12 JSON Schema file.
    :type json_path: Path
    :param schema_key: Disallowed for single-document JSON Schema files.
    :type schema_key: str | None
    :return: Converted PyArrow schema with ``b"comment"`` table metadata when ``description`` is set.
    :rtype: pa.Schema
    :raises ValueError: If ``schema_key`` is provided.
    :raises TypeError: If the JSON root is not an object.
    """
    if schema_key is not None:
        msg = f"schema_key={schema_key!r} is not supported when loading JSON Schema file {json_path}"
        raise ValueError(msg)
    raw = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"JSON Schema file {json_path} must contain a JSON object at root"
        raise TypeError(msg)
    converted = JSONSchemaToPyArrow().convert(raw)
    root_desc = raw.get("description")
    if isinstance(root_desc, str) and root_desc.strip() and extract_metadata_definition(converted.metadata) is None:
        merged_meta: dict[bytes | str, bytes | str] = dict(converted.metadata or {})
        merged_meta[b"comment"] = root_desc
        converted = converted.with_metadata(merged_meta)
    return converted


def load_pyarrow_schema(schema_source: str, *, schema_key: str | None = None) -> pa.Schema:
    """Load a ``pa.Schema`` from a Python module/file spec or a JSON Schema file.

    Supported ``schema_source`` forms:

    - ``package.module:ATTRIBUTE`` or ``path/to/schema.py:ATTRIBUTE``
    - ``package.module`` or ``path/to/schema.py`` (defaults to ``SCHEMA``, or ``OUTPUT_SCHEMAS`` / ``SCHEMAS`` when ``schema_key`` is set)
    - ``path/to/schema.json`` (converted via :class:`JSONSchemaToPyArrow`)

    :param schema_source: Module path, Python file path (with optional ``:ATTR``), or ``.json`` schema file path.
    :type schema_source: str
    :param schema_key: Key within a schema mapping or ``ArrowSchemaRegistry`` when selecting from multiple schemas.
    :type schema_key: str | None
    :return: Loaded PyArrow schema.
    :rtype: pa.Schema
    :raises ValueError: If no matching schema attribute can be resolved.
    """
    target, attr_name = _split_schema_source(schema_source)
    target_path = Path(target)
    if attr_name is None and target_path.suffix.lower() == ".json":
        return _load_json_schema_as_pyarrow(target_path, schema_key=schema_key)

    module = _load_python_module(target)
    if attr_name is not None:
        if not hasattr(module, attr_name):
            msg = f"Module {target!r} has no attribute {attr_name!r}"
            raise ValueError(msg)
        return _select_schema_from_object(getattr(module, attr_name), f"{target}:{attr_name}", schema_key=schema_key)

    if schema_key is not None:
        for candidate in ("OUTPUT_SCHEMAS", "SCHEMAS"):
            if hasattr(module, candidate):
                return _select_schema_from_object(
                    getattr(module, candidate),
                    f"{target}:{candidate}",
                    schema_key=schema_key,
                )
        msg = f"Module {target!r} defines neither 'OUTPUT_SCHEMAS' nor 'SCHEMAS' for schema_key={schema_key!r}"
        raise ValueError(msg)

    if hasattr(module, "SCHEMA"):
        return _select_schema_from_object(module.SCHEMA, f"{target}:SCHEMA")

    msg = f"Module {target!r} has no default 'SCHEMA' attribute; specify '{target}:<ATTR>' or pass --schema-key"
    raise ValueError(msg)


def run_from_settings(settings: PolarisSchemaDefinitionsSettings) -> SchemaDefinitionsReport:
    """Connect to the configured Polaris catalog, load the PyArrow schema, and apply definitions.

    :param settings: Parsed CLI or programmatic settings.
    :type settings: PolarisSchemaDefinitionsSettings
    :return: Report summarizing applied and skipped definitions.
    :rtype: SchemaDefinitionsReport
    """
    schema = load_pyarrow_schema(settings.schema_source, schema_key=settings.schema_key)
    catalog_cfg = PolarisCatalogConfig(
        catalog=settings.catalog,
        uri=settings.uri,
        warehouse=settings.warehouse,
        credential=settings.credential,
        token=settings.token,
        scope=settings.scope,
    )
    options = DefinitionSyncOptions(
        table_description=settings.table_description,
        metadata_keys=settings.metadata_keys,
        normalize_root_names=settings.normalize_root_names,
        strict=settings.strict,
        dry_run=settings.dry_run,
    )
    with connect_polaris_catalog(catalog_cfg) as catalog:
        report = apply_schema_definitions(catalog, settings.table, schema, options=options)

    action = "Planned" if report.dry_run else "Applied"
    logger.info(
        "%s definitions on %s: table_comment=%r, updated_fields=%d, unchanged_fields=%d",
        action,
        ".".join(report.table_identifier),
        report.table_comment_applied,
        len(report.updated_fields),
        len(report.unchanged_fields),
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and apply PyArrow schema definitions to a Polaris table.

    :param argv: Optional CLI argument sequence (defaults to ``sys.argv[1:]``).
    :type argv: Sequence[str] | None
    :return: Exit code ``0`` on completion.
    :rtype: int
    """
    cli_args = list(argv) if argv is not None else None
    settings = CliApp.run(PolarisSchemaDefinitionsSettings, cli_args=cli_args)
    init_logger(settings)
    report = run_from_settings(settings)
    print(report.model_dump_json(indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
