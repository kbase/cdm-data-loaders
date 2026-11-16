"""Pluggable record and PyArrow batch filtering, transformation, multi-table splitting, and output hints."""

import functools
import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Final,
    Self,
    TypeAliasType,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa
from dlt.destinations.impl.filesystem.iceberg_adapter import (
    PARTITION_HINT,
    TABLE_PROPERTIES_HINT,
    PartitionSpec,
)
from dlt.extract import DltResource
from pydantic import BaseModel, ConfigDict, InstanceOf, PrivateAttr, model_validator
from pydantic_settings import BaseSettings

from cdm_data_loaders.coercion.pyarrow import coerce_record_for_pyarrow
from cdm_data_loaders.core.fields import (
    PYARROW_FILE_FORMATS,
    REJECTED_TABLE_NAME,
    LoaderFileFormatEnum,
    OutputSchemasModuleSpec,
    PreprocessorsModuleSpec,
    TransformsModuleSpec,
)
from cdm_data_loaders.core.preprocessors import validate_file_preprocessors
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    FilePreprocessFn,
    PreprocessorRegistry,
    RegistryKey,
    TableTransformFn,
    TransformContext,
    TransformRegistry,
    load_file_preprocessors,
    load_registry_dict,
    validate_registry_table_names,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

if TYPE_CHECKING:
    from dlt.common.schema.typing import TAnySchemaColumns

RAW_TABLE_PREFIX: Final[str] = "raw_"
CONTEXT_POSITIONAL_ARITY: Final[int] = 2
CONTEXT_PARAM_NAMES: Final[frozenset[str]] = frozenset({"ctx", "context"})


def extract_schema_descriptions(schema: pa.Schema) -> tuple[str | None, dict[str, str]]:
    """Extract the table description and per-column descriptions from ``schema`` metadata.

    :param schema: PyArrow schema with optional ``b"comment"`` metadata on the schema and fields
    :type schema: pa.Schema
    :return: tuple of ``(table_description, {column_name: column_description})``
    :rtype: tuple[str | None, dict[str, str]]
    """
    table_desc: str | None = None
    if schema.metadata and b"comment" in schema.metadata:
        decoded = schema.metadata[b"comment"].decode("utf-8")
        if decoded:
            table_desc = decoded

    col_descs: dict[str, str] = {}
    for field in schema:
        if field.metadata and b"comment" in field.metadata:
            col_comment = field.metadata[b"comment"].decode("utf-8")
            if col_comment:
                col_descs[field.name] = col_comment
    return table_desc, col_descs


class OutputTableHints(BaseModel):
    """Per-table schema descriptions and Iceberg partition hints registered on a ``DltResource``.

    :param table_name: destination table name
    :type table_name: str
    :param pyarrow_schema: optional PyArrow schema carrying table and field ``b"comment"`` metadata
    :type pyarrow_schema: pa.Schema | None
    :param description: explicit table description overriding ``pyarrow_schema`` metadata when set
    :type description: str | None
    :param iceberg_partitions: Iceberg partition specifications applied via ``x-iceberg-partition``
    :type iceberg_partitions: tuple[PartitionSpec, ...]
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    table_name: RegistryKey
    pyarrow_schema: InstanceOf[pa.Schema] | None = None
    description: str | None = None
    iceberg_partitions: tuple[InstanceOf[PartitionSpec], ...] = ()

    def apply_to_resource(self, resource: DltResource) -> DltResource:
        """Register an isolated table hint variant for ``self.table_name`` on ``resource``.

        :param resource: target dlt resource
        :type resource: DltResource
        :return: ``resource`` with the table variant hints applied
        :rtype: DltResource
        """
        schema_desc: str | None = None
        col_descs: dict[str, str] = {}
        if self.pyarrow_schema is not None:
            schema_desc, col_descs = extract_schema_descriptions(self.pyarrow_schema)

        resolved_description = self.description if self.description is not None else schema_desc
        columns_hint: TAnySchemaColumns | None = (
            {col: {"description": desc} for col, desc in col_descs.items()} if col_descs else None
        )

        extra_hints: dict[str, Any] = {}
        if resolved_description:
            extra_hints["description"] = resolved_description
            extra_hints[TABLE_PROPERTIES_HINT] = {"comment": resolved_description}
        if self.iceberg_partitions:
            extra_hints[PARTITION_HINT] = [spec.to_dict() for spec in self.iceberg_partitions]

        # initialize the variant first so dlt preserves additional_table_hints even when resource._hints is None
        resource.apply_hints(table_name=self.table_name, create_table_variant=True)
        resource.apply_hints(
            table_name=self.table_name,
            columns=columns_hint,
            additional_table_hints=extra_hints or None,
            create_table_variant=True,
        )
        return resource


def align_and_cast_table(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Align columns of ``table`` to ``target_schema``, cast types, and attach ``target_schema`` metadata.

    :param table: input PyArrow table
    :type table: pa.Table
    :param target_schema: target PyArrow schema
    :type target_schema: pa.Schema
    :return: aligned and cast PyArrow table matching ``target_schema``
    :rtype: pa.Table
    """
    if table.schema.equals(target_schema, check_metadata=True):
        return table
    aligned = table
    for field in target_schema:
        if field.name not in aligned.column_names:
            aligned = aligned.append_column(field, pa.nulls(aligned.num_rows, type=field.type))
    return aligned.select(target_schema.names).cast(target_schema).replace_schema_metadata(target_schema.metadata)


def emit_raw_table(
    data: pa.Table | dict[str, Any],
    ctx: TransformContext,
    *,
    prefix: str = RAW_TABLE_PREFIX,
) -> dict[str, pa.Table | list[dict[str, Any]]]:
    """Emit ``data`` as-is to a destination table named ``f"{prefix}{ctx.table_name}"``.

    Reusable across any pipeline's ``TABLE_TRANSFORMS`` registry; accepts both vectorized
    :class:`pyarrow.Table` batches and single ``dict[str, Any]`` records.

    :param data: validated PyArrow table batch or single record dictionary
    :type data: pa.Table | dict[str, Any]
    :param ctx: transform context identifying the input table name and source file
    :type ctx: TransformContext
    :param prefix: prefix prepended to ``ctx.table_name``, defaults to ``"raw_"``
    :type prefix: str
    :raises ValueError: if ``prefix`` is empty
    :return: mapping routing ``data`` unchanged to ``<prefix><ctx.table_name>``
    :rtype: dict[str, pa.Table | list[dict[str, Any]]]
    """
    if not prefix:
        err_msg = "prefix must be a non-empty string"
        raise ValueError(err_msg)
    out_table = f"{prefix}{ctx.table_name}"
    if isinstance(data, pa.Table):
        return {out_table: data}
    return {out_table: [data]}


@dataclass(frozen=True, slots=True)
class _ComposedTransform:
    """Callable composition of two or more ``TableTransformFn`` steps."""

    steps: tuple[TableTransformFn, ...]
    accepts_pyarrow: bool

    def __call__(
        self,
        data: pa.Table | dict[str, Any],
        ctx: TransformContext,
    ) -> dict[str, list[dict[str, Any]] | pa.Table]:
        """Execute every transform in ``self.steps`` in order and merge their routed outputs.

        :param data: input ``pyarrow.Table`` batch or ``dict[str, Any]`` record
        :type data: pa.Table | dict[str, Any]
        :param ctx: transform context for the input table and source file
        :type ctx: TransformContext
        :return: merged dictionary of routed table outputs
        :rtype: dict[str, list[dict[str, Any]] | pa.Table]
        """
        merged: dict[str, list[dict[str, Any]] | pa.Table] = {}
        for step in self.steps:
            _merge_routed_outputs(merged, _invoke_transform(step, data, ctx))
        return merged


def _unwrap_partial_target(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Unwrap nested ``functools.partial`` wrappers to the underlying callable.

    :param fn: callable or ``functools.partial`` wrapper
    :type fn: Callable[..., Any]
    :return: underlying callable object
    :rtype: Callable[..., Any]
    """
    target: Callable[..., Any] = fn
    while isinstance(target, functools.partial):
        target = target.func
    return target


def _safe_type_hints(fn: Callable[..., Any]) -> dict[str, Any]:
    """Resolve type hints for ``fn`` when possible, returning an empty dict on unresolvable locals.

    :param fn: callable to inspect
    :type fn: Callable[..., Any]
    :return: mapping of parameter names to resolved annotations
    :rtype: dict[str, Any]
    """
    target = _unwrap_partial_target(fn)
    try:
        return get_type_hints(target)
    except (TypeError, NameError, AttributeError, ValueError):
        if not inspect.isfunction(target) and not inspect.ismethod(target) and callable(target):
            try:
                return get_type_hints(type(target).__call__)
            except (TypeError, NameError, AttributeError, ValueError):
                return {}
        return {}


def _annotation_capabilities(ann: object) -> tuple[bool, bool]:
    """Inspect a first-parameter type annotation and return ``(accepts_pyarrow_table, accepts_record_dict)``.

    :param ann: parameter type annotation
    :type ann: object
    :return: pair of booleans ``(accepts_pyarrow_table, accepts_record_dict)``
    :rtype: tuple[bool, bool]
    """
    if isinstance(ann, TypeAliasType):
        return _annotation_capabilities(ann.__value__)
    origin = get_origin(ann)
    if origin is Annotated:
        args = get_args(ann)
        return _annotation_capabilities(args[0]) if args else (False, True)
    if origin in (UnionType, Union):
        sub_caps = [_annotation_capabilities(arg) for arg in get_args(ann) if arg is not type(None)]
        return (any(cap[0] for cap in sub_caps), any(cap[1] for cap in sub_caps)) if sub_caps else (False, True)
    if ann is pa.Table:
        return (True, False)
    if isinstance(ann, str):
        has_table = "Table" in ann
        has_dict = "dict" in ann or "Mapping" in ann
        return (has_table, has_dict or not has_table)
    return (False, True)


def _annotation_is_transform_context(ann: object) -> bool:
    """Return whether ``ann`` refers to :class:`TransformContext` (directly or in a union/Annotated).

    :param ann: parameter type annotation
    :type ann: object
    :return: ``True`` if ``TransformContext`` appears in ``ann``
    :rtype: bool
    """
    if ann is TransformContext:
        return True
    if isinstance(ann, TypeAliasType):
        return _annotation_is_transform_context(ann.__value__)
    origin = get_origin(ann)
    if origin is Annotated:
        args = get_args(ann)
        return _annotation_is_transform_context(args[0]) if args else False
    if origin in (UnionType, Union):
        return any(_annotation_is_transform_context(arg) for arg in get_args(ann))
    if isinstance(ann, str):
        return "TransformContext" in ann
    return False


def _inspect_transform_signature(transform: Callable[..., Any]) -> tuple[bool, bool, bool]:
    """Determine ``(accepts_pyarrow, accepts_dict, takes_context)`` for ``transform``.

    :param transform: table transform callable
    :type transform: Callable[..., Any]
    :return: tuple ``(accepts_pyarrow_table, accepts_record_dict, takes_transform_context)``
    :rtype: tuple[bool, bool, bool]
    """
    if isinstance(transform, _ComposedTransform):
        return (transform.accepts_pyarrow, True, True)

    positional_kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    positional_params = [
        param for param in inspect.signature(transform).parameters.values() if param.kind in positional_kinds
    ]
    hints = _safe_type_hints(transform)

    if positional_params:
        first_param = positional_params[0]
        first_ann = hints.get(first_param.name, first_param.annotation)
        accepts_arrow, accepts_dict = _annotation_capabilities(first_ann)
    else:
        accepts_arrow, accepts_dict = (False, True)

    takes_ctx = False
    if len(positional_params) >= CONTEXT_POSITIONAL_ARITY:
        second_param = positional_params[1]
        second_ann = hints.get(second_param.name, second_param.annotation)
        takes_ctx = (
            second_param.default is inspect.Parameter.empty
            or _annotation_is_transform_context(second_ann)
            or second_param.name in CONTEXT_PARAM_NAMES
        )

    return (accepts_arrow, accepts_dict, takes_ctx)


def accepts_pyarrow_table(transform: Callable[..., Any] | None) -> bool:
    """Return whether ``transform`` declares support for :class:`pyarrow.Table` batch inputs.

    :param transform: optional table transform callable
    :type transform: Callable[..., Any] | None
    :return: ``True`` if ``transform`` accepts a ``pyarrow.Table`` as its first argument
    :rtype: bool
    """
    if transform is None:
        return False
    return _inspect_transform_signature(transform)[0]


def accepts_record_dict(transform: Callable[..., Any] | None) -> bool:
    """Return whether ``transform`` accepts ``dict[str, Any]`` single-record inputs.

    :param transform: optional table transform callable
    :type transform: Callable[..., Any] | None
    :return: ``True`` if ``transform`` accepts a ``dict[str, Any]`` as its first argument
    :rtype: bool
    """
    if transform is None:
        return True
    return _inspect_transform_signature(transform)[1]


def resolve_schema_map(
    default_table: str,
    *,
    transform: TableTransformFn | None = None,
    unreconciled_output_schemas: ArrowSchemaRegistry | None = None,
    default_pyarrow_schema: pa.Schema | None = None,
) -> ArrowSchemaRegistry | None:
    """Resolve the active table-to-PyArrow-schema registry for a reader.

    :param default_table: default target table name for the pipeline/entity
    :type default_table: str
    :param transform: optional ``TableTransformFn`` configured for ``default_table``
    :type transform: TableTransformFn | None
    :param unreconciled_output_schemas: optional ``ArrowSchemaRegistry``
    :type unreconciled_output_schemas: ArrowSchemaRegistry | None
    :param default_pyarrow_schema: default PyArrow schema when no override is configured
    :type default_pyarrow_schema: pa.Schema | None
    :return: ``ArrowSchemaRegistry`` mapping destination table names to ``pa.Schema`` objects, or ``None``
    :rtype: ArrowSchemaRegistry | None
    """
    if unreconciled_output_schemas is not None:
        if transform is None and default_table not in unreconciled_output_schemas.registry:
            err_msg = f"unreconciled_output_schemas for {default_table!r} must include {default_table!r} when transform is None"
            raise ValueError(err_msg)
        return unreconciled_output_schemas
    if default_pyarrow_schema is not None and transform is None:
        return ArrowSchemaRegistry(
            registry={
                default_table: default_pyarrow_schema,
            }
        )
    return None


def validate_routed_output(raw_routed: object) -> dict[str, list[dict[str, Any]] | pa.Table]:
    """Validate the return value of a table transform and strip empty row sequences and 0-row tables.

    :param raw_routed: raw output from ``transform_fn`` for a record or batch
    :type raw_routed: object
    :raises ValueError: if ``raw_routed`` is not a mapping, any table name is empty,
        or values are neither a sequence of non-empty dicts nor a valid :class:`pyarrow.Table`
    :return: validated dictionary mapping destination table names to non-empty row lists or ``pyarrow.Table`` objects
    :rtype: dict[str, list[dict[str, Any]] | pa.Table]
    """
    routed: dict[str, list[dict[str, Any]] | pa.Table] = {}
    errors: list[str] = []
    if not isinstance(raw_routed, Mapping):
        errors.append("transform_fn must return a mapping of table names to sequences of row dicts or pyarrow.Table")
    else:
        for table_name, rows in raw_routed.items():
            if not isinstance(table_name, str) or not table_name.strip():
                errors.append("transform_fn output keys must be non-empty table names")

            if isinstance(rows, pa.Table):
                if rows.num_rows > 0 and rows.num_columns == 0:
                    errors.append(f"transform_fn table for table {table_name!r} must have at least one column")
                    continue
                if not errors and rows.num_rows > 0:
                    routed[table_name.strip()] = rows
                continue

            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray, Mapping)):
                errors.append(
                    f"transform_fn rows for table {table_name!r} must be a sequence of dicts or pyarrow.Table"
                )
                continue

            row_list = list(rows)
            if not all(isinstance(row, dict) for row in row_list) or not all(row for row in row_list):
                errors.append(f"transform_fn rows for table {table_name!r} must be non-empty dicts")
                continue

            if not errors and row_list:
                routed[table_name.strip()] = row_list

    if errors:
        raise ValueError("\n".join(errors))
    return routed


def _merge_routed_outputs(
    target: dict[str, list[dict[str, Any]] | pa.Table],
    addition: Mapping[str, list[dict[str, Any]] | pa.Table],
) -> None:
    """Merge validated routed table outputs from ``addition`` into ``target`` in place.

    :param target: accumulator mapping destination table names to row lists or ``pyarrow.Table`` batches
    :type target: dict[str, list[dict[str, Any]] | pa.Table]
    :param addition: newly validated routed output mapping to append
    :type addition: Mapping[str, list[dict[str, Any]] | pa.Table]
    :return: ``None``
    :rtype: None
    """
    for out_table, out_val in addition.items():
        existing = target.get(out_table)
        if existing is None:
            target[out_table] = out_val
        elif isinstance(existing, pa.Table) and isinstance(out_val, pa.Table):
            target[out_table] = pa.concat_tables([existing, out_val], promote_options="default")
        elif isinstance(existing, list) and isinstance(out_val, list):
            target[out_table] = [*existing, *out_val]
        else:
            existing_rows = existing.to_pylist() if isinstance(existing, pa.Table) else existing
            added_rows = out_val.to_pylist() if isinstance(out_val, pa.Table) else out_val
            target[out_table] = [*existing_rows, *added_rows]


def _call_transform(
    transform: TableTransformFn,
    item: pa.Table | dict[str, Any],
    ctx: TransformContext,
    *,
    takes_ctx: bool,
) -> object:
    """Call ``transform`` with ``(item, ctx)`` when ``takes_ctx`` is ``True``, else ``(item,)``.

    :param transform: table transform callable
    :type transform: TableTransformFn
    :param item: single ``pyarrow.Table`` batch or ``dict[str, Any]`` record
    :type item: pa.Table | dict[str, Any]
    :param ctx: transform context for the input table and source file
    :type ctx: TransformContext
    :param takes_ctx: whether ``transform`` accepts ``ctx`` as its second positional argument
    :type takes_ctx: bool
    :return: raw unvalidated return value from ``transform``
    :rtype: object
    """
    if takes_ctx:
        return transform(item, ctx)  # type: ignore[call-arg, arg-type]
    return transform(item)  # type: ignore[call-arg, arg-type]


def _invoke_transform(
    transform: TableTransformFn,
    data: pa.Table | dict[str, Any],
    ctx: TransformContext,
    *,
    input_schema: pa.Schema | None = None,
) -> dict[str, list[dict[str, Any]] | pa.Table]:
    """Invoke ``transform`` on ``data`` (adapting ``dict``/``pa.Table`` and optional ``ctx``) and validate output.

    :param transform: table transform callable
    :type transform: TableTransformFn
    :param data: input ``pyarrow.Table`` batch or ``dict[str, Any]`` record
    :type data: pa.Table | dict[str, Any]
    :param ctx: transform context for the input table and source file
    :type ctx: TransformContext
    :param input_schema: optional PyArrow schema used when wrapping a single dict into a 1-row ``pa.Table``
    :type input_schema: pa.Schema | None
    :return: validated dictionary of routed table outputs
    :rtype: dict[str, list[dict[str, Any]] | pa.Table]
    """
    accepts_arrow, accepts_dict, takes_ctx = _inspect_transform_signature(transform)

    if isinstance(data, pa.Table):
        if accepts_arrow:
            return validate_routed_output(_call_transform(transform, data, ctx, takes_ctx=takes_ctx))
        merged: dict[str, list[dict[str, Any]] | pa.Table] = {}
        for row in data.to_pylist():
            _merge_routed_outputs(
                merged,
                validate_routed_output(_call_transform(transform, row, ctx, takes_ctx=takes_ctx)),
            )
        return merged

    if accepts_dict:
        return validate_routed_output(_call_transform(transform, data, ctx, takes_ctx=takes_ctx))

    coerced = coerce_record_for_pyarrow(data, input_schema) if input_schema is not None else data
    one_row_table = pa.Table.from_pylist([coerced], schema=input_schema)
    return validate_routed_output(_call_transform(transform, one_row_table, ctx, takes_ctx=takes_ctx))


@functools.cache
def _compose_transform_tuple(step_tuple: tuple[TableTransformFn, ...]) -> TableTransformFn:
    """Build a cached composed transform function from a tuple of two or more transforms.

    :param step_tuple: ordered tuple of table transform callables
    :type step_tuple: tuple[TableTransformFn, ...]
    :return: composed ``TableTransformFn`` executing every transform in ``step_tuple`` in order
    :rtype: TableTransformFn
    """
    return _ComposedTransform(
        steps=step_tuple,
        accepts_pyarrow=all(accepts_pyarrow_table(step) for step in step_tuple),
    )


def compose_transforms(transforms: Sequence[TableTransformFn]) -> TableTransformFn:
    """Compose one or more table transforms so their routed outputs are merged in order.

    :param transforms: non-empty sequence of table transform callables
    :type transforms: Sequence[TableTransformFn]
    :raises ValueError: if ``transforms`` is empty
    :return: single transform when ``len(transforms) == 1``, else a composed ``TableTransformFn``
    :rtype: TableTransformFn
    """
    step_tuple = tuple(transforms)
    if not step_tuple:
        err_msg = "transforms must contain at least one callable"
        raise ValueError(err_msg)
    if len(step_tuple) == 1:
        return step_tuple[0]
    return _compose_transform_tuple(step_tuple)


def _resolve_transform_context(
    default_table: str | TransformContext,
    source_file: str | None = None,
) -> TransformContext:
    """Normalize ``default_table`` (and optional ``source_file``) into a :class:`TransformContext`.

    :param default_table: destination table name or pre-built :class:`TransformContext`
    :type default_table: str | TransformContext
    :param source_file: optional relative path or identifier of the input file being parsed
    :type source_file: str | None
    :return: normalized :class:`TransformContext`
    :rtype: TransformContext
    """
    if isinstance(default_table, TransformContext):
        return (
            TransformContext.for_source(default_table.table_name, source_file)
            if source_file is not None and default_table.source_file is None
            else default_table
        )
    return TransformContext.for_source(default_table, source_file)


def route_and_coerce_record(
    record: dict[str, Any],
    default_table: str | TransformContext,
    *,
    transform: TableTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
    source_file: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Route a validated record through ``transform`` and optionally coerce rows for PyArrow.

    :param record: parsed and validated input record dictionary
    :type record: dict[str, Any]
    :param default_table: destination table name or :class:`TransformContext` when ``transform`` is ``None``
    :type default_table: str | TransformContext
    :param transform: optional ``TableTransformFn`` for ``default_table``
    :type transform: TableTransformFn | None
    :param schema_map: active destination ``ArrowSchemaRegistry``, if any
    :type schema_map: ArrowSchemaRegistry | None
    :param source_file: relative path or identifier of the input file being parsed, if any
    :type source_file: str | None
    :return: dictionary mapping destination table names to non-empty lists of row dictionaries
    :rtype: dict[str, list[dict[str, Any]]]
    """
    ctx = _resolve_transform_context(default_table, source_file)
    routed: dict[str, list[dict[str, Any]] | pa.Table] = (
        _invoke_transform(transform, record, ctx) if transform is not None else {ctx.table_name: [record]}
    )

    if not routed:
        return {}

    result: dict[str, list[dict[str, Any]]] = {}
    for out_table, out_val in routed.items():
        target_schema = schema_map.registry.get(out_table) if schema_map is not None else None
        if isinstance(out_val, pa.Table):
            cast_tbl = align_and_cast_table(out_val, target_schema) if target_schema is not None else out_val
            rows = cast_tbl.to_pylist()
            if rows:
                result[out_table] = rows
        elif target_schema is not None:
            result[out_table] = [coerce_record_for_pyarrow(row, target_schema) for row in out_val]
        else:
            result[out_table] = out_val
    return result


def route_and_coerce_table(
    table: pa.Table,
    default_table: str | TransformContext,
    *,
    transform: TableTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
    use_pyarrow: bool = True,
) -> dict[str, pa.Table | list[dict[str, Any]]]:
    """Route a validated :class:`pyarrow.Table` batch through ``transform`` and cast outputs against ``schema_map``.

    :param table: validated input PyArrow table batch
    :type table: pa.Table
    :param default_table: destination table name or :class:`TransformContext` carrying ``source_file``
    :type default_table: str | TransformContext
    :param transform: optional ``TableTransformFn`` for ``default_table``
    :type transform: TableTransformFn | None
    :param schema_map: active destination ``ArrowSchemaRegistry``, if any
    :type schema_map: ArrowSchemaRegistry | None
    :param use_pyarrow: whether mapped destination tables should be emitted as ``pyarrow.Table`` objects
    :type use_pyarrow: bool
    :return: dictionary mapping destination table names to non-empty ``pyarrow.Table`` or ``list[dict[str, Any]]`` pages
    :rtype: dict[str, pa.Table | list[dict[str, Any]]]
    """
    ctx = _resolve_transform_context(default_table)
    if table.num_rows == 0 and transform is None:
        return {}

    routed: dict[str, list[dict[str, Any]] | pa.Table] = (
        _invoke_transform(transform, table, ctx) if transform is not None else {ctx.table_name: table}
    )

    if not routed:
        return {}

    result: dict[str, pa.Table | list[dict[str, Any]]] = {}
    for out_table, out_val in routed.items():
        if not use_pyarrow:
            rows = out_val.to_pylist() if isinstance(out_val, pa.Table) else out_val
            if rows:
                result[out_table] = rows
            continue

        target_schema = (
            REJECTED_PYARROW_SCHEMA
            if out_table == REJECTED_TABLE_NAME
            else (schema_map.registry.get(out_table) if schema_map is not None else None)
        )
        if isinstance(out_val, pa.Table):
            result[out_table] = align_and_cast_table(out_val, target_schema) if target_schema is not None else out_val
        elif target_schema is not None:
            coerced_rows = [coerce_record_for_pyarrow(row, target_schema) for row in out_val]
            result[out_table] = pa.Table.from_pylist(coerced_rows, schema=target_schema)
        else:
            result[out_table] = out_val

    return result


def validate_table_transforms(
    settings: object,
    valid_tables: Iterable[str],
) -> None:
    """Ensure every key in ``settings.table_transforms`` matches a configured input table.

    :param settings: pipeline settings instance
    :type settings: object
    :param valid_tables: valid input table names for the pipeline
    :type valid_tables: Iterable[str]
    :return: ``None``
    :rtype: None
    """
    table_transforms: TransformRegistry | None = getattr(settings, "table_transforms", None)
    if table_transforms is None:
        return
    validate_registry_table_names(table_transforms.registry, valid_tables, "transforms_module")


def uses_pyarrow_format(settings: object) -> bool:
    """Return whether ``settings.loader_file_format`` is a PyArrow-backed loader format.

    :param settings: pipeline settings instance
    :type settings: object
    :return: ``True`` if ``loader_file_format`` uses PyArrow pages
    :rtype: bool
    """
    return getattr(settings, "loader_file_format", LoaderFileFormatEnum.PARQUET) in PYARROW_FILE_FORMATS


def needs_default_pyarrow_schema(
    table_name: str,
    *,
    use_pyarrow: bool = True,
    table_transforms: TransformRegistry | None = None,
    output_schemas: ArrowSchemaRegistry | None = None,
) -> bool:
    """Return whether ``table_name`` needs its PyArrow schema generated from the input schema.

    :param table_name: target table name
    :type table_name: str
    :param use_pyarrow: whether PyArrow output is enabled
    :type use_pyarrow: bool
    :param table_transforms: optional configured table transforms registry
    :type table_transforms: TransformRegistry | None
    :param output_schemas: optional user-supplied output schemas registry
    :type output_schemas: ArrowSchemaRegistry | None
    :return: ``True`` if a default PyArrow schema should be generated for ``table_name``
    :rtype: bool
    """
    if not use_pyarrow:
        return False
    if table_transforms is not None and table_name in table_transforms.registry:
        return False
    return not (output_schemas is not None and table_name in output_schemas.registry)


class TransformSettingsMixin(BaseSettings):
    """Mixin for pipelines that accept `FILE_PREPROCESSORS`, `TABLE_TRANSFORMS`, and `OUTPUT_SCHEMAS` modules."""

    DEFAULT_FILE_PREPROCESSORS: ClassVar[tuple[FilePreprocessFn, ...]] = ()

    _file_preprocessors: PreprocessorRegistry | None = PrivateAttr(default=None)
    _table_transforms: TransformRegistry | None = PrivateAttr(default=None)
    _unreconciled_output_schemas: ArrowSchemaRegistry | None = PrivateAttr(default=None)

    """User-supplied output table schemas in pyarrow format."""

    _schema_map: ArrowSchemaRegistry | None = PrivateAttr(default=None)

    """Output table schemas, derived from the user-supplied schemas and/or from the input schema.

    Tables without a user-supplied schema and with a transform specified will not have their
    schema generated from the input schema, since the transform may change the output fields.
    """

    preprocessors_module: PreprocessorsModuleSpec
    transforms_module: TransformsModuleSpec
    output_schemas_module: OutputSchemasModuleSpec

    @model_validator(mode="after")
    def _validate_transforms_module(self) -> Self:
        """Load and validate `FILE_PREPROCESSORS`, `TABLE_TRANSFORMS`, and `OUTPUT_SCHEMAS` if configured."""
        if self.preprocessors_module is not None:
            self._file_preprocessors = load_file_preprocessors(self.preprocessors_module)
        elif self.DEFAULT_FILE_PREPROCESSORS:
            self._file_preprocessors = PreprocessorRegistry(default=self.DEFAULT_FILE_PREPROCESSORS)
        if self.transforms_module is not None:
            self._table_transforms = load_registry_dict(self.transforms_module, "TABLE_TRANSFORMS")
        if self.output_schemas_module is not None and self.use_pyarrow:
            self._unreconciled_output_schemas = load_registry_dict(self.output_schemas_module, "OUTPUT_SCHEMAS")
        self._resolve_settings_schema_map()
        return self

    @property
    def use_pyarrow(self) -> bool:
        """Return whether this pipeline yields ``pyarrow.Table`` pages instead of row dicts."""
        return uses_pyarrow_format(self)

    def _validate_configured_tables(
        self,
        valid_tables: Iterable[str],
        *,
        validate_transforms_fn: Callable[[Any, Iterable[str]], None] = validate_table_transforms,
    ) -> None:
        """Validate both ``file_preprocessors`` and ``table_transforms`` against ``valid_tables``."""
        validate_file_preprocessors(self, valid_tables)
        validate_transforms_fn(self, valid_tables)

    def _needs_default_pyarrow_schema(self, table_name: str) -> bool:
        """Return whether ``table_name`` needs its PyArrow schema generated from the input schema."""
        if not self.use_pyarrow or self.get_table_transform(table_name) is not None:
            return False
        return needs_default_pyarrow_schema(
            table_name,
            use_pyarrow=self.use_pyarrow,
            output_schemas=self._unreconciled_output_schemas,
        )

    def _resolve_default_table_schema(
        self,
        table_name: str,
        default_pyarrow_schema: pa.Schema | None,
        build_default: Callable[[], pa.Schema],
    ) -> pa.Schema | None:
        """Return ``default_pyarrow_schema`` if provided, else call ``build_default`` when needed."""
        if default_pyarrow_schema is not None or not self._needs_default_pyarrow_schema(table_name):
            return default_pyarrow_schema
        return build_default()

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Resolve `_schema_map` from `_unreconciled_output_schemas`, `_table_transforms`, and any default PyArrow schema."""
        table_name = getattr(self, "table_name", None)
        if isinstance(table_name, str):
            self._validate_configured_tables({table_name})
            self._schema_map = (
                resolve_schema_map(
                    table_name,
                    transform=self.get_table_transform(table_name),
                    unreconciled_output_schemas=self._unreconciled_output_schemas,
                    default_pyarrow_schema=default_pyarrow_schema,
                )
                if self.use_pyarrow
                else None
            )
        else:
            self._schema_map = self._unreconciled_output_schemas if self.use_pyarrow else None

    @property
    def file_preprocessors(self) -> PreprocessorRegistry | None:
        """The loaded `FILE_PREPROCESSORS` registry (`None` if not configured)."""
        return self._file_preprocessors

    @property
    def table_transforms(self) -> TransformRegistry | None:
        """The loaded `TABLE_TRANSFORMS` registry (`None` if not configured)."""
        return self._table_transforms

    @property
    def schema_map(self) -> ArrowSchemaRegistry | None:
        """The resolved `ArrowSchemaRegistry` for this pipeline (`None` if unschematized)."""
        return self._schema_map

    def get_file_preprocessors(self, table_name: str | None = None) -> tuple[FilePreprocessFn, ...]:
        """Return the `FilePreprocessFn` sequence for `table_name`, if configured."""
        if self._file_preprocessors is None:
            return ()
        return self._file_preprocessors.get(table_name)

    def get_table_transforms(self, table_name: str) -> tuple[TableTransformFn, ...]:
        """Return the ordered tuple of transform callables configured for ``table_name``.

        :param table_name: input table name
        :type table_name: str
        :return: tuple of configured transform callables (empty if unconfigured)
        :rtype: tuple[TableTransformFn, ...]
        """
        if self._table_transforms is None:
            return ()
        return self._table_transforms.get_tuple(table_name)

    def get_table_transform(self, table_name: str) -> TableTransformFn | None:
        """Return the single or composed ``TableTransformFn`` for ``table_name``, if configured.

        :param table_name: input table name
        :type table_name: str
        :return: configured or composed transform callable, or ``None``
        :rtype: TableTransformFn | None
        """
        transforms = self.get_table_transforms(table_name)
        if not transforms:
            return None
        return compose_transforms(transforms)

    def get_output_table_hints(self) -> tuple[OutputTableHints, ...]:
        """Return per-table output hints (descriptions and Iceberg partitions) for this pipeline.

        :return: tuple of output table hints
        :rtype: tuple[OutputTableHints, ...]
        """
        return ()
