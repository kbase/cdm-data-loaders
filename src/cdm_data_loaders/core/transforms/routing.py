"""Record and PyArrow batch routing, composition, validation, and schema alignment."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

import pyarrow as pa
from pydantic import BaseModel

from cdm_data_loaders.coercion.json_to_arrow import coerce_json_record_for_pyarrow
from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    TableTransformFn,
    TransformContext,
)
from cdm_data_loaders.core.transforms.introspection import (
    accepts_pyarrow_table,
    inspect_transform_signature,
)

RAW_TABLE_PREFIX: Final[str] = "raw_"

type RecordCoercerFn = Callable[[Any, pa.Schema], dict[str, Any]]


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
    """Emit ``data`` unchanged to a destination table named ``f"{prefix}{ctx.table_name}"``.

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


def merge_routed_outputs(
    target: dict[str, list[dict[str, Any]] | pa.Table],
    addition: Mapping[str, list[dict[str, Any]] | pa.Table],
) -> None:
    """Merge validated routed table outputs from ``addition`` into ``target`` in place.

    :param target: accumulator mapping destination table names to row lists or ``pyarrow.Table`` batches
    :type target: dict[str, list[dict[str, Any]] | pa.Table]
    :param addition: validated routed output mapping to append
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


def invoke_transform(
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
    accepts_arrow, accepts_dict, takes_ctx = inspect_transform_signature(transform)

    if isinstance(data, pa.Table):
        if accepts_arrow:
            return validate_routed_output(_call_transform(transform, data, ctx, takes_ctx=takes_ctx))
        merged: dict[str, list[dict[str, Any]] | pa.Table] = {}
        for row in data.to_pylist():
            merge_routed_outputs(
                merged,
                validate_routed_output(_call_transform(transform, row, ctx, takes_ctx=takes_ctx)),
            )
        return merged

    if accepts_dict:
        return validate_routed_output(_call_transform(transform, data, ctx, takes_ctx=takes_ctx))

    coerced = coerce_json_record_for_pyarrow(data, input_schema) if input_schema is not None else data
    one_row_table = pa.Table.from_pylist([coerced], schema=input_schema)
    return validate_routed_output(_call_transform(transform, one_row_table, ctx, takes_ctx=takes_ctx))


@dataclass(frozen=True, slots=True)
class ComposedTransform:
    """Callable composition of two or more ``TableTransformFn`` steps.

    :param steps: ordered tuple of transform callables
    :type steps: tuple[TableTransformFn, ...]
    :param accepts_pyarrow: whether every step accepts ``pyarrow.Table`` inputs
    :type accepts_pyarrow: bool
    """

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
            merge_routed_outputs(merged, invoke_transform(step, data, ctx))
        return merged


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
    return ComposedTransform(
        steps=step_tuple,
        accepts_pyarrow=all(accepts_pyarrow_table(step) for step in step_tuple),
    )


def resolve_schema_map(
    default_table: str,
    *,
    transform: TableTransformFn | None = None,
    unreconciled_output_schemas: ArrowSchemaRegistry | None = None,
    default_pyarrow_schema: pa.Schema | None = None,
) -> ArrowSchemaRegistry | None:
    """Resolve the active table-to-PyArrow-schema registry for a reader.

    :param default_table: default target table name for the pipeline or entity
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
            err_msg = (
                f"unreconciled_output_schemas for {default_table!r} "
                f"must include {default_table!r} when transform is None"
            )
            raise ValueError(err_msg)
        return unreconciled_output_schemas
    if default_pyarrow_schema is not None and transform is None:
        return ArrowSchemaRegistry(
            registry={
                default_table: default_pyarrow_schema,
            }
        )
    return None


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


def make_record_router(
    default_table: str | TransformContext,
    *,
    transform: TableTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
    record_coercer: RecordCoercerFn = coerce_json_record_for_pyarrow,
) -> Callable[[BaseModel | dict[str, Any], str | None], dict[str, list[dict[str, Any]]]]:
    """Build a reusable per-record router and PyArrow coercer for ``default_table``.

    :param default_table: destination table name or :class:`TransformContext` when ``transform`` is ``None``
    :type default_table: str | TransformContext
    :param transform: optional ``TableTransformFn`` for ``default_table``
    :type transform: TableTransformFn | None
    :param schema_map: active destination ``ArrowSchemaRegistry``, if any
    :type schema_map: ArrowSchemaRegistry | None
    :param record_coercer: callable coercing a record or model against a target ``pa.Schema``
    :type record_coercer: RecordCoercerFn
    :return: callable accepting ``(record, source_file)`` and returning routed per-table row lists
    :rtype: Callable[[BaseModel | dict[str, Any], str | None], dict[str, list[dict[str, Any]]]]
    """

    def _route_record(
        record: BaseModel | dict[str, Any],
        source_file: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        ctx = _resolve_transform_context(default_table, source_file)
        if transform is not None:
            if isinstance(record, BaseModel):
                err_msg = "TableTransformFn requires a dict[str, Any] input record, got BaseModel"
                raise TypeError(err_msg)
            routed: dict[str, list[dict[str, Any]] | pa.Table] = invoke_transform(transform, record, ctx)
        else:
            routed = {ctx.table_name: [record]}  # type: ignore[list-item]

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
                result[out_table] = [record_coercer(row, target_schema) for row in out_val]
            else:
                result[out_table] = out_val
        return result

    return _route_record


def route_and_coerce_record(
    record: BaseModel | dict[str, Any],
    default_table: str | TransformContext,
    *,
    transform: TableTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
    source_file: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Route a validated record through ``transform`` and coerce rows for PyArrow when schematized.

    :param record: parsed and validated input record dictionary or ``BaseModel`` instance
    :type record: BaseModel | dict[str, Any]
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
    return make_record_router(
        default_table,
        transform=transform,
        schema_map=schema_map,
    )(record, source_file)


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
    :param use_pyarrow: whether mapped destination tables are emitted as ``pyarrow.Table`` objects
    :type use_pyarrow: bool
    :return: dictionary mapping destination table names to non-empty ``pyarrow.Table`` or ``list[dict[str, Any]]`` pages
    :rtype: dict[str, pa.Table | list[dict[str, Any]]]
    """
    ctx = _resolve_transform_context(default_table)
    if table.num_rows == 0 and transform is None:
        return {}

    routed: dict[str, list[dict[str, Any]] | pa.Table] = (
        invoke_transform(transform, table, ctx) if transform is not None else {ctx.table_name: table}
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
            coerced_rows = [coerce_json_record_for_pyarrow(row, target_schema) for row in out_val]
            result[out_table] = pa.Table.from_pylist(coerced_rows, schema=target_schema)
        else:
            result[out_table] = out_val

    return result
