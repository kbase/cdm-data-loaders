"""Pluggable record filtering, transformation, multi-table splitting, and PyArrow schema overrides."""

import importlib
from collections.abc import Callable, Mapping, Sequence
from typing import Annotated, Any, Final

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, InstanceOf, StringConstraints

from cdm_data_loaders.converters.jsonschema_to_pyarrow import coerce_record_for_pyarrow
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME

DEFAULT_TRANSFORMS_ATTR: Final[str] = "TABLE_TRANSFORMS"
REJECTED_PYARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("table_name", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("line_no", pa.int64()),
        pa.field("raw_record", pa.string()),
        pa.field("error_detail", pa.string()),
    ]
)

type RecordTransformFn = Callable[[dict[str, Any]], Mapping[str, Sequence[dict[str, Any]]]]
type TableNameKey = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, strict=True)]


class TableTransform(BaseModel):
    """Per-table record transformation and optional PyArrow schema mapping.

    :param transform_fn: callable mapping a single input record dictionary to a mapping of
        ``{destination_table_name: [row_dict, ...]}``. Returning ``{}`` (or empty row sequences)
        filters out the record.
    :param table_to_schema_map: optional mapping of destination table names to ``pyarrow.Schema``
        instances used to coerce emitted rows and construct ``ArrowDictBuffer`` pages.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    transform_fn: RecordTransformFn | None = None
    table_to_schema_map: Annotated[
        dict[TableNameKey, InstanceOf[pa.Schema]] | None,
        Field(default=None, min_length=1),
    ] = None


def resolve_schema_map(
    default_table: str,
    *,
    transform: TableTransform | None = None,
    default_pyarrow_schema: pa.Schema | None = None,
) -> dict[str, pa.Schema] | None:
    """Resolve the active table-to-PyArrow-schema mapping for a reader.

    Automatically includes ``_rejected`` mapped to ``REJECTED_PYARROW_SCHEMA``
    whenever a PyArrow schema map is active.

    :param default_table: default target table name for the pipeline/entity
    :param transform: optional ``TableTransform`` configured for ``default_table``
    :param default_pyarrow_schema: default PyArrow schema when no override is configured
    :return: dictionary mapping destination table names to ``pa.Schema`` objects, or ``None``
    """
    if transform is not None and transform.table_to_schema_map is not None:
        return {REJECTED_TABLE_NAME: REJECTED_PYARROW_SCHEMA, **transform.table_to_schema_map}
    if default_pyarrow_schema is not None:
        return {
            default_table: default_pyarrow_schema,
            REJECTED_TABLE_NAME: REJECTED_PYARROW_SCHEMA,
        }
    return None


def _validate_routed_output(raw_routed: object) -> dict[str, list[dict[str, Any]]]:
    """Validate the return value of ``transform_fn`` and strip empty table lists."""
    if not isinstance(raw_routed, Mapping):
        err_msg = "transform_fn must return a mapping of table names to sequences of row dicts"
        raise TypeError(err_msg)

    routed: dict[str, list[dict[str, Any]]] = {}
    for table_name, rows in raw_routed.items():
        if not isinstance(table_name, str) or not table_name.strip():
            err_msg = "transform_fn output keys must be non-empty table names"
            raise ValueError(err_msg)
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray, Mapping)):
            err_msg = f"transform_fn rows for table {table_name!r} must be a sequence of dicts"
            raise TypeError(err_msg)
        row_list = list(rows)
        if not all(isinstance(row, dict) for row in row_list):
            err_msg = f"transform_fn rows for table {table_name!r} must be dicts"
            raise TypeError(err_msg)
        if row_list:
            routed[table_name] = row_list
    return routed


def route_and_coerce_record(
    record: dict[str, Any],
    default_table: str,
    *,
    transform: TableTransform | None = None,
    table_to_schema_map: Mapping[str, pa.Schema] | None = None,
    coerce_untransformed: bool = True,
) -> dict[str, list[dict[str, Any]]]:
    """Route a validated record through ``transform`` and optionally coerce rows for PyArrow.

    :param record: parsed and validated input record dictionary
    :param default_table: destination table name when ``transform`` or ``transform.transform_fn`` is ``None``
    :param transform: optional ``TableTransform`` for ``default_table``
    :param table_to_schema_map: active destination table-to-PyArrow schema mapping, if any
    :param coerce_untransformed: whether to run ``coerce_record_for_pyarrow`` when no transform modified the record
    :return: dictionary mapping destination table names to non-empty lists of row dictionaries
    """
    if transform is not None and transform.transform_fn is not None:
        routed = _validate_routed_output(transform.transform_fn(record))
    else:
        routed = {default_table: [record]}

    if not routed:
        return {}

    has_transform_override = transform is not None and (
        transform.transform_fn is not None or transform.table_to_schema_map is not None
    )
    if table_to_schema_map is not None and (coerce_untransformed or has_transform_override):
        coerced: dict[str, list[dict[str, Any]]] = {}
        for out_table, rows in routed.items():
            if out_table not in table_to_schema_map:
                err_msg = f"Cannot coerce data for {out_table!r} table: no PyArrow schema supplied"
                raise ValueError(err_msg)
            target_schema = table_to_schema_map[out_table]
            coerced[out_table] = [coerce_record_for_pyarrow(row, target_schema) for row in rows]
        return coerced

    return routed


def load_table_transforms(
    spec: str,
    default_attr: str = DEFAULT_TRANSFORMS_ATTR,
) -> dict[str, TableTransform]:
    """Import a module spec (``module`` or ``module:ATTR``) and load its ``TableTransform`` mapping.

    :param spec: dotted module path, optionally suffixed with ``:ATTRIBUTE_NAME``
    :param default_attr: attribute name to load when ``spec`` does not include ``:ATTRIBUTE_NAME``
    :return: dictionary mapping input table names to ``TableTransform`` instances
    :raises ValueError: if ``spec`` is malformed or the target attribute is missing/empty
    :raises TypeError: if the target attribute is not a mapping of ``str`` to ``TableTransform``
    """
    module_path, sep, attr_name = spec.partition(":")
    module_path = module_path.strip()
    resolved_attr = attr_name.strip() if sep else default_attr
    if not module_path or not resolved_attr:
        err_msg = f"Invalid transforms module specification: {spec!r}"
        raise ValueError(err_msg)

    module = importlib.import_module(module_path)
    raw = getattr(module, resolved_attr, None)
    if not isinstance(raw, Mapping) or not raw:
        err_msg = f"{module_path} does not define a non-empty {resolved_attr}: dict[str, TableTransform]"
        raise ValueError(err_msg)

    transforms: dict[str, TableTransform] = {}
    for table_name, transform in raw.items():
        if not isinstance(table_name, str) or not table_name.strip():
            err_msg = f"{resolved_attr} keys in {module_path} must be non-empty table names"
            raise ValueError(err_msg)
        if not isinstance(transform, TableTransform):
            err_msg = f"{resolved_attr}[{table_name!r}] in {module_path} must be a TableTransform instance"
            raise TypeError(err_msg)
        transforms[table_name] = transform
    return transforms
