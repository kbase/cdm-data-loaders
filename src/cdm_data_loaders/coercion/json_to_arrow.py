"""Coerce JSON-decoded record dictionaries to match a target PyArrow schema."""

from typing import Any

import pyarrow as pa

from cdm_data_loaders.coercion._arrow_core import JSON_COERCION_POLICY, coerce_value_for_pyarrow


def coerce_json_record_for_pyarrow(
    record: dict[str, Any],
    schema: pa.Schema,
) -> dict[str, Any]:
    """Coerce a JSON-validated record dictionary to match a PyArrow schema.

    Retains and coerces fields declared in ``schema`` so ``pyarrow.Table.from_pylist``
    can construct a table matching ``schema``.

    :param record: JSON-parsed and schema-validated record dictionary.
    :type record: dict[str, Any]
    :param schema: Target PyArrow schema for the record's table.
    :type schema: pa.Schema
    :return: Coerced record dictionary ready for ``DictBuffer`` or ``pa.Table.from_pylist``.
    :rtype: dict[str, Any]
    """
    return {
        field.name: coerce_value_for_pyarrow(
            record[field.name],
            field.type,
            field.metadata,
            policy=JSON_COERCION_POLICY,
        )
        for field in schema
        if field.name in record
    }
