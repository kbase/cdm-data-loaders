"""Pure PyArrow schema assembly and byte-metadata decoding helpers."""

from collections.abc import Mapping, Sequence

import pyarrow as pa


def assemble_pyarrow_schema(
    component: pa.StructType | Sequence[pa.Field],
    *,
    metadata: Mapping[str, str | bytes] | None = None,
) -> pa.Schema:
    """Assemble a PyArrow table schema from a ``pa.StructType`` component or sequence of ``pa.Field`` instances.

    :param component: struct component or sequence of PyArrow fields
    :type component: pa.StructType | Sequence[pa.Field]
    :param metadata: optional schema-level metadata
    :type metadata: Mapping[str, str | bytes] | None
    :return: assembled PyArrow schema
    :rtype: pa.Schema
    :raises ValueError: if ``component`` contains no fields
    """
    fields = list(component)
    if not fields:
        msg = "Cannot assemble a PyArrow schema from an empty component"
        raise ValueError(msg)
    return pa.schema(fields, metadata=metadata)


def decode_field_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Decode PyArrow UTF-8 byte key/value metadata into string key/value pairs.

    :param metadata: optional raw PyArrow field metadata mapping
    :type metadata: Mapping[bytes, bytes] | None
    :return: decoded string-to-string metadata dictionary
    :rtype: dict[str, str]
    """
    if not metadata:
        return {}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in metadata.items()}
