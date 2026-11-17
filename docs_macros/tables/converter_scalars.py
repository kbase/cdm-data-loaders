"""Live converter scalar mapping builders for converter specification reference tables."""

from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import IntEnum, StrEnum
from ipaddress import IPv4Address
from pathlib import Path
from typing import Annotated, Any, Final, Literal
from uuid import UUID

import pyarrow as pa
from frozendict import frozendict
from pydantic import Base64Bytes, Field, NaiveDatetime, PositiveFloat, PositiveInt

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import resolve_scalar_type
from cdm_data_loaders.converters.facades.pyarrow_to_jsonschema import (
    resolve_scalar_type as resolve_pyarrow_jsonschema_scalar_type,
)
from cdm_data_loaders.converters.facades.pyarrow_to_linkml import (
    resolve_scalar_type as resolve_pyarrow_linkml_scalar_type,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import (
    resolve_scalar_type as resolve_pydantic_scalar_type,
)
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import SCALAR_ARROW_TYPES
from cdm_data_loaders.converters.sources.json_schema.strict_scalars import (
    NON_EXTENDABLE_SCALAR_HINTS,
    STRING_CONTENT_ENCODING_SPECS,
    STRING_FORMAT_SPECS,
)
from cdm_data_loaders.validation.xsd.scalars import BUILTIN_LIST_ITEM_TYPES
from docs_macros.tables.models import (
    JsonSchemaScalarRow,
    PyArrowJsonSchemaRow,
    PyArrowLinkmlRow,
    PyArrowScalarProbeSpec,
    PydanticProbeSpec,
    PydanticScalarRow,
    XsdScalarRow,
)


class SampleStrEnum(StrEnum):
    """Sample string enumeration used for Pydantic scalar table introspection."""

    ALPHA = "alpha"
    BETA = "beta"


class SampleIntEnum(IntEnum):
    """Sample integer enumeration used for Pydantic scalar table introspection."""

    ONE = 1
    TWO = 2


JSONSCHEMA_SCALAR_PROBES: Final[tuple[Mapping[str, Any], ...]] = (
    *(frozendict({"type": type_name}) for type_name in (*NON_EXTENDABLE_SCALAR_HINTS, "string")),
    *(frozendict({"type": "string", "format": fmt}) for fmt in STRING_FORMAT_SPECS),
    *(frozendict({"type": "string", "contentEncoding": enc}) for enc in STRING_CONTENT_ENCODING_SPECS),
    frozendict({"enum": [1, 2]}),
    frozendict({"enum": [1, 2.5]}),
    frozendict({"enum": [1, "two", True]}),
    frozendict({"type": ["integer", "number"]}),
    frozendict({"type": ["integer", "string"]}),
    frozendict({"type": ["string", "null"]}),
)

PYDANTIC_SCALAR_PROBES: Final[tuple[PydanticProbeSpec, ...]] = (
    PydanticProbeSpec(type_tokens=("bool", "StrictBool"), qualifier="", annotation=bool),
    PydanticProbeSpec(
        type_tokens=("int", "StrictInt", "PositiveInt", "NegativeInt", "NonNegativeInt", "NonPositiveInt"),
        qualifier="",
        annotation=PositiveInt,
    ),
    PydanticProbeSpec(
        type_tokens=("float", "StrictFloat", "PositiveFloat", "NegativeFloat", "NonNegativeFloat", "NonPositiveFloat"),
        qualifier="",
        annotation=PositiveFloat,
    ),
    PydanticProbeSpec(
        type_tokens=("str", "StrictStr", "SecretStr", "EmailStr", "NameEmail", "AnyUrl", "HttpUrl", "Base64Str"),
        qualifier="",
        annotation=str,
    ),
    PydanticProbeSpec(type_tokens=("Decimal",), qualifier="(unconstrained)", annotation=Decimal),
    PydanticProbeSpec(
        type_tokens=("Annotated[Decimal, Field(max_digits=10, decimal_places=2)]",),
        qualifier="",
        annotation=Annotated[Decimal, Field(max_digits=10, decimal_places=2)],
    ),
    PydanticProbeSpec(type_tokens=("datetime.date", "PastDate", "FutureDate"), qualifier="", annotation=date),
    PydanticProbeSpec(
        type_tokens=("datetime.datetime", "AwareDatetime", "PastDatetime", "FutureDatetime"),
        qualifier="",
        annotation=datetime,
    ),
    PydanticProbeSpec(type_tokens=("NaiveDatetime",), qualifier="", annotation=NaiveDatetime),
    PydanticProbeSpec(type_tokens=("datetime.time",), qualifier="", annotation=time),
    PydanticProbeSpec(type_tokens=("datetime.timedelta",), qualifier="", annotation=timedelta),
    PydanticProbeSpec(type_tokens=("UUID",), qualifier="", annotation=UUID, range_token=("UUID1", "UUID5")),
    PydanticProbeSpec(
        type_tokens=("Path", "PurePath", "FilePath", "DirectoryPath", "NewPath"),
        qualifier="",
        annotation=Path,
    ),
    PydanticProbeSpec(
        type_tokens=(
            "IPv4Address",
            "IPv6Address",
            "IPv4Network",
            "IPv6Network",
            "IPvAnyAddress",
            "IPvAnyInterface",
            "IPvAnyNetwork",
        ),
        qualifier="",
        annotation=IPv4Address,
    ),
    PydanticProbeSpec(type_tokens=("bytes", "bytearray", "SecretBytes"), qualifier="", annotation=bytes),
    PydanticProbeSpec(type_tokens=("Base64Bytes",), qualifier="", annotation=Base64Bytes),
    PydanticProbeSpec(type_tokens=("StrEnum", "Enum"), qualifier="(str values)", annotation=SampleStrEnum),
    PydanticProbeSpec(type_tokens=("IntEnum", "Enum"), qualifier="(int values)", annotation=SampleIntEnum),
    PydanticProbeSpec(type_tokens=("Literal[1, 2]",), qualifier="", annotation=Literal[1, 2]),
    PydanticProbeSpec(type_tokens=("Literal[1, 2.5]",), qualifier="", annotation=Literal[1, 2.5]),
    PydanticProbeSpec(type_tokens=('Literal[1, "two", True]',), qualifier="", annotation=Literal[1, "two", True]),
    PydanticProbeSpec(type_tokens=("int | float",), qualifier="", annotation=int | float),
    PydanticProbeSpec(type_tokens=("int | str",), qualifier="", annotation=int | str),
    PydanticProbeSpec(type_tokens=("str | None",), qualifier="", annotation=str | None),
)

PYARROW_SCALAR_PROBES: Final[tuple[PyArrowScalarProbeSpec, ...]] = (
    PyArrowScalarProbeSpec(dtype=pa.bool_(), json_value_type="bool"),
    PyArrowScalarProbeSpec(dtype=pa.int8(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.int16(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.int32(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.int64(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.uint8(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.uint16(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.uint32(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.uint64(), json_value_type="int"),
    PyArrowScalarProbeSpec(dtype=pa.float16(), json_value_type="float"),
    PyArrowScalarProbeSpec(dtype=pa.float32(), json_value_type="float"),
    PyArrowScalarProbeSpec(dtype=pa.float64(), json_value_type="float"),
    PyArrowScalarProbeSpec(dtype=pa.decimal128(10, 2), json_value_type="str", json_value_note="fixed-point decimal"),
    PyArrowScalarProbeSpec(dtype=pa.decimal128(2, 2), json_value_type="str", json_value_note="fixed-point decimal"),
    PyArrowScalarProbeSpec(
        dtype=pa.decimal128(10, 0),
        json_value_type="str",
        json_value_note="fixed-point integer decimal",
    ),
    PyArrowScalarProbeSpec(dtype=pa.decimal256(50, 10), json_value_type="str", json_value_note="fixed-point decimal"),
    PyArrowScalarProbeSpec(dtype=pa.string(), json_value_type="str"),
    PyArrowScalarProbeSpec(dtype=pa.large_string(), json_value_type="str"),
    PyArrowScalarProbeSpec(dtype=pa.string_view(), json_value_type="str"),
    PyArrowScalarProbeSpec(dtype=pa.binary(), json_value_type="str", json_value_note="Base64"),
    PyArrowScalarProbeSpec(dtype=pa.large_binary(), json_value_type="str", json_value_note="Base64"),
    PyArrowScalarProbeSpec(dtype=pa.binary_view(), json_value_type="str", json_value_note="Base64"),
    PyArrowScalarProbeSpec(dtype=pa.binary(16), json_value_type="str", json_value_note="Base64"),
    PyArrowScalarProbeSpec(dtype=pa.date32(), json_value_type="str", json_value_note="ISO-8601 date"),
    PyArrowScalarProbeSpec(dtype=pa.date64(), json_value_type="str", json_value_note="ISO-8601 date"),
    PyArrowScalarProbeSpec(dtype=pa.time32("s"), json_value_type="str", json_value_note="ISO-8601 time"),
    PyArrowScalarProbeSpec(dtype=pa.time32("ms"), json_value_type="str", json_value_note="ISO-8601 time"),
    PyArrowScalarProbeSpec(dtype=pa.time64("us"), json_value_type="str", json_value_note="ISO-8601 time"),
    PyArrowScalarProbeSpec(dtype=pa.time64("ns"), json_value_type="str", json_value_note="ISO-8601 time"),
    PyArrowScalarProbeSpec(
        dtype=pa.timestamp("s", tz="UTC"), json_value_type="str", json_value_note="ISO-8601 date-time"
    ),
    PyArrowScalarProbeSpec(
        dtype=pa.timestamp("us", tz="UTC"),
        json_value_type="str",
        json_value_note="ISO-8601 date-time",
    ),
    PyArrowScalarProbeSpec(
        dtype=pa.timestamp("ns", tz="America/Los_Angeles"),
        json_value_type="str",
        json_value_note="ISO-8601 date-time",
    ),
    PyArrowScalarProbeSpec(dtype=pa.timestamp("us"), json_value_type="str", json_value_note="naive ISO-8601 timestamp"),
    PyArrowScalarProbeSpec(dtype=pa.timestamp("ns"), json_value_type="str", json_value_note="naive ISO-8601 timestamp"),
    PyArrowScalarProbeSpec(dtype=pa.dictionary(pa.int16(), pa.string()), json_value_type="str"),
    PyArrowScalarProbeSpec(dtype=pa.dictionary(pa.int8(), pa.int64(), ordered=True), json_value_type="int"),
)


def build_xsd_scalar_rows() -> tuple[XsdScalarRow, ...]:
    """Extract XSD built-in scalar and list type rows from ``SCALAR_ARROW_TYPES`` and ``BUILTIN_LIST_ITEM_TYPES``.

    :return: Tuple of ``XsdScalarRow`` records.
    :rtype: tuple[XsdScalarRow, ...]
    """
    grouped: dict[str, list[str]] = {}
    dtypes: dict[str, pa.DataType] = {}
    for xsd_name, dtype in SCALAR_ARROW_TYPES.items():
        key = str(dtype)
        grouped.setdefault(key, []).append(xsd_name)
        dtypes[key] = dtype

    rows: list[XsdScalarRow] = [
        XsdScalarRow.model_validate(
            {
                "xsd_names": tuple(names),
                "list_items": (),
                "python_type_label": dtypes[key],
                "arrow_type_expr": dtypes[key],
            }
        )
        for key, names in grouped.items()
    ]

    list_grouped: dict[str, list[tuple[str, str]]] = {}
    list_item_dtypes: dict[str, pa.DataType] = {}
    for xsd_list_name, item_xsd_name in BUILTIN_LIST_ITEM_TYPES.items():
        item_dtype = SCALAR_ARROW_TYPES[item_xsd_name]
        key = str(item_dtype)
        list_grouped.setdefault(key, []).append((xsd_list_name, item_xsd_name))
        list_item_dtypes[key] = item_dtype

    for key, pairs in list_grouped.items():
        item_dtype = list_item_dtypes[key]
        list_dtype = pa.list_(item_dtype)
        rows.append(
            XsdScalarRow.model_validate(
                {
                    "xsd_names": (),
                    "list_items": tuple(pairs),
                    "python_type_label": list_dtype,
                    "arrow_type_expr": list_dtype,
                }
            )
        )
    return tuple(rows)


def build_jsonschema_scalar_rows() -> tuple[JsonSchemaScalarRow, ...]:
    """Extract JSON Schema scalar probe rows by calling ``resolve_scalar_type``.

    :return: Tuple of ``JsonSchemaScalarRow`` records.
    :rtype: tuple[JsonSchemaScalarRow, ...]
    """
    rows: list[JsonSchemaScalarRow] = []
    for probe in JSONSCHEMA_SCALAR_PROBES:
        dtype, label = resolve_scalar_type(probe)
        rows.append(
            JsonSchemaScalarRow.model_validate(
                {
                    "json_schema": probe,
                    "python_type_label": dtype,
                    "arrow_type_expr": dtype,
                    "metadata_type_label": label,
                }
            )
        )
    return tuple(rows)


def build_pydantic_scalar_rows() -> tuple[PydanticScalarRow, ...]:
    """Extract Pydantic scalar probe rows by calling ``resolve_pydantic_scalar_type``.

    :return: Tuple of ``PydanticScalarRow`` records.
    :rtype: tuple[PydanticScalarRow, ...]
    """
    rows: list[PydanticScalarRow] = []
    for probe in PYDANTIC_SCALAR_PROBES:
        dtype, label = resolve_pydantic_scalar_type(probe.annotation)
        rows.append(
            PydanticScalarRow.model_validate(
                {
                    "type_tokens": probe.type_tokens,
                    "range_token": probe.range_token,
                    "qualifier": probe.qualifier,
                    "python_type_label": dtype,
                    "arrow_type_expr": dtype,
                    "metadata_type_label": label,
                }
            )
        )
    return tuple(rows)


def build_pyarrow_to_jsonschema_rows() -> tuple[PyArrowJsonSchemaRow, ...]:
    """Extract PyArrow-to-JSON-Schema scalar probe rows from ``resolve_pyarrow_jsonschema_scalar_type``.

    :return: Tuple of ``PyArrowJsonSchemaRow`` records.
    :rtype: tuple[PyArrowJsonSchemaRow, ...]
    """
    rows: list[PyArrowJsonSchemaRow] = []
    for probe in PYARROW_SCALAR_PROBES:
        full_schema = resolve_pyarrow_jsonschema_scalar_type(probe.dtype)
        x_pa = full_schema.get("x-pyarrow", {})
        core = {k: v for k, v in full_schema.items() if k != "x-pyarrow"}
        rows.append(
            PyArrowJsonSchemaRow.model_validate(
                {
                    "arrow_type_expr": probe.dtype,
                    "json_value_type": probe.json_value_type,
                    "json_value_note": probe.json_value_note,
                    "core_schema_json": core,
                    "x_pyarrow_json": x_pa,
                }
            )
        )
    return tuple(rows)


def build_pyarrow_to_linkml_rows() -> tuple[PyArrowLinkmlRow, ...]:
    """Extract PyArrow-to-LinkML scalar probe rows from ``resolve_pyarrow_linkml_scalar_type``.

    :return: Tuple of ``PyArrowLinkmlRow`` records.
    :rtype: tuple[PyArrowLinkmlRow, ...]
    """
    rows: list[PyArrowLinkmlRow] = []
    for probe in PYARROW_SCALAR_PROBES:
        slot = resolve_pyarrow_linkml_scalar_type(probe.dtype)
        rows.append(
            PyArrowLinkmlRow.model_validate(
                {
                    "arrow_type_expr": probe.dtype,
                    "json_value_type": probe.json_value_type,
                    "json_value_note": probe.json_value_note,
                    "linkml_range": slot.range,
                    "min_value": slot.minimum_value,
                    "max_value": slot.maximum_value,
                }
            )
        )
    return tuple(rows)
