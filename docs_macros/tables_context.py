"""Structured row models and live code-introspection builders for reference tables."""

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import IntEnum, StrEnum
from ipaddress import IPv4Address
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Final, Literal
from uuid import UUID

import pyarrow as pa
from frozendict import frozendict
from jsonschema.validators import validator_for
from pydantic import (
    Base64Bytes,
    BeforeValidator,
    ConfigDict,
    Field,
    NaiveDatetime,
    PositiveFloat,
    PositiveInt,
)

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    convert_xsv_schema_to_pyarrow,
    resolve_scalar_type,
)
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
from cdm_data_loaders.converters.readers.json_schema_strict import (
    NON_EXTENDABLE_SCALAR_HINTS,
    STRING_CONTENT_ENCODING_SPECS,
    STRING_FORMAT_SPECS,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    PubMedXmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    XmlToPyArrowSettings,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_reader import (
    PARSING_CONFIG_KEYS,
    resolve_duckdb_column_config,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.helpers import CleanerValidatorArgs
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.validation.jsonschema.dereference import dereference_schema
from cdm_data_loaders.validation.jsonschema.metaschema import (
    CUSTOM_META_SCHEMA_URI,
    get_xsv_config_schema,
    register_xsv_validator,
)
from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema
from cdm_data_loaders.validation.xsd.scalars import BUILTIN_LIST_ITEM_TYPES
from docs_macros.constants import PIPELINE_SPECS_ROOT
from docs_macros.dlt_nested_output_context import (
    build_dlt_nested_placement_rows,
    build_dlt_nested_single_table_column_rows,
    build_dlt_unnested_table_rows,
)
from docs_macros.dlt_pyarrow_context import (
    build_dlt_pyarrow_flat_column_rows,
    build_dlt_pyarrow_identifier_rows,
    build_dlt_pyarrow_nested_dlt_column_rows,
    build_dlt_pyarrow_nested_path_rows,
)
from docs_macros.formatting import (
    FrozenDocModel,
    JsonText,
    PyArrowPythonTypeLabel,
    PyArrowTypeExpr,
    coerce_json_text,
    escape_table_cell,
)
from tests.converter_spec import load_cases


def coerce_optional_json_text(value: object) -> str | None:
    """Coerce ``None`` to ``None`` or a JSON-serializable value into a JSON string.

    :param value: ``None``, JSON string, ``Mapping``, or sequence.
    :type value: object
    :return: JSON-encoded string or ``None``.
    :rtype: str | None
    """
    if value is None:
        return None
    return coerce_json_text(value)


def coerce_callable_names(value: object) -> tuple[str, ...]:
    """Coerce a sequence of callables or strings into a tuple of function/attribute names.

    :param value: Sequence of callables or string names.
    :type value: object
    :return: Tuple of name strings.
    :rtype: tuple[str, ...]
    """
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        msg = f"Expected sequence of callables or strings, got {type(value).__name__}"
        raise TypeError(msg)
    return tuple(item if isinstance(item, str) else getattr(item, "__name__", str(item)) for item in value)


def coerce_str_label(value: object) -> str:
    """Coerce an object (such as a LinkML range) into its string representation.

    :param value: Value to convert via ``str()``.
    :type value: object
    :return: String representation.
    :rtype: str
    """
    return value if isinstance(value, str) else str(value)


type OptionalJsonText = Annotated[str | None, BeforeValidator(coerce_optional_json_text)]
type CallableNameTuple = Annotated[tuple[str, ...], BeforeValidator(coerce_callable_names)]
type StrLabel = Annotated[str, BeforeValidator(coerce_str_label)]


class SampleStrEnum(StrEnum):
    """Sample string enumeration used for Pydantic scalar table introspection."""

    ALPHA = "alpha"
    BETA = "beta"


class SampleIntEnum(IntEnum):
    """Sample integer enumeration used for Pydantic scalar table introspection."""

    ONE = 1
    TWO = 2


class PydanticProbeSpec(FrozenDocModel):
    """Display tokens and probe annotation for a row in ``tables/pydantic_scalar_table.md.j2``."""

    type_tokens: tuple[str, ...]
    qualifier: str
    annotation: object
    range_token: tuple[str, str] | None = None


class PyArrowScalarProbeSpec(FrozenDocModel):
    """PyArrow scalar ``DataType`` and serialized JSON value metadata for scalar tables."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    dtype: pa.DataType
    json_value_type: str
    json_value_note: str = ""


def _code_cell(text: str, *, escape: bool = False) -> str:
    """Wrap ``text`` in Markdown inline backticks, optionally escaping pipe characters.

    :param text: Raw cell text.
    :type text: str
    :param escape: Whether to escape Markdown table pipes via ``escape_table_cell``.
    :type escape: bool
    :return: Backtick-wrapped cell string.
    :rtype: str
    """
    rendered = escape_table_cell(text) if escape else text
    return f"`{rendered}`"


def _code_csv(items: Sequence[str], *, escape: bool = False) -> str:
    """Format a sequence of strings as comma-separated inline code spans.

    :param items: Sequence of string tokens.
    :type items: Sequence[str]
    :param escape: Whether to escape Markdown table pipes in each item.
    :type escape: bool
    :return: Comma-separated inline code string.
    :rtype: str
    """
    return ", ".join(_code_cell(item, escape=escape) for item in items)


class XsdScalarRow(FrozenDocModel):
    """Row model for the XSD scalar mapping reference table."""

    xsd_names: tuple[str, ...]
    list_items: tuple[tuple[str, str], ...]
    python_type_label: PyArrowPythonTypeLabel
    arrow_type_expr: PyArrowTypeExpr

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        if self.xsd_names:
            first_cell = _code_csv(self.xsd_names)
        else:
            first_cell = ", ".join(f"`{list_name}` (items `{item_name}`)" for list_name, item_name in self.list_items)
        return (first_cell, self.python_type_label, _code_cell(self.arrow_type_expr))


class JsonSchemaScalarRow(FrozenDocModel):
    """Row model for the JSON Schema scalar mapping reference table."""

    json_schema: JsonText
    python_type_label: PyArrowPythonTypeLabel
    arrow_type_expr: PyArrowTypeExpr
    metadata_type_label: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            _code_cell(self.json_schema, escape=True),
            self.python_type_label,
            _code_cell(self.arrow_type_expr),
            _code_cell(self.metadata_type_label, escape=True),
        )


class PydanticScalarRow(FrozenDocModel):
    """Row model for the Pydantic scalar mapping reference table."""

    type_tokens: tuple[str, ...]
    range_token: tuple[str, str] | None
    qualifier: str
    python_type_label: PyArrowPythonTypeLabel
    arrow_type_expr: PyArrowTypeExpr
    metadata_type_label: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        if self.range_token:
            first_cell = f"`{self.type_tokens[0]}`, `{self.range_token[0]}`..`{self.range_token[1]}`"
        else:
            first_cell = _code_csv(self.type_tokens, escape=True)
            if self.qualifier:
                first_cell = f"{first_cell} {self.qualifier}"
        return (
            first_cell,
            self.python_type_label,
            _code_cell(self.arrow_type_expr),
            _code_cell(self.metadata_type_label, escape=True),
        )


class PyArrowJsonSchemaRow(FrozenDocModel):
    """Row model for the PyArrow-to-JSON-Schema scalar mapping reference table."""

    arrow_type_expr: PyArrowTypeExpr
    json_value_type: str
    json_value_note: str
    core_schema_json: JsonText
    x_pyarrow_json: JsonText

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        json_val = _code_cell(self.json_value_type)
        if self.json_value_note:
            json_val = f"{json_val} ({self.json_value_note})"
        return (
            _code_cell(self.arrow_type_expr),
            json_val,
            _code_cell(self.core_schema_json, escape=True),
            _code_cell(self.x_pyarrow_json, escape=True),
        )


class PyArrowLinkmlRow(FrozenDocModel):
    """Row model for the PyArrow-to-LinkML scalar mapping reference table."""

    arrow_type_expr: PyArrowTypeExpr
    json_value_type: str
    json_value_note: str
    linkml_range: StrLabel
    min_value: int | float | None
    max_value: int | float | None

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        json_val = _code_cell(self.json_value_type)
        if self.json_value_note:
            json_val = f"{json_val} ({self.json_value_note})"
        bounds = (
            f"`{self.min_value}` .. `{self.max_value}`"
            if self.min_value is not None or self.max_value is not None
            else "—"
        )
        return (
            _code_cell(self.arrow_type_expr),
            json_val,
            _code_cell(self.linkml_range),
            bounds,
        )


class XsvConfigRow(FrozenDocModel):
    """Row model for the ``x-xsv-config`` reference table."""

    key: str
    constraint_json: JsonText
    target_field: str | None
    is_has_header: bool
    metadata_only: bool
    default_repr: str | None

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        if self.is_has_header:
            target_cell = "`missing_header` (inverted: `not x-has-header`)"
            default_cell = "`False` (`x-has-header=True`)"
        elif self.metadata_only:
            target_cell = "*(output metadata only; not passed to `CleanerValidatorArgs`)*"
            default_cell = "—"
        else:
            target_cell = _code_cell(self.target_field or "")
            default_cell = _code_cell(self.default_repr or "")
        return (
            _code_cell(self.key),
            _code_cell(self.constraint_json, escape=True),
            target_cell,
            default_cell,
        )


class XsvColumnTypeRow(FrozenDocModel):
    """Row model for the XSV column type mapping reference table."""

    json_schema: OptionalJsonText
    duckdb_dtype: str
    force_not_null: bool
    arrow_type_expr: PyArrowTypeExpr
    nullable: bool

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        schema_cell = (
            _code_cell(self.json_schema, escape=True)
            if self.json_schema is not None
            else "*(column in `required`, omitted from `properties`)*"
        )
        return (
            schema_cell,
            _code_cell(self.duckdb_dtype),
            _code_cell(str(self.force_not_null)),
            _code_cell(self.arrow_type_expr),
            _code_cell(str(self.nullable)),
        )


class XmlPresetRow(FrozenDocModel):
    """Row model for the ``xmltopyarrow`` presets reference table."""

    cli_name: str
    settings_class_name: str
    dataset_repr: str | None
    xml_level: int | None
    root_tag_repr: str | None
    xml_tag_repr: str | None
    table_name_repr: str | None
    file_glob_repr: str
    preprocessors: CallableNameTuple

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        dataset_cell = f"`{self.dataset_repr}` (locked)" if self.dataset_repr is not None else "*(required)*"
        if self.xml_level is not None:
            target_cell = f"`xml_level={self.xml_level}`, `root_tag={self.root_tag_repr}` (locked)"
        elif self.xml_tag_repr is not None:
            target_cell = f"`xml_tag={self.xml_tag_repr}`, `table_name={self.table_name_repr}` (locked)"
        else:
            target_cell = "`xml_tag` + `table_name` **or** `xml_level` (+ optional `root_tag`)"
        preproc_cell = _code_csv(self.preprocessors) if self.preprocessors else "—"
        return (
            _code_cell(self.cli_name),
            _code_cell(self.settings_class_name),
            dataset_cell,
            target_cell,
            _code_cell(self.file_glob_repr),
            preproc_cell,
        )


class SupportedDraftRow(FrozenDocModel):
    """Row model for the supported JSON Schema drafts reference table."""

    schema_uri: str
    validator_class_name: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            _code_cell(self.schema_uri),
            _code_cell(self.validator_class_name),
            "`FormatChecker()` enabled",
        )


class RejectedColumnRow(FrozenDocModel):
    """Row model for the ``REJECTED_PYARROW_SCHEMA`` reference table."""

    name: str
    arrow_type_expr: PyArrowTypeExpr
    nullable: bool
    description: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            _code_cell(self.name),
            _code_cell(self.arrow_type_expr),
            _code_cell(str(self.nullable)),
            escape_table_cell(self.description),
        )


class SimpleTableRow(FrozenDocModel):
    """Generic pre-formatted row model for ``tables/reference_table.md.j2``."""

    cells: tuple[str, ...]


class TableRenderContext(FrozenDocModel):
    """Bound template path, headers, and typed row tuple for a reference table."""

    table_id: str
    template: str = "tables/reference_table.md.j2"
    headers: tuple[str, ...]
    rows: tuple[object, ...]


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

XMLTOPYARROW_SETTINGS_CLASSES: Final[tuple[type[XmlToPyArrowSettings], ...]] = (
    XmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    PubMedXmlToPyArrowSettings,
)

JSONL_JSONSCHEMA_DRAFT_URIS: Final[tuple[str, ...]] = (
    "https://json-schema.org/draft/2020-12/schema",
    "https://json-schema.org/draft/2019-09/schema",
    "http://json-schema.org/draft-07/schema#",
    "http://json-schema.org/draft-06/schema#",
    "http://json-schema.org/draft-04/schema#",
    CUSTOM_META_SCHEMA_URI,
)

REJECTED_COLUMN_DESCRIPTIONS: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        "table_name": "Target entity table name (`None` for level-mode file pre-processor errors)",
        "source_file": "Input file path relative to `input_dir`",
        "line_no": "1-based physical line number in `source_file` (`None` for file-level errors)",
        "raw_record": "Stripped raw input line (`None` for file-level errors)",
        "error_detail": "Parser error message, JSON-encoded list of schema errors, or pre-processor error",
    }
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


def build_xsv_config_rows() -> tuple[XsvConfigRow, ...]:
    """Extract ``x-xsv-config`` metaschema and ``CleanerValidatorArgs`` field mapping rows.

    :return: Tuple of ``XsvConfigRow`` records.
    :rtype: tuple[XsvConfigRow, ...]
    """
    props: Mapping[str, Any] = get_xsv_config_schema()["properties"]
    arg_fields = CleanerValidatorArgs.model_fields
    rows: list[XsvConfigRow] = []
    for raw_key, prop_schema in props.items():
        stripped = raw_key.removeprefix("x-").replace("-", "_")
        constraint_dict = {k: v for k, v in prop_schema.items() if k != "description"}
        if stripped == "has_header":
            rows.append(
                XsvConfigRow.model_validate(
                    {
                        "key": raw_key,
                        "constraint_json": constraint_dict,
                        "target_field": "missing_header",
                        "is_has_header": True,
                        "metadata_only": False,
                        "default_repr": "False",
                    }
                )
            )
        elif stripped in PARSING_CONFIG_KEYS:
            target_field = PARSING_CONFIG_KEYS[stripped]
            default_val = arg_fields[target_field].default
            rows.append(
                XsvConfigRow.model_validate(
                    {
                        "key": raw_key,
                        "constraint_json": constraint_dict,
                        "target_field": target_field,
                        "is_has_header": False,
                        "metadata_only": False,
                        "default_repr": repr(default_val),
                    }
                )
            )
        else:
            rows.append(
                XsvConfigRow.model_validate(
                    {
                        "key": raw_key,
                        "constraint_json": constraint_dict,
                        "target_field": None,
                        "is_has_header": False,
                        "metadata_only": True,
                        "default_repr": None,
                    }
                )
            )
    return tuple(rows)


def _build_xsv_column_probes() -> list[dict[str, Any] | None]:
    """Derive XSV column probes from scalar type hints and the XSV specification case schemas.

    :return: Ordered list of distinct column JSON Schema dicts (plus ``None`` for unlisted ``required`` columns).
    :rtype: list[dict[str, Any] | None]
    """
    seen: set[str] = set()
    probes: list[dict[str, Any] | None] = []

    def add_probe(probe: dict[str, Any]) -> None:
        """Record ``probe`` if its canonical JSON key has not been seen.

        :param probe: Column JSON Schema dictionary.
        :type probe: dict[str, Any]
        :return: None.
        :rtype: None
        """
        key = json.dumps(probe, sort_keys=True)
        if key not in seen:
            seen.add(key)
            probes.append(probe)

    for scalar_type in (*NON_EXTENDABLE_SCALAR_HINTS, "string"):
        add_probe({"type": scalar_type})
        add_probe({"type": [scalar_type, "null"]})

    has_unlisted_required = False
    for case in load_cases(PIPELINE_SPECS_ROOT / "xsv" / "spec"):
        raw_schema = json.loads(case.schema.read_text(encoding="utf-8"))
        deref = dereference_schema(raw_schema)
        props = deref.get("properties", {})
        if any(col not in props for col in deref.get("required", ())):
            has_unlisted_required = True
        for prop_schema in props.values():
            cleaned = {k: v for k, v in prop_schema.items() if k not in {"title", "description"}}
            add_probe(cleaned)

    if has_unlisted_required:
        probes.append(None)
    return probes


def build_xsv_column_type_rows() -> tuple[XsvColumnTypeRow, ...]:
    """Extract XSV column type and nullability rows from ``convert_xsv_schema_to_pyarrow`` and DuckDB config.

    :return: Tuple of ``XsvColumnTypeRow`` records.
    :rtype: tuple[XsvColumnTypeRow, ...]
    """
    rows: list[XsvColumnTypeRow] = []
    for probe in _build_xsv_column_probes():
        properties: dict[str, Any] = {"col": dict(probe)} if probe is not None else {}
        raw_schema: dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["col"],
            "properties": properties,
        }
        pa_schema = convert_xsv_schema_to_pyarrow(ValidatedXsvSchema(jsonschema=raw_schema))
        pa_field = pa_schema.field("col")
        duckdb_cfg = resolve_duckdb_column_config(pa_schema if probe is not None else None)
        duckdb_dtype = str(duckdb_cfg.get("dtype", {}).get("col", "VARCHAR (auto)"))
        force_not_null = "col" in duckdb_cfg.get("force_not_null", [])
        rows.append(
            XsvColumnTypeRow.model_validate(
                {
                    "json_schema": probe,
                    "duckdb_dtype": duckdb_dtype,
                    "force_not_null": force_not_null,
                    "arrow_type_expr": pa_field.type,
                    "nullable": pa_field.nullable,
                }
            )
        )
    return tuple(rows)


def build_xmltopyarrow_preset_rows() -> tuple[XmlPresetRow, ...]:
    """Extract XML-to-PyArrow base and preset settings rows from ``XMLTOPYARROW_SETTINGS_CLASSES``.

    :return: Tuple of ``XmlPresetRow`` records.
    :rtype: tuple[XmlPresetRow, ...]
    """
    rows: list[XmlPresetRow] = []
    for cls in XMLTOPYARROW_SETTINGS_CLASSES:
        cli_name = str(cls.model_config.get("cli_prog_name", ""))
        dataset_default = getattr(cls, "dataset_name", None)
        xml_level = getattr(cls, "xml_level", None)
        xml_tag = getattr(cls, "xml_tag", None)
        root_tag = getattr(cls, "root_tag", None)
        table_name = getattr(cls, "table_name", None)
        glob_default = cls.model_fields["file_glob"].default
        preprocessors = getattr(cls, "DEFAULT_FILE_PREPROCESSORS", ())
        rows.append(
            XmlPresetRow.model_validate(
                {
                    "cli_name": cli_name,
                    "settings_class_name": cls.__name__,
                    "dataset_repr": repr(dataset_default) if isinstance(dataset_default, str) else None,
                    "xml_level": xml_level if isinstance(xml_level, int) else None,
                    "root_tag_repr": repr(root_tag) if isinstance(xml_level, int) else None,
                    "xml_tag_repr": repr(xml_tag) if isinstance(xml_tag, str) else None,
                    "table_name_repr": repr(table_name) if isinstance(xml_tag, str) else None,
                    "file_glob_repr": repr(glob_default),
                    "preprocessors": preprocessors,
                }
            )
        )
    return tuple(rows)


def build_jsonlines_jsonschema_draft_rows() -> tuple[SupportedDraftRow, ...]:
    """Extract supported JSON Schema draft URI and validator class rows via ``validator_for``.

    :return: Tuple of ``SupportedDraftRow`` records.
    :rtype: tuple[SupportedDraftRow, ...]
    """
    register_xsv_validator()
    rows: list[SupportedDraftRow] = []
    for uri in JSONL_JSONSCHEMA_DRAFT_URIS:
        probe_schema: Any = {"$schema": uri}
        validator_cls = validator_for(probe_schema, default=False)
        cls_name = validator_cls.__name__ if isinstance(validator_cls, type) else "None"
        rows.append(SupportedDraftRow(schema_uri=uri, validator_class_name=cls_name))
    return tuple(rows)


def build_rejected_schema_rows() -> tuple[RejectedColumnRow, ...]:
    """Extract ``REJECTED_PYARROW_SCHEMA`` column rows for the rejected schema reference table.

    :return: Tuple of ``RejectedColumnRow`` records.
    :rtype: tuple[RejectedColumnRow, ...]
    """
    return tuple(
        RejectedColumnRow.model_validate(
            {
                "name": field.name,
                "arrow_type_expr": field.type,
                "nullable": field.nullable,
                "description": REJECTED_COLUMN_DESCRIPTIONS.get(field.name, "—"),
            }
        )
        for field in REJECTED_PYARROW_SCHEMA
    )


TABLE_TEMPLATE_REGISTRY: Final[MappingProxyType[str, tuple[tuple[str, ...], Callable[[], tuple[object, ...]]]]] = (
    MappingProxyType(
        {
            "xsd_scalar_table": (
                ("XSD built-in type(s)", "Python type (coerced)", "PyArrow `DataType`"),
                build_xsd_scalar_rows,
            ),
            "jsonschema_scalar_table": (
                ("JSON Schema", "Python type (coerced)", "PyArrow `DataType`", '`metadata["jsonschema_type"]`'),
                build_jsonschema_scalar_rows,
            ),
            "pydantic_scalar_table": (
                (
                    "Pydantic type / annotation",
                    "Python type (coerced)",
                    "PyArrow `DataType`",
                    '`metadata["pydantic_type"]`',
                ),
                build_pydantic_scalar_rows,
            ),
            "pyarrow_to_jsonschema_scalar_table": (
                ("PyArrow `DataType`", "Serialized JSON value", "JSON Schema (core)", "`x-pyarrow`"),
                build_pyarrow_to_jsonschema_rows,
            ),
            "pyarrow_to_linkml_scalar_table": (
                (
                    "PyArrow `DataType`",
                    "Serialized JSON value",
                    "LinkML `range`",
                    "Slot bounds (`minimum_value` .. `maximum_value`)",
                ),
                build_pyarrow_to_linkml_rows,
            ),
            "xsv_config_table": (
                ("`x-xsv-config` key", "JSON Schema constraint", "`CleanerValidatorArgs` field", "Default"),
                build_xsv_config_rows,
            ),
            "column_type_table": (
                (
                    "Column JSON Schema",
                    "DuckDB CSV `dtype`",
                    "`force_not_null`",
                    "PyArrow `DataType`",
                    "`Field.nullable`",
                ),
                build_xsv_column_type_rows,
            ),
            "presets_table": (
                (
                    "CLI Command",
                    "Settings Class",
                    "`dataset_name`",
                    "Target Selection",
                    "`file_glob`",
                    "Default Pre-processors",
                ),
                build_xmltopyarrow_preset_rows,
            ),
            "supported_drafts_table": (
                ("Declared `$schema` URI", "Validator Class", "Format Checking"),
                build_jsonlines_jsonschema_draft_rows,
            ),
            "rejected_schema_table": (
                ("Column", "PyArrow `DataType`", "`Field.nullable`", "Description"),
                build_rejected_schema_rows,
            ),
            "dlt_pyarrow_flat_columns_table": (
                (
                    "Column",
                    "Input PyArrow Field",
                    "Input Metadata Keys",
                    "`dlt` Column",
                    "Parquet Field",
                    "Iceberg `NestedField`",
                    "Iceberg Scan (`to_arrow()`)",
                ),
                build_dlt_pyarrow_flat_column_rows,
            ),
            "dlt_pyarrow_nested_dlt_columns_table": (
                (
                    "Column",
                    "Input PyArrow Field",
                    "`dlt` Column",
                    "Arrow IPC Hint (`deserialize_type`)",
                ),
                build_dlt_pyarrow_nested_dlt_column_rows,
            ),
            "dlt_pyarrow_nested_paths_table": (
                (
                    "Input Path",
                    "Slot Kind",
                    "Input Type and Metadata",
                    "Parquet Type and Metadata",
                    "Iceberg Slot and `doc`",
                    "Iceberg Scan (`to_arrow()`)",
                ),
                build_dlt_pyarrow_nested_path_rows,
            ),
            "dlt_pyarrow_identifier_normalization_table": (
                (
                    "Fixture",
                    "Input Path",
                    "Slot Kind",
                    "`dlt` Path (`default_schema` / `x-nested-type`)",
                    "Parquet Path",
                    "Iceberg Path and `doc`",
                ),
                build_dlt_pyarrow_identifier_rows,
            ),
            "dlt_nested_placement_matrix_table": (
                (
                    "Placement",
                    "Where `max_table_nesting=0` Is Set",
                    "Python `dict` Outcome",
                    "`pyarrow.Table` Outcome",
                ),
                build_dlt_nested_placement_rows,
            ),
            "dlt_nested_single_table_columns_table": (
                (
                    "Column",
                    "`dict` (`nesting=0`): `dlt` / Parquet / Iceberg",
                    "`dict` (`nesting=0`): JSONL",
                    "`pyarrow.Table`: `dlt` / Parquet / Iceberg",
                    "`pyarrow.Table`: JSONL (`ArrowToJsonlWriter`)",
                ),
                build_dlt_nested_single_table_column_rows,
            ),
            "dlt_nested_unnested_tables_table": (
                (
                    "Generated Table",
                    "Source Path and Role",
                    "`dlt` Columns",
                    "Parquet Schema",
                    "Iceberg Schema",
                ),
                build_dlt_unnested_table_rows,
            ),
        }
    )
)


def build_table_context(table_id: str) -> TableRenderContext:
    """Resolve ``table_id`` in ``TABLE_TEMPLATE_REGISTRY`` into a ``TableRenderContext``.

    :param table_id: Canonical reference table identifier.
    :type table_id: str
    :return: Bound ``TableRenderContext`` with headers and extracted rows.
    :rtype: TableRenderContext
    """
    if table_id not in TABLE_TEMPLATE_REGISTRY:
        msg = f"Unknown reference table identifier: {table_id!r}"
        raise KeyError(msg)
    headers, builder = TABLE_TEMPLATE_REGISTRY[table_id]
    return TableRenderContext(table_id=table_id, headers=headers, rows=builder())
