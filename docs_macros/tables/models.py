"""Row models, table cell helpers, and render context models for reference tables."""

from collections.abc import Sequence
from typing import Annotated

import pyarrow as pa
from pydantic import BeforeValidator, ConfigDict

from docs_macros.formatting import (
    FrozenDocModel,
    JsonText,
    PyArrowPythonTypeLabel,
    PyArrowTypeExpr,
    coerce_json_text,
    escape_table_cell,
)


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


def code_cell(text: str, *, escape: bool = False) -> str:
    """Wrap ``text`` in Markdown inline backticks, escaping pipe characters when requested.

    :param text: Raw cell text.
    :type text: str
    :param escape: Whether to escape Markdown table pipes via ``escape_table_cell``.
    :type escape: bool
    :return: Backtick-wrapped cell string.
    :rtype: str
    """
    rendered = escape_table_cell(text) if escape else text
    return f"`{rendered}`"


def code_csv(items: Sequence[str], *, escape: bool = False) -> str:
    """Format a sequence of strings as comma-separated inline code spans.

    :param items: Sequence of string tokens.
    :type items: Sequence[str]
    :param escape: Whether to escape Markdown table pipes in each item.
    :type escape: bool
    :return: Comma-separated inline code string.
    :rtype: str
    """
    return ", ".join(code_cell(item, escape=escape) for item in items)


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
            first_cell = code_csv(self.xsd_names)
        else:
            first_cell = ", ".join(f"`{list_name}` (items `{item_name}`)" for list_name, item_name in self.list_items)
        return (first_cell, self.python_type_label, code_cell(self.arrow_type_expr))


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
            code_cell(self.json_schema, escape=True),
            self.python_type_label,
            code_cell(self.arrow_type_expr),
            code_cell(self.metadata_type_label, escape=True),
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
            first_cell = code_csv(self.type_tokens, escape=True)
            if self.qualifier:
                first_cell = f"{first_cell} {self.qualifier}"
        return (
            first_cell,
            self.python_type_label,
            code_cell(self.arrow_type_expr),
            code_cell(self.metadata_type_label, escape=True),
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
        json_val = code_cell(self.json_value_type)
        if self.json_value_note:
            json_val = f"{json_val} ({self.json_value_note})"
        return (
            code_cell(self.arrow_type_expr),
            json_val,
            code_cell(self.core_schema_json, escape=True),
            code_cell(self.x_pyarrow_json, escape=True),
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
        json_val = code_cell(self.json_value_type)
        if self.json_value_note:
            json_val = f"{json_val} ({self.json_value_note})"
        bounds = (
            f"`{self.min_value}` .. `{self.max_value}`"
            if self.min_value is not None or self.max_value is not None
            else "—"
        )
        return (
            code_cell(self.arrow_type_expr),
            json_val,
            code_cell(self.linkml_range),
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
            target_cell = code_cell(self.target_field or "")
            default_cell = code_cell(self.default_repr or "")
        return (
            code_cell(self.key),
            code_cell(self.constraint_json, escape=True),
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
            code_cell(self.json_schema, escape=True)
            if self.json_schema is not None
            else "*(column in `required`, omitted from `properties`)*"
        )
        return (
            schema_cell,
            code_cell(self.duckdb_dtype),
            code_cell(str(self.force_not_null)),
            code_cell(self.arrow_type_expr),
            code_cell(str(self.nullable)),
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
        preproc_cell = code_csv(self.preprocessors) if self.preprocessors else "—"
        return (
            code_cell(self.cli_name),
            code_cell(self.settings_class_name),
            dataset_cell,
            target_cell,
            code_cell(self.file_glob_repr),
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
            code_cell(self.schema_uri),
            code_cell(self.validator_class_name),
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
            code_cell(self.name),
            code_cell(self.arrow_type_expr),
            code_cell(str(self.nullable)),
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
