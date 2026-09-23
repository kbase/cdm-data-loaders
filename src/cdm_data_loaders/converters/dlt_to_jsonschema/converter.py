"""Converts a dlt stored schema (or tables dict) into a JSON Schema draft 2020-12 document.

Accepts a full `TStoredSchema`-shaped dict (`name`, `tables`, ...) as produced by
`JSONSchemaToDlt.convert()` or `dlt.Schema.to_dict()`, or a plain `{table_name: TTableSchema}`
dict. Each root table becomes a JSON Schema document; nested child tables (those with a
`parent` entry) are folded back into their parent as object properties or `items` subschemas,
reversing dlt's relational-normalizer naming (`parent__child` with `__` path separators).

Nullable columns are rendered as `anyOf: [<type>, {"type": "null"}]` per the repo's
iceberg-to-jsonschema conventions; dlt's `_dlt_*` internal columns are dropped by default.
"""

import logging
from copy import deepcopy
from typing import Any, Final, Literal

from dlt.common.schema.typing import TTableSchema
from pydantic import BaseModel, ConfigDict, Field, field_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.io import load_schema_file, load_schema_text
from cdm_data_loaders.converters.dlt_normalization import DltNormalizationError, DltTableNode, unflatten_tables

logger = logging.getLogger(__name__)

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"

# dlt internal column prefix; these loader-added columns have no JSON Schema equivalent
DLT_INTERNAL_PREFIX: Final[str] = "_dlt_"

# dlt -> JSON Schema data type map; formats refine the string type below
TYPE_MAP: Final[dict[str, dict[str, Any]]] = {
    "text": {"type": "string"},
    "bigint": {"type": "integer"},
    "double": {"type": "number"},
    "decimal": {
        "type": "string",
        "pattern": r"^-?\d+(\.\d+)?$",
        "x-dlt": {"data_type": "decimal"},
    },
    "bool": {"type": "boolean"},
    "timestamp": {
        "type": "string",
        "format": "date-time",
        "x-dlt": {"data_type": "timestamp"},
    },
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
    "binary": {"type": "string", "contentEncoding": "base64"},
    "json": {},
    "wei": {"type": "integer", "x-dlt": {"data_type": "wei"}},
}


class DltToJSONSchemaError(ConversionError):
    """Raised when a dlt schema construct cannot be converted."""


class DltToJSONSchema(BaseModel):
    """Converts dlt stored schemas into JSON Schema draft 2020-12 documents.

    Input may be:
      - a full stored-schema dict (`name` + `tables`) as produced by
        `JSONSchemaToDlt.convert()` or `dlt.Schema.to_dict()`
      - a plain `{table_name: TTableSchema}` dict
      - a YAML/JSON file of either shape

    Root tables (no `parent` entry) each become a JSON Schema document with
    `$schema` set to draft 2020-12. Nested child tables are folded back into
    their parents: `parent__orders` becomes a property, rendered as an object
    (or an array of objects with `child_table_mode="array"`) depending on
    configuration. Scalar-value child tables (single `value` column) always
    become arrays of the value type.

    Note: dlt's stored schema does not record whether a child table came from
    a nested dict or from a list of objects -- both flatten to the same
    child-table shape -- so the object-vs-array choice is a configuration
    decision, not something recoverable from the schema.

    Example:
        >>> converter = DltToJSONSchema()
        >>> docs = converter.convert(stored_schema_dict)
        >>> docs["customers"]  # a draft 2020-12 document for the customers table
    """

    model_config = ConfigDict(frozen=True)

    child_table_mode: Literal["object", "array"] = Field(
        default="object",
        description=(
            "How to render nested child tables in their parent: 'object' renders "
            "them as object properties; 'array' renders them as arrays of objects. "
            "dlt's stored schema does not record the original nesting kind, so "
            "this is a configuration choice. Scalar-value child tables are always "
            "arrays regardless of this setting."
        ),
    )
    include_dlt_columns: bool = Field(
        default=False,
        description=(
            "If True, dlt's internal `_dlt_*` columns are kept in the output. "
            "By default they are dropped, since loader-added bookkeeping columns "
            "have no JSON Schema equivalent."
        ),
    )
    include_variant_columns: bool = Field(
        default=False,
        description=(
            "If True, dlt variant columns (`col__v_text` etc.) are kept. "
            "By default they are dropped, since they are runtime coercion artifacts."
        ),
    )
    preserve_unknown_hints: bool = Field(
        default=True,
        description="Copy unknown/extension column hints into an `x-dlt` metadata block.",
    )

    @field_validator("preserve_unknown_hints", "include_dlt_columns", "include_variant_columns", mode="before")
    @classmethod
    def _coerce_bool(cls, value: Any) -> Any:  # noqa: ANN401
        """Let pydantic handle coercion; hook reserved for future normalization."""
        return value

    def convert(self, schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Convert a dlt stored schema into JSON Schema draft 2020-12 documents.

        :param schema: a stored-schema dict (`name` + `tables`) or a `{table_name: TTableSchema}` dict
        :type schema: dict[str, Any]
        :return: mapping of root table name -> draft 2020-12 JSON Schema document
        :rtype: dict[str, dict[str, Any]]
        :raises DltToJSONSchemaError: if the input has no `tables` key and no table-shaped values
        """
        tables = self._extract_tables(schema)
        try:
            roots = unflatten_tables(tables)
        except DltNormalizationError as error:
            raise DltToJSONSchemaError(str(error)) from error
        return {name: self._convert_root_table(node) for name, node in roots.items()}

    def convert_from_string(self, schema_str: str) -> dict[str, dict[str, Any]]:
        """Convert a dlt schema supplied as a JSON or YAML string.

        :param schema_str: JSON or YAML text of a stored schema or tables dict
        :type schema_str: str
        :return: mapping of root table name -> draft 2020-12 JSON Schema document
        :rtype: dict[str, dict[str, Any]]
        """
        return self.convert(load_schema_text(schema_str, allow_yaml=True))

    def convert_from_file(self, path: str) -> dict[str, dict[str, Any]]:
        """Convert a dlt schema from a JSON or YAML file.

        :param path: path to the schema; format is chosen by the `.json`/`.yaml` extension
        :type path: str
        :return: mapping of root table name -> draft 2020-12 JSON Schema document
        :rtype: dict[str, dict[str, Any]]
        """
        return self.convert(load_schema_file(path))

    def _extract_tables(self, schema: dict[str, Any]) -> dict[str, TTableSchema]:
        """Extract the tables dict from a stored schema or a plain tables mapping.

        :param schema: the input schema dict
        :type schema: dict[str, Any]
        :return: mapping of table name -> TTableSchema
        :rtype: dict[str, TTableSchema]
        :raises DltToJSONSchemaError: if the input shape is unrecognized
        """
        if "tables" in schema and isinstance(schema["tables"], dict):
            tables = schema["tables"]
        elif all(isinstance(v, dict) and ("columns" in v or "parent" in v) for v in schema.values()):
            tables = schema
        else:
            err_msg = (
                "Input must be a dlt stored schema (with a 'tables' dict) or a "
                "{table_name: TTableSchema} mapping; no tables found."
            )
            raise DltToJSONSchemaError(err_msg)
        if not tables:
            err_msg = "The schema contains no tables."
            raise DltToJSONSchemaError(err_msg)
        return tables

    def _convert_root_table(self, node: DltTableNode) -> dict[str, Any]:
        """Convert a root node and its descendants into a JSON Schema document."""
        root_name = node.name
        root_table = node.table
        document = self._convert_table_object(node)
        document["$schema"] = JSON_SCHEMA_DIALECT
        document["$id"] = f"urn:dlt:{root_name}"
        if "description" in root_table:
            document["title"] = root_name
            document["description"] = root_table["description"]
        else:
            document.setdefault("title", root_name)
        return document

    def _convert_table_object(self, node: DltTableNode) -> dict[str, Any]:
        """Convert a node's columns and children into object properties."""
        table = node.table
        properties: dict[str, Any] = {}
        required: list[str] = []

        for col_name, col in table.get("columns", {}).items():
            if self._skip_column(col_name):
                continue
            if col.get("nullable", True) is False:
                required.append(col_name)
            properties[col_name] = self._convert_column(col_name, col, nullable=col.get("nullable", True))

        for prop_key, child in node.children.items():
            properties[prop_key] = self._convert_child(child)
            if self._child_all_required(child.table):
                required.append(prop_key)

        result: dict[str, Any] = {"type": "object", "properties": properties, "additionalProperties": False}
        if required:
            result["required"] = required
        return result

    def _skip_column(self, col_name: str) -> bool:
        """Decide whether a dlt column should be omitted from the output.

        Skips dlt internal `_dlt_*` columns unless `include_dlt_columns` is set, and
        variant columns (`col__v_*`) unless `include_variant_columns` is set.

        :param col_name: the column's name
        :type col_name: str
        :return: True if the column should be omitted
        :rtype: bool
        """
        if not self.include_dlt_columns and col_name.startswith(DLT_INTERNAL_PREFIX):
            return True
        return not self.include_variant_columns and self._is_variant_column(col_name)

    @staticmethod
    def _is_variant_column(col_name: str) -> bool:
        """Check whether a column name matches dlt's variant-column pattern (`col__v_<type>`).

        :param col_name: the column's name
        :type col_name: str
        :return: True if the name contains the `__v_` variant marker
        :rtype: bool
        """
        return "__v_" in col_name

    def _child_all_required(self, child_table: TTableSchema) -> bool:
        """Check whether a child table declares at least one non-nullable column.

        Used to decide whether the folded property should appear in the parent's `required`.

        :param child_table: the child table schema
        :type child_table: TTableSchema
        :return: True if the child has any non-nullable, non-skipped column
        :rtype: bool
        """
        return any(
            col.get("nullable", True) is False and not self._skip_column(col_name)
            for col_name, col in child_table.get("columns", {}).items()
        )

    def _convert_child(self, node: DltTableNode) -> dict[str, Any]:
        """Render a child as an object or array according to converter policy."""
        child_table = node.table
        if self._is_scalar_value_table(node):
            value_col = child_table["columns"]["value"]
            child_schema: dict[str, Any] = {
                "type": "array",
                "items": self._convert_column("value", value_col, nullable=value_col.get("nullable", True)),
            }
        else:
            object_schema = self._convert_table_object(node)
            if self.child_table_mode == "array":
                child_schema = {"type": "array", "items": object_schema}
            else:
                child_schema = object_schema

        if "description" in child_table:
            child_schema["description"] = child_table["description"]
        return child_schema

    def _is_scalar_value_table(self, node: DltTableNode) -> bool:
        """Check for a single visible value column with no child tables."""
        if node.children:
            return False
        data_columns = [name for name in node.table.get("columns", {}) if not self._skip_column(name)]
        return data_columns == ["value"]

    def _convert_column(self, col_name: str, col: dict[str, Any], *, nullable: bool = True) -> dict[str, Any]:
        """Convert a single dlt column into a JSON Schema property.

        Nullable columns wrap the mapped type in `anyOf: [<type>, {"type": "null"}]`,
        matching the repo's iceberg-to-jsonschema convention.

        :param col_name: the column's name
        :type col_name: str
        :param col: the column schema
        :type col: dict[str, Any]
        :param nullable: whether the column may contain nulls
        :type nullable: bool
        :return: a draft 2020-12 property schema
        :rtype: dict[str, Any]
        :raises DltToJSONSchemaError: if the column's data_type is not a known dlt type
        """
        data_type = col.get("data_type")
        if data_type is None:
            # incomplete column (no data type yet): accept anything
            node: dict[str, Any] = {}
        elif data_type in TYPE_MAP:
            node = deepcopy(TYPE_MAP[data_type])
        else:
            err_msg = f"Column {col_name!r} has unknown dlt data_type {data_type!r}."
            raise DltToJSONSchemaError(err_msg)

        if col.get("precision") is not None or col.get("scale") is not None:
            node.setdefault("x-dlt", {})["precision"] = col.get("precision")
            if col.get("scale") is not None:
                node["x-dlt"]["scale"] = col.get("scale")

        if "description" in col:
            node["description"] = col["description"]

        if self.preserve_unknown_hints:
            hints = self._collect_unknown_hints(col)
            if hints:
                node.setdefault("x-dlt", {}).update(hints)

        if nullable and node:
            return {"anyOf": [node, {"type": "null"}]}
        return node

    def _collect_unknown_hints(self, col: dict[str, Any]) -> dict[str, Any]:
        """Collect dlt column hints that have no JSON Schema equivalent.

        Keeps primary/foreign keys, precision/scale and vendor `x-*` hints in an
        `x-dlt` metadata block so they survive a round trip.

        :param col: the column schema
        :type col: dict[str, Any]
        :return: mapping of hint name -> value for hints worth preserving
        :rtype: dict[str, Any]
        """
        known = {"name", "data_type", "nullable", "description", "precision", "scale"}
        hints: dict[str, Any] = {}
        for key, value in col.items():
            if key in known or value is None:
                continue
            hints[key] = value
        return hints
