"""Converts a fully-dereferenced JSON Schema document into a dlt stored-schema dict.

The output is a `TStoredSchema`-shaped dict (`name`, `tables`, `engine_version`, ...) that can be
loaded with `dlt.Schema.from_dict(...)`, applied as `@dlt.resource(columns=...)`, or written to a
`{schema_name}.schema.yaml` file for dlt's import-schema workflow.

Nested objects and arrays are flattened into dlt child tables with `parent` linkage, following
dlt's own relational normalizer naming (`parent__child` with `__` path separators). Set
`skip_nested_types=True` to keep them in place as `data_type: json` columns instead.

This module assumes the input schema has already been validated against the appropriate metaschema
and fully dereferenced: no `$ref` or `allOf` keys may remain anywhere in the document. Use
`jsonschema_to_dlt.dereferencing.dereference_schema()` first if your schema uses either.
"""

import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import yaml
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import (
    TColumnSchema,
    TStoredSchema,
    TTableSchema,
    TTableSchemaColumns,
)
from dlt.common.schema.utils import new_column, new_table
from pydantic import BaseModel, ConfigDict, Field, field_validator

from cdm_data_loaders.converters.jsonschema_to_pyspark.converter import _infer_implicit_type

if TYPE_CHECKING:
    from dlt.common.schema import Schema

logger = logging.getLogger(__name__)

SCHEMA_ENGINE_VERSION: Final[int] = 11

# dlt's child-table path separator, used by the relational normalizer
NESTED_TABLE_SEPARATOR: Final[str] = "__"

# JSON Schema 'format' values that map to dlt temporal/binary types; everything else -> text
DATETIME_FORMATS: Final[frozenset[str]] = frozenset({"date-time", "datetime", "timestamp"})
DATE_FORMATS: Final[frozenset[str]] = frozenset({"date"})
TIME_FORMATS: Final[frozenset[str]] = frozenset({"time"})
BINARY_FORMATS: Final[frozenset[str]] = frozenset({"byte", "binary", "base64"})


class JSONSchemaToDltError(ValueError):
    """Raised when a JSON Schema construct cannot be converted."""


class InvalidJSONSchemaError(JSONSchemaToDltError):
    """Raised when the input document is not a valid JSON Schema."""


def _decimal_places(value: float) -> int:
    """Number of digits after the decimal point needed to represent `value` exactly."""
    exponent = Decimal(str(value)).as_tuple().exponent
    if isinstance(exponent, int):
        return max(-exponent, 0)
    return 0


def _data_type_from_enum(values: list[Any]) -> TDataType:
    """Map an `enum` value list to the narrowest dlt data type covering all values."""
    if not values:
        return "text"
    if all(isinstance(v, bool) for v in values):
        return "bool"
    # bool is a subtype of int, so exclude bools before checking ints
    if all(isinstance(v, int) and not isinstance(v, bool) for v in values):
        return "bigint"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
        return "double"
    return "text"


class JSONSchemaToDlt(BaseModel):
    """Converts fully-dereferenced JSON Schema documents to dlt stored-schema dicts.

    The input schema must:
      - already be dereferenced: no `$ref` and no `allOf` may remain anywhere
        in the document. Use `jsonschema_to_dlt.dereferencing.dereference_schema()`
        first if your schema uses either.
      - declare its dialect via a top-level `$schema` keyword.

    Example:
        >>> from cdm_data_loaders.converters.jsonschema_to_dlt.converter import JSONSchemaToDlt
        >>> stored = JSONSchemaToDlt(schema_name="my_source").convert(my_dereferenced_schema)
        >>> schema = Schema.from_dict(stored)
    """

    model_config = ConfigDict(frozen=True)

    schema_name: str = Field(
        description="Name for the dlt schema; also becomes the default source name.",
    )
    skip_nested_types: bool = Field(
        default=False,
        description=(
            "If True, nested objects and arrays are kept in place as `data_type: json` "
            "columns instead of being flattened into dlt child tables."
        ),
    )
    max_nesting: int = Field(
        default=10,
        ge=1,
        description=(
            "Maximum child-table depth when flattening nested structures. Deeper levels "
            "are kept as `data_type: json` columns."
        ),
    )
    write_disposition: str | None = Field(
        default=None,
        description="Optional write_disposition applied to all root tables.",
    )
    flatten_scalars: bool = Field(
        default=True,
        description=(
            "If True, arrays of scalar values become dlt child tables with a `value` column "
            "(dlt's relational normalizer behavior). If False, they become `data_type: json` columns."
        ),
    )

    @field_validator("schema_name")
    @classmethod
    def _validate_schema_name(cls, value: str) -> str:
        """Reject empty schema names."""
        if not value or not value.strip():
            err_msg = "schema_name must be a non-empty string"
            raise ValueError(err_msg)
        return value

    def convert(self, schema: dict[str, Any]) -> TStoredSchema:
        """Convert a fully-dereferenced JSON Schema document into a dlt stored-schema dict.

        :param schema: the fully-dereferenced JSON Schema document to convert
        :type schema: dict[str, Any]
        :return: a dict loadable with `dlt.Schema.from_dict(...)`
        :rtype: TStoredSchema
        :raises InvalidJSONSchemaError: if the schema has no top-level '$schema' keyword
        :raises JSONSchemaToDltError: if the schema's root type isn't 'object' or still
            contains unresolved `$ref`/`allOf`
        """
        if not schema.get("$schema"):
            err_msg = (
                "Input JSON Schema is missing a '$schema' keyword. JSONSchemaToDlt requires schemas "
                "to explicitly declare their dialect via '$schema'; it will not assume a default."
            )
            raise InvalidJSONSchemaError(err_msg)

        if schema.get("type") not in (None, "object"):
            err_msg = f"Root schema must be of type 'object' to map to a dlt table, got: {schema.get('type')!r}"
            raise JSONSchemaToDltError(err_msg)

        self._reject_unresolved_references(schema)

        tables: dict[str, TTableSchema] = {}
        self._convert_object_table(
            schema=schema,
            table_name=self.schema_name,
            parent_table_name=None,
            tables=tables,
            depth=0,
        )

        stored_schema: TStoredSchema = {
            "name": self.schema_name,
            "version": 1,
            "previous_hashes": [],
            "engine_version": SCHEMA_ENGINE_VERSION,
            "tables": tables,
        }
        return stored_schema

    def convert_from_string(self, schema_str: str) -> TStoredSchema:
        """Convert a JSON Schema document supplied as a JSON string.

        :param schema_str: JSON string representing a schema
        :type schema_str: str
        :return: a dict loadable with `dlt.Schema.from_dict(...)`
        :rtype: TStoredSchema
        """
        return self.convert(json.loads(schema_str))

    def convert_from_file(self, path: str) -> TStoredSchema:
        """Convert a JSON Schema document from a JSON or YAML file.

        :param path: path to the schema; format is chosen by the `.json`/`.yaml` extension
        :type path: str
        :return: a dict loadable with `dlt.Schema.from_dict(...)`
        :rtype: TStoredSchema
        """
        loader = json.loads if path.endswith(".json") else yaml.safe_load
        return self.convert(loader(Path(path).read_bytes()))

    def to_yaml(self, schema: dict[str, Any]) -> str:
        """Convert a schema and serialize the resulting stored schema as YAML.

        :param schema: the fully-dereferenced JSON Schema document to convert
        :type schema: dict[str, Any]
        :return: YAML text of the dlt stored schema
        :rtype: str
        """
        return yaml.dump(
            self.convert(schema),
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )

    def to_schema(self, schema: dict[str, Any]) -> "Schema":
        """Convert a fully-dereferenced JSON Schema document into a live dlt `Schema` object.

        Constructs a real `Schema` instance (which seeds dlt's internal `_dlt_version` and
        `_dlt_loads` tables plus normalizers) and merges the converted tables into it, parents
        before children, via `Schema.update_table`.

        :param schema: the fully-dereferenced JSON Schema document to convert
        :type schema: dict[str, Any]
        :return: a dlt Schema object ready for `pipeline.run(..., schema=...)`
        :rtype: Schema
        :raises InvalidJSONSchemaError: if the schema has no top-level '$schema' keyword
        :raises JSONSchemaToDltError: if the schema's root type isn't 'object' or still
            contains unresolved `$ref`/`allOf`
        """
        from dlt.common.schema import Schema  # noqa: PLC0415

        stored = self.convert(schema)
        live_schema = Schema(stored["name"])
        # parents must merge before children or update_table raises ParentTableNotFoundException
        for table in self._tables_parents_first(stored["tables"]):
            live_schema.update_table(table, normalize_identifiers=False)
        return live_schema

    @staticmethod
    def _tables_parents_first(tables: dict[str, TTableSchema]) -> list[TTableSchema]:
        """Order tables so that every parent precedes its children.

        :param tables: dict of table name -> TTableSchema
        :type tables: dict[str, TTableSchema]
        :return: tables ordered parents-first, root tables in declaration order
        :rtype: list[TTableSchema]
        """
        ordered: list[TTableSchema] = []
        emitted: set[str] = set()

        def emit(table: TTableSchema) -> None:
            name = table["name"]
            if name in emitted:
                return
            parent_name = table.get("parent")
            if parent_name and parent_name in tables:
                emit(tables[parent_name])
            emitted.add(name)
            ordered.append(table)

        for table in tables.values():
            emit(table)
        return ordered

    @staticmethod
    def _reject_unresolved_references(schema: dict[str, Any]) -> None:
        """Guard against un-dereferenced schemas reaching the type-dispatch logic.

        :param schema: the schema fragment to check for unresolved `$ref`/`allOf`
        :type schema: dict[str, Any]
        :raises JSONSchemaToDltError: if `schema` still contains `$ref` or `allOf`
        """
        if "$ref" in schema:
            err_msg = (
                f"Encountered an unresolved $ref {schema['$ref']!r}. JSONSchemaToDlt requires a fully "
                "dereferenced schema -- this includes references to external JSON Schema documents. Use "
                "`jsonschema_to_dlt.dereferencing.dereference_schema()` to resolve all $refs before "
                "calling `convert()`."
            )
            raise JSONSchemaToDltError(err_msg)
        if "allOf" in schema:
            err_msg = (
                "Encountered an unmerged 'allOf'. JSONSchemaToDlt requires a fully dereferenced schema "
                "with 'allOf' already merged. Use `jsonschema_to_dlt.dereferencing.dereference_schema()` "
                "before calling `convert()`."
            )
            raise JSONSchemaToDltError(err_msg)

    def _convert_object_table(
        self,
        schema: dict[str, Any],
        table_name: str,
        parent_table_name: str | None,
        tables: dict[str, TTableSchema],
        depth: int,
    ) -> None:
        """Convert an object schema into a dlt table, recursing into nested structures.

        Nested objects and arrays become child tables named `<parent>__<key>` with a `parent`
        entry, matching dlt's relational normalizer. Columns produced here are incomplete until
        dlt's `apply_defaults` runs at load time (`_dlt_id`, `_dlt_load_id`, etc.).

        :param schema: the object schema fragment to convert
        :type schema: dict[str, Any]
        :param table_name: the dlt table name for this object
        :type table_name: str
        :param parent_table_name: parent dlt table name, or None for the root table
        :type parent_table_name: str | None
        :param tables: accumulator dict of table name -> TTableSchema, updated in place
        :type tables: dict[str, TTableSchema]
        :param depth: current nesting depth, starting at 0 for the root table; levels at or
            beyond `max_nesting` stay as json columns
        :type depth: int
        """
        properties: dict[str, Any] = schema.get("properties", {})
        required: list[str] = schema.get("required", [])
        columns: TTableSchemaColumns = {}

        for prop_name, prop_schema in properties.items():
            if isinstance(prop_schema, bool):
                # `true`/`false` boolean schemas carry no type information
                columns[prop_name] = new_column(prop_name, data_type="text", nullable=prop_name not in required)
                logger.warning("Boolean schema for property %r carries no type; using data_type 'text'.", prop_name)
                continue

            self._reject_unresolved_references(prop_schema)
            json_type = self._resolve_type(prop_schema)

            if json_type == "object" and not self.skip_nested_types and depth < self.max_nesting:
                child_name = f"{table_name}{NESTED_TABLE_SEPARATOR}{prop_name}"
                self._convert_object_table(
                    schema=prop_schema,
                    table_name=child_name,
                    parent_table_name=table_name,
                    tables=tables,
                    depth=depth + 1,
                )
            elif json_type == "array" and not self.skip_nested_types and self._array_items_are_objects(prop_schema):
                items_schema = self._single_items_schema(prop_schema)
                child_name = f"{table_name}{NESTED_TABLE_SEPARATOR}{prop_name}"
                self._convert_object_table(
                    schema=items_schema,
                    table_name=child_name,
                    parent_table_name=table_name,
                    tables=tables,
                    depth=depth + 1,
                )
            elif json_type == "array" and not self.skip_nested_types and not self._array_items_are_objects(prop_schema):
                if self.flatten_scalars and (items_schema := self._single_items_schema(prop_schema)):
                    # dlt's normalizer puts list elements in a child table with a `value` column
                    scalar_type = self._scalar_data_type(items_schema)
                    child_name = f"{table_name}{NESTED_TABLE_SEPARATOR}{prop_name}"
                    tables[child_name] = new_table(
                        table_name=child_name,
                        parent_table_name=table_name,
                        columns=[new_column("value", data_type=scalar_type)],
                    )
                else:
                    # arrays with no `items` have no element type; keep them as json
                    columns[prop_name] = new_column(prop_name, data_type="json", nullable=prop_name not in required)
            else:
                columns[prop_name] = self._convert_column(prop_name, prop_schema, nullable=prop_name not in required)

        table: TTableSchema = new_table(
            table_name=table_name,
            parent_table_name=parent_table_name,
            write_disposition=None if parent_table_name else self.write_disposition,
            columns=list(columns.values()),
        )
        if "description" in schema:
            table["description"] = schema["description"]
        tables[table_name] = table

    def _convert_column(self, name: str, prop_schema: dict[str, Any], *, nullable: bool) -> TColumnSchema:
        """Convert a non-nested property schema into a single dlt column definition.

        :param name: the property/column name
        :type name: str
        :param prop_schema: the property's schema fragment
        :type prop_schema: dict[str, Any]
        :param nullable: whether the column may contain nulls (from the parent's `required` list)
        :type nullable: bool
        :return: a TColumnSchema dict for the property
        :rtype: TColumnSchema
        """
        data_type = self._scalar_data_type(prop_schema)
        column = new_column(name, data_type=data_type, nullable=nullable)
        if "description" in prop_schema:
            column["description"] = prop_schema["description"]
        return column

    def _scalar_data_type(self, prop_schema: dict[str, Any]) -> TDataType:
        """Map a scalar property schema to a dlt data type.

        Nested objects and arrays that reach this method (via `skip_nested_types` or deep
        beyond `max_nesting`) are kept in place as dlt `json` columns.

        :param prop_schema: the schema fragment (must not be an object or array of objects)
        :type prop_schema: dict[str, Any]
        :return: the mapped dlt data type
        :rtype: TDataType
        """
        json_type = self._resolve_type(prop_schema)
        if json_type in ("object", "array"):
            return "json"
        return self._map_scalar_type(prop_schema, json_type)

    def _resolve_type(self, schema: dict[str, Any]) -> str:  # noqa: PLR0911
        """Resolve a schema fragment's effective JSON Schema type, resolving unions and enums.

        Single-non-null type unions resolve to that type; multi-type unions collapse to `text`
        with a warning (dlt has no union types); `enum`-only and type-less schemas infer from
        their keywords, matching the PySpark converter's behavior.

        :param schema: the schema fragment
        :type schema: dict[str, Any]
        :return: the effective JSON Schema type name
        :rtype: str
        """
        json_type = schema.get("type")

        if isinstance(json_type, list):
            non_null_types = [t for t in json_type if t != "null"]
            if len(non_null_types) == 1:
                json_type = non_null_types[0]
            elif not non_null_types:
                return "null"
            else:
                logger.warning("Collapsing multi-type union %r to 'text' (dlt has no union types).", json_type)
                return "text"

        if json_type is None and "enum" in schema:
            return self._json_type_from_dlt_type(_data_type_from_enum(schema["enum"]))

        if json_type is None and (inferred := _infer_implicit_type(schema)) is not None:
            return inferred

        if json_type in (None, "object", "array") or "oneOf" in schema or "anyOf" in schema:
            for combiner in ("oneOf", "anyOf"):
                if combinations := schema.get(combiner):
                    logger.warning(
                        "Approximating '%s' by using only its first branch for type inference.",
                        combiner,
                    )
                    first = combinations[0]
                    if isinstance(first, dict):
                        return self._resolve_type(first)
                    return "text"
            return str(json_type) if json_type is not None else "text"

        return str(json_type)

    @staticmethod
    def _json_type_from_dlt_type(data_type: TDataType) -> str:
        """Reverse-map a dlt data type (from enum inference) to a JSON Schema type name.

        :param data_type: dlt data type inferred from an enum's values
        :type data_type: TDataType
        :return: the equivalent JSON Schema type name
        :rtype: str
        """
        return {
            "bool": "boolean",
            "bigint": "integer",
            "double": "number",
        }.get(data_type, "string")

    def _map_scalar_type(self, schema: dict[str, Any], json_type: str | None) -> TDataType:
        """Map a resolved JSON Schema type (with its format/bounds keywords) to a dlt data type.

        :param schema: the schema fragment
        :type schema: dict[str, Any]
        :param json_type: the resolved JSON Schema type name
        :type json_type: str | None
        :return: the mapped dlt data type
        :rtype: TDataType
        """
        simple_map: dict[str, TDataType] = {
            "null": "text",
            "boolean": "bool",
            # dlt has a single integer type (bigint); int32-vs-int64 width hints are not representable
            "integer": "bigint",
        }
        if json_type in simple_map:
            return simple_map[json_type]

        if json_type == "number":
            if "multipleOf" in schema and isinstance(schema["multipleOf"], (int, float)):
                return "decimal"
            return "double"

        if json_type == "string":
            return self._string_format_type(schema.get("format"))

        return "text"

    @staticmethod
    def _string_format_type(fmt: Any) -> TDataType:  # noqa: ANN401
        """Map a JSON Schema string `format` value to a dlt data type.

        :param fmt: the schema's `format` value, or None
        :type fmt: Any
        :return: the mapped dlt data type
        :rtype: TDataType
        """
        if fmt in DATETIME_FORMATS:
            return "timestamp"
        if fmt in DATE_FORMATS:
            return "date"
        if fmt in TIME_FORMATS:
            return "time"
        if fmt in BINARY_FORMATS:
            return "binary"
        return "text"

    def _array_items_are_objects(self, prop_schema: dict[str, Any]) -> bool:
        """Check whether an array schema's items are object-typed.

        :param prop_schema: the array schema fragment
        :type prop_schema: dict[str, Any]
        :return: True if the array's element schema resolves to type 'object'
        :rtype: bool
        """
        items_schema = self._single_items_schema(prop_schema)
        if not items_schema or not isinstance(items_schema, dict):
            return False
        return self._resolve_type(items_schema) == "object"

    @staticmethod
    def _single_items_schema(prop_schema: dict[str, Any]) -> dict[str, Any] | bool | None:
        """Extract the single element schema from an array schema.

        Handles both modern `items` (single schema) and tuple-style `items` (list of schemas,
        first element wins) as well as `prefixItems`, mirroring the PySpark converter.

        :param prop_schema: the array schema fragment
        :type prop_schema: dict[str, Any]
        :return: the element schema, or None if the array declares none
        :rtype: dict[str, Any] | bool | None
        """
        if "prefixItems" in prop_schema:
            prefix_items = prop_schema["prefixItems"]
            return prefix_items[0] if prefix_items else None
        items = prop_schema.get("items")
        if isinstance(items, list):
            return items[0] if items else None
        return items
