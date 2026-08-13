"""Converts a fully-dereferenced JSON Schema document into a PySpark StructType schema.

JSON Schema metadata (by default, title and description, with other fields at the user's discretion)
is preserved in each StructField's `metadata` dict.

This module assumes the input schema has already been validated against the appropriate metaschema
and fully dereferenced, i.e. there are no remaining `$ref` or `allOf` keys anywhere in the document.
"""

import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from functools import cache
from pathlib import Path
from typing import Any, Final

import yaml
from frozendict import frozendict
from jsonschema import Draft7Validator
from jsonschema.validators import validator_for
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

INT32_MIN: Final[int] = -2_147_483_648
INT32_MAX: Final[int] = 2_147_483_647

# fields from JSON Schema to automatically copy in the StructField `metadata`
DEFAULT_METADATA_KEYWORDS: Final[frozenset[str]] = frozenset({"title"})

# mapping of JSON Schema 'format' fields to PySpark types
DEFAULT_FORMAT_MAP: frozendict[str, DataType] = frozendict(
    {
        # see RFC 3339, section 5.6, for format definitions
        # YYYY-MM-DD (RFC3339:full-date)
        "date": DateType(),
        # YYYY-MM-DD"T"HH:MM:SS(.n*)?"Z"[+-]HH:MM (RFC3339:date-time)
        "date-time": TimestampType(),
        # HH:MM:SS(.n*)?"Z"[+-]HH:MM (RFC3339:full-time)
        # PySparks's TimeType has no timezone
        "time": StringType(),
        **{
            t: StringType()
            for t in [
                "duration",
                "email",
                "hostname",
                "idn-email",
                "idn-hostname",
                "ipv4",
                "ipv6",
                "iri-reference",
                "iri",
                "uri-reference",
                "uri",
                "uuid",
            ]
        },
    }
)


# JSON Schema keywords that control $ref resolution / schema identity
REF_AND_IDENTITY_KEYWORDS: frozenset[str] = frozenset(
    {
        "$anchor",
        "$defs",
        "$dynamicAnchor",
        "$dynamicRef",
        "$id",
        "$ref",
        "$schema",
        "$vocabulary",
        "definitions",
        "id",
    }
)

# Keywords that are structural/compositional -- not useful for StructField metadata.
STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS: frozenset[str] = frozenset(
    {
        "additionalItems",
        "additionalProperties",
        "allOf",
        "anyOf",
        "contains",
        "contentSchema",
        "dependencies",
        "dependentRequired",
        "dependentSchemas",
        "else",
        "if",
        "items",
        "not",
        "oneOf",
        "patternProperties",
        "prefixItems",
        "properties",
        "propertyNames",
        "required",
        "then",
        "type",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)


# Keywords whose mere presence in a `type`-less schema implies a specific
# JSON Schema type -- per the spec, `type` is optional, and a schema that
# omits it is still constrained by whichever type-specific keywords it uses.
IMPLICIT_OBJECT_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "properties",
        "patternProperties",
        "additionalProperties",
        "unevaluatedProperties",
        "required",
        "propertyNames",
        "minProperties",
        "maxProperties",
        "dependentSchemas",
        "dependentRequired",
        "dependencies",
    }
)
IMPLICIT_ARRAY_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "items",
        "prefixItems",
        "additionalItems",
        "unevaluatedItems",
        "contains",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minContains",
        "maxContains",
    }
)
IMPLICIT_STRING_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "pattern",
        "minLength",
        "maxLength",
        "format",
        "contentEncoding",
        "contentMediaType",
        "contentSchema",
    }
)
IMPLICIT_NUMBER_KEYWORDS: Final[frozenset[str]] = frozenset(
    {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"}
)


def _infer_implicit_type(schema: dict[str, Any]) -> str | None:
    """Infer an implicit JSON Schema `type` for a schema that omits `type` entirely.

    Omitting `type` is valid JSON Schema -- the schema is still constrained by
    whichever type-specific keywords it declares (e.g. a schema with only
    `properties` is implicitly object-shaped). Checked in order: object,
    array, string, number. `integer` is never inferred, since numeric
    keywords (`minimum`, `multipleOf`, etc.) apply equally to `integer` and
    `number`, and `number` is the non-narrowing, safer approximation.

    :param schema: the schema fragment to inspect (already known to lack a `type` keyword)
    :type schema: dict[str, Any]
    :return: an inferred JSON Schema type name, or None if no type-specific keyword is present
    :rtype: str | None
    """
    if schema.keys() & IMPLICIT_OBJECT_KEYWORDS:
        return "object"
    if schema.keys() & IMPLICIT_ARRAY_KEYWORDS:
        return "array"
    if schema.keys() & IMPLICIT_STRING_KEYWORDS:
        return "string"
    if schema.keys() & IMPLICIT_NUMBER_KEYWORDS:
        return "number"
    return None


def get_known_jsonschema_keywords(validator_cls: type) -> set[str]:
    """
    Programmatically derive the set of JSON Schema keywords recognized by `jsonschema`.

    Combines:
      - Draft-07's meta-schema `properties` -- a flat, self-contained set
        covering both annotation keywords (`description`, `title`,
        `examples`) and assertion keywords (`pattern`, `minLength`, `enum`).
        Later drafts split these into separate vocabularies, so Draft-07's
        meta-schema is used as a stable baseline for annotation keywords.
      - `validator_cls.VALIDATORS.keys()` -- covers assertion keywords
        specific to (or introduced after) the draft actually in use, e.g.
        `prefixItems`, `dependentRequired`, `unevaluatedProperties`,
        `contentSchema`, which purely-annotation-focused Draft-07 doesn't
        itself define as "assertions".

    :param validator_cls: appropriate `jsonschema` validator for the schema.
    :type validator_cls: type
    :return: set of valid jsonschema keywords
    :rtype: set[str]
    """
    keywords = set(Draft7Validator.META_SCHEMA["properties"].keys())
    keywords |= set(validator_cls.VALIDATORS.keys())
    return keywords - REF_AND_IDENTITY_KEYWORDS


@cache
def _metadata_keys_for(validator_cls: type) -> frozenset[str]:
    """Retrieve JSON Schema keywords that can be copied into StructField.metadata.

    Comprises all JSON Schema keywords, minus ref/identity and structural/compositional
    keywords already handled elsewhere.

    Used for validating any user-supplied `extra_metadata_keywords`.

    See `JSONSchemaToPySpark._build_metadata` for the function that creates the metadata.

    :param validator_cls: relevant `jsonschema` validator class for the schema
    :type validator_cls: type
    :return: the set of keywords eligible for copying into field metadata
    :rtype: frozenset[str]
    """
    known = get_known_jsonschema_keywords(validator_cls)
    return frozenset(known - STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS)


def _decimal_places(value: float) -> int:
    """Number of digits after the decimal point needed to represent `value` exactly.

    :param value: the numeric value to inspect (typically a JSON Schema `multipleOf`)
    :type value: float
    :return: number of fractional digits needed to represent `value` exactly
    :rtype: int
    """
    exponent = Decimal(str(value)).as_tuple().exponent
    if isinstance(exponent, int):
        return max(-exponent, 0)
    # exponent is 'n'/'N'/'F' for NaN/Infinity -- not valid here
    return 0


def _infer_type_from_enum(values: list[Any]) -> DataType:
    """Infer the PySpark type of an `enum` keyword's value list.

    :param values: enum values
    :type values: list[Any]
    :return: the inferred type of the enum
    :rtype: DataType
    """
    if not values:
        return StringType()
    if all(isinstance(v, bool) for v in values):
        return BooleanType()
    # booleans are a subtype of int (wtf?!), so this exclusion is required
    if all(isinstance(v, int) and not isinstance(v, bool) for v in values):
        return LongType()
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
        return DoubleType()
    return StringType()


class JSONSchemaToPySparkError(ValueError):
    """Raised when a JSON Schema construct cannot be converted."""


class InvalidJSONSchemaError(JSONSchemaToPySparkError):
    """Raised when the input document is not a valid JSON Schema."""


@dataclass
class ConversionContext:
    """The context for a specific call to convert a schema.

    :param validator_cls: relevant `jsonschema` validator class for the schema
    :type validator_cls: type
    :param extra_metadata_keywords: user-supplied extra keywords to consider for field metadata
    :type extra_metadata_keywords: frozenset[str]
    """

    validator_cls: type
    extra_metadata_keywords: frozenset[str] = field(default_factory=frozenset)

    @property
    def metadata_keys(self) -> frozenset[str]:
        """Metadata keys for this particular JSON Schema draft.

        :return: the set of keywords eligible for copying into field metadata for this draft
        :rtype: frozenset[str]
        """
        return _metadata_keys_for(self.validator_cls)

    @property
    def allowed_extra_metadata_keywords(self) -> frozenset[str]:
        """Subset of valid `extra_metadata_keywords`.

        Comprises keywords that are either recognised standard JSON Schema keywords for this draft, or
        a vendor extension keyword prefixed with `x-`.

        :return: the valid subset of `extra_metadata_keywords`
        :rtype: frozenset[str]
        """
        return frozenset(k for k in self.extra_metadata_keywords if k in self.metadata_keys or k.startswith("x-"))

    @property
    def invalid_extra_metadata_keywords(self) -> frozenset[str]:
        """Subset of `extra_metadata_keywords` that will be ignored.

        Keywords that are not standard keywords (including those that are in the ref/identity and structural
        sets) and don't start with `x-`.

        :return: the invalid subset of `extra_metadata_keywords`
        :rtype: frozenset[str]
        """
        return self.extra_metadata_keywords - self.allowed_extra_metadata_keywords


class JSONSchemaToPySpark(BaseModel):
    """
    Converts fully-dereferenced JSON Schema documents to PySpark StructType schemas.

    The input schema must:
      - already be dereferenced: no `$ref` (including references into
        external JSON Schema documents) and no `allOf` may remain anywhere
        in the document. Use
        `jsonschema_to_pyspark.dereferencing.dereference_schema()` first if
        your schema uses either.
      - declare its dialect via a top-level `$schema` keyword.
      - be a valid JSON schema, successfully validated against its meta-schema.

    Example:
        >>> from jsonschema_to_pyspark.dereferencing import dereference_schema
        >>> dereferenced = dereference_schema(my_json_schema_dict, additional_resources=my_resources)
        >>> converter = JSONSchemaToPySpark()
        >>> spark_schema = converter.convert(dereferenced)
    """

    model_config = ConfigDict(
        # Allow `pyspark.sql.types.DataType` instances in fields
        arbitrary_types_allowed=True,
        # freeze config on class instantiation
        frozen=True,
    )

    format_map: frozendict[str, DataType] = Field(
        default_factory=lambda: DEFAULT_FORMAT_MAP,
        description=(
            "Mapping of JSON Schema `format` values to PySpark DataTypes. "
            "User-supplied mapping entries take precedence over the defaults."
        ),
    )
    treat_unknown_as_string: bool = Field(
        default=True,
        description=(
            "Treat unrecognised and unsupported JSON schema constructs (including "
            "boolean `true`/`false` schemas) as StringType. If false, unrecognized/"
            "unsupported constructs will raise an error instead."
        ),
    )
    extra_metadata_keywords: frozenset[str] = Field(
        default_factory=frozenset,
        description=(
            "Additional keywords to copy into field metadata, on top of the "
            "default `title`/`description`. Only takes effect for keywords "
            "that are either recognised standard JSON Schema keywords for "
            "the schema's draft (see `get_known_jsonschema_keywords`) or "
            "that begin with 'x-' (vendor extensions); any other value is "
            "logged as a warning and discarded during `convert()`, since "
            "arbitrary/unknown keywords can't be distinguished from typos."
        ),
    )

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: Any) -> frozendict[str, DataType] | Any:  # noqa: ANN401
        """Merge user-supplied format mapping overrides into the default JSONSchema to PySpark type map.

        :param value: user-supplied mapping of formats <==> pyspark types
        :type value: dict[str, DataType]
        :return: merged mapping
        :rtype: frozendict[str, DataType]
        """
        if value and isinstance(value, (dict, frozendict)):
            return frozendict({**DEFAULT_FORMAT_MAP, **value})
        # let god sort 'em out!
        return value

    @field_validator("extra_metadata_keywords", mode="before")
    @classmethod
    def _coerce_extra_metadata_keywords(cls, value: Any) -> frozenset[str] | Any:  # noqa: ANN401
        """Coerce the extra_metadata_keywords value into a frozenset.

        :param value: extra metadata keywords supplied in args
        :type value: set[str] | list[str] | tuple[str, ...]
        :return: coerced extra metadata keywords
        :rtype: frozenset[str]
        """
        if isinstance(value, frozenset):
            return value
        if isinstance(value, (set, list, tuple)):
            return frozenset(value)
        return value

    def _build_context(self, validator_cls: type) -> ConversionContext:
        """Build the ConversionContext for a conversion.

        :param validator_cls: the `jsonschema` validator class in effect for the schema being converted
        :type validator_cls: type
        :return: a new ConversionContext consistent with this converter instance's configuration
        :rtype: ConversionContext
        """
        return ConversionContext(validator_cls=validator_cls, extra_metadata_keywords=self.extra_metadata_keywords)

    # core function: read and convert JSON schema ==> PySpark StructType
    def convert(self, schema: dict[str, Any]) -> StructType:
        """Convert a fully-dereferenced JSON Schema document into a PySpark StructType.

        :param schema: the fully-dereferenced JSON Schema document to convert
        :type schema: dict[str, Any]
        :return: the equivalent PySpark StructType
        :rtype: StructType
        :raises InvalidJSONSchemaError: if the schema has no top-level '$schema' keyword
        :raises JSONSchemaToPySparkError: if the schema's root type isn't 'object', if it still
            contains unresolved `$ref`/`allOf`, or if it doesn't resolve to a StructType
        """
        if not schema.get("$schema"):
            err_msg = (
                "Input JSON Schema is missing a '$schema' keyword. JSONSchemaToPySpark requires schemas "
                "to explicitly declare their dialect via '$schema'; it will not assume a default."
            )
            raise InvalidJSONSchemaError(err_msg)

        validator_cls = validator_for(schema)
        ctx = self._build_context(validator_cls)

        if ctx.invalid_extra_metadata_keywords:
            logger.warning(
                "Ignoring invalid extra_metadata_keywords %s: these are neither recognised JSON Schema "
                "keywords for the detected draft (%s) nor vendor extensions prefixed with 'x-'.",
                sorted(ctx.invalid_extra_metadata_keywords),
                validator_cls.__name__,
            )

        if schema.get("type") not in (None, "object"):
            err_msg = f"Root schema must be of type 'object' to map to a StructType, got: {schema.get('type')!r}"
            raise JSONSchemaToPySparkError(err_msg)

        data_type = self._convert_type(schema, ctx)
        if isinstance(data_type, StructType):
            return data_type

        err_msg = (
            "Root JSON Schema did not resolve to a StructType.\nThis can happen if it has no "
            "'properties' but does have 'patternProperties' or 'additionalProperties', which map to "
            "MapType instead. PySpark requires a StructType at the top level of the table schema."
        )
        raise JSONSchemaToPySparkError(err_msg)

    # entry points
    def convert_from_string(self, schema_str: str) -> StructType:
        """Import a JSON Schema as a string and convert it to a PySpark structure.

        :param schema_str: JSON string representing a schema
        :type schema_str: str
        :return: pyspark version of the schema
        :rtype: StructType
        """
        return self.convert(json.loads(schema_str))

    def convert_from_file(self, path: str) -> StructType:
        """Import a JSON Schema from a file and convert it to a PySpark structure.

        :param path: path to the schema
        :type path: str
        :return: pyspark version of the schema
        :rtype: StructType
        """
        loader = json.loads if path.endswith(".json") else yaml.safe_load

        return self.convert(loader(Path(path).read_bytes()))

    # dereferencing guard
    @staticmethod
    def _reject_unresolved_references(schema: dict[str, Any]) -> None:
        """Guard against un-dereferenced schemas reaching the type-dispatch logic.

        `JSONSchemaToPySpark` expects a fully dereferenced schema as input --
        see `jsonschema_to_pyspark.dereferencing.dereference_schema`. `$ref`
        (including references into external JSON Schema documents) and
        `allOf` resolution are intentionally *not* performed here.

        :param schema: the schema fragment to check for unresolved `$ref`/`allOf`
        :type schema: dict[str, Any]
        :return: None
        :rtype: None
        :raises JSONSchemaToPySparkError: if `schema` still contains `$ref` or `allOf`
        """
        if "$ref" in schema:
            err_msg = (
                f"Encountered an unresolved $ref {schema['$ref']!r}. JSONSchemaToPySpark requires a fully "
                "dereferenced schema -- this includes references to external JSON Schema documents. Use "
                "`jsonschema_to_pyspark.dereferencing.dereference_schema()` to resolve all $refs before "
                "calling `convert()`."
            )
            raise JSONSchemaToPySparkError(err_msg)
        if "allOf" in schema:
            err_msg = (
                "Encountered an unmerged 'allOf'. JSONSchemaToPySpark requires a fully dereferenced schema "
                "with 'allOf' already merged. Use `jsonschema_to_pyspark.dereferencing.dereference_schema()` "
                "before calling `convert()`."
            )
            raise JSONSchemaToPySparkError(err_msg)

    def _build_metadata(self, schema: dict[str, Any], ctx: ConversionContext) -> dict[str, Any]:
        """Build a StructField.metadata dict for `schema`.

        By default, only `title` (nested under "jsonschema") and
        `description` (set as the "comment" key, which is standard for field definitions) are copied.
        `ctx.allowed_extra_metadata_keywords` may add further keywords --
        invalid entries in `extra_metadata_keywords` have already been
        logged and excluded by `convert()` when `ctx` was built.

        Neither the "jsonschema" key nor the "comment" key -- nor the
        metadata dict itself -- is populated unless there's actual data to
        put in it, so a schema with no matching keywords produces `{}`.

        :param schema: the schema fragment to build metadata from
        :type schema: dict[str, Any]
        :param ctx: the active conversion context
        :type ctx: ConversionContext
        :return: the StructField metadata dict, possibly empty
        :rtype: dict[str, Any]
        """
        keys = DEFAULT_METADATA_KEYWORDS | ctx.allowed_extra_metadata_keywords
        jsonschema_metadata = {k: schema[k] for k in keys if k in schema}

        metadata: dict[str, Any] = {}
        if jsonschema_metadata:
            metadata["jsonschema"] = jsonschema_metadata
        if "description" in schema:
            metadata["comment"] = schema["description"]
        return metadata

    # Core type conversion
    def _convert_type(self, schema: dict[str, Any] | bool, ctx: ConversionContext) -> DataType:  # noqa: FBT001
        """Convert a schema fragment (object or boolean schema) to a PySpark DataType.

        :param schema: the schema fragment to convert
        :type schema: dict[str, Any] | bool
        :param ctx: the active conversion context
        :type ctx: ConversionContext
        :return: the converted PySpark DataType
        :rtype: DataType
        :raises JSONSchemaToPySparkError: if `schema` still contains `$ref`/`allOf`, or is an
            unrepresentable boolean schema and `treat_unknown_as_string` is False
        """
        if isinstance(schema, bool):
            return self._convert_boolean_schema(schema)

        self._reject_unresolved_references(schema)
        return self._dispatch_type(schema, ctx)

    def _convert_property(
        self,
        prop_schema: dict[str, Any] | bool,  # noqa: FBT001
        ctx: ConversionContext,
    ) -> tuple[DataType, dict[str, Any]]:
        """Convert an object property's schema to a (DataType, metadata) pair.

        :param prop_schema: the property's schema fragment
        :type prop_schema: dict[str, Any] | bool
        :param ctx: the active conversion context
        :type ctx: ConversionContext
        :return: a tuple of the converted PySpark DataType and the StructField metadata dict
        :rtype: tuple[DataType, dict[str, Any]]
        :raises JSONSchemaToPySparkError: if `prop_schema` still contains `$ref`/`allOf`, or is an
            unrepresentable boolean schema and `treat_unknown_as_string` is False
        """
        if isinstance(prop_schema, bool):
            return self._convert_boolean_schema(prop_schema), {}

        self._reject_unresolved_references(prop_schema)
        data_type = self._dispatch_type(prop_schema, ctx)
        metadata = self._build_metadata(prop_schema, ctx)
        return data_type, metadata

    def _convert_boolean_schema(self, schema: bool) -> DataType:  # noqa: FBT001
        """Convert a JSON Schema boolean schema to a PySpark type.

        This is where a literal `true`/`false` is used as a schema, not where a field has "type": "boolean".

        `true` means "any value is valid" (an unconstrained/"any" type);
        `false` means "no value is ever valid" (an unsatisfiable/"never" type).

        Neither has a PySpark equivalent, so this follows the same
        `treat_unknown_as_string` contract as every other unrepresentable
        construct in the converter.

        :param schema: the boolean schema value (`True` or `False`)
        :type schema: bool
        :return: StringType, as a lossy fallback approximation
        :rtype: DataType
        :raises JSONSchemaToPySparkError: if `treat_unknown_as_string` is False
        """
        if self.treat_unknown_as_string:
            logger.warning(
                "Boolean JSON Schema `%s` has no PySpark equivalent; falling back to StringType.",
                schema,
            )
            return StringType()
        msg = (
            f"Boolean JSON Schema `{schema}` has no PySpark equivalent "
            "(unconstrained 'any' type or unsatisfiable 'never' type)."
        )
        raise JSONSchemaToPySparkError(msg)

    def _dispatch_type(self, schema: dict[str, Any], ctx: ConversionContext) -> DataType:  # noqa: C901
        """Route a schema fragment to the appropriate per-`type` conversion method.

        :param schema: the schema fragment to convert
        :type schema: dict[str, Any]
        :param ctx: the active conversion context
        :type ctx: ConversionContext
        :return: the converted PySpark DataType
        :rtype: DataType
        :raises JSONSchemaToPySparkError: if `schema` is an unsupported multi-type union or
            an unrecognized/unsupported construct, and `treat_unknown_as_string` is False
        """
        json_type = schema.get("type")

        if isinstance(json_type, list):
            non_null_types = [t for t in json_type if t != "null"]
            if len(non_null_types) == 1:
                # all the same non-null type
                json_type = non_null_types[0]
            elif not non_null_types:
                # a list of nulls (?!)
                return NullType()
            elif self.treat_unknown_as_string:
                # more than one type present, collapse down to string type
                logger.warning("Collapsing multi-type union %r to StringType.", json_type)
                return StringType()
            else:
                # more than one type present, PANIC!!
                err_msg = f"Unsupported multi-type union: {json_type!r}"
                raise JSONSchemaToPySparkError(err_msg)

        if json_type is None and "enum" in schema:
            return _infer_type_from_enum(schema["enum"])

        if json_type is None:
            inferred_type = _infer_implicit_type(schema)
            if inferred_type is not None:
                logger.debug(
                    "No 'type' keyword present; inferred implicit type %r from schema keywords.",
                    inferred_type,
                )
                json_type = inferred_type

        if json_type == "object":
            return self._convert_object(schema, ctx)
        if json_type == "array":
            return self._convert_array(schema, ctx)
        if json_type == "string":
            return self._convert_string(schema)
        if json_type == "integer":
            return self._convert_integer(schema)
        if json_type == "number":
            return self._convert_number(schema)
        if json_type == "boolean":
            return BooleanType()
        if json_type == "null":
            return NullType()

        for combiner in ("oneOf", "anyOf"):
            if combinations := schema.get(combiner):
                logger.warning(
                    "Approximating '%s' by using only its first branch for type inference; PySpark has no union type.",
                    combiner,
                )
                return self._convert_type(combinations[0], ctx)

        for ignored_keyword in ("not", "if", "then", "else"):
            if ignored_keyword in schema:
                logger.warning(
                    "Ignoring unsupported conditional keyword '%s'; it has no effect on the resulting PySpark type.",
                    ignored_keyword,
                )

        if self.treat_unknown_as_string:
            logger.warning(
                "Unrecognized/unsupported schema %r; falling back to StringType.",
                schema,
            )
            return StringType()
        msg = f"Unsupported/unknown schema type: {schema!r}"
        raise JSONSchemaToPySparkError(msg)

    def _convert_object(self, schema: dict[str, Any], ctx: ConversionContext) -> DataType:
        """Convert an object schema (`type: object`) to a StructType or MapType.

        :param schema: the object schema fragment
        :type schema: dict[str, Any]
        :param ctx: the active conversion context
        :type ctx: ConversionContext
        :return: a StructType if `properties` is present, otherwise a MapType (or empty
            StructType if the object is fully open with no dynamic-key schema either)
        :rtype: DataType
        """
        properties: dict[str, Any] = schema.get("properties", {})
        required: list[str] = schema.get("required", [])
        pattern_properties: dict[str, Any] = schema.get("patternProperties", {})
        additional_properties = schema.get("additionalProperties")

        if properties:
            if pattern_properties or isinstance(additional_properties, dict):
                logger.warning(
                    "Object schema declares both fixed 'properties' and dynamic 'patternProperties' / "
                    "'additionalProperties'; PySpark's StructType requires fixed field names, so the "
                    "dynamic keys are ignored."
                )
            fields: list[StructField] = []
            for prop_name, prop_schema in properties.items():
                data_type, metadata = self._convert_property(prop_schema, ctx)
                nullable = prop_name not in required
                fields.append(
                    StructField(
                        name=prop_name,
                        dataType=data_type,
                        nullable=nullable,
                        metadata=metadata,
                    )
                )
            return StructType(fields)

        # No fixed properties: fall back to MapType for dynamic/open objects.
        value_schema: dict[str, Any] | bool | None = None
        if pattern_properties:
            if len(pattern_properties) > 1:
                logger.warning(
                    "Multiple 'patternProperties' patterns found; only the first pattern's schema is "
                    "used as the MapType value type, since PySpark's MapType has a single value type."
                )
            value_schema = next(iter(pattern_properties.values()))
        elif isinstance(additional_properties, dict):
            value_schema = additional_properties

        if value_schema is not None:
            value_type = self._convert_type(value_schema, ctx)
            return MapType(keyType=StringType(), valueType=value_type, valueContainsNull=True)

        logger.warning(
            "Object schema has no 'properties', 'patternProperties', or schema-valued "
            "'additionalProperties'; converting to an empty StructType()."
        )
        return StructType([])

    def _convert_array(self, schema: dict[str, Any], ctx: ConversionContext) -> ArrayType:
        """Convert an array schema (`type: array`) to an ArrayType.

        :param schema: the array schema fragment
        :type schema: dict[str, Any]
        :param ctx: the active conversion context
        :type ctx: ConversionContext
        :return: an ArrayType whose element type is derived from `items`/`prefixItems`
            (only the first element/slot schema is used, since PySpark arrays are homogeneous)
        :rtype: ArrayType
        """
        if "prefixItems" in schema:
            prefix_items = schema["prefixItems"]
            logger.warning(
                "Array schema uses 'prefixItems' (tuple validation); PySpark arrays are homogeneous, so "
                "only the first tuple slot's type is used as the array's element type."
            )
            element_schema = prefix_items[0] if prefix_items else {}
        else:
            items_schema = schema.get("items", {})
            if isinstance(items_schema, list):
                logger.warning(
                    "Array schema uses tuple-style 'items' (list of schemas); PySpark arrays are "
                    "homogeneous, so only the first item's type is used as the array's element type."
                )
                element_schema = items_schema[0] if items_schema else {}
            else:
                element_schema = items_schema

        element_type = self._convert_type(element_schema, ctx) if element_schema else StringType()
        return ArrayType(elementType=element_type, containsNull=True)

    def _convert_string(self, schema: dict[str, Any]) -> DataType:
        """Convert a string schema (`type: string`) to a PySpark type based on its `format`.

        :param schema: the string schema fragment
        :type schema: dict[str, Any]
        :return: the type from `format_map` matching the schema's `format` keyword, or
            StringType if `format` is absent or unrecognized
        :rtype: DataType
        """
        fmt = schema.get("format")
        return self.format_map.get(fmt, StringType()) if fmt else StringType()

    @staticmethod
    def _convert_integer(schema: dict[str, Any]) -> DataType:
        """Convert a numeric value into an integer or a long, depending on its size.

        :param schema: schema scrap
        :type schema: dict[str, Any]
        :return: appropriate type
        :rtype: DataType
        """
        minimum = schema.get("minimum", schema.get("exclusiveMinimum"))
        maximum = schema.get("maximum", schema.get("exclusiveMaximum"))
        if minimum is not None and maximum is not None and minimum >= INT32_MIN and maximum <= INT32_MAX:
            return IntegerType()
        return LongType()

    @staticmethod
    def _convert_number(schema: dict[str, Any]) -> DataType:
        """Convert a number schema (`type: number`) to a DecimalType or DoubleType.

        :param schema: the number schema fragment
        :type schema: dict[str, Any]
        :return: a DecimalType derived from `multipleOf` if present, otherwise DoubleType
        :rtype: DataType
        """
        if "multipleOf" in schema and isinstance(schema["multipleOf"], (int, float)):
            scale = _decimal_places(schema["multipleOf"])
            # precision 38 is the maximum allowed by Spark's DecimalType
            # precision must be greater than or equal to scale
            return DecimalType(precision=38, scale=scale)
        return DoubleType()
