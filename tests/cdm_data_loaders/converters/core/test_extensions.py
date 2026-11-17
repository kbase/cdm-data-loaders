"""Closed extension contracts and custom registry isolation."""

from collections.abc import Callable
from decimal import Decimal, localcontext
from functools import partial
from typing import Any, Final

import pytest
from jsonschema.exceptions import SchemaError, ValidationError

from cdm_data_loaders.converters.core.extensions import (
    DEFAULT_EXTENSIONS,
    ExtensionError,
    ExtensionRegistry,
    Extensions,
    ExtensionSpec,
)
from cdm_data_loaders.converters.core.ir import TypedNode
from cdm_data_loaders.converters.core.ir_values import Value, freeze_value
from cdm_data_loaders.validation.jsonschema.metaschema import get_xsv_config_schema


@pytest.mark.parametrize(
    ("amount", "divisor"),
    [
        (Decimal("0.3"), 0.1),
        (0.3, Decimal("0.1")),
        (0.3, 0.1),
        (Decimal("0.3"), Decimal("0.1")),
        (1, 0.1),
        (0, Decimal("0.1")),
        (Decimal("-0.3"), 0.1),
        (Decimal("123456789012345678901234567890.3"), 0.1),
        (Decimal("1e1000"), Decimal("1e-1000")),
    ],
    ids=[
        "decimal-float",
        "float-decimal",
        "floats",
        "decimals",
        "integer",
        "zero",
        "negative",
        "many-digits",
        "exponents",
    ],
)
def test_extensions_pass_decimal_multiple_of_float(
    amount: float | Decimal,
    divisor: float | Decimal,
) -> None:
    """Fractional validation accepts exact multiples regardless of Decimal context precision."""
    registry = ExtensionRegistry((ExtensionSpec("x-amount", {"type": "number", "multipleOf": divisor}),))
    with localcontext() as context:
        context.prec = 2
        extensions = Extensions({"x-amount": amount}, registry)
        assert extensions["x-amount"] is amount


@pytest.mark.parametrize(
    ("divisor", "invalid"),
    [
        (0.1, Decimal("0.31")),
        (Decimal("0.1"), 0.31),
        (0.1, 0.31),
        (Decimal("0.1"), Decimal("0.31")),
        (0.1, Decimal("-0.31")),
        (0.1, Decimal("123456789012345678901234567890.31")),
        (Decimal("1e-1000"), Decimal("1e-1001")),
    ],
    ids=[
        "float-divisor-decimal-value",
        "decimal-divisor-float-value",
        "float-divisor-float-value",
        "decimal-divisor-decimal-value",
        "negative",
        "many-digits",
        "exponents",
    ],
)
def test_extensions_fail_decimal_multiple_of_float(
    divisor: float | Decimal,
    invalid: float | Decimal,
) -> None:
    """Fractional validation rejects non-multiples regardless of Decimal context precision."""
    registry = ExtensionRegistry((ExtensionSpec("x-amount", {"type": "number", "multipleOf": divisor}),))
    with localcontext() as context:
        context.prec = 2
        with pytest.raises(ExtensionError, match=r"Invalid x-amount at \[\]: .*not a multiple"):
            Extensions({"x-amount": invalid}, registry)


@pytest.mark.parametrize(
    "instance_type", [int, float, Decimal], ids=["integer-instance", "float-instance", "decimal-instance"]
)
@pytest.mark.parametrize("schema_type", [int, float, Decimal], ids=["integer-schema", "float-schema", "decimal-schema"])
@pytest.mark.parametrize(
    ("keyword", "accepted"),
    [
        ("multipleOf", "4"),
        ("minimum", "2"),
        ("maximum", "2"),
        ("exclusiveMinimum", "3"),
        ("exclusiveMaximum", "1"),
    ],
    ids=["multiple-of", "minimum", "maximum", "exclusive-minimum", "exclusive-maximum"],
)
def test_extensions_pass_mixed_numeric_keywords(
    instance_type: Callable[[str], float | Decimal],
    schema_type: Callable[[str], float | Decimal],
    keyword: str,
    accepted: str,
) -> None:
    """All numeric keywords accept valid mixed representations consistently."""
    registry = ExtensionRegistry((ExtensionSpec("x-amount", {"type": "number", keyword: schema_type("2")}),))
    amount = instance_type(accepted)
    assert Extensions({"x-amount": amount}, registry)["x-amount"] is amount


@pytest.mark.parametrize(
    "instance_type", [int, float, Decimal], ids=["integer-instance", "float-instance", "decimal-instance"]
)
@pytest.mark.parametrize("schema_type", [int, float, Decimal], ids=["integer-schema", "float-schema", "decimal-schema"])
@pytest.mark.parametrize(
    ("keyword", "rejected"),
    [
        ("multipleOf", "3"),
        ("minimum", "1"),
        ("maximum", "3"),
        ("exclusiveMinimum", "2"),
        ("exclusiveMaximum", "2"),
    ],
    ids=["multiple-of", "minimum", "maximum", "exclusive-minimum", "exclusive-maximum"],
)
def test_extensions_fail_mixed_numeric_keywords(
    instance_type: Callable[[str], float | Decimal],
    schema_type: Callable[[str], float | Decimal],
    keyword: str,
    rejected: str,
) -> None:
    """All numeric keywords reject invalid mixed representations consistently."""
    registry = ExtensionRegistry((ExtensionSpec("x-amount", {"type": "number", keyword: schema_type("2")}),))
    with pytest.raises(ExtensionError) as error:
        Extensions({"x-amount": instance_type(rejected)}, registry)
    assert isinstance(error.value.__cause__, ValidationError)
    assert error.value.__cause__.validator == keyword


@pytest.mark.parametrize(
    ("amount", "boundary"),
    [(Decimal("0.1"), 0.1), (0.1, Decimal("0.1"))],
    ids=["decimal-instance", "float-instance"],
)
@pytest.mark.parametrize(
    "keyword",
    ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"],
    ids=["minimum", "maximum", "exclusive-minimum", "exclusive-maximum"],
)
def test_extensions_pass_exact_fractional_bounds(
    amount: float | Decimal, boundary: float | Decimal, keyword: str
) -> None:
    """Decimal and JSON float boundaries compare as equal without binary rounding."""
    registry = ExtensionRegistry((ExtensionSpec("x-amount", {"type": "number", keyword: boundary}),))
    if keyword.startswith("exclusive"):
        with pytest.raises(ExtensionError):
            Extensions({"x-amount": amount}, registry)
    else:
        assert Extensions({"x-amount": amount}, registry)["x-amount"] is amount


def _nested_numeric_ref_registry() -> ExtensionRegistry:
    """Build an ExtensionRegistry whose x-amount item property resolves a local numeric $ref."""
    schema = {
        "$defs": {
            "amount": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "number",
                "multipleOf": 0.1,
                "minimum": 0.1,
                "maximum": Decimal("0.3"),
            }
        },
        "type": "array",
        "items": {"type": "object", "properties": {"amount": {"$ref": "#/$defs/amount"}}},
    }
    return ExtensionRegistry((ExtensionSpec("x-amount", schema),))


def test_extensions_pass_nested_numeric_refs() -> None:
    """Local refs retain exact arithmetic and preserve Decimal identity in valid nested items."""
    registry = _nested_numeric_ref_registry()
    amount = Decimal("0.3")
    extensions = Extensions({"x-amount": [{"amount": amount}]}, registry)
    assert extensions["x-amount"] == ({"amount": amount},)
    assert extensions["x-amount"][0]["amount"] is amount


def test_extensions_fail_nested_numeric_refs() -> None:
    """Local refs report nested validation error paths when exact arithmetic fails."""
    registry = _nested_numeric_ref_registry()
    with pytest.raises(ExtensionError, match=r"Invalid x-amount at \[0, 'amount'\]: .*not a multiple") as error:
        Extensions({"x-amount": [{"amount": Decimal("0.31")}]}, registry)
    assert isinstance(error.value.__cause__, ValidationError)
    assert list(error.value.__cause__.path) == [0, "amount"]


@pytest.mark.parametrize("schema", [True, False], ids=["true-schema", "false-schema"])
@pytest.mark.parametrize(
    "value",
    [Decimal("0.3"), 1.0, True, "text", None, ({"amount": Decimal("0.3")},)],
    ids=["decimal", "float", "boolean", "string", "null", "nested"],
)
def test_extensions_pass_boolean_schemas(value: Value, schema: bool) -> None:
    """True and false schemas remain unconditional for all finite payloads."""
    registry = ExtensionRegistry((ExtensionSpec("x-value", schema=schema),))
    if schema:
        assert Extensions({"x-value": value}, registry)["x-value"] == freeze_value(value)
    else:
        with pytest.raises(ExtensionError, match="False schema"):
            Extensions({"x-value": value}, registry)


@pytest.mark.parametrize(
    ("schema", "value", "valid"),
    [
        ({"type": "number"}, Decimal("0.3"), True),
        ({"type": "number"}, True, False),
        ({"type": "integer"}, False, False),
        ({"type": "integer"}, 2.0, True),
        ({"type": "integer"}, 0.3, False),
        ({"type": "integer"}, Decimal(2), False),
        ({"type": "boolean"}, 1, False),
        ({"multipleOf": 0.1, "minimum": 1, "maximum": 2}, True, True),
        ({"multipleOf": 0.1, "minimum": 1, "maximum": 2}, "text", True),
        ({"type": "number"}, "0.3", False),
        ({"enum": [0.1]}, Decimal("0.1"), True),
        ({"const": {"amount": Decimal("0.1")}}, {"amount": 0.1}, True),
    ],
    ids=[
        "decimal-number",
        "boolean-not-number",
        "boolean-not-integer",
        "integral-float",
        "fractional-float",
        "decimal-integer-unchanged",
        "integer-not-boolean",
        "boolean-ignores-numeric",
        "string-ignores-numeric",
        "string-not-number",
        "numeric-enum",
        "nested-numeric-const",
    ],
)
def test_extensions_pass_custom_schema_types(schema: dict[str, Any], value: Value, valid: bool) -> None:
    """Numeric normalization preserves type checks and literal schema comparisons."""
    registry = ExtensionRegistry((ExtensionSpec("x-value", schema),))
    if valid:
        assert Extensions({"x-value": value}, registry)["x-value"] == freeze_value(value)
    else:
        with pytest.raises(ExtensionError):
            Extensions({"x-value": value}, registry)


def _spec_with_minimum(value: float | Decimal) -> ExtensionSpec:
    """Construct an ExtensionSpec with a numeric minimum bound."""
    return ExtensionSpec("x-amount", {"minimum": value})


def _extensions_with_amount(value: float | Decimal) -> Extensions:
    """Construct an Extensions payload for an unconditional x-amount namespace."""
    registry = ExtensionRegistry((ExtensionSpec("x-amount", schema=True),))
    return Extensions({"x-amount": value}, registry)


@pytest.mark.parametrize(
    "target",
    [_spec_with_minimum, _extensions_with_amount],
    ids=["spec-schema", "extensions-payload"],
)
@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), Decimal("NaN"), Decimal("Infinity")],
    ids=["float-nan", "float-infinity", "decimal-nan", "decimal-infinity"],
)
def test_extensions_fail_nonfinite_numbers(
    value: float | Decimal,
    target: Callable[[float | Decimal], object],
) -> None:
    """Non-finite numbers fail before numeric normalization in schemas and payloads."""
    with pytest.raises(ValueError, match="finite"):
        target(value)


@pytest.mark.parametrize(
    "values",
    [
        {"x-dlt": {"data_type": "decimal", "precision": 20, "scale": 2, "timezone": None}},
        {"x-iceberg": {"field_id": 1, "required": False, "initial_default": [None, Decimal("1.20")]}},
        {"x-xsv-config": {"x-delimiter": "\t", "x-null-cols": ["a"]}},
        {"x-file-glob": "*.tsv", "x-delimiter": ",", "x-dlt-prefix": "id:", "x-dlt-split": ";", "x-pii": True},
        {},
    ],
    ids=["dlt", "iceberg-default", "xsv", "discovered", "empty"],
)
def test_extensions_pass_builtin(values: dict[str, Any]) -> None:
    """Builtin namespaces validate exact internal key spellings."""
    extensions = Extensions(values)
    assert list(extensions) == list(values)
    assert len(extensions) == len(values)
    assert TypedNode(type="any", extensions=extensions).extensions is extensions


@pytest.mark.parametrize(
    "values",
    [
        {"title": "bad"},
        {"x-unknown": None},
        {"x-pii": "yes"},
        {"x-dlt": {"bogus": True}},
        {"x-iceberg": {"bogus": 1}},
        {"x-xsv-config": {}},
        {"x-delimiter": "::"},
        {"x-dlt": []},
    ],
    ids=["namespace", "unknown", "wrong-type", "dlt-key", "iceberg-key", "empty-xsv", "delimiter", "array"],
)
def test_extensions_fail_invalid(values: dict[str, Any]) -> None:
    """Unknown keys and invalid payloads fail on ordinary node construction."""
    with pytest.raises(ExtensionError):
        TypedNode(type="any", extensions=values)


def test_extension_registry_pass_custom_copy_isolation() -> None:
    """Custom arrays and null are allowed only under an explicit schema."""
    schema = {"type": "array", "items": {"type": ["string", "null"]}}
    registry = DEFAULT_EXTENSIONS.register(ExtensionSpec("x-custom", schema))
    values = {"x-custom": ["data", None]}
    extensions = Extensions(values, registry)
    schema["items"]["type"].append("number")
    values["x-custom"].append("changed")
    assert extensions["x-custom"] == ("data", None)
    with pytest.raises(ExtensionError):
        registry.validate({"x-custom": [1]})
    with pytest.raises(TypeError):
        extensions.payload["x-custom"] = ()
    custom_dlt = registry.extend_payload("x-dlt", {"x-custom": {"type": ["array", "null"]}})
    assert custom_dlt.validate({"x-dlt": {"x-custom": None}}) == {"x-dlt": {"x-custom": None}}
    with pytest.raises(ExtensionError):
        DEFAULT_EXTENSIONS.validate({"x-dlt": {"x-custom": None}})


@pytest.mark.parametrize("namespace", ["", "x-", "ordinary"], ids=["empty", "prefix-only", "no-prefix"])
def test_extension_spec_fail_namespace(namespace: str) -> None:
    """Only named x-prefixed namespaces can be registered."""
    with pytest.raises(ExtensionError):
        ExtensionSpec(namespace, schema=True)


DUPLICATE_TEST_SPEC: Final[ExtensionSpec] = ExtensionSpec("x-test", schema=True)


@pytest.mark.parametrize(
    ("call", "match"),
    [
        pytest.param(
            partial(ExtensionRegistry, (DUPLICATE_TEST_SPEC, DUPLICATE_TEST_SPEC)),
            "Duplicate extension namespace",
            id="duplicate-in-constructor",
        ),
        pytest.param(
            partial(ExtensionRegistry((DUPLICATE_TEST_SPEC,)).register, DUPLICATE_TEST_SPEC),
            "Already registered",
            id="duplicate-on-register",
        ),
    ],
)
def test_extension_registry_fail_duplicates(call: Callable[[], object], match: str) -> None:
    """Duplicate declarations fail without explicit replacement."""
    with pytest.raises(ExtensionError, match=match):
        call()


def test_extension_registry_pass_register_replace() -> None:
    """Explicit replacement updates an existing namespace contract."""
    registry = ExtensionRegistry((DUPLICATE_TEST_SPEC,))
    assert registry.register(DUPLICATE_TEST_SPEC, replace=True) == registry


@pytest.mark.parametrize(
    ("schema", "error_type", "match"),
    [
        (1, ExtensionError, "validation schemas"),
        ({"type": "bogus"}, SchemaError, None),
    ],
    ids=["non-schema", "invalid-schema"],
)
def test_extension_spec_fail_invalid_schemas(schema: object, error_type: type[Exception], match: str | None) -> None:
    """Invalid extension validation schemas fail at spec construction."""
    with pytest.raises(error_type, match=match):
        ExtensionSpec("x-bad", schema)


def test_extension_registry_fail_invalid_entries() -> None:
    """Registry entries must be ExtensionSpec instances."""
    with pytest.raises(TypeError, match="entries"):
        ExtensionRegistry(({},))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("namespace", "properties", "match"),
    [
        ("x-pii", {"extra": True}, "Not an object"),
        ("x-dlt", {"data_type": True}, "already registered"),
        ("x-missing", {"extra": True}, "Unregistered"),
    ],
    ids=["non-object-payload", "duplicate-property", "unregistered-namespace"],
)
def test_extension_registry_fail_extend_payload(namespace: str, properties: dict[str, Any], match: str) -> None:
    """Payload extensions fail on non-object schemas, collisions, and missing namespaces."""
    with pytest.raises(ExtensionError, match=match):
        DEFAULT_EXTENSIONS.extend_payload(namespace, properties)


def test_extensions_pass_exact_xsv_schema_and_mapping_api() -> None:
    """The exact existing XSV schema is reused, and mapping methods remain callable."""
    xsv_config_schema = get_xsv_config_schema()
    spec = next(spec for spec in DEFAULT_EXTENSIONS.specs if spec.namespace == "x-xsv-config")
    extensions = Extensions({"x-pii": True})
    assert list(extensions.values()) == [True]
    assert list(extensions.items()) == [("x-pii", True)]
    assert spec.schema["additionalProperties"] == xsv_config_schema["additionalProperties"]
    assert tuple(spec.schema["properties"]) == tuple(xsv_config_schema["properties"])
    with pytest.raises(KeyError):
        extensions["x-missing"]
    with pytest.raises(TypeError):
        spec.schema["properties"]["x-delimiter"]["type"] = "integer"


def test_extension_spec_pass_lazy_loader_defers_until_accessed() -> None:
    """A callable schema loader is invoked only on first schema access and cached thereafter."""
    calls = 0

    def load_schema() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"type": "string", "minLength": 1}

    spec = ExtensionSpec("x-lazy", load_schema)
    registry = ExtensionRegistry((spec, ExtensionSpec("x-other", {"type": "boolean"})))
    assert calls == 0
    assert Extensions({"x-other": True}, registry)["x-other"] is True
    assert calls == 0
    assert Extensions({"x-lazy": "ok"}, registry)["x-lazy"] == "ok"
    assert calls == 1
    assert spec.schema == {"type": "string", "minLength": 1}
    assert calls == 1
    assert spec == ExtensionSpec("x-lazy", {"type": "string", "minLength": 1})
    assert spec != "x-lazy"
    assert hash(spec) == hash(ExtensionSpec("x-lazy", {"type": "string", "minLength": 1}))


@pytest.mark.parametrize(
    ("bad_schema", "error_type"),
    [
        pytest.param(1, ExtensionError, id="non-schema-from-loader"),
        pytest.param({"type": "bogus"}, SchemaError, id="invalid-schema-from-loader"),
    ],
)
def test_extension_spec_fail_lazy_loader_invalid_schema(bad_schema: object, error_type: type[Exception]) -> None:
    """Invalid schemas returned by a lazy loader fail when `.schema` is accessed."""
    spec = ExtensionSpec("x-lazy", lambda: bad_schema)  # type: ignore[arg-type, return-value]
    with pytest.raises(error_type):
        _ = spec.schema


DLT_BOOLEAN_HINTS: Final[tuple[str, ...]] = (
    "primary_key",
    "unique",
    "foreign_key",
    "sort",
    "cluster",
    "partition",
    "merge_key",
    "row_key",
    "root_key",
    "variant",
    "timezone",
)
DLT_BOOLEAN_HINT_IDS: Final[tuple[str, ...]] = (
    "primary",
    "unique",
    "foreign",
    "sort",
    "cluster",
    "partition",
    "merge",
    "row",
    "root",
    "variant",
    "timezone",
)


@pytest.mark.parametrize("hint", DLT_BOOLEAN_HINTS, ids=DLT_BOOLEAN_HINT_IDS)
def test_extensions_pass_dlt_boolean_hints(hint: str) -> None:
    """Each observed dlt column hint accepts boolean values."""
    assert Extensions({"x-dlt": {hint: True}})["x-dlt"] == {hint: True}


@pytest.mark.parametrize("hint", DLT_BOOLEAN_HINTS, ids=DLT_BOOLEAN_HINT_IDS)
def test_extensions_fail_dlt_nonboolean_hints(hint: str) -> None:
    """Each observed dlt column hint rejects non-boolean values."""
    with pytest.raises(ExtensionError):
        Extensions({"x-dlt": {hint: "yes"}})
