"""Tests for `cdm_data_loaders.core.transforms` and `TransformSettingsMixin`."""

from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from frozendict import frozendict
from pydantic import ValidationError

from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    RecordTransformFn,
    TransformRegistry,
)
from cdm_data_loaders.core.transforms import (
    resolve_schema_map,
    route_and_coerce_record,
    validate_routed_output,
    validate_table_transforms,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from tests.helpers import register_temp_module


def _identity_items_transform(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Route a single record to the items table."""
    return {"items": [record]}


def _custom_table_transform(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Route a single record to the custom table."""
    return {"custom": [record]}


def _split_parent_child_transform(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Filter empty records and split non-empty records into parent and child tables."""
    if record.get("total_count") == 0:
        return {}
    return {
        "parent": [{"id": record["id"]}],
        "child": [{"parent_id": record["id"], "tag": t} for t in record.get("tags", [])],
        "empty_table": [],
    }


ITEMS_SCHEMA = pa.schema([pa.field("id", pa.string())])
ITEMS_EXTRA_SCHEMA = pa.schema([pa.field("id", pa.string()), pa.field("extra", pa.int64())])
DEFAULT_SCHEMA = pa.schema([pa.field("a", pa.int64())])
OVERRIDE_SCHEMA = pa.schema([pa.field("b", pa.string())])
PARENT_SCHEMA = pa.schema([pa.field("id", pa.string()), pa.field("note", pa.string())])
CHILD_SCHEMA = pa.schema([pa.field("parent_id", pa.string()), pa.field("tag", pa.string())])

ITEMS_SCHEMA_REGISTRY = ArrowSchemaRegistry(registry={"items": ITEMS_SCHEMA})
ITEMS_EXTRA_SCHEMA_REGISTRY = ArrowSchemaRegistry(registry={"items": ITEMS_EXTRA_SCHEMA})
DEFAULT_SCHEMA_REGISTRY = ArrowSchemaRegistry(registry={"items": DEFAULT_SCHEMA})
OVERRIDE_SCHEMA_REGISTRY = ArrowSchemaRegistry(registry={"custom": OVERRIDE_SCHEMA})
SPLIT_SCHEMA_REGISTRY = ArrowSchemaRegistry(
    registry={"parent": PARENT_SCHEMA, "child": CHILD_SCHEMA, "empty_table": PARENT_SCHEMA}
)
ITEMS_TRANSFORM_REGISTRY = TransformRegistry(registry={"items": _identity_items_transform})


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param({}, None, id="unschematized_default"),
        pytest.param(
            {"default_pyarrow_schema": DEFAULT_SCHEMA},
            DEFAULT_SCHEMA_REGISTRY,
            id="default_pyarrow_schema_without_transform",
        ),
        pytest.param(
            {
                "transform": _identity_items_transform,
                "default_pyarrow_schema": DEFAULT_SCHEMA,
            },
            None,
            id="transform_skips_default_pyarrow_schema",
        ),
        pytest.param(
            {
                "transform": _custom_table_transform,
                "unreconciled_output_schemas": OVERRIDE_SCHEMA_REGISTRY,
                "default_pyarrow_schema": DEFAULT_SCHEMA,
            },
            OVERRIDE_SCHEMA_REGISTRY,
            id="unreconciled_output_schemas_takes_precedence",
        ),
    ],
)
def test_resolve_schema_map_pass_precedence(
    kwargs: dict[str, Any],
    expected: ArrowSchemaRegistry | None,
) -> None:
    """resolve_schema_map prefers unreconciled_output_schemas and skips default_pyarrow_schema when transform is set."""
    assert resolve_schema_map("items", **kwargs) == expected


def test_resolve_schema_map_fail_unmapped_default_table_without_transform_fn() -> None:
    """resolve_schema_map rejects an unreconciled_output_schemas that omits default_table when transform is None."""
    with pytest.raises(
        ValueError,
        match="unreconciled_output_schemas for 'items' must include 'items' when transform is None",
    ):
        resolve_schema_map(
            "items",
            unreconciled_output_schemas=OVERRIDE_SCHEMA_REGISTRY,
        )


@pytest.mark.parametrize(
    ("schema_map", "expected"),
    [
        pytest.param(
            ITEMS_EXTRA_SCHEMA_REGISTRY,
            {"items": [{"id": "r1", "extra": 5}]},
            id="coerced_with_schema_map",
        ),
        pytest.param(
            None,
            {"items": [{"id": "r1", "extra": 5.0, "undeclared": "drop"}]},
            id="uncoerced_without_schema_map",
        ),
    ],
)
def test_route_and_coerce_record_pass_untransformed_and_coerced(
    schema_map: ArrowSchemaRegistry | None,
    expected: dict[str, list[dict[str, Any]]],
) -> None:
    """route_and_coerce_record routes untransformed records and optionally coerces for PyArrow."""
    record = {"id": "r1", "extra": 5.0, "undeclared": "drop"}
    assert (
        route_and_coerce_record(
            record,
            "items",
            schema_map=schema_map,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param(
            {"id": "p0", "total_count": 0},
            {},
            id="filtered_record_returns_empty_dict",
        ),
        pytest.param(
            {"id": "p1", "total_count": 2, "tags": ["a", "b"]},
            {
                "parent": [{"id": "p1"}],
                "child": [
                    {"parent_id": "p1", "tag": "a"},
                    {"parent_id": "p1", "tag": "b"},
                ],
            },
            id="multi_table_split_and_empty_table_dropped",
        ),
    ],
)
def test_route_and_coerce_record_pass_filter_transform_and_split(
    record: dict[str, Any],
    expected: dict[str, list[dict[str, Any]]],
) -> None:
    """route_and_coerce_record supports filtering, 1-to-N row transforms, and multi-table splits."""
    assert (
        route_and_coerce_record(
            record,
            "raw",
            transform=_split_parent_child_transform,
            schema_map=SPLIT_SCHEMA_REGISTRY,
        )
        == expected
    )


VALID_ROUTED_OUTPUT_CASES = [
    pytest.param({}, {}, id="empty_mapping"),
    pytest.param({"items": [], "other": ()}, {}, id="only_empty_sequences"),
    pytest.param({"items": [{"id": "1"}]}, {"items": [{"id": "1"}]}, id="single_table_list"),
    pytest.param({"  items \n": [{"id": "1"}]}, {"items": [{"id": "1"}]}, id="whitespace_padded_table_name"),
    pytest.param({"_rejected": [{"id": "1"}]}, {"_rejected": [{"id": "1"}]}, id="rejected_table_key"),
    pytest.param(
        {"  _rejected \n": [{"id": "1"}]},
        {"_rejected": [{"id": "1"}]},
        id="rejected_table_key_whitespace",
    ),
    pytest.param(
        {"items": ({"id": "1"}, {"id": "2"})},
        {"items": [{"id": "1"}, {"id": "2"}]},
        id="tuple_of_dicts_coerced_to_list",
    ),
    pytest.param(
        {"parent": [{"id": "p1"}], "child": [{"id": "c1"}, {"id": "c2"}], "empty": []},
        {"parent": [{"id": "p1"}], "child": [{"id": "c1"}, {"id": "c2"}]},
        id="multi_table_with_empty_stripped",
    ),
    pytest.param(
        frozendict({"items": [{"id": "1"}]}),
        {"items": [{"id": "1"}]},
        id="frozendict_mapping",
    ),
]

INVALID_ROUTED_OUTPUT_CASES = [
    pytest.param(
        None,
        "transform_fn must return a mapping of table names to sequences of row dicts",
        id="not_mapping_none",
    ),
    pytest.param(
        ["not_a_mapping"],
        "transform_fn must return a mapping of table names to sequences of row dicts",
        id="not_mapping_list",
    ),
    pytest.param(
        "not_a_mapping",
        "transform_fn must return a mapping of table names to sequences of row dicts",
        id="not_mapping_str",
    ),
    pytest.param(
        123,
        "transform_fn must return a mapping of table names to sequences of row dicts",
        id="not_mapping_int",
    ),
    pytest.param(
        {123: [{"id": 1}]},
        "transform_fn output keys must be non-empty table names",
        id="non_str_key_int",
    ),
    pytest.param(
        {None: [{"id": 1}]},
        "transform_fn output keys must be non-empty table names",
        id="non_str_key_none",
    ),
    pytest.param(
        {"": [{"id": 1}]},
        "transform_fn output keys must be non-empty table names",
        id="empty_key",
    ),
    pytest.param(
        {"   \t\n": [{"id": 1}]},
        "transform_fn output keys must be non-empty table names",
        id="whitespace_key",
    ),
    pytest.param(
        {"items": None},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_none",
    ),
    pytest.param(
        {"items": 123},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_int",
    ),
    pytest.param(
        {"items": {"a"}},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_set",
    ),
    pytest.param(
        {"items": "not_a_sequence_of_dicts"},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_str",
    ),
    pytest.param(
        {"items": b"not_a_sequence_of_dicts"},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_bytes",
    ),
    pytest.param(
        {"items": bytearray(b"not_a_sequence_of_dicts")},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_bytearray",
    ),
    pytest.param(
        {"items": {"id": 1}},
        "transform_fn rows for table 'items' must be a sequence of dicts",
        id="rows_dict",
    ),
    pytest.param(
        {"items": ["not_a_dict"]},
        "transform_fn rows for table 'items' must be non-empty dicts",
        id="row_not_dict_str",
    ),
    pytest.param(
        {"items": [{"id": 1}, 123]},
        "transform_fn rows for table 'items' must be non-empty dicts",
        id="row_not_dict_after_valid",
    ),
    pytest.param(
        {"items": [{}]},
        "transform_fn rows for table 'items' must be non-empty dicts",
        id="row_empty_dict",
    ),
    pytest.param(
        {"items": [{"id": 1}, {}]},
        "transform_fn rows for table 'items' must be non-empty dicts",
        id="row_empty_dict_after_valid",
    ),
    pytest.param(
        {"": "not_a_seq"},
        "transform_fn output keys must be non-empty table names\n"
        "transform_fn rows for table '' must be a sequence of dicts",
        id="empty_key_and_bad_rows_sequence",
    ),
    pytest.param(
        {123: [{}]},
        "transform_fn output keys must be non-empty table names\n"
        "transform_fn rows for table 123 must be non-empty dicts",
        id="non_str_key_and_empty_row_dict",
    ),
    pytest.param(
        {
            "": [{"id": 1}],
            "valid_before": [{"id": 2}],
            123: "bad_seq",
            "_rejected": [{"id": 3}],
            "  _rejected\n": [{}],
            "bad_rows": 456,
            "non_dict_row": [{"id": 4}, "bad"],
            "empty_dict_row": [{}],
            "valid_after": [{"id": 5}],
        },
        (
            "transform_fn output keys must be non-empty table names\n"
            "transform_fn output keys must be non-empty table names\n"
            "transform_fn rows for table 123 must be a sequence of dicts\n"
            "transform_fn rows for table '  _rejected\\n' must be non-empty dicts\n"
            "transform_fn rows for table 'bad_rows' must be a sequence of dicts\n"
            "transform_fn rows for table 'non_dict_row' must be non-empty dicts\n"
            "transform_fn rows for table 'empty_dict_row' must be non-empty dicts"
        ),
        id="all_errors_collected_across_entries",
    ),
]


@pytest.mark.parametrize(("raw_routed", "expected"), VALID_ROUTED_OUTPUT_CASES)
def test_validate_routed_output_pass(
    raw_routed: object,
    expected: dict[str, list[dict[str, Any]]],
) -> None:
    """validate_routed_output strips table name whitespace, drops empty row sequences, and returns row lists."""
    assert validate_routed_output(raw_routed) == expected


@pytest.mark.parametrize(("raw_routed", "expected_error"), INVALID_ROUTED_OUTPUT_CASES)
def test_validate_routed_output_fail(raw_routed: object, expected_error: str) -> None:
    """validate_routed_output collects all validation errors in raw_routed and raises ValueError."""
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        validate_routed_output(raw_routed)
    assert str(exc_info.value) == expected_error


@pytest.mark.parametrize(("raw_routed", "expected"), VALID_ROUTED_OUTPUT_CASES)
def test_route_and_coerce_record_pass_validates_routed_output(
    raw_routed: Any,  # noqa: ANN401
    expected: dict[str, list[dict[str, Any]]],
) -> None:
    """route_and_coerce_record accepts all valid transform_fn outputs accepted by validate_routed_output."""
    transform = lambda _r: raw_routed  # noqa: E731
    assert route_and_coerce_record({"id": "1"}, "items", transform=transform) == expected


@pytest.mark.parametrize(("return_value", "expected_error"), INVALID_ROUTED_OUTPUT_CASES)
def test_route_and_coerce_record_fail_invalid_transform_fn_output(
    return_value: Any,  # noqa: ANN401
    expected_error: str,
) -> None:
    """route_and_coerce_record propagates all ValueError paths from validate_routed_output."""
    transform = lambda _r: return_value  # noqa: E731
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        route_and_coerce_record({"id": "1"}, "items", transform=transform)
    assert str(exc_info.value) == expected_error


def test_route_and_coerce_record_pass_unmapped_output_table_left_uncoerced() -> None:
    """route_and_coerce_record coerces mapped tables and leaves unmapped output tables as uncoerced dicts."""
    transform = lambda r: {"items": [r], "unmapped": [r]}  # noqa: E731
    record = {"id": "1", "extra": 5.0, "undeclared": "keep_on_unmapped"}
    assert route_and_coerce_record(
        record,
        "items",
        transform=transform,
        schema_map=ITEMS_EXTRA_SCHEMA_REGISTRY,
    ) == {
        "items": [{"id": "1", "extra": 5}],
        "unmapped": [record],
    }


@pytest.mark.parametrize(
    ("settings", "valid_tables"),
    [
        pytest.param(SimpleNamespace(), (), id="missing_table_transforms_attribute"),
        pytest.param(SimpleNamespace(table_transforms=None), (), id="table_transforms_is_none"),
        pytest.param(
            SimpleNamespace(table_transforms=ITEMS_TRANSFORM_REGISTRY),
            ["items"],
            id="exact_table_match",
        ),
        pytest.param(
            SimpleNamespace(table_transforms=ITEMS_TRANSFORM_REGISTRY),
            {"items", "other"},
            id="subset_of_valid_tables",
        ),
        pytest.param(
            SimpleNamespace(
                table_transforms=TransformRegistry(
                    registry={"items": _identity_items_transform, "custom": _custom_table_transform}
                )
            ),
            ("items", "custom", "extra"),
            id="multiple_transforms_all_valid",
        ),
    ],
)
def test_validate_table_transforms_pass(
    settings: Any,  # noqa: ANN401
    valid_tables: Iterable[str],
) -> None:
    """validate_table_transforms succeeds when table_transforms is absent or all transform keys are in valid_tables."""
    validate_table_transforms(settings, iter(valid_tables))


@pytest.mark.parametrize(
    ("table_transforms", "valid_tables", "match"),
    [
        pytest.param(
            ITEMS_TRANSFORM_REGISTRY,
            ["other"],
            r"^Unknown table names in transforms_module: items$",
            id="single_unknown_table",
        ),
        pytest.param(
            TransformRegistry(
                registry={
                    "zeta": _identity_items_transform,
                    "items": _identity_items_transform,
                    "alpha": _custom_table_transform,
                }
            ),
            ["items"],
            r"^Unknown table names in transforms_module: alpha, zeta$",
            id="multiple_unknown_tables_sorted",
        ),
        pytest.param(
            ITEMS_TRANSFORM_REGISTRY,
            (),
            r"^Unknown table names in transforms_module: items$",
            id="empty_valid_tables",
        ),
    ],
)
def test_validate_table_transforms_fail_unknown_tables(
    table_transforms: TransformRegistry,
    valid_tables: Iterable[str],
    match: str,
) -> None:
    """validate_table_transforms raises ValueError with sorted unknown table names."""
    settings = SimpleNamespace(table_transforms=table_transforms)
    with pytest.raises(ValueError, match=match):
        validate_table_transforms(settings, valid_tables)


@pytest.mark.parametrize(
    (
        "use_transforms_module",
        "use_output_schemas_module",
        "expected_transforms",
        "expected_schema_map",
        "expected_item_transform",
    ),
    [
        pytest.param(False, False, None, None, None, id="unconfigured_defaults"),
        pytest.param(
            True,
            False,
            ITEMS_TRANSFORM_REGISTRY,
            None,
            _identity_items_transform,
            id="transforms_module_only",
        ),
        pytest.param(
            False,
            True,
            None,
            ITEMS_SCHEMA_REGISTRY,
            None,
            id="output_schemas_module_only",
        ),
        pytest.param(
            True,
            True,
            ITEMS_TRANSFORM_REGISTRY,
            ITEMS_SCHEMA_REGISTRY,
            _identity_items_transform,
            id="both_modules_configured",
        ),
    ],
)
def test_transform_settings_mixin_pass_loads_transforms(
    test_defaults: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    use_transforms_module: bool,
    use_output_schemas_module: bool,
    expected_transforms: TransformRegistry | None,
    expected_schema_map: ArrowSchemaRegistry | None,
    expected_item_transform: RecordTransformFn | None,
) -> None:
    """TransformSettingsMixin loads TABLE_TRANSFORMS and OUTPUT_SCHEMAS when configured."""
    mod_name = register_temp_module(
        monkeypatch,
        "_test_settings_transforms_mod",
        TABLE_TRANSFORMS={"items": _identity_items_transform},
        OUTPUT_SCHEMAS={"items": ITEMS_SCHEMA},
    )
    transforms_module = mod_name if use_transforms_module else None
    output_schemas_module = mod_name if use_output_schemas_module else None

    settings = JsonlExtractSettings(
        **test_defaults,
        dataset_name="test_ds",
        table_name="items",
        transforms_module=transforms_module,
        output_schemas_module=output_schemas_module,
    )

    assert settings.transforms_module == transforms_module
    assert settings.output_schemas_module == output_schemas_module
    assert settings.table_transforms == expected_transforms
    assert settings.schema_map == expected_schema_map
    assert settings.get_table_transform("items") is expected_item_transform
    assert settings.get_table_transform("other") is None


@pytest.mark.parametrize(
    ("module_attrs", "use_transforms_module", "use_output_schemas_module", "match"),
    [
        pytest.param(
            {"OUTPUT_SCHEMAS": {"other": ITEMS_SCHEMA}},
            False,
            True,
            "unreconciled_output_schemas for 'items' must include 'items' when transform is None",
            id="unmapped_default_table_without_transform",
        ),
        pytest.param(
            {"TABLE_TRANSFORMS": {"other": _identity_items_transform}},
            True,
            False,
            "Unknown table names in transforms_module: other",
            id="unknown_table_in_transforms_module",
        ),
    ],
)
def test_transform_settings_mixin_fail_invalid_table_mapping(
    test_defaults: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    module_attrs: dict[str, Any],
    use_transforms_module: bool,
    use_output_schemas_module: bool,
    match: str,
) -> None:
    """TransformSettingsMixin rejects unmapped output schemas and unknown transform table names."""
    mod_name = register_temp_module(monkeypatch, "_test_invalid_settings_transforms_mod", **module_attrs)
    with pytest.raises(ValidationError, match=match):
        JsonlExtractSettings(
            **test_defaults,
            dataset_name="test_ds",
            table_name="items",
            transforms_module=mod_name if use_transforms_module else None,
            output_schemas_module=mod_name if use_output_schemas_module else None,
        )


def test_transform_settings_mixin_pass_table_transforms_is_immutable(
    test_defaults: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TransformSettingsMixin.table_transforms cannot be mutated in place."""
    mod_name = register_temp_module(
        monkeypatch,
        "_test_immutable_settings_transforms_mod",
        TABLE_TRANSFORMS={"items": _identity_items_transform},
    )

    configured_settings = JsonlExtractSettings(
        **test_defaults,
        dataset_name="test_ds",
        table_name="items",
        transforms_module=mod_name,
    )
    assert configured_settings.table_transforms is not None
    with pytest.raises(TypeError):
        configured_settings.table_transforms.registry["injected"] = _identity_items_transform  # type: ignore[index]
    assert "injected" not in configured_settings.table_transforms.registry


@pytest.mark.parametrize(
    ("loader_file_format", "expected_use_pyarrow", "expected_schema_map"),
    [
        pytest.param("parquet", True, ITEMS_SCHEMA_REGISTRY, id="parquet"),
        pytest.param("iceberg", True, ITEMS_SCHEMA_REGISTRY, id="iceberg"),
        pytest.param("jsonl", False, None, id="jsonl"),
    ],
)
def test_transform_settings_mixin_pass_use_pyarrow_respects_loader_file_format(
    test_defaults: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    loader_file_format: str,
    expected_use_pyarrow: bool,
    expected_schema_map: ArrowSchemaRegistry | None,
) -> None:
    """TransformSettingsMixin sets use_pyarrow False and leaves schema_map None when loader_file_format is jsonl."""
    mod_name = register_temp_module(
        monkeypatch,
        f"_test_format_transforms_mod_{loader_file_format}",
        TABLE_TRANSFORMS={"items": _identity_items_transform},
        OUTPUT_SCHEMAS={"items": ITEMS_SCHEMA},
    )
    settings = JsonlExtractSettings(
        **test_defaults,
        dataset_name="test_ds",
        table_name="items",
        loader_file_format=loader_file_format,
        transforms_module=mod_name,
        output_schemas_module=mod_name,
    )
    assert settings.use_pyarrow is expected_use_pyarrow
    assert settings.schema_map == expected_schema_map
    assert settings.table_transforms == ITEMS_TRANSFORM_REGISTRY
