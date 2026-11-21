"""Tests for `cdm_data_loaders.core.transforms` and `TransformSettingsMixin`."""

import functools
import operator
from collections.abc import Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, Final, Union

import pyarrow as pa
import pytest
from frozendict import frozendict
from pydantic import BaseModel, SecretStr, ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.coercion.pydantic_to_arrow import coerce_pydantic_record_for_pyarrow
from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA, make_rejected_row
from cdm_data_loaders.core.file_items import as_file_item
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    RecordTransformFn,
    TableTransformFn,
    TransformContext,
    TransformRegistry,
)
from cdm_data_loaders.core.transforms.introspection import (
    _annotation_capabilities,
    _annotation_is_transform_context,
    _safe_type_hints,
    _unwrap_partial_target,
    accepts_pyarrow_table,
    accepts_record_dict,
    inspect_transform_signature,
)
from cdm_data_loaders.core.transforms.routing import (
    align_and_cast_table,
    compose_transforms,
    emit_raw_table,
    invoke_transform,
    make_record_router,
    merge_routed_outputs,
    resolve_schema_map,
    route_and_coerce_record,
    route_and_coerce_table,
    validate_routed_output,
)
from cdm_data_loaders.core.transforms.settings_mixin import (
    LockedTransformHooksMixin,
    TransformSettingsMixin,
    reconcile_multi_table_schema_map,
    validate_table_transforms,
)
from cdm_data_loaders.pipelines.jsonlines.routing import route_jsonl_lines
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.pipelines.xmltodict.processors import process_xml_file_to_dict
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from tests.helpers.cli_assertions import make_cts_settings, register_temp_module, settings_to_argv


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


SAMPLE_PA_TABLE = pa.table({"id": ["1", "2"]})

VALID_ROUTED_OUTPUT_CASES = [
    pytest.param({}, {}, id="empty_mapping"),
    pytest.param({"items": [], "other": ()}, {}, id="only_empty_sequences"),
    pytest.param({"items": ITEMS_SCHEMA.empty_table()}, {}, id="empty_pyarrow_table_stripped"),
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
        "transform_fn must return a mapping of table names to sequences of row dicts or pyarrow.Table",
        id="not_mapping_none",
    ),
    pytest.param(
        ["not_a_mapping"],
        "transform_fn must return a mapping of table names to sequences of row dicts or pyarrow.Table",
        id="not_mapping_list",
    ),
    pytest.param(
        "not_a_mapping",
        "transform_fn must return a mapping of table names to sequences of row dicts or pyarrow.Table",
        id="not_mapping_str",
    ),
    pytest.param(
        123,
        "transform_fn must return a mapping of table names to sequences of row dicts or pyarrow.Table",
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
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_none",
    ),
    pytest.param(
        {"items": 123},
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_int",
    ),
    pytest.param(
        {"items": {"a"}},
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_set",
    ),
    pytest.param(
        {"items": "not_a_sequence_of_dicts"},
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_str",
    ),
    pytest.param(
        {"items": b"not_a_sequence_of_dicts"},
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_bytes",
    ),
    pytest.param(
        {"items": bytearray(b"not_a_sequence_of_dicts")},
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_bytearray",
    ),
    pytest.param(
        {"items": {"id": 1}},
        "transform_fn rows for table 'items' must be a sequence of dicts or pyarrow.Table",
        id="rows_dict",
    ),
    pytest.param(
        {"items": SAMPLE_PA_TABLE.select([])},
        "transform_fn table for table 'items' must have at least one column",
        id="pyarrow_table_zero_columns_with_rows",
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
        "transform_fn rows for table '' must be a sequence of dicts or pyarrow.Table",
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
            "transform_fn rows for table 123 must be a sequence of dicts or pyarrow.Table\n"
            "transform_fn rows for table '  _rejected\\n' must be non-empty dicts\n"
            "transform_fn rows for table 'bad_rows' must be a sequence of dicts or pyarrow.Table\n"
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
    "lookup_table",
    [
        pytest.param("items", id="configured-table"),
        pytest.param("other", id="unconfigured-table"),
    ],
)
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
    lookup_table: str,
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
    expected_for_lookup = expected_item_transform if lookup_table == "items" else None
    assert settings.get_table_transforms(lookup_table) == ((expected_for_lookup,) if expected_for_lookup else ())
    assert settings.get_table_transform(lookup_table) is expected_for_lookup


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


type PolymorphicBatch = pa.Table | dict[str, Any]
type AnnotatedArrowTable = Annotated[pa.Table, "batch"]

RAW_SOURCED_ITEMS_SCHEMA: Final[pa.Schema] = pa.schema(
    [pa.field("id", pa.string(), nullable=False)],
    metadata={"comment": "Raw items"},
)
SOURCED_ITEMS_SCHEMA: Final[pa.Schema] = pa.schema(
    [pa.field("id", pa.string(), nullable=False), pa.field("source", pa.string(), nullable=False)],
    metadata={"comment": "Sourced items"},
)
SOURCED_ITEMS_SCHEMA_MAP: Final[ArrowSchemaRegistry] = ArrowSchemaRegistry(
    registry={"raw_items": RAW_SOURCED_ITEMS_SCHEMA, "items": SOURCED_ITEMS_SCHEMA}
)


def _unannotated_transform(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Route a record whose runtime annotations have been cleared to items.

    :param record: input record dictionary
    :type record: dict[str, Any]
    :return: routed mapping targeting ``items``
    :rtype: dict[str, list[dict[str, Any]]]
    """
    return {"items": [record]}


_unannotated_transform.__annotations__.clear()


def _arrow_only_transform(table: pa.Table, ctx: TransformContext) -> dict[str, pa.Table]:
    """Attach a ``source`` basename column to an Arrow table batch.

    :param table: input PyArrow table batch
    :type table: pa.Table
    :param ctx: transform context carrying ``file_name``
    :type ctx: TransformContext
    :return: routed mapping targeting ``items``
    :rtype: dict[str, pa.Table]
    """
    src_col = pa.repeat(pa.scalar(ctx.file_name or "", type=pa.string()), table.num_rows)
    return {"items": table.append_column("source", src_col)}


def _annotated_arrow_transform(table: AnnotatedArrowTable) -> dict[str, pa.Table]:
    """Route an ``Annotated`` ``pyarrow.Table`` batch to ``items``.

    :param table: input annotated PyArrow table batch
    :type table: AnnotatedArrowTable
    :return: routed mapping targeting ``items``
    :rtype: dict[str, pa.Table]
    """
    return {"items": table}


def _polymorphic_alias_transform(
    data: PolymorphicBatch, ctx: TransformContext
) -> dict[str, pa.Table | list[dict[str, Any]]]:
    """Route either an Arrow table or a record dict using a ``TypeAliasType`` union.

    :param data: input PyArrow table batch or record dictionary
    :type data: PolymorphicBatch
    :param ctx: transform context for the input table
    :type ctx: TransformContext
    :return: routed raw table mapping
    :rtype: dict[str, pa.Table | list[dict[str, Any]]]
    """
    return emit_raw_table(data, ctx)


def _dict_with_optional_non_ctx_param(
    record: dict[str, Any], uppercase: bool = True
) -> dict[str, list[dict[str, Any]]]:
    """Route a record with an optional boolean flag that must not receive ``TransformContext``.

    :param record: input record dictionary
    :type record: dict[str, Any]
    :param uppercase: whether to uppercase ``record["id"]``
    :type uppercase: bool
    :return: routed mapping targeting ``items``
    :rtype: dict[str, list[dict[str, Any]]]
    """
    val = str(record["id"]).upper() if uppercase else str(record["id"])
    return {"items": [{"id": val}]}


def _dict_with_optional_ctx_param(
    record: dict[str, Any],
    ctx: TransformContext | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Route a record with an optional ``TransformContext`` parameter.

    :param record: input record dictionary
    :type record: dict[str, Any]
    :param ctx: optional transform context
    :type ctx: TransformContext | None
    :return: routed mapping targeting ``items``
    :rtype: dict[str, list[dict[str, Any]]]
    """
    return {"items": [{"id": record["id"], "source": ctx.file_name if ctx else ""}]}


def _emit_items_and_rejected(
    table: pa.Table,
    ctx: TransformContext,
) -> dict[str, pa.Table | list[dict[str, Any]]]:
    """Emit ``items`` with ``source``, a ``_rejected`` row, and an unmapped dict table.

    :param table: input PyArrow table batch
    :type table: pa.Table
    :param ctx: transform context for the input table and source file
    :type ctx: TransformContext
    :return: multi-table routed outputs mixing ``pa.Table`` and ``list[dict[str, Any]]``
    :rtype: dict[str, pa.Table | list[dict[str, Any]]]
    """
    src = pa.repeat(pa.scalar(ctx.file_name or "", type=pa.string()), table.num_rows)
    return {
        "items": table.append_column("source", src),
        "_rejected": [make_rejected_row(ctx.table_name, "bad row", source_file=ctx.source_file)],
        "unmapped": [{"id": "u1"}],
    }


@pytest.mark.parametrize(
    ("fn", "expected_arrow", "expected_dict"),
    [
        pytest.param(None, False, True, id="none_transform"),
        pytest.param(_unannotated_transform, False, True, id="unannotated_fn"),
        pytest.param(_identity_items_transform, False, True, id="dict_only_fn"),
        pytest.param(_arrow_only_transform, True, False, id="arrow_table_only_fn"),
        pytest.param(_annotated_arrow_transform, True, False, id="annotated_arrow_table_fn"),
        pytest.param(emit_raw_table, True, True, id="polymorphic_emit_raw_table"),
        pytest.param(_polymorphic_alias_transform, True, True, id="type_alias_polymorphic_fn"),
        pytest.param(
            functools.partial(emit_raw_table, prefix="orig_"),
            True,
            True,
            id="partial_polymorphic_fn",
        ),
        pytest.param(
            compose_transforms((emit_raw_table, _arrow_only_transform)),
            True,
            True,
            id="composed_all_arrow_capable",
        ),
        pytest.param(
            compose_transforms((emit_raw_table, _identity_items_transform)),
            False,
            True,
            id="composed_mixed_arrow_and_dict",
        ),
    ],
)
def test_accepts_pyarrow_table_and_record_dict(
    fn: TableTransformFn | None,
    expected_arrow: bool,
    expected_dict: bool,
) -> None:
    """accepts_pyarrow_table and accepts_record_dict inspect annotations, partials, aliases, and compositions."""
    assert accepts_pyarrow_table(fn) is expected_arrow
    assert accepts_record_dict(fn) is expected_dict


@pytest.mark.parametrize(
    ("data", "prefix", "expected"),
    [
        pytest.param(SAMPLE_PA_TABLE, "raw_", {"raw_id_mapping": SAMPLE_PA_TABLE}, id="arrow_table_default_prefix"),
        pytest.param({"id": "1"}, "orig_", {"orig_id_mapping": [{"id": "1"}]}, id="dict_record_custom_prefix"),
    ],
)
def test_emit_raw_table_pass_arrow_and_dict(
    data: pa.Table | dict[str, Any],
    prefix: str,
    expected: dict[str, pa.Table | list[dict[str, Any]]],
) -> None:
    """emit_raw_table prefixes ctx.table_name for both pyarrow.Table and dict inputs."""
    ctx = TransformContext.for_source("id_mapping", "dir/sample.dat")
    assert emit_raw_table(data, ctx, prefix=prefix) == expected


def test_emit_raw_table_fail_empty_prefix() -> None:
    """emit_raw_table raises ValueError when prefix is empty."""
    ctx = TransformContext.for_source("id_mapping", "dir/sample.dat")
    with pytest.raises(ValueError, match=r"^prefix must be a non-empty string$"):
        emit_raw_table({"id": "1"}, ctx, prefix="")


@pytest.mark.parametrize(
    "already_aligned",
    [
        pytest.param(False, id="misaligned-casts-reorders-and-fills"),
        pytest.param(True, id="already-aligned-fast-path"),
    ],
)
def test_align_and_cast_table_casts_reorders_fills_missing_and_attaches_metadata(
    already_aligned: bool,
) -> None:
    """align_and_cast_table aligns mismatched tables and returns already-aligned tables unchanged."""
    target_schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False, metadata={"comment": "Identifier"}),
            pa.field("count", pa.int64(), nullable=True, metadata={"comment": "Count"}),
            pa.field("note", pa.string(), nullable=True),
        ],
        metadata={"comment": "Target table description"},
    )
    input_table = (
        pa.Table.from_pylist([{"id": "a1", "count": 3, "note": None}], schema=target_schema)
        if already_aligned
        else pa.table({"extra": ["drop"], "count": pa.array([3], type=pa.int32()), "id": ["a1"]})
    )
    aligned = align_and_cast_table(input_table, target_schema)
    assert aligned.schema.equals(target_schema, check_metadata=True)
    assert aligned.to_pylist() == [{"id": "a1", "count": 3, "note": None}]
    if already_aligned:
        assert aligned is input_table


def test_compose_transforms_fails_with_no_transforms() -> None:
    """compose_transforms throws an error without any transforms."""
    with pytest.raises(ValueError, match="transforms must contain at least one callable"):
        compose_transforms(())


def test_compose_transforms_returns_single_transform_as_is() -> None:
    """compose_transforms validates a single non-empty input and returns it."""
    assert compose_transforms((_identity_items_transform,)) is _identity_items_transform


def test_compose_transforms_merges_arrow_dict_and_mixed_outputs() -> None:
    """compose_transforms merges Arrow, dict, and mixed outputs in step order."""

    def _arrow_part_a(table: pa.Table) -> dict[str, pa.Table]:
        """Route ``table`` to ``same_arrow`` and ``mixed``.

        :param table: input PyArrow table batch
        :type table: pa.Table
        :return: routed Arrow table mapping
        :rtype: dict[str, pa.Table]
        """
        return {"same_arrow": table, "mixed": table}

    def _arrow_part_b(table: pa.Table) -> dict[str, pa.Table | list[dict[str, Any]]]:
        """Route ``table`` to ``same_arrow`` and a dict list to ``mixed``.

        :param table: input PyArrow table batch
        :type table: pa.Table
        :return: routed mapping mixing ``pa.Table`` and ``list[dict[str, Any]]``
        :rtype: dict[str, pa.Table | list[dict[str, Any]]]
        """
        return {"same_arrow": table, "mixed": [{"id": "from_dict"}]}

    composed = compose_transforms((_arrow_part_a, _arrow_part_b))
    input_table = pa.table({"id": ["r1"]})
    routed = route_and_coerce_table(input_table, "items", transform=composed, use_pyarrow=True)
    assert isinstance(routed["same_arrow"], pa.Table)
    assert routed["same_arrow"].to_pylist() == [{"id": "r1"}, {"id": "r1"}]
    assert routed["mixed"] == [{"id": "r1"}, {"id": "from_dict"}]


@pytest.mark.parametrize(
    ("record", "transform", "schema_map", "source_file", "expected"),
    [
        pytest.param(
            {"id": "abc"},
            _dict_with_optional_non_ctx_param,
            None,
            "nested/file.jsonl",
            {"items": [{"id": "ABC"}]},
            id="preserves_optional_non_context_default",
        ),
        pytest.param(
            {"id": "abc"},
            _dict_with_optional_ctx_param,
            None,
            "nested/file.jsonl",
            {"items": [{"id": "abc", "source": "file.jsonl"}]},
            id="passes_optional_transform_context",
        ),
        pytest.param(
            {"id": "abc", "extra": 99},
            _arrow_only_transform,
            ArrowSchemaRegistry(registry={"items": SOURCED_ITEMS_SCHEMA}),
            "sub/batch.tsv",
            {"items": [{"id": "abc", "source": "batch.tsv"}]},
            id="adapts_arrow_only_transform_for_single_record",
        ),
    ],
)
def test_route_and_coerce_record_pass_context_and_arrow_adaptation(
    record: dict[str, Any],
    transform: TableTransformFn,
    schema_map: ArrowSchemaRegistry | None,
    source_file: str,
    expected: dict[str, list[dict[str, Any]]],
) -> None:
    """route_and_coerce_record passes TransformContext when requested, preserves non-context defaults, and adapts Arrow transforms."""
    assert (
        route_and_coerce_record(
            record,
            "items",
            transform=transform,
            schema_map=schema_map,
            source_file=source_file,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("table", "default_table", "transform", "schema_map", "use_pyarrow", "expected_rows_by_table"),
    [
        pytest.param(
            ITEMS_SCHEMA.empty_table(),
            "items",
            None,
            None,
            True,
            {},
            id="empty_untransformed_table_dropped",
        ),
        pytest.param(
            SAMPLE_PA_TABLE,
            "items",
            None,
            None,
            False,
            {"items": [{"id": "1"}, {"id": "2"}]},
            id="untransformed_jsonl_mode_converts_to_dicts",
        ),
        pytest.param(
            SAMPLE_PA_TABLE,
            TransformContext.for_source("items", "dir/input.tsv.gz"),
            compose_transforms((emit_raw_table, _emit_items_and_rejected)),
            SOURCED_ITEMS_SCHEMA_MAP,
            True,
            {
                "raw_items": [{"id": "1"}, {"id": "2"}],
                "items": [{"id": "1", "source": "input.tsv.gz"}, {"id": "2", "source": "input.tsv.gz"}],
                "_rejected": [
                    {
                        "table_name": "items",
                        "source_file": "dir/input.tsv.gz",
                        "line_no": None,
                        "raw_record": None,
                        "error_detail": "bad row",
                    }
                ],
                "unmapped": [{"id": "u1"}],
            },
            id="composed_arrow_transform_with_schema_map_and_rejected",
        ),
        pytest.param(
            SAMPLE_PA_TABLE,
            TransformContext.for_source("items", "dir/input.tsv.gz"),
            compose_transforms((emit_raw_table, _emit_items_and_rejected)),
            None,
            False,
            {
                "raw_items": [{"id": "1"}, {"id": "2"}],
                "items": [{"id": "1", "source": "input.tsv.gz"}, {"id": "2", "source": "input.tsv.gz"}],
                "_rejected": [
                    {
                        "table_name": "items",
                        "source_file": "dir/input.tsv.gz",
                        "line_no": None,
                        "raw_record": None,
                        "error_detail": "bad row",
                    }
                ],
                "unmapped": [{"id": "u1"}],
            },
            id="composed_arrow_transform_in_jsonl_mode",
        ),
        pytest.param(
            pa.table({"id": ["p1"], "total_count": [1], "tags": [["x", "y"]]}),
            "raw",
            _split_parent_child_transform,
            SPLIT_SCHEMA_REGISTRY,
            True,
            {
                "parent": [{"id": "p1", "note": None}],
                "child": [{"parent_id": "p1", "tag": "x"}, {"parent_id": "p1", "tag": "y"}],
            },
            id="dict_only_transform_adapted_on_arrow_table",
        ),
    ],
)
def test_route_and_coerce_table_pass_arrow_dict_rejected_and_jsonl_modes(
    table: pa.Table,
    default_table: str | TransformContext,
    transform: TableTransformFn | None,
    schema_map: ArrowSchemaRegistry | None,
    use_pyarrow: bool,
    expected_rows_by_table: dict[str, list[dict[str, Any]]],
) -> None:
    """route_and_coerce_table handles untransformed tables, Arrow transforms, dict transforms, _rejected, and use_pyarrow=False."""
    routed = route_and_coerce_table(
        table,
        default_table,
        transform=transform,
        schema_map=schema_map,
        use_pyarrow=use_pyarrow,
    )
    assert set(routed) == set(expected_rows_by_table)
    for out_table, out_val in routed.items():
        if not use_pyarrow or out_table == "unmapped":
            assert isinstance(out_val, list)
            assert out_val == expected_rows_by_table[out_table]
        else:
            assert isinstance(out_val, pa.Table)
            if out_table == "_rejected":
                assert out_val.schema.equals(REJECTED_PYARROW_SCHEMA)
            elif schema_map is not None and out_table in schema_map.registry:
                assert out_val.schema.equals(schema_map.registry[out_table], check_metadata=True)
            assert out_val.to_pylist() == expected_rows_by_table[out_table]


def test_source_file_propagated_to_table_transforms_in_jsonlines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """route_jsonl_lines passes source_file into 2-arg TABLE_TRANSFORMS."""
    mod_name = register_temp_module(
        monkeypatch,
        "_test_reader_ctx_transforms_mod",
        TABLE_TRANSFORMS={"items": (emit_raw_table, _dict_with_optional_ctx_param)},
    )

    jsonl_dir = tmp_path / "jsonl_in"
    jsonl_dir.mkdir()
    jsonl_file = jsonl_dir / "events.jsonl"
    jsonl_file.write_text('{"id": "j1"}\n', encoding="utf-8")
    jsonl_settings = make_cts_settings(
        JsonlExtractSettings,
        tmp_path,
        input_dir=str(jsonl_dir),
        dataset_name="ds",
        table_name="items",
        loader_file_format="jsonl",
        transforms_module=mod_name,
    )
    assert jsonl_settings.get_table_transforms("items") == (emit_raw_table, _dict_with_optional_ctx_param)
    assert jsonl_settings.get_table_transform("items") is jsonl_settings.get_table_transform("items")
    jsonl_pages = {
        page.meta.table_name: page.data for page in route_jsonl_lines(iter([as_file_item(jsonl_file)]), jsonl_settings)
    }
    assert jsonl_pages == {
        "raw_items": [{"id": "j1"}],
        "items": [{"id": "j1", "source": "events.jsonl"}],
    }


def test_source_file_propagated_to_table_transforms_in_xml_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """process_xml_file_to_dict passes source_file into 2-arg TABLE_TRANSFORMS."""
    xml_dir = tmp_path / "xml_in"
    xml_dir.mkdir()
    xml_file = xml_dir / "records.xml"
    xml_file.write_text("<root><item><id>x1</id></item></root>", encoding="utf-8")

    def _xml_ctx_transform(record: dict[str, Any], ctx: TransformContext) -> dict[str, list[dict[str, Any]]]:
        """Extract ``id`` from ``record["item"]`` and attach ``ctx.file_name``.

        :param record: parsed XML element dictionary
        :type record: dict[str, Any]
        :param ctx: transform context carrying ``file_name``
        :type ctx: TransformContext
        :return: routed mapping targeting ``items``
        :rtype: dict[str, list[dict[str, Any]]]
        """
        item_obj = record["item"]
        return {"items": [{"id": item_obj["id"], "source": ctx.file_name or ""}]}

    xml_mod = register_temp_module(
        monkeypatch,
        "_test_xml_ctx_transforms_mod",
        TABLE_TRANSFORMS={"items": _xml_ctx_transform},
    )
    xml_settings = make_cts_settings(
        XmlToDictSettings,
        tmp_path,
        input_dir=str(xml_dir),
        dataset_name="ds",
        table_name="items",
        xml_tag="item",
        loader_file_format="jsonl",
        transforms_module=xml_mod,
    )
    xml_pages = {
        page.meta.table_name: page.data for page in process_xml_file_to_dict(as_file_item(xml_file), xml_settings)
    }
    assert xml_pages == {"items": [{"id": "x1", "source": "records.xml"}]}


class _SecretItem(BaseModel):
    id: SecretStr


@pytest.mark.parametrize(
    ("record", "transform", "schema_map", "source_file", "expected"),
    [
        pytest.param(
            _SecretItem(id=SecretStr("s-1")),
            None,
            ArrowSchemaRegistry(registry={"items": pa.schema([pa.field("id", pa.string(), nullable=False)])}),
            "batch.jsonl",
            {"items": [{"id": "s-1"}]},
            id="base_model_without_transform",
        ),
        pytest.param(
            {"id": SecretStr("s-2")},
            _dict_with_optional_ctx_param,
            ArrowSchemaRegistry(registry={"items": SOURCED_ITEMS_SCHEMA}),
            "sub/batch.jsonl",
            {"items": [{"id": "s-2", "source": "batch.jsonl"}]},
            id="dict_with_context_transform",
        ),
    ],
)
def test_make_record_router_pass_coerces_pydantic_base_model_and_custom_coercer(
    record: BaseModel | dict[str, Any],
    transform: TableTransformFn | None,
    schema_map: ArrowSchemaRegistry,
    source_file: str,
    expected: dict[str, list[dict[str, Any]]],
) -> None:
    """make_record_router coerces BaseModel instances without a transform and transformed dicts via record_coercer."""
    router = make_record_router(
        "items",
        transform=transform,
        schema_map=schema_map,
        record_coercer=coerce_pydantic_record_for_pyarrow,
    )
    assert router(record, source_file) == expected


def test_make_record_router_fail_rejects_base_model_when_transform_configured() -> None:
    """make_record_router raises TypeError if a BaseModel instance is passed when a TableTransformFn is configured."""

    class SimpleItem(BaseModel):
        id: str

    router = make_record_router("items", transform=_identity_items_transform)
    with pytest.raises(TypeError, match="TableTransformFn requires a dict"):
        router(SimpleItem(id="a"), None)


def test_make_record_router_coerces_rejected_rows_when_schema_map_omits_rejected() -> None:
    """make_record_router coerces _rejected rows against REJECTED_PYARROW_SCHEMA when schema_map omits _rejected."""

    def _reject_item(row: dict[str, Any], ctx: TransformContext) -> dict[str, list[dict[str, Any]]]:
        return {
            "_rejected": [
                {
                    "table_name": ctx.table_name,
                    "source_file": ctx.source_file,
                    "line_no": "7",
                    "raw_record": str(row.get("id")),
                    "error_detail": "invalid item",
                    "undeclared_extra": "dropped",
                }
            ]
        }

    router = make_record_router("items", transform=_reject_item, schema_map=ITEMS_SCHEMA_REGISTRY)
    assert router({"id": 42}, "batch/items.jsonl") == {
        "_rejected": [
            {
                "table_name": "items",
                "source_file": "batch/items.jsonl",
                "line_no": 7,
                "raw_record": "42",
                "error_detail": "invalid item",
            }
        ]
    }


def test_locked_transform_hooks_mixin_locks_transforms_and_output_schemas_off_cli(
    test_defaults: dict[str, Any],
) -> None:
    """LockedTransformHooksMixin removes transforms_module and output_schemas_module from CLI while keeping preprocessors_module."""

    class LockedExtractSettings(LockedTransformHooksMixin, JsonlExtractSettings, cli_prog_name="locked_extract"):
        pass

    assert "transforms_module" not in LockedExtractSettings.model_fields
    assert "output_schemas_module" not in LockedExtractSettings.model_fields
    assert "preprocessors_module" in LockedExtractSettings.model_fields

    settings = LockedExtractSettings(**test_defaults, dataset_name="ds", table_name="items")
    assert settings.transforms_module is None
    assert settings.output_schemas_module is None

    argv = [*settings_to_argv(settings), "--transforms-module", "some.mod"]
    with pytest.raises(SettingsError, match="--transforms-module"):
        CliApp.run(LockedExtractSettings, cli_args=argv)


@pytest.mark.parametrize(
    (
        "use_pyarrow",
        "transformed_tables",
        "explicit_schemas",
        "none_default_tables",
        "expected_built",
        "expected_registry",
    ),
    [
        pytest.param(False, (), {}, (), [], None, id="use-pyarrow-false"),
        pytest.param(
            True,
            (),
            {},
            (),
            ["alpha", "beta"],
            {"alpha": ITEMS_SCHEMA, "beta": ITEMS_SCHEMA},
            id="all-defaults",
        ),
        pytest.param(
            True,
            ("alpha",),
            {},
            (),
            ["beta"],
            {"beta": ITEMS_SCHEMA},
            id="skips-transformed-without-explicit-schema",
        ),
        pytest.param(
            True,
            ("alpha",),
            {"alpha_out": SOURCED_ITEMS_SCHEMA},
            (),
            ["beta"],
            {"alpha_out": SOURCED_ITEMS_SCHEMA, "beta": ITEMS_SCHEMA},
            id="merges-explicit-fanout-and-untransformed-default",
        ),
        pytest.param(
            True,
            (),
            {"alpha": OVERRIDE_SCHEMA},
            (),
            ["beta"],
            {"alpha": OVERRIDE_SCHEMA, "beta": ITEMS_SCHEMA},
            id="explicit-source-table-override-skips-default-builder",
        ),
        pytest.param(
            True,
            (),
            {},
            ("alpha",),
            ["alpha", "beta"],
            {"beta": ITEMS_SCHEMA},
            id="skips-none-default-schema",
        ),
        pytest.param(True, ("alpha", "beta"), {}, (), [], None, id="all-transformed-unschematized"),
    ],
)
def test_reconcile_multi_table_schema_map_reconciles_defaults_and_overrides(
    use_pyarrow: bool,
    transformed_tables: tuple[str, ...],
    explicit_schemas: dict[str, pa.Schema],
    none_default_tables: tuple[str, ...],
    expected_built: list[str],
    expected_registry: dict[str, pa.Schema] | None,
) -> None:
    """reconcile_multi_table_schema_map builds defaults only for untransformed unmapped tables and merges explicit schemas."""
    built: list[str] = []

    def _build_default(table_name: str) -> pa.Schema | None:
        built.append(table_name)
        return None if table_name in none_default_tables else ITEMS_SCHEMA

    transforms = (
        TransformRegistry(registry=dict.fromkeys(transformed_tables, _identity_items_transform))
        if transformed_tables
        else None
    )
    out_schemas = ArrowSchemaRegistry(registry=explicit_schemas) if explicit_schemas else None

    result = reconcile_multi_table_schema_map(
        ["alpha", "beta"],
        _build_default,
        use_pyarrow=use_pyarrow,
        table_transforms=transforms,
        output_schemas=out_schemas,
    )

    assert built == expected_built
    expected = ArrowSchemaRegistry(registry=expected_registry) if expected_registry is not None else None
    assert result == expected


type _CtxAlias = TransformContext


def test_unwrap_partial_target_unwraps_nested_partials() -> None:
    """_unwrap_partial_target unwraps arbitrarily nested functools.partial wrappers."""

    def base_fn(table: pa.Table, *, _flag: bool = False) -> dict[str, Any]:
        del table, _flag
        return {}

    nested = functools.partial(functools.partial(base_fn, _flag=True))
    assert _unwrap_partial_target(nested) is base_fn


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("nested_partial", id="nested-partial"),
        pytest.param("callable_object", id="callable-object"),
        pytest.param("callable_object_with_attrs", id="callable-object-with-attrs"),
        pytest.param("unresolvable_annotation", id="unresolvable-annotation"),
    ],
)
def test_safe_type_hints_handles_callables_and_errors(case: str) -> None:
    """_safe_type_hints resolves hints on partials and callable objects and returns empty dict on errors."""
    if case == "nested_partial":

        def base_fn(table: pa.Table, *, _flag: bool = False) -> dict[str, Any]:
            del table, _flag
            return {}

        target: object = functools.partial(functools.partial(base_fn, _flag=True))
        assert _safe_type_hints(target)["table"] is pa.Table  # type: ignore[arg-type]
    elif case == "callable_object":

        class _CallableObj:
            def __call__(self, table: pa.Table, ctx: TransformContext | None = None) -> dict[str, Any]:
                del table, ctx
                return {}

        assert _safe_type_hints(_CallableObj())["table"] is pa.Table
    elif case == "callable_object_with_attrs":

        class _CallableWithAttrs:
            label: str = "x"

            def __call__(self, table: pa.Table, ctx: TransformContext | None = None) -> dict[str, Any]:
                del table, ctx
                return {}

        assert _safe_type_hints(_CallableWithAttrs())["table"] is pa.Table
    else:

        def bad_hint_fn(_x: object) -> dict[str, Any]:
            return {}

        bad_hint_fn.__annotations__["_x"] = "MissingLocalType"
        assert _safe_type_hints(bad_hint_fn) == {}


@pytest.mark.parametrize(
    ("ann", "expected_caps"),
    [
        pytest.param(pa.Table, (True, False), id="pa_table"),
        pytest.param("pa.Table", (True, False), id="str_table"),
        pytest.param("pa.Table | dict[str, Any]", (True, True), id="str_table_or_dict"),
        pytest.param("pa.Table | Mapping[str, Any]", (True, True), id="str_table_or_mapping"),
        pytest.param("dict[str, Any]", (False, True), id="str_dict"),
        pytest.param(pa.Table | None, (True, False), id="optional_pa_table"),
        pytest.param(Annotated[pa.Table, "meta"], (True, False), id="annotated_pa_table"),
        pytest.param(int, (False, True), id="fallback_int"),
    ],
)
def test_annotation_capabilities_classifies_types(ann: object, expected_caps: tuple[bool, bool]) -> None:
    """_annotation_capabilities classifies table, dict, union, annotated, and string annotations."""
    assert _annotation_capabilities(ann) == expected_caps


@pytest.mark.parametrize(
    ("ann", "expected"),
    [
        pytest.param(TransformContext, True, id="direct"),
        pytest.param(_CtxAlias, True, id="type_alias"),
        pytest.param(Annotated[TransformContext, "meta"], True, id="annotated"),
        pytest.param(Annotated[int, "meta"], False, id="annotated_non_ctx"),
        pytest.param(Annotated[int, TransformContext], False, id="annotated_ctx_in_metadata_only"),
        pytest.param(TransformContext | None, True, id="union"),
        pytest.param(int | None, False, id="union_non_ctx"),
        pytest.param(operator.getitem(Union, (TransformContext, None)), True, id="typing_union_ctx"),
        pytest.param(operator.getitem(Union, (int, str)), False, id="typing_union_non_ctx"),
        pytest.param("TransformContext | None", True, id="str_ctx"),
        pytest.param("dict[str, Any]", False, id="str_non_ctx"),
        pytest.param(int, False, id="non_ctx"),
    ],
)
def test_annotation_is_transform_context_detects_variants(ann: object, expected: bool) -> None:
    """_annotation_is_transform_context detects TransformContext across aliases, unions, Annotated, and strings."""
    assert _annotation_is_transform_context(ann) is expected


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        pytest.param("kwonly_only", (False, True, False), id="kwonly-only"),
        pytest.param("posonly_arrow", (True, False, True), id="posonly-arrow-and-annotated-ctx"),
        pytest.param("optional_non_ctx", (False, True, False), id="optional-non-ctx"),
        pytest.param("required_second_param", (False, True, True), id="required-second-param"),
        pytest.param("str_annotated", (True, False, True), id="string-annotations"),
        pytest.param("flagged_attr", (True, True, True), id="accepts-pyarrow-attr"),
    ],
)
def test_inspect_transform_signature_handles_parameter_variants(
    case: str,
    expected: tuple[bool, bool, bool],
) -> None:
    """inspect_transform_signature classifies kwonly, posonly, optional, string, and flagged callables."""
    if case == "kwonly_only":

        def fn(*, _flag: bool = False) -> dict[str, Any]:
            return {}

    elif case == "posonly_arrow":

        def fn(_table: pa.Table, _info: Annotated[TransformContext, "m"] | None = None, /) -> dict[str, Any]:  # type: ignore[misc]
            return {}

    elif case == "optional_non_ctx":

        def fn(_row: dict[str, Any], _limit: int = 10) -> dict[str, Any]:  # type: ignore[misc]
            return {}

    elif case == "required_second_param":

        def fn(_row: dict[str, Any], _extra: int) -> dict[str, Any]:  # type: ignore[misc]
            return {}

    elif case == "str_annotated":

        def fn(_data: "pa.Table", _info: "TransformContext | None" = None) -> dict[str, Any]:  # type: ignore[misc]
            return {}

    else:

        def fn(_x: object) -> dict[str, Any]:  # type: ignore[misc]
            return {}

        fn.accepts_pyarrow = True  # type: ignore[attr-defined]

    assert inspect_transform_signature(fn) == expected


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("table_to_dict_with_ctx", id="table-to-dict-with-ctx"),
        pytest.param("dict_to_table_with_input_schema", id="dict-to-table-with-input-schema"),
    ],
)
def test_invoke_transform_adapts_input_type(case: str) -> None:
    """invoke_transform adapts PyArrow tables for dict transforms and dicts for PyArrow transforms."""
    ctx = TransformContext.for_source("items", "input.json")

    if case == "table_to_dict_with_ctx":

        def _dict_with_ctx(row: dict[str, Any], info: TransformContext) -> dict[str, list[dict[str, Any]]]:
            return {"items": [{"id": row["id"], "src": info.source_file}]}

        tbl = pa.table({"id": ["1"]})
        assert invoke_transform(_dict_with_ctx, tbl, ctx) == {"items": [{"id": "1", "src": "input.json"}]}
    else:
        arrow_schema = pa.schema([pa.field("id", pa.string()), pa.field("opt", pa.int64(), nullable=True)])

        def _arrow_only(table: pa.Table) -> dict[str, pa.Table]:
            return {"items": table}

        coerced_out = invoke_transform(_arrow_only, {"id": "1"}, ctx, input_schema=arrow_schema)
        assert isinstance(coerced_out["items"], pa.Table)
        assert coerced_out["items"].to_pylist() == [{"id": "1", "opt": None}]


@pytest.mark.parametrize(
    ("initial", "addition", "expect_arrow", "expected_rows"),
    [
        pytest.param({}, {"t": [{"a": 1}]}, False, [{"a": 1}], id="new-table-key"),
        pytest.param(
            {"t": pa.table({"a": [1]})},
            {"t": pa.table({"b": [2]})},
            True,
            [{"a": 1, "b": None}, {"a": None, "b": 2}],
            id="arrow-plus-arrow-mismatched-columns",
        ),
        pytest.param(
            {"t": [{"a": 1}]},
            {"t": [{"a": 2}]},
            False,
            [{"a": 1}, {"a": 2}],
            id="list-plus-list",
        ),
        pytest.param(
            {"t": pa.table({"a": [1]})},
            {"t": [{"a": 2}]},
            False,
            [{"a": 1}, {"a": 2}],
            id="arrow-plus-list",
        ),
        pytest.param(
            {"t": [{"a": 1}]},
            {"t": pa.table({"a": [2]})},
            False,
            [{"a": 1}, {"a": 2}],
            id="list-plus-arrow",
        ),
    ],
)
def test_merge_routed_outputs_pass_merges_tables_lists_and_mixed(
    initial: dict[str, list[dict[str, Any]] | pa.Table],
    addition: dict[str, list[dict[str, Any]] | pa.Table],
    *,
    expect_arrow: bool,
    expected_rows: list[dict[str, Any]],
) -> None:
    """merge_routed_outputs merges new keys, mismatched PyArrow tables, row lists, and mixed outputs."""
    merged: dict[str, list[dict[str, Any]] | pa.Table] = dict(initial)
    merge_routed_outputs(merged, addition)

    result = merged["t"]
    if expect_arrow:
        assert isinstance(result, pa.Table)
        assert result.to_pylist() == expected_rows
    else:
        assert isinstance(result, list)
        assert result == expected_rows


def test_align_and_cast_table_preserves_target_schema_metadata() -> None:
    """align_and_cast_table preserves metadata attached to the target PyArrow schema."""
    base_tbl = pa.table({"id": pa.array(["1"], type=pa.string())})
    target_with_meta = pa.schema([pa.field("id", pa.string())], metadata={b"k": b"v"})

    aligned = align_and_cast_table(base_tbl, target_with_meta)

    assert aligned.schema.metadata == {b"k": b"v"}


def test_validate_routed_output_drops_empty_zero_column_table() -> None:
    """validate_routed_output drops zero-row, zero-column PyArrow tables."""
    assert validate_routed_output({"empty": pa.table({})}) == {}


def test_compose_transforms_fail_empty_list() -> None:
    """compose_transforms raises ValueError when given an empty list."""
    with pytest.raises(ValueError, match=r"^transforms must contain at least one callable$"):
        compose_transforms([])


def test_route_and_coerce_table_defaults_to_pyarrow_output() -> None:
    """route_and_coerce_table returns PyArrow tables by default when use_pyarrow is omitted."""
    base_tbl = pa.table({"id": pa.array(["1"], type=pa.string())})

    default_routed = route_and_coerce_table(base_tbl, "items")

    assert isinstance(default_routed["items"], pa.Table)
    assert default_routed["items"].to_pylist() == [{"id": "1"}]


@pytest.mark.parametrize(
    ("use_multi_table", "loader_file_format", "expected_keys"),
    [
        pytest.param(False, "parquet", {"custom"}, id="no-table-name-parquet"),
        pytest.param(False, "jsonl", None, id="no-table-name-jsonl"),
        pytest.param(True, "parquet", {"custom", "other"}, id="multi-table-parquet"),
        pytest.param(True, "jsonl", None, id="multi-table-jsonl"),
    ],
)
def test_transform_settings_mixin_multi_table_and_no_table_name(
    monkeypatch: pytest.MonkeyPatch,
    use_multi_table: bool,
    loader_file_format: str,
    expected_keys: set[str] | None,
) -> None:
    """TransformSettingsMixin handles multi-table default schema reconciliation and subclasses without table_name."""
    items_schema = ITEMS_SCHEMA
    other_schema = pa.schema([pa.field("other_id", pa.string())])
    mod_name = register_temp_module(
        monkeypatch,
        "_test_settings_mixin_multi_and_none",
        TABLE_TRANSFORMS={"items": _identity_items_transform},
        OUTPUT_SCHEMAS={"custom": items_schema},
    )

    class _NoTableNameSettings(TransformSettingsMixin, BaseModel):
        loader_file_format: str = "parquet"

    class _MultiTableSettings(_NoTableNameSettings):
        def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
            del default_pyarrow_schema
            self._resolve_multi_table_schema_map({"items": items_schema, "other": other_schema})

    if use_multi_table:
        settings: TransformSettingsMixin = _MultiTableSettings(
            loader_file_format=loader_file_format,
            transforms_module=mod_name,
            output_schemas_module=mod_name,
        )
    else:
        settings = _NoTableNameSettings(
            loader_file_format=loader_file_format,
            output_schemas_module=mod_name,
        )

    if expected_keys is None:
        assert settings.schema_map is None
    else:
        expected_registry = {"custom": items_schema}
        if "other" in expected_keys:
            expected_registry["other"] = other_schema
        assert settings.schema_map == ArrowSchemaRegistry(registry=expected_registry)
