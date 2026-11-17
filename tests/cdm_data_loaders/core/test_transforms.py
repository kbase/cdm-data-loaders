"""Tests for `cdm_data_loaders.core.transforms` and `TransformSettingsMixin`."""

import functools
from collections.abc import Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, Final

import dlt
import pyarrow as pa
import pytest
from dlt.destinations.impl.filesystem.iceberg_adapter import (
    PARTITION_HINT,
    TABLE_PROPERTIES_HINT,
    iceberg_partition,
)
from frozendict import frozendict
from pydantic import BaseModel, SecretStr, ValidationError

from cdm_data_loaders.coercion.pydantic import (
    coerce_record_for_pyarrow as coerce_pydantic_record_for_pyarrow,
)
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    RecordTransformFn,
    TableTransformFn,
    TransformContext,
    TransformRegistry,
)
from cdm_data_loaders.core.transforms import (
    OutputTableHints,
    accepts_pyarrow_table,
    accepts_record_dict,
    align_and_cast_table,
    compose_transforms,
    emit_raw_table,
    extract_schema_descriptions,
    make_record_router,
    resolve_schema_map,
    route_and_coerce_record,
    route_and_coerce_table,
    validate_routed_output,
    validate_table_transforms,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.readers.jsonlines import route_jsonl_lines
from cdm_data_loaders.readers.xml import process_xml_file_to_dict
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA, make_rejected_row
from cdm_data_loaders.utils.file_items import as_file_item
from tests.helpers import make_cts_settings, register_temp_module


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
    assert settings.get_table_transforms("items") == ((expected_item_transform,) if expected_item_transform else ())
    assert settings.get_table_transforms("other") == ()
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
    with pytest.raises(ValueError, match="prefix must be a non-empty string"):
        emit_raw_table({"id": "1"}, ctx, prefix="")


def test_align_and_cast_table_casts_reorders_fills_missing_and_attaches_metadata() -> None:
    """align_and_cast_table appends missing nullable columns, casts types, orders fields, and sets schema metadata."""
    target_schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False, metadata={"comment": "Identifier"}),
            pa.field("count", pa.int64(), nullable=True, metadata={"comment": "Count"}),
            pa.field("note", pa.string(), nullable=True),
        ],
        metadata={"comment": "Target table description"},
    )
    raw_table = pa.table({"extra": ["drop"], "count": pa.array([3], type=pa.int32()), "id": ["a1"]})
    aligned = align_and_cast_table(raw_table, target_schema)
    assert aligned.schema.equals(target_schema, check_metadata=True)
    assert aligned.to_pylist() == [{"id": "a1", "count": 3, "note": None}]
    # fast-path returns the exact same table instance when schema and metadata already match
    assert align_and_cast_table(aligned, target_schema) is aligned


def test_extract_schema_descriptions_and_output_table_hints() -> None:
    """OutputTableHints registers isolated table descriptions, column descriptions, and Iceberg partitions on DltResource."""
    commented_schema = pa.schema(
        [
            pa.field("uniprot_id", pa.string(), nullable=False, metadata={"comment": "Protein ID"}),
            pa.field("uncommented", pa.string(), nullable=True),
        ],
        metadata={"comment": "Schema-level table comment"},
    )
    table_desc, col_descs = extract_schema_descriptions(commented_schema)
    assert table_desc == "Schema-level table comment"
    assert col_descs == {"uniprot_id": "Protein ID"}
    assert extract_schema_descriptions(pa.schema([pa.field("x", pa.int64())])) == (None, {})

    resource = dlt.resource([{"uniprot_id": "P12345"}], name="src")
    bucket_count = 32
    hint = OutputTableHints(
        table_name="id_mapping",
        pyarrow_schema=commented_schema,
        description="Explicit override description",
        iceberg_partitions=(iceberg_partition.bucket(bucket_count, "uniprot_id"),),
    )
    hint.apply_to_resource(resource)
    marked = dlt.mark.with_table_name({"uniprot_id": "P12345"}, "id_mapping")
    computed = resource.compute_table_schema(marked.data, marked.meta)
    assert computed["name"] == "id_mapping"
    assert computed["description"] == "Explicit override description"
    assert computed["columns"]["uniprot_id"]["description"] == "Protein ID"
    assert computed[TABLE_PROPERTIES_HINT] == {"comment": "Explicit override description"}
    assert computed[PARTITION_HINT] == [
        {"transform": "bucket", "source_column": "uniprot_id", "param_value": bucket_count}
    ]


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


def test_make_record_router_pass_coerces_pydantic_base_model_and_custom_coercer() -> None:
    """make_record_router coerces BaseModel instances when transform is None and coerces transformed dicts via record_coercer."""

    class SecretItem(BaseModel):
        id: SecretStr

    target_schema = pa.schema([pa.field("id", pa.string(), nullable=False)])
    schema_map = ArrowSchemaRegistry(registry={"items": target_schema})
    router = make_record_router(
        "items",
        schema_map=schema_map,
        record_coercer=coerce_pydantic_record_for_pyarrow,
    )
    assert router(SecretItem(id=SecretStr("s-1")), "batch.jsonl") == {"items": [{"id": "s-1"}]}

    transformed_router = make_record_router(
        "items",
        transform=_dict_with_optional_ctx_param,
        schema_map=ArrowSchemaRegistry(registry={"items": SOURCED_ITEMS_SCHEMA}),
        record_coercer=coerce_pydantic_record_for_pyarrow,
    )
    assert transformed_router({"id": SecretStr("s-2")}, "sub/batch.jsonl") == {
        "items": [{"id": "s-2", "source": "batch.jsonl"}]
    }


def test_make_record_router_fail_rejects_base_model_when_transform_configured() -> None:
    """make_record_router raises TypeError if a BaseModel instance is passed when a TableTransformFn is configured."""

    class SimpleItem(BaseModel):
        id: str

    router = make_record_router("items", transform=_identity_items_transform)
    with pytest.raises(TypeError, match="TableTransformFn requires a dict"):
        router(SimpleItem(id="a"), None)
