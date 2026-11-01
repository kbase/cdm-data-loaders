"""Tests for `cdm_data_loaders.core.transforms` and `TransformSettingsMixin`."""

import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pyarrow as pa
import pytest
from pydantic import ValidationError

from cdm_data_loaders.core.transforms import (
    REJECTED_PYARROW_SCHEMA,
    TableTransform,
    load_table_transforms,
    resolve_schema_map,
    route_and_coerce_record,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings


def test_table_transform_pass_defaults_and_copies_schema_map() -> None:
    """TableTransform accepts defaults and defensively copies table_to_schema_map."""
    no_op = TableTransform()
    assert no_op.transform_fn is None
    assert no_op.table_to_schema_map is None

    schema = pa.schema([pa.field("id", pa.string())])
    source_map = {"items": schema}
    transform = TableTransform(
        transform_fn=lambda row: {"items": [row]},
        table_to_schema_map=source_map,
    )
    source_map["other"] = schema
    assert transform.table_to_schema_map == {"items": schema}


def test_table_transform_fail_invalid_transform_fn() -> None:
    """TableTransform rejects non-callable transform_fn values."""
    with pytest.raises(ValidationError, match="Input should be callable"):
        TableTransform(transform_fn="not_a_callable")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("schema_map", "match"),
    [
        ("not_a_mapping", "Input should be a valid dictionary"),
        ({}, "Dictionary should have at least 1 item"),
        ({"": pa.schema([])}, "String should have at least 1 character"),
        ({"   ": pa.schema([])}, "String should have at least 1 character"),
        ({123: pa.schema([])}, "Input should be a valid string"),
        ({"items": "not_a_schema"}, "Input should be an instance of Schema"),
    ],
)
def test_table_transform_fail_invalid_schema_map(
    schema_map: Any,  # noqa: ANN401
    match: str,
) -> None:
    """TableTransform validates table_to_schema_map structure and types."""
    with pytest.raises(ValidationError, match=match):
        TableTransform(table_to_schema_map=schema_map)


def test_resolve_schema_map_pass_precedence() -> None:
    """resolve_schema_map prefers transform.table_to_schema_map over default_pyarrow_schema and includes rejected."""
    default_schema = pa.schema([pa.field("a", pa.int64())])
    override_schema = pa.schema([pa.field("b", pa.string())])
    transform_with_schema = TableTransform(table_to_schema_map={"custom": override_schema})
    transform_without_schema = TableTransform(transform_fn=lambda r: {"items": [r]})

    assert resolve_schema_map("items") is None
    assert resolve_schema_map("items", default_pyarrow_schema=default_schema) == {
        "items": default_schema,
        "_rejected": REJECTED_PYARROW_SCHEMA,
    }
    assert resolve_schema_map(
        "items",
        transform=transform_without_schema,
        default_pyarrow_schema=default_schema,
    ) == {
        "items": default_schema,
        "_rejected": REJECTED_PYARROW_SCHEMA,
    }
    assert resolve_schema_map(
        "items",
        transform=transform_with_schema,
        default_pyarrow_schema=default_schema,
    ) == {
        "custom": override_schema,
        "_rejected": REJECTED_PYARROW_SCHEMA,
    }


def test_route_and_coerce_record_pass_untransformed_and_coerced() -> None:
    """route_and_coerce_record routes untransformed records and optionally coerces for PyArrow."""
    schema = pa.schema([pa.field("id", pa.string()), pa.field("extra", pa.int64())])
    record = {"id": "r1", "extra": 5.0, "undeclared": "drop"}

    coerced = route_and_coerce_record(
        record,
        "items",
        table_to_schema_map={"items": schema},
        coerce_untransformed=True,
    )
    assert coerced == {"items": [{"id": "r1", "extra": 5}]}

    uncoerced = route_and_coerce_record(
        record,
        "items",
        table_to_schema_map={"items": schema},
        coerce_untransformed=False,
    )
    assert uncoerced == {"items": [record]}


def test_route_and_coerce_record_pass_filter_transform_and_split() -> None:
    """route_and_coerce_record supports filtering, 1-to-N row transforms, and multi-table splits."""
    parent_schema = pa.schema([pa.field("id", pa.string()), pa.field("note", pa.string())])
    child_schema = pa.schema([pa.field("parent_id", pa.string()), pa.field("tag", pa.string())])

    def _split(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        if record.get("total_count") == 0:
            return {}
        return {
            "parent": [{"id": record["id"]}],
            "child": [{"parent_id": record["id"], "tag": t} for t in record.get("tags", [])],
            "empty_table": [],
        }

    transform = TableTransform(
        transform_fn=_split,
        table_to_schema_map={"parent": parent_schema, "child": child_schema, "empty_table": parent_schema},
    )

    assert (
        route_and_coerce_record(
            {"id": "p0", "total_count": 0},
            "raw",
            transform=transform,
            table_to_schema_map=transform.table_to_schema_map,
        )
        == {}
    )

    routed = route_and_coerce_record(
        {"id": "p1", "total_count": 2, "tags": ["a", "b"]},
        "raw",
        transform=transform,
        table_to_schema_map=transform.table_to_schema_map,
    )
    assert routed == {
        "parent": [{"id": "p1"}],
        "child": [
            {"parent_id": "p1", "tag": "a"},
            {"parent_id": "p1", "tag": "b"},
        ],
    }


@pytest.mark.parametrize(
    ("return_value", "schema_map", "exc_type", "match"),
    [
        (["not_a_mapping"], None, TypeError, "transform_fn must return a mapping"),
        ({"": [{"id": 1}]}, None, ValueError, "transform_fn output keys must be non-empty table names"),
        ({"items": "not_a_sequence_of_dicts"}, None, TypeError, "must be a sequence of dicts"),
        ({"items": ["not_a_dict"]}, None, TypeError, "must be dicts"),
        (
            {"unmapped": [{"id": "1"}]},
            {"items": pa.schema([pa.field("id", pa.string())])},
            ValueError,
            "no PyArrow schema supplied",
        ),
    ],
)
def test_route_and_coerce_record_fail_invalid_transform_fn_output(
    return_value: Any,  # noqa: ANN401
    schema_map: dict[str, pa.Schema] | None,
    exc_type: type[Exception],
    match: str,
) -> None:
    """route_and_coerce_record validates the structure returned by transform_fn."""
    transform = TableTransform(transform_fn=lambda _r: return_value)
    with pytest.raises(exc_type, match=match):
        route_and_coerce_record(
            {"id": "1"},
            "items",
            transform=transform,
            table_to_schema_map=schema_map,
        )


def test_load_table_transforms_pass_and_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """load_table_transforms validates TABLE_TRANSFORMS module exports."""
    valid_mod = ModuleType("_test_valid_transforms_mod")
    expected_transform = TableTransform(transform_fn=lambda r: {"items": [r]})
    valid_mod.TABLE_TRANSFORMS = {"items": expected_transform}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, valid_mod.__name__, valid_mod)

    loaded = load_table_transforms(valid_mod.__name__)
    assert loaded == {"items": expected_transform}

    empty_mod = ModuleType("_test_empty_transforms_mod")
    empty_mod.TABLE_TRANSFORMS = {}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, empty_mod.__name__, empty_mod)
    with pytest.raises(ValueError, match="does not define a non-empty TABLE_TRANSFORMS"):
        load_table_transforms(empty_mod.__name__)

    bad_key_mod = ModuleType("_test_bad_key_transforms_mod")
    bad_key_mod.TABLE_TRANSFORMS = {"": expected_transform}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, bad_key_mod.__name__, bad_key_mod)
    with pytest.raises(ValueError, match="must be non-empty table names"):
        load_table_transforms(bad_key_mod.__name__)

    bad_val_mod = ModuleType("_test_bad_val_transforms_mod")
    bad_val_mod.TABLE_TRANSFORMS = {"items": "not_a_transform"}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, bad_val_mod.__name__, bad_val_mod)
    with pytest.raises(TypeError, match="must be a TableTransform"):
        load_table_transforms(bad_val_mod.__name__)


def test_transform_settings_mixin_pass_loads_transforms(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TransformSettingsMixin loads TABLE_TRANSFORMS when transforms_module is configured."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()

    mod = ModuleType("_test_settings_transforms_mod")
    expected_transform = TableTransform(transform_fn=lambda r: {"items": [r]})
    mod.TABLE_TRANSFORMS = {"items": expected_transform}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, mod.__name__, mod)

    default_settings = JsonlExtractSettings(
        input_dir=input_dir,
        output_dir=output_dir,
        dataset_name="test_ds",
        use_destination="local_fs",
        table_name="items",
    )
    assert default_settings.transforms_module is None
    assert default_settings.table_transforms == {}
    assert default_settings.get_table_transform("items") is None

    configured_settings = JsonlExtractSettings(
        input_dir=input_dir,
        output_dir=output_dir,
        dataset_name="test_ds",
        use_destination="local_fs",
        table_name="items",
        transforms_module=mod.__name__,
    )
    assert configured_settings.table_transforms == {"items": expected_transform}
    assert configured_settings.get_table_transform("items") is expected_transform
    assert configured_settings.get_table_transform("other") is None
