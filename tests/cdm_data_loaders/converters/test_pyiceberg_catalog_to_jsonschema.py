"""End-to-end tests for pyiceberg_to_jsonschema.

Chains a real pyiceberg SQL catalog (file-backed SQLite) through
dump_catalog_schemas and validates the emitted documents with jsonschema's
Draft 2020-12 validator.
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from linkml_runtime.linkml_model.meta import SchemaDefinition
from linkml_runtime.loaders import yaml_loader
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BinaryType, NestedField

from cdm_data_loaders.converters._iceberg_catalog_dump import IcebergCatalogDumpSettings
from cdm_data_loaders.converters.pyiceberg_catalog_to_jsonschema import (
    IcebergToJsonSchemaSettings,
    dump_catalog_schemas,
)
from cdm_data_loaders.converters.pyiceberg_catalog_to_linkml import (
    IcebergToLinkMLSettings,
)
from cdm_data_loaders.converters.pyiceberg_catalog_to_linkml import (
    dump_catalog_schemas as dump_linkml_catalog_schemas,
)
from tests.cdm_data_loaders.converters.conftest import (
    CATALOG_NAME,
    JSON_SCHEMA_KEYWORDS,
    iter_extension_keys,
    iter_schema_keywords,
    to_snake_case,
)


@dataclass(frozen=True, slots=True)
class CatalogDumpTarget:
    """Target specification for shared catalog dump CLI tests."""

    settings_cls: type[IcebergCatalogDumpSettings]
    dump_fn: Callable[[Any], None]
    suffix: str
    create_bad_table: Callable[[Catalog, Schema], None]
    assert_table_written: Callable[[Path, str, bool], None]


def _create_bad_jsonschema_table(catalog: Catalog, _simple_schema: Schema) -> None:
    unsupported_schema = Schema(NestedField(1, "value", BinaryType(), required=True))
    catalog.create_table(("ns", "bad"), schema=unsupported_schema)


def _create_bad_linkml_table(catalog: Catalog, simple_schema: Schema) -> None:
    bad_table = catalog.create_table(("ns", "bad"), schema=simple_schema)
    bad_table.io.delete(bad_table.metadata_location)


def _assert_jsonschema_written(path: Path, table_name: str, group_by_namespace: bool) -> None:
    written = json.loads(path.read_text())
    assert (set(written["$defs"]) == {table_name}) if group_by_namespace else (written["title"] == table_name)


def _assert_linkml_written(path: Path, table_name: str, group_by_namespace: bool) -> None:
    written = yaml_loader.load(str(path), target_class=SchemaDefinition)
    assert set(written.classes) == {table_name}
    assert written.name == ("ns" if group_by_namespace else table_name)


CATALOG_DUMP_TARGETS = [
    pytest.param(
        CatalogDumpTarget(
            settings_cls=IcebergToJsonSchemaSettings,
            dump_fn=dump_catalog_schemas,
            suffix=".schema.json",
            create_bad_table=_create_bad_jsonschema_table,
            assert_table_written=_assert_jsonschema_written,
        ),
        id="jsonschema",
    ),
    pytest.param(
        CatalogDumpTarget(
            settings_cls=IcebergToLinkMLSettings,
            dump_fn=dump_linkml_catalog_schemas,
            suffix=".schema.yaml",
            create_bad_table=_create_bad_linkml_table,
            assert_table_written=_assert_linkml_written,
        ),
        id="linkml",
    ),
]


@pytest.mark.parametrize("group_by_namespace", [False, True], ids=["per-table", "per-namespace"])
def test_end_to_end_dump_catalog_schemas_writes_valid_docs(
    catalog: Catalog, catalog_env: Path, simple_schema: Schema, wide_schema: Schema, group_by_namespace: bool
) -> None:
    """dump_catalog_schemas loads the catalog by name and writes valid schema docs, per table or per namespace."""
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)
    catalog.create_table(("ns", "wide"), schema=wide_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToJsonSchemaSettings(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=group_by_namespace
    )
    dump_catalog_schemas(settings)

    files = sorted(out_dir.iterdir())
    assert [f.name for f in files] == (
        ["ns.schema.json"] if group_by_namespace else ["ns.simple.schema.json", "ns.wide.schema.json"]
    )

    if group_by_namespace:
        namespace_doc = json.loads(files[0].read_text())
        assert namespace_doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert namespace_doc["$id"] == "urn:iceberg:ns"
        assert namespace_doc["title"] == "ns"
        assert namespace_doc["x-iceberg"]["generated_at"]
        simple_doc = namespace_doc["$defs"]["simple"]
        wide_doc = namespace_doc["$defs"]["wide"]
        assert "$schema" not in simple_doc
        assert "$schema" not in wide_doc
        checked_docs = [namespace_doc]
    else:
        simple_doc = json.loads((out_dir / "ns.simple.schema.json").read_text())
        wide_doc = json.loads((out_dir / "ns.wide.schema.json").read_text())
        assert simple_doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert simple_doc["$id"] == "urn:iceberg:ns.simple"
        assert simple_doc["x-iceberg"]["generated_at"]
        checked_docs = [simple_doc, wide_doc]

    assert simple_doc["title"] == "simple"
    assert wide_doc["title"] == "wide"
    Draft202012Validator.check_schema(simple_doc)
    simple_validator = Draft202012Validator(simple_doc, format_checker=FormatChecker())
    assert simple_validator.is_valid({"id": 1, "name": "n"})
    assert not simple_validator.is_valid({"name": "n"})
    assert not simple_validator.is_valid({"id": "not-an-int", "name": "n"})

    Draft202012Validator.check_schema(wide_doc)
    wide_validator = Draft202012Validator(wide_doc, format_checker=FormatChecker())
    assert wide_validator.is_valid(
        {
            "id": 1,
            "name": "n",
            "count": 3,
            "ratio": 0.5,
            "as_of": "2026-09-22T00:00:00+00:00",
            "born_on": "2026-09-22",
            "rate": "1234.5678",
            "tags": ["a"],
            "attrs": [{"key": "k", "value": 1.5}],
            "address": {"street": "s", "city": "c"},
        }
    )
    assert wide_validator.is_valid({"id": 1, "name": "n", "address": {"street": "s"}})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "address": {"city": "c"}})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "rate": 1234.5678})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "tags": "not-an-array"})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "attrs": [{"key": "k", "value": "x"}]})

    for doc in checked_docs:
        for key in iter_schema_keywords(doc):
            assert key in JSON_SCHEMA_KEYWORDS or key.startswith("x-"), f"non-standard key without x- prefix: {key}"
        for key in iter_extension_keys(doc):
            assert key == to_snake_case(key), f"extension key is not snake case: {key}"


@pytest.mark.parametrize("target", CATALOG_DUMP_TARGETS)
@pytest.mark.parametrize("group_by_namespace", [False, True], ids=["per-table", "per-namespace"])
@pytest.mark.parametrize("empty_namespace", [False, True], ids=["empty-catalog", "empty-namespace"])
def test_dump_catalog_schemas_pass_empty_catalog(
    catalog: Catalog,
    catalog_env: Path,
    target: CatalogDumpTarget,
    group_by_namespace: bool,
    empty_namespace: bool,
) -> None:
    """Empty catalogs and namespaces write no files."""
    if empty_namespace:
        catalog.create_namespace("empty")
    out_dir = catalog_env / "schemas"
    settings = target.settings_cls(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=group_by_namespace
    )
    target.dump_fn(settings)
    assert list(out_dir.iterdir()) == []


@pytest.mark.parametrize("target", CATALOG_DUMP_TARGETS)
@pytest.mark.parametrize("group_by_namespace", [False, True], ids=["per-table", "per-namespace"])
@pytest.mark.parametrize("include_good", [False, True], ids=["all-fail", "partial-failure"])
def test_dump_catalog_schemas_pass_skips_failing_table_and_logs(
    catalog: Catalog,
    catalog_env: Path,
    simple_schema: Schema,
    caplog: pytest.LogCaptureFixture,
    target: CatalogDumpTarget,
    group_by_namespace: bool,
    include_good: bool,
) -> None:
    """A table that fails to convert is logged and skipped; other tables still succeed."""
    catalog.create_namespace("ns")
    target.create_bad_table(catalog, simple_schema)
    if include_good:
        catalog.create_table(("ns", "good"), schema=simple_schema)

    out_dir = catalog_env / "schemas"
    settings = target.settings_cls(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=group_by_namespace
    )
    with caplog.at_level(logging.WARNING):
        target.dump_fn(settings)

    expected_files = (
        [f"ns{target.suffix}" if group_by_namespace else f"ns.good{target.suffix}"] if include_good else []
    )
    assert [f.name for f in sorted(out_dir.iterdir())] == expected_files
    if include_good:
        target.assert_table_written(out_dir / expected_files[0], "good", group_by_namespace)
    assert "Failed to convert table ('ns', 'bad'); skipping" in caplog.text
    assert "Completed with 1 table(s) skipped due to conversion failures" in caplog.text


@pytest.mark.parametrize("target", CATALOG_DUMP_TARGETS)
@pytest.mark.parametrize("group_by_namespace", [False, True], ids=["per-table", "per-namespace"])
def test_dump_catalog_schemas_pass_overwrites_existing_file_with_warning(
    catalog: Catalog,
    catalog_env: Path,
    simple_schema: Schema,
    caplog: pytest.LogCaptureFixture,
    target: CatalogDumpTarget,
    group_by_namespace: bool,
) -> None:
    """A pre-existing output file for a table is overwritten, with a warning logged first."""
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)

    out_dir = catalog_env / "schemas"
    out_dir.mkdir(parents=True)
    stale_file = out_dir / (f"ns{target.suffix}" if group_by_namespace else f"ns.simple{target.suffix}")
    stale_file.write_text("stale")

    settings = target.settings_cls(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=group_by_namespace
    )
    with caplog.at_level(logging.WARNING):
        target.dump_fn(settings)

    assert "Overwriting existing schema file" in caplog.text
    target.assert_table_written(stale_file, "simple", group_by_namespace)


def test_dump_catalog_schemas_pass_namespace_isolation(
    catalog: Catalog, catalog_env: Path, simple_schema: Schema
) -> None:
    """Each namespace's grouped document holds only that namespace's tables."""
    for namespace in ("first", "second"):
        catalog.create_namespace(namespace)
        catalog.create_table((namespace, "record"), schema=simple_schema, properties={"comment": "Table description."})
    catalog.create_table(("first", "other"), schema=simple_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToJsonSchemaSettings(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=True
    )
    dump_catalog_schemas(settings)

    assert sorted(file.name for file in out_dir.iterdir()) == ["first.schema.json", "second.schema.json"]
    first = json.loads((out_dir / "first.schema.json").read_text())
    second = json.loads((out_dir / "second.schema.json").read_text())
    assert set(first["$defs"]) == {"record", "other"}
    assert set(second["$defs"]) == {"record"}
    for namespace, document in (("first", first), ("second", second)):
        assert document["$id"] == f"urn:iceberg:{namespace}"
        assert document["title"] == namespace
        assert document["$defs"]["record"]["description"] == "Table description."
        Draft202012Validator.check_schema(document["$defs"]["record"])
    assert first["x-iceberg"]["generated_at"] == second["x-iceberg"]["generated_at"]


@pytest.mark.parametrize("target", CATALOG_DUMP_TARGETS)
@pytest.mark.parametrize(
    ("arguments", "expected"),
    [([], False), (["--group-by-namespace", "true"], True), (["--group-by-namespace", "false"], False)],
    ids=["default", "enabled", "disabled"],
)
def test_iceberg_catalog_dump_settings_pass_grouping_cli(
    tmp_path: Path, target: CatalogDumpTarget, arguments: list[str], expected: bool
) -> None:
    """Parse namespace grouping from the CLI while retaining the per-table default."""
    settings = target.settings_cls(  # pyright: ignore[reportCallIssue]
        _cli_parse_args=["--catalog", CATALOG_NAME, "--output-dir", str(tmp_path), *arguments]
    )
    assert settings.group_by_namespace is expected
