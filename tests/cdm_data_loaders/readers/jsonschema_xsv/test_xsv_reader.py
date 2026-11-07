"""Tests for the XSV reader (post-qsv row casting and parsing-config resolution)."""

from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest

from cdm_data_loaders.readers.jsonschema_xsv.xsv_reader import (
    read_validated_rows,
    resolve_duckdb_column_config,
    resolve_xsv_parsing_config,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedXsvSchema
from tests.xsv_fixtures import COLUMNS, write_xsv

VALID_SCHEMA_URI: Final[str] = "https://json-schema.org/draft/2020-12/schema"


def _schema(properties: dict[str, Any] | None = None, x_xsv_config: dict[str, Any] | None = None) -> ValidatedXsvSchema:
    """Build a minimal ValidatedXsvSchema, optionally with properties and/or an x-xsv-config block."""
    jsonschema: dict[str, Any] = {"$schema": VALID_SCHEMA_URI, "required": COLUMNS}
    if properties is not None:
        jsonschema["properties"] = properties
    if x_xsv_config is not None:
        jsonschema["x-xsv-config"] = x_xsv_config
    return ValidatedXsvSchema(jsonschema=jsonschema)


"""resolve_duckdb_column_config"""


def test_resolve_duckdb_column_config_pass_empty_properties() -> None:
    """An empty properties block only sets auto_type_candidates=['VARCHAR']."""
    assert resolve_duckdb_column_config({}) == {"auto_type_candidates": ["VARCHAR"]}


def test_resolve_duckdb_column_config_pass_maps_types_and_force_not_null() -> None:
    """Properties map to DuckDB SQL types and non-nullable string columns populate force_not_null."""
    properties = {
        "int_col": {"type": "integer"},
        "int_list_col": {"type": ["integer"]},
        "str_then_int_col": {"type": ["string", "integer"]},
        "num_col": {"type": "number"},
        "nullable_num_col": {"type": ["number", "null"]},
        "bool_col": {"type": "boolean"},
        "str_col": {"type": "string"},
        "nullable_str_col": {"type": ["string", "null"]},
        "untyped_col": {},
    }

    assert resolve_duckdb_column_config(properties) == {
        "auto_type_candidates": ["VARCHAR"],
        "dtype": {
            "int_col": "BIGINT",
            "int_list_col": "BIGINT",
            "str_then_int_col": "BIGINT",
            "num_col": "DOUBLE",
            "nullable_num_col": "DOUBLE",
            "bool_col": "BOOLEAN",
            "str_col": "VARCHAR",
            "nullable_str_col": "VARCHAR",
            "untyped_col": "VARCHAR",
        },
        "force_not_null": ["str_col"],
    }


"""resolve_xsv_parsing_config"""


def test_resolve_xsv_parsing_config_pass_no_config_block_returns_empty_dict() -> None:
    """A schema without an x-xsv-config block resolves to an empty dict."""
    assert resolve_xsv_parsing_config(_schema()) == {}


def test_resolve_xsv_parsing_config_pass_maps_all_keys() -> None:
    """Every supported x-xsv-config key maps to its CleanerValidatorArgs kwarg name."""
    schema = _schema(
        x_xsv_config={
            "x-delimiter": ",",
            "x-comment-char": ";",
            "x-quote": "'",
            "x-escape": "\\",
            "x-null-regex": "^NA$",
            "x-null-cols": ["float"],
            "x-has-header": False,
        }
    )

    assert resolve_xsv_parsing_config(schema) == {
        "delimiter": ",",
        "comment_char": ";",
        "quote": "'",
        "escape": "\\",
        "null_regex": "^NA$",
        "null_regex_cols": ["float"],
        "missing_header": True,
    }


def test_resolve_xsv_parsing_config_pass_has_header_true_inverts_to_missing_header_false() -> None:
    """x-has-header: true resolves to missing_header: false."""
    schema = _schema(x_xsv_config={"x-has-header": True})
    assert resolve_xsv_parsing_config(schema) == {"missing_header": False}


def test_resolve_xsv_parsing_config_pass_partial_config_only_sets_present_keys() -> None:
    """Only keys present in x-xsv-config appear in the resolved config."""
    schema = _schema(x_xsv_config={"x-delimiter": "\t"})
    assert resolve_xsv_parsing_config(schema) == {"delimiter": "\t"}


def test_resolve_xsv_parsing_config_pass_quoting_policy_not_propagated() -> None:
    """x-quoting-policy configures XSV output, not qsv cleaning, and is never returned."""
    schema = _schema(x_xsv_config={"x-delimiter": ",", "x-quoting-policy": "necessary"})
    config = resolve_xsv_parsing_config(schema)
    assert "quoting_policy" not in config
    assert config == {"delimiter": ","}


"""read_validated_rows"""


def test_read_validated_rows_pass_casts_rows_by_property_type(tmp_path: Path) -> None:
    """Rows are cast according to the declared property types, keyed by header column name."""
    properties = {
        "number": {"type": ["integer", "null"]},
        "date": {"type": "string"},
        "float": {"type": ["string", "null"]},
        "boolean": {"type": "boolean"},
        "string": {"type": "string"},
    }
    content = "number,date,float,boolean,string\n2,2023-01-15,3.14,true,key:value1\n,2023-02-20,,false,\n"
    xsv_path = write_xsv(tmp_path, content, file_name="data.csv")

    rows = list(read_validated_rows(xsv_path, properties, delimiter=","))

    assert rows == [
        {"number": 2, "date": "2023-01-15", "float": "3.14", "boolean": True, "string": "key:value1"},
        {"number": None, "date": "2023-02-20", "float": None, "boolean": False, "string": ""},
    ]


def test_read_validated_rows_pass_tab_delimited(tmp_path: Path) -> None:
    r"""A tab-delimited file is read correctly when delimiter="\t"."""
    properties = {"a": {"type": "integer"}, "b": {"type": "string"}}
    xsv_path = write_xsv(tmp_path, "a\tb\n1\tfoo\n2\tbar\n", file_name="data.tsv")

    rows = list(read_validated_rows(xsv_path, properties, delimiter="\t"))

    assert rows == [{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}]


def test_read_validated_rows_pass_custom_quote_char(tmp_path: Path) -> None:
    """A field containing the delimiter is read as one field when wrapped in the custom quote char."""
    properties = {"a": {"type": "string"}, "b": {"type": "string"}}
    xsv_path = write_xsv(tmp_path, "a,b\n'contains, a comma',plain\n", file_name="data.csv")

    rows = list(read_validated_rows(xsv_path, properties, delimiter=",", quote="'"))

    assert rows == [{"a": "contains, a comma", "b": "plain"}]


def test_read_validated_rows_pass_custom_escape_char_disables_doublequote(tmp_path: Path) -> None:
    """A backslash-escaped quote inside a field is read literally when escape is set."""
    properties = {"a": {"type": "string"}}
    xsv_path = write_xsv(tmp_path, 'a\n"contains \\"a quote\\""\n', file_name="data.csv")

    rows = list(read_validated_rows(xsv_path, properties, delimiter=",", escape="\\"))

    assert rows == [{"a": 'contains "a quote"'}]


@pytest.mark.parametrize("use_pyarrow", [False, True], ids=["dicts", "pyarrow"])
def test_read_validated_rows_pass_header_only_file_yields_no_rows(tmp_path: Path, use_pyarrow: bool) -> None:
    """A file with only a header row yields no rows in either dict or PyArrow mode."""
    xsv_path = write_xsv(tmp_path, "number,date,float,boolean,string\n", file_name="data.csv")

    assert list(read_validated_rows(xsv_path, {}, delimiter=",", use_pyarrow=use_pyarrow)) == []


def test_read_validated_rows_pass_unknown_column_defaults_to_nullable_string(tmp_path: Path) -> None:
    """A column absent from properties defaults to nullable VARCHAR (empty becomes None, non-empty stays str)."""
    xsv_path = write_xsv(tmp_path, "unknown,other\n,text\n3.14,more\n", file_name="data.csv")

    rows = list(read_validated_rows(xsv_path, {}, delimiter=","))

    assert rows == [
        {"unknown": None, "other": "text"},
        {"unknown": "3.14", "other": "more"},
    ]


def test_read_validated_rows_pass_emits_pyarrow_tables_when_use_pyarrow_true(tmp_path: Path) -> None:
    """When use_pyarrow=True, read_validated_rows yields pa.Table chunks aligned to pa_schema."""
    properties = {
        "number": {"type": "integer"},
        "score": {"type": ["number", "null"]},
        "active": {"type": "boolean"},
        "label": {"type": "string"},
    }
    pa_schema = pa.schema(
        [
            pa.field("number", pa.int64(), nullable=False),
            pa.field("score", pa.float64(), nullable=True),
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("label", pa.string(), nullable=False),
            pa.field("missing_optional", pa.string(), nullable=True),
        ]
    )
    content = "number,score,active,label\n1,2.5,true,a\n2,,false,\n3,4.0,1,c\n"
    xsv_path = write_xsv(tmp_path, content, file_name="data.csv")

    tables = list(
        read_validated_rows(
            xsv_path,
            properties,
            delimiter=",",
            chunk_size=2,
            use_pyarrow=True,
            pa_schema=pa_schema,
        )
    )

    assert all(isinstance(t, pa.Table) for t in tables)
    combined = pa.concat_tables(tables)
    assert combined.schema == pa_schema
    assert combined.to_pylist() == [
        {"number": 1, "score": 2.5, "active": True, "label": "a", "missing_optional": None},
        {"number": 2, "score": None, "active": False, "label": "", "missing_optional": None},
        {"number": 3, "score": 4.0, "active": True, "label": "c", "missing_optional": None},
    ]


def test_read_validated_rows_pass_emits_pyarrow_tables_without_pa_schema(tmp_path: Path) -> None:
    """When use_pyarrow=True and pa_schema is None, read_validated_rows yields DuckDB's native pa.Table."""
    properties = {"a": {"type": "integer"}, "b": {"type": "string"}}
    xsv_path = write_xsv(tmp_path, "a,b\n1,foo\n2,bar\n", file_name="data.csv")

    tables = list(read_validated_rows(xsv_path, properties, delimiter=",", use_pyarrow=True))

    assert len(tables) == 1
    assert isinstance(tables[0], pa.Table)
    assert tables[0].to_pylist() == [{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}]
