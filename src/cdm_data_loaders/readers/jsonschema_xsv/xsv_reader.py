"""Reader for XSV data cleaned and validated by `xsv_validator.qsv`.

Reads rows from a file produced by `xsv_validator.qsv.clean_validate_file`, casting each field
to the type declared for it in the target PyArrow schema. Also resolves the
qsv cleaning/validation parameters (delimiter, quote, escape, comment character, null handling)
from a schema's `x-xsv-config` extension block.
"""

import csv
from collections.abc import Generator
from pathlib import Path
from typing import Any, Final

import duckdb
import pyarrow as pa
from dlt.sources.filesystem import read_csv_duckdb

from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.helpers import non_header_lines_present
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import (
    ValidatedXsvSchema,
    get_schema_parsing_metadata,
)
from cdm_data_loaders.utils.buffer import DEFAULT_BUFFER_MAX_ITEMS
from cdm_data_loaders.utils.file_items import as_file_item

# maps x-xsv-config metadata keys (after get_schema_parsing_metadata strips the "x-" prefix) to
# the CleanerValidatorArgs kwarg they configure. x-quoting-policy is omitted: it configures XSV
# output, not the cleaning/validation pass driven by this config.
PARSING_CONFIG_KEYS: Final[dict[str, str]] = {
    "delimiter": "delimiter",
    "comment_char": "comment_char",
    "quote": "quote",
    "escape": "escape",
    "null_regex": "null_regex",
    "null_cols": "null_regex_cols",
}


def resolve_xsv_parsing_config(validated_schema: ValidatedXsvSchema) -> dict[str, Any]:
    """Derive `CleanerValidatorArgs` kwargs from a schema's `x-xsv-config` extension block.

    Schemas without an `x-xsv-config` block resolve to an empty dict, letting
    `CleanerValidatorArgs` fall back to its own defaults (tab-delimited, `#`-commented, header
    present).

    :param validated_schema: parsed, validated JSON Schema for the XSV data
    :type validated_schema: ValidatedXsvSchema
    :return: kwargs accepted by `CleanerValidatorArgs`
    :rtype: dict[str, Any]
    """
    if not validated_schema.has_xsv_parser_config:
        return {}

    metadata = get_schema_parsing_metadata(validated_schema)
    config = {arg_name: metadata[key] for key, arg_name in PARSING_CONFIG_KEYS.items() if key in metadata}
    if "has_header" in metadata:
        config["missing_header"] = not metadata["has_header"]
    return config


def resolve_duckdb_column_config(pa_schema: pa.Schema | None) -> dict[str, Any]:
    """Derive DuckDB CSV reader column-type and nullability kwargs from a PyArrow schema.

    Maps each field in `pa_schema` to a `DuckDBPyType` via `duckdb.from_arrow`. Columns absent
    from `pa_schema` default to nullable `VARCHAR` via `auto_type_candidates=["VARCHAR"]`.
    Non-nullable string columns are listed in `force_not_null` so empty cells remain `""`
    rather than `None`.

    :param pa_schema: target PyArrow schema for the XSV table, or `None`
    :type pa_schema: pa.Schema | None
    :return: kwargs (`auto_type_candidates`, and optional `dtype` and `force_not_null`) for DuckDB `from_csv_auto`
    :rtype: dict[str, Any]
    """
    config: dict[str, Any] = {"auto_type_candidates": ["VARCHAR"]}
    if pa_schema is None or len(pa_schema) == 0:
        return config

    dtypes = dict(
        zip(
            pa_schema.names,
            duckdb.from_arrow(pa_schema.empty_table()).dtypes,
            strict=True,
        )
    )
    force_not_null = [
        field.name
        for field in pa_schema
        if (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)) and not field.nullable
    ]
    config["dtype"] = dtypes
    if force_not_null:
        config["force_not_null"] = force_not_null
    return config


def _read_header_columns(
    xsv_path: Path,
    delimiter: str,
    quote_char: str,
    escape: str | None,
) -> set[str]:
    """Read the column names from the first non-blank (header) line of `xsv_path`."""
    with xsv_path.open(newline="") as f:
        header_line = next(line for line in f if line.strip())
    row = next(
        csv.reader(
            [header_line],
            delimiter=delimiter,
            quotechar=quote_char,
            escapechar=escape,
            doublequote=escape is None,
        )
    )
    return {col.strip() for col in row}


def _coerce_batch_to_table(batch: pa.RecordBatch, pa_schema: pa.Schema | None) -> pa.Table:
    """Wrap a DuckDB `RecordBatch` in a `pa.Table` and align it to `pa_schema` when supplied."""
    table = pa.Table.from_batches([batch])
    if pa_schema is None:
        return table
    for field in pa_schema:
        if field.name not in table.column_names:
            table = table.append_column(field, pa.nulls(table.num_rows, type=field.type))
    return table.select(pa_schema.names).cast(pa_schema)


def read_validated_rows(  # noqa: PLR0913
    xsv_path: Path,
    pa_schema: pa.Schema | None = None,
    delimiter: str = "\t",
    quote: str | None = None,
    escape: str | None = None,
    *,
    chunk_size: int = DEFAULT_BUFFER_MAX_ITEMS,
    use_pyarrow: bool = False,
) -> Generator[dict[str, Any] | pa.Table, Any, Any]:
    """Read a cleaned, validated XSV file via `read_csv_duckdb`, coercing columns to schema types on read.

    The file is expected to carry a header row matching the schema's column names, as produced
    by `xsv_validator.qsv.clean_validate_file`. When `use_pyarrow` is `True`, yields `pa.Table`
    chunks (aligned to `pa_schema` when provided); when `False`, yields row dicts.

    :param xsv_path: path to the cleaned, validated XSV file
    :type xsv_path: Path
    :param pa_schema: optional target PyArrow schema used to configure DuckDB column types and align `pa.Table` chunks
    :type pa_schema: pa.Schema | None, optional
    :param delimiter: delimiter used in the file, defaults to tab
    :type delimiter: str, optional
    :param quote: quote character used in the file; defaults to `"` when unset
    :type quote: str | None, optional
    :param escape: escape character used in the file; when unset, quotes are escaped by doubling
    :type escape: str | None, optional
    :param chunk_size: maximum number of rows per DuckDB batch
    :type chunk_size: int, optional
    :param use_pyarrow: when True, yield `pa.Table` chunks instead of row dicts
    :type use_pyarrow: bool, optional
    :yield: row dicts (when `use_pyarrow=False`) or `pa.Table` chunks (when `use_pyarrow=True`)
    :rtype: Generator[dict[str, Any] | pa.Table, Any, Any]
    """
    if not non_header_lines_present(xsv_path):
        return

    quote_char = quote or '"'
    duckdb_schema = pa_schema
    if pa_schema is not None:
        header_cols = _read_header_columns(xsv_path, delimiter, quote_char, escape)
        duckdb_schema = pa.schema([field for field in pa_schema if field.name in header_cols])

    reader = [[as_file_item(xsv_path)]] | read_csv_duckdb(
        chunk_size=chunk_size,
        use_pyarrow=use_pyarrow,
        header=True,
        delimiter=delimiter,
        quotechar=quote_char,
        escapechar=escape or quote_char,
        **resolve_duckdb_column_config(duckdb_schema),
    )
    for item in reader:
        if use_pyarrow:
            if item.num_rows == 0:
                continue
            yield _coerce_batch_to_table(item, pa_schema)
        else:
            yield item
