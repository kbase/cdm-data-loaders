"""DuckDB CSV/TSV reader for XSV files cleaned and validated by :mod:`cdm_data_loaders.readers.xsv.qsv`."""

import csv
from collections.abc import Generator
from pathlib import Path
from typing import Any, Final, TypedDict, Unpack

import duckdb
import pyarrow as pa
from dlt.sources.filesystem import read_csv_duckdb

from cdm_data_loaders.core.buffer import DEFAULT_BUFFER_MAX_ITEMS
from cdm_data_loaders.core.file_items import as_file_item
from cdm_data_loaders.core.transforms.routing import align_and_cast_table
from cdm_data_loaders.readers.xsv.models import non_header_lines_present
from cdm_data_loaders.validation.jsonschema.models import (
    ValidatedXsvSchema,
    get_schema_parsing_metadata,
)

# x-quoting-policy configures XSV output, not the qsv cleaning/validation pass
PARSING_CONFIG_KEYS: Final[dict[str, str]] = {
    "delimiter": "delimiter",
    "comment_char": "comment_char",
    "quote": "quote",
    "escape": "escape",
    "null_regex": "null_regex",
    "null_cols": "null_regex_cols",
}


class DuckDbReadOptions(TypedDict, total=False):
    """Optional keyword arguments configuring :func:`read_validated_rows`."""

    delimiter: str
    quote: str | None
    escape: str | None
    chunk_size: int
    use_pyarrow: bool


def resolve_xsv_parsing_config(validated_schema: ValidatedXsvSchema) -> dict[str, Any]:
    """Derive :class:`CleanerValidatorArgs` kwargs from a schema's ``x-xsv-config`` extension block.

    Schemas without an ``x-xsv-config`` block resolve to an empty dict, letting :class:`CleanerValidatorArgs` use its
    defaults (tab-delimited, ``#``-commented, header present).

    :param validated_schema: Parsed, validated JSON Schema for the XSV data.
    :type validated_schema: ValidatedXsvSchema
    :return: Keyword arguments accepted by :class:`CleanerValidatorArgs`.
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

    Maps each field in ``pa_schema`` to a ``DuckDBPyType`` via ``duckdb.from_arrow``. Columns absent from ``pa_schema``
    default to nullable ``VARCHAR`` via ``auto_type_candidates=["VARCHAR"]``. Non-nullable string columns are listed in
    ``force_not_null`` so empty cells remain ``""`` rather than ``None``.

    :param pa_schema: Target PyArrow schema for the XSV table, or ``None``.
    :type pa_schema: pa.Schema | None
    :return: Keyword arguments for DuckDB ``from_csv_auto``.
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
    """Read the column names from the first non-blank (header) line of ``xsv_path``.

    :param xsv_path: Path to the cleaned XSV file.
    :type xsv_path: Path
    :param delimiter: Field delimiter character.
    :type delimiter: str
    :param quote_char: Quote character.
    :type quote_char: str
    :param escape: Optional escape character.
    :type escape: str | None
    :return: Set of stripped header column names.
    :rtype: set[str]
    """
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
    """Wrap a DuckDB ``RecordBatch`` in a ``pa.Table`` and align it to ``pa_schema`` when supplied.

    :param batch: DuckDB Arrow record batch.
    :type batch: pa.RecordBatch
    :param pa_schema: Optional target PyArrow schema.
    :type pa_schema: pa.Schema | None
    :return: Aligned PyArrow table.
    :rtype: pa.Table
    """
    table = pa.Table.from_batches([batch])
    if pa_schema is None:
        return table
    return align_and_cast_table(table, pa_schema)


def read_validated_rows(
    xsv_path: Path,
    pa_schema: pa.Schema | None = None,
    **options: Unpack[DuckDbReadOptions],
) -> Generator[dict[str, Any] | pa.Table, Any, Any]:
    """Read a cleaned, validated XSV file via ``read_csv_duckdb``, coercing columns to schema types on read.

    The file carries a header row matching the schema's column names, as produced by :func:`clean_validate_file`. When
    ``use_pyarrow`` is ``True``, yields ``pa.Table`` chunks (aligned to ``pa_schema`` when provided); when ``False``,
    yields row dicts.

    :param xsv_path: Path to the cleaned, validated XSV file.
    :type xsv_path: Path
    :param pa_schema: Optional target PyArrow schema configuring DuckDB column types and ``pa.Table`` alignment.
    :type pa_schema: pa.Schema | None
    :param options: Optional delimiter, quote, escape, chunk size, and ``use_pyarrow`` flags.
    :type options: Unpack[DuckDbReadOptions]
    :yield: Row dicts (when ``use_pyarrow=False``) or ``pa.Table`` chunks (when ``use_pyarrow=True``).
    :rtype: Generator[dict[str, Any] | pa.Table, Any, Any]
    """
    if not non_header_lines_present(xsv_path):
        return

    delimiter = options.get("delimiter", "\t")
    quote = options.get("quote")
    escape = options.get("escape")
    chunk_size = options.get("chunk_size", DEFAULT_BUFFER_MAX_ITEMS)
    use_pyarrow = options.get("use_pyarrow", False)

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
