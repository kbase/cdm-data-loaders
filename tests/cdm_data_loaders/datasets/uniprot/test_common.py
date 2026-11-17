"""Unit tests for ``cdm_data_loaders.datasets.uniprot.common``."""

import datetime as dt
import json
import logging
from collections.abc import Mapping
from typing import Any

import pytest

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import TransformContext
from cdm_data_loaders.core.transforms.introspection import inspect_transform_signature
from cdm_data_loaders.datasets.uniprot.common import (
    DEFAULT_ENTRY_TABLE_NAME,
    NCBI_TAXON_PREFIX,
    PREFIX_TRANSLATION,
    UNIPROT_CURIE_PREFIX,
    UNIREF_CURIE_PREFIX,
    ensembl_type,
    format_curie,
    format_date,
    iter_mappings,
    make_cdm_entry_transform,
    reject_cdm_entry,
    resolve_timestamp,
    run_cdm_entry_parser,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param({"a": 1}, [{"a": 1}], id="single-mapping"),
        pytest.param([{"a": 1}, "skip", {"b": 2}], [{"a": 1}, {"b": 2}], id="sequence-filters-non-mappings"),
        pytest.param("not-a-sequence-of-mappings", [], id="string-ignored"),
        pytest.param(b"bytes-ignored", [], id="bytes-ignored"),
        pytest.param(None, [], id="none-empty"),
    ],
)
def test_iter_mappings_yields_only_mapping_items(value: object, expected: list[dict[str, Any]]) -> None:
    """iter_mappings yields dicts from single mappings or sequences while ignoring strings, bytes, and None."""
    assert [dict(item) for item in iter_mappings(value)] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(dt.date(2025, 2, 5), "2025-02-05", id="date"),
        pytest.param(dt.datetime(2025, 2, 5, 12, 30, tzinfo=dt.UTC), "2025-02-05T12:30:00+00:00", id="datetime"),
        pytest.param("2025-02-05", "2025-02-05", id="string"),
        pytest.param(12345, None, id="int-returns-none"),
        pytest.param(None, None, id="none-returns-none"),
    ],
)
def test_format_date_formats_dates_and_strings(value: object, expected: str | None) -> None:
    """format_date returns ISO strings for dates and strings and None for other types."""
    assert format_date(value) == expected


@pytest.mark.parametrize(
    ("xref", "expected"),
    [
        pytest.param("ENST000001", "transcript", id="enst"),
        pytest.param("ENSP000001", "protein sequence", id="ensp"),
        pytest.param("ENSG000001", "gene sequence", id="ensg"),
        pytest.param("ENSE000001", "exon", id="ense"),
        pytest.param("ENSFM0001", "protein family", id="ensfm"),
        pytest.param("ENSGT0001", "gene tree", id="ensgt"),
        pytest.param("ENSR000001", "regulatory feature", id="ensr"),
        pytest.param("OTHER123", "sequence", id="fallback-sequence"),
    ],
)
def test_ensembl_type_infers_entity_category(xref: str, expected: str) -> None:
    """ensembl_type maps 4- and 5-character Ensembl prefixes and falls back to sequence."""
    assert ensembl_type(xref) == expected


@pytest.mark.parametrize(
    ("db", "accession", "fallback_upper", "expected"),
    [
        pytest.param("UniProtKB", "P12345", False, "uniprot:P12345", id="uniprotkb"),
        pytest.param("UniProtKB/Swiss-Prot", "P12345", False, "uniprot:P12345", id="swissprot"),
        pytest.param("UniProtKB/TrEMBL", "A0A001", False, "uniprot:A0A001", id="trembl"),
        pytest.param("UniProtKB accession", "P12345", False, "uniprot:P12345", id="uniprotkb-accession"),
        pytest.param("UniProtKB ID", "ACE_HUMAN", False, "uniprot_name:ACE_HUMAN", id="uniprotkb-id"),
        pytest.param("UniParc ID", "UPI0001", False, "uniparc:UPI0001", id="uniparc"),
        pytest.param("UniRef50 ID", "UniRef50_P12345", False, "uniref:UniRef50_P12345", id="uniref50"),
        pytest.param("UniRef90 ID", "UniRef90_P12345", False, "uniref:UniRef90_P12345", id="uniref90"),
        pytest.param("UniRef100 ID", "UniRef100_P12345", False, "uniref:UniRef100_P12345", id="uniref100"),
        pytest.param("NCBI Taxonomy", "9606", False, "NCBITaxon:9606", id="ncbi-taxonomy-upper-t"),
        pytest.param("NCBI taxonomy", "9606", False, "NCBITaxon:9606", id="ncbi-taxonomy-lower-t"),
        pytest.param("GeneID", "1636", False, "NCBIGene:1636", id="geneid"),
        pytest.param("pubmed", "12345678", True, "PMID:12345678", id="pubmed-mapped"),
        pytest.param("doi", "10.1000/x", True, "DOI:10.1000/x", id="unmapped-fallback-upper"),
        pytest.param("CustomDB", "X1", False, "CustomDB:X1", id="unmapped-preserve-case"),
    ],
)
def test_format_curie_translates_prefixes_and_applies_fallback(
    db: str,
    accession: str,
    fallback_upper: bool,
    expected: str,
) -> None:
    """format_curie translates PREFIX_TRANSLATION keys and applies optional uppercase fallback."""
    assert UNIPROT_CURIE_PREFIX == "uniprot:"
    assert UNIREF_CURIE_PREFIX == "uniref:"
    assert NCBI_TAXON_PREFIX == "NCBITaxon"
    assert "pubmed" in PREFIX_TRANSLATION
    assert format_curie(db, accession, fallback_upper=fallback_upper) == expected


def test_resolve_timestamp_returns_explicit_or_utc_now() -> None:
    """resolve_timestamp preserves an explicit timestamp and generates a UTC timestamp when None."""
    fixed = dt.datetime(2025, 3, 1, 8, 0, tzinfo=dt.UTC)
    assert resolve_timestamp(fixed) == fixed

    generated = resolve_timestamp(None)
    assert generated.tzinfo == dt.UTC


@pytest.mark.parametrize(
    ("ctx", "default_table", "expected_table", "expected_source"),
    [
        pytest.param(None, DEFAULT_ENTRY_TABLE_NAME, "entry", None, id="no-ctx-default-table"),
        pytest.param(None, "custom_entry", "custom_entry", None, id="no-ctx-custom-default-table"),
        pytest.param(
            TransformContext(table_name="uniprot_entry", source_file="chunks/part_01.xml.gz"),
            DEFAULT_ENTRY_TABLE_NAME,
            "uniprot_entry",
            "chunks/part_01.xml.gz",
            id="with-ctx-table-and-source",
        ),
    ],
)
def test_reject_cdm_entry_preserves_context_provenance(
    ctx: TransformContext | None,
    default_table: str,
    expected_table: str,
    expected_source: str | None,
) -> None:
    """reject_cdm_entry populates _rejected row with ctx.table_name and ctx.source_file when provided."""
    rejected = reject_cdm_entry({"_id": "bad"}, "missing id", ctx, default_table=default_table)
    assert rejected == {
        REJECTED_TABLE_NAME: [
            {
                "table_name": expected_table,
                "source_file": expected_source,
                "line_no": None,
                "raw_record": json.dumps({"_id": "bad"}),
                "error_detail": "missing id",
            }
        ]
    }


def test_run_cdm_entry_parser_returns_parsed_or_routes_exception_with_ctx(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """run_cdm_entry_parser returns parse_fn output on success and routes exceptions to _rejected with ctx."""
    test_logger = logging.getLogger("test_uniprot_common")
    ctx = TransformContext(table_name="entry", source_file="batch/in.xml.gz")

    ok = run_cdm_entry_parser(
        {"_id": "ok"},
        lambda: {"entity": [{"entity_id": "uniprot:P1"}]},
        ctx=ctx,
        logger=test_logger,
        error_message="Unexpected parse failure",
    )
    assert ok == {"entity": [{"entity_id": "uniprot:P1"}]}

    def _fail() -> dict[str, list[dict[str, Any]]]:
        msg = "explode"
        raise RuntimeError(msg)

    with caplog.at_level(logging.ERROR, logger="test_uniprot_common"):
        failed = run_cdm_entry_parser(
            {"_id": "broken"},
            _fail,
            ctx=ctx,
            logger=test_logger,
            error_message="Unexpected parse failure",
        )

    assert "Unexpected parse failure" in caplog.text
    assert failed[REJECTED_TABLE_NAME][0]["source_file"] == "batch/in.xml.gz"
    assert failed[REJECTED_TABLE_NAME][0]["table_name"] == "entry"
    assert failed[REJECTED_TABLE_NAME][0]["error_detail"] == "explode"


def test_make_cdm_entry_transform_supports_single_arg_and_context_invocation() -> None:
    """make_cdm_entry_transform produces a context-aware RecordTransformFn callable with or without ctx."""

    def _entry_fn(entry: Mapping[str, Any], ctx: TransformContext | None) -> dict[str, list[dict[str, Any]]]:
        return {
            "entity": [
                {
                    "id": entry["id"],
                    "source": ctx.source_file if ctx is not None else None,
                }
            ]
        }

    transform_fn = make_cdm_entry_transform(_entry_fn)
    _, _, takes_ctx = inspect_transform_signature(transform_fn)

    assert takes_ctx is True
    assert transform_fn({"id": "P1"}) == {"entity": [{"id": "P1", "source": None}]}
    assert transform_fn({"id": "P1"}, TransformContext(table_name="entry", source_file="chunk.xml")) == {
        "entity": [{"id": "P1", "source": "chunk.xml"}]
    }
