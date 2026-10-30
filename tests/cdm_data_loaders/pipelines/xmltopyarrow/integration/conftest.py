"""Shared fixtures for xmltopyarrow pipeline integration tests."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo
from frozendict import frozendict

from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from tests.cdm_data_loaders.pipelines.helpers import (
    denormalize_identifier,
    strip_metadata_columns,
)
from tests.conftest import REFERENCE_XML_TAG
from tests.helpers import strip_none_values

DEFAULT_XMLTOPYARROW_SETTINGS: frozendict = frozendict(
    {
        "buffer_size": 10,
        "dlt_dev_mode": False,
        "file_glob": "*.xml*",
        "log_interval": 1000,
        "use_output_dir_for_pipeline_metadata": False,
    }
)


@pytest.fixture
def settings_factory(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    dlt_destination_config: str,
) -> Callable[..., XmlToPyArrowSettings]:
    """Return a factory that builds a valid XmlToPyArrowSettings (or subclass), with fields open to override."""
    return lambda settings_cls=XmlToPyArrowSettings, **overrides: settings_factory(
        settings_cls,
        **{
            **DEFAULT_XMLTOPYARROW_SETTINGS,
            "use_destination": dlt_destination_config,
            **overrides,
        },
    )


@pytest.fixture
def run_xmltopyarrow_pipeline_fxt(
    reference_xml_data_dir: Path,
    dlt_destination_config: str,
    isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]],
) -> Callable[..., Any]:
    """Return a factory that runs the xmltopyarrow pipeline on a chunk dir with full isolation.

    Each run gets a unique output_dir and dataset_name under tmp_path, so
    parallel-safe repeated runs never collide or accumulate rows.
    """
    base_kwargs = {
        **DEFAULT_XMLTOPYARROW_SETTINGS,
        "settings_cls": XmlToPyArrowSettings,
        "table_name": "entry",
        "use_destination": dlt_destination_config,
        "xml_tag": REFERENCE_XML_TAG,
        "xsd_file": str(reference_xml_data_dir / "uniref.xsd"),
    }
    runner = isolated_run_factory(
        dataset_prefix="reference_run",
        run_fn=run_xmltopyarrow_pipeline,
        base_kwargs=base_kwargs,
    )

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:  # noqa: ANN401
        return runner(input_dir=str(reference_xml_data_dir / chunk_dir), **overrides)

    return _factory


def strip_xsd_arrow_row(row: dict[str, Any]) -> dict[str, Any]:
    """Strip dlt metadata columns, unmangle top-level column names, and drop None struct fields."""
    unmangled = {denormalize_identifier(k): v for k, v in strip_metadata_columns(row).items()}
    return strip_none_values(unmangled)


def _normalize_xsd_reference_member(member: dict[str, Any]) -> dict[str, Any]:
    """Normalize an xmltodict member dict to match XSD-typed cardinality and scalar types."""
    normalized = dict(member)
    db_ref = dict(normalized["dbReference"])
    if "property" in db_ref and isinstance(db_ref["property"], dict):
        db_ref["property"] = [db_ref["property"]]
    normalized["dbReference"] = db_ref
    if "sequence" in normalized and isinstance(normalized["sequence"], dict):
        seq = dict(normalized["sequence"])
        if "_length" in seq:
            seq["_length"] = int(seq["_length"])
        normalized["sequence"] = seq
    return normalized


def normalize_uniref_entry_for_xsd(raw_entry: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw xmltodict UniRef entry dict to match XSD-backed Arrow output."""
    entry = dict(raw_entry.get("entry", raw_entry))
    if "property" in entry and isinstance(entry["property"], dict):
        entry["property"] = [entry["property"]]
    entry["representativeMember"] = _normalize_xsd_reference_member(entry["representativeMember"])
    if "member" in entry:
        members = entry["member"] if isinstance(entry["member"], list) else [entry["member"]]
        entry["member"] = [_normalize_xsd_reference_member(m) for m in members]
    return entry


def reconstruct_xsd_entries(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Extract normalized XSD entry dicts from the single Arrow-backed entry table."""
    return [strip_xsd_arrow_row(row) for row in tables.get("entry", [])]
