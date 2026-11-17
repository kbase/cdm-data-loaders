"""End-to-end tests for the UniProt ID mapping pipeline across DuckDB and Iceberg destinations."""

import functools
import gzip
from operator import itemgetter
from pathlib import Path

import pytest
from pyiceberg.catalog import load_catalog

import cdm_data_loaders.pipelines.xsv.pipeline as xsv_pipeline_module
from cdm_data_loaders.pipelines.presets.uniprot_idmapping import (
    PIPELINE_NAME,
    UniProtIdMappingSettings,
)
from cdm_data_loaders.pipelines.xsv.pipeline import run_xsv_pipeline
from cdm_data_loaders.pipelines.xsv.settings import XsvIngestSettings
from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    ID_MAPPING_TABLE_NAME,
    IDMAPPING_SCHEMA_FILE,
    RAW_ID_MAPPING_TABLE_NAME,
)
from tests.e2e.pipelines.presets._helpers import SAMPLE_RAW_ROWS
from tests.helpers.cli_assertions import make_cts_settings
from tests.helpers.dlt_isolation import isolated_dlt_config
from tests.helpers.iceberg_catalog import configure_pyiceberg_home_sqlite_catalog
from tests.helpers.paths_and_constants import LOCAL_FS
from tests.helpers.table_readers import (
    fetch_all_duckdb,
    run_with_duckdb_pipeline,
)


@pytest.mark.usefixtures("qsv_cmd")
@pytest.mark.parametrize("loader_file_format", ["parquet", "jsonl"], ids=["parquet", "jsonl"])
def test_run_xsv_pipeline_uniprot_idmapping_duckdb_multi_file_and_rejections(
    tmp_path: Path,
    loader_file_format: str,
) -> None:
    """run_xsv_pipeline with UniProtIdMappingSettings loads raw_id_mapping, id_mapping, and _rejected across .dat and .dat.gz files."""
    input_dir = tmp_path / "input"
    nested_dir = input_dir / "by_organism"
    nested_dir.mkdir(parents=True)

    with gzip.open(nested_dir / "HUMAN_9606_idmapping.dat.gz", "wt", encoding="utf-8") as gz_fh:
        gz_fh.write("P12345\tNCBI_TaxID\t9606\nQ8N158\tGeneID\t284697\n")

    # include one valid row, one ragged row, and one minLength-violating row (< 6 chars uniprot_id)
    (input_dir / "PARTIAL_idmapping.dat").write_text(
        "A0A024RBG1\tUniProtKB-ID\tNUD4B_HUMAN\nBAD\tRefSeq\tNP_001\nONLY_ONE_COL\n",
        encoding="utf-8",
    )
    (input_dir / "CORRUPT_idmapping.dat.gz").write_bytes(b"not-a-valid-gzip-stream")

    settings = make_cts_settings(
        UniProtIdMappingSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=LOCAL_FS,
        loader_file_format=loader_file_format,
    )

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        functools.partial(run_xsv_pipeline, settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name=PIPELINE_NAME,
        pipeline_name_prefix=f"uniprot_idmap_{loader_file_format}",
    )

    raw_rows = fetch_all_duckdb(
        load_info.pipeline,
        "SELECT uniprot_id, db_reference_type, db_reference_id FROM raw_id_mapping ORDER BY uniprot_id",
    )
    assert raw_rows == [
        ("A0A024RBG1", "UniProtKB-ID", "NUD4B_HUMAN"),
        ("P12345", "NCBI_TaxID", "9606"),
        ("Q8N158", "GeneID", "284697"),
    ]

    mapped_rows = fetch_all_duckdb(
        load_info.pipeline,
        "SELECT uniprot_id, db_reference, source FROM id_mapping ORDER BY uniprot_id",
    )
    assert mapped_rows == [
        ("uniprot:A0A024RBG1", "UniProtKB-ID:NUD4B_HUMAN", "PARTIAL_idmapping.dat"),
        ("uniprot:P12345", "NCBI_TaxID:9606", "HUMAN_9606_idmapping.dat.gz"),
        ("uniprot:Q8N158", "GeneID:284697", "HUMAN_9606_idmapping.dat.gz"),
    ]

    rejected_rows = fetch_all_duckdb(
        load_info.pipeline,
        "SELECT DISTINCT table_name, source_file FROM _rejected ORDER BY source_file",
    )
    assert rejected_rows == [
        (ID_MAPPING_TABLE_NAME, "CORRUPT_idmapping.dat.gz"),
        (ID_MAPPING_TABLE_NAME, "PARTIAL_idmapping.dat"),
    ]


@pytest.mark.usefixtures("qsv_cmd")
def test_run_xsv_pipeline_generic_settings_with_idmapping_transforms_module(tmp_path: Path) -> None:
    """Generic XsvIngestSettings runs cdm_data_loaders.datasets.uniprot.idmapping via --transforms-module and --output-schemas-module."""
    input_dir = tmp_path / "xsv_in"
    input_dir.mkdir()
    (input_dir / "YEAST_idmapping.dat").write_text("P00549\tGeneID\t851487\n", encoding="utf-8")

    settings = make_cts_settings(
        XsvIngestSettings,
        tmp_path,
        input_dir=str(input_dir),
        dataset_name="custom_xsv_ds",
        table_name=ID_MAPPING_TABLE_NAME,
        file_glob="*.dat",
        schema_file=str(IDMAPPING_SCHEMA_FILE),
        transforms_module="cdm_data_loaders.datasets.uniprot.idmapping",
        output_schemas_module="cdm_data_loaders.datasets.uniprot.idmapping",
        use_destination=LOCAL_FS,
    )

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        functools.partial(run_xsv_pipeline, settings),
        expected_pipeline_name="xsv",
        expected_dataset_name="custom_xsv_ds",
        pipeline_name_prefix="generic_xsv_idmap",
    )
    assert fetch_all_duckdb(
        load_info.pipeline,
        "SELECT uniprot_id, db_reference_type, db_reference_id FROM raw_id_mapping",
    ) == [("P00549", "GeneID", "851487")]
    assert fetch_all_duckdb(
        load_info.pipeline,
        "SELECT uniprot_id, db_reference, source FROM id_mapping",
    ) == [("uniprot:P00549", "GeneID:851487", "YEAST_idmapping.dat")]


@pytest.mark.usefixtures("qsv_cmd")
def test_run_xsv_pipeline_uniprot_idmapping_iceberg_tables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iceberg output writes both raw_id_mapping and id_mapping tables for UniProtIdMappingSettings."""
    warehouse_dir, catalog_cfg = configure_pyiceberg_home_sqlite_catalog(
        tmp_path,
        monkeypatch,
        db_filename="uniprot_iceberg_catalog.db",
    )

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    with gzip.open(input_dir / "HUMAN_9606_idmapping.dat.gz", "wt", encoding="utf-8") as gz_fh:
        gz_fh.write("P12345\tNCBI_TaxID\t9606\nQ8N158\tGeneID\t284697\n")

    settings = make_cts_settings(
        UniProtIdMappingSettings,
        tmp_path,
        input_dir=str(input_dir),
        output_dir=str(tmp_path / "cts_out"),
        use_destination=LOCAL_FS,
        loader_file_format="iceberg",
    )

    dlt_cfg = {
        "destination": {
            LOCAL_FS: {
                "destination_type": "filesystem",
                "bucket_url": str(warehouse_dir),
            }
        }
    }
    with isolated_dlt_config(config=dlt_cfg, secrets={}):
        load_info = run_xsv_pipeline(settings)

    assert load_info is not None
    assert not load_info.has_failed_jobs

    with load_catalog("default", **catalog_cfg) as catalog:
        raw_table = catalog.load_table((PIPELINE_NAME, RAW_ID_MAPPING_TABLE_NAME))
        std_table = catalog.load_table((PIPELINE_NAME, ID_MAPPING_TABLE_NAME))

        raw_records = sorted(
            raw_table.scan(selected_fields=("uniprot_id", "db_reference_type", "db_reference_id"))
            .to_arrow()
            .to_pylist(),
            key=itemgetter("uniprot_id"),
        )
        assert raw_records == [dict(row) for row in SAMPLE_RAW_ROWS]

        std_records = sorted(
            std_table.scan(selected_fields=("uniprot_id", "db_reference", "source")).to_arrow().to_pylist(),
            key=itemgetter("uniprot_id"),
        )
        assert std_records == [
            {
                "uniprot_id": "uniprot:P12345",
                "db_reference": "NCBI_TaxID:9606",
                "source": "HUMAN_9606_idmapping.dat.gz",
            },
            {
                "uniprot_id": "uniprot:Q8N158",
                "db_reference": "GeneID:284697",
                "source": "HUMAN_9606_idmapping.dat.gz",
            },
        ]
