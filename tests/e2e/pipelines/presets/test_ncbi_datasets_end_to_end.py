"""End-to-end tests for the NCBI Datasets preset pipeline."""

import json
import shutil
from operator import itemgetter
from pathlib import Path
from typing import Final

import pytest

from cdm_data_loaders.datasets.ncbi_datasets.hooks import (
    ANNOTATION_REPORT_TABLE,
    DATASET_REPORT_TABLE,
)
from cdm_data_loaders.pipelines.jsonlines.jsonschema import run_jsonlines_pipeline
from cdm_data_loaders.pipelines.presets.ncbi_datasets import (
    NcbiDatasetsSettings,
)
from cdm_data_loaders.pipelines.presets.ncbi_datasets import (
    cli as ncbi_datasets_cli,
)
from tests.e2e.framework import PipelineTestRunner
from tests.helpers.cli_assertions import make_cts_settings, settings_to_argv
from tests.helpers.paths_and_constants import TEST_DATA_DIR
from tests.helpers.table_readers import assert_run_ok, read_pipeline_tables

MIN_EMPTY_RESPONSE_RATIO: Final[float] = 0.25


@pytest.mark.parametrize(
    "annotation_fixture_name",
    ["annotations.jsonl", "synthesized_annotations.jsonl"],
    ids=["three-example-responses", "synthesized-twenty-responses"],
)
def test_ncbi_datasets_pipeline_pass_loads_realistic_data_and_discards_empty_annotations(
    tmp_path: Path,
    dlt_destination_config: str,
    pipeline_runner: PipelineTestRunner,
    annotation_fixture_name: str,
) -> None:
    """ncbi_datasets loads dataset_report and annotation_report records while discarding responses without 'reports'."""
    ncbi_data_dir = TEST_DATA_DIR / "ncbi_datasets"
    source_ann_file = ncbi_data_dir / "annotation_report" / annotation_fixture_name
    source_ds_file = ncbi_data_dir / "dataset_report" / "chunk_4" / "dataset" / "dataset_reports_0000.jsonl"

    ann_records = [
        json.loads(line) for line in source_ann_file.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    expected_kept_ann = [rec for rec in ann_records if rec.get("reports")]
    expected_dropped_count = len(ann_records) - len(expected_kept_ann)
    assert expected_dropped_count >= 1
    assert expected_dropped_count / len(ann_records) >= MIN_EMPTY_RESPONSE_RATIO

    ds_records = [json.loads(line) for line in source_ds_file.read_text(encoding="utf-8").splitlines() if line.strip()]

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "bucket"
    ann_input_dir = input_dir / ANNOTATION_REPORT_TABLE
    ds_input_dir = input_dir / DATASET_REPORT_TABLE
    ann_input_dir.mkdir(parents=True)
    ds_input_dir.mkdir(parents=True)

    shutil.copyfile(source_ann_file, ann_input_dir / annotation_fixture_name)
    shutil.copyfile(source_ds_file, ds_input_dir / source_ds_file.name)

    settings = make_cts_settings(
        NcbiDatasetsSettings,
        tmp_path,
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )
    load_info = assert_run_ok(pipeline_runner.run(ncbi_datasets_cli, cli_args=settings_to_argv(settings)))
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, str(settings.loader_file_format))

    assert "_rejected" not in tables
    assert len(tables[ANNOTATION_REPORT_TABLE]) == len(expected_kept_ann)
    assert [row["total_count"] for row in tables[ANNOTATION_REPORT_TABLE]] == [
        rec["total_count"] for rec in expected_kept_ann
    ]
    assert [len(row["reports"]) for row in tables[ANNOTATION_REPORT_TABLE]] == [
        len(rec["reports"]) for rec in expected_kept_ann
    ]
    assert len(tables[DATASET_REPORT_TABLE]) == len(ds_records)
    assert [row["accession"] for row in tables[DATASET_REPORT_TABLE]] == [rec["accession"] for rec in ds_records]


def test_ncbi_datasets_pipeline_pass_routes_invalid_records_to_rejected(
    tmp_path: Path,
    dlt_destination_config: str,
) -> None:
    """Invalid JSON or schema-invalid records in ncbi_datasets route to _rejected while no-reports rows are dropped."""
    ncbi_data_dir = TEST_DATA_DIR / "ncbi_datasets"
    ann_lines = (ncbi_data_dir / "annotation_report" / "annotations.jsonl").read_text(encoding="utf-8").splitlines()

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "bucket"
    ann_input_dir = input_dir / ANNOTATION_REPORT_TABLE
    ds_input_dir = input_dir / DATASET_REPORT_TABLE
    ann_input_dir.mkdir(parents=True)
    ds_input_dir.mkdir(parents=True)

    # line 1: {"total_count": 0} (dropped), line 2: 1-report valid, line 3: invalid type for total_count
    (ann_input_dir / "batch.jsonl").write_text(
        f"{ann_lines[0]}\n{ann_lines[1]}\n{json.dumps({'total_count': 'not-an-int'})}\n",
        encoding="utf-8",
    )
    (ds_input_dir / "batch.jsonl").write_text(
        f"{json.dumps({'accession': 'GCF_000005845.2'})}\n{{malformed json\n",
        encoding="utf-8",
    )

    settings = make_cts_settings(
        NcbiDatasetsSettings,
        tmp_path,
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )
    load_info = assert_run_ok(run_jsonlines_pipeline(settings))
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, str(settings.loader_file_format))

    assert len(tables[ANNOTATION_REPORT_TABLE]) == 1
    assert tables[ANNOTATION_REPORT_TABLE][0]["total_count"] == 1
    assert len(tables[DATASET_REPORT_TABLE]) == 1
    assert tables[DATASET_REPORT_TABLE][0]["accession"] == "GCF_000005845.2"
    rejected = sorted(tables["_rejected"], key=itemgetter("table_name"))
    assert [(row["table_name"], row["line_no"]) for row in rejected] == [
        (ANNOTATION_REPORT_TABLE, 3),
        (DATASET_REPORT_TABLE, 2),
    ]
