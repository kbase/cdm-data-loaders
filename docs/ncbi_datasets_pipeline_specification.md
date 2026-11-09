<!-- AUTO-GENERATED from specs/pipelines/ncbi_datasets by scripts/gen_pipeline_spec.py; do not edit by hand. Run `uv run python -m scripts.gen_pipeline_spec` to regenerate. -->

---

# NCBI Datasets Pipeline Specification

This document specifies the end-to-end contract for the `ncbi_datasets` pipeline
(`cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline:ncbi_datasets_cli`,
configured via `NcbiDatasetsSettings`):

1. **Directory-per-table discovery** under `input_dir/annotation_report/` and `input_dir/dataset_report/`.
2. **JSON Schema validation and precomputed PyArrow schemas** from `cdm_data_loaders.schemas.ncbi_datasets.ncbi_openapi`.
3. **Empty annotation report filtering** via `filter_annotation_report`.
4. **Rejection provenance and pipeline report persistence** (`_rejected`, `_load_info`, and `report.txt`).

---

## Entity Directory Discovery and Table Routing

- **`R-NCBI-ENTITY-DIRS`** — The pipeline scans `input_dir/<table_name>/` for files matching `file_glob` (default `*.jsonl*`)
across the two registered NCBI Datasets tables: `annotation_report` and `dataset_report`. *(tested in [`01_valid_and_filtered`](#case-ncbi-datasets-01-valid-and-filtered), [`02_rejected_records`](#case-ncbi-datasets-02-rejected-records))*
- **`R-NCBI-SCHEMA`** — Validated records are coerced into PyArrow tables using the precomputed schemas in
`cdm_data_loaders.schemas.ncbi_datasets.schema.SCHEMAS` (`ANNOTATION_REPORT_SCHEMA` and
`DATASET_REPORT_SCHEMA`). *(tested in [`01_valid_and_filtered`](#case-ncbi-datasets-01-valid-and-filtered), [`02_rejected_records`](#case-ncbi-datasets-02-rejected-records))*

---

## Annotation Report Filtering

- **`R-NCBI-FILTER-EMPTY`** — `filter_annotation_report` inspects each validated `annotation_report` record and drops records
where top-level `reports` is missing or empty (for example `{"total_count": 0}`). Dropped records
do not enter `annotation_report` or `_rejected`. *(tested in [`01_valid_and_filtered`](#case-ncbi-datasets-01-valid-and-filtered), [`02_rejected_records`](#case-ncbi-datasets-02-rejected-records))*
- **`R-NCBI-KEEP-NONEMPTY`** — `annotation_report` records with a non-empty `reports` list pass through `filter_annotation_report`
into the `annotation_report` table. *(tested in [`01_valid_and_filtered`](#case-ncbi-datasets-01-valid-and-filtered), [`02_rejected_records`](#case-ncbi-datasets-02-rejected-records))*

---

## JSON Parsing, Schema Validation, and Rejection Provenance

- **`R-NCBI-REJECT`** — Lines that fail JSON parsing or Draft 2020-12 JSON Schema validation are routed to `_rejected`
with `REJECTED_PYARROW_SCHEMA` (`table_name`, `source_file`, `line_no`, `raw_record`,
`error_detail`), while valid records in the same file load into their target tables. *(tested in [`02_rejected_records`](#case-ncbi-datasets-02-rejected-records))*

---

## Pipeline Metadata and Load Report

- **`R-NCBI-REPORT`** — Every completed run persists dlt load metadata to `_load_info` in the destination dataset and writes
`report.txt` containing `pipeline: ncbi_datasets`, `dataset: ncbi_datasets`, `started`, `finished`,
`status`, `load_ids`, `ingested_files`, and `pipeline_load_info_errs`. *(tested in [`01_valid_and_filtered`](#case-ncbi-datasets-01-valid-and-filtered), [`02_rejected_records`](#case-ncbi-datasets-02-rejected-records))*

---

## Smoke Test Cases

<a id="case-ncbi-datasets-01-valid-and-filtered"></a>
### `01_valid_and_filtered` — Valid Records and Empty Annotation Report Filtering

**Exercises:** `R-NCBI-ENTITY-DIRS`, `R-NCBI-SCHEMA`, `R-NCBI-FILTER-EMPTY`, `R-NCBI-KEEP-NONEMPTY`, `R-NCBI-REPORT`

Loads `annotation_report/annotations.jsonl` (one empty response `{"total_count": 0}` dropped by
`filter_annotation_report`, and one populated annotation report retained) alongside
`dataset_report/dataset_reports.jsonl` (one assembly dataset report).

**Settings (`settings.toml`)**

```toml
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input (`input/annotation_report/annotations.jsonl`)**

```json
{"total_count":0}
{"reports":[{"annotation":{"annotations":[{"assembly_accession":"GCA_000234645.4"}],"gene_type":"protein-coding","genomic_regions":[{"gene_range":{"accession_version":"JF734911.1","range":[{"begin":"562","end":"1713","orientation":"plus"}]}}],"name":"integrase/recombinase","proteins":[{"accession_version":"AET85156.1","length":383,"name":"integrase/recombinase"}],"symbol":"int"},"row_id":"1"}],"total_count":1}
```

**Input (`input/dataset_report/dataset_reports.jsonl`)**

```json
{"accession":"GCA_018350175.1","current_accession":"GCA_018350175.1","paired_accession":"GCF_018350175.1","source_database":"SOURCE_DATABASE_GENBANK","organism":{"tax_id":9685,"organism_name":"Felis catus","common_name":"domestic cat","infraspecific_names":{"isolate":"Fca126","sex":"female"}},"assembly_info":{"assembly_level":"Chromosome","assembly_status":"current","assembly_name":"F.catus_Fca126_mat1.0","assembly_type":"haploid","bioproject_accession":"PRJNA684600","release_date":"2021-05-13","submitter":"Texas A&M University","refseq_category":"reference genome","sequencing_tech":"PacBio Sequel 2","assembly_method":"NextDenovo v. 2.2-beta.0"},"assembly_stats":{"total_number_of_chromosomes":19,"total_sequence_length":"2425730029","number_of_contigs":109,"contig_n50":90731473,"contig_l50":10,"gc_percent":42.0},"wgs_info":{"wgs_project_accession":"JAFEKA01"}}
```

**Expected Report (`report.txt` summary)**

```yaml
status: completed
pipeline_load_info_errs: null
ingested_files:
- annotation_report/annotations.jsonl
- dataset_report/dataset_reports.jsonl
```

**Expected PyArrow Tables (`expected_tables.py`)**

```python
"""Expected PyArrow tables for ncbi_datasets 01_valid_and_filtered."""

import pyarrow as pa

from cdm_data_loaders.schemas.ncbi_datasets.schema import (
    ANNOTATION_REPORT_SCHEMA,
    DATASET_REPORT_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "annotation_report": pa.Table.from_pylist(
        [
            {
                "reports": [
                    {
                        "annotation": {
                            "annotations": [{"assembly_accession": "GCA_000234645.4"}],
                            "gene_type": "protein-coding",
                            "genomic_regions": [
                                {
                                    "gene_range": {
                                        "accession_version": "JF734911.1",
                                        "range": [{"begin": "562", "end": "1713", "orientation": "plus"}],
                                    }
                                }
                            ],
                            "name": "integrase/recombinase",
                            "proteins": [
                                {
                                    "accession_version": "AET85156.1",
                                    "length": 383,
                                    "name": "integrase/recombinase",
                                }
                            ],
                            "symbol": "int",
                        },
                        "row_id": "1",
                    }
                ],
                "total_count": 1,
            }
        ],
        schema=ANNOTATION_REPORT_SCHEMA,
    ),
    "dataset_report": pa.Table.from_pylist(
        [
            {
                "accession": "GCA_018350175.1",
                "current_accession": "GCA_018350175.1",
                "paired_accession": "GCF_018350175.1",
                "source_database": "SOURCE_DATABASE_GENBANK",
                "organism": {
                    "tax_id": 9685,
                    "organism_name": "Felis catus",
                    "common_name": "domestic cat",
                    "infraspecific_names": {"isolate": "Fca126", "sex": "female"},
                },
                "assembly_info": {
                    "assembly_level": "Chromosome",
                    "assembly_status": "current",
                    "assembly_name": "F.catus_Fca126_mat1.0",
                    "assembly_type": "haploid",
                    "bioproject_accession": "PRJNA684600",
                    "release_date": "2021-05-13",
                    "submitter": "Texas A&M University",
                    "refseq_category": "reference genome",
                    "sequencing_tech": "PacBio Sequel 2",
                    "assembly_method": "NextDenovo v. 2.2-beta.0",
                },
                "assembly_stats": {
                    "total_number_of_chromosomes": 19,
                    "total_sequence_length": "2425730029",
                    "number_of_contigs": 109,
                    "contig_n50": 90731473,
                    "contig_l50": 10,
                    "gc_percent": 42.0,
                },
                "wgs_info": {"wgs_project_accession": "JAFEKA01"},
            }
        ],
        schema=DATASET_REPORT_SCHEMA,
    ),
}
```

**Expected JSON (`expected.json`, non-null fields)**

```json
{
  "annotation_report": [
    {
      "reports": [
        {
          "annotation": {
            "annotations": [
              {
                "assembly_accession": "GCA_000234645.4"
              }
            ],
            "gene_type": "protein-coding",
            "genomic_regions": [
              {
                "gene_range": {
                  "accession_version": "JF734911.1",
                  "range": [
                    {
                      "begin": "562",
                      "end": "1713",
                      "orientation": "plus"
                    }
                  ]
                }
              }
            ],
            "name": "integrase/recombinase",
            "proteins": [
              {
                "accession_version": "AET85156.1",
                "length": 383,
                "name": "integrase/recombinase"
              }
            ],
            "symbol": "int"
          },
          "row_id": "1"
        }
      ],
      "total_count": 1
    }
  ],
  "dataset_report": [
    {
      "accession": "GCA_018350175.1",
      "assembly_info": {
        "assembly_level": "Chromosome",
        "assembly_method": "NextDenovo v. 2.2-beta.0",
        "assembly_name": "F.catus_Fca126_mat1.0",
        "assembly_status": "current",
        "assembly_type": "haploid",
        "bioproject_accession": "PRJNA684600",
        "refseq_category": "reference genome",
        "release_date": "2021-05-13",
        "sequencing_tech": "PacBio Sequel 2",
        "submitter": "Texas A&M University"
      },
      "assembly_stats": {
        "contig_l50": 10,
        "contig_n50": 90731473,
        "gc_percent": 42.0,
        "number_of_contigs": 109,
        "total_number_of_chromosomes": 19,
        "total_sequence_length": "2425730029"
      },
      "current_accession": "GCA_018350175.1",
      "organism": {
        "common_name": "domestic cat",
        "infraspecific_names": {
          "isolate": "Fca126",
          "sex": "female"
        },
        "organism_name": "Felis catus",
        "tax_id": 9685
      },
      "paired_accession": "GCF_018350175.1",
      "source_database": "SOURCE_DATABASE_GENBANK",
      "wgs_info": {
        "wgs_project_accession": "JAFEKA01"
      }
    }
  ]
}
```

---

<a id="case-ncbi-datasets-02-rejected-records"></a>
### `02_rejected_records` — Malformed JSON and Schema-Invalid Record Rejection (`_rejected`)

**Exercises:** `R-NCBI-ENTITY-DIRS`, `R-NCBI-SCHEMA`, `R-NCBI-FILTER-EMPTY`, `R-NCBI-KEEP-NONEMPTY`, `R-NCBI-REJECT`, `R-NCBI-REPORT`

Mixed valid, filtered, schema-invalid, and malformed JSON lines across `annotation_report/batch.jsonl`
and `dataset_report/batch.jsonl`. Valid records load into their target tables, empty `reports` rows are
discarded, and invalid lines route to `_rejected` with line-level provenance.

**Settings (`settings.toml`)**

```toml
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input (`input/annotation_report/batch.jsonl`)**

```json
{"total_count":0}
{"reports":[{"annotation":{"name":"integrase/recombinase","symbol":"int"},"row_id":"1"}],"total_count":1}
{"total_count":"not-an-int"}
```

**Input (`input/dataset_report/batch.jsonl`)**

```json
{"accession":"GCF_000005845.2","source_database":"SOURCE_DATABASE_REFSEQ"}
{malformed json
```

**Expected Report (`report.txt` summary)**

```yaml
status: completed
pipeline_load_info_errs: null
ingested_files:
- annotation_report/batch.jsonl
- dataset_report/batch.jsonl
```

**Expected PyArrow Tables (`expected_tables.py`)**

```python
"""Expected PyArrow tables for ncbi_datasets 02_rejected_records."""

import pyarrow as pa

from cdm_data_loaders.schemas.ncbi_datasets.schema import (
    ANNOTATION_REPORT_SCHEMA,
    DATASET_REPORT_SCHEMA,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EXPECTED_TABLES: dict[str, pa.Table] = {
    "annotation_report": pa.Table.from_pylist(
        [
            {
                "reports": [
                    {
                        "annotation": {"name": "integrase/recombinase", "symbol": "int"},
                        "row_id": "1",
                    }
                ],
                "total_count": 1,
            }
        ],
        schema=ANNOTATION_REPORT_SCHEMA,
    ),
    "dataset_report": pa.Table.from_pylist(
        [
            {
                "accession": "GCF_000005845.2",
                "source_database": "SOURCE_DATABASE_REFSEQ",
            }
        ],
        schema=DATASET_REPORT_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "annotation_report",
                "source_file": "annotation_report/batch.jsonl",
                "line_no": 3,
                "raw_record": '{"total_count":"not-an-int"}',
                "error_detail": '["\'not-an-int\' is not of type \'integer\'"]',
            },
            {
                "table_name": "dataset_report",
                "source_file": "dataset_report/batch.jsonl",
                "line_no": 2,
                "raw_record": "{malformed json",
                "error_detail": "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
            },
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected JSON (`expected.json`, non-null fields)**

```json
{
  "_rejected": [
    {
      "error_detail": "[\"'not-an-int' is not of type 'integer'\"]",
      "line_no": 3,
      "raw_record": "{\"total_count\":\"not-an-int\"}",
      "source_file": "annotation_report/batch.jsonl",
      "table_name": "annotation_report"
    },
    {
      "error_detail": "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
      "line_no": 2,
      "raw_record": "{malformed json",
      "source_file": "dataset_report/batch.jsonl",
      "table_name": "dataset_report"
    }
  ],
  "annotation_report": [
    {
      "reports": [
        {
          "annotation": {
            "name": "integrase/recombinase",
            "symbol": "int"
          },
          "row_id": "1"
        }
      ],
      "total_count": 1
    }
  ],
  "dataset_report": [
    {
      "accession": "GCF_000005845.2",
      "source_database": "SOURCE_DATABASE_REFSEQ"
    }
  ]
}
```
