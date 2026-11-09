# cdm-data-loaders

Repo for CDM input data loading and wrangling

- [cdm-data-loaders](#cdm-data-loaders)
  - [Documentation](#documentation)
  - [Environment and python management](#environment-and-python-management)
  - [Installation](#installation)
    - [Lakehouse and Use with Jupyter notebooks](#lakehouse-and-use-with-jupyter-notebooks)
      - [Jupyter Kernel Environment Variables](#jupyter-kernel-environment-variables)
  - [Running import pipelines](#running-import-pipelines)
    - [Container Entrypoint Modes (`scripts/entrypoint.sh`)](#container-entrypoint-modes-scriptsentrypointsh)
    - [Registered Pipeline CLIs](#registered-pipeline-clis)
  - [Development](#development)
    - [Spark and other non-python dependencies](#spark-and-other-non-python-dependencies)
    - [Tests](#tests)
      - [CEPH-Backed integration tests (CEPH + NCBI FTP)](#ceph-backed-integration-tests-ceph--ncbi-ftp)
  - [Running bbmap stats and checkm2 on genome or contigset files](#running-bbmap-stats-and-checkm2-on-genome-or-contigset-files)


## Documentation

Architecture, pipeline execution, schema conversion, and CLI reference guides live in [`docs/`](docs/):

- **[Core & CTS Pipeline Execution Guide](docs/core_cts_execution.md)**: Settings hierarchy (`CdmDataLoadersBase` / `CtsSettings`), `CDL_*` environment variables, `dlt` destination configuration (`/input_dir/dlt_conf`), output artifacts (`report.txt`, `_load_info`, `log.json`), and the KBase Container Task Service (CTS) exit-code-0 contract.
- **[CLI & Settings Reference](docs/cli_reference.md)**: Auto-generated reference for all 19 pipeline CLI entry points, their flags, shortcuts, environment variables, defaults, and settings models.
- **[JSONL Ingest Pipelines](docs/jsonlines_ingest.md)**: Guide to `jsonlines_extract`, `jsonlines_jsonschema`, and `jsonlines_pydantic`.
- **[Table Transforms Guide](docs/table_transforms.md)**: Pluggable record filtering, transformation, multi-table splitting, and PyArrow schema overrides (`--transforms-module`, `--output-schemas-module`).
- **[UniProt & UniRef Ingestion Guide](docs/uniprot.md)**: XSD-driven XML-to-PyArrow pipelines (`uniprot`, `uniref50`, `uniref90`, `uniref100`) and CDM-normalized multi-table pipelines (`uniprot_cdm`, `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm`).
- **[Converters IR & Package Architecture](docs/converters_ir.md)**: Intermediate representation (IR) contract for translating between JSON Schema, Pydantic, `dlt`, PyIceberg, PySpark, PyArrow, and LinkML.
- **[Schema Converter Specifications Developer Guide](docs/schema_converter_specs_guide.md)** & **[Pipeline Specifications Developer Guide](docs/pipeline_specs_guide.md)**: How to author, edit, and sync `specs/schema_converters/` and `specs/pipelines/` rules, error catalogs, and test cases.
- **[Pipeline E2E & Smoke Testing Across Environments Guide](docs/pipeline_e2e_testing_guide.md)**: How to run pipeline smoke tests across `local_fs`, `s3_moto`, `s3_ceph`, custom `dlt` environments (`--dlt-config-dir`, `--settings-file`), and Docker (`--run-docker`).
- **[JSON Schema to PyArrow Specification](docs/jsonschema_to_pyarrow_specification.md)**, **[Pydantic to PyArrow Specification](docs/pydantic_to_pyarrow_specification.md)** & **[XSD to PyArrow Specification](docs/xsd_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalogs.
- **[PubMed Pipeline Specification](docs/pubmed_pipeline_specification.md)** & **[NCBI Datasets Pipeline Specification](docs/ncbi_datasets_pipeline_specification.md)**: Auto-generated end-to-end smoke test specifications for `pubmed` and `ncbi_datasets`.
- **[NCBI FTP End-to-End Walkthrough](docs/ncbi_ftp_e2e_walkthrough.md)** & **[rclone Instructions](docs/rclone_instructions.md)**: Three-phase NCBI assembly synchronization and S3 Lakehouse promotion.

To build or preview the full documentation website locally (including auto-generated Python API reference pages):

```sh
uv sync --group docs
uv run --group docs mkdocs serve
```



## Environment and python management

The data loader utils package uses [uv](https://docs.astral.sh/uv/) for python environment and package management. See the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) to set up uv on your system.


## Installation

The CDM data loaders run on python 3.13 and above.

Most python code can be run using the command

```sh
> uv run <path_to_file.py>
```

This will automatically launch a virtual environment and install all required dependencies.

To manually set up the virtual environment and install dependencies (including python), run

```sh
> uv sync
```

To activate a virtual environment with these dependencies installed, run

```sh
> uv venv
# you will now be prompted to activate the virtual environment
> source .venv/bin/activate
```

If you are using IDEs like VSCode, they should pick up the creation of the new environment and offer it for executing python code.


### Lakehouse and Use with Jupyter notebooks

`cdm-data-loaders` can be installed on platforms like the KBase Lakehouse using the same installation steps:

```sh
cd cdm-data-loaders
uv sync
source .venv/bin/activate
```

To use the library in a Jupyter notebook, it must be registered as a Jupyter kernel. After performing the three steps above,
run the following commands:

```sh
uv pip install -e .
uv pip install ipykernel
uv run python -m ipykernel install --user --name cdm-data-loaders --display-name "cdm-data-loaders"
```

The `cdm-data-loaders` kernel should now be available from the dropdown list of kernels in the Jupyter notebook interface.

#### Jupyter Kernel Environment Variables

If you would like to include environment variables for your kernel that are not present in your default environment, you can add them to the `kernel.json` for your new kernel (e.g., in `cdm-data-loaders/.venv/share/jupyter/kernels/python3/kernel.json`):
```json
{
  "argv": ["..."],
  "display_name": "cdm-data-loaders",
  "language": "python",
  "env": {
    "MY_CUSTOM_VAR": "...",
    ...
  }
}
```


## Running import pipelines

Pipelines can be run directly via `uv run <pipeline_cli> [args...]` or inside the Docker container via [`scripts/entrypoint.sh`](scripts/entrypoint.sh). See the [Core & CTS Pipeline Execution Guide](docs/core_cts_execution.md) and the [CLI & Settings Reference](docs/cli_reference.md) for full configuration details.

### Container Entrypoint Modes (`scripts/entrypoint.sh`)

- `pipeline <pipeline_cli> [args...]`: copies `/input_dir/dlt_conf/{config.toml,secrets.toml,.pyiceberg.yaml}` (if present) into `$HOME/.dlt` and executes `uv run <pipeline_cli> [args...]`.
- `xml_split <n_elements> <tag> <file_1> [<file_2> ...]`: splits XML files into chunks of `<n_elements>` `<tag>` elements using `xml_split`.
- `test`: runs unit tests (`scripts/run_tests.sh unit`).
- `ceph_integration_test`: runs Ceph-backed S3 and NCBI FTP integration tests.
- `bash`: launches an interactive shell inside the container.

### Registered Pipeline CLIs

| CLI Entry Point        | Description                                                                | Source Module                                                                                                                                     | Guide                                                       |
| :--------------------- | :------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------ | :---------------------------------------------------------- |
| `all_the_bacteria`     | All The Bacteria bulk data downloader from OSF                             | [`pipelines/all_the_bacteria.py`](src/cdm_data_loaders/pipelines/all_the_bacteria.py)                                                             | [CLI Reference](docs/cli_reference.md#all_the_bacteria)     |
| `jsonlines_extract`    | Plain JSONL extraction into `<table_name>` and `_rejected`                 | [`pipelines/jsonlines/extract_pipeline.py`](src/cdm_data_loaders/pipelines/jsonlines/extract_pipeline.py)                                         | [JSONL Ingest](docs/jsonlines_ingest.md)                    |
| `jsonlines_jsonschema` | Multi-table JSONL ingestion validated against `ENTITY_SCHEMAS`             | [`pipelines/jsonlines/extract_jsonschema_validate_pipeline.py`](src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | [JSONL Ingest](docs/jsonlines_ingest.md)                    |
| `jsonlines_pydantic`   | Multi-table JSONL ingestion validated against `ENTITY_MODELS`              | [`pipelines/jsonlines/extract_pydantic_validate_pipeline.py`](src/cdm_data_loaders/pipelines/jsonlines/extract_pydantic_validate_pipeline.py)     | [JSONL Ingest](docs/jsonlines_ingest.md)                    |
| `ncbi_datasets`        | Preset NCBI Datasets JSONL ingestion (`annotation_report`, `dataset_report`) | [`pipelines/jsonlines/extract_jsonschema_validate_pipeline.py`](src/cdm_data_loaders/pipelines/jsonlines/extract_jsonschema_validate_pipeline.py) | [NCBI Datasets Spec](docs/ncbi_datasets_pipeline_specification.md) |
| `ncbi_ftp_sync`        | NCBI FTP assembly batch download (Phase 2)                                 | [`pipelines/ncbi_ftp_download.py`](src/cdm_data_loaders/pipelines/ncbi_ftp_download.py)                                                           | [NCBI FTP Walkthrough](docs/ncbi_ftp_e2e_walkthrough.md)    |
| `ncbi_ftp_promote`     | NCBI FTP staged file promotion to Lakehouse S3 (Phase 3)                   | [`pipelines/ncbi_ftp_promote.py`](src/cdm_data_loaders/pipelines/ncbi_ftp_promote.py)                                                             | [NCBI FTP Walkthrough](docs/ncbi_ftp_e2e_walkthrough.md)    |
| `pubmed`               | Preset XSD-to-PyArrow ingestion for PubMed XML (`pubmed.xsd`)              | [`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                                   | [PubMed Spec](docs/pubmed_pipeline_specification.md)        |
| `pyiceberg_to_jsonschema` | Dump PyIceberg catalog table schemas as JSON Schema documents              | [`converters/cli/pyiceberg_to_jsonschema.py`](src/cdm_data_loaders/converters/cli/pyiceberg_to_jsonschema.py)                                     | [Converters IR](docs/converters_ir.md)                      |
| `pyiceberg_to_linkml`     | Dump PyIceberg catalog table schemas as LinkML YAML schemas                | [`converters/cli/pyiceberg_to_linkml.py`](src/cdm_data_loaders/converters/cli/pyiceberg_to_linkml.py)                                             | [Converters IR](docs/converters_ir.md)                      |
| `uniprot`              | Preset XSD-to-PyArrow ingestion for UniProtKB XML (`uniprot.xsd`)          | [`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                                   | [UniProt Guide](docs/uniprot.md)                            |
| `uniprot_cdm`          | CDM-normalized multi-table UniProtKB XML pipeline                          | [`pipelines/uniprot_kb_cdm.py`](src/cdm_data_loaders/pipelines/uniprot_kb_cdm.py)                                                                 | [UniProt Guide](docs/uniprot.md)                            |
| `uniref50`             | Preset XSD-to-PyArrow ingestion for UniRef50 XML (`uniref.xsd`)            | [`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                                   | [UniProt Guide](docs/uniprot.md)                            |
| `uniref50_cdm`         | CDM-normalized multi-table UniRef50 XML pipeline                           | [`pipelines/uniref_cdm.py`](src/cdm_data_loaders/pipelines/uniref_cdm.py)                                                                         | [UniProt Guide](docs/uniprot.md)                            |
| `uniref90`             | Preset XSD-to-PyArrow ingestion for UniRef90 XML (`uniref.xsd`)            | [`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                                   | [UniProt Guide](docs/uniprot.md)                            |
| `uniref90_cdm`         | CDM-normalized multi-table UniRef90 XML pipeline                           | [`pipelines/uniref_cdm.py`](src/cdm_data_loaders/pipelines/uniref_cdm.py)                                                                         | [UniProt Guide](docs/uniprot.md)                            |
| `uniref100`            | Preset XSD-to-PyArrow ingestion for UniRef100 XML (`uniref.xsd`)           | [`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                                   | [UniProt Guide](docs/uniprot.md)                            |
| `uniref100_cdm`        | CDM-normalized multi-table UniRef100 XML pipeline                          | [`pipelines/uniref_cdm.py`](src/cdm_data_loaders/pipelines/uniref_cdm.py)                                                                         | [UniProt Guide](docs/uniprot.md)                            |
| `xmltodict`            | Generic XML-to-dictionary ingestion using `xmltodict`                      | [`pipelines/xmltodict/pipeline.py`](src/cdm_data_loaders/pipelines/xmltodict/pipeline.py)                                                         | [CLI Reference](docs/cli_reference.md#xmltodict)            |
| `xmltopyarrow`         | Generic XSD-driven XML-to-PyArrow ingestion                                | [`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)                                                   | [XSD to PyArrow Spec](docs/xsd_to_pyarrow_specification.md) |
| `xsd_to_pyarrow`       | Generate static Python PyArrow schema modules from XSD schemas             | [`converters/cli/xsd_to_pyarrow.py`](src/cdm_data_loaders/converters/cli/xsd_to_pyarrow.py)                                                       | [XSD to PyArrow Spec](docs/xsd_to_pyarrow_specification.md) |
| `xsv`                  | CSV/TSV ingestion with `qsv` two-pass validation and JSON Schema casting   | [`pipelines/xsv/pipeline.py`](src/cdm_data_loaders/pipelines/xsv/pipeline.py)                                                                     | [CLI Reference](docs/cli_reference.md#xsv)                  |


## Development


### Spark and other non-python dependencies

Some parts of this codebase rely on having a Spark instance available. Spark dependencies are pulled in by the `berdl-notebook-utils` package from [KBaseDataLakehouse/spark_notebook](https://github.com/KBaseDataLakehouse/spark_notebook), and the [Docker container](https://github.com/KBaseDataLakehouse/spark_notebook/pkgs/container/spark_notebook) generated by the same repo should be used for development and testing to mimic the container where code will be run.

Pull the docker image:

```sh
> docker pull ghcr.io/kbasedatalakehouse/spark_notebook:main
```

Mount the current directory at `/tmp/cdm` and run the tests:

```sh
> docker run --rm -e NB_USER=runner -v .:/tmp/cdm ghcr.io/kbasedatalakehouse/spark_notebook:main /bin/bash /tmp/cdm/scripts/run_tests.sh
```

Run the container interactively as the user `runner`; current directory is mounted at `/tmp/cdm`:

```sh
> docker run --rm -e NB_USER=runner -it -v .:/tmp/cdm ghcr.io/kbasedatalakehouse/spark_notebook:main
```

This will launch a bash shell; the contents of the `cdm-data-loaders` directory are mounted at `/tmp/cdm`.


Run the container and sleep:

```sh
> docker run --rm -e NB_USER=runner -it -v .:/tmp/cdm ghcr.io/kbasedatalakehouse/spark_notebook:main sleep 100000000
```

The `sleep` command will run the container for long enough that you can then connect to it via Docker Desktop or the [VSCode Containers extension](https://code.visualstudio.com/docs/containers/overview).


See the [KBaseDataLakehouse/spark_notebook](https://github.com/KBaseDataLakehouse/spark_notebook) repo for more information on the container and for a full docker-compose set up to mimic the BER Data Lakehouse container infrastructure.


### Tests

Tests are categorised using pytest markers to allow developers to execute some or all the tests. See [pyproject.toml](pyproject.toml) for the markers used.

To run all tests (requires a running Spark instance and a running CEPH test container), execute the command:

```sh
> uv run pytest
```

To run the standard test suite (excluding tests that require Spark, CEPH, or documentation verification), run

```sh
> uv run pytest -m "not requires_spark and not requires_ceph and not docs"
```

To run the documentation tests, run

```sh
> uv run pytest -m docs
```

To generate coverage for the tests, run
```sh
> uv run pytest --cov=src --cov-report=xml
```

The standard python `coverage` package is used and coverage can be generated as html or other formats by changing the parameters.


#### CEPH-Backed integration tests (CEPH + NCBI FTP)

End-to-end integration tests for the NCBI assembly pipeline live in `tests/integration/`. They exercise the full flow — manifest diffing, FTP download, S3 promote/archive — against a locally running [CEPH](https://ceph.io/) container and the real NCBI FTP server.

**Requirements:**
- Docker or Podman (for CEPH)
- Network access to `ftp.ncbi.nlm.nih.gov`

**Running with Docker Compose (easiest)**

The [docker-compose.yml](docker-compose.yml) at the repo root defines both a CEPH service and the integration test runner. To build the image, start CEPH, and run the integration tests in one command:

```sh
docker compose up --build --abort-on-container-exit
```

Compose will stream test output to the terminal and exit with the pytest exit code. To clean up afterwards:

```sh
docker compose down --volumes
```

**Running manually**

If you prefer to run the tests directly against a local CEPH instance (e.g. for faster iteration during development), follow the steps below.

**1. Start CEPH locally:**

```sh
docker run -d \
  --name ceph \
  -p 9000:8080 \
  -p 9001:8443 \
  -e RGW_PORT=8080 \
  -e RGW_ACCESS_KEY=test_access_key \
  -e RGW_SECRET_KEY=test_access_secret \
  ghcr.io/kbasetest/ceph-rgw-test-image:0.1.5
```

**2. Run the integration tests:**

```sh
# Run pure-S3 integration tests (moto-backed, fast)
uv run pytest tests/integration/ -m s3 -v

# Run NCBI FTP e2e tests (requires running CEPH instance)
uv run pytest tests/integration/ -m requires_ceph -v
```

**3. Inspect results:**

For CEPH-backed tests, buckets are **not** cleaned up after tests.

**4. Stop CEPH when done:**

```sh
docker stop ceph && docker rm ceph
```

> **Note:** These tests download real assemblies from NCBI FTP and are inherently slow (~30–60s per assembly). They are also marked `slow_test` so you can exclude them independently: `uv run pytest -m "not slow_test"`.


## Running bbmap stats and checkm2 on genome or contigset files

[`scripts/run_tools.sh`](scripts/run_tools.sh) runs the stats script from [bbmap](https://sourceforge.net/projects/bbmap/) and [checkm2](https://github.com/chklovski/CheckM2) on genome FASTA (`fna`) files listed in a genome paths JSON file (parsed downstream by [`bbmap_stats.py`](src/cdm_data_loaders/parsers/bbmap_stats.py) and [`checkm2.py`](src/cdm_data_loaders/parsers/checkm2.py)):

```json
{
    "FW305-3-2-15-C-TSA1.1": {
        "fna": "tests/data/FW305-3-2-15-C-TSA1/FW305-3-2-15-C-TSA1_scaffolds.fna",
        "gff": "tests/data/FW305-3-2-15-C-TSA1/FW305-3-2-15-C-TSA1_genes.gff",
        "protein": "tests/data/FW305-3-2-15-C-TSA1/FW305-3-2-15-C-TSA1_genes.faa"
    },
    "FW305-C-112.1": {
        "fna": "tests/data/FW305-C-112.1/FW305-C-112.1_scaffolds.fna",
        "gff": "tests/data/FW305-C-112.1/FW305-C-112.1_genes.gff",
        "protein": "tests/data/FW305-C-112.1/FW305-C-112.1_genes.faa"
    }
}
```

These tools can be installed using conda:

```sh
conda env create -f env.yml
conda activate genome_loader_env
# download the checkm2 database
checkm2 database --download
```

Run the stats and checkm2 tools with the following command:

```sh
bash scripts/run_tools.sh path/to/genome_paths_file.json output_dir
```
where `path/to/genome_paths_file.json` specifies the path to the genome paths file (format specified above) and `output_dir` is the directory for the results.
