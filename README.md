# cdm-data-loaders

Data loaders, schema converters, and ingestion wranglers for the KBase Common Data Model (CDM).

- [cdm-data-loaders](#cdm-data-loaders)
  - [Documentation](#documentation)
  - [Environment and python management](#environment-and-python-management)
  - [Installation](#installation)
    - [Lakehouse and Use with Jupyter notebooks](#lakehouse-and-use-with-jupyter-notebooks)
      - [Jupyter Kernel Environment Variables](#jupyter-kernel-environment-variables)
  - [Running import pipelines](#running-import-pipelines)
    - [Container Entrypoint Modes (`scripts/entrypoint.sh`)](#container-entrypoint-modes-scriptsentrypointsh)
    - [Quick-Start CLI Execution](#quick-start-cli-execution)
  - [Running bbmap stats and checkm2 on genome or contigset files](#running-bbmap-stats-and-checkm2-on-genome-or-contigset-files)


## Documentation

Full user guides, specifications, developer guides, and API reference are published on the documentation website:

- **[Home](https://kbase.github.io/cdm-data-loaders/)**: documentation overview and package layout.
- **[User Guides](https://kbase.github.io/cdm-data-loaders/#user-guides)**: running pipelines and CTS containers, CLI reference, custom transforms and file pre-processors, supplying schemas, schema converters, UniProt and UniRef, NCBI FTP synchronization, and `rclone` bulk transfers.
- **[Specifications](https://kbase.github.io/cdm-data-loaders/#specifications)**: schema converter and pipeline behavioral rules, test cases, and error catalogs.
- **[Developer Guides](https://kbase.github.io/cdm-data-loaders/#developer-guides)**: development environment, running unit, Spark, Ceph, and smoke tests, authoring converter and pipeline specifications, and building documentation.
- **[API and Schema Reference](https://kbase.github.io/cdm-data-loaders/#api-and-schema-reference)**: auto-generated Python API reference and bundled XSD and JSON Schema assets.


## Environment and python management

The data loader utils package uses [uv](https://docs.astral.sh/uv/) for python environment and package management. See the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) to set up uv on your system.


## Installation

The CDM data loaders run on python 3.13 and above.

Most python code can be run using the command

```sh
uv run <path_to_file.py>
```

This will automatically launch a virtual environment and install all required dependencies.

To manually set up the virtual environment and install dependencies (including python), run

```sh
uv sync
```

To activate a virtual environment with these dependencies installed, run

```sh
uv venv
# you will now be prompted to activate the virtual environment
source .venv/bin/activate
```

If you are using IDEs like VSCode, they should pick up the creation of the new environment and offer it for executing python code.


### Lakehouse and Use with Jupyter notebooks

`cdm-data-loaders` can be installed on platforms like the KBase Lakehouse using the same installation steps:

```sh
cd cdm-data-loaders
uv sync
source .venv/bin/activate
```

To use the library in a Jupyter notebook, it must be registered as a Jupyter kernel. After performing the steps above, run the following commands:

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
    "MY_CUSTOM_VAR": "..."
  }
}
```


## Running import pipelines

Pipelines and schema converter CLIs can be run directly via `uv run <cli_command> [OPTIONS]` or inside the Docker container via [`scripts/entrypoint.sh`](scripts/entrypoint.sh). See the [User Guides](https://kbase.github.io/cdm-data-loaders/#user-guides) and the [CLI Reference](https://kbase.github.io/cdm-data-loaders/cli_reference/) for full configuration details.

### Container Entrypoint Modes (`scripts/entrypoint.sh`)

- `pipeline <cli_command> [args...]`: copies `/input_dir/dlt_conf/{config.toml,secrets.toml,.pyiceberg.yaml}` (if present) into `$HOME/.dlt` and executes `uv run <cli_command> [args...]`.
- `xml_split <n_elements> <tag> <file_1> [<file_2> ...]`: splits XML files into chunks of `<n_elements>` `<tag>` elements using `xml_split`.
- `bash`: launches an interactive shell inside the container.

Container test runner modes (`test` and `ceph_integration_test`) are covered in the [Developer Guides](https://kbase.github.io/cdm-data-loaders/#developer-guides).

### Quick-Start CLI Execution

All CLI entry points are registered under `[project.scripts]` in [`pyproject.toml`](pyproject.toml) and documented in the [CLI Reference](https://kbase.github.io/cdm-data-loaders/cli_reference/):

```sh
# inspect options and CDL_* environment variables for any registered CLI
uv run xsv --help

# run a tabular XSV ingestion pipeline into a local filesystem destination
uv run xsv \
  --input-dir ./input_data \
  --output-dir ./output_data \
  --dataset-name my_dataset \
  --table-name sample_metadata \
  --schema-file ./sample_metadata.schema.json \
  --use-destination local_fs

# generate a static PyArrow schema module from an XSD schema
uv run xsd_to_pyarrow \
  --xsd-file src/cdm_data_loaders/schemas/uniprot/uniprot.xsd \
  --xml-tag entry \
  --output ./uniprot_schema.py
```


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
