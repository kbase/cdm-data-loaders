# CDM Data Loaders Changelog

- [CDM Data Loaders Changelog](#cdm-data-loaders-changelog)
    - [v0.1.13](#v0113)
    - [v0.1.12](#v0112)
    - [v0.1.11](#v0111)
    - [v0.1.10](#v0110)
    - [v0.1.9](#v019)
    - [v0.1.8](#v018)
    - [v0.1.7](#v017)
    - [v0.1.6](#v016)
    - [v0.1.5](#v015)
    - [v0.1.4](#v014)
    - [v0.1.3](#v013)
    - [v0.1.2](#v012)
    - [v0.1.1](#v011)
    - [v0.1.0](#v010)


### v0.1.13

#### Added
- **Schema-to-PyArrow converters**: Generate PyArrow schemas and coerce records from XSD ([`xsd_to_pyarrow.py`](src/cdm_data_loaders/converters/xsd_to_pyarrow.py)), JSON Schema ([`jsonschema_to_pyarrow.py`](src/cdm_data_loaders/converters/jsonschema_to_pyarrow.py)), and Pydantic v2 models ([`pydantic_to_pyarrow.py`](src/cdm_data_loaders/converters/pydantic_to_pyarrow.py)), or emit standalone Python schema modules via [`scripts/gen_pyarrow_schema.py`](scripts/gen_pyarrow_schema.py).
- **Custom table transforms**: Added `--transforms-module` and `--output-schemas-module` CLI parameters ([`core/transforms.py`](src/cdm_data_loaders/core/transforms.py)) to JSONL, XML, and XSV pipelines for record filtering, transformation, multi-table splitting, and PyArrow schema overrides.
- **Level-based XML ingestion**: Added `--level` to [`XmlToPyArrowSettings`](src/cdm_data_loaders/pipelines/xmltopyarrow/settings.py) to extract multiple XML element types at a specified tree depth into separate tables.
- **PubMed pipeline**: Added the `pubmed` CLI entrypoint ([`PubMedXmlToPyArrowSettings`](src/cdm_data_loaders/pipelines/xmltopyarrow/settings.py)) and bundled PubMed XSD and PyArrow schemas ([`pubmed.xsd`](src/cdm_data_loaders/schemas/pubmed/pubmed.xsd), [`schemas/pubmed/schema.py`](src/cdm_data_loaders/schemas/pubmed/schema.py)).
- **Cluster-specific UniRef entrypoints**: Replaced `uniref` and `uniref_cdm` CLIs with `uniref50`, `uniref90`, `uniref100` ([`pipelines/xmltopyarrow/pipeline.py`](src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py)) and `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm` ([`pipelines/uniref_cdm.py`](src/cdm_data_loaders/pipelines/uniref_cdm.py)).
- **Remote filesystem input**: XML, JSONL, and XSV readers accept remote filesystem inputs (such as S3) in addition to local paths ([`pipelines/core.py`](src/cdm_data_loaders/pipelines/core.py), [`utils/file_items.py`](src/cdm_data_loaders/utils/file_items.py)).
- **Documentation**: Added MkDocs site ([`mkdocs.yml`](mkdocs.yml)), CLI reference ([`docs/cli_reference.md`](docs/cli_reference.md)), table transforms guide ([`docs/table_transforms.md`](docs/table_transforms.md)), and schema-to-PyArrow converter specifications.

#### Changed
- **Pipeline reports and source paths**: `report.txt` lists ingested files ([`pipelines/core.py`](src/cdm_data_loaders/pipelines/core.py)), and `source_file` (UniProt/UniRef) and `_rejected.file` (XSV) record paths relative to `--input-dir`.
- **JSONL PyArrow schemas**: `jsonlines_jsonschema` and `jsonlines_pydantic` derive PyArrow output schemas from the input validation schemas by default ([`pipelines/jsonlines/settings.py`](src/cdm_data_loaders/pipelines/jsonlines/settings.py)).

#### Removed
- **Pipelines and CLIs**: Removed the `xml2db` pipeline, `pdb_manifest` pipeline, and `hello_world` CLI ([`pyproject.toml`](pyproject.toml)).
- **JSONL `--table-names` flag**: Removed `--table-names` from JSONL pipelines; table names come from the schema registry or `--transforms-module` ([`pipelines/jsonlines/settings.py`](src/cdm_data_loaders/pipelines/jsonlines/settings.py)).
### v0.1.12

- Dedicated UniProt and UniRef XML import pipelines using custom XSD-aware XML-to-JSON converter.

### v0.1.11

- Preliminary XSV, XML, and JSONLines importers

### v0.1.10

- Add in the ability to configure loggers using a file
- Bugfix for UniProt pipeline

### v0.1.9

- Splitting out NCBI REST API pipeline into annotation and dataset components.
- NCBI FTP pipeline and accompanying notebooks added.

### v0.1.8

- Add rotating file log handler for easier debugging.

### v0.1.7

- Add in AllTheBacteria file download client.

### v0.1.6

- Make NCBI REST API client more resilient to errors and ensure existing imports are not lost.

### v0.1.5

- Add batch size parameter to the NCBI REST API interface.


### v0.1.4

- Add in NCBI REST API interface.


### v0.1.3

- Add in file batcher for use with file-based importers.


### v0.1.2

- Update XML File Splitter to use the latest version, which includes the `gzip` parameter.

### v0.1.1

- Add [XML File Splitter](https://github.com/ialarmedalien/xml_file_splitter) to the container.

### v0.1.0

- Initial release.
