<!-- GENERATED FILE - DO NOT EDIT. Source: `pyproject.toml`, `src/cdm_data_loaders/`. Regenerate with `uv run python -m scripts.gen_docs`. -->

# CLI Reference

Auto-generated reference for all registered pipeline CLIs in `pyproject.toml` (`[project.scripts]`).

- **Local execution**: `uv run <cli_command> [OPTIONS]`
- **Container execution**: `./scripts/entrypoint.sh pipeline <cli_command> [OPTIONS]`
- **Environment variables**: Every CLI option can also be configured via its `CDL_*` environment variable.
- **Execution & CTS semantics**: See [Core Pipeline & KBase CTS Execution Guide](core_cts_execution.md).

## Registered Pipeline CLIs

| CLI Command | Settings Class | Entry Point | Summary |
| :--- | :--- | :--- | :--- |
| [`all_the_bacteria`](#all_the_bacteria) | `AtbSettings` | `cdm_data_loaders.pipelines.all_the_bacteria:cli` | Configuration for running the AllTheBacteria import pipeline. |
| [`jsonlines_extract`](#jsonlines_extract) | `JsonlExtractSettings` | `cdm_data_loaders.pipelines.jsonlines.extract_pipeline:cli` | Settings for the JSONL extraction-only pipeline. |
| [`jsonlines_jsonschema`](#jsonlines_jsonschema) | `JsonlJsonschemaIngestSettings` | `cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline:cli` | Pipeline settings for JSONL ingestion with JSON Schema validation. |
| [`jsonlines_pydantic`](#jsonlines_pydantic) | `JsonlPydanticIngestSettings` | `cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline:cli` | Pipeline settings for JSONL pipeline that uses Pydantic for validation. |
| [`ncbi_datasets`](#ncbi_datasets) | `NcbiDatasetsSettings` | `cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline:ncbi_datasets_cli` | Pipeline settings for NCBI Datasets JSONL ingestion (`annotation_report` and `dataset_report`). |
| [`ncbi_ftp_promote`](#ncbi_ftp_promote) | `PromoteSettings` | `cdm_data_loaders.pipelines.ncbi_ftp_promote:cli` | Configuration for the NCBI FTP file promotion pipeline. |
| [`ncbi_ftp_sync`](#ncbi_ftp_sync) | `DownloadSettings` | `cdm_data_loaders.pipelines.ncbi_ftp_download:cli` | Configuration for the NCBI FTP assembly download pipeline. |
| [`pubmed`](#pubmed) | `PubMedXmlToPyArrowSettings` | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:pubmed_cli` | Settings for the PubMed XML to PyArrow ingestion pipeline. |
| [`pyiceberg_to_jsonschema`](#pyiceberg_to_jsonschema) | `IcebergToJsonSchemaSettings` | `cdm_data_loaders.converters.cli.pyiceberg_to_jsonschema:cli` | Settings for generating a set of JSONSchemas from an Iceberg data catalog. |
| [`pyiceberg_to_linkml`](#pyiceberg_to_linkml) | `IcebergToLinkMLSettings` | `cdm_data_loaders.converters.cli.pyiceberg_to_linkml:cli` | Settings for generating a set of LinkML schemas from an Iceberg data catalog. |
| [`uniprot`](#uniprot) | `UniProtXmlToPyArrowSettings` | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniprot_cli` | Settings for the UniProt XML to PyArrow ingestion pipeline. |
| [`uniprot_cdm`](#uniprot_cdm) | `UniProtSettings` | `cdm_data_loaders.pipelines.uniprot_kb_cdm:cli` | Configuration for running the UniProt KB CDM XML-to-PyArrow import pipeline. |
| [`uniref100`](#uniref100) | `UniRef100XmlToPyArrowSettings` | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniref100_cli` | UniRef 100 XML to PyArrow ingest settings. |
| [`uniref100_cdm`](#uniref100_cdm) | `UniRef100CdmSettings` | `cdm_data_loaders.pipelines.uniref_cdm:uniref100_cdm_cli` | UniRef 100 CDM XML-to-PyArrow ingest settings. |
| [`uniref50`](#uniref50) | `UniRef50XmlToPyArrowSettings` | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniref50_cli` | UniRef 50 XML to PyArrow ingest settings. |
| [`uniref50_cdm`](#uniref50_cdm) | `UniRef50CdmSettings` | `cdm_data_loaders.pipelines.uniref_cdm:uniref50_cdm_cli` | UniRef 50 CDM XML-to-PyArrow ingest settings. |
| [`uniref90`](#uniref90) | `UniRef90XmlToPyArrowSettings` | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniref90_cli` | UniRef 90 XML to PyArrow ingest settings. |
| [`uniref90_cdm`](#uniref90_cdm) | `UniRef90CdmSettings` | `cdm_data_loaders.pipelines.uniref_cdm:uniref90_cdm_cli` | UniRef 90 CDM XML-to-PyArrow ingest settings. |
| [`xmltodict`](#xmltodict) | `XmlToDictSettings` | `cdm_data_loaders.pipelines.xmltodict.pipeline:cli` | Settings for the XML ingestion pipeline. |
| [`xmltopyarrow`](#xmltopyarrow) | `XmlToPyArrowSettings` | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:cli` | Settings for the XML to PyArrow ingestion pipeline. |
| [`xsd_to_pyarrow`](#xsd_to_pyarrow) | `XsdToPyArrowCliSettings` | `cdm_data_loaders.converters.cli.xsd_to_pyarrow:cli` | Settings for generating a static Python PyArrow schema module from an XSD file. |
| [`xsv`](#xsv) | `XsvIngestSettings` | `cdm_data_loaders.pipelines.xsv.pipeline:cli` | Settings for the XSV (CSV/TSV) ingestion pipeline. |

## `all_the_bacteria`

Configuration for running the AllTheBacteria import pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.all_the_bacteria:cli`
- **Settings hierarchy**: `AtbSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--version` | `-v` | `CDL_VERSION` | `str` | `'2025-05'` | Name of the current AllTheBacteria version |
| `--pattern-file` | — | `CDL_PATTERN_FILE` | `Path \| None` | `None` | Path to a file containing patterns to match when downloading ATB files |

## `jsonlines_extract`

Settings for the JSONL extraction-only pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.jsonlines.extract_pipeline:cli`
- **Settings hierarchy**: `JsonlExtractSettings` -> `JsonlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--dataset-name` | — | `CDL_DATASET_NAME` | `str` | *(Required)* | The name of the dataset being produced |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.jsonl*'` | Glob pattern for JSONL files |
| `--table-name` | `-t` | `CDL_TABLE_NAME` | `str` | *(Required)* | The name of the table to export parsed data to |

## `jsonlines_jsonschema`

Pipeline settings for JSONL ingestion with JSON Schema validation.

- **Entry point**: `cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline:cli`
- **Settings hierarchy**: `JsonlJsonschemaIngestSettings` -> `JsonlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--dataset-name` | — | `CDL_DATASET_NAME` | `str` | *(Required)* | The name of the dataset being produced |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.jsonl*'` | Glob pattern for JSONL files |
| `--entity-schemas` | `-m` | `CDL_ENTITY_SCHEMAS` | `str` | *(Required)* | Dotted import path to a module that defines `ENTITY_SCHEMAS: dict[str, dict]`. Each key is a table name and a subdirectory of `input_dir`. Each value is a JSON Schema dictionary declaring its draft with `$schema`. |

## `jsonlines_pydantic`

Pipeline settings for JSONL pipeline that uses Pydantic for validation.

- **Entry point**: `cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline:cli`
- **Settings hierarchy**: `JsonlPydanticIngestSettings` -> `JsonlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--dataset-name` | — | `CDL_DATASET_NAME` | `str` | *(Required)* | The name of the dataset being produced |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.jsonl*'` | Glob pattern for JSONL files |
| `--entity-models` | `-m` | `CDL_ENTITY_MODELS` | `str` | *(Required)* | Dotted import path to a module that defines `ENTITY_MODELS: dict[str, type[BaseModel]]`. Each key is a table name and a subdirectory of `input_dir`. Each value is the Pydantic model for that table. |

## `ncbi_datasets`

Pipeline settings for NCBI Datasets JSONL ingestion (`annotation_report` and `dataset_report`).

- **Entry point**: `cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline:ncbi_datasets_cli`
- **Settings hierarchy**: `NcbiDatasetsSettings` -> `JsonlJsonschemaIngestSettings` -> `JsonlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.jsonl*'` | Glob pattern for JSONL files |

## `ncbi_ftp_promote`

Configuration for the NCBI FTP file promotion pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.ncbi_ftp_promote:cli`
- **Settings hierarchy**: `PromoteSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--staging-bucket` | — | `CDL_STAGING_BUCKET` | `PurePosixPath` | `cts` | Bucket where staged files are located after download |
| `--destination-bucket` | — | `CDL_DESTINATION_BUCKET` | `PurePosixPath` | `cdm-lake` | Bucket to which staged files will be promoted. |
| `--staging-path` | `-s` | `CDL_STAGING_PATH` | `PurePosixPath` | *(Required)* | Path to folder in the staging bucket where staged files are located; Should contain `raw_data/` folder |
| `--destination-path` | — | `CDL_DESTINATION_PATH` | `PurePosixPath` | `tenant-general-warehouse/kbase/datasets/ncbi` | Path to folder in the destination bucket where files will be promoted to; Will contain `raw_data/` folder |
| `--removed-manifest-path` | `-r`, `--removed-manifest`, `--removed_manifest` | `CDL_REMOVED_MANIFEST_PATH` | `Path \| None` | `None` | Local filesystem path to the removed files manifest from Phase 1, or None to skip archiving removed records |
| `--updated-manifest-path` | `-u`, `--updated-manifest`, `--updated_manifest` | `CDL_UPDATED_MANIFEST_PATH` | `Path \| None` | `None` | Local filesystem path to the updated files manifest from Phase 1, or None to skip archiving updated records |
| `--transfer-manifest-path` | `-t`, `--transfer-manifest`, `--transfer_manifest` | `CDL_TRANSFER_MANIFEST_PATH` | `PurePosixPath \| None` | *(Dynamic)* | S3 object key of the transfer manifest to trim after promotion, or None to skip pruning staged files |
| `--dry-run` | — | `CDL_DRY_RUN` | `bool` | `False` | Log actions without making changes |

## `ncbi_ftp_sync`

Configuration for the NCBI FTP assembly download pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.ncbi_ftp_download:cli`
- **Settings hierarchy**: `DownloadSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--manifest` | `-m` | `CDL_MANIFEST` | `Path` | `/input_dir/transfer_manifest.txt` | Path to the transfer manifest file listing FTP paths to download |
| `--output-dir` | — | `CDL_OUTPUT_DIR` | `Path` | `/output_dir` | Output directory for downloaded assembly files |
| `--threads` | `-t` | `CDL_THREADS` | `int` | `4` | Number of parallel download threads |
| `--ftp-host` | — | `CDL_FTP_HOST` | `str` | `'ftp.ncbi.nlm.nih.gov'` | NCBI FTP hostname |
| `--limit` | `-l` | `CDL_LIMIT` | `int \| None` | `None` | Limit to first N assemblies (for testing) |

## `pubmed`

Settings for the PubMed XML to PyArrow ingestion pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:pubmed_cli`
- **Settings hierarchy**: `PubMedXmlToPyArrowSettings` -> `LevelPresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml.gz'` | Glob pattern for PubMed XML files |
| `--root-tag` | — | `CDL_ROOT_TAG` | `str \| None` | `None` | Optional root XML element tag name used to anchor level-based XSD element resolution. |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/pubmed/pubmed.xsd` | Path to the PubMed XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `pyiceberg_to_jsonschema`

Settings for generating a set of JSONSchemas from an Iceberg data catalog.

- **Entry point**: `cdm_data_loaders.converters.cli.pyiceberg_to_jsonschema:cli`
- **Settings hierarchy**: `IcebergToJsonSchemaSettings` -> `IcebergCatalogDumpSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--catalog` | `-c` | `CDL_CATALOG` | `str` | *(Required)* | Name of the catalog to retrieve schemas from |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--group-by-namespace` | `-g` | `CDL_GROUP_BY_NAMESPACE` | `bool` | `False` | Export one JSON Schema per namespace |

## `pyiceberg_to_linkml`

Settings for generating a set of LinkML schemas from an Iceberg data catalog.

- **Entry point**: `cdm_data_loaders.converters.cli.pyiceberg_to_linkml:cli`
- **Settings hierarchy**: `IcebergToLinkMLSettings` -> `IcebergCatalogDumpSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--catalog` | `-c` | `CDL_CATALOG` | `str` | *(Required)* | Name of the catalog to retrieve schemas from |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--group-by-namespace` | `-g` | `CDL_GROUP_BY_NAMESPACE` | `bool` | `False` | Export one LinkML schema per namespace |

## `uniprot`

Settings for the UniProt XML to PyArrow ingestion pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniprot_cli`
- **Settings hierarchy**: `UniProtXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniprot/uniprot.xsd` | Path to the UniProt XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniprot_cdm`

Configuration for running the UniProt KB CDM XML-to-PyArrow import pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.uniprot_kb_cdm:cli`
- **Settings hierarchy**: `UniProtSettings` -> `UniProtXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniprot/uniprot.xsd` | Path to the UniProt XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniref100`

UniRef 100 XML to PyArrow ingest settings.

- **Entry point**: `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniref100_cli`
- **Settings hierarchy**: `UniRef100XmlToPyArrowSettings` -> `UniRefXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniref/uniref.xsd` | Path to the UniRef XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniref100_cdm`

UniRef 100 CDM XML-to-PyArrow ingest settings.

- **Entry point**: `cdm_data_loaders.pipelines.uniref_cdm:uniref100_cdm_cli`
- **Settings hierarchy**: `UniRef100CdmSettings` -> `UniRefCdmSettings` -> `UniRefXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `10000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniref/uniref.xsd` | Path to the UniRef XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniref50`

UniRef 50 XML to PyArrow ingest settings.

- **Entry point**: `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniref50_cli`
- **Settings hierarchy**: `UniRef50XmlToPyArrowSettings` -> `UniRefXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniref/uniref.xsd` | Path to the UniRef XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniref50_cdm`

UniRef 50 CDM XML-to-PyArrow ingest settings.

- **Entry point**: `cdm_data_loaders.pipelines.uniref_cdm:uniref50_cdm_cli`
- **Settings hierarchy**: `UniRef50CdmSettings` -> `UniRefCdmSettings` -> `UniRefXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `10000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniref/uniref.xsd` | Path to the UniRef XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniref90`

UniRef 90 XML to PyArrow ingest settings.

- **Entry point**: `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:uniref90_cli`
- **Settings hierarchy**: `UniRef90XmlToPyArrowSettings` -> `UniRefXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniref/uniref.xsd` | Path to the UniRef XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `uniref90_cdm`

UniRef 90 CDM XML-to-PyArrow ingest settings.

- **Entry point**: `cdm_data_loaders.pipelines.uniref_cdm:uniref90_cdm_cli`
- **Settings hierarchy**: `UniRef90CdmSettings` -> `UniRefCdmSettings` -> `UniRefXmlToPyArrowSettings` -> `PresetXmlToPyArrowSettings` -> `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `10000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | `src/cdm_data_loaders/schemas/uniref/uniref.xsd` | Path to the UniRef XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `xmltodict`

Settings for the XML ingestion pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.xmltodict.pipeline:cli`
- **Settings hierarchy**: `XmlToDictSettings` -> `SingleTableXmlIngestSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--dataset-name` | — | `CDL_DATASET_NAME` | `str` | *(Required)* | The name of the dataset being produced |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--table-name` | — | `CDL_TABLE_NAME` | `str` | *(Required)* | The name of the table to export parsed data to |
| `--xml-tag` | — | `CDL_XML_TAG` | `str` | *(Required)* | XML tag to capture the contents of |

## `xmltopyarrow`

Settings for the XML to PyArrow ingestion pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.xmltopyarrow.pipeline:cli`
- **Settings hierarchy**: `XmlToPyArrowSettings` -> `XmlIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--dataset-name` | — | `CDL_DATASET_NAME` | `str` | *(Required)* | The name of the dataset being produced |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*.xml*'` | Glob pattern for XML files |
| `--table-name` | — | `CDL_TABLE_NAME` | `str \| None` | `None` | The name of the table to export parsed data to (required when xml_level is not set). |
| `--xml-tag` | — | `CDL_XML_TAG` | `str \| None` | `None` | XML tag to capture the contents of (required when xml_level is not set). |
| `--xml-level` | — | `CDL_XML_LEVEL` | `int \| None` | `None` | XML depth level relative to the root element to extract all elements from and assign to tables by tag name. |
| `--root-tag` | — | `CDL_ROOT_TAG` | `str \| None` | `None` | Optional root XML element tag name used to anchor level-based XSD element resolution. |
| `--xsd-file` | — | `CDL_XSD_FILE` | `Path` | *(Required)* | Path to the XSD file describing the XML documents to be imported. |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth for self-referential XSD types before serializing deeper content as JSON. |

## `xsd_to_pyarrow`

Settings for generating a static Python PyArrow schema module from an XSD file.

- **Entry point**: `cdm_data_loaders.converters.cli.xsd_to_pyarrow:cli`
- **Settings hierarchy**: `XsdToPyArrowCliSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--xsd-file` | `-x` | `CDL_XSD_FILE` | `Path` | *(Required)* | Path to the input XSD file |
| `--output` | `-o` | `CDL_OUTPUT` | `Path` | *(Required)* | Destination Python file path for the generated schema module |
| `--xml-level` | `-v` | `CDL_XML_LEVEL` | `int` | `1` | Element depth relative to root (default: 1) |
| `--xml-tag` | `-m` | `CDL_XML_TAG` | `str \| None` | `None` | Single target XML tag (overrides --xml-level) |
| `--root-tag` | `-r` | `CDL_ROOT_TAG` | `str \| None` | `None` | Explicit root XML tag for level discovery |
| `--max-recursion-depth` | — | `CDL_MAX_RECURSION_DEPTH` | `int` | `2` | Maximum recursion depth before JSON string truncation |

## `xsv`

Settings for the XSV (CSV/TSV) ingestion pipeline.

- **Entry point**: `cdm_data_loaders.pipelines.xsv.pipeline:cli`
- **Settings hierarchy**: `XsvIngestSettings` -> `FileFormatIngestSettings` -> `CtsSettings` -> `InputLocalOutputSettings` -> `LoggerSettings` -> `CdmDataLoadersBase`

| CLI Flag | Shortcut | Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `--preprocessors-module` | — | `CDL_PREPROCESSORS_MODULE` | `str \| None` | `None` | Optional Python module path exporting FILE_PREPROCESSORS (Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] \| FilePreprocessFn]) for file-level validation and filtering before opening and parsing. |
| `--transforms-module` | — | `CDL_TRANSFORMS_MODULE` | `str \| None` | `None` | Optional Python module path exporting TABLE_TRANSFORMS (dict[str, RecordTransformFn]) for record filtering, transformation, and multi-table splitting. |
| `--output-schemas-module` | — | `CDL_OUTPUT_SCHEMAS_MODULE` | `str \| None` | `None` | Optional Python module path exporting OUTPUT_SCHEMAS (dict[str, pyarrow.Schema]) for PyArrow schema overrides. |
| `--log-config-file` | — | `CDL_LOG_CONFIG_FILE` | `str \| None` | `None` | Location of configuration file for the logger |
| `--input-dir` | `-i` | `CDL_INPUT_DIR` | `str` | *(Required)* | Local directory or fsspec URL containing file(s) to import |
| `--output-dir` | `-o` | `CDL_OUTPUT_DIR` | `Path` | *(Required)* | Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data. |
| `--buffer-size` | — | `CDL_BUFFER_SIZE` | `int` | `100` | Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer. |
| `--dlt-dev-mode` | — | `CDL_DLT_DEV_MODE` | `bool` | `False` | Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run. |
| `--log-interval` | — | `CDL_LOG_INTERVAL` | `int` | `1000` | How often (in number of processed entries) to emit a progress log message. |
| `--use-destination` | `-d` | `CDL_USE_DESTINATION` | `str` | *(Required)* | Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config. |
| `--use-output-dir-for-pipeline-metadata` | `-p` | `CDL_USE_OUTPUT_DIR_FOR_PIPELINE_METADATA` | `bool` | `False` | Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem. |
| `--dataset-name` | — | `CDL_DATASET_NAME` | `str` | *(Required)* | The name of the dataset being produced |
| `--loader-file-format` | `-f` | `CDL_LOADER_FILE_FORMAT` | `'iceberg' \| 'jsonl' \| 'parquet'` | `'parquet'` | Format of the output files. Choices: ['iceberg', 'jsonl', 'parquet'] |
| `--file-glob` | `-g` | `CDL_FILE_GLOB` | `str` | `'*'` | File glob for input files |
| `--schema-file` | `-s` | `CDL_SCHEMA_FILE` | `Path` | *(Required)* | Path to a JSON Schema file describing the XSV data's columns and types. May include an x-xsv-config block with xsv parser config metadata. |
| `--table-name` | `-t` | `CDL_TABLE_NAME` | `str` | *(Required)* | The name of the table to export parsed data to |
