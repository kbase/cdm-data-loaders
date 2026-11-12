# CDM Data Loaders Documentation

Data loaders, schema converters, and ingestion wranglers for the KBase Comparative Data Model (CDM).

---

## Architecture Overview

`cdm-data-loaders` separates domain logic from pipeline orchestration:

- **Core and Settings (`cdm_data_loaders.core`)**: `pydantic-settings` base class hierarchy (`CdmDataLoadersBase` -> `LoggerSettings` -> `LocalInputOutputSettings` -> `CtsSettings` -> `FileFormatIngestSettings` -> `XmlIngestSettings` -> `SingleTableXmlIngestSettings`) and declarative table/column transforms (`TransformSettingsMixin`).
- **Schema Converters (`cdm_data_loaders.converters`)**: Hub-and-spoke schema translation via a frozen Intermediate Representation (`IR`): source readers (`JsonSchemaReader`, `DltReader`, `IcebergReader`) and target emitters (`JsonSchemaEmitter`, `DltEmitter`, `PySparkEmitter`, `PyArrowEmitter`, `LinkMLEmitter`).
- **Readers and Parsers (`cdm_data_loaders.readers`, `cdm_data_loaders.parsers`)**: Streaming readers for JSONLines, XML (`xmltodict` and XSD-guided PyArrow), XSV (`qsv` + JSON Schema `x-xsv-config`), and domain parsers for UniProt/UniRef, NCBI Datasets, CheckM2, and BBMap.
- **Pipelines (`cdm_data_loaders.pipelines`)**: `dlt`-backed ingestion pipelines designed to run locally via `uv run` or in containers on the KBase Container Task Service (CTS).

---

## Guides and Specifications

### Pipeline and Execution Guides

- **[Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md)**: Settings hierarchy, `CDL_*` environment variables, `dlt` destination configuration, `PipelineReport` (`report.txt`), `_load_info` and `_rejected` tables, and the KBase CTS exit-code-0 contract.
- **[CLI Reference](cli_reference.md)**: Auto-generated reference of all 19 registered `[project.scripts]` pipeline CLIs, flags, shortcuts, environment variables, and defaults.
- **[Pipeline E2E and Smoke Testing Across Environments Guide](pipeline_e2e_testing_guide.md)**: Running specification-driven smoke tests across `core`, `cli`, and `docker` modes and `local_fs`, `s3_moto`, `s3_ceph`, and custom `--dlt-config-dir` / Iceberg environments.
- **[Pipeline Specifications Developer Guide](pipeline_specs_guide.md)**: Authoring and maintaining pipeline specification rules, `spec/` and `presets/` fixture manifests, and generated specification pages.
- **[Schema Converter Specifications Developer Guide](schema_converter_specs_guide.md)**: Authoring and maintaining schema converter rules, error codes (`E1..En`), fixture manifests, and generated reference pages.
- **[JSONLines Ingestion Pipelines](jsonlines_ingest.md)**: Guide to `jsonlines_extract`, `jsonlines_jsonschema`, and `jsonlines_pydantic`.
- **[Table and Column Transformations](table_transforms.md)**: Record-level filtering, single-table row transformation, multi-table splitting, and PyArrow schema overrides (`TransformSettingsMixin`).
- **[UniProt and UniRef Ingestion Guide](uniprot.md)**: XSD-driven XML-to-PyArrow pipelines (`uniprot`, `uniref50`, `uniref90`, `uniref100`) and normalized multi-table CDM pipelines (`uniprot_cdm`, `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm`).
- **[NCBI FTP End-to-End Walkthrough](ncbi_ftp_e2e_walkthrough.md)**: Three-phase NCBI assembly manifest, FTP download (`ncbi_ftp_sync`), and S3 Lakehouse promotion (`ncbi_ftp_promote`) workflow.
- **[Rclone Bulk Transfer Instructions](rclone_instructions.md)**: Bulk transfer setup for large datasets.

### Schema Converter Architecture and Specifications

- **[Schema Converters and Intermediate Representation (IR)](converters_ir.md)**: Reader -> IR -> Emitter architecture and invariants.
- **[JSON Schema to PyArrow Specification](jsonschema_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[Pydantic to PyArrow Specification](pydantic_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[XSD to PyArrow Specification](xsd_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[XSV (CSV/TSV) Schema and Pipeline Specification](xsv_specification.md)**: Auto-generated XSV schema validation, `x-xsv-config` parsing, PyArrow conversion, two-pass `qsv` cleaning/validation, DuckDB coercion, and pipeline execution specification.
- **[XML to PyArrow Pipeline Specification](xmltopyarrow_specification.md)**: Auto-generated target selection, preset pipelines, streaming XML-to-PyArrow coercion, MD5 pre-processing, dead-letter routing, and table transform specification.
- **[XML to Dict Pipeline Specification](xmltodict_specification.md)**: Auto-generated outermost `<xml_tag>` streaming, `xmltodict` parsing (`_` attribute prefix and `_xmlns` stripping), MD5 pre-processing, dead-letter routing, and table transform specification.
- **[JSONLines Extract Pipeline Specification](jsonlines_extract_specification.md)**: Auto-generated plain JSONL file discovery, `.gz` decompression, MD5 pre-processing, dead-letter routing, and table transform specification.
- **[JSONLines (JSON Schema) Pipeline Specification](jsonlines_jsonschema_specification.md)**: Auto-generated `ENTITY_SCHEMAS` registry, JSON Schema dialect validation, PyArrow schema conversion, per-line validation, dead-letter routing, and table transform specification.
- **[JSONLines (Pydantic) Pipeline Specification](jsonlines_pydantic_specification.md)**: Auto-generated `ENTITY_MODELS` registry, `PydanticToPyArrow` schema conversion, single-pass `model_validate` / `model_dump` coercion, dead-letter routing, and table transform specification.

### Code and Schema Reference

- **[Python API Reference](api/cdm_data_loaders/index.md)**: Auto-generated API reference for all packages and modules under `src/cdm_data_loaders/`.
- **[Bundled Schemas and XSD Assets](bundled_schemas.md)**: Auto-generated catalog and syntax-highlighted source pages for all non-Python schema assets bundled in `src/cdm_data_loaders/schemas/`.
