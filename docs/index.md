# CDM Data Loaders Documentation

Data loaders, schema converters, and ingestion wranglers for the KBase Comparative Data Model (CDM).

---

## Architecture Overview

`cdm-data-loaders` separates domain logic from pipeline orchestration:

- **Core & Settings (`cdm_data_loaders.core`)**: `pydantic-settings` base class hierarchy (`CdmDataLoadersBase` -> `LoggerSettings` -> `LocalInputOutputSettings` -> `CtsSettings` -> `FileFormatIngestSettings` -> `XmlIngestSettings` -> `SingleTableXmlIngestSettings`) and declarative table/column transforms (`TransformSettingsMixin`).
- **Schema Converters (`cdm_data_loaders.converters`)**: Hub-and-spoke schema translation via a frozen Intermediate Representation (`IR`): source readers (`JsonSchemaReader`, `DltReader`, `IcebergReader`) and target emitters (`JsonSchemaEmitter`, `DltEmitter`, `PySparkEmitter`, `PyArrowEmitter`, `LinkMLEmitter`).
- **Readers & Parsers (`cdm_data_loaders.readers`, `cdm_data_loaders.parsers`)**: Streaming readers for JSONLines, XML (`xmltodict` and XSD-guided PyArrow), XSV (`qsv` + JSON Schema `x-xsv-config`), and domain parsers for UniProt/UniRef, NCBI Datasets, CheckM2, and BBMap.
- **Pipelines (`cdm_data_loaders.pipelines`)**: `dlt`-backed ingestion pipelines designed to run locally via `uv run` or in containers on the KBase Container Task Service (CTS).

---

## Guides & Specifications

### Pipeline & Execution Guides

- **[Core Pipeline & KBase CTS Execution Guide](core_cts_execution.md)**: Settings hierarchy, `CDL_*` environment variables, `dlt` destination configuration, `PipelineReport` (`report.txt`), `_load_info` and `_rejected` tables, and the KBase CTS exit-code-0 contract.
- **[CLI Reference](cli_reference.md)**: Auto-generated reference of all 18 registered `[project.scripts]` pipeline CLIs, flags, shortcuts, environment variables, and defaults.
- **[JSONLines Ingestion Pipelines](jsonlines_ingest.md)**: Guide to `jsonlines_extract`, `jsonlines_jsonschema`, and `jsonlines_pydantic`.
- **[Table & Column Transformations](table_transforms.md)**: Record-level filtering, single-table row transformation, multi-table splitting, and PyArrow schema overrides (`TransformSettingsMixin`).
- **[UniProt & UniRef Ingestion Guide](uniprot.md)**: XSD-driven XML-to-PyArrow pipelines (`uniprot`, `uniref50`, `uniref90`, `uniref100`) and normalized multi-table CDM pipelines (`uniprot_cdm`, `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm`).
- **[NCBI FTP End-to-End Walkthrough](ncbi_ftp_e2e_walkthrough.md)**: Three-phase NCBI assembly manifest, FTP download (`ncbi_ftp_sync`), and S3 Lakehouse promotion (`ncbi_ftp_promote`) workflow.
- **[Rclone Bulk Transfer Instructions](rclone_instructions.md)**: Bulk transfer setup for large datasets.

### Schema Converter Architecture & Specifications

- **[Schema Converters & Intermediate Representation (IR)](converters_ir.md)**: Reader -> IR -> Emitter architecture and invariants.
- **[JSON Schema to PyArrow Specification](jsonschema_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[Pydantic to PyArrow Specification](pydantic_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[XSD to PyArrow Specification](xsd_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.

### Code & Schema Reference

- **[Python API Reference](api/index.md)**: Auto-generated API reference for all packages and modules under `src/cdm_data_loaders/`.
- **[Bundled Schemas & XSD Assets](api/schemas.md)**: Catalog of bundled JSON Schemas and XML Schemas (`src/cdm_data_loaders/schemas/` and `parsers/`).

