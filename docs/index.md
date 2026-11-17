# CDM Data Loaders Documentation

Data loaders, schema converters, and ingestion wranglers for the KBase Common Data Model (CDM).

`cdm-data-loaders` provides CLI entry points (registered under `[project.scripts]` in [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml)) and Python APIs for:

- **Ingestion pipelines** ([`src/cdm_data_loaders/pipelines`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines)): `dlt`-backed data loaders for JSONLines, XSV (CSV/TSV), XML, UniProt/UniRef, All The Bacteria, and NCBI FTP assemblies, designed to run locally via `uv run` or in containers on the KBase CDM Task Service (CTS).
- **Schema converters** ([`src/cdm_data_loaders/converters`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters)): translation CLIs and Python facades across JSON Schema, Pydantic, XSD, PyArrow, PyIceberg, LinkML, `dlt`, and PySpark.
- **Streaming readers, parsers, and bundled schemas** ([`src/cdm_data_loaders/readers`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/readers), [`src/cdm_data_loaders/parsers`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers), [`src/cdm_data_loaders/schemas`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas)): streaming file readers, domain parsers, and bundled XSD and JSON Schema assets.

---

## User Guides

- **[CLI Reference](cli_reference.md)**: Auto-generated reference of all registered `[project.scripts]` CLIs, flags, shortcuts, environment variables, and defaults.
- **[Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md)**: Running pipelines locally and in CTS containers, `CDL_*` environment variables, `dlt` and Iceberg destination setup, `report.txt`, `_load_info` and `_rejected` tables, and the CTS exit-code-0 contract.
- **[Table and Column Transformations](table_transforms.md)**: Custom file pre-processors (`FILE_PREPROCESSORS`), record-level filtering, single-table row transformation, multi-table splitting (`TABLE_TRANSFORMS`), and PyArrow schema overrides (`OUTPUT_SCHEMAS`).
- **[JSONLines Ingestion Pipelines](jsonlines_ingest.md)**: Running `jsonlines_extract`, `jsonlines_jsonschema`, `jsonlines_pydantic`, and `ncbi_datasets` with custom schema and model registries.
- **[UniProt and UniRef Ingestion Guide](uniprot.md)**: XSD-driven XML-to-PyArrow pipelines (`uniprot`, `uniref50`, `uniref90`, `uniref100`), CDM-normalized multi-table pipelines (`uniprot_cdm`, `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm`), the `uniprot_idmapping` XSV preset pipeline, and metalink/release-note parsers.
- **[Schema Converters Guide](schema_converters.md)**: Running `xsd_to_pyarrow`, `pyiceberg_to_jsonschema`, and `pyiceberg_to_linkml` from the CLI, calling converter facades from Python, and registering custom `x-` extensions.
- **[NCBI FTP End-to-End Walkthrough](ncbi_ftp_e2e_walkthrough.md)**: Three-phase NCBI assembly manifest generation, FTP download (`ncbi_ftp_sync`), and S3 Lakehouse promotion (`ncbi_ftp_promote`).
- **[Rclone Bulk Transfer Instructions](rclone_instructions.md)**: Configuring `rclone` remotes and running high-throughput bulk transfers to Lakehouse S3 buckets.

## Specifications

### Schema Converter Architecture and Specifications

- **[JSON Schema to PyArrow Specification](jsonschema_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[Pydantic to PyArrow Specification](pydantic_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[XSD to PyArrow Specification](xsd_to_pyarrow_specification.md)**: Auto-generated conversion rules, test cases, and error catalog.
- **[PyArrow to JSON Schema Specification](pyarrow_to_jsonschema_specification.md)**: Auto-generated conversion rules, `x-pyarrow` extension contract, test cases, and error catalog.
- **[PyArrow to LinkML Specification](pyarrow_to_linkml_specification.md)**: Auto-generated conversion rules, LinkML slot/class mapping, test cases, and error catalog.

### Pipeline Specifications

- **[XSV (CSV/TSV) Schema and Pipeline Specification](xsv_specification.md)**: Auto-generated XSV schema validation, `x-xsv-config` parsing, PyArrow conversion, two-pass `qsv` cleaning/validation, DuckDB coercion, `uniprot_idmapping` preset, and pipeline execution specification.
- **[XML to PyArrow Pipeline Specification](xmltopyarrow_specification.md)**: Auto-generated target selection, preset pipelines, streaming XML-to-PyArrow coercion, MD5 pre-processing, dead-letter routing, and table transform specification.
- **[XML to Dict Pipeline Specification](xmltodict_specification.md)**: Auto-generated outermost `<xml_tag>` streaming, `xmltodict` parsing (`_` attribute prefix and `_xmlns` stripping), MD5 pre-processing, dead-letter routing, and table transform specification.
- **[JSONLines Extract Pipeline Specification](jsonlines_extract_specification.md)**: Auto-generated plain JSONL file discovery, `.gz` decompression, MD5 pre-processing, dead-letter routing, and table transform specification.
- **[JSONLines (JSON Schema) Pipeline Specification](jsonlines_jsonschema_specification.md)**: Auto-generated `ENTITY_SCHEMAS` registry, JSON Schema dialect validation, PyArrow schema conversion, per-line validation, dead-letter routing, and table transform specification.
- **[JSONLines (Pydantic) Pipeline Specification](jsonlines_pydantic_specification.md)**: Auto-generated `ENTITY_MODELS` registry, `PydanticToPyArrow` schema conversion, single-pass `model_validate` and `coerce_pydantic_record_for_pyarrow` coercion, dead-letter routing, and table transform specification.

## Developer Guides

- **[Development and Testing Guide](development_and_testing.md)**: Local development setup, Spark container workflows, `pytest` markers and coverage, Ceph S3 and NCBI FTP integration tests, and building/verifying the documentation site.
- **[Pipeline E2E and Smoke Testing Across Environments Guide](pipeline_e2e_testing_guide.md)**: Running specification-driven smoke tests across `core`, `cli`, and `docker` modes and `local_fs`, `s3_moto`, `s3_ceph`, and custom `--dlt-config-dir` / Iceberg environments.
- **[Pipeline Specifications Developer Guide](pipeline_specs_guide.md)**: Authoring and maintaining pipeline specification rules, `spec/` and `presets/` fixture manifests, and rendered specification pages.
- **[Schema Converter Specifications Developer Guide](schema_converter_specs_guide.md)**: Authoring and maintaining schema converter rules, error codes (`E1..En`), fixture manifests, and rendered specification pages.
- **[dlt PyArrow Table and Column Definitions](dlt_pyarrow_definitions.md)**: Characterisation of table metadata, field descriptions, scalar and nested types, nullability, and identifier normalization across `dlt` schemas, Parquet files, and Iceberg tables.
- **[dlt Nested Output Behaviour](dlt_nested_output_behaviour.md)**: Characterisation of Python `dict` vs `pyarrow.Table` nested output, `max_table_nesting` propagation across `@dlt.resource` and `@dlt.transformer`, and Parquet, Iceberg, and JSONL representations.

## API and Schema Reference

- **[Python API Reference](api/cdm_data_loaders/index.md)**: Auto-generated API reference for all packages and modules under [`src/cdm_data_loaders`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders).
- **[Bundled Schemas and XSD Assets](bundled_schemas.md)**: Auto-generated catalog and inline collapsible syntax-highlighted source previews for all non-Python schema assets bundled in [`src/cdm_data_loaders/schemas`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas).

## About These Docs

- **[About These Docs](about_these_docs.md)**: Site architecture, automated drift verification, and interactive primary and accent theme color customization.
