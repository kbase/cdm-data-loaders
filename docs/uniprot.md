# UniProt and UniRef Ingestion Guide

`cdm-data-loaders` provides two complementary families of `dlt` pipelines for ingesting UniProt Knowledgebase (UniProtKB) and UniRef XML datasets (`*.xml` and `*.xml.gz`), along with parsers and schemas for UniProt release metadata and ID mappings.

| Pipeline CLI    | Mode                          | Source Module                                      | Bundled Schema / Parser                        | Default Dataset / Table       |
| :-------------- | :---------------------------- | :------------------------------------------------- | :--------------------------------------------- | :---------------------------- |
| `uniprot`       | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniprot/uniprot.xsd` | `uniprot` / `entry`           |
| `uniref50`      | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniref/uniref.xsd`   | `uniref50` / `entry`          |
| `uniref90`      | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniref/uniref.xsd`   | `uniref90` / `entry`          |
| `uniref100`     | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniref/uniref.xsd`   | `uniref100` / `entry`         |
| `uniprot_cdm`   | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniprot_kb_cdm`        | `cdm_data_loaders.parsers.uniprot.uniprot_kb`  | `uniprot_cdm` (multi-table)   |
| `uniref50_cdm`  | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniref_cdm`            | `cdm_data_loaders.parsers.uniprot.uniref`      | `uniref50_cdm` (multi-table)  |
| `uniref90_cdm`  | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniref_cdm`            | `cdm_data_loaders.parsers.uniprot.uniref`      | `uniref90_cdm` (multi-table)  |
| `uniref100_cdm` | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniref_cdm`            | `cdm_data_loaders.parsers.uniprot.uniref`      | `uniref100_cdm` (multi-table) |

---

## 1. XSD-Driven XML-to-PyArrow Pipelines (`uniprot`, `uniref50`, `uniref90`, `uniref100`)

The preset XML-to-PyArrow pipelines stream `<entry>` elements from UniProtKB or UniRef XML files, validate and coerce each element against its bundled XML Schema (XSD) via `cdm_data_loaders.converters.facades.xsd_to_pyarrow`, and emit nested PyArrow tables matching the generated `pyarrow.Schema` (see [`xsd_to_pyarrow_specification.md`](xsd_to_pyarrow_specification.md) for the complete XSD-to-PyArrow conversion rules).

### Preset Configuration

Defined in `cdm_data_loaders.pipelines.xmltopyarrow.settings`:

- **`uniprot`** (`UniProtXmlToPyArrowSettings`):
  - **Bundled XSD**: `cdm_data_loaders/schemas/uniprot/uniprot.xsd`
  - **Target XML tag**: `{https://uniprot.org/uniprot}entry`
  - **Default `dataset_name`**: `uniprot`
  - **Default `table_name`**: `entry`
- **`uniref50` / `uniref90` / `uniref100`** (`UniRef50XmlToPyArrowSettings`, `UniRef90XmlToPyArrowSettings`, `UniRef100XmlToPyArrowSettings`):
  - **Bundled XSD**: `cdm_data_loaders/schemas/uniref/uniref.xsd`
  - **Target XML tag**: `{http://uniprot.org/uniref}entry`
  - **Default `dataset_name`**: `uniref50`, `uniref90`, or `uniref100`
  - **Default `table_name`**: `entry`

Because the XSD path and target XML tag are pre-configured, these CLIs only require input/output paths (and accept any standard `FileFormatIngestSettings` or `TransformSettingsMixin` flags; see [`core_cts_execution.md`](core_cts_execution.md) and [`table_transforms.md`](table_transforms.md)):

```bash
# Ingest UniProtKB XML files into Parquet via the preset XSD-to-PyArrow pipeline
uv run uniprot \
  --input-dir /path/to/uniprot_xml \
  --output-dir /path/to/output \
  --file-glob "*.xml.gz"

# Ingest UniRef90 XML files into a DuckDB destination
uv run uniref90 \
  --input-dir /path/to/uniref90_xml \
  --output-dir /path/to/output \
  --use-destination duckdb
```

---

## 2. CDM-Normalized Multi-Table Pipelines (`uniprot_cdm`, `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm`)

When loading directly into the KBase Common Data Model (CDM) relational table structure, `uniprot_cdm` and the `uniref*_cdm` pipelines route each `<entry>` element into normalized CDM entity tables.

### 2.1 `uniprot_cdm` (`cdm_data_loaders.pipelines.uniprot_kb_cdm`)

`UniProtSettings` extends `UniProtXmlToPyArrowSettings` and delegates to `run_xmltopyarrow_pipeline` with a pre-configured `TableTransform` (`make_uniprot_cdm_transform` in `cdm_data_loaders.parsers.uniprot.uniprot_kb`). Each `{https://uniprot.org/uniprot}entry` element is converted against `uniprot.xsd` and routed by `parse_uniprot_entry` into CDM tables with explicit PyArrow schemas (`UNIPROT_CDM_SCHEMA_MAP`) under dataset `uniprot_cdm`.

#### Output Tables

| Table Name             | Contents                                                                                                                                                                                                                                                       |
| :--------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `entity`               | Core entity record (`entity_id = "uniprot:<primary_accession>"`, `entity_type = "protein"`, `data_source = "UniProt/<dataset>"`, creation/modification dates, version, and ingestion timestamp).                                                               |
| `protein`              | Protein sequence metadata (`protein_id`, `evidence_for_existence`, `length`, `hash`, `sequence`).                                                                                                                                                              |
| `identifier`           | Primary and secondary UniProt accessions, database cross-references (`<dbReference>`) normalized with CURIE prefixes (`UniProt`, `NCBITaxon`, `NCBIGene`, `PMID`, Ensembl gene/protein/transcript splits), and organism relationships (`RO:0002162` in taxon). |
| `name`                 | Entry names (`<name>`), protein recommended/alternative/submitted full and short names, and `<gene><name>` values with their name type description.                                                                                                            |
| `entity_x_publication` | Links between `entity_id` and prioritized publication identifiers (`DOI`, `PMCID`, `PMID`, `AGRICOLA`).                                                                                                                                                        |
| `_rejected`            | Entries that failed transformation (e.g., missing `<accession>`), recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`.                                                                                                          |

#### Running `uniprot_cdm`

```bash
uv run uniprot_cdm \
  --input-dir /path/to/uniprot_xml \
  --output-dir /path/to/output \
  --log-interval 1000
```

### 2.2 `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm` (`cdm_data_loaders.pipelines.uniref_cdm`)

`UniRef50CdmSettings`, `UniRef90CdmSettings`, and `UniRef100CdmSettings` extend `UniRefXmlToPyArrowSettings` and delegate to `run_xmltopyarrow_pipeline` with a pre-configured `TableTransform` (`make_uniref_cdm_transform` in `cdm_data_loaders.parsers.uniprot.uniref`). Each `{http://uniprot.org/uniref}entry` element is converted against `uniref.xsd` and routed by `parse_uniref_entry` into CDM tables with explicit PyArrow schemas (`UNIREF_CDM_SCHEMA_MAP`) under datasets `uniref50_cdm`, `uniref90_cdm`, or `uniref100_cdm`.

#### Output Tables

| Table Name      | Contents                                                                                                                                                                        |
| :-------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `entity`        | Core cluster entity record (`entity_id = "uniref:<entry_id>"`, `entity_type = "Cluster"`, `data_source = "UniRef"`, `data_source_entity_id`, `data_source_updated`, `updated`). |
| `cluster`       | Cluster definition (`cluster_id`, `name`, `cluster_type = "Protein"`, `protocol = "UniRef 50" \| "UniRef 90" \| "UniRef 100"`, `description`).                                  |
| `clustermember` | Representative and member cross-references (`cluster_id`, `entity_id`, `is_representative`, `is_seed`).                                                                         |
| `_rejected`     | Entries that failed transformation (e.g., missing cluster `_id` attribute), recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`.                 |

#### Running `uniref50_cdm`, `uniref90_cdm`, or `uniref100_cdm`

```bash
uv run uniref90_cdm \
  --input-dir /path/to/uniref_xml \
  --output-dir /path/to/output \
  --use-destination local_fs
```

---

## 3. Ancillary UniProt Parsers and Schemas

In addition to the XML pipelines, `cdm_data_loaders` includes utilities for UniProt release metadata and tabular ID mappings:

- **Metalink Manifest Parser (`cdm_data_loaders.parsers.uniprot.metalink`)**:
  - `parse_metalink(path)` parses `RELEASE.metalink` XML manifests using `defusedxml`.
  - `generate_data_source_table(path)` extracts dataset provenance (`license`, `publisher`, `resource_type`, `version`).
  - `get_files(path, files_to_find=...)` extracts download URLs, file sizes, and checksums for release files.
- **Release Notes Parser (`cdm_data_loaders.parsers.uniprot.relnotes`)**:
  - `parse_relnotes(path)` parses UniProt `relnotes.txt` files to extract the release version, release date, and entry counts across `UniProtKB`, `UniProtKB/Swiss-Prot`, `UniProtKB/TrEMBL`, `UniRef100`, `UniRef90`, `UniRef50`, and `UniParc`.
- **ID Mapping TSV Schema (`cdm_data_loaders/schemas/uniprot/idmapping.schema.json`)**:
  - JSON Schema with `x-xsv-config` metadata (tab-delimited, headerless, columns `id`, `db`, `xref`) for ingesting UniProt `idmapping.dat` / `idmapping.dat.gz` files with the `xsv` pipeline:
    ```bash
    uv run xsv \
      --input-dir /path/to/idmapping \
      --output-dir /path/to/output \
      --file-glob "idmapping*.dat.gz" \
      --schema-file src/cdm_data_loaders/schemas/uniprot/idmapping.schema.json \
      --table-name uniprot_idmapping
    ```
