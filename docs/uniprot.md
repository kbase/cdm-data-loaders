# UniProt and UniRef Ingestion Guide

`cdm-data-loaders` provides `dlt` pipelines for ingesting UniProt Knowledgebase (UniProtKB) and UniRef XML datasets (`*.xml` and `*.xml.gz`) as well as tabular UniProt ID mapping releases (`*.dat` and `*.dat.gz`), along with parsers for UniProt release metadata.

| Pipeline CLI        | Mode                          | Source Module                                      | Bundled Schema / Parser                        | Default Dataset / Table                              |
| :------------------ | :---------------------------- | :------------------------------------------------- | :--------------------------------------------- | :--------------------------------------------------- |
| `uniprot`           | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniprot/uniprot.xsd` | `uniprot` / `entry`                                  |
| `uniref50`          | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniref/uniref.xsd`   | `uniref50` / `entry`                                 |
| `uniref90`          | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniref/uniref.xsd`   | `uniref90` / `entry`                                 |
| `uniref100`         | XSD-to-PyArrow (single table) | `cdm_data_loaders.pipelines.xmltopyarrow.pipeline` | `cdm_data_loaders/schemas/uniref/uniref.xsd`   | `uniref100` / `entry`                                |
| `uniprot_cdm`       | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniprot_kb_cdm`        | `cdm_data_loaders.parsers.uniprot.uniprot_kb`  | `uniprot_cdm` (multi-table)                          |
| `uniref50_cdm`      | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniref_cdm`            | `cdm_data_loaders.parsers.uniprot.uniref`      | `uniref50_cdm` (multi-table)                         |
| `uniref90_cdm`      | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniref_cdm`            | `cdm_data_loaders.parsers.uniprot.uniref`      | `uniref90_cdm` (multi-table)                         |
| `uniref100_cdm`     | CDM-normalized (multi-table)  | `cdm_data_loaders.pipelines.uniref_cdm`            | `cdm_data_loaders.parsers.uniprot.uniref`      | `uniref100_cdm` (multi-table)                        |
| `uniprot_idmapping` | XSV preset (dual-table)       | `cdm_data_loaders.pipelines.uniprot_idmapping`     | `cdm_data_loaders.parsers.uniprot.idmapping`   | `uniprot_idmapping` (`raw_id_mapping`, `id_mapping`) |

---

## XSD-Driven XML-to-PyArrow Pipelines (`uniprot`, `uniref50`, `uniref90`, `uniref100`)

The preset XML-to-PyArrow pipelines (configured in [`src/cdm_data_loaders/pipelines/xmltopyarrow/settings.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/settings.py)) stream `<entry>` elements from UniProtKB or UniRef XML files, validate and coerce each element against its bundled XML Schema (XSD), and emit nested PyArrow tables matching the generated `pyarrow.Schema`. See [XSD to PyArrow Specification](xsd_to_pyarrow_specification.md) and [XML to PyArrow Pipeline Specification](xmltopyarrow_specification.md) for conversion and streaming rules.

Because the XSD path, target `<entry>` tag, `dataset_name`, and `table_name` are pre-configured, these CLIs only require input, output, and destination settings (and accept all standard options documented in [Core Pipeline and KBase CTS Execution Guide](core_cts_execution.md) and [Table and Column Transformations](table_transforms.md)):

```bash
# ingest UniProtKB XML files into Parquet via the preset XSD-to-PyArrow pipeline
uv run uniprot \
  --input-dir /path/to/uniprot_xml \
  --output-dir /path/to/output \
  --use-destination local_fs \
  --file-glob "*.xml.gz"

# ingest UniRef90 XML files into a DuckDB destination
uv run uniref90 \
  --input-dir /path/to/uniref90_xml \
  --output-dir /path/to/output \
  --use-destination duckdb
```

---

## CDM-Normalized Multi-Table Pipelines (`uniprot_cdm`, `uniref50_cdm`, `uniref90_cdm`, `uniref100_cdm`)

When loading directly into the KBase Common Data Model (CDM) relational table structure, `uniprot_cdm` and `uniref*_cdm` route each `<entry>` element into normalized CDM entity tables with precomputed PyArrow schemas.

### `uniprot_cdm`

[`uniprot_cdm`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniprot_kb_cdm.py) converts each `{https://uniprot.org/uniprot}entry` element against [`uniprot.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniprot/uniprot.xsd) and routes rows via [`parsers/uniprot/uniprot_kb.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniprot_kb.py) into normalized tables under dataset `uniprot_cdm`:

| Table Name             | Contents                                                                                                                                                                                                                                                             |
| :--------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `entity`               | Core entity record (`entity_id = "uniprot:<primary_accession>"`, `entity_type = "protein"`, `data_source = "UniProt/<dataset>"`, creation and modification dates, version, and ingestion timestamp).                                                                 |
| `protein`              | Protein sequence metadata (`protein_id`, `evidence_for_existence`, `length`, `hash`, `sequence`).                                                                                                                                                                    |
| `identifier`           | Primary and secondary UniProt accessions, database cross-references (`<dbReference>`) normalized with CURIE prefixes (`UniProt`, `NCBITaxon`, `NCBIGene`, `PMID`, Ensembl gene, protein, and transcript splits), and organism relationships (`RO:0002162` in taxon). |
| `name`                 | Entry names (`<name>`), protein recommended, alternative, and submitted full and short names, and `<gene><name>` values with their name type description.                                                                                                            |
| `entity_x_publication` | Links between `entity_id` and prioritized publication identifiers (`DOI`, `PMCID`, `PMID`, `AGRICOLA`).                                                                                                                                                              |
| `_rejected`            | Entries that failed transformation (such as a missing `<accession>`), recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`.                                                                                                            |

```bash
uv run uniprot_cdm \
  --input-dir /path/to/uniprot_xml \
  --output-dir /path/to/output \
  --use-destination local_fs \
  --log-interval 1000
```

### `uniref50_cdm`, `uniref90_cdm`, and `uniref100_cdm`

[`uniref50_cdm`, `uniref90_cdm`, and `uniref100_cdm`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniref_cdm.py) convert each `{http://uniprot.org/uniref}entry` element against [`uniref.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniref/uniref.xsd) and route rows via [`parsers/uniprot/uniref.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniref.py) into normalized tables under datasets `uniref50_cdm`, `uniref90_cdm`, or `uniref100_cdm`:

| Table Name      | Contents                                                                                                                                                                        |
| :-------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `entity`        | Core cluster entity record (`entity_id = "uniref:<entry_id>"`, `entity_type = "Cluster"`, `data_source = "UniRef"`, `data_source_entity_id`, `data_source_updated`, `updated`). |
| `cluster`       | Cluster definition (`cluster_id`, `name`, `cluster_type = "Protein"`, `protocol = "UniRef 50" \| "UniRef 90" \| "UniRef 100"`, `description`).                                  |
| `clustermember` | Representative and member cross-references (`cluster_id`, `entity_id`, `is_representative`, `is_seed`).                                                                         |
| `_rejected`     | Entries that failed transformation (such as a missing cluster `_id` attribute), recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`.             |

```bash
uv run uniref90_cdm \
  --input-dir /path/to/uniref_xml \
  --output-dir /path/to/output \
  --use-destination local_fs
```

---

## 3. UniProt ID Mapping Pipeline (`uniprot_idmapping`)

[`uniprot_idmapping.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniprot_idmapping.py) provides a preset XSV pipeline (`UniProtIdMappingSettings`, sharing `run_xsv_pipeline`) for ingesting headerless, tab-delimited UniProt `idmapping.dat` and `idmapping.dat.gz` files validated against [`idmapping.schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniprot/idmapping/idmapping.schema.json).

Each validated `pyarrow.Table` batch passes through two composed `TABLE_TRANSFORMS` steps ([`emit_raw_table`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/core/transforms.py) and [`transform_uniprot_id_mapping`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/idmapping.py)), emitting both a raw table and a standardised cross-reference table under dataset `uniprot_idmapping`. Both tables carry explicit PyArrow schemas ([`schemas/uniprot/idmapping/schema.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniprot/idmapping/schema.py)) and Iceberg hash-bucket partition specs (`bucket(N, "uniprot_id")`, configurable via `--partition-buckets`).

### Output Tables

| Table Name       | Table Description                        | Columns                                                                                                                                                     |
| :--------------- | :--------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `raw_id_mapping` | `raw ID mapping data from UniProt`       | `uniprot_id`, `db_reference_type`, `db_reference_id` (all non-nullable `string`).                                                                           |
| `id_mapping`     | `Standardised mapping data from UniProt` | `uniprot_id` (`uniprot:<uniprot_id>`), `db_reference` (`<db_reference_type>:<db_reference_id>`), `source` (input file basename); all non-nullable `string`. |
| `_rejected`      | —                                        | Malformed files or `qsv` validation failures, recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`.                           |

### Running `uniprot_idmapping`

```bash
# Ingest UniProt ID mapping .dat or .dat.gz files into Parquet or Iceberg
uv run uniprot_idmapping \
  --input-dir /path/to/idmapping \
  --output-dir /path/to/output \
  --loader-file-format iceberg \
  --partition-buckets 64
```

---

## 4. Ancillary UniProt Parsers and Schemas

In addition to the ingestion pipelines, `cdm_data_loaders` includes utilities for UniProt release metadata and raw TSV schemas:

- **Metalink Manifest Parser (`cdm_data_loaders.parsers.uniprot.metalink`)**:
  - `parse_metalink(path)` parses `RELEASE.metalink` XML manifests using `defusedxml`.
  - `generate_data_source_table(path)` extracts dataset provenance (`license`, `publisher`, `resource_type`, `version`).
  - `get_files(path, files_to_find=...)` extracts download URLs, file sizes, and checksums for release files.
- **Release Notes Parser (`cdm_data_loaders.parsers.uniprot.relnotes`)**:
  - `parse_relnotes(path)` parses UniProt `relnotes.txt` files to extract the release version, release date, and entry counts across `UniProtKB`, `UniProtKB/Swiss-Prot`, `UniProtKB/TrEMBL`, `UniRef100`, `UniRef90`, `UniRef50`, and `UniParc`.
- **ID Mapping TSV Schema (`cdm_data_loaders/schemas/uniprot/idmapping/idmapping.schema.json`)**:
  - JSON Schema with `x-xsv-config` metadata (tab-delimited, headerless, columns `uniprot_id`, `db_reference_type`, `db_reference_id`) used by `uniprot_idmapping` and usable with the generic `xsv` pipeline:
    ```bash
    uv run xsv \
      --input-dir /path/to/idmapping \
      --output-dir /path/to/output \
      --file-glob "*idmapping*.dat.gz" \
      --schema-file src/cdm_data_loaders/schemas/uniprot/idmapping/idmapping.schema.json \
      --table-name raw_id_mapping
    ```
