# UniProt and UniRef Ingestion Guide

`cdm-data-loaders` provides two complementary families of `dlt` pipelines for ingesting UniProt Knowledgebase (UniProtKB) and UniRef XML datasets (`*.xml` and `*.xml.gz`), along with parsers and bundled schemas for UniProt release metadata and ID mappings.

| Pipeline CLI | Mode | Source Module | Bundled Schema or Parser | Default Dataset and Table |
| :--- | :--- | :--- | :--- | :--- |
| [`uniprot`](cli_reference.md#uniprot) | XSD-to-PyArrow (single table) | [`pipelines/xmltopyarrow/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py) | [`schemas/uniprot/uniprot.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniprot/uniprot.xsd) | `uniprot` / `entry` |
| [`uniref50`](cli_reference.md#uniref50) | XSD-to-PyArrow (single table) | [`pipelines/xmltopyarrow/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py) | [`schemas/uniref/uniref.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniref/uniref.xsd) | `uniref50` / `entry` |
| [`uniref90`](cli_reference.md#uniref90) | XSD-to-PyArrow (single table) | [`pipelines/xmltopyarrow/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py) | [`schemas/uniref/uniref.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniref/uniref.xsd) | `uniref90` / `entry` |
| [`uniref100`](cli_reference.md#uniref100) | XSD-to-PyArrow (single table) | [`pipelines/xmltopyarrow/pipeline.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/xmltopyarrow/pipeline.py) | [`schemas/uniref/uniref.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniref/uniref.xsd) | `uniref100` / `entry` |
| [`uniprot_cdm`](cli_reference.md#uniprot_cdm) | CDM-normalized (multi-table) | [`pipelines/uniprot_kb_cdm.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniprot_kb_cdm.py) | [`parsers/uniprot/uniprot_kb.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniprot_kb.py) | `uniprot_cdm` (multi-table) |
| [`uniref50_cdm`](cli_reference.md#uniref50_cdm) | CDM-normalized (multi-table) | [`pipelines/uniref_cdm.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniref_cdm.py) | [`parsers/uniprot/uniref.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniref.py) | `uniref50_cdm` (multi-table) |
| [`uniref90_cdm`](cli_reference.md#uniref90_cdm) | CDM-normalized (multi-table) | [`pipelines/uniref_cdm.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniref_cdm.py) | [`parsers/uniprot/uniref.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniref.py) | `uniref90_cdm` (multi-table) |
| [`uniref100_cdm`](cli_reference.md#uniref100_cdm) | CDM-normalized (multi-table) | [`pipelines/uniref_cdm.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniref_cdm.py) | [`parsers/uniprot/uniref.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniref.py) | `uniref100_cdm` (multi-table) |

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

| Table Name | Contents |
| :--- | :--- |
| `entity` | Core entity record (`entity_id = "uniprot:<primary_accession>"`, `entity_type = "protein"`, `data_source = "UniProt/<dataset>"`, creation and modification dates, version, and ingestion timestamp). |
| `protein` | Protein sequence metadata (`protein_id`, `evidence_for_existence`, `length`, `hash`, `sequence`). |
| `identifier` | Primary and secondary UniProt accessions, database cross-references (`<dbReference>`) normalized with CURIE prefixes (`UniProt`, `NCBITaxon`, `NCBIGene`, `PMID`, Ensembl gene, protein, and transcript splits), and organism relationships (`RO:0002162` in taxon). |
| `name` | Entry names (`<name>`), protein recommended, alternative, and submitted full and short names, and `<gene><name>` values with their name type description. |
| `entity_x_publication` | Links between `entity_id` and prioritized publication identifiers (`DOI`, `PMCID`, `PMID`, `AGRICOLA`). |
| `_rejected` | Entries that failed transformation (such as a missing `<accession>`), recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`. |

```bash
uv run uniprot_cdm \
  --input-dir /path/to/uniprot_xml \
  --output-dir /path/to/output \
  --use-destination local_fs \
  --log-interval 1000
```

### `uniref50_cdm`, `uniref90_cdm`, and `uniref100_cdm`

[`uniref50_cdm`, `uniref90_cdm`, and `uniref100_cdm`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/uniref_cdm.py) convert each `{http://uniprot.org/uniref}entry` element against [`uniref.xsd`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniref/uniref.xsd) and route rows via [`parsers/uniprot/uniref.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/uniref.py) into normalized tables under datasets `uniref50_cdm`, `uniref90_cdm`, or `uniref100_cdm`:

| Table Name | Contents |
| :--- | :--- |
| `entity` | Core cluster entity record (`entity_id = "uniref:<entry_id>"`, `entity_type = "Cluster"`, `data_source = "UniRef"`, `data_source_entity_id`, `data_source_updated`, `updated`). |
| `cluster` | Cluster definition (`cluster_id`, `name`, `cluster_type = "Protein"`, `protocol = "UniRef 50" \| "UniRef 90" \| "UniRef 100"`, `description`). |
| `clustermember` | Representative and member cross-references (`cluster_id`, `entity_id`, `is_representative`, `is_seed`). |
| `_rejected` | Entries that failed transformation (such as a missing cluster `_id` attribute), recording `table_name`, `source_file`, `line_no`, `raw_record`, and `error_detail`. |

```bash
uv run uniref90_cdm \
  --input-dir /path/to/uniref_xml \
  --output-dir /path/to/output \
  --use-destination local_fs
```

---

## Release Metadata Parsers and ID Mapping Schema

### Parsing `RELEASE.metalink` and `relnotes.txt` in Python

Use [`src/cdm_data_loaders/parsers/uniprot/metalink.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/metalink.py) and [`src/cdm_data_loaders/parsers/uniprot/relnotes.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/parsers/uniprot/relnotes.py) to extract dataset provenance, download URLs and checksums, and release entry counts:

```python
from pathlib import Path
from typing import Any

from cdm_data_loaders.parsers.uniprot.metalink import generate_data_source_table, get_files
from cdm_data_loaders.parsers.uniprot.relnotes import parse_relnotes

metalink_path = Path("/path/to/RELEASE.metalink")
relnotes_path = Path("/path/to/relnotes.txt")

# extract license, publisher, resource_type, and version from RELEASE.metalink
data_source_info: dict[str, Any] = generate_data_source_table(metalink_path)

# extract download URL, byte size, and checksum metadata for selected archives
release_files: dict[str, Any] = get_files(
    metalink_path,
    files_to_find=["uniprot_sprot.xml.gz", "idmapping.dat.gz"],
)

# parse release version, publication date, and database entry counts from relnotes.txt
release_stats: dict[str, Any] = parse_relnotes(relnotes_path)
```

### Ingesting `idmapping.dat.gz` with `xsv`

Bundled [`src/cdm_data_loaders/schemas/uniprot/idmapping.schema.json`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/schemas/uniprot/idmapping.schema.json) (also viewable on [Bundled Schemas and XSD Assets](bundled_schemas.md)) configures tab-delimited, headerless `id`, `db`, and `xref` columns via `x-xsv-config` for the [`xsv`](cli_reference.md#xsv) pipeline:

```bash
uv run xsv \
  --input-dir /path/to/idmapping \
  --output-dir /path/to/output \
  --use-destination local_fs \
  --dataset-name uniprot \
  --table-name uniprot_idmapping \
  --file-glob "idmapping*.dat.gz" \
  --schema-file src/cdm_data_loaders/schemas/uniprot/idmapping.schema.json
```
