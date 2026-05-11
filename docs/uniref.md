# TODO: Ingest UniRef Cluster Data → refdata_uniref

**Date:** 2026-05-07
**Author:** elishawc
**Priority:** Deferred — no active use case; raw data is preserved

---

## Current State

UniRef data exists in MinIO as raw XML.gz split files only. No Delta tables exist
anywhere for UniRef50/90/100. The previously registered databases (`kbase_uniref50`,
`kbase_uniref90`, `kbase_uniref100`) were dropped on 2026-05-07 because they pointed
to wrong paths (`tenant-sql-warehouse/kbase/...`) and were never queryable.

`refdata_uniprot.cluster` (120M rows, `cluster_type = 'Protein'`) is **not** UniRef —
it is a protein-level cluster table from the UniProtKB pipeline.

---

## Raw Data Location

All files are raw XML.gz splits under:
```
s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/uniprot/derived/
```

| Version | Type | Files | Size |
|---------|------|-------|------|
| 2025_03 | uniref50 | 703 | 36.2 GB |
| 2025_03 | uniref90 | 2,082 | 77.7 GB |
| 2025_03 | uniref100 | 4,655 | 158.9 GB |
| 2026_01 | uniref50 | 604 | 32.4 GB |
| 2026_01 | uniref90 | 1,889 | 71.5 GB |
| 2026_01 | uniref100 | 4,753 | 162.8 GB |

Schema reference: `derived/2025_03/uniref/uniref.xsd`

The broken old registrations had 4 tables per namespace:
`cluster`, `clustermember`, `crossreference`, `entity`

---

## What Would Need to Be Done

### Step 1: Parse XML.gz → Parquet or Delta

UniRef XML has a well-defined schema (`uniref.xsd`). Each file contains `<entry>`
records with nested `<member>` elements. Parsing options:

- **PySpark XML reader** (`spark-xml` library) — reads split XML in parallel; needs
  `com.databricks:spark-xml` jar on the Spark cluster
- **Python streaming parser** (lxml / ElementTree) — stream-parse each `.xml.gz` and
  write Parquet; simpler but single-threaded per file
- **CTS batch job** — submit a parsing job via the CDM Task Service for heavy compute

Key tables to extract per UniRef type:

| Table | Source element | Key columns |
|-------|---------------|-------------|
| `cluster` | `<entry>` | `cluster_id`, `name`, `updated`, `member_count` |
| `clustermember` | `<entry>/<member>` | `cluster_id`, `entity_id`, `is_representative`, `is_seed`, `source_db` |
| `entity` | `<entry>/<representativeMember>` | `entity_id`, `protein_name`, `organism`, `tax_id`, `length`, `sequence` |
| `crossreference` | `<entry>/<property>` | `cluster_id`, `property_type`, `value` |

### Step 2: Write Delta tables

Target path:
```
s3a://cdm-lake/tenant-general-warehouse/refdata/datasets/uniprot/uniref/{version}/{type}/
```
e.g. `uniref/2026_01/uniref50/cluster/`, `uniref/2026_01/uniref50/clustermember/`, etc.

### Step 3: Register in Spark metastore

```python
spark.sql("CREATE DATABASE IF NOT EXISTS refdata_uniref LOCATION 's3a://cdm-lake/tenant-general-warehouse/refdata/refdata_uniref.db'")

for utype in ['uniref50', 'uniref90', 'uniref100']:
    for table in ['cluster', 'clustermember', 'entity', 'crossreference']:
        loc = f"s3a://cdm-lake/tenant-general-warehouse/refdata/datasets/uniprot/uniref/2026_01/{utype}/{table}"
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS refdata_uniref.{utype}_{table}
            USING DELTA LOCATION '{loc}'
        """)
```

Or use separate databases per type: `refdata_uniref50`, `refdata_uniref90`, `refdata_uniref100`.

---

## Open Questions

- [ ] Is there an active use case? (e.g., mapping pangenome proteins → UniRef50 for
  cross-study comparisons, or linking to eggNOG clusters)
- [ ] Which version to prioritize — 2026_01 (newer) or 2025_03?
- [ ] Single `refdata_uniref` database with `uniref50_*` table prefixes, or separate
  databases per type?
- [ ] Does `spark-xml` need to be added to the BERDL Spark cluster, or use a Python
  streaming approach instead?

---

## Do Not Delete Raw Files

The XML.gz files at `derived/2025_03/uniref/` and `derived/2026_01/uniref/` are the
only copy of the UniRef source data. Do not delete them until Delta tables are verified.

---

## Update — 2026-05-07 (matt and copilot)

All open questions from above were resolved and ingestion was completed the same day.

### What was done

**2025_03 — all three variants (uniref50, uniref90, uniref100)**
- Processed Parquet already existed at `cts_output/2025_03/uniref/{type}/uniprot_kb/` (written by dlt pipeline with hardcoded `dataset_name="uniprot_kb"`)
- Copied to SQL warehouse under `tenant-sql-warehouse/refdata/refdata_uniref{type}_2025_03.db/`
- Ran `CONVERT TO DELTA` in-place (plain Parquet, no `_delta_log`)
- Registered in Hive metastore

**2026_01 — uniref50 and uniref90**
- No pre-processed Parquet existed; parsed directly from raw XML.gz using `spark-xml` (available on BERDL cluster)
- Written as Delta directly to `tenant-sql-warehouse/refdata/refdata_uniref{type}_2026_01.db/`
- Registered in Hive metastore

**2026_01 — uniref100**
- spark-xml job submitted; still running at time of this update

### Delta table locations

| Namespace | S3 path |
|-----------|---------|
| `refdata_uniref50_2025_03` | `s3a://cdm-lake/tenant-sql-warehouse/refdata/refdata_uniref50_2025_03.db/` |
| `refdata_uniref90_2025_03` | `s3a://cdm-lake/tenant-sql-warehouse/refdata/refdata_uniref90_2025_03.db/` |
| `refdata_uniref100_2025_03` | `s3a://cdm-lake/tenant-sql-warehouse/refdata/refdata_uniref100_2025_03.db/` |
| `refdata_uniref50_2026_01` | `s3a://cdm-lake/tenant-sql-warehouse/refdata/refdata_uniref50_2026_01.db/` |
| `refdata_uniref90_2026_01` | `s3a://cdm-lake/tenant-sql-warehouse/refdata/refdata_uniref90_2026_01.db/` |
| `refdata_uniref100_2026_01` | pending — job still running |

Tables per namespace: `cluster`, `clustermember`, `entity`, `entity_x_source_file`
(no `crossreference` table — not extracted by the dlt pipeline or spark-xml job)

### Design decisions

- Separate namespace per variant+version (e.g. `refdata_uniref50_2026_01`) — not a single `refdata_uniref` database with prefixed table names
- Raw XML.gz source path confirmed as `s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/uniprot/derived/{version}/uniref/{type}/`
- Known pipeline bug: `dataset_name="uniprot_kb"` hardcoded in `src/cdm_data_loaders/pipelines/uniref.py` — should be `dataset_name=f"uniref_{settings.uniref_variant}"`

### Pending

- [ ] Verify `COUNT(DISTINCT source_file)` for uniref50 2026_01 == 604 (expected from raw file count); 60.3M clusters vs 70.2M in 2025_03 may simply reflect a smaller UniProt 2026_01 release
- [ ] Register `refdata_uniref100_2026_01` in Hive once spark-xml job completes
- [ ] Fix `dataset_name` bug in pipeline for future runs