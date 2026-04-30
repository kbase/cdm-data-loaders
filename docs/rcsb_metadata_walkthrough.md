# RCSB Derived Metadata — Walkthrough

Step-by-step guide for fetching per-entry metadata from the RCSB PDB GraphQL
API and uploading NDJSON files to the Lakehouse bronze layer, locally with
MinIO or directly to production S3.

> **Prerequisites**
> - [uv](https://docs.astral.sh/uv/) installed
> - Docker or Podman (for the local MinIO walkthrough)
> - Network access to `data.rcsb.org`

---

## What this pipeline fetches

For every released PDB entry (~226 K entries as of 2025), the pipeline queries
the [RCSB GraphQL API](https://data.rcsb.org/graphql) and writes one NDJSON
file per entity type.

| Entity type | File | Contents |
|-------------|------|----------|
| `entries` | `entries.ndjson` | Core metadata: experimental method, resolution, dates, organism, DOI |
| `validation` | `validation.ndjson` | wwPDB validation scores (clashscore, Ramachandran, RMSZ) |
| `taxonomy` | `taxonomy.ndjson` | NCBI taxonomy lineage per polymer entity |
| `ligands` | `ligands.ndjson` | Bound ligand chemical components |
| `citations` | `citations.ndjson` | Primary and related citations, DOIs, PubMed IDs |
| `pfam` | `pfam.ndjson` | Pfam domain annotations per polymer entity |
| `sequence_clusters` | `sequence_clusters.ndjson` | RCSB sequence cluster membership |

---

## Output paths

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}/derived_data/rcsb/{entity_type}.ndjson
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}/derived_data/archive/{YYYY-MM-DD}/rcsb/{entity_type}.ndjson
```

An archive copy is created for each file when its content has changed since the
last run.  Unchanged files are skipped.

---

## Runtime estimates

Fetching all entity types for the full ~226 K entry set takes roughly:

| Batch size | Approx. requests | Estimated time |
|-----------|-----------------|---------------|
| 1000 (default) | ~1600 per entity type | 30–60 min total |
| 500 | ~3200 per entity type | 60–90 min total |

Run time depends heavily on network latency and RCSB API responsiveness.  The
pipeline uses automatic exponential-backoff retry on transient errors.

---

## Local run with MinIO

### 1. Start MinIO

```bash
docker compose up -d minio
```

### 2. Create the Lakehouse bucket

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

aws --endpoint-url http://localhost:9000 s3 mb s3://cdm-lake
```

### 3. Configure and run the notebook

Open `notebooks/pdb_rcsb_metadata.ipynb` and set:

```python
LAKEHOUSE_BUCKET     = "cdm-lake"
LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
BATCH_SIZE           = 1000
DRY_RUN              = False
```

Run all cells.  Expected output (abbreviated):

```
Total PDB entries : 226341
Dry run           : False

Entity type               Status                    Records  Archive key
--------------------------------------------------------------------------------
entries                   new                        226341
validation                new                        226341
taxonomy                  new                        226341
ligands                   new                        226341
citations                 new                        226341
pfam                      new                        226341
sequence_clusters         new                        226341
```

### 4. Verify the uploads

```bash
aws --endpoint-url http://localhost:9000 s3 ls \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/derived_data/rcsb/
```

### 5. Re-running (idempotency check)

On a second run where RCSB data has not changed, all files report `unchanged`:

```
entries                   unchanged                  226341
```

When RCSB releases new entries, changed files report `archived_and_replaced`
and the previous version is preserved in the archive path.

---

## Production run

### Prerequisites

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

### Configure and run

```python
LAKEHOUSE_BUCKET     = "kbase-cdm-lake-prod"
LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
BATCH_SIZE           = 1000
DRY_RUN              = False
```

Or via environment variables:

```bash
LAKEHOUSE_BUCKET=kbase-cdm-lake-prod uv run python -c "
from cdm_data_loaders.rcsb_metadata.run import run_rcsb_metadata
from cdm_data_loaders.rcsb_metadata.settings import RcsbMetadataSettings
result = run_rcsb_metadata(RcsbMetadataSettings())
print(result.to_dict())
"
```

---

## Dry-run mode

Setting `DRY_RUN = True` logs what the pipeline would do without making any
API calls or S3 uploads:

```
Entity type               Status
--------------------------------------------------------------------------------
entries                   dry_run
validation                dry_run
...
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `NonRetryableDownloadError` on GraphQL query | Invalid query / field not found | Check RCSB schema changes |
| All entities report `error` | `fetch_entry_ids` failed | Check network access to `data.rcsb.org` |
| One entity reports `error` | GraphQL quota / temporary API issue | Re-run; only errored entities need to be re-fetched |
| `NoCredentialError` from boto3 | AWS credentials not set | Set env vars or IAM role |
| `unchanged` on first run | Files already in S3 with matching MD5 | Expected — nothing to do |
| Slow progress | RCSB API rate-limiting | Reduce `BATCH_SIZE` or run during off-peak hours |
