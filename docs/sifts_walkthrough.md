# SIFTS Derived Data — Walkthrough

Step-by-step guide for downloading the EBI SIFTS `pdb_chain_uniprot.tsv.gz`
mapping file and uploading it to the Lakehouse bronze layer, locally with
MinIO or directly to production S3.

> **Prerequisites**
> - [uv](https://docs.astral.sh/uv/) installed
> - Docker or Podman (for the local MinIO walkthrough)
> - Network access to `ftp.ebi.ac.uk`

---

## External APIs

| Service | Endpoint | Documentation |
|---------|----------|---------------|
| EBI SIFTS FTP | `ftp://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/` | [SIFTS documentation](https://www.ebi.ac.uk/pdbe/docs/sifts/) |

---

## What SIFTS provides

[SIFTS](https://www.ebi.ac.uk/pdbe/docs/sifts/) (Structure Integration with
Function, Taxonomy and Sequences) is a residue-level mapping resource
maintained by PDBe and UniProt.  The `pdb_chain_uniprot.tsv.gz` file maps
every PDB polymer chain to its canonical UniProt accession.

---

## Output paths

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}/derived_data/sifts/pdb_chain_uniprot.tsv.gz
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}/derived_data/archive/{YYYY-MM-DD}/sifts/pdb_chain_uniprot.tsv.gz
```

An archive copy is created automatically when the file content has changed
since the last run.  If the file is identical, the existing object is left in
place (`status = "unchanged"`).

---

## Local run with MinIO

### 1. Start MinIO

```sh
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:RELEASE.2025-02-28T09-55-16Z server /data --console-address ":9001"
```

### 2. Create the Lakehouse bucket

Create the test bucket via the [MinIO console](http://localhost:9001)
(login: `minioadmin` / `minioadmin`), or with the included helper:

```sh
uv run python scripts/s3_local.py mb s3://cdm-lake
```

### 3. Configure and run the notebook

Open `notebooks/pdb_sifts.ipynb` and set:

```python
LAKEHOUSE_BUCKET     = "cdm-lake"
LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
DRY_RUN              = False
```

To test against local MinIO, also set `PROVIDE_CREDENTIALS = True` in the
credentials cell (cell 4) — it will configure the S3 client with
`http://localhost:9000` and the `minioadmin` credentials automatically.

Run all cells.  Expected output on first run:

```
Upload status : new
Destination   : cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/derived_data/sifts/pdb_chain_uniprot.tsv.gz
Archive key   : None
Dry run       : False
```

### 4. Verify the upload

```bash
aws --endpoint-url http://localhost:9000 s3 ls \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/derived_data/sifts/
```

### 5. Re-running (idempotency check)

Run the notebook a second time.  Because the file has not changed, the pipeline
skips the upload:

```
Upload status : unchanged
```

---

## Production run

### Prerequisites

Set the following environment variables (or use an IAM role):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

### Configure and run

```python
LAKEHOUSE_BUCKET     = "kbase-cdm-lake-prod"
LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
DRY_RUN              = False
```

Or pass settings as environment variables when invoking from a container:

```bash
LAKEHOUSE_BUCKET=kbase-cdm-lake-prod uv run python -c "
from cdm_data_loaders.sifts.run import run_sifts
from cdm_data_loaders.sifts.settings import SiftsSettings
result = run_sifts(SiftsSettings())
print(result)
"
```

---

## Dry-run mode

Setting `DRY_RUN = True` (or `dry_run=True` in `SiftsSettings`) logs what the
pipeline *would* do without downloading or uploading anything:

```
Upload status : dry_run
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ConnectionRefusedError` from FTP | EBI FTP unreachable | Check network; retry |
| `NoCredentialError` from boto3 | AWS credentials not set | Set env vars or IAM role |
| `NoSuchBucket` | Bucket does not exist | Create it first |
| `unchanged` on first run | Object already in S3 with matching MD5 | Expected — nothing to do |
