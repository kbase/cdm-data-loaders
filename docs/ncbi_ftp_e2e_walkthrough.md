# NCBI FTP Pipeline — Local End-to-End Walkthrough

Step-by-step instructions for running a full production transfer in the Lakehouse, or running a small (up to 10 assembly) local end-to-end sync of NCBI RefSeq records against a local CEPH container. The walkthrough uses a Jupyter notebook ([`notebooks/ncbi_ftp_manifest.ipynb`](https://github.com/kbase/cdm-data-loaders/blob/develop/notebooks/ncbi_ftp_manifest.ipynb)) for manifest generation, the [`ncbi_ftp_sync`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/ncbi_ftp_download.py) CLI for parallel FTP downloads, and the [`ncbi_ftp_promote`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/ncbi_ftp_promote.py) CLI for promoting staged records to an S3 store.

> [!NOTE]
> Prerequisites: Docker or Podman, [uv](https://docs.astral.sh/uv/) (for running notebooks and CLIs locally), and network access to `ftp.ncbi.nlm.nih.gov`.

---

## Architecture Overview

```
 Manifest (notebook)          Download (container CLI)      Promote (local CLI)
┌────────────────────┐     ┌───────────────────────┐     ┌──────────────────────┐
│ Manifest notebook  │     │ ncbi_ftp_sync CLI     │     │ ncbi_ftp_promote CLI │
│ ─ download FTP     │────▶│ ─ read manifest       │────▶│ ─ promote staged     │
│   assembly summary │     │ ─ parallel FTP DL     │     │   files to Lakehouse │
│ ─ diff against     │     │ ─ MD5 verify          │     │ ─ archive old ver.   │
│   previous         │     │ ─ write .md5 sidecars │     │ ─ trim manifest      │
│ ─ write manifests  │     └──────────┬────────────┘     └──────────────────────┘
└────────────────────┘                │
                                 local volume
                                 mounted into
                                 the container
```

### Path Formats Used

| Format | Example | Description |
| --- | --- | --- |
| **s3:// URI** | `s3://cdm-lake/staging/run1/` | Full URI with scheme, bucket, and key |
| **bucket name** | `cdm-lake` | Bucket name without scheme |
| **S3 key prefix** | `tenant-general-warehouse/kbase/datasets/ncbi/` | Path within a bucket (no scheme, no bucket) |
| **S3 object key** | `staging/transfer_manifest.txt` | Single object key within a bucket |
| **local path** | `output/removed_manifest.txt` | Filesystem path on the host |

### Lakehouse Object (Final Location)

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}/raw_data/{GCF|GCA}/{nnn}/{nnn}/{nnn}/{assembly_dir}/{filename}
     └── bucket ─────┘ └── key prefix ───────┘└── build_accession_path() ────────────────────────┘
```

Example:

```
s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/raw_data/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly/GCF_900000615.1_PRJEB7657_assembly_genomic.fna.gz
```

### Staging Object (Phase 2 Output)

```
s3://{STAGING_BUCKET}/{STAGING_KEY_PREFIX}/raw_data/{GCF|GCA}/{nnn}/{nnn}/{nnn}/{assembly_dir}/{filename}
     └── bucket ─────┘ └── key prefix ────┘└── build_accession_path() ────────────────────────┘
```

### Local Output (Phase 1)

```
{OUTPUT_DIR}/transfer_manifest.txt
{OUTPUT_DIR}/removed_manifest.txt
{OUTPUT_DIR}/updated_manifest.txt
{OUTPUT_DIR}/diff_summary.json
```

---

## 1. Setup

### Local Testing: Start CEPH

```sh
docker run -d \
  --name ceph \
  -p 9000:8080 \
  -p 9001:8443 \
  -e RGW_PORT=8080 \
  -e RGW_ACCESS_KEY=test_access_key \
  -e RGW_SECRET_KEY=test_access_secret \
  ghcr.io/kbasetest/ceph-rgw-test-image:0.1.5
```

Set CEPH credentials and region as environment variables:

```sh
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=test_access_key
export AWS_SECRET_ACCESS_KEY=test_access_secret
export AWS_DEFAULT_REGION=us-east-1
```

(A matching service is configured in [`docker-compose.yml`](https://github.com/kbase/cdm-data-loaders/blob/develop/docker-compose.yml) for CI integration workflows.)

Create test buckets from the command line using the AWS CLI:

```sh
aws s3 mb s3://cdm-lake
aws s3 mb s3://cts
```

### Lakehouse: Build `cdm-data-loaders`

Clone the `cdm-data-loaders` repository in your Lakehouse user space, synchronize the virtual environment, and register it as a Jupyter kernel:

```bash
cd cdm-data-loaders
uv sync
source .venv/bin/activate
uv pip install -e .
uv pip install ipykernel
uv run python -m ipykernel install --user --name cdm-data-loaders --display-name "cdm-data-loaders"
```

When opening the manifest notebook, select the `cdm-data-loaders` kernel.

---

## 2. Phase 1 — Generate Manifests (Notebook)

Open [`notebooks/ncbi_ftp_manifest.ipynb`](https://github.com/kbase/cdm-data-loaders/blob/develop/notebooks/ncbi_ftp_manifest.ipynb) in JupyterLab or VS Code.

### Constants to Change (Cell 3)

| Constant | Walkthrough Value | Format | Purpose |
| --- | --- | --- | --- |
| `DATABASE` | `"refseq"` | string | Keep as-is |
| `PREFIX_FROM` | `"900"` | string | High-numbered prefix with few assemblies for fast diffing |
| `PREFIX_TO` | `"900"` | string | Single prefix bucket |
| `LIMIT` | `10` | int | Cap to 10 assemblies |
| `PREVIOUS_SUMMARY_URI` | `None` | s3:// URI | First run — treat every assembly as new |
| `SNAPSHOT_UPLOAD_URI` | `None` | s3:// URI | Skip S3 upload for local testing |
| `LAKEHOUSE_BUCKET` | `"cdm-lake"` (or `None`) | bucket name | Prune assemblies already present in the Lakehouse |
| `STORE_KEY_PREFIX` | `"tenant-general-warehouse/kbase/datasets/ncbi/"` | S3 key prefix | Default Lakehouse path prefix |
| `OUTPUT_DIR` | `Path("output")` | local path | Local manifest output directory |

### Initialize the S3 Client for CEPH

If `PREVIOUS_SUMMARY_URI`, `SNAPSHOT_UPLOAD_URI`, `LAKEHOUSE_BUCKET`, or `STAGING_URI` points at local CEPH, initialize the S3 client before running the cells that access S3. Insert a cell after Cell 1 (Imports):

```python
from cdm_data_loaders.io.s3.client import get_s3_client, reset_s3_client

reset_s3_client()
get_s3_client(
    {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "test_access_key",
        "aws_secret_access_key": "test_access_secret",
    }
)
```

If all S3 variables are `None`, this cell can be skipped, though on repeat runs setting `LAKEHOUSE_BUCKET` prunes assemblies already promoted in Step 4 from the transfer manifest.

### Optional: Bootstrap from an Existing Store (Cell 5)

If you have a pre-populated S3 store without a baseline assembly summary, scan the store to generate a synthetic baseline without running multi-day FTP checksum comparisons:

- Set `SCAN_STORE = True` in Cell 5.
- The notebook scans all objects under `s3://{LAKEHOUSE_BUCKET}/{STORE_KEY_PREFIX}`.
- For each unique assembly found, it extracts the accession and uses the earliest object `LastModified` timestamp as a conservative `seq_rel_date`.
- The synthetic summary is written to `LOCAL_SYNTHETIC_SUMMARY` (default `output/synthetic_summary_from_store.txt`) for use as `PREVIOUS_SUMMARY_URI` on subsequent runs.

```python
from pathlib import Path

SCAN_STORE = True
LAKEHOUSE_BUCKET = "cdm-lake"
STORE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"
LOCAL_SYNTHETIC_SUMMARY = Path("output/synthetic_summary_from_store.txt")

# after running cell 5, upload the result to S3 for future runs:
# s3 cp output/synthetic_summary_from_store.txt s3://cdm-lake/assembly_summaries/synthetic_base.txt
# then in future runs set PREVIOUS_SUMMARY_URI = "s3://cdm-lake/assembly_summaries/synthetic_base.txt"
```

### Run the Notebook

Execute all cells in order. After Cell 7 finishes, `output/` contains:

```
output/
  transfer_manifest.txt   # up to 10 FTP directory paths
  removed_manifest.txt    # empty on first run
  updated_manifest.txt    # empty on first run
  diff_summary.json       # counts of new, updated, replaced, and suppressed entries
```

Each line in `transfer_manifest.txt` is an FTP directory path:

```
/genomes/all/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly
```

### Optional: Upload Manifests to S3 for CTS

Cell 7 optionally uploads the manifests to an S3 staging prefix so CTS can stage them into the container. For local testing, keep `STAGING_URI = None` and copy the manifest manually in Step 3b below. To test S3 upload against CEPH:

```python
STAGING_URI = "s3://cts/staging/run1/"
```

---

## 3. Phase 2 — Download Assemblies (`ncbi_ftp_sync`)

Phase 2 uses the [`ncbi_ftp_sync`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/ncbi_ftp_download.py) CLI ([CLI options](cli_reference.md#ncbi_ftp_sync)) to download assemblies in parallel from NCBI FTP and verify MD5 checksums.

> [!NOTE]
> In production, Phase 2 runs as a CTS job: CTS stages input files from S3 into `/input_dir` and uploads `/output_dir` back to S3 without exposing S3 credentials inside the container.

For local testing without CTS, run the container directly with Docker or Podman, mounting the manifest from Step 2 as input and a local staging directory as output.

### 3a. Build the Container Image

```sh
# from the repository root
docker build -t cdm-data-loaders .
```

### 3b. Prepare Local Directories

```sh
mkdir -p notebooks/staging/input
cp notebooks/output/* notebooks/staging/input
```

### 3c. Run the Download

```sh
docker run --rm \
  --userns=keep-id \
  -v "$(pwd)/notebooks/staging/input:/input:ro" \
  -v "$(pwd)/notebooks/staging:/output" \
  cdm-data-loaders pipeline ncbi_ftp_sync \
    --manifest /input/transfer_manifest.txt \
    --output-dir /output \
    --threads 2 \
    --limit 10
```

(`--userns=keep-id` maps your host UID in Podman rootless mode; with Docker, use `--user "$(id -u):$(id -g)"` instead.)

You can also run `ncbi_ftp_sync` directly on the host without Docker (see [`ncbi_ftp_sync` in the CLI Reference](cli_reference.md#ncbi_ftp_sync) for all options):

```sh
uv run ncbi_ftp_sync \
  --manifest notebooks/output/transfer_manifest.txt \
  --output-dir staging \
  --threads 2 \
  --limit 10
```

After `ncbi_ftp_sync` completes, the staging directory contains downloaded files and verified `.md5` sidecars:

```
staging/
  raw_data/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly/
    GCF_900000615.1_PRJEB7657_assembly_genomic.fna.gz
    GCF_900000615.1_PRJEB7657_assembly_genomic.fna.gz.md5
    GCF_900000615.1_PRJEB7657_assembly_protein.faa.gz
    GCF_900000615.1_PRJEB7657_assembly_protein.faa.gz.md5
    ...
  download_report.json
```

### 3d. Local Testing: Upload Staged Files to CEPH

Upload the staged files into CEPH under a staging prefix to feed Step 4:

```sh
aws s3 cp --recursive notebooks/staging/raw_data/ s3://cts/staging/run1/raw_data/
aws s3 ls --recursive s3://cts/staging/run1/
```

---

## 4. Phase 3 — Promote and Archive (`ncbi_ftp_promote`)

Phase 3 uses [`ncbi_ftp_promote`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/ncbi_ftp_promote.py) ([CLI options](cli_reference.md#ncbi_ftp_promote)) to promote staged assemblies from the S3 staging prefix to their final Lakehouse paths, archive replaced or suppressed assemblies, and trim the transfer manifest for resumability.

Run a dry run first to verify planned operations:

```sh
uv run ncbi_ftp_promote \
  --staging-path staging/run1 \
  --removed-manifest notebooks/output/removed_manifest.txt \
  --updated-manifest notebooks/output/updated_manifest.txt \
  --dry-run
```

Then execute the promotion:

```sh
uv run ncbi_ftp_promote \
  --staging-path staging/run1 \
  --removed-manifest notebooks/output/removed_manifest.txt \
  --updated-manifest notebooks/output/updated_manifest.txt
```

If `--removed-manifest` and `--updated-manifest` are omitted, only staged records are promoted and no archiving occurs. On completion, the CLI prints:

```
PROMOTE SUMMARY: 10 promoted, 0 archived, 0 failed
```

---

## 5. Inspect Results in CEPH

Use the AWS CLI to inspect promoted objects and their user metadata:

```sh
# list final Lakehouse objects
aws s3 ls --recursive \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/raw_data/

# inspect user metadata (md5) on a specific object
aws s3api head-object \
  --bucket cdm-lake \
  --key tenant-general-warehouse/kbase/datasets/ncbi/raw_data/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly/GCF_900000615.1_PRJEB7657_assembly_genomic.fna.gz \
  --query Metadata
```

### Frictionless Metadata Descriptors

Each promoted assembly writes a [Frictionless](https://framework.frictionlessdata.io/) data package descriptor at:

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}metadata/{assembly_dir}_datapackage.json
```

When an assembly is archived (updated or removed), its live descriptor is copied to:

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}archive/{release_tag}/metadata/{assembly_dir}_datapackage.json
```

List and inspect descriptors with the AWS CLI:

```sh
aws s3 ls \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/metadata/

aws s3 cp \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/metadata/GCF_900000615.1_PRJEB7657_assembly_datapackage.json \
  -
```

---

## 6. Incremental Run (Second Sync)

To exercise incremental diff, update, and archive behavior against a prior snapshot:

- **Phase 1 (Step 2)**: Set `PREVIOUS_SUMMARY_URI` to the S3 URI of the raw summary from the first run (or pass a local snapshot path to `parse_assembly_summary(Path("prev.txt"))`). The diff reports `updated`, `replaced`, and `suppressed` entries that changed since the baseline.
- **Phase 2 (Step 3)**: Download the updated `transfer_manifest.txt` via `ncbi_ftp_sync`.
- **Phase 3 (Step 4)**: Pass `--removed-manifest` and `--updated-manifest` from Phase 1 to `ncbi_ftp_promote`. Updated assemblies are archived before overwrite, and removed assemblies are archived and deleted from live `raw_data/`.

---

## 7. Cleanup

```sh
# stop and remove CEPH
docker stop ceph && docker rm ceph

# remove local staging and manifest data
rm -rf staging/ output/
```

---

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `connect_ftp() timeout` | NCBI FTP may be slow or rate-limited | Retry and reduce `--threads` to 1 |
| Phase 3 shows 0 promoted | Staging prefix or bucket mismatch | Verify `--staging-path` matches the S3 upload path from Step 3d |
| Phase 3 S3 auth error | Missing CEPH credentials | Export `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` before running |
| Container cannot reach FTP | Docker network isolation | Pass `--network host` or verify DNS resolution inside the container |

---

## Reference: Downloaded File Suffixes

Phase 2 downloads files matching `FILE_FILTERS` in [`src/cdm_data_loaders/ncbi_ftp/assembly.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/ncbi_ftp/assembly.py) (plus `md5checksums.txt` for integrity verification):

| Suffix | Content |
| --- | --- |
| `_genomic.fna.gz` | Genome nucleotide sequences |
| `_genomic.gff.gz` | Genome annotations (GFF3) |
| `_protein.faa.gz` | Protein sequences |
| `_gene_ontology.gaf.gz` | GO annotations |
| `_assembly_report.txt` | Assembly metadata |
| `_assembly_stats.txt` | Assembly statistics |
| `_assembly_regions.txt` | Assembly regions |
| `_ani_contam_ranges.tsv` | ANI contamination ranges |
| `_gene_expression_counts.txt.gz` | Gene expression counts |
| `_normalized_gene_expression_counts.txt.gz` | Normalized expression counts |
