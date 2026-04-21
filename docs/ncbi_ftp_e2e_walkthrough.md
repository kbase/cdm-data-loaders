# NCBI FTP Pipeline — Local End-to-End Walkthrough

Step-by-step instructions for running a small (≤ 10 assembly) end-to-end sync
of NCBI RefSeq records against a local MinIO container.  The walkthrough uses
the two existing Jupyter notebooks for Phases 1 and 3, and the project's Docker
image for the Phase 2 download step.

> **Prerequisites:**
> - Docker or Podman
> - [uv](https://docs.astral.sh/uv/) (for running notebooks locally)
> - Network access to `ftp.ncbi.nlm.nih.gov`

---

## Architecture overview

```
 Phase 1 (notebook)         Phase 2 (container)           Phase 3 (notebook)
┌────────────────────┐     ┌───────────────────────┐     ┌──────────────────────┐
│ Manifest notebook  │     │ ncbi_ftp_sync CLI     │     │ Promote notebook     │
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

---

## Path anatomy

All S3 paths in this pipeline compose from a small set of variables.
Understanding this decomposition is the key to configuring the notebooks.

### Path formats used

| Format | Example | Description |
|--------|---------|-------------|
| **s3:// URI** | `s3://cdm-lake/staging/run1/` | Full URI with scheme + bucket + key |
| **bucket name** | `cdm-lake` | Just the bucket, no scheme |
| **S3 key prefix** | `tenant-general-warehouse/kbase/datasets/ncbi/` | Path within a bucket (no scheme, no bucket) |
| **S3 object key** | `staging/transfer_manifest.txt` | Single object key within a bucket |
| **local path** | `output/removed_manifest.txt` | Filesystem path on the host |

### Lakehouse object (final location)

```
s3://{STORE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}raw_data/{GCF|GCA}/{nnn}/{nnn}/{nnn}/{assembly_dir}/{filename}
     └── bucket ──┘ └── key prefix ──────┘└── build_accession_path() ────────────────────────┘
```

Example:
```
s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/raw_data/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly/GCF_900000615.1_PRJEB7657_assembly_genomic.fna.gz
```

### Staging object (Phase 2 output)

```
s3://{STORE_BUCKET}/{STAGING_KEY_PREFIX}raw_data/{GCF|GCA}/{nnn}/{nnn}/{nnn}/{assembly_dir}/{filename}
     └── bucket ──┘ └── key prefix ────┘└── build_accession_path() ────────────────────────┘
```

### Local output (Phase 1)

```
{OUTPUT_DIR}/transfer_manifest.txt
{OUTPUT_DIR}/removed_manifest.txt
{OUTPUT_DIR}/updated_manifest.txt
{OUTPUT_DIR}/diff_summary.json
```

---

## 1. Setup

### Local testing

### Start MinIO

```sh
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:RELEASE.2025-02-28T09-55-16Z server /data --console-address ":9001"
```

Create a test bucket via the [MinIO console](http://localhost:9001)
(login: `minioadmin` / `minioadmin`), or from the command line using the
included `scripts/s3_local.py` helper (requires no extra installs — only
`boto3` which is already a project dependency):

```sh
uv run python scripts/s3_local.py mb s3://cdm-lake
```

### Lakehouse

#### Build `cdm-data-loaders`

First, clone the `cdm-data-loaders` repo in your Lakehouse user space. Then, build the package
in a virtual environment and register it as a Jupyter kernel:
```
cd cdm-data-loaders
uv sync
source .venv/bin/activate
uv pip install -e .
uv pip install ipykernel
uv run python -m ipykernel install --user --name cdm-data-loaders --display-name "cdm-data-loaders"
```
Then, when you open the manifest or promote notebooks, choose the `cdm-data-loaders` kernel.

---

## 2. Phase 1 — Generate manifests (notebook)

Open `notebooks/ncbi_ftp_manifest.ipynb` in JupyterLab or VS Code.

### Constants to change (Cell 3)

| Constant              | Walkthrough value                | Format | Why                                                     |
|-----------------------|----------------------------------|--------|---------------------------------------------------------|
| `DATABASE`            | `"refseq"`                       | string | keep as-is                                              |
| `PREFIX_FROM`         | `"900"`                          | string | high-numbered prefix → few assemblies, fast diffing     |
| `PREFIX_TO`           | `"900"`                          | string | single prefix bucket                                    |
| `LIMIT`               | `10`                             | int    | cap to 10 assemblies                                    |
| `PREVIOUS_SUMMARY_URI` | `None`                          | s3:// URI | first run — everything is "new"                       |
| `SNAPSHOT_UPLOAD_URI`  | `None`                          | s3:// URI | skip S3 upload for local testing                      |
| `STORE_BUCKET`        | `"cdm-lake"` (or `None`)         | bucket name | set to prune assemblies already in the Lakehouse   |
| `STORE_KEY_PREFIX`    | `"tenant-general-warehouse/kbase/datasets/ncbi/"` | S3 key prefix | default Lakehouse path prefix    |
| `OUTPUT_DIR`          | `Path("output")`                 | local path | keep as-is (local directory)                        |

### Initialise the S3 client for MinIO

If you set `PREVIOUS_SUMMARY_URI`, `SNAPSHOT_UPLOAD_URI`, `STORE_BUCKET`,
or `STAGING_URI` to point at your local MinIO, you must initialise
the S3 client **before** running the cells that use them.  Insert a new cell
after Cell 1 (Imports) with:

```python
from cdm_data_loaders.utils.s3 import get_s3_client, reset_s3_client

reset_s3_client()
get_s3_client({
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
})
```

If all three S3 variables are `None` (purely local testing), this cell can
be skipped — though on repeat runs you should set `STORE_BUCKET` so
assemblies already promoted to the Lakehouse are pruned from the transfer
manifest.

### Optional: Bootstrap from existing store (Cell 5)

If you have a pre-populated S3 store but lack a baseline assembly summary,
you can scan the store to generate a synthetic baseline. This is especially
useful for large stores (100K+ assemblies) where verifying against FTP
checksums would take days.

**When to use this:**
- First run against an existing, pre-populated store
- You want to start diffing without waiting for checksum verification
- You don't have a previous assembly summary snapshot to compare against

**How it works:**
1. Set `SCAN_STORE = True` in Cell 5
2. The notebook scans all objects under `s3://{STORE_BUCKET}/{STORE_KEY_PREFIX}`
3. For each unique assembly found, it extracts the accession and uses the
   earliest object `LastModified` as a conservative `seq_rel_date`
4. It saves the synthetic summary to `LOCAL_SYNTHETIC_SUMMARY` (default:
   `output/synthetic_summary_from_store.txt`)
5. This becomes the baseline for diffing; subsequent runs can load this
   file as `PREVIOUS_SUMMARY_URI`

**Example (for a 500K-assembly store):**
```python
SCAN_STORE = True
STORE_BUCKET = "cdm-lake"
STORE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"
LOCAL_SYNTHETIC_SUMMARY = Path("output/synthetic_summary_from_store.txt")

# After running Cell 5, upload the result to S3 for future runs:
# s3 cp output/synthetic_summary_from_store.txt s3://cdm-lake/assembly_summaries/synthetic_base.txt
# Then in future runs, set:
# PREVIOUS_SUMMARY_URI = "s3://cdm-lake/assembly_summaries/synthetic_base.txt"
```

**Performance:** Scanning typically takes 5–10 minutes for 500K assemblies
(vs. ~6 days of checksum verification).

### Run the notebook

Execute all cells in order.  After Cell 7 finishes you should see files in
`output/`:

```
output/
  transfer_manifest.txt   # ≤ 10 FTP directory paths
  removed_manifest.txt    # empty on first run
  updated_manifest.txt    # empty on first run
  diff_summary.json       # counts of new/updated/replaced/suppressed
```

Inspect `transfer_manifest.txt` — each line is an FTP directory path like:

```
/genomes/all/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly
```

### Optional: upload manifests to S3 for CTS

Cell 7 optionally uploads the manifests to an S3 staging prefix so that CTS
can stage them into the container.  For local testing, set
`STAGING_URI = None` (the default) and copy the manifest manually in
Step 3b below.

If you are testing against MinIO and want to exercise the S3 upload path:

```python
STAGING_URI = "s3://cdm-lake/staging/run1/"
```

> **Tip:** If you re-run later with `PREVIOUS_SUMMARY_URI` pointing at a
> snapshot from a prior run you will see `updated`, `replaced`, and
> `suppressed` entries in the diff.

---

## 3. Phase 2 — Download assemblies (container)

Phase 2 uses the `ncbi_ftp_sync` CLI, which is the container's built-in entry
point for parallel FTP downloads.

> **CTS (CDM Task Service):** In production, Phase 2 runs as a CTS job.
> CTS stages input files from S3 into the container's filesystem mount
> (`/input_dir`) and copies container output back to S3 (`/output_dir`).
> The container itself never receives S3 credentials.
> See [cdm-task-service](https://github.com/kbase/cdm-task-service) for details.

For local testing without a CTS instance we run the container directly with
Docker (or Podman), mounting the manifest produced in Phase 1 as input and a
local staging directory as output.

### 3a. Build the container image

```sh
# From the repository root
docker build -t cdm-data-loaders .
```

### 3b. Prepare local directories

```sh
mkdir -p notebooks/staging
cp notebooks/output/transfer_manifest.txt notebooks/staging/
```

### 3c. Run the download

```sh
docker run --rm \
  --userns=keep-id \
  -v "$(pwd)/notebooks/staging:/input:ro" \
  -v "$(pwd)/notebooks/staging:/output" \
  cdm-data-loaders ncbi_ftp_sync \
    --manifest /input/transfer_manifest.txt \
    --output-dir /output \
    --threads 2 \
    --limit 10
```

> **Note:** `--userns=keep-id` maps your host UID into the container so
> bind-mount writes work with Podman's rootless mode.  If you use Docker
> instead, replace it with `--user "$(id -u):$(id -g)"`.

| Flag            | Purpose                                                   |
|-----------------|-----------------------------------------------------------|
| `--manifest`    | Path to the transfer manifest inside the container        |
| `--output-dir`  | Where downloads land (mounted from host `staging/`)       |
| `--threads`     | Parallel FTP connections (2 is polite for testing)        |
| `--limit`       | Redundant safety cap (already limited in Phase 1)         |

After the container exits, `notebooks/staging/` will contain:

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

Each data file has a `.md5` sidecar containing the hex digest verified against
the FTP server's `md5checksums.txt`.

> **Without Docker:** You can also run the CLI directly if you have the project
> installed locally:
>
> ```sh
> uv run ncbi_ftp_sync \
>   --manifest notebooks/output/transfer_manifest.txt \
>   --output-dir staging \
>   --threads 2 --limit 10
> ```

### 3d. Upload staged files to MinIO

The download step writes to the local filesystem.  To feed Phase 3 we need
to upload the staged files into MinIO under a staging prefix:

```sh
uv run python scripts/s3_local.py cp notebooks/staging/raw_data/ s3://cdm-lake/staging/run1/raw_data/
```

Verify the upload:

```sh
uv run python scripts/s3_local.py ls s3://cdm-lake/staging/run1/
```

---

## 4. Phase 3 — Promote & archive (notebook)

Open `notebooks/ncbi_ftp_promote.ipynb`.

### Constants to change (Cell 3)

| Constant                | Walkthrough value                                    | Format | Why                                         |
|-------------------------|------------------------------------------------------|--------|---------------------------------------------|
| `STORE_BUCKET`          | `"cdm-lake"`                                         | bucket name | matches the bucket created in Step 1   |
| `STAGING_KEY_PREFIX`    | `"staging/run1/"`                                    | S3 key prefix | matches the upload prefix from Step 3d |
| `REMOVED_MANIFEST_PATH` | `None`                                              | local path | nothing to remove on first run            |
| `UPDATED_MANIFEST_PATH` | `None`                                              | local path | nothing to archive on first run           |
| `NCBI_RELEASE`          | `None`                                               | string | no release tag needed for local testing     |
| `MANIFEST_S3_KEY`       | `None`                                               | S3 object key | skip manifest trimming               |
| `LAKEHOUSE_KEY_PREFIX`  | `"tenant-general-warehouse/kbase/datasets/ncbi/"`    | S3 key prefix | keep default                         |
| `DRY_RUN`               | `True`                                               | bool   | **start with dry-run!**                     |

### Initialise the S3 client for MinIO

The notebook calls `get_s3_client()` which, by default, tries to import
credentials from `berdl_notebook_utils`.  For local MinIO you need to
initialise the client manually **before** running Cell 4.  Insert a new cell
after Cell 2 (Imports) with:

```python
from cdm_data_loaders.utils.s3 import get_s3_client, reset_s3_client

reset_s3_client()  # clear any cached client
get_s3_client({
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
})
```

### Run the notebook

1. Execute all cells.  With `DRY_RUN = True` the promote step will log what it
   *would* do without moving any objects.
2. Review the report in Cell 6.
3. If the dry-run looks correct, set `DRY_RUN = False` in Cell 3 and re-run
   from Cell 3.

After promotion the final Lakehouse layout in MinIO will look like:

```
cdm-lake/
  tenant-general-warehouse/kbase/datasets/ncbi/
    raw_data/GCF/900/000/615/GCF_900000615.1_.../
      GCF_900000615.1_..._genomic.fna.gz          (with md5 in user metadata)
      GCF_900000615.1_..._protein.faa.gz
      ...
```

---

## 5. Inspect results in MinIO

Browse the [MinIO console](http://localhost:9001) or use the CLI:

```sh
# List final Lakehouse objects
uv run python scripts/s3_local.py ls \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/raw_data/

# Check user metadata (md5) on a specific object
uv run python scripts/s3_local.py head \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/raw_data/GCF/900/000/615/GCF_900000615.1_PRJEB7657_assembly/GCF_900000615.1_PRJEB7657_assembly_genomic.fna.gz
```

---

## 6. Incremental run (second sync)

To exercise the diff/update/archive logic, repeat the pipeline with a
previous snapshot:

2. **Phase 1:** Set `PREVIOUS_SUMMARY_URI` to an S3 path where you upload the
   raw summary from the first run, or save the `raw_summary` string from Cell 4
   to a local file and pass it via `parse_assembly_summary(Path("prev.txt"))`.
2. **Phase 1:** The diff will now show `updated`, `replaced`, and
   `suppressed` entries (if any changed between runs).
3. **Phase 2:** Download the new manifest.
4. **Phase 3:** Set `REMOVED_MANIFEST_PATH` and `UPDATED_MANIFEST_PATH` to the paths
   from Phase 1.  Updated assemblies will be archived before overwrite;
   removed assemblies will be archived and deleted.

---

## 7. Cleanup

```sh
# Stop and remove MinIO
docker stop minio && docker rm minio

# Remove local staging data
rm -rf staging/ output/
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `berdl_notebook_utils` import error in notebook | Missing local MinIO client init | Add the `get_s3_client({...})` cell described in Step 4 |
| `connect_ftp() timeout` | NCBI FTP may be slow or rate-limited | Retry; reduce `--threads` to 1 |
| `CRC64NVME` errors uploading to MinIO | MinIO version too old (needs ≥ `2025-02-07`) | Pin to `minio/minio:RELEASE.2025-02-28T09-55-16Z` or newer |
| Phase 3 shows 0 promoted | Staging prefix doesn't match or bucket is wrong | Verify `STAGING_KEY_PREFIX` matches the S3 upload path from Step 3d |
| Container can't reach FTP | Docker network isolation | Use `--network host` or ensure DNS resolution works inside the container |

---

## Reference: file filters

Phase 2 downloads only files matching these suffixes (defined in
`cdm_data_loaders.ncbi_ftp.assembly.FILE_FILTERS`):

| Suffix | Content |
|--------|---------|
| `_genomic.fna.gz` | Genome nucleotide sequences |
| `_genomic.gff.gz` | Genome annotations (GFF3) |
| `_protein.faa.gz` | Protein sequences |
| `_gene_ontology.gaf.gz` | GO annotations |
| `_assembly_report.txt` | Assembly metadata |
| `_assembly_stats.txt` | Assembly statistics |
| `_assembly_regions.txt` | Assembly regions |
| `_ani_contam_ranges.tsv` | ANI contamination ranges |
| `_gene_expression_counts.txt.gz` | Gene expression counts |
| `_normalized_gene_expression_counts.txt.gz` | Normalised expression counts |

Plus the per-assembly `md5checksums.txt` which is always downloaded for
integrity verification.
