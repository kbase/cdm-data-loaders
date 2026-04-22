# PDB Pipeline — Local End-to-End Walkthrough

Step-by-step instructions for running a small (≤ 10 entry) end-to-end sync
of wwPDB Beta records against a local MinIO container.  The walkthrough uses
the two Jupyter notebooks for Phases 1 and 3, and the project's Docker image
for the Phase 2 rsync download step.

> **Prerequisites:**
> - Docker or Podman
> - [uv](https://docs.astral.sh/uv/) (for running notebooks locally)
> - `rsync` (installed in the Docker image; also needed locally if running the
>   CLI outside the container)
> - Network access to `rsync-beta.rcsb.org` (port 32382) and
>   `files-beta.rcsb.org` (HTTPS)

---

## Architecture overview

```
 Phase 1 (notebook)         Phase 2 (container)           Phase 3 (notebook)
┌────────────────────┐     ┌───────────────────────┐     ┌──────────────────────┐
│ Manifest notebook  │     │ pdb_rsync_sync CLI    │     │ Promote notebook     │
│ ─ download wwPDB   │────▶│ ─ read manifest       │────▶│ ─ promote staged     │
│   Beta holdings    │     │ ─ parallel rsync DL   │     │   files to Lakehouse │
│ ─ diff against     │     │ ─ write .crc64nvme    │     │ ─ archive old ver.   │
│   previous         │     │   sidecar files       │     │ ─ trim manifest      │
│ ─ write manifests  │     └──────────┬────────────┘     │ ─ write descriptors  │
└────────────────────┘                │                   └──────────────────────┘
                                 local volume
                                 mounted into
                                 the container
```

---

## PDB ID format

The wwPDB Beta archive uses **extended IDs** — a `pdb_` prefix followed by
8 lower-case alphanumeric characters (zero-padded from the classic 4-char ID):

```
classic:   1ABC
extended:  pdb_00001abc
```

The 2-character **hash directory** is derived from characters at positions
`[-3:-1]` of the extended ID:

```
pdb_id   = "pdb_00001abc"
hash_dir = "ab"            # pdb_id[-3:-1]
```

---

## Path anatomy

All S3 paths in this pipeline compose from a small set of variables.

### Path formats used

| Format | Example | Description |
|--------|---------|-------------|
| **s3:// URI** | `s3://cdm-lake/staging/pdb-run1/` | Full URI with scheme + bucket + key |
| **bucket name** | `cdm-lake` | Just the bucket, no scheme |
| **S3 key prefix** | `tenant-general-warehouse/kbase/datasets/pdb/` | Path within a bucket (no scheme, no bucket) |
| **local path** | `output/removed_manifest.txt` | Filesystem path on the host |

### Lakehouse object (final location)

```
s3://{BUCKET}/{LAKEHOUSE_KEY_PREFIX}raw_data/{hash_dir}/{pdb_id}/{file_type}/{filename}
     └─ bucket ─┘└── key prefix ───┘└───── build_entry_path() ──────────────┘
```

Example:
```
s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/raw_data/cn/pdb_00001crn/structures/pdb_00001crn.cif.gz
```

### Staging object (Phase 2 output)

```
s3://{BUCKET}/{STAGING_KEY_PREFIX}raw_data/{hash_dir}/{pdb_id}/{file_type}/{filename}
```

### Local output (Phase 1)

```
{OUTPUT_DIR}/transfer_manifest.txt
{OUTPUT_DIR}/removed_manifest.txt
{OUTPUT_DIR}/updated_manifest.txt
{OUTPUT_DIR}/diff_summary.json
{OUTPUT_DIR}/holdings_snapshot.json.gz
```

---

## 1. Setup

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
(login: `minioadmin` / `minioadmin`), or with the included helper:

```sh
uv run python scripts/s3_local.py mb s3://cdm-lake
```

### Install the package and register a Jupyter kernel

Clone the repo (if not already done) and set up the environment:

```bash
cd cdm-data-loaders
uv sync
source .venv/bin/activate
uv pip install -e .
uv pip install ipykernel
uv run python -m ipykernel install --user --name cdm-data-loaders --display-name "cdm-data-loaders"
```

When opening the manifest or promote notebooks, select the `cdm-data-loaders`
kernel.

### Add S3 credentials to the kernel (Lakehouse only)

Open a notebook with the default kernel and run:

```python
import os
for k, v in sorted(os.environ.items()):
    if "AWS" in k or "S3" in k or "MINIO" in k:
        print(f"{k}={v}")
```

Add the printed variables to the `env` block in the kernel's `kernel.json`
(e.g. `.venv/share/jupyter/kernels/cdm-data-loaders/kernel.json`):

```json
{
  "argv": ["..."],
  "display_name": "cdm-data-loaders",
  "language": "python",
  "env": {
    "AWS_ACCESS_KEY_ID": "...",
    "AWS_SECRET_ACCESS_KEY": "...",
    "AWS_DEFAULT_REGION": "..."
  }
}
```

---

## 2. Phase 1 — Generate manifests (notebook)

Open `notebooks/pdb_manifest.ipynb` in JupyterLab or VS Code.

### What the notebook does

1. Downloads three gzipped JSON holdings files from the wwPDB Beta archive
   over HTTPS (`https://files-beta.rcsb.org/pub/wwpdb/pdb/holdings`).
2. Parses them into a structured snapshot of every entry with its file types
   and last-modified date.
3. Diffs the current snapshot against a previous one (or a store scan).
4. Writes the four output manifest files.
5. Saves a `holdings_snapshot.json.gz` to carry forward as the next run's
   baseline.

### Constants to change (Cell 2)

| Constant | Walkthrough value | Format | Why |
|----------|-------------------|--------|-----|
| `HASH_FROM` | `"ab"` | 2-char string | narrow to a single hash bucket |
| `HASH_TO` | `"ab"` | 2-char string | same bucket → very few entries |
| `LIMIT` | `10` | int | cap to 10 entries for speed |
| `PREVIOUS_SNAPSHOT_URI` | `None` | s3:// URI | first run — everything is "new" |
| `SCAN_STORE` | `False` | bool | no prior data to scan on first run |
| `STORE_BUCKET` | `"cdm-lake"` | bucket name | used to prune already-promoted entries |
| `STORE_KEY_PREFIX` | `"tenant-general-warehouse/kbase/datasets/pdb/"` | S3 key prefix | default Lakehouse path prefix |
| `OUTPUT_DIR` | `Path("output")` | local path | keep as-is |

### Initialise the S3 client for MinIO

If any S3 variables point at your local MinIO, you must initialise the client
before running the cells that use it.  Insert a new cell after the Imports
cell with:

```python
from cdm_data_loaders.utils.s3 import get_s3_client, reset_s3_client

reset_s3_client()
get_s3_client({
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
})
```

If `STORE_BUCKET`, `PREVIOUS_SNAPSHOT_URI`, and the snapshot upload URI are
all `None`, this cell can be skipped.

### Optional: Bootstrap from an existing store (SCAN_STORE)

If you have a pre-populated S3 store but no previous snapshot, set
`SCAN_STORE = True`.  The notebook will scan all objects under
`s3://{STORE_BUCKET}/{STORE_KEY_PREFIX}` and extract existing PDB IDs as the
baseline, so only genuinely new or updated entries appear in
`transfer_manifest.txt`.

```python
SCAN_STORE = True
STORE_BUCKET = "cdm-lake"
STORE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb/"
```

The `tqdm` progress bar shows the running count and the most recently scanned
ID.  For a store with hundreds of thousands of entries this step takes a few
minutes.

### Run the notebook

Execute all cells in order.  After the final cell you should have:

```
output/
  transfer_manifest.txt        # ≤ 10 PDB IDs (one per line)
  removed_manifest.txt         # empty on first run
  updated_manifest.txt         # empty on first run
  diff_summary.json            # counts: new / updated / removed
  holdings_snapshot.json.gz    # serialised snapshot for next run
```

Inspect `transfer_manifest.txt` — each line is a lowercase extended PDB ID:

```
pdb_00001abc
pdb_00001crn
...
```

### Upload manifests to S3 for Phase 2

Cell 7 optionally uploads the manifests to an S3 staging prefix so that CTS
can stage them into the container.  For local testing, copy them manually:

```sh
# Upload transfer manifest to MinIO staging prefix
uv run python scripts/s3_local.py cp \
  output/transfer_manifest.txt \
  s3://cdm-lake/staging/pdb-run1/transfer_manifest.txt
```

---

## 3. Phase 2 — Download entries (container)

Phase 2 uses the `pdb_rsync_sync` CLI, which performs parallel rsync-based
downloads from `rsync-beta.rcsb.org` (port 32382).

> **CTS (CDM Task Service):** In production, Phase 2 runs as a CTS job.
> CTS stages `transfer_manifest.txt` into `/input_dir` inside the container
> and copies container output back to the S3 staging prefix in `/output_dir`.
> The container never receives S3 credentials.
> See [cdm-task-service](https://github.com/kbase/cdm-task-service) for details.

For local testing, run the container directly.

### 3a. Build the container image

```sh
# From the repository root
docker build -t cdm-data-loaders .
```

### 3b. Prepare local directories

```sh
mkdir -p notebooks/staging
cp output/transfer_manifest.txt notebooks/staging/
```

### 3c. Run the download

```sh
docker run --rm \
  --userns=keep-id \
  -v "$(pwd)/notebooks/staging:/input:ro" \
  -v "$(pwd)/notebooks/staging:/output" \
  cdm-data-loaders pdb_rsync_sync \
    --manifest /input/transfer_manifest.txt \
    --output-dir /output \
    --workers 2 \
    --limit 10
```

> **Note:** `--userns=keep-id` maps your host UID into the container for
> Podman rootless mode.  With Docker, use `--user "$(id -u):$(id -g)"` instead.

| Flag | Purpose |
|------|---------|
| `--manifest` | Path to the transfer manifest inside the container |
| `--output-dir` | Where downloads land (mounted from host `staging/`) |
| `--workers` | Parallel rsync connections (2 is polite for testing) |
| `--limit` | Redundant safety cap (already limited in Phase 1) |
| `--file-types` | Comma-separated subset of file types (optional; default: all) |

After the container exits, `notebooks/staging/` will contain:

```
staging/
  raw_data/
    ab/
      pdb_00001abc/
        structures/
          pdb_00001abc.cif.gz
          pdb_00001abc.cif.gz.crc64nvme
        experimental_data/
          pdb_00001abc-sf.cif.gz
          pdb_00001abc-sf.cif.gz.crc64nvme
        validation_reports/
          pdb_00001abc_validation.pdf.gz
          pdb_00001abc_validation.pdf.gz.crc64nvme
        assemblies/
          pdb_00001abc-assembly1.cif.gz
          pdb_00001abc-assembly1.cif.gz.crc64nvme
  download_report.json
```

Each data file has a `.crc64nvme` sidecar containing the CRC64NVME checksum
used to verify the transfer.

> **Without Docker:** You can also run the CLI directly with rsync installed:
>
> ```sh
> uv run pdb_rsync_sync \
>   --manifest output/transfer_manifest.txt \
>   --output-dir staging \
>   --workers 2 --limit 10
> ```

### 3d. Upload staged files to MinIO

```sh
uv run python scripts/s3_local.py cp \
  notebooks/staging/raw_data/ \
  s3://cdm-lake/staging/pdb-run1/raw_data/
```

Verify:

```sh
uv run python scripts/s3_local.py ls s3://cdm-lake/staging/pdb-run1/
```

---

## 4. Phase 3 — Promote & archive (notebook)

Open `notebooks/pdb_promote.ipynb`.

### Constants to change (Cell 2)

| Constant | Walkthrough value | Format | Why |
|----------|-------------------|--------|-----|
| `BUCKET` | `"cdm-lake"` | bucket name | matches the bucket from Step 1 |
| `STAGING_KEY_PREFIX` | `"staging/pdb-run1/"` | S3 key prefix | matches the upload prefix from Step 3d |
| `REMOVED_MANIFEST_PATH` | `None` | local path | nothing to remove on first run |
| `UPDATED_MANIFEST_PATH` | `None` | local path | nothing to archive on first run |
| `PDB_RELEASE` | `None` | string | no release tag needed for local testing |
| `MANIFEST_S3_KEY` | `None` | S3 object key | skip manifest trimming |
| `LAKEHOUSE_KEY_PREFIX` | `"tenant-general-warehouse/kbase/datasets/pdb/"` | S3 key prefix | keep default |
| `DRY_RUN` | `True` | bool | **start with dry-run!** |

### Initialise the S3 client for MinIO

Insert a cell after the Imports cell:

```python
from cdm_data_loaders.utils.s3 import get_s3_client, reset_s3_client

reset_s3_client()
get_s3_client({
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
})
```

### Run the notebook

1. Execute all cells with `DRY_RUN = True`.  The promote cell will log what it
   *would* do without moving any objects.
2. Review the report output.
3. If the dry-run looks correct, set `DRY_RUN = False` and re-run from Cell 2.

After a successful promote run the Lakehouse in MinIO will look like:

```
cdm-lake/
  tenant-general-warehouse/kbase/datasets/pdb/
    raw_data/
      ab/
        pdb_00001abc/
          structures/pdb_00001abc.cif.gz
          experimental_data/pdb_00001abc-sf.cif.gz
          validation_reports/pdb_00001abc_validation.pdf.gz
          assemblies/pdb_00001abc-assembly1.cif.gz
    metadata/
      pdb_00001abc_datapackage.json
```

The `.crc64nvme` sidecars are **not** copied to the Lakehouse — they are used
by the promote step for transfer-integrity verification only.

---

## 5. Inspect results in MinIO

Browse the [MinIO console](http://localhost:9001) or use the CLI:

```sh
# List final Lakehouse objects for a single entry
uv run python scripts/s3_local.py ls \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/raw_data/ab/pdb_00001abc/

# List all promoted entries
uv run python scripts/s3_local.py ls \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/raw_data/
```

### Frictionless metadata descriptors

Each promoted entry gets a [frictionless](https://framework.frictionlessdata.io/)
data package descriptor stored at:

```
s3://{BUCKET}/{LAKEHOUSE_KEY_PREFIX}metadata/{pdb_id}_datapackage.json
```

Example:

```
s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/metadata/pdb_00001abc_datapackage.json
```

The descriptor follows the KBase credit metadata schema (v1.0) and records:

- **identifier** — `PDB:{pdb_id}`, e.g. `PDB:pdb_00001abc`
- **resource_type** — always `"dataset"`
- **url** — canonical RCSB URL, e.g. `https://www.rcsb.org/structure/1ABC`
- **resources** — list of promoted files with their final S3 key, byte size,
  and file format
- **contributors / publisher** — RCSB organizational metadata (ROR:02e8wq794)
- **meta.saved_by** — `"cdm-data-loaders-pdb"`
- **meta.credit_metadata_source** — `"rsync-beta.rcsb.org"`

When an entry is archived (updated or obsoleted), its live descriptor is
copied to:

```
s3://{BUCKET}/{LAKEHOUSE_KEY_PREFIX}archive/{pdb_release}/metadata/{pdb_id}_datapackage.json
```

To inspect a descriptor directly:

```sh
uv run python scripts/s3_local.py cat \
  s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/metadata/pdb_00001abc_datapackage.json
```

---

## 6. Incremental run (second sync)

PDB releases new and updated structures every **Wednesday**.  To exercise the
diff/update/archive logic on a subsequent run:

1. **Phase 1:** Set `PREVIOUS_SNAPSHOT_URI` to the S3 path where you uploaded
   `holdings_snapshot.json.gz` from the previous run.  The diff will now show
   `updated` and `removed` entries.
2. **Phase 1:** Download `removed_manifest.txt` and `updated_manifest.txt`
   from the staging prefix before running Phase 3.
3. **Phase 2:** Download and stage only the entries in the new
   `transfer_manifest.txt`.
4. **Phase 3:** Set the following values:
   - `REMOVED_MANIFEST_PATH` → path to `removed_manifest.txt`
   - `UPDATED_MANIFEST_PATH` → path to `updated_manifest.txt`
   - `PDB_RELEASE` → the Wednesday release date (e.g. `"2025-04-23"`)

   Updated entries will be pre-archived before the new files overwrite them.
   Obsoleted entries will be archived and deleted from the live Lakehouse.

---

## 7. Production notes

### Hash-range batching

For a full production sync of the ~230,000 PDB entries, divide the work into
hash-range batches.  Each batch is a separate Phase 1 → Phase 2 → Phase 3
cycle:

| Batch | `HASH_FROM` | `HASH_TO` |
|-------|-------------|-----------|
| 1     | `"00"`      | `"3f"`    |
| 2     | `"40"`      | `"7f"`    |
| 3     | `"80"`      | `"bf"`    |
| 4     | `"c0"`      | `"ff"`    |

Set `LIMIT = None` for production runs.

### Manifest trimming

Set `MANIFEST_S3_KEY` to the S3 object key of `transfer_manifest.txt`
(e.g. `"staging/pdb-run1/transfer_manifest.txt"`) so the promote notebook
removes each successfully promoted ID from the manifest.  This makes the
pipeline **resumable** — a re-run of Phase 3 only processes entries that
haven't been promoted yet.

### Release tag

Always set `PDB_RELEASE` to the current Wednesday release date in
`YYYY-MM-DD` format.  It is embedded in the archive path and in the
`pdb_last_release` metadata key of archived objects, making it easy to audit
which release each version came from.

---

## 8. Cleanup

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
| `connect` timeout in Phase 1 | `files-beta.rcsb.org` unreachable | Check network; retry |
| `rsync: connection unexpectedly closed` | Port 32382 blocked | Ensure outbound TCP 32382 is open |
| `rsync: failed to connect` | Wrong host or port | Verify `RSYNC_HOST = "rsync-beta.rcsb.org"`, `RSYNC_PORT = 32382` |
| Phase 3 shows 0 promoted | Staging prefix doesn't match | Verify `STAGING_KEY_PREFIX` matches the upload path from Step 3d |
| `CRC64NVME` errors uploading to MinIO | MinIO version too old | Pin to `minio/minio:RELEASE.2025-02-28T09-55-16Z` or newer |
| Phase 1 downloads ~200K entries on first run | `HASH_FROM`/`HASH_TO` not set | Set a narrow range (`"ab"` → `"ab"`) or set `LIMIT = 10` |
| `berdl_notebook_utils` import error | Missing local client init | Add the `get_s3_client({...})` cell described in Step 2 |
| Descriptor not written | Entry had no promotable files | Verify staging prefix is correct and files exist under `raw_data/` |

---

## Reference: rsync source layout

Phase 2 syncs from the wwPDB Beta rsync server.  Each entry maps to:

```
rsync-beta.rcsb.org::pdb_data/entries/{hash_dir}/{pdb_id}/
```

Example:

```
rsync-beta.rcsb.org::pdb_data/entries/ab/pdb_00001abc/
  structures/
    pdb_00001abc.cif.gz
  experimental_data/
    pdb_00001abc-sf.cif.gz
  validation_reports/
    pdb_00001abc_validation.pdf.gz
  assemblies/
    pdb_00001abc-assembly1.cif.gz
```

### File-type subdirectories

| Subdirectory | Content |
|--------------|---------|
| `structures` | PDBx/mmCIF coordinate files |
| `experimental_data` | Structure factor / NMR restraint files |
| `validation_reports` | wwPDB validation PDF and XML reports |
| `assemblies` | Biological assembly coordinate files |

All four subdirectories are downloaded by default.  Pass `--file-types` to
restrict to a subset (e.g. `--file-types structures,validation_reports`).
