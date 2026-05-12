# PDB Pipeline — End-to-End Walkthrough

Step-by-step instructions for running the PDB sync pipeline end-to-end.

Two Phase 2 download approaches are supported.  Choose the one that matches
your situation:

| Approach | When to use |
|----------|-------------|
| **Option A — `pdb_download` notebook** | Local development or one-off production runs via SSH tunnel |
| **Option B — `pdb_rsync_sync` container (CTS)** | Long-term production — runs as a CTS job |

Phases 1 and 3 always run as Jupyter notebooks, in two scenarios:

| Phase | Testing | Production |
|-------|---------|------------|
| Phase 1 — manifest | Local notebook (local MinIO) | Lakehouse JupyterHub (production MinIO) |
| Phase 2 — download | Option A: local notebook · Option B: local container | Option A: local notebook via SSH tunnel · Option B: CTS container |
| Phase 3 — promote | Local notebook (local MinIO) | Lakehouse JupyterHub (production MinIO) |

> **Prerequisites by scenario:**
>
> *Local testing (all phases):*
> - Docker or Podman
> - [uv](https://docs.astral.sh/uv/)
> - `rsync` installed on your local machine
> - Network access to `rsync-beta.rcsb.org` (port 32382) and `files-beta.rcsb.org` (HTTPS)
>
> *Lakehouse (Phases 1 and 3 in production):*
> - Access to [hub.berdl.kbase.us](https://hub.berdl.kbase.us)
> - `cdm-data-loaders` package installed and registered as a Jupyter kernel (see Section 1)
>
> *Option A production download only (SSH tunnel):*
> - SSH access to `login.kbase.us` (contact KBase sysadmin)
> - `rsync` on your local machine
> - MinIO credentials for `minio.berdl.kbase.us`

---

## External APIs

| Service | Endpoint | Documentation |
|---------|----------|---------------|
| wwPDB Beta holdings | `https://files-beta.rcsb.org/pub/wwpdb/pdb/holdings` | [RCSB file download services](https://www.rcsb.org/docs/programmatic-access/file-download-services) |
| wwPDB Beta rsync | `rsync://rsync-beta.rcsb.org/pdb_data/` (port 32382) | [RCSB file download services](https://www.rcsb.org/docs/programmatic-access/file-download-services) |

---

## Architecture overview

**Option A — `pdb_download` notebook**

```
 Phase 1 (notebook)         Phase 2 (notebook)            Phase 3 (notebook)
┌────────────────────┐     ┌───────────────────────┐     ┌──────────────────────┐
│ Manifest notebook  │     │ Download notebook     │     │ Promote notebook     │
│ ─ download wwPDB   │────▶│ ─ read manifest       │────▶│ ─ promote staged     │
│   Beta holdings    │     │ ─ parallel rsync DL   │     │   files to Lakehouse │
│ ─ diff against     │     │ ─ upload staged files │     │ ─ archive old ver.   │
│   previous         │     │   directly to MinIO   │     │ ─ trim manifest      │
│ ─ write manifests  │     └───────────────────────┘     │ ─ write descriptors  │
└────────────────────┘                                    └──────────────────────┘
```

**Option B — `pdb_rsync_sync` container (CTS)**

```
 Phase 1 (notebook)         Phase 2 (container)            Phase 3 (notebook)
┌────────────────────┐     ┌───────────────────────┐     ┌──────────────────────┐
│ Manifest notebook  │     │ pdb_rsync_sync CLI    │     │ Promote notebook     │
│ ─ download wwPDB   │────▶│ ─ read manifest       │────▶│ ─ promote staged     │
│   Beta holdings    │     │ ─ parallel rsync DL   │     │   files to Lakehouse │
│ ─ diff against     │     │ ─ write to local vol  │     │ ─ archive old ver.   │
│   previous         │     │ (CTS stages to S3)    │     │ ─ trim manifest      │
│ ─ write manifests  │     └───────────────────────┘     │ ─ write descriptors  │
└────────────────────┘                                    └──────────────────────┘
```

> **Note:** The `pdb_rsync_sync` container never receives S3 credentials.
> In production, CTS stages the manifest from S3 into the container's
> `/input/` mount and copies `/output/` back to S3 after the job finishes.

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
| **s3:// URI** | `s3://cts/staging/pdb-run1/` | Full URI with scheme + bucket + key |
| **bucket name** | `cdm-lake` | Just the bucket, no scheme |
| **S3 key prefix** | `tenant-general-warehouse/kbase/datasets/pdb/` | Path within a bucket (no scheme, no bucket) |
| **local path** | `output/removed_manifest.txt` | Filesystem path on the host |

Two separate S3 buckets are involved:

| Variable | Walkthrough value | Purpose |
|----------|------------------|---------|
| `STAGING_BUCKET` | `cts` | Staging bucket — Phase 2 writes downloaded files here |
| `LAKEHOUSE_BUCKET` | `cdm-lake` | Final Lakehouse destination — Phase 3 promotes files here |

### Lakehouse object (final location)

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}raw_data/{hash_dir}/{pdb_id}/{file_type}/{filename}
     └── bucket ───────┘└── key prefix ───┘└───── build_entry_path() ──────────────┘
```

Example:
```
s3://cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/raw_data/cn/pdb_00001crn/structures/pdb_00001crn.cif.gz
```

### Staging object (Phase 2 output)

```
s3://{STAGING_BUCKET}/{STAGING_KEY_PREFIX}raw_data/{hash_dir}/{pdb_id}/{file_type}/{filename}
```

Example:
```
s3://cts/staging/pdb-run1/output/raw_data/cn/pdb_00001crn/structures/pdb_00001crn.cif.gz
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

### Local testing

#### Start MinIO

```sh
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:RELEASE.2025-02-28T09-55-16Z server /data --console-address ":9001"
```

(A similar service is included in the `docker-compose.yml` at the repository
root, used in CI test workflows.)

Create test buckets via the [MinIO console](http://localhost:9001)
(login: `minioadmin` / `minioadmin`), or with the included helper:

```sh
uv run python scripts/s3_local.py mb s3://cdm-lake
uv run python scripts/s3_local.py mb s3://cts
```

Two buckets are needed:
- **`cdm-lake`** — the Lakehouse destination (Phase 3 promotes files here)
- **`cts`** — staging bucket (Phase 2 writes downloaded files here)

#### Install the package and register a Jupyter kernel

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

---

### Lakehouse (Phases 1 and 3 in production)

#### Build `cdm-data-loaders`

Clone the `cdm-data-loaders` repo in your Lakehouse user space.  Then build
the package in a virtual environment and register it as a Jupyter kernel:

```bash
cd cdm-data-loaders
uv sync
source .venv/bin/activate
uv pip install -e .
uv pip install ipykernel
uv run python -m ipykernel install --user --name cdm-data-loaders --display-name "cdm-data-loaders"
```

When opening the manifest or promote notebooks, choose the `cdm-data-loaders`
kernel.

#### Add S3 credentials to the kernel

Open a new Jupyter Notebook with the **default kernel** and run:

```python
import os
for k, v in sorted(os.environ.items()):
    if "AWS" in k or "S3" in k or "MINIO" in k:
        print(f"{k}={v}")
```

Copy the output into the `kernel.json` for the `cdm-data-loaders` kernel
(e.g. `cdm-data-loaders/.venv/share/jupyter/kernels/cdm-data-loaders/kernel.json`):

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

Restart the `cdm-data-loaders` kernel after editing `kernel.json`.

---

### SSH tunnel (Option A production download only)

The `pdb_download` notebook runs locally and pushes staged files directly to
the production Lakehouse MinIO.  An SSH SOCKS5 tunnel is required to reach
`minio.berdl.kbase.us` from your local machine.

**One-time setup (run in a terminal before launching Jupyter):**

```sh
# 1. Open the SOCKS5 tunnel (runs in background)
ssh -f -D 1338 -N <ac.anl_username>@login.kbase.us

# 2. Route HTTPS through the tunnel (affects this terminal session only)
export HTTPS_PROXY=socks5h://127.0.0.1:1338

# 3. Verify the tunnel is running
ps aux | grep "ssh -f -D 1338"
```

> ⚠️ `HTTPS_PROXY` routes all HTTPS traffic in this terminal through the
> tunnel.  Open a dedicated terminal for MinIO operations, or
> `unset HTTPS_PROXY` when done.

Retrieve your production MinIO credentials from the Lakehouse JupyterHub
(see **Add S3 credentials to the kernel** above), then set them in your shell
before launching Jupyter:

```sh
export MINIO_ACCESS_KEY=<your-access-key>
export MINIO_SECRET_KEY=<your-secret-key>
```

Or leave them unset — the notebook will prompt via `getpass`.

> 💡 To close the tunnel when finished:
> ```sh
> kill $(ps aux | grep "ssh -f -D 1338" | grep -v grep | awk '{print $2}')
> ```

---

## 2. Phase 1 — Generate manifests (notebook)

Open `notebooks/pdb_manifest.ipynb`.

### What the notebook does

1. Downloads three gzipped JSON holdings files from the wwPDB Beta archive
   over HTTPS (`https://files-beta.rcsb.org/pub/wwpdb/pdb/holdings`).
2. Parses them into a structured snapshot of every entry with its file types
   and last-modified date.
3. Diffs the current snapshot against a previous one (or a store scan).
4. Writes the four output manifest files and a `holdings_snapshot.json.gz`.

### Constants to change (Cell 5)

| Constant | Testing value | Production value | Format | Why |
|----------|---------------|-----------------|--------|-----|
| `HASH_FROM` | `"ab"` | `"00"` (or batch start) | 2-char hex | narrow to single hash bucket for testing |
| `HASH_TO` | `"ab"` | `"3f"` (or batch end) | 2-char hex | same bucket |
| `LIMIT` | `10` | `None` | int or None | cap entries for speed |
| `PREVIOUS_SNAPSHOT_URI` | `None` | S3 URI of previous snapshot | s3:// URI | first run: everything is "new" |
| `SCAN_STORE` | `False` | `False` | bool | set True only to bootstrap from existing store |
| `STORE_BUCKET` | `"cdm-lake"` | production Lakehouse bucket | bucket name | used to prune already-promoted entries |
| `STORE_KEY_PREFIX` | `"tenant-general-warehouse/kbase/datasets/pdb/"` | same or team-specific | S3 key prefix | default Lakehouse path prefix |
| `OUTPUT_DIR` | `Path("output")` | `Path("output")` | local path | keep as-is |

### Credentials (Cell 4)

The notebook has a dedicated credentials cell (Cell 4) immediately after
Imports.

- **Local testing:** Set `PROVIDE_CREDENTIALS = True`.  The cell configures
  the S3 client with `http://localhost:9000` and the `minioadmin` credentials.
- **Production (Lakehouse JupyterHub):** Leave `PROVIDE_CREDENTIALS = False`
  (the default).  Credentials are picked up from the kernel environment
  (see Section 1 — Add S3 credentials to the kernel).

### Optional: Bootstrap from an existing store (SCAN_STORE)

If you have a pre-populated Lakehouse but no previous snapshot, set
`SCAN_STORE = True`.  The notebook will scan all objects under
`s3://{STORE_BUCKET}/{STORE_KEY_PREFIX}` and extract existing PDB IDs as the
baseline, so only genuinely new or updated entries appear in
`transfer_manifest.txt`.

```python
SCAN_STORE = True
STORE_BUCKET = "cdm-lake"
STORE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb/"
```

### Run the notebook

Execute all cells in order.  After the final cell you should have in
`output/`:

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

The final cell also uploads `holdings_snapshot.json.gz` to
`s3://{STORE_BUCKET}/{STORE_KEY_PREFIX}holdings_snapshot.json.gz` so it is
available as `PREVIOUS_SNAPSHOT_URI` on the next run.

### Preparing for Phase 2

The manifest is written to `output/transfer_manifest.txt` on the machine
(or Lakehouse node) where Phase 1 ran.

**Option A (notebook) — local testing:** the manifest is already on your
local machine; nothing extra is needed.

**Option A (notebook) — production:** Phase 1 runs on the Lakehouse
JupyterHub and Phase 2 runs on your local machine.  Download
`transfer_manifest.txt` from the JupyterHub file browser to your local
`output/` directory before running `pdb_download`.

**Option B (CTS container):** Upload the manifest to the staging bucket so
CTS can stage it into the container:

```sh
# From the Lakehouse JupyterHub terminal (production)
aws s3 cp output/transfer_manifest.txt \
  s3://<staging-bucket>/<staging-prefix>/transfer_manifest.txt

# From your workstation (local testing)
uv run python scripts/s3_local.py cp \
  output/transfer_manifest.txt \
  s3://cts/staging/pdb-run1/transfer_manifest.txt
```

---

## 3. Phase 2 — Download & stage entries

### Option A: `pdb_download` notebook

Open `notebooks/pdb_download.ipynb`.  The notebook runs `rsync` locally and
uploads staged files directly to MinIO, pipelining downloads so only one
entry is on disk at a time.

If running locally targetting production, start the notebook from the terminal with the ssh tunnel running:
```bash
uv pip install notebook
jupyter notebook notebooks/pdb_download.ipynb
```

> **Note:** This notebook requires `rsync` on the machine where it runs.
> It is not suitable for running inside JupyterHub (no rsync available there).

#### A1. Local testing (`TARGET = "local"`)

**Constants to set (Cell 4 — parameters, Cell 5 — TARGET):**

| Constant | Testing value | Format | Notes |
|----------|--------------|--------|-------|
| `STAGING_BUCKET` | `"cts"` | bucket name | local test staging bucket |
| `STAGING_KEY_PREFIX` | `"staging/pdb-run1/output/"` | S3 key prefix | must include trailing `output/` |
| `MANIFEST_LOCAL_PATH` | `"output/transfer_manifest.txt"` | local path | written by Phase 1 |
| `MANIFEST_S3_KEY` | `None` | — | set to None when using local path |
| `WORKERS` | `2` | int | polite for testing |
| `LIMIT` | `10` | int | cap for safety |
| `DRY_RUN` | `False` | bool | |
| `TARGET` | `"local"` | string | uses `http://localhost:9000` |

Execute all cells.  After the notebook completes:

```
cts/staging/pdb-run1/output/
  raw_data/
    ab/
      pdb_00001abc/
        structures/pdb_00001abc.cif.gz
        structures/pdb_00001abc.cif.gz.crc64nvme
        experimental_data/pdb_00001abc-sf.cif.gz
        ...
  download_report.json
```

Each data file has a `.crc64nvme` sidecar for transfer-integrity verification.

#### A2. Production (`TARGET = "production"`, SSH tunnel)

Ensure the SSH tunnel is running and `HTTPS_PROXY` is set in your Jupyter
terminal (see Section 1 — SSH tunnel setup).

**Constants to set (Cell 4 — parameters, Cell 5 — TARGET):**

| Constant | Production value | Format | Notes |
|----------|-----------------|--------|-------|
| `STAGING_BUCKET` | production staging bucket | bucket name | confirm with your team |
| `STAGING_KEY_PREFIX` | `"<prefix>/output/"` | S3 key prefix | set for this run; must include trailing `output/` |
| `MANIFEST_LOCAL_PATH` | `"output/transfer_manifest.txt"` | local path | downloaded from JupyterHub in Phase 1 |
| `MANIFEST_S3_KEY` | `None` | — | or set this instead of local path |
| `WORKERS` | `4` | int | adjust to available bandwidth |
| `LIMIT` | `None` | — | process all entries |
| `DRY_RUN` | `False` | bool | |
| `TARGET` | `"production"` | string | uses `https://minio.berdl.kbase.us` |

The notebook will prompt for MinIO credentials via `getpass` if
`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` are not set in the shell environment.

---

### Option B: `pdb_rsync_sync` container (CTS)

> **Long-term production approach.**  In production, CTS runs Phase 2 as a
> container job: it stages `transfer_manifest.txt` from S3 into `/input/`
> inside the container and copies `/output/` back to the S3 staging prefix
> after the job finishes.  The container never receives S3 credentials.
> See [cdm-task-service](https://github.com/kbase/cdm-task-service) for details.

For local integration testing, run the container directly (or call
`pdb_rsync_sync` via `uv run`) and upload the output to MinIO manually.

#### B1. Build the container image

```sh
# From the repository root
docker build -t cdm-data-loaders .
```

#### B2. Prepare local directories

```sh
mkdir -p notebooks/staging
cp output/transfer_manifest.txt notebooks/staging/
```

#### B3. Run the download

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

> **Without Docker:** You can also call the CLI directly:
> ```sh
> uv run pdb_rsync_sync \
>   --manifest output/transfer_manifest.txt \
>   --output-dir notebooks/staging \
>   --workers 2 --limit 10
> ```

After the container (or CLI) exits, `notebooks/staging/` will contain:

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

#### B4. Upload staged files to MinIO (local testing only)

In production, CTS handles this upload automatically.  For local testing,
upload manually to the staging bucket:

```sh
uv run python scripts/s3_local.py cp \
  notebooks/staging/ \
  s3://cts/staging/pdb-run1/output/
```

Verify:

```sh
uv run python scripts/s3_local.py ls s3://cts/staging/pdb-run1/output/
```

---

## 4. Phase 3 — Promote & archive (notebook)

Open `notebooks/pdb_promote.ipynb`.

### Constants to change (Cell 4)

| Constant | Testing value | Production value | Format | Why |
|----------|---------------|-----------------|--------|-----|
| `STAGING_BUCKET` | `"cts"` | production staging bucket | bucket name | must match the bucket used in Phase 2 |
| `LAKEHOUSE_BUCKET` | `"cdm-lake"` | production Lakehouse bucket | bucket name | final Lakehouse destination |
| `STAGING_KEY_PREFIX` | `"staging/pdb-run1/output/"` | prefix used in Phase 2 | S3 key prefix | must match `STAGING_KEY_PREFIX` from Phase 2 |
| `REMOVED_MANIFEST_PATH` | `None` | `"output/removed_manifest.txt"` | local path | download from JupyterHub; None on first run |
| `UPDATED_MANIFEST_PATH` | `None` | `"output/updated_manifest.txt"` | local path | download from JupyterHub; None on first run |
| `PDB_RELEASE` | `None` | `"YYYY-MM-DD"` | string | Wednesday release date; None on first test run |
| `MANIFEST_S3_KEY` | `None` | key to trim after promote | S3 object key | skip trimming when None |
| `LAKEHOUSE_KEY_PREFIX` | `"tenant-general-warehouse/kbase/datasets/pdb/"` | same | S3 key prefix | keep default |
| `DRY_RUN` | `True` | `False` | bool | **start with dry-run!** |

### Credentials (Cell 5)

The notebook has a dedicated credentials cell (Cell 5) between Configure and
S3 validation.

- **Local testing:** Set `PROVIDE_CREDENTIALS = True`.  The cell configures
  the S3 client with `http://localhost:9000` and the `minioadmin` credentials.
- **Production (Lakehouse JupyterHub):** Leave `PROVIDE_CREDENTIALS = False`
  (the default).  Credentials are picked up from the kernel environment
  (see Section 1 — Add S3 credentials to the kernel).

### Run the notebook

1. Execute all cells with `DRY_RUN = True`.  The promote step logs what it
   *would* do without moving any objects.
2. Review the report output.
3. If the dry-run looks correct, set `DRY_RUN = False` and re-run from Cell 4.

After a successful promote run the Lakehouse will look like:

```
cdm-lake/                                           ← LAKEHOUSE_BUCKET
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

# Verify staged files are still in the CTS bucket
uv run python scripts/s3_local.py ls s3://cts/staging/pdb-run1/
```

### Frictionless metadata descriptors

Each promoted entry gets a [frictionless](https://framework.frictionlessdata.io/)
data package descriptor stored at:

```
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}metadata/{pdb_id}_datapackage.json
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
s3://{LAKEHOUSE_BUCKET}/{LAKEHOUSE_KEY_PREFIX}archive/{pdb_release}/metadata/{pdb_id}_datapackage.json
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

1. **Phase 1:** Set `PREVIOUS_SNAPSHOT_URI` to the S3 path where the previous
   snapshot was uploaded (logged at the end of Phase 1).  The diff will now
   show `updated` and `removed` entries.
2. **Phase 1:** Download `removed_manifest.txt` and `updated_manifest.txt`
   from the Lakehouse JupyterHub (production) or from the local `output/`
   directory (testing) before running Phase 3.
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

Set `MANIFEST_S3_KEY` to the S3 object key of `transfer_manifest.txt` within
`STAGING_BUCKET` (e.g. `"staging/pdb-run1/transfer_manifest.txt"`) so the
promote notebook removes each successfully promoted ID from the manifest.
This makes the pipeline **resumable** — a re-run of Phase 3 only processes
entries that haven't been promoted yet.

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
rm -rf notebooks/staging/ output/
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `connect` timeout in Phase 1 | `files-beta.rcsb.org` unreachable | Check network; retry |
| `rsync: connection unexpectedly closed` | Port 32382 blocked | Ensure outbound TCP 32382 is open |
| `rsync: failed to connect` | Wrong host or port | Verify `RSYNC_HOST = "rsync-beta.rcsb.org"`, `RSYNC_PORT = 32382` |
| Phase 3 shows 0 promoted | Staging prefix or bucket doesn't match | Verify `STAGING_BUCKET` and `STAGING_KEY_PREFIX` match the Phase 2 upload path |
| `CRC64NVME` errors uploading to MinIO | MinIO version too old | Pin to `minio/minio:RELEASE.2025-02-28T09-55-16Z` or newer |
| Phase 1 downloads ~200K entries on first run | `HASH_FROM`/`HASH_TO` not set | Set a narrow range (`"ab"` → `"ab"`) or set `LIMIT = 10` |
| No credentials in Lakehouse kernel env | S3 vars not in `kernel.json` | Re-run the env-var snippet in the default kernel and update `kernel.json` |
| Descriptor not written | Entry had no promotable files | Verify staging prefix is correct and files exist under `raw_data/` |
| `HTTPS_PROXY` not working for Option A production | `pysocks` not installed, or using `socks5://` instead of `socks5h://` | Use `socks5h://` (remote DNS) and ensure `pysocks` is installed (`uv pip install pysocks`) |

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
