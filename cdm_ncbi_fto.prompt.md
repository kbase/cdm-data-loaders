# Plan: Port NCBI Assembly Sync to cdm-data-loaders

Port the 3-phase NCBI assembly transfer pipeline from this repo
([kbase-transfers](https://github.com/kbase/kbase-transfers)) on the `develop-ncbi-automation` branchinto
[kbase/cdm-data-loaders](https://github.com/kbase/cdm-data-loaders/tree/develop)
(develop branch).

- **Phase 2** (container download) becomes a new CTS entrypoint command.
- **Phases 1 and 3** become Jupyter notebooks in `notebooks/`.
- The existing cdm-data-loaders `utils/s3.py` gets new functions for metadata support
  (existing functions are not modified).
- Tests use **moto** (matching cdm-data-loaders conventions).
- FTP logic stays as **ftplib**.
- New code lives in a dedicated `src/cdm_data_loaders/ncbi_ftp/` module,
  separate from existing NCBI REST / refseq code.

### Phase responsibilities

Each phase has a deliberately narrow scope:

| Phase | Input | Output | Responsibility |
|-------|-------|--------|----------------|
| 1 — Manifest | NCBI assembly summary + previous snapshot from S3 | `transfer_manifest.txt`, `removed_manifest.txt`, `diff_summary.json` | **All** filtering logic (prefix ranges, limits, diffing). Produces a final list of what to transfer and what to archive. |
| 2 — Download | `transfer_manifest.txt` (from input mount) | Downloaded files in output mount, preserving FTP directory structure (`GCF/000/001/.../assembly_dir/`) | Reads the manifest; downloads exactly those assemblies from NCBI FTP; verifies MD5; writes `.md5` sidecars. No filtering, no S3 access. |
| 3 — Promote  | Downloaded files in S3 staging prefix + `removed_manifest.txt` | Files at final Lakehouse paths; archived replaced assemblies | Syncs staging → final location. Archives replaced/suppressed assemblies. **Removes successfully-promoted entries from `transfer_manifest.txt`** so an interrupted Phase 2 can resume from where it left off. |

---

## Background: the 3-phase pipeline

The pipeline is documented in this repo's
[scripts/ncbi/README.md](README.md#semi-automated-transfer-pipeline):

| Phase | Script (this repo) | Purpose |
|-------|-------------------|---------|
| 1 — Manifest | [`generate_transfer_manifest.py`](generate_transfer_manifest.py) | Diff NCBI assembly summary against previous snapshot; produce download + removal manifests |
| 2 — Download | [`container_download.py`](container_download.py) | Download assemblies from NCBI FTP, verify MD5, write `.md5` sidecars (runs in CTS container) |
| 3 — Promote  | [`promote_staged_files.py`](promote_staged_files.py) | Copy staged files to final Lakehouse paths; archive replaced/suppressed assemblies |

Supporting code in this repo:

| File | What to port |
|------|-------------|
| [`download_genomes.py`](download_genomes.py) | FTP resilience patterns (TCP keepalive, NOOP pings, thread-local connections), file filters, accession path construction |
| [`../../kbase_transfers/minio_client.py`](../../kbase_transfers/minio_client.py) | Metadata-aware upload pattern (MD5 as user metadata, CRC64/NVME checksums) |
| [`../../tests/test_sync.py`](../../tests/test_sync.py) | Unit tests for parsing, diffing — port to moto-based tests |
| [`../../tests/test_minio_client.py`](../../tests/test_minio_client.py) | Integration test patterns for S3 operations |

---

## Phase A: Extend cdm-data-loaders `utils/s3.py`

The promote step needs to attach user-metadata (MD5) to uploads and read
checksums via HEAD. The existing
[`s3.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/s3.py)
doesn't support custom metadata on upload or `head_object` with checksum
retrieval.

**Prefer adding new functions over modifying existing ones to minimise
impact on other scripts.**

### Steps

1. **Add `upload_file_with_metadata()`** — new function that accepts
   `local_file_path`, `destination_dir`, `metadata: dict[str, str]`,
   optional `object_name`. Passes `Metadata` in `ExtraArgs` alongside the
   existing `ChecksumAlgorithm: CRC64NVME`. Same upload logic as
   `upload_file()` but with metadata support.

2. **Add `head_object(s3_path)`** — new function returning dict with `size`,
   `metadata`, `checksum_crc64nvme` (from `ChecksumCRC64NVME` header), or
   `None` if 404. Uses `ChecksumMode='ENABLED'`.

3. **Add `copy_object_with_metadata()`** — new function wrapping
   `s3.copy_object()` that accepts `metadata` + `MetadataDirective='REPLACE'`
   for archiving replaced assemblies with tags (`archive_reason`, `archive_date`,
   `ncbi_last_release`).

4. **Add moto-based tests** following the existing `mock_s3_client` fixture pattern
   in [`tests/utils/test_s3.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/utils/test_s3.py).
   Use the existing `strip_checksum_algorithm` workaround for moto's CRC64NVME limitation.

**Files modified:**
- `src/cdm_data_loaders/utils/s3.py` — add 3 new functions (no changes to existing functions)
- `tests/utils/test_s3.py` — add corresponding tests

---

## Phase B: Create the `ncbi_ftp` module

New module at `src/cdm_data_loaders/ncbi_ftp/`, separate from the
existing `ncbi_rest_api.py` pipeline and `refseq_pipeline/`.

```
src/cdm_data_loaders/ncbi_ftp/
├── __init__.py
├── ftp_client.py        # FTP: connect, keepalive, list, download, retry
├── manifest.py          # Phase 1: summary diffing, manifest generation, ALL filtering
├── download.py          # Phase 2: CTS container download + MD5 verification
├── promote.py           # Phase 3: sync staging → final, archive, trim manifest
├── checksums.py         # MD5 verification, CRC64/NVME computation
└── settings.py          # Pydantic settings (extends CtsDefaultSettings)
```

### Steps

5. **`ftp_client.py`** — Port from [`download_genomes.py`](download_genomes.py)
   and [`container_download.py`](container_download.py):
   - `connect_ftp(host, timeout)` with TCP keepalive (`SO_KEEPALIVE`, `TCP_KEEPIDLE`, `TCP_KEEPINTVL`)
   - `ftp_noop_keepalive(ftp, interval)` — NOOP sender for idle connections
   - `ftp_list_dir(ftp, path)` — NLST wrapper with retry on `error_temp`
   - `ftp_download_file(ftp, remote_path, local_path)` — `RETR` with retry
   - Thread-local FTP connection management for parallel downloads
   - Use `get_cdm_logger()` instead of print statements

6. **`checksums.py`** — Port from [`download_genomes.py`](download_genomes.py):
   - `compute_crc64nvme(file_path) -> str` — reads in 1MB chunks, returns base64-encoded 8-byte big-endian (uses `awscrt.checksums.crc64nvme`)
   - `verify_md5(file_path, expected_md5) -> bool`
   - `parse_md5_checksums_file(text) -> dict[str, str]` — parses NCBI `md5checksums.txt`

7. **`settings.py`** — Pydantic settings following
   [`cts_defaults.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/cts_defaults.py) pattern:
   - `DownloadSettings(CtsDefaultSettings)` — adds `manifest`, `threads`, `ftp_host`
   - CLI-parseable with `AliasChoices`, `field_validator` for constraints

8. **`manifest.py`** — Port from [`generate_transfer_manifest.py`](generate_transfer_manifest.py):
   - `download_assembly_summary(ftp, database) -> str`
   - `parse_assembly_summary(text) -> dict[str, AssemblyRecord]`
   - `compute_diff(current, previous) -> DiffResult` — new/updated/replaced/suppressed/withdrawn
   - `write_manifest(diff, output_dir)` — writes `transfer_manifest.txt`, `removed_manifest.txt`, `diff_summary.json`
   - **All filtering logic lives here**: prefix-range filtering (`--prefix-from`, `--prefix-to`), `--limit`, any other subsetting
   - The output `transfer_manifest.txt` is the final, filtered list — Phase 2 downloads exactly what's in it

9. **`download.py`** — Port from [`container_download.py`](container_download.py).
   **This phase is deliberately simple**: read manifest, download, verify.
   - `run_download(settings: DownloadSettings)` — main CTS entry point
   - Reads `transfer_manifest.txt` from input mount; each line is an FTP path
   - `download_assembly(ftp, ftp_path, output_dir) -> DownloadResult`
   - File filters: `_genomic.fna.gz`, `_genomic.gff.gz`, `_protein.faa.gz`, `_gene_ontology.gaf.gz`, `_assembly_report.txt`, `_assembly_stats.txt`, etc.
   - MD5 verification (3 retries), `.md5` sidecar writing
   - `ThreadPoolExecutor` for parallel downloads
   - Output preserves FTP directory structure: `{GCF|GCA}/{000}/{001}/{215}/{assembly_dir}/` (same subfolder hierarchy as on the FTP server)
   - Writes `download_report.json` summary
   - `cli()` function using `run_cli(DownloadSettings, run_download)` from
     [`core.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/core.py)
   - **No filtering or subsetting logic** — downloads exactly what's in the manifest

10. **`promote.py`** — Port from [`promote_staged_files.py`](promote_staged_files.py):
    - `run_promote(staging_prefix, removed_manifest, release_tag, manifest_path)`
    - Walk staged files in S3 staging prefix, upload each to final Lakehouse path via `upload_file_with_metadata()` with MD5 from `.md5` sidecars
    - Skip `.md5` and `.crc64nvme` sidecar files themselves
    - Archive replaced/suppressed assemblies (from `removed_manifest.txt`): `copy_object_with_metadata()` to `archive/{release}/...` with metadata tags, then `delete_object()`
    - **Manifest trimming for resumability**: after successfully promoting an assembly's files, remove that entry from `transfer_manifest.txt`. If Phase 2 is re-run after a partial failure, it only downloads the remaining entries.
    - `--dry-run` support

---

## Phase C: Notebooks for Phases 1 and 3

11. **`notebooks/ncbi_ftp_manifest.ipynb`** — Phase 1:
    - Cell 1: imports + S3 client init (`get_s3_client()`)
    - Cell 2: configure parameters (database, prefix-from/to, limit, dry-run)
    - Cell 3: download current assembly summary from FTP
    - Cell 4: load previous summary from S3 (or scan existing prefixes)
    - Cell 5: compute diff, display summary
    - Cell 6: apply filters (prefix range, limit), write manifest files, upload new summary to S3

12. **`notebooks/ncbi_ftp_promote.ipynb`** — Phase 3:
    - Cell 1: imports + S3 client init
    - Cell 2: configure parameters (staging prefix, removed manifest path, release tag, manifest path for trimming, dry-run)
    - Cell 3: scan staged files, display summary
    - Cell 4: promote files to final paths
    - Cell 5: archive replaced/suppressed assemblies
    - Cell 6: trim manifest (remove promoted entries), display promotion report

---

## Phase D: Container integration (Phase 2)

13. Register CLI entry point in `pyproject.toml`:
    ```toml
    [project.scripts]
    ncbi_ftp_sync = "cdm_data_loaders.ncbi_ftp.download:cli"
    ```

14. Add command to `scripts/entrypoint.sh`:
    ```bash
    ncbi_ftp_sync)
      exec /usr/bin/tini -- uv run --no-sync ncbi_ftp_sync "$@"
      ;;
    ```

15. No Dockerfile changes needed (package installed via `uv sync`; entrypoint dispatches).

---

## Phase E: Tests

All tests use **moto** for S3 mocking. No live MinIO dependency in CI.

```
tests/ncbi_ftp/
├── __init__.py
├── conftest.py           # Mock FTP, sample manifests, assembly records
├── test_ftp_client.py    # Mock ftplib: keepalive, retry, thread-local
├── test_checksums.py     # MD5 verify, CRC64/NVME, md5checksums.txt parsing
├── test_manifest.py      # Summary parsing, diff logic, filtering (port from test_sync.py)
├── test_download.py      # Mock FTP + fs: filters, MD5 verify, sidecars, layout
├── test_promote.py       # moto S3: upload with metadata, archive, manifest trimming, dry-run
└── test_settings.py      # Pydantic validation (follow test_ncbi_rest_api.py)
```

### Steps

16. **`test_checksums.py`** — `verify_md5` correct/incorrect, `parse_md5_checksums_file`,
    `compute_crc64nvme` (skip if `awscrt` unavailable)

17. **`test_manifest.py`** — Port relevant tests from this repo's
    [`tests/test_sync.py`](../../tests/test_sync.py): `parse_assembly_summary`,
    `compute_diff` (new/updated/replaced/suppressed/withdrawn), `write_manifest`,
    prefix-range filtering, limit filtering

18. **`test_ftp_client.py`** — Mock `ftplib.FTP`: keepalive options, retry on
    `error_temp`, thread-local connections

19. **`test_download.py`** — Mock FTP + filesystem: file filter logic, MD5
    verification, sidecar writing, directory layout preserves FTP structure,
    `download_report.json`

20. **`test_promote.py`** — moto `mock_s3_client` fixture: upload with metadata,
    archive copy with tags, deletion of originals, **manifest trimming** (verify
    promoted entries removed, remaining entries preserved), dry-run no side effects.
    Use `strip_checksum_algorithm` workaround for CRC64NVME.

21. **`test_settings.py`** — Follow
    [`test_ncbi_rest_api.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/pipelines/test_ncbi_rest_api.py)
    pattern: defaults, all params, CLI variants, invalid values, boolean parsing

---

## Phase F: Dependencies and CI

22. Add `awscrt` to `pyproject.toml` if not already covered by `boto3[crt]`.

23. All new tests run automatically in CI — no `requires_spark` marks needed.
    ruff checks apply (120 char lines, py313 target).

---

## Key reference patterns in cdm-data-loaders

| Pattern | Where to find it |
|---------|-----------------|
| S3 utility functions + moto tests | [`utils/s3.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/s3.py), [`tests/utils/test_s3.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/utils/test_s3.py) |
| CTS settings base class | [`pipelines/cts_defaults.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/cts_defaults.py) |
| `run_cli()` entry point | [`pipelines/core.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/pipelines/core.py) |
| Logger | [`utils/cdm_logger.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/utils/cdm_logger.py) |
| Settings test pattern | [`tests/pipelines/test_ncbi_rest_api.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/pipelines/test_ncbi_rest_api.py) |
| Entrypoint dispatch | [`scripts/entrypoint.sh`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/entrypoint.sh) |
| moto CRC64NVME workaround | `strip_checksum_algorithm()` in [`tests/utils/test_s3.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/utils/test_s3.py) |

---

## Verification

1. `ruff check src/cdm_data_loaders/ncbi_ftp/ tests/ncbi_ftp/` — lint passes
2. `ruff format --check src/cdm_data_loaders/ncbi_ftp/ tests/ncbi_ftp/` — formatting passes
3. `uv run pytest tests/ncbi_ftp/ -v` — all unit tests pass
4. `uv run pytest tests/utils/test_s3.py -v` — new S3 function tests pass
5. Manual: build Docker image, run `ncbi_ftp_sync` with small manifest against local MinIO
6. Manual: run both notebooks against local MinIO for end-to-end verification

---

## Decisions

- **`ncbi_ftp` naming** — distinguishes bulk FTP file transfer from the existing NCBI REST API pipeline (`ncbi_rest_api.py`) and Spark-based refseq processing (`refseq_pipeline/`)
- **New functions in `s3.py`, not modified existing ones** — minimises impact on other scripts; avoids signature changes that could break callers
- **All filtering in Phase 1** — Phase 2 is a pure download-what's-in-the-manifest step; Phase 3 is a pure sync-and-archive step. Clean separation of concerns.
- **Manifest trimming in Phase 3** — enables resumable Phase 2 runs. After promoting files, Phase 3 removes those entries from `transfer_manifest.txt`. Re-running Phase 2 only downloads what's left.
- **Output preserves FTP directory structure** — Phase 2 writes files under the same `GCF/000/001/.../assembly_dir/` path used on the FTP server, making it trivial to correlate staged files with their NCBI source
- **moto for tests** — matches cdm-data-loaders conventions; fast, no Docker in CI. The `strip_checksum_algorithm` workaround handles the CRC64NVME gap.
- **ftplib over httpx** — NCBI FTP is the established bulk download protocol; existing keepalive/NOOP/retry patterns are proven
- **Notebooks for Phases 1 & 3** — interactive, judgement-requiring steps; natural fit for JupyterLab
- **Phase 2 as CTS command** — matches the entrypoint dispatch pattern and CTS mount contract

## Excluded from scope

- Frictionless `datapackage.json` descriptors (only in old monolithic `download_genomes.py`)
- `backfill_checksums.py` (legacy utility, not part of ongoing pipeline)
- `download_genomes.py` monolith (superseded by the 3-phase pipeline)
- Spark/Delta Lake integration (assembly sync is file-level, not data transformation)
