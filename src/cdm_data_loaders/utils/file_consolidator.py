# consolidation_core.py
"""Reusable core functionality for consolidating JSON and JSON Lines files into JSONL files.

Includes optional gzip compression and manifest files for tracking and resuming after a crash.
"""

import gzip
import json
import logging
import os
import shutil
import tempfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, RootModel, ValidationError
from smart_open import open as smart_open_fn

logger = logging.getLogger(__name__)

TMP_SUFFIX = ".tmp"
MANIFEST_NAME = "manifest.json"


class RelatedDirectoriesError(ValueError):
    """Raised when two directories that must be independent (e.g. an
    input directory and an output directory) turn out to be the same
    directory, or one nested inside the other.
    """


def _is_relative_to(path: Path, other: Path) -> bool:
    try:
        path.relative_to(other)
    except ValueError:
        return False
    return True


def check_dirs_unrelated(dirs: dict[str, Path]) -> None:
    """Verify that none of the given directories are the same as, or nested within, any of the others.

    `dirs` maps a human-readable label to a path, e.g.
    {"input_dir": Path("/data/in"), "output_dir": Path("/data/out")}.
    Paths are resolved (symlinks followed, relative components collapsed)
    before comparison so that e.g. "./out" and "/abs/path/out" are
    correctly recognized as identical. Paths need not exist yet.

    This exists primarily to prevent a script from later reading its own
    freshly-written output as new input on a subsequent run -- which,
    given these scripts now recognize .jsonl files as valid input, could
    otherwise cause unbounded, ever-growing output on repeated runs.
    """
    resolved = {label: Path(p).resolve() for label, p in dirs.items()}
    labels = list(resolved)

    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            label_a, label_b = labels[i], labels[j]
            path_a, path_b = resolved[label_a], resolved[label_b]

            if path_a == path_b:
                err_msg = f"{label_a} and {label_b} resolve to the same directory ({path_a}); they must be distinct."
                raise RelatedDirectoriesError(err_msg)

            if _is_relative_to(path_b, path_a):
                err_msg = (
                    f"{label_b} ({path_b}) is nested inside {label_a} ({path_a}); "
                    "this risks the script consuming its own output as input "
                    "on a future run."
                )
                raise RelatedDirectoriesError(err_msg)

            if _is_relative_to(path_a, path_b):
                err_msg = (
                    f"{label_a} ({path_a}) is nested inside {label_b} ({path_b}); "
                    "this risks the script consuming its own output as input "
                    "on a future run."
                )
                raise RelatedDirectoriesError(err_msg)


def output_suffix(gzip_output: bool) -> str:
    """Return the file extension to use for a jsonl output file."""
    return ".jsonl.gz" if gzip_output else ".jsonl"


# Manifest entry models
class ManifestEntry(BaseModel):
    """Fields common to every manifest entry, regardless of how the file was built."""

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    child_dirs: list[str] = Field(default_factory=list)


class ConsolidatedEntry(ManifestEntry):
    """Entry for a file built by directly parsing raw per-record JSON files (consolidate_json_files)."""

    kind: Literal["consolidated"] = "consolidated"
    n_records: int
    n_errors: int = 0
    n_source_files: int | None = None
    first_file: str | None = None
    last_file: str | None = None


class MergedEntry(ManifestEntry):
    """Entry for a file built by byte-concatenating already-built jsonl files (merge_jsonl_files), with no JSON re-parsing."""

    kind: Literal["merged"] = "merged"
    n_source_files: int


AnyManifestEntry = Annotated[ConsolidatedEntry | MergedEntry, Field(discriminator="kind")]


class Manifest(RootModel[dict[str, AnyManifestEntry]]):
    """The full manifest for a level/output directory: maps output filename -> the typed entry describing how it was built."""

    root: dict[str, AnyManifestEntry] = Field(default_factory=dict)

    def __contains__(self, filename: str) -> bool:
        return filename in self.root

    def __getitem__(self, filename: str) -> AnyManifestEntry:
        return self.root[filename]

    def __setitem__(self, filename: str, entry: AnyManifestEntry) -> None:
        self.root[filename] = entry

    def __delitem__(self, filename: str) -> None:
        del self.root[filename]

    def __iter__(self):
        return iter(self.root)

    def __len__(self) -> int:
        return len(self.root)

    def items(self):
        return self.root.items()


# Atomic write helpers


def atomic_replace(tmp_path: Path, final_path: Path) -> None:
    """Atomically rename a completed temp file into place. Requires tmp_path and final_path to be on the same filesystem."""
    # replace with Path.replace
    os.replace(tmp_path, final_path)


def cleanup_orphaned_tmp_files(root_dir: Path) -> int:
    """Remove leftover *.tmp files left behind by a crashed run.

    Returns the number of files removed.
    """
    if not root_dir.exists():
        return 0

    n_removed = 0
    for tmp_file in root_dir.rglob(f"*{TMP_SUFFIX}"):
        try:
            tmp_file.unlink()
            n_removed += 1
        except OSError:
            logger.exception("Failed to remove orphaned temp file %s", tmp_file)

    if n_removed:
        logger.info("Removed %d orphaned temp file(s) under %s", n_removed, root_dir)

    return n_removed


# Manifest handling
def manifest_path(level_dir: Path) -> Path:
    return level_dir / MANIFEST_NAME


def load_manifest(level_dir: Path) -> Manifest:
    """Load and validate a directory's manifest, returning an empty Manifest if absent, unreadable, or failing schema validation."""
    path = manifest_path(level_dir)
    if not path.exists():
        return Manifest()

    try:
        raw = path.read_text(encoding="utf-8")
        return Manifest.model_validate_json(raw)
    except (ValidationError, json.JSONDecodeError, OSError):
        logger.exception("Could not read/validate manifest %s; treating as empty", path)
        return Manifest()


def save_manifest_atomic(level_dir: Path, manifest: Manifest) -> None:
    """Write a manifest atomically so it is never left half-written."""
    level_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=level_dir, prefix=MANIFEST_NAME, suffix=TMP_SUFFIX)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(manifest.model_dump_json())
        atomic_replace(tmp_path, manifest_path(level_dir))
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def cleanup_stale_outputs(
    level_dir: Path,
    is_stale: Callable[[AnyManifestEntry], bool],
) -> None:
    """Remove any manifest entry (and its output file) for which `is_stale(entry)` returns True.

    The staleness check is left to the caller since only they know what "stale" means for their grouping
    scheme (e.g. a source directory that has since become incomplete).
    """
    manifest = load_manifest(level_dir)
    if not len(manifest):
        return

    dirty = False
    for filename in list(manifest.root):
        entry = manifest[filename]
        if is_stale(entry):
            (level_dir / filename).unlink(missing_ok=True)
            del manifest[filename]
            dirty = True
            logger.info("Removed stale output %s", level_dir / filename)

    if dirty:
        save_manifest_atomic(level_dir, manifest)


# Core consolidation operations


@dataclass
class ConsolidationResult:
    """Outcome of building or merging a single output jsonl file."""

    status: str
    filename: str | None = None
    entry: AnyManifestEntry | None = None
    skipped: bool = False


def consolidate_json_files(
    json_files: list[Path],
    output_dir: Path,
    output_name: str,
    *,
    gzip_output: bool = False,
    force: bool = False,
    extra_manifest_fields: dict | None = None,
) -> ConsolidationResult:
    """Read a list of individual JSON files and write them out as a single JSON Lines file.

    Files are written atomically, with optional gzip compression. This parses each source file, validating it and normalizing formatting.
    """
    suffix = output_suffix(gzip_output)
    filename = f"{output_name}{suffix}"
    final_path = output_dir / filename

    if not force and final_path.exists():
        return ConsolidationResult(status=f"skip (already complete): {filename}", skipped=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=output_dir, suffix=TMP_SUFFIX)
    os.close(fd)
    tmp_path = Path(tmp_name)

    n_records = 0
    n_errors = 0
    try:
        write_fh = (
            gzip.open(tmp_path, mode="wt", encoding="utf-8")
            if gzip_output
            else tmp_path.open(mode="w", encoding="utf-8")
        )
        with write_fh as out_fh:
            for json_file in sorted(json_files):
                try:
                    with json_file.open("r", encoding="utf-8") as in_fh:
                        record = json.load(in_fh)
                except (json.JSONDecodeError, OSError):
                    logger.exception("Failed to load %s; skipping", json_file)
                    n_errors += 1
                    continue

                out_fh.write(json.dumps(record) + "\n")
                n_records += 1

        atomic_replace(tmp_path, final_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    entry = ConsolidatedEntry(
        n_records=n_records,
        n_errors=n_errors,
        **(extra_manifest_fields or {}),
    )

    status = f"built {filename} ({n_records} records"
    if n_errors:
        status += f", {n_errors} error(s)"
    status += ")"

    return ConsolidationResult(status=status, filename=filename, entry=entry)


def merge_jsonl_files(
    source_paths: list[Path],
    output_dir: Path,
    output_name: str,
    *,
    gzip_output: bool = False,
    force: bool = False,
    extra_manifest_fields: dict[str, Any] | None = None,
) -> ConsolidationResult:
    """Merge already-built JSON Lines files into one larger JSON Lines file by raw byte copy (no JSON parsing).

    IMPORTANT: every file in `source_paths` must already match the
    compression state implied by `gzip_output`. Byte-concatenating valid
    gzip streams produces one valid gzip stream containing all records
    (gzip explicitly supports multi-member streams), so this works
    cleanly for both compressed and uncompressed inputs.
    """
    suffix = output_suffix(gzip_output)
    filename = f"{output_name}{suffix}"
    final_path = output_dir / filename

    if not force and final_path.exists():
        return ConsolidationResult(status=f"skip (already complete): {filename}", skipped=True)

    missing = [p for p in source_paths if not p.exists()]
    if missing:
        return ConsolidationResult(
            status=f"skip {filename}: missing prerequisite file(s): {', '.join(str(p) for p in missing)}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=output_dir, suffix=TMP_SUFFIX)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as out_fh:
            for source_path in sorted(source_paths):
                with source_path.open("rb") as in_fh:
                    shutil.copyfileobj(in_fh, out_fh)
        atomic_replace(tmp_path, final_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    entry = MergedEntry(
        n_source_files=len(source_paths),
        **(extra_manifest_fields or {}),
    )

    return ConsolidationResult(
        status=f"built {filename} (merged {len(source_paths)} file(s))",
        filename=filename,
        entry=entry,
    )


def run_and_collect_manifest(
    jobs: list[Callable[[], ConsolidationResult]],
    level_dir: Path,
    max_workers: int,
) -> None:
    """Run a list of zero-arg consolidation jobs in a thread pool, log each outcome, and atomically persist any new manifest entries."""
    manifest = load_manifest(level_dir)
    dirty = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(job) for job in jobs]
        for future in as_completed(futures):
            try:
                result = future.result()
                logger.info(result.status)
                if result.filename is not None and result.entry is not None:
                    manifest[result.filename] = result.entry
                    dirty = True
            except Exception:
                logger.exception("Error running consolidation job")

    if dirty:
        save_manifest_atomic(level_dir, manifest)


# Verification
def verify_level(level_dir: Path, *, gzip_output: bool = False) -> None:
    """Re-validate every file recorded in a directory's manifest.

    The following checks are run:
    - file existence
    - it is valid JSON lines (decompressing first if gzip)
    - record count (where known) matches what was found.
    """
    manifest = load_manifest(level_dir)
    if not len(manifest):
        logger.info("No manifest found at %s; nothing to verify", level_dir)
        return

    n_ok = n_missing = n_bad = 0

    for filename, entry in manifest.items():
        path = level_dir / filename
        if not path.exists():
            logger.error("VERIFY: missing file listed in manifest: %s", path)
            n_missing += 1
            continue

        expected_records = entry.n_records if isinstance(entry, ConsolidatedEntry) else None
        opener = gzip.open if filename.endswith(".gz") else open
        n_lines = 0
        ok = True
        try:
            with opener(path, mode="rt", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    json.loads(line)
                    n_lines += 1
        except (json.JSONDecodeError, OSError, gzip.BadGzipFile):
            logger.exception("VERIFY: invalid file %s", path)
            ok = False

        if ok and expected_records is not None and n_lines != expected_records:
            logger.error(
                "VERIFY: record count mismatch for %s: expected %d, found %d",
                path,
                expected_records,
                n_lines,
            )
            ok = False

        if ok:
            n_ok += 1
        else:
            n_bad += 1

    logger.info(
        "VERIFY %s: %d ok, %d bad, %d missing (of %d total)",
        level_dir,
        n_ok,
        n_bad,
        n_missing,
        len(manifest),
    )


# Suffixes recognized as consolidation input, in the order they're globbed.
# ".json" -> file contains exactly one JSON object.
# ".jsonl" / ".jsonl.gz" -> file contains one JSON object per non-blank line.
INPUT_SUFFIXES: tuple[str, ...] = (".json", ".jsonl", ".jsonl.gz")


def discover_input_files(directory: Path, recursive: bool = False) -> list[Path]:
    """Find and sort all recognized input files (.json / .jsonl / .jsonl.gz)
    under `directory`.
    """
    files: list[Path] = []
    for suffix in INPUT_SUFFIXES:
        pattern = f"**/*{suffix}" if recursive else f"*{suffix}"
        files.extend(directory.glob(pattern))
    return sorted(files)


def _classify_input_file(path: Path) -> tuple[str, bool]:
    """Return (inner_suffix, is_gzipped) for an input file, e.g.
    ('.jsonl', True) for 'foo.jsonl.gz', ('.json', False) for 'foo.json'.
    """
    if path.suffix.lower() == ".gz":
        if len(path.suffixes) < 2:
            return ("", True)
        return (path.suffixes[-2].lower(), True)
    return (path.suffix.lower(), False)


def _read_records_safely(path: Path) -> tuple[list[Any], int]:
    """Read all JSON records from a single input file, parsed according
    to its suffix. Errors are tolerated at the finest granularity
    available: a single bad line in a .jsonl file is skipped without
    losing the rest of that file's valid records, whereas a .json file
    is all-or-nothing (it holds exactly one record).

    Returns (records, n_errors).
    """
    records: list[Any] = []
    n_errors = 0
    inner_suffix, is_gz = _classify_input_file(path)
    opener = gzip.open if is_gz else open

    if inner_suffix == ".json":
        try:
            with opener(path, mode="rt", encoding="utf-8") as fh:
                records.append(json.load(fh))
        except (json.JSONDecodeError, OSError):
            logger.exception("Failed to load %s; skipping", path)
            n_errors += 1

    elif inner_suffix == ".jsonl":
        try:
            with opener(path, mode="rt", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.exception(
                            "Failed to parse line %d of %s; skipping line",
                            line_no,
                            path,
                        )
                        n_errors += 1
        except OSError:
            logger.exception("Failed to open %s; skipping", path)
            n_errors += 1

    else:
        logger.error("Unrecognized input file suffix for %s; skipping", path)
        n_errors += 1

    return records, n_errors


def consolidate_input_files(
    input_files: list[Path],
    output_dir: Path,
    output_name: str,
    *,
    gzip_output: bool = False,
    force: bool = False,
    extra_manifest_fields: dict[str, Any] | None = None,
) -> ConsolidationResult:
    """Read a list of input files -- each either a single-object .json
    file or a multi-record .jsonl/.jsonl.gz file -- and write all of
    their records out as a single JSON Lines file, atomically, with
    optional gzip compression.

    (Renamed from consolidate_json_files: it now accepts a mix of .json
    and .jsonl input files rather than assuming one JSON object per file.)
    """
    suffix = output_suffix(gzip_output)
    filename = f"{output_name}{suffix}"
    final_path = output_dir / filename

    if not force and final_path.exists():
        return ConsolidationResult(status=f"skip (already complete): {filename}", skipped=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=output_dir, suffix=TMP_SUFFIX)
    os.close(fd)
    tmp_path = Path(tmp_name)

    n_records = 0
    n_errors = 0
    try:
        write_fh = (
            gzip.open(tmp_path, mode="wt", encoding="utf-8")
            if gzip_output
            else tmp_path.open(mode="w", encoding="utf-8")
        )
        with write_fh as out_fh:
            for input_file in sorted(input_files):
                records, file_errors = _read_records_safely(input_file)
                n_errors += file_errors
                for record in records:
                    out_fh.write(json.dumps(record) + "\n")
                    n_records += 1

        atomic_replace(tmp_path, final_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    entry = ConsolidatedEntry(
        n_records=n_records,
        n_errors=n_errors,
        **(extra_manifest_fields or {}),
    )

    status = f"built {filename} ({n_records} records"
    if n_errors:
        status += f", {n_errors} error(s)"
    status += ")"

    return ConsolidationResult(status=status, filename=filename, entry=entry)


# Optional S3 upload
def upload_manifest_outputs_to_s3(
    level_dir: Path,
    s3_prefix: str,
    *,
    transport_params: dict[str, Any] | None = None,
    force: bool = False,
) -> None:
    """Upload every file referenced in a level's manifest to S3, skipping files already present remotely unless force=True.

    s3_prefix should be a full s3://bucket/path/ prefix ending in '/'.
    """
    manifest = load_manifest(level_dir)
    for filename in manifest:
        local_path = level_dir / filename
        if not local_path.exists():
            logger.warning("Manifest entry %s has no local file; skipping upload", filename)
            continue

        s3_uri = s3_prefix.rstrip("/") + "/" + filename

        if not force:
            try:
                with smart_open_fn(s3_uri, "rb", compression="disable", transport_params=transport_params):
                    logger.info("Already on S3, skipping: %s", s3_uri)
                    continue
            except OSError:
                pass

        logger.info("Uploading %s -> %s", local_path, s3_uri)
        with (
            local_path.open("rb") as src,
            smart_open_fn(s3_uri, "wb", compression="disable", transport_params=transport_params) as dst,
        ):
            shutil.copyfileobj(src, dst)
