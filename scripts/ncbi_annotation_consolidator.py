"""NCBI assembly annotation-report consolidation script.

Reads per-assembly JSON files from a GC[AF]_XXX/YYY/GC[AF]_XXXYYYZZZ.N.json
tree, cross-checks them against lists of expected assembly IDs, and
consolidates them into JSON Lines files at a chosen granularity, resumably
and safely across crashes.
"""

import logging
import os
import re
import tempfile
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Final, Literal

from pydantic import Field, model_validator
from pydantic_settings import CliImplicitFlag, SettingsConfigDict

import cdm_data_loaders.utils.file_consolidator as fc
from cdm_data_loaders.core.settings import DEFAULT_SETTINGS_CONFIG_DICT, LoggerSettings

logger = logging.getLogger(__name__)

NCBI_BASE_DIR: Final[Path] = Path("/home") / "ialarmedalien" / "ncbi" / "gtdb_232"

GENBANK: Final[str] = "genbank"
REFSEQ: Final[str] = "refseq"

BASE_DIR: Final[dict[str, Path]] = {
    GENBANK: NCBI_BASE_DIR / GENBANK,
    REFSEQ: NCBI_BASE_DIR / REFSEQ,
}

NCBI_ASSEMBLY_ID_REGEX = re.compile(pattern=r"^(GC[AF]_\d{3})(\d{3})(\d{3})\.(\d+)$")
PARENT_DIR_REGEX = re.compile(r"^GC[AF]_\d{3}$")
CHILD_DIR_REGEX = re.compile(r"^\d{3}$")


def gen_output_path(output_dir: Path, assembly_id: str) -> Path:
    """Generate an appropriate output path for the assembly."""
    assembly_file_name = f"{assembly_id}.json"
    match = NCBI_ASSEMBLY_ID_REGEX.match(assembly_id)
    if not match:
        msg = f"assembly_id '{assembly_id}' does not match expected pattern"
        logger.error(msg)
        return output_dir / assembly_file_name

    return output_dir / match.group(1) / match.group(2) / assembly_file_name


@dataclass(frozen=True)
class GroupLevel:
    name: str
    child_chars: int

    @property
    def wildcard_digits(self) -> int:
        return 6 - self.child_chars


GROUP_LEVELS: dict[str, GroupLevel] = {
    "1K": GroupLevel("1K", child_chars=3),
    "10K": GroupLevel("10K", child_chars=2),
    "100K": GroupLevel("100K", child_chars=1),
    "1M": GroupLevel("1M", child_chars=0),
}


# CLI settings
class Settings(LoggerSettings):
    """Command-line settings for NCBI annotation report consolidation."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name="ncbi_annotation_consolidate",
        extra="forbid",
    )

    target_dataset: Literal["genbank", "refseq"] = Field(
        default="refseq",
        description="Which dataset to process.",
    )
    output_dir: Path | None = Field(
        default=None,
        description="Directory containing the per-assembly JSON annotation "
        "reports (the 'annotation_report' dir). Defaults to "
        "BASE_DIR[<target-dataset>] / 'annotation_report'.",
    )
    group_level: Literal["1K", "10K", "100K", "1M"] = Field(
        default="1K",
        description="Granularity at which to concatenate JSON files into "
        "JSONL files: 1K (per child_dir, canonical unit), 10K, 100K, or 1M "
        "(per parent_dir). Coarser levels are built by merging pre-built "
        "1K files.",
    )
    threads: int = Field(
        default=4,
        ge=1,
        description="Number of worker threads to use for concatenation.",
    )
    force: CliImplicitFlag[bool] = Field(
        default=False,
        description="Rebuild output files even if they already exist and appear complete.",
    )
    verify: CliImplicitFlag[bool] = Field(
        default=False,
        description="Instead of building files, re-validate existing output files against their manifests and exit.",
    )
    gzip: CliImplicitFlag[bool] = Field(
        default=False,
        description="Gzip-compress output jsonl files (.jsonl.gz).",
    )
    s3_prefix: str | None = Field(
        default=None,
        description="If set, upload completed jsonl files to this "
        "s3://bucket/path/ prefix as a final step, after local "
        "consolidation finishes.",
    )

    @model_validator(mode="after")
    def _fill_output_dir_default(self) -> "Settings":
        if self.output_dir is None:
            self.output_dir = BASE_DIR[self.target_dataset] / "annotation_report"
        return self


# Core script logic
def find_redo_ids_and_incomplete_dirs(
    target_dir: Path,
    output_dir: Path,
    redo_file: Path,
) -> set[Path]:
    """Scan ID files, atomically write missing assembly IDs to the redo file, and return the set of leaf (child) output dirs missing files."""
    incomplete_dirs: set[Path] = set()
    missing_ids: list[str] = []

    for id_file in target_dir.glob("*.txt"):
        with id_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                assembly_id = line.strip()
                if not assembly_id:
                    continue
                out_path = gen_output_path(output_dir, assembly_id)
                if not out_path.exists():
                    missing_ids.append(assembly_id)
                    incomplete_dirs.add(out_path.parent)

    fd, tmp_name = tempfile.mkstemp(dir=redo_file.parent, prefix=redo_file.name, suffix=fc.TMP_SUFFIX)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("\n".join(missing_ids))
            if missing_ids:
                fh.write("\n")
        fc.atomic_replace(tmp_path, redo_file)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.info(
        "Found %d missing assembly file(s) across %d incomplete dir(s)",
        len(missing_ids),
        len(incomplete_dirs),
    )
    return incomplete_dirs


def build_groups(
    output_dir: Path,
    group_level: GroupLevel,
) -> dict[tuple[Path, str], list[Path]]:
    groups: dict[tuple[Path, str], list[Path]] = {}
    for parent_dir in output_dir.iterdir():
        if not parent_dir.is_dir() or not PARENT_DIR_REGEX.match(parent_dir.name):
            continue
        for child_dir in parent_dir.iterdir():
            if not child_dir.is_dir() or not CHILD_DIR_REGEX.match(child_dir.name):
                continue
            group_key = child_dir.name[: group_level.child_chars]
            groups.setdefault((parent_dir, group_key), []).append(child_dir)
    return groups


def group_output_name(parent_dir: Path, group_key: str, group_level: GroupLevel) -> str:
    wildcard = "n" * group_level.wildcard_digits
    return f"{parent_dir.name}{group_key}{wildcard}"


def build_1k_level(
    output_dir: Path,
    jsonl_dir: Path,
    incomplete_dirs: set[Path],
    max_workers: int,
    force: bool,
    gzip_output: bool,
) -> Path:
    level_dir = jsonl_dir / "1K"
    level_dir.mkdir(parents=True, exist_ok=True)

    incomplete_strs = {str(p) for p in incomplete_dirs}
    fc.cleanup_stale_outputs(
        level_dir,
        is_stale=lambda entry: bool(set(entry.child_dirs) & incomplete_strs),
    )
    groups = build_groups(output_dir, GROUP_LEVELS["1K"])
    jobs = []
    n_skipped_incomplete = 0

    for (parent_dir, group_key), child_dirs in groups.items():
        child_dir = child_dirs[0]
        if child_dir in incomplete_dirs:
            n_skipped_incomplete += 1
            continue
        json_files = sorted(child_dir.glob("*.json"))
        output_name = group_output_name(parent_dir, group_key, GROUP_LEVELS["1K"])
        jobs.append(
            partial(
                fc.consolidate_json_files,
                json_files,
                level_dir,
                output_name,
                gzip_output=gzip_output,
                force=force,
                extra_manifest_fields={"child_dirs": [str(child_dir)]},
            )
        )

    fc.run_and_collect_manifest(jobs, level_dir, max_workers)

    if n_skipped_incomplete:
        logger.info("Skipped %d incomplete director(y/ies) at 1K level", n_skipped_incomplete)

    return level_dir


def merge_coarser_level(
    output_dir: Path,
    jsonl_dir: Path,
    level_1k_dir: Path,
    group_level: GroupLevel,
    incomplete_dirs: set[Path],
    max_workers: int,
    force: bool,
    gzip_output: bool,
) -> None:
    level_dir = jsonl_dir / group_level.name
    level_dir.mkdir(parents=True, exist_ok=True)

    incomplete_strs = {str(p) for p in incomplete_dirs}
    fc.cleanup_stale_outputs(
        level_dir,
        is_stale=lambda entry: bool(set(entry.child_dirs) & incomplete_strs),
    )
    groups = build_groups(output_dir, group_level)
    jobs = []
    n_skipped_incomplete = 0
    suffix = fc.output_suffix(gzip_output)

    for (parent_dir, group_key), child_dirs in groups.items():
        bad_dirs = [d for d in child_dirs if d in incomplete_dirs]
        if bad_dirs:
            n_skipped_incomplete += len(bad_dirs)
            continue

        source_paths = [
            level_1k_dir / f"{group_output_name(parent_dir, d.name, GROUP_LEVELS['1K'])}{suffix}"
            for d in sorted(child_dirs)
        ]
        output_name = group_output_name(parent_dir, group_key, group_level)
        jobs.append(
            partial(
                fc.merge_jsonl_files,
                source_paths,
                level_dir,
                output_name,
                gzip_output=gzip_output,
                force=force,
                extra_manifest_fields={"child_dirs": [str(d) for d in child_dirs]},
            )
        )

    fc.run_and_collect_manifest(jobs, level_dir, max_workers)

    if n_skipped_incomplete:
        logger.info(
            "Skipped %d incomplete director(y/ies) at %s level",
            n_skipped_incomplete,
            group_level.name,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    settings = Settings()  # parses sys.argv (and env vars) on construction
    group_level = GROUP_LEVELS[settings.group_level]

    target_dataset = settings.target_dataset
    target_dir = BASE_DIR[target_dataset] / f"{target_dataset}_ids"

    output_dir = settings.output_dir
    output_dir.mkdir(exist_ok=True, parents=True)

    redo_file = NCBI_BASE_DIR / f"redo-{target_dataset}.txt"
    jsonl_dir = BASE_DIR[target_dataset] / "annotation_report_jsonl"

    fc.cleanup_orphaned_tmp_files(jsonl_dir)

    if settings.verify:
        fc.verify_level(jsonl_dir / "1K", gzip_output=settings.gzip)
        if group_level.name != "1K":
            fc.verify_level(jsonl_dir / group_level.name, gzip_output=settings.gzip)
        return

    incomplete_dirs = find_redo_ids_and_incomplete_dirs(
        target_dir=target_dir,
        output_dir=output_dir,
        redo_file=redo_file,
    )

    level_1k_dir = build_1k_level(
        output_dir=output_dir,
        jsonl_dir=jsonl_dir,
        incomplete_dirs=incomplete_dirs,
        max_workers=settings.threads,
        force=settings.force,
        gzip_output=settings.gzip,
    )

    if group_level.name != "1K":
        merge_coarser_level(
            output_dir=output_dir,
            jsonl_dir=jsonl_dir,
            level_1k_dir=level_1k_dir,
            group_level=group_level,
            incomplete_dirs=incomplete_dirs,
            max_workers=settings.threads,
            force=settings.force,
            gzip_output=settings.gzip,
        )

    if settings.s3_prefix:
        level_dir = jsonl_dir / group_level.name
        fc.upload_manifest_outputs_to_s3(level_dir, settings.s3_prefix, force=settings.force)


if __name__ == "__main__":
    main()
