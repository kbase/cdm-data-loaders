"""Generic directory-batching JSONL consolidation script.

Given a directory of individual .json files, groups them into
fixed-size batches (in sorted filename order) and consolidates each
batch into its own JSON Lines file, resumably and safely across crashes.
"""

import logging
from functools import partial
from pathlib import Path
from typing import Annotated, Literal

from pydantic import DirectoryPath, Field, field_validator
from pydantic_settings import CliImplicitFlag, SettingsConfigDict

import cdm_data_loaders.utils.file_consolidator as fc
from cdm_data_loaders.core.settings import LoggerSettings

logger = logging.getLogger(__name__)


class Settings(LoggerSettings):
    """Command-line settings for batch JSONL consolidation."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_prog_name="batch_consolidate",
        # bool fields get --flag/--no-flag
        cli_implicit_flags=True,
        env_prefix="BATCH_CONSOLIDATE_",
        extra="forbid",
    )

    input_dir: Annotated[
        DirectoryPath, Field(description="Directory containing the individual .json files to consolidate.")
    ]
    output_dir: Annotated[Path, Field(description="Directory to write the batched .jsonl (or .jsonl.gz) files to.")]
    batch_size: Annotated[
        Literal[10, 50, 100, 500, 1000, 5000],
        Field(description="Number of source files to consolidate per output jsonl file."),
    ]
    base_name: str = Field(
        default="batch_",
        description="Base name (prefix) for output batch files, e.g. 'batch_' produces 'batch_00001.jsonl'.",
    )
    gzip: CliImplicitFlag[bool] = Field(
        default=False,
        description="Gzip-compress output jsonl files (.jsonl.gz).",
    )
    recursive: CliImplicitFlag[bool] = Field(
        default=False,
        description="Search --input-dir recursively for .json files (default: top level only).",
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
        description="Instead of building files, re-validate existing output files against their manifest and exit.",
    )

    @field_validator("input_dir")
    @classmethod
    def _input_dir_must_exist(cls, v: Path) -> Path:
        if not v.is_dir():
            msg = f"input_dir does not exist or is not a directory: {v}"
            raise ValueError(msg)
        return v


def discover_json_files(input_dir: Path, recursive: bool) -> list[Path]:
    """Find and sort all .json files under input_dir."""
    pattern = "**/*.json" if recursive else "*.json"
    return sorted(input_dir.glob(pattern))


def make_batches(files: list[Path], batch_size: int) -> list[list[Path]]:
    """Split a sorted file list into consecutive fixed-size batches."""
    return [files[i : i + batch_size] for i in range(0, len(files), batch_size)]


def batch_output_name(batch_index: int, n_batches: int, base_name: str = "batch_") -> str:
    """Zero-padded batch name, e.g. batch_00007."""
    width = max(5, len(str(n_batches)))
    return f"{base_name}{batch_index:0{width}d}"


def run_batch_consolidation(settings: Settings) -> None:
    """Run the file consolidation itself.

    :param settings: settings object
    :type settings: Settings
    """
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    fc.cleanup_orphaned_tmp_files(settings.output_dir)

    files = discover_json_files(settings.input_dir, settings.recursive)
    if not files:
        logger.warning("No .json files found under %s", settings.input_dir)
        return

    batches = make_batches(files, settings.batch_size)
    logger.info(
        "Found %d .json file(s) under %s; split into %d batch(es) of up to %d",
        len(files),
        settings.input_dir,
        len(batches),
        settings.batch_size,
    )

    jobs = []
    for batch_index, batch_files in enumerate(batches):
        output_name = batch_output_name(batch_index, len(batches), settings.base_name)
        jobs.append(
            partial(
                fc.consolidate_json_files,
                batch_files,
                settings.output_dir,
                output_name,
                gzip_output=settings.gzip,
                force=settings.force,
                extra_manifest_fields={
                    "n_source_files": len(batch_files),
                    "first_file": str(batch_files[0]),
                    "last_file": str(batch_files[-1]),
                },
            )
        )

    fc.run_and_collect_manifest(jobs, settings.output_dir, settings.threads)


def cli() -> None:
    """Run the file consolidator application."""
    settings = Settings()  # pyright: ignore[reportCallIssue]

    if settings.verify:
        fc.verify_level(settings.output_dir, gzip_output=settings.gzip)
        return

    run_batch_consolidation(settings)


if __name__ == "__main__":
    cli()
