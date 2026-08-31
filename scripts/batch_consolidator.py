"""Generic directory-batching JSONL consolidation script.

Given a directory of individual .json files, groups them into
fixed-size batches (in sorted filename order) and consolidates each
batch into its own JSON Lines file, resumably and safely across crashes.
"""

import logging
from functools import partial
from pathlib import Path
from typing import Annotated, Literal

from pydantic import DirectoryPath, Field, field_validator, model_validator
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
    file_glob: str | None = Field(
        default=None,
        description="Optional shell-style glob pattern (fnmatch syntax: * ? "
        "[seq] [!seq]) to further restrict which files are consolidated, "
        "matched against the filename only. This is applied in addition "
        "to, not instead of, the recognized .json/.jsonl/.jsonl.gz suffix "
        "check -- e.g. 'DSCF*' selects any recognized input file whose "
        "name starts with 'DSCF', regardless of which of those suffixes "
        "it has.",
    )

    @field_validator("input_dir")
    @classmethod
    def _input_dir_must_exist(cls, v: Path) -> Path:
        if not v.is_dir():
            msg = f"input_dir does not exist or is not a directory: {v}"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _check_dirs_unrelated(self) -> "Settings":
        fc.check_dirs_unrelated({"input_dir": self.input_dir, "output_dir": self.output_dir})
        return self


def make_batches(files: list[Path], batch_size: int) -> list[list[Path]]:
    """Split a sorted file list into consecutive fixed-size batches."""
    return [files[i : i + batch_size] for i in range(0, len(files), batch_size)]


def batch_output_name(batch_index: int, n_batches: int, base_name: str = "batch_") -> str:
    """Zero-padded batch name, e.g. batch_00007."""
    width = max(5, len(str(n_batches)))
    return f"{base_name}{batch_index:0{width}d}"


def run_batch_consolidation(settings: Settings) -> None:
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    fc.cleanup_orphaned_tmp_files(settings.output_dir)

    files = fc.discover_input_files(settings.input_dir, settings.recursive)
    files = fc.filter_by_glob(files, settings.file_glob)

    if not files:
        logger.warning(
            "No matching input file(s) found under %s%s",
            settings.input_dir,
            f" (glob: {settings.file_glob!r})" if settings.file_glob else "",
        )
        return

    batches = make_batches(files, settings.batch_size)
    logger.info(
        "Found %d input file(s) under %s%s; split into %d batch(es) of up to %d",
        len(files),
        settings.input_dir,
        f" matching glob {settings.file_glob!r}" if settings.file_glob else "",
        len(batches),
        settings.batch_size,
    )

    jobs = []
    for batch_index, batch_files in enumerate(batches):
        output_name = batch_output_name(batch_index, len(batches), settings.base_name)
        jobs.append(
            partial(
                fc.consolidate_input_files,
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
