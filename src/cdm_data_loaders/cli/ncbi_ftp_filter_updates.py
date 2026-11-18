"""CLI interface for filtering NCBI FTP assembly modification dates into batched ID lists."""

from datetime import date
from logging import Logger, getLogger
from pathlib import Path
from typing import Final

from pydantic import Field, FilePath, PositiveInt, field_validator
from pydantic_settings import CliImplicitFlag, SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.datasets.ncbi_ftp.constants import Source
from cdm_data_loaders.datasets.ncbi_ftp.updates import (
    BATCH_SIZE,
    Splits,
    build_splits,
    compute_batch_width,
    filter_to_interesting,
    load_ftp_mod_times,
    load_interesting_assembly_ids,
    save_tsv,
    write_id_batches,
)

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "filter_ncbi_updates"


class FilterNcbiUpdatesSettings(LoggerSettings):
    """Settings for generating batched NCBI assembly ID lists filtered by modification date."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
    )

    ftp_mod_times_path: FilePath = Field(
        default=Path("ftp_mod_times.txt"),
        description=(
            "NCBI FTP directory update times TSV (no header: assembly_dir<TAB>modification_date) "
            "from https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/ftp_mod_times.txt.gz"
        ),
    )
    assemblies_of_interest_path: FilePath = Field(
        default=Path("assemblies.txt"),
        description="Single-column file of assembly IDs (no header)",
    )
    mod_date: date = Field(
        description="Modification date threshold in YYYY/MM/DD or YYYY-MM-DD format",
    )
    output_dir: Path = Field(
        description="Directory to write derived TSV tables and batched ID files",
    )
    filter_refseq: CliImplicitFlag[bool] = Field(
        default=False,
        description=(
            "Filter RefSeq assembly IDs against the assembly list in addition to GenBank IDs "
            "(by default all RefSeq IDs are retained)"
        ),
    )
    batch_size: PositiveInt = Field(
        default=BATCH_SIZE,
        description="Number of assembly IDs per output batch file",
    )

    @field_validator("mod_date", mode="before")
    @classmethod
    def _parse_mod_date(cls, value: date | str) -> date:
        """Parse ``YYYY/MM/DD`` or ISO ``YYYY-MM-DD`` date strings into :class:`datetime.date`."""
        if isinstance(value, date):
            return value
        return date.fromisoformat(value.replace("/", "-"))

    @property
    def derived_dir(self) -> Path:
        """Return subdirectory for derived TSV tables."""
        return self.output_dir / "derived"

    @property
    def mod_date_tag(self) -> str:
        """Return compact ``YYYYMMDD`` modification date string."""
        return self.mod_date.strftime("%Y%m%d")

    def source_dir(self, source: Source) -> Path:
        """Return output subdirectory for ``source``.

        :param source: assembly database source
        :type source: Source
        :return: source subdirectory path
        :rtype: Path
        """
        return self.output_dir / source.value

    def ensure_dirs(self) -> None:
        """Create ``derived/`` and per-source output subdirectories."""
        self.derived_dir.mkdir(parents=True, exist_ok=True)
        for source in Source:
            self.source_dir(source).mkdir(parents=True, exist_ok=True)


def save_split_tables(splits: Splits, settings: FilterNcbiUpdatesSettings) -> None:
    """Write each ``(Source, Period)`` DataFrame partition to ``settings.derived_dir``.

    :param splits: ``(Source, Period)`` DataFrame splits
    :type splits: Splits
    :param settings: filter settings
    :type settings: FilterNcbiUpdatesSettings
    """
    for (source, period), df in splits.items():
        path = settings.derived_dir / f"{source.value}_{period.value}_{settings.mod_date_tag}.tsv"
        save_tsv(df, path)


def write_batched_ids(splits: Splits, settings: FilterNcbiUpdatesSettings) -> None:
    """Write numbered batch ID files for each ``(Source, Period)`` partition.

    :param splits: ``(Source, Period)`` DataFrame splits
    :type splits: Splits
    :param settings: filter settings
    :type settings: FilterNcbiUpdatesSettings
    """
    width = compute_batch_width(splits, settings.batch_size)
    for (source, period), df in splits.items():
        ids = df["assembly_id"].tolist()
        prefix = f"{period.value}_{settings.mod_date_tag}"
        n_written = write_id_batches(ids, settings.source_dir(source), prefix, settings.batch_size, width)
        logger.info("Wrote %d batch file(s) for %s/%s", n_written, source.value, period.value)


def run_filter_ncbi_updates(settings: FilterNcbiUpdatesSettings) -> None:
    """Parse modification times, filter assemblies of interest, and emit split TSVs and ID batches.

    :param settings: validated CLI settings
    :type settings: FilterNcbiUpdatesSettings
    """
    settings.ensure_dirs()

    logger.info("Parsing %s", settings.ftp_mod_times_path)
    parsed_df = load_ftp_mod_times(settings.ftp_mod_times_path)
    save_tsv(parsed_df, settings.derived_dir / "ftp_mod_times_parsed.tsv")

    logger.info("Reading %s", settings.assemblies_of_interest_path)
    interesting_ids = load_interesting_assembly_ids(settings.assemblies_of_interest_path)
    logger.info("Found %d unique assembly IDs", len(interesting_ids))

    mode = "filtering RefSeq and GenBank" if settings.filter_refseq else "keeping all RefSeq, filtering GenBank"
    logger.info("NCBI filtering mode: %s", mode)

    interesting_df = filter_to_interesting(parsed_df, interesting_ids, filter_refseq=settings.filter_refseq)
    save_tsv(interesting_df, settings.derived_dir / "interesting_assemblies.tsv")

    splits = build_splits(interesting_df, settings.mod_date)
    save_split_tables(splits, settings)
    write_batched_ids(splits, settings)
    logger.info("Completed NCBI update filtering")


cli = CliRunner(FilterNcbiUpdatesSettings, run_filter_ncbi_updates)


if __name__ == "__main__":
    cli()
