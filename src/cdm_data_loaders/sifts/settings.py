"""Pydantic settings for the SIFTS download pipeline."""

from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
SIFTS_RAW_DATA_PREFIX = "metadata/sifts/raw_data"
SIFTS_METADATA_PREFIX = "metadata/sifts/metadata"
SIFTS_ARCHIVE_PREFIX = "metadata/archive"


class SiftsSettings(BaseSettings):
    """Configuration for the SIFTS download pipeline.

    All fields can be supplied as environment variables (case-insensitive) or
    passed explicitly.  ``LAKEHOUSE_BUCKET`` is the only required field —
    everything else has a sensible default for production.

    Example environment variables::

        LAKEHOUSE_BUCKET=cdm-lake
        LAKEHOUSE_KEY_PREFIX=tenant-general-warehouse/kbase/datasets/pdb
        SIFTS_FTP_HOST=ftp.ebi.ac.uk
        SIFTS_FILES=pdb_chain_uniprot.tsv.gz,pdb_chain_go.tsv.gz
        DRY_RUN=false
    """

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False, extra="ignore")

    lakehouse_bucket: str
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX
    sifts_ftp_host: str = "ftp.ebi.ac.uk"
    sifts_files: list[str] | None = None
    dry_run: bool = False
