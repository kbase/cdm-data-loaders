"""Pydantic settings for the RCSB metadata pipeline."""

from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
RCSB_DERIVED_DATA_PREFIX = "derived_data/rcsb"
RCSB_ARCHIVE_PREFIX = "derived_data/archive"


class RcsbMetadataSettings(BaseSettings):
    """Configuration for the RCSB metadata download pipeline.

    Example environment variables::

        LAKEHOUSE_BUCKET=cdm-lake
        LAKEHOUSE_KEY_PREFIX=tenant-general-warehouse/kbase/datasets/pdb
        RCSB_GRAPHQL_URL=https://data.rcsb.org/graphql
        RCSB_BATCH_SIZE=1000
        DRY_RUN=false
    """

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False, extra="ignore")

    lakehouse_bucket: str
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX
    rcsb_graphql_url: str = "https://data.rcsb.org/graphql"
    rcsb_entry_ids_url: str = "https://data.rcsb.org/holdings/released/entry_ids"
    rcsb_batch_size: int = 1000
    dry_run: bool = False
