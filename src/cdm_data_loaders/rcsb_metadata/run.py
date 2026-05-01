"""Orchestrate the RCSB metadata download and Lakehouse upload pipeline.

For each entity type (entries, validation, taxonomy, ligands, citations, pfam,
sequence_clusters), the pipeline:

1. Fetches the full list of released PDB entry IDs from the RCSB holdings API.
2. Batches the IDs and queries the RCSB GraphQL endpoint.
3. Streams results to a local NDJSON temp file (one JSON object per line).
4. Calls :func:`~cdm_data_loaders.utils.s3_versioned_upload.versioned_upload`
   to upload the file — archiving the previous version only when content
   has actually changed.

Typical usage::

    from cdm_data_loaders.rcsb_metadata.settings import RcsbMetadataSettings
    from cdm_data_loaders.rcsb_metadata.run import run_rcsb_metadata

    settings = RcsbMetadataSettings(lakehouse_bucket="cdm-lake")
    result = run_rcsb_metadata(settings)
    for entity, summary in result.entity_results.items():
        print(entity, summary)
"""

import json
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import tqdm

from cdm_data_loaders.rcsb_metadata.fetch import fetch_entity, fetch_entry_ids
from cdm_data_loaders.rcsb_metadata.metadata import (
    DescriptorResource,
    archive_descriptor,
    create_descriptor,
    upload_descriptor,
    validate_descriptor,
)
from cdm_data_loaders.rcsb_metadata.queries import ENTITY_TYPES
from cdm_data_loaders.rcsb_metadata.settings import (
    RCSB_ARCHIVE_PREFIX,
    RCSB_RAW_DATA_PREFIX,
    RcsbMetadataSettings,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.download.graphql_client import GraphQLClient
from cdm_data_loaders.utils.s3_versioned_upload import UploadResult, versioned_upload

logger = get_cdm_logger()


@dataclass
class EntityResult:
    """Result for a single RCSB entity type.

    :param entity_type: entity name, e.g. ``"entries"``
    :param upload_status: ``"new"``, ``"archived_and_replaced"``, ``"unchanged"``, ``"dry_run"``, or ``"error"``
    :param records_written: number of NDJSON records written to disk
    :param dest_path: S3 destination path
    :param archive_key: S3 key of the archived old version, or ``None``
    :param local_md5: hex MD5 of the uploaded file, or ``None``
    :param local_bytes: byte size of the uploaded file, or ``None``
    :param error: error message if ``upload_status == "error"``
    """

    entity_type: str
    upload_status: str
    records_written: int
    dest_path: str
    archive_key: str | None
    local_md5: str | None = None
    local_bytes: int | None = None
    error: str | None = None


@dataclass
class RcsbMetadataResult:
    """Result of a full RCSB metadata pipeline run.

    :param entity_results: per-entity :class:`EntityResult` objects
    :param total_entries: total number of entry IDs retrieved
    :param dry_run: True if this was a dry run
    :param descriptor_key: S3 key of the written descriptor, or ``None`` if not written
    """

    entity_results: dict[str, EntityResult] = field(default_factory=dict)
    total_entries: int = 0
    dry_run: bool = False
    descriptor_key: str | None = None

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return {
            "total_entries": self.total_entries,
            "dry_run": self.dry_run,
            "descriptor_key": self.descriptor_key,
            "entities": {k: asdict(v) for k, v in self.entity_results.items()},
        }


def _write_ndjson(entity_type: str, pdb_ids: list[str], dest: Path, settings: RcsbMetadataSettings) -> int:
    """Fetch *entity_type* for all *pdb_ids* and write to *dest* as NDJSON.

    :param entity_type: RCSB entity type name
    :param pdb_ids: list of all entry IDs to fetch
    :param dest: local output path
    :param settings: pipeline settings (URL, batch size)
    :return: number of records written
    """
    count = 0
    with dest.open("w") as f, GraphQLClient() as gql:
        for record in tqdm.tqdm(
            fetch_entity(
                entity_type,
                pdb_ids,
                gql_client=gql,
                graphql_url=settings.rcsb_graphql_url,
                batch_size=settings.rcsb_batch_size,
            ),
            desc=entity_type,
            unit="entry",
            smoothing=0.01,
            total=len(pdb_ids),
        ):
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += 1
    return count


def run_rcsb_metadata(settings: RcsbMetadataSettings) -> RcsbMetadataResult:
    """Run the full RCSB metadata pipeline for all entity types.

    :param settings: pipeline configuration
    :return: :class:`RcsbMetadataResult` with per-entity outcomes
    """
    logger.debug("RCSB metadata pipeline starting (dry_run=%s)", settings.dry_run)

    if settings.dry_run:
        logger.debug(
            "[dry-run] would fetch %d entity types and upload to s3://%s", len(ENTITY_TYPES), settings.lakehouse_bucket
        )
        result = RcsbMetadataResult(dry_run=True)
        for entity_type in ENTITY_TYPES:
            filename = f"{entity_type}.ndjson"
            dest_key = f"{settings.lakehouse_key_prefix.strip('/')}/{RCSB_RAW_DATA_PREFIX.strip('/')}/{filename}"
            result.entity_results[entity_type] = EntityResult(
                entity_type=entity_type,
                upload_status="dry_run",
                records_written=0,
                dest_path=f"{settings.lakehouse_bucket}/{dest_key}",
                archive_key=None,
            )
        return result

    pdb_ids = fetch_entry_ids(url=settings.rcsb_entry_ids_url)
    if settings.limit is not None:
        pdb_ids = pdb_ids[: settings.limit]
    result = RcsbMetadataResult(total_entries=len(pdb_ids), dry_run=False)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for entity_type in ENTITY_TYPES:
            filename = f"{entity_type}.ndjson"
            dest_key = f"{settings.lakehouse_key_prefix.strip('/')}/{RCSB_RAW_DATA_PREFIX.strip('/')}/{filename}"
            dest_path = f"{settings.lakehouse_bucket}/{dest_key}"
            archive_base = f"{settings.lakehouse_bucket}/{settings.lakehouse_key_prefix.strip('/')}/{RCSB_ARCHIVE_PREFIX.strip('/')}"
            sub_path = f"rcsb/raw_data/{filename}"

            logger.debug("Processing entity type: %s", entity_type)
            local_path = tmp_dir / filename

            try:
                records = _write_ndjson(entity_type, pdb_ids, local_path, settings)
                upload_result: UploadResult = versioned_upload(
                    local_path=local_path,
                    s3_dest_path=dest_path,
                    archive_base_path=archive_base,
                    sub_path=sub_path,
                )
                result.entity_results[entity_type] = EntityResult(
                    entity_type=entity_type,
                    upload_status=upload_result.status,
                    records_written=records,
                    dest_path=upload_result.dest_path,
                    archive_key=upload_result.archive_key,
                    local_md5=upload_result.local_md5,
                    local_bytes=upload_result.local_bytes,
                )
                logger.debug(
                    "Entity %s: %d records, status=%s",
                    entity_type,
                    records,
                    upload_result.status,
                )
            except Exception as exc:
                logger.exception("Failed to process entity type %s", entity_type)
                result.entity_results[entity_type] = EntityResult(
                    entity_type=entity_type,
                    upload_status="error",
                    records_written=0,
                    dest_path=dest_path,
                    archive_key=None,
                    error=str(exc),
                )

    logger.debug("RCSB metadata pipeline complete: %d entity types processed", len(result.entity_results))

    # Write descriptor if any file was new or changed
    _write_rcsb_descriptor(result, settings)

    return result


def _write_rcsb_descriptor(result: RcsbMetadataResult, settings: RcsbMetadataSettings) -> None:
    """Build and upload the RCSB metadata frictionless descriptor.

    Archives the previous descriptor first if any entity file changed.
    Skips writing if all entities were unchanged or errored (no new data).
    """
    from datetime import UTC, datetime  # noqa: PLC0415

    changed_statuses = {"new", "archived_and_replaced"}
    any_changed = any(er.upload_status in changed_statuses for er in result.entity_results.values())
    any_uploaded = any(er.upload_status in changed_statuses | {"unchanged"} for er in result.entity_results.values())
    if not any_uploaded:
        logger.debug("No successful uploads — skipping descriptor write")
        return

    date_tag = datetime.now(UTC).strftime("%Y-%m-%d")

    # Archive old descriptor if any data file changed
    if any_changed:
        archive_descriptor(
            bucket=settings.lakehouse_bucket,
            key_prefix=settings.lakehouse_key_prefix,
            date_tag=date_tag,
        )

    # Build resource list from successfully uploaded entity results
    resources: list[DescriptorResource] = []
    entity_types_uploaded: list[str] = []
    for entity_type, er in result.entity_results.items():
        if er.upload_status in changed_statuses | {"unchanged"}:
            resources.append(
                DescriptorResource(
                    name=f"{entity_type}.ndjson",
                    path=er.dest_path,
                    format="ndjson",
                    bytes=er.local_bytes,
                    hash=f"md5:{er.local_md5}" if er.local_md5 else None,
                )
            )
            entity_types_uploaded.append(entity_type)

    descriptor = create_descriptor(entity_types_uploaded, resources)
    validate_descriptor(descriptor)
    descriptor_key = upload_descriptor(
        descriptor,
        bucket=settings.lakehouse_bucket,
        key_prefix=settings.lakehouse_key_prefix,
    )
    result.descriptor_key = descriptor_key
    logger.debug("RCSB metadata descriptor written: %s", descriptor_key)
