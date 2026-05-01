"""MinIO integration tests for the RCSB metadata pipeline.

Requires a running MinIO instance (see tests/integration/conftest.py).
Tests are automatically skipped when MinIO is not reachable.

Two test classes:

* :class:`TestRcsbMetadataIntegration` — uses VCR cassettes for GraphQL, real MinIO for S3
* :class:`TestRcsbMetadataExternalRequest` — hits real RCSB API (``external_request`` + ``slow_test``)
"""

import json
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import cdm_data_loaders.rcsb_metadata.run as rcsb_run_mod
from cdm_data_loaders.rcsb_metadata.queries import ENTITY_TYPES
from cdm_data_loaders.rcsb_metadata.run import run_rcsb_metadata
from cdm_data_loaders.rcsb_metadata.settings import (
    RCSB_ARCHIVE_PREFIX,
    RCSB_RAW_DATA_PREFIX,
    RcsbMetadataSettings,
)
from cdm_data_loaders.utils.download.graphql_client import GraphQLClient

SAMPLE_IDS = ["4HHB", "1CBS", "2RH1"]

pytestmark = pytest.mark.integration


def _settings(bucket: str, batch_size: int = 3, dry_run: bool = False) -> RcsbMetadataSettings:
    return RcsbMetadataSettings(
        lakehouse_bucket=bucket,
        rcsb_batch_size=batch_size,
        dry_run=dry_run,
    )


def _dest_key(entity_type: str, settings: RcsbMetadataSettings) -> str:
    prefix = settings.lakehouse_key_prefix.strip("/")
    return f"{prefix}/{RCSB_RAW_DATA_PREFIX.strip('/')}/{entity_type}.ndjson"


def _mock_gql_records(entity_type: str, ids: list[str]) -> list[dict[str, Any]]:
    """Return minimal synthetic records for *entity_type*."""
    return [{"rcsb_id": pdb_id, "entity_type": entity_type} for pdb_id in ids]


class TestRcsbMetadataIntegration:
    """Integration tests using mocked GraphQL + real MinIO."""

    @pytest.fixture
    def mock_gql(self):
        """Patch fetch_entry_ids and GraphQLClient.post_query with synthetic data."""

        def post_query_side_effect(url, query, variables=None):
            ids = (variables or {}).get("ids", [])
            # Guess entity type from query text
            entity_type = "entries"
            for et in ENTITY_TYPES:
                if et in query.lower():
                    entity_type = et
                    break
            return {"entries": _mock_gql_records(entity_type, ids)}

        with (
            patch.object(rcsb_run_mod, "fetch_entry_ids", return_value=SAMPLE_IDS),
            patch.object(GraphQLClient, "post_query", side_effect=post_query_side_effect),
        ):
            yield

    def test_full_run_uploads_all_entity_types(self, minio_s3_client, test_bucket, mock_gql):
        settings = _settings(test_bucket)
        result = run_rcsb_metadata(settings)

        assert set(result.entity_results.keys()) == set(ENTITY_TYPES)
        for entity_type, er in result.entity_results.items():
            assert er.upload_status == "new", f"{entity_type}: {er}"
        assert result.descriptor_key is not None
        assert "rcsb/metadata" in result.descriptor_key

    def test_files_present_in_minio(self, minio_s3_client, test_bucket, mock_gql):
        settings = _settings(test_bucket)
        run_rcsb_metadata(settings)

        for entity_type in ENTITY_TYPES:
            key = _dest_key(entity_type, settings)
            resp = minio_s3_client.get_object(Bucket=test_bucket, Key=key)
            lines = resp["Body"].read().decode().strip().splitlines()
            assert len(lines) == len(SAMPLE_IDS), f"{entity_type}: expected {len(SAMPLE_IDS)} lines"
            first = json.loads(lines[0])
            assert "rcsb_id" in first

    def test_identical_rerun_is_unchanged(self, minio_s3_client, test_bucket, mock_gql):
        settings = _settings(test_bucket)
        run_rcsb_metadata(settings)
        result2 = run_rcsb_metadata(settings)

        for entity_type, er in result2.entity_results.items():
            assert er.upload_status == "unchanged", f"{entity_type}: expected unchanged"

    def test_changed_content_archives_old_version(self, minio_s3_client, test_bucket):
        settings = _settings(test_bucket)
        small_ids = ["4HHB"]
        large_ids = ["4HHB", "1CBS"]

        def post_query_side_effect(url, query, variables=None):
            ids = (variables or {}).get("ids", [])
            return {"entries": _mock_gql_records("entries", ids)}

        with (
            patch.object(rcsb_run_mod, "fetch_entry_ids", return_value=small_ids),
            patch.object(GraphQLClient, "post_query", side_effect=post_query_side_effect),
        ):
            run_rcsb_metadata(settings)

        with (
            patch.object(rcsb_run_mod, "fetch_entry_ids", return_value=large_ids),
            patch.object(GraphQLClient, "post_query", side_effect=post_query_side_effect),
        ):
            result2 = run_rcsb_metadata(settings)

        for entity_type, er in result2.entity_results.items():
            assert er.upload_status == "archived_and_replaced", f"{entity_type}: {er}"
            assert er.archive_key is not None

    def test_total_entries_reflects_id_count(self, minio_s3_client, test_bucket, mock_gql):
        settings = _settings(test_bucket)
        result = run_rcsb_metadata(settings)
        assert result.total_entries == len(SAMPLE_IDS)

    def test_limit_restricts_entries_processed(self, minio_s3_client, test_bucket, mock_gql):
        """limit=1 should process only the first entry ID for each entity type."""
        settings = RcsbMetadataSettings(
            lakehouse_bucket=test_bucket,
            rcsb_batch_size=3,
            limit=1,
        )
        result = run_rcsb_metadata(settings)

        assert result.total_entries == 1
        for entity_type, er in result.entity_results.items():
            assert er.upload_status == "new", f"{entity_type}: {er}"
            assert er.records_written == 1, f"{entity_type}: expected 1 record, got {er.records_written}"

    def test_limit_files_have_correct_line_count(self, minio_s3_client, test_bucket, mock_gql):
        """NDJSON files in MinIO should contain exactly limit lines."""
        limit = 2
        settings = RcsbMetadataSettings(
            lakehouse_bucket=test_bucket,
            rcsb_batch_size=3,
            limit=limit,
        )
        run_rcsb_metadata(settings)

        for entity_type in ENTITY_TYPES:
            key = _dest_key(entity_type, settings)
            resp = minio_s3_client.get_object(Bucket=test_bucket, Key=key)
            lines = resp["Body"].read().decode().strip().splitlines()
            assert len(lines) == limit, f"{entity_type}: expected {limit} lines, got {len(lines)}"


@pytest.mark.external_request
@pytest.mark.slow_test
class TestRcsbMetadataRealApi:
    """Hit the real RCSB holdings and GraphQL APIs; upload results to MinIO.

    These tests exercise the full pipeline end-to-end with no mocking of
    external network calls.  They are marked ``slow_test`` so they can be
    excluded from fast CI runs (``-m "integration and not slow_test"``), but
    they are part of the normal ``integration`` tier and MUST NOT be skipped
    by default — skipping them is what allowed a broken holdings URL to go
    undetected.
    """

    def test_real_rcsb_pipeline(self, minio_s3_client, test_bucket):
        """Fetch 10 entries from the real RCSB holdings + GraphQL APIs and upload to MinIO."""
        settings = RcsbMetadataSettings(
            lakehouse_bucket=test_bucket,
            rcsb_batch_size=5,
            limit=10,
        )
        result = run_rcsb_metadata(settings)

        assert result.total_entries == 10  # noqa: PLR2004
        for entity_type, er in result.entity_results.items():
            assert er.upload_status in ("new", "archived_and_replaced", "unchanged"), f"{entity_type}: {er}"
            assert er.records_written > 0, f"{entity_type}: no records written"

    def test_real_rcsb_files_present_in_minio(self, minio_s3_client, test_bucket):
        """Verify NDJSON files for every entity type land in MinIO with correct content."""
        settings = RcsbMetadataSettings(
            lakehouse_bucket=test_bucket,
            rcsb_batch_size=5,
            limit=3,
        )
        run_rcsb_metadata(settings)

        for entity_type in ENTITY_TYPES:
            key = _dest_key(entity_type, settings)
            resp = minio_s3_client.get_object(Bucket=test_bucket, Key=key)
            lines = resp["Body"].read().decode().strip().splitlines()
            assert len(lines) == 3, f"{entity_type}: expected 3 lines, got {len(lines)}"
            first = json.loads(lines[0])
            assert "rcsb_id" in first, f"{entity_type}: first record missing rcsb_id"
