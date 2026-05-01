"""Tests for rcsb_metadata.run."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import cdm_data_loaders.rcsb_metadata.run as run_mod
from cdm_data_loaders.rcsb_metadata.queries import ENTITY_TYPES
from cdm_data_loaders.rcsb_metadata.run import EntityResult, RcsbMetadataResult, run_rcsb_metadata
from cdm_data_loaders.rcsb_metadata.settings import RcsbMetadataSettings
from cdm_data_loaders.utils.s3_versioned_upload import UploadResult

TEST_BUCKET = "test-lake"
TEST_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb"
SAMPLE_IDS = ["4HHB", "1CBS", "2RH1"]


def _make_settings(**kwargs) -> RcsbMetadataSettings:
    defaults = {
        "lakehouse_bucket": TEST_BUCKET,
        "lakehouse_key_prefix": TEST_PREFIX,
    }
    defaults.update(kwargs)
    return RcsbMetadataSettings.model_construct(**defaults)


def _new_upload_result(entity_type: str) -> UploadResult:
    return UploadResult(
        status="new",
        archive_key=None,
        dest_path=f"{TEST_BUCKET}/{TEST_PREFIX}/derived_data/rcsb/raw_data/{entity_type}.ndjson",
        local_md5="deadbeef",
        local_bytes=1024,
    )


@pytest.fixture(autouse=True)
def _patch_descriptor_step(monkeypatch):
    """Suppress descriptor write in all run tests unless explicitly testing it."""
    monkeypatch.setattr(run_mod, "_write_rcsb_descriptor", lambda result, settings: None)


class TestRunRcsbMetadataDryRun:
    def test_returns_dry_run_result(self):
        settings = _make_settings(dry_run=True)
        result = run_rcsb_metadata(settings)
        assert result.dry_run is True

    def test_all_entity_types_present(self):
        settings = _make_settings(dry_run=True)
        result = run_rcsb_metadata(settings)
        assert set(result.entity_results.keys()) == set(ENTITY_TYPES)

    def test_all_entity_statuses_are_dry_run(self):
        settings = _make_settings(dry_run=True)
        result = run_rcsb_metadata(settings)
        for entity_type, er in result.entity_results.items():
            assert er.upload_status == "dry_run", entity_type

    def test_no_records_written_in_dry_run(self):
        settings = _make_settings(dry_run=True)
        result = run_rcsb_metadata(settings)
        for er in result.entity_results.values():
            assert er.records_written == 0

    def test_no_external_calls_in_dry_run(self):
        settings = _make_settings(dry_run=True)
        with patch.object(run_mod, "fetch_entry_ids") as mock_fetch:
            run_rcsb_metadata(settings)
        mock_fetch.assert_not_called()

    def test_dest_path_contains_bucket(self):
        settings = _make_settings(dry_run=True)
        result = run_rcsb_metadata(settings)
        for er in result.entity_results.values():
            assert TEST_BUCKET in er.dest_path


class TestRunRcsbMetadataLive:
    @pytest.fixture
    def patched_run(self, mock_s3_client):
        """Patch all external calls: fetch_entry_ids, _write_ndjson, versioned_upload."""
        upload_results = {et: _new_upload_result(et) for et in ENTITY_TYPES}

        with (
            patch.object(run_mod, "fetch_entry_ids", return_value=SAMPLE_IDS) as mock_ids,
            patch.object(run_mod, "_write_ndjson", return_value=len(SAMPLE_IDS)) as mock_write,
            patch.object(
                run_mod, "versioned_upload", side_effect=lambda **kw: upload_results[kw["local_path"].stem]
            ) as mock_upload,
        ):
            yield {
                "mock_ids": mock_ids,
                "mock_write": mock_write,
                "mock_upload": mock_upload,
                "upload_results": upload_results,
            }

    def test_returns_result(self, patched_run):
        settings = _make_settings()
        result = run_rcsb_metadata(settings)
        assert isinstance(result, RcsbMetadataResult)

    def test_total_entries_set(self, patched_run):
        settings = _make_settings()
        result = run_rcsb_metadata(settings)
        assert result.total_entries == len(SAMPLE_IDS)

    def test_all_entity_types_in_result(self, patched_run):
        settings = _make_settings()
        result = run_rcsb_metadata(settings)
        assert set(result.entity_results.keys()) == set(ENTITY_TYPES)

    def test_records_written_set(self, patched_run):
        settings = _make_settings()
        result = run_rcsb_metadata(settings)
        for er in result.entity_results.values():
            assert er.records_written == len(SAMPLE_IDS)

    def test_upload_status_from_versioned_upload(self, patched_run):
        settings = _make_settings()
        result = run_rcsb_metadata(settings)
        for er in result.entity_results.values():
            assert er.upload_status == "new"

    def test_fetch_entry_ids_called_once(self, patched_run):
        settings = _make_settings()
        run_rcsb_metadata(settings)
        patched_run["mock_ids"].assert_called_once()

    def test_write_ndjson_called_for_each_entity(self, patched_run):
        settings = _make_settings()
        run_rcsb_metadata(settings)
        assert patched_run["mock_write"].call_count == len(ENTITY_TYPES)

    def test_versioned_upload_called_for_each_entity(self, patched_run):
        settings = _make_settings()
        run_rcsb_metadata(settings)
        assert patched_run["mock_upload"].call_count == len(ENTITY_TYPES)

    def test_entity_error_captured(self, mock_s3_client):
        """If one entity fails, others still run and the error is captured."""
        fail_entity = ENTITY_TYPES[0]
        upload_results = {et: _new_upload_result(et) for et in ENTITY_TYPES}

        call_count = {"n": 0}

        def mock_write(entity_type, pdb_ids, dest, settings):
            call_count["n"] += 1
            if entity_type == fail_entity:
                raise RuntimeError("GraphQL quota exceeded")
            return len(pdb_ids)

        with (
            patch.object(run_mod, "fetch_entry_ids", return_value=SAMPLE_IDS),
            patch.object(run_mod, "_write_ndjson", side_effect=mock_write),
            patch.object(run_mod, "versioned_upload", side_effect=lambda **kw: upload_results[kw["local_path"].stem]),
        ):
            settings = _make_settings()
            result = run_rcsb_metadata(settings)

        assert result.entity_results[fail_entity].upload_status == "error"
        assert result.entity_results[fail_entity].error is not None
        # Other entities should still succeed
        ok_types = [et for et in ENTITY_TYPES if et != fail_entity]
        for et in ok_types:
            assert result.entity_results[et].upload_status == "new"


class TestRcsbMetadataResultToDict:
    def test_to_dict_structure(self):
        result = RcsbMetadataResult(
            total_entries=5,
            dry_run=False,
            entity_results={
                "entries": EntityResult(
                    entity_type="entries",
                    upload_status="new",
                    records_written=5,
                    dest_path="bucket/key.ndjson",
                    archive_key=None,
                )
            },
        )
        d = result.to_dict()
        assert d["total_entries"] == 5
        assert d["dry_run"] is False
        assert "entries" in d["entities"]
        assert d["entities"]["entries"]["upload_status"] == "new"


class TestRunRcsbMetadataLimit:
    """Tests for the limit setting that slices the entry ID list."""

    @pytest.fixture
    def patched_run_with_ids(self, mock_s3_client):
        ten_ids = [f"ID{i:04d}" for i in range(10)]
        upload_results = {et: _new_upload_result(et) for et in ENTITY_TYPES}
        with (
            patch.object(run_mod, "fetch_entry_ids", return_value=ten_ids) as mock_ids,
            patch.object(run_mod, "_write_ndjson", return_value=3) as mock_write,
            patch.object(run_mod, "versioned_upload", side_effect=lambda **kw: upload_results[kw["local_path"].stem]),
        ):
            yield mock_ids, mock_write, ten_ids

    def test_limit_slices_ids(self, patched_run_with_ids):
        _, mock_write, _ = patched_run_with_ids
        settings = _make_settings(limit=3)
        result = run_rcsb_metadata(settings)
        assert result.total_entries == 3  # noqa: PLR2004
        first_call_ids = mock_write.call_args_list[0][0][1]
        assert len(first_call_ids) == 3  # noqa: PLR2004

    def test_limit_none_uses_all_ids(self, patched_run_with_ids):
        _, mock_write, ten_ids = patched_run_with_ids
        settings = _make_settings(limit=None)
        result = run_rcsb_metadata(settings)
        assert result.total_entries == len(ten_ids)

    def test_limit_larger_than_list_uses_all(self, patched_run_with_ids):
        _, mock_write, ten_ids = patched_run_with_ids
        settings = _make_settings(limit=9999)
        result = run_rcsb_metadata(settings)
        assert result.total_entries == len(ten_ids)


class TestWriteNdjson:
    """Unit tests for the _write_ndjson helper."""

    def _settings(self) -> RcsbMetadataSettings:
        return RcsbMetadataSettings.model_construct(
            lakehouse_bucket="bucket",
            lakehouse_key_prefix="prefix",
            rcsb_graphql_url="https://data.rcsb.org/graphql",
            rcsb_batch_size=2,
            limit=None,
            dry_run=False,
        )

    def test_writes_one_line_per_record(self):
        records = [{"rcsb_id": "4HHB"}, {"rcsb_id": "1CBS"}]
        with (
            patch.object(run_mod, "fetch_entity", return_value=iter(records)),
            tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp,
        ):
            tmp_path = Path(tmp.name)
        try:
            count = run_mod._write_ndjson("entries", ["4HHB", "1CBS"], tmp_path, self._settings())
            lines = tmp_path.read_text().splitlines()
        finally:
            tmp_path.unlink(missing_ok=True)
        assert count == 2  # noqa: PLR2004
        assert len(lines) == 2  # noqa: PLR2004

    def test_each_line_is_valid_json(self):
        records = [{"rcsb_id": "4HHB", "x": 1}, {"rcsb_id": "1CBS", "x": 2}]
        with (
            patch.object(run_mod, "fetch_entity", return_value=iter(records)),
            tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp,
        ):
            tmp_path = Path(tmp.name)
        try:
            run_mod._write_ndjson("entries", ["4HHB", "1CBS"], tmp_path, self._settings())
            parsed = [json.loads(line) for line in tmp_path.read_text().splitlines()]
        finally:
            tmp_path.unlink(missing_ok=True)
        assert parsed[0]["rcsb_id"] == "4HHB"
        assert parsed[1]["rcsb_id"] == "1CBS"

    def test_returns_zero_for_empty_results(self):
        with (
            patch.object(run_mod, "fetch_entity", return_value=iter([])),
            tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp,
        ):
            tmp_path = Path(tmp.name)
        try:
            count = run_mod._write_ndjson("entries", [], tmp_path, self._settings())
        finally:
            tmp_path.unlink(missing_ok=True)
        assert count == 0

    def test_passes_batch_size_to_fetch_entity(self):
        with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            with patch.object(run_mod, "fetch_entity", return_value=iter([])) as mock_fetch:
                run_mod._write_ndjson("entries", ["4HHB"], tmp_path, self._settings())
            call_kwargs = mock_fetch.call_args[1]
            assert call_kwargs["batch_size"] == 2  # noqa: PLR2004
        finally:
            tmp_path.unlink(missing_ok=True)
