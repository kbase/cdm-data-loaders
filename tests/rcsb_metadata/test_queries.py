"""Tests for rcsb_metadata.queries."""

import pytest

from cdm_data_loaders.rcsb_metadata.queries import (
    ENTITY_QUERIES,
    ENTITY_TYPES,
    get_query,
)

EXPECTED_ENTITY_TYPES = [
    "entries",
    "validation",
    "taxonomy",
    "ligands",
    "citations",
    "pfam",
    "sequence_clusters",
]


class TestEntityTypeList:
    def test_all_expected_types_present(self):
        assert set(EXPECTED_ENTITY_TYPES) == set(ENTITY_TYPES)

    def test_count(self):
        assert len(ENTITY_TYPES) == 7

    def test_no_duplicates(self):
        assert len(ENTITY_TYPES) == len(set(ENTITY_TYPES))


class TestEntityQueries:
    @pytest.mark.parametrize("entity_type", EXPECTED_ENTITY_TYPES)
    def test_query_string_present(self, entity_type):
        assert entity_type in ENTITY_QUERIES

    @pytest.mark.parametrize("entity_type", EXPECTED_ENTITY_TYPES)
    def test_query_contains_ids_variable(self, entity_type):
        assert "$ids" in ENTITY_QUERIES[entity_type]

    @pytest.mark.parametrize("entity_type", EXPECTED_ENTITY_TYPES)
    def test_query_is_nonempty_string(self, entity_type):
        q = ENTITY_QUERIES[entity_type]
        assert isinstance(q, str)
        assert len(q.strip()) > 20  # noqa: PLR2004

    def test_entries_query_has_rcsb_id(self):
        assert "rcsb_id" in ENTITY_QUERIES["entries"]

    def test_validation_query_has_clashscore(self):
        assert "clashscore" in ENTITY_QUERIES["validation"]

    def test_pfam_query_has_accession(self):
        assert "rcsb_pfam_accession" in ENTITY_QUERIES["pfam"]


class TestGetQuery:
    def test_returns_correct_query(self):
        q = get_query("entries")
        assert q == ENTITY_QUERIES["entries"]

    def test_raises_on_unknown_type(self):
        with pytest.raises(KeyError, match="Unknown RCSB entity type"):
            get_query("nonexistent_type")
