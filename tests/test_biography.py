"""Tests for the biography enrichment pipeline."""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from app.wikishia.biography import (
    enrich_narrator_json,
    enrich_narrator_index_with_transliterations,
    load_narrator_index,
    run_matching_pipeline,
)


class TestLoadNarratorIndex:
    """Test loading narrator index from ThaqalaynData."""

    @patch("app.wikishia.biography.load_json")
    def test_load_success(self, mock_load):
        mock_load.return_value = {
            "data": {
                "1": {"titles": {"ar": "محمد بن يعقوب"}, "narrations": 10},
                "2": {"titles": {"ar": "علي بن ابراهيم"}, "narrations": 5},
            }
        }
        result = load_narrator_index()
        assert len(result) == 2
        assert result[1] == "محمد بن يعقوب"
        assert result[2] == "علي بن ابراهيم"

    @patch("app.wikishia.biography.load_json")
    def test_load_empty(self, mock_load):
        mock_load.return_value = {"data": {}}
        result = load_narrator_index()
        assert result == {}

    @patch("app.wikishia.biography.load_json")
    def test_load_error(self, mock_load):
        mock_load.side_effect = Exception("File not found")
        result = load_narrator_index()
        assert result == {}

    @patch("app.wikishia.biography.load_json")
    def test_load_skips_missing_titles(self, mock_load):
        mock_load.return_value = {
            "data": {
                "1": {"titles": {"ar": "محمد"}, "narrations": 10},
                "2": {"narrations": 5},  # No titles
                "3": {"titles": {}, "narrations": 3},  # Empty titles
            }
        }
        result = load_narrator_index()
        assert len(result) == 1
        assert 1 in result


class TestEnrichNarratorJson:
    """Test adding biography data to narrator JSON files."""

    @patch("app.wikishia.biography.write_file")
    @patch("app.wikishia.biography.load_json")
    def test_add_transliteration(self, mock_load, mock_write):
        mock_load.return_value = {
            "index": 1,
            "kind": "person_content",
            "data": {
                "index": 1,
                "titles": {"ar": "محمد بن يعقوب"},
                "verse_paths": [],
            }
        }

        result = enrich_narrator_json(1, None, "Muhammad ibn Ya'qub")
        assert result is True

        # Verify write was called with transliteration added
        call_args = mock_write.call_args
        written_data = call_args[0][1]
        assert written_data["data"]["titles"]["en"] == "Muhammad ibn Ya'qub"

    @patch("app.wikishia.biography.write_file")
    @patch("app.wikishia.biography.load_json")
    def test_add_biography(self, mock_load, mock_write):
        mock_load.return_value = {
            "index": 1,
            "kind": "person_content",
            "data": {
                "index": 1,
                "titles": {"ar": "محمد"},
                "verse_paths": [],
            }
        }

        bio = {
            "birth_date": "250 AH",
            "death_date": "329 AH",
            "era": "Early Islamic",
            "reliability": "Thiqah (Trustworthy)",
            "teachers": ["Teacher 1"],
            "students": ["Student 1"],
            "biography_summary": "A great scholar.",
            "biography_source": "WikiShia",
            "wikishia_url": "https://en.wikishia.net/view/Test",
        }

        result = enrich_narrator_json(1, bio, None)
        assert result is True

        written_data = mock_write.call_args[0][1]
        assert written_data["data"]["birth_date"] == "250 AH"
        assert written_data["data"]["death_date"] == "329 AH"
        assert written_data["data"]["biography_summary"] == "A great scholar."

    @patch("app.wikishia.biography.write_file")
    @patch("app.wikishia.biography.load_json")
    def test_no_update_needed(self, mock_load, mock_write):
        """No writes when there's nothing to add."""
        mock_load.return_value = {
            "index": 1,
            "kind": "person_content",
            "data": {
                "index": 1,
                "titles": {"ar": "محمد", "en": "Muhammad"},
                "verse_paths": [],
            }
        }

        # English title already exists and no biography data
        result = enrich_narrator_json(1, None, "Muhammad")
        assert result is False
        mock_write.assert_not_called()

    @patch("app.wikishia.biography.load_json")
    def test_load_error(self, mock_load):
        mock_load.side_effect = Exception("not found")
        result = enrich_narrator_json(999, None, "Test")
        assert result is False


class TestEnrichNarratorIndex:
    """Test updating narrator index with transliterations."""

    @patch("app.wikishia.biography.write_file")
    @patch("app.wikishia.biography.load_json")
    def test_add_transliterations_to_index(self, mock_load, mock_write):
        mock_load.return_value = {
            "index": "people",
            "kind": "person_list",
            "data": {
                "1": {"titles": {"ar": "محمد"}, "narrations": 10},
                "2": {"titles": {"ar": "علي"}, "narrations": 5},
            }
        }

        transliterations = {1: "Muhammad", 2: "Ali"}
        enrich_narrator_index_with_transliterations(transliterations)

        written_data = mock_write.call_args[0][1]
        assert written_data["data"]["1"]["titles"]["en"] == "Muhammad"
        assert written_data["data"]["2"]["titles"]["en"] == "Ali"

    @patch("app.wikishia.biography.write_file")
    @patch("app.wikishia.biography.load_json")
    def test_preserves_existing_english(self, mock_load, mock_write):
        """Don't overwrite existing English titles."""
        mock_load.return_value = {
            "index": "people",
            "kind": "person_list",
            "data": {
                "1": {"titles": {"ar": "محمد", "en": "Existing Name"}, "narrations": 10},
            }
        }

        transliterations = {1: "Muhammad"}
        enrich_narrator_index_with_transliterations(transliterations)

        written_data = mock_write.call_args[0][1]
        assert written_data["data"]["1"]["titles"]["en"] == "Existing Name"


class TestRunMatchingPipeline:
    """Test the matching pipeline orchestration."""

    def test_basic_matching(self):
        """Test basic matching with exact and normalized matches."""
        narrator_names = {
            1: "محمد بن يعقوب",
            2: "مُحَمَّدُ بْنُ يَحْيَى",
        }
        wikishia_titles = ["محمد بن يعقوب", "محمد بن يحيى"]

        results = run_matching_pipeline(
            narrator_names,
            wikishia_titles=wikishia_titles,
            manual_mapping_path="/nonexistent/path.json",
        )

        assert len(results) == 2
        assert results[1].matched_title is not None
        assert results[2].matched_title is not None

    def test_no_wikishia_titles(self):
        """Pipeline works even with no WikiShia titles."""
        narrator_names = {1: "test name"}
        results = run_matching_pipeline(
            narrator_names,
            wikishia_titles=None,
            manual_mapping_path="/nonexistent/path.json",
        )
        assert len(results) == 1
        assert results[1].matched_title is None

    def test_empty_narrators(self):
        results = run_matching_pipeline(
            {},
            wikishia_titles=["test"],
            manual_mapping_path="/nonexistent/path.json",
        )
        assert results == {}
