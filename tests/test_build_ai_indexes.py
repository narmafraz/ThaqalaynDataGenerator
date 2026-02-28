"""Tests for build_ai_indexes.py — topics and phrases index builders."""

import json
import os
import tempfile

import pytest

from app.build_ai_indexes import (
    _build_l2_to_l1_map,
    _extract_verses,
    _normalize_arabic,
    build_phrases_index,
    build_topics_index,
)


@pytest.fixture
def tmp_dest(tmp_path):
    """Create a temp destination directory with books/ and index/ subdirs."""
    books_dir = tmp_path / "books" / "al-kafi" / "1" / "1" / "1"
    books_dir.mkdir(parents=True)
    index_dir = tmp_path / "index"
    index_dir.mkdir()
    return tmp_path


@pytest.fixture
def verse_with_topics():
    """A verse_list JSON document with AI topics."""
    return {
        "index": "al-kafi:1:1:1",
        "kind": "verse_list",
        "data": {
            "verses": [
                {
                    "path": "/books/al-kafi:1:1:1:1",
                    "text": ["test"],
                    "ai": {
                        "topics": ["tawhid", "divine_attributes"],
                        "content_type": "creedal",
                    },
                },
                {
                    "path": "/books/al-kafi:1:1:1:2",
                    "text": ["test2"],
                    "ai": {
                        "topics": ["patience"],
                        "content_type": "ethical_teaching",
                    },
                },
                {
                    "path": "/books/al-kafi:1:1:1:3",
                    "text": ["no ai"],
                },
            ],
        },
    }


@pytest.fixture
def verse_with_phrases():
    """A verse_list JSON document with AI key_phrases."""
    return {
        "index": "al-kafi:1:1:1",
        "kind": "verse_list",
        "data": {
            "verses": [
                {
                    "path": "/books/al-kafi:1:1:1:1",
                    "text": ["test"],
                    "ai": {
                        "key_phrases": [
                            {
                                "phrase_ar": "بِسْمِ اللَّهِ",
                                "phrase_en": "In the name of Allah",
                                "category": "quranic_echo",
                            },
                        ],
                    },
                },
                {
                    "path": "/books/al-kafi:1:1:1:2",
                    "text": ["test2"],
                    "ai": {
                        "key_phrases": [
                            {
                                "phrase_ar": "بِسْمِ اللَّهِ",
                                "phrase_en": "In the name of Allah",
                                "category": "quranic_echo",
                            },
                            {
                                "phrase_ar": "الصِّرَاطُ الْمُسْتَقِيمُ",
                                "phrase_en": "The Straight Path",
                                "category": "quranic_echo",
                            },
                        ],
                    },
                },
            ],
        },
    }


class TestNormalizeArabic:
    def test_strips_diacritics(self):
        assert _normalize_arabic("بِسْمِ") == "بسم"

    def test_keeps_base_letters(self):
        assert _normalize_arabic("الله") == "الله"

    def test_empty_string(self):
        assert _normalize_arabic("") == ""


class TestBuildL2ToL1Map:
    def test_maps_subtopics(self):
        taxonomy = {
            "theology": {"topics": {"tawhid": {}, "divine_attributes": {}}},
            "ethics": {"topics": {"patience": {}}},
        }
        mapping = _build_l2_to_l1_map(taxonomy)
        assert mapping["tawhid"] == "theology"
        assert mapping["divine_attributes"] == "theology"
        assert mapping["patience"] == "ethics"


class TestExtractVerses:
    def test_verse_list(self, tmp_path):
        doc = {
            "kind": "verse_list",
            "data": {"verses": [{"path": "/books/test:1", "text": ["hello"]}]},
        }
        filepath = tmp_path / "test.json"
        filepath.write_text(json.dumps(doc), encoding="utf-8")
        verses = _extract_verses(str(filepath))
        assert len(verses) == 1
        assert verses[0]["path"] == "/books/test:1"

    def test_verse_detail(self, tmp_path):
        doc = {
            "kind": "verse_detail",
            "data": {"verse": {"path": "/books/test:1:1", "text": ["hi"]}},
        }
        filepath = tmp_path / "test.json"
        filepath.write_text(json.dumps(doc), encoding="utf-8")
        verses = _extract_verses(str(filepath))
        assert len(verses) == 1

    def test_chapter_list_returns_empty(self, tmp_path):
        doc = {"kind": "chapter_list", "data": {"chapters": []}}
        filepath = tmp_path / "test.json"
        filepath.write_text(json.dumps(doc), encoding="utf-8")
        assert _extract_verses(str(filepath)) == []

    def test_malformed_file_returns_empty(self, tmp_path):
        filepath = tmp_path / "bad.json"
        filepath.write_text("not json", encoding="utf-8")
        assert _extract_verses(str(filepath)) == []


class TestBuildTopicsIndex:
    def test_builds_index_from_verses(self, tmp_dest, verse_with_topics, monkeypatch):
        # Write test verse file
        verse_file = tmp_dest / "books" / "al-kafi" / "1" / "1" / "1.json"
        verse_file.parent.mkdir(parents=True, exist_ok=True)
        verse_file.write_text(json.dumps(verse_with_topics), encoding="utf-8")

        # Mock taxonomy
        monkeypatch.setattr(
            "app.build_ai_indexes._load_topic_taxonomy",
            lambda: {
                "theology": {"topics": {"tawhid": {}, "divine_attributes": {}}},
                "ethics": {"topics": {"patience": {}, "honesty": {}}},
            },
        )

        result = build_topics_index(str(tmp_dest))

        # Should have theology and ethics categories
        assert "theology" in result
        assert "ethics" in result
        # tawhid should have 1 hadith
        assert result["theology"]["tawhid"]["count"] == 1
        assert result["theology"]["tawhid"]["paths"] == ["/books/al-kafi:1:1:1:1"]
        # divine_attributes should have 1 hadith
        assert result["theology"]["divine_attributes"]["count"] == 1
        # patience should have 1 hadith
        assert result["ethics"]["patience"]["count"] == 1
        # honesty should NOT be in result (pruned — zero count)
        assert "honesty" not in result.get("ethics", {})

        # Verify file was written
        topics_file = tmp_dest / "index" / "topics.json"
        assert topics_file.exists()

    def test_empty_books_dir(self, tmp_dest, monkeypatch):
        monkeypatch.setattr("app.build_ai_indexes._load_topic_taxonomy", lambda: {})
        result = build_topics_index(str(tmp_dest))
        assert result == {}

    def test_deduplicates_paths(self, tmp_dest, monkeypatch):
        """Same verse appearing in verse_list and verse_detail should be counted once."""
        monkeypatch.setattr(
            "app.build_ai_indexes._load_topic_taxonomy",
            lambda: {"theology": {"topics": {"tawhid": {}}}},
        )
        # Write verse_list
        list_doc = {
            "kind": "verse_list",
            "data": {
                "verses": [{"path": "/books/test:1:1", "ai": {"topics": ["tawhid"]}}]
            },
        }
        list_file = tmp_dest / "books" / "al-kafi" / "1.json"
        list_file.write_text(json.dumps(list_doc), encoding="utf-8")

        # Write verse_detail with same path
        detail_doc = {
            "kind": "verse_detail",
            "data": {
                "verse": {"path": "/books/test:1:1", "ai": {"topics": ["tawhid"]}}
            },
        }
        detail_dir = tmp_dest / "books" / "al-kafi" / "1"
        detail_dir.mkdir(parents=True, exist_ok=True)
        detail_file = detail_dir / "1.json"
        detail_file.write_text(json.dumps(detail_doc), encoding="utf-8")

        result = build_topics_index(str(tmp_dest))
        assert result["theology"]["tawhid"]["count"] == 1


class TestBuildPhrasesIndex:
    def test_builds_index_from_verses(self, tmp_dest, verse_with_phrases):
        verse_file = tmp_dest / "books" / "al-kafi" / "1" / "1" / "1.json"
        verse_file.parent.mkdir(parents=True, exist_ok=True)
        verse_file.write_text(json.dumps(verse_with_phrases), encoding="utf-8")

        result = build_phrases_index(str(tmp_dest))

        # Should have 2 unique phrases (normalized keys)
        assert len(result) == 2

        # Check the "بسم الله" entry (normalized — no diacritics)
        bism_key = _normalize_arabic("بِسْمِ اللَّهِ")
        assert bism_key in result
        assert result[bism_key]["phrase_en"] == "In the name of Allah"
        assert result[bism_key]["category"] == "quranic_echo"
        assert len(result[bism_key]["paths"]) == 2  # appears in 2 verses

        # Check the "الصراط المستقيم" entry
        sirat_key = _normalize_arabic("الصِّرَاطُ الْمُسْتَقِيمُ")
        assert sirat_key in result
        assert len(result[sirat_key]["paths"]) == 1

        # Verify file was written
        phrases_file = tmp_dest / "index" / "phrases.json"
        assert phrases_file.exists()

    def test_empty_books_dir(self, tmp_dest):
        result = build_phrases_index(str(tmp_dest))
        assert result == {}

    def test_deduplicates_paths(self, tmp_dest):
        """Same phrase in same verse should not create duplicate path entries."""
        doc = {
            "kind": "verse_list",
            "data": {
                "verses": [
                    {
                        "path": "/books/test:1:1",
                        "ai": {
                            "key_phrases": [
                                {"phrase_ar": "test", "phrase_en": "test", "category": "other"},
                                {"phrase_ar": "test", "phrase_en": "test", "category": "other"},
                            ]
                        },
                    }
                ]
            },
        }
        verse_file = tmp_dest / "books" / "al-kafi" / "1.json"
        verse_file.write_text(json.dumps(doc), encoding="utf-8")

        result = build_phrases_index(str(tmp_dest))
        assert len(result["test"]["paths"]) == 1

    def test_preserves_original_diacritized_form(self, tmp_dest):
        """The phrase_ar field in the index should keep the original diacritics."""
        doc = {
            "kind": "verse_list",
            "data": {
                "verses": [
                    {
                        "path": "/books/test:1:1",
                        "ai": {
                            "key_phrases": [
                                {"phrase_ar": "بِسْمِ اللَّهِ", "phrase_en": "Bismillah", "category": "quranic_echo"},
                            ]
                        },
                    }
                ]
            },
        }
        verse_file = tmp_dest / "books" / "al-kafi" / "1.json"
        verse_file.write_text(json.dumps(doc), encoding="utf-8")

        result = build_phrases_index(str(tmp_dest))
        key = _normalize_arabic("بِسْمِ اللَّهِ")
        assert result[key]["phrase_ar"] == "بِسْمِ اللَّهِ"  # Original with diacritics
