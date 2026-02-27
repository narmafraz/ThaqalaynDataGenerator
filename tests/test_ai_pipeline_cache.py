"""Tests for ai_pipeline_cache module.

Tests cover: hashing, path helpers, save/load round-trips, metadata,
staleness detection, and invalidation.
"""

import json
import os

import pytest

from app.ai_pipeline import PIPELINE_VERSION, VALID_LANGUAGE_KEYS, PipelineRequest
from app.ai_pipeline_cache import (
    CACHE_FORMAT_VERSION,
    STRUCTURE_SCHEMA_VERSION,
    CacheStaleness,
    _cache_dir,
    _hash_glossary,
    _hash_text,
    _verse_id_from_path,
    check_cache_staleness,
    get_cached_or_plan,
    invalidate_cache,
    invalidate_chunks,
    load_cache_metadata,
    load_chunk_cache,
    load_structure_cache,
    save_chunk_cache,
    save_structure_cache,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_request(verse_path="/books/al-kafi:1:1:1:1",
                  arabic_text="\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650 \u0627\u0644\u0631\u0651\u064e\u062d\u0652\u0645\u064e\u0646\u0650"):
    return PipelineRequest(
        verse_path=verse_path,
        arabic_text=arabic_text,
        english_text="In the name of God",
        book_name="al-kafi",
        chapter_title="The Book of Intellect",
        hadith_number=1,
    )


def _make_structure_result(chunk_count=3):
    """Create a minimal structure result dict."""
    chunks = []
    for i in range(chunk_count):
        chunks.append({
            "chunk_type": "body",
            "arabic_text": "\u0648\u064e \u0642\u064e\u0627\u0644\u064e",
            "word_start": i * 10,
            "word_end": (i + 1) * 10,
            "translations": {},
        })
    return {
        "diacritized_text": "\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650",
        "diacritics_status": "validated",
        "diacritics_changes": [],
        "word_analysis": [],
        "tags": ["theology"],
        "content_type": "theological",
        "related_quran": [],
        "isnad_matn": {"has_chain": False, "isnad_ar": "", "matn_ar": "", "narrators": []},
        "translations": {"en": {"text": "test", "summary": "t", "key_terms": [], "seo_question": "?"}},
        "chunks": chunks,
    }


def _make_chunk_detail():
    """Create a minimal chunk detail dict."""
    return {
        "word_analysis": [
            {"word": "\u0648\u064e", "translation": {"en": "and"}, "pos": "CONJ"},
            {"word": "\u0642\u064e\u0627\u0644\u064e", "translation": {"en": "he said"}, "pos": "V"},
        ],
        "translations": {"en": "And he said"},
    }


DUMMY_GLOSSARY = {"terms": [{"ar": "\u0627\u0644\u0644\u0647", "en": "God"}]}


# ---------------------------------------------------------------------------
# Hashing tests
# ---------------------------------------------------------------------------

class TestHashing:
    def test_hash_text_deterministic(self):
        assert _hash_text("hello") == _hash_text("hello")

    def test_hash_text_different_inputs(self):
        assert _hash_text("hello") != _hash_text("world")

    def test_hash_glossary_deterministic(self):
        g = {"terms": [{"ar": "x", "en": "y"}]}
        assert _hash_glossary(g) == _hash_glossary(g)


# ---------------------------------------------------------------------------
# Path helper tests
# ---------------------------------------------------------------------------

class TestPathHelpers:
    def test_verse_id_from_path_kafi(self):
        assert _verse_id_from_path("/books/al-kafi:1:1:1:1") == "al-kafi_1_1_1_1"

    def test_verse_id_from_path_quran(self):
        assert _verse_id_from_path("/books/quran:1:1") == "quran_1_1"

    def test_cache_dir_uses_base(self, tmp_path):
        result = _cache_dir("/books/quran:2:255", str(tmp_path))
        assert result == os.path.join(str(tmp_path), "quran_2_255")


# ---------------------------------------------------------------------------
# Save/Load round-trip tests
# ---------------------------------------------------------------------------

class TestSaveLoad:
    def test_save_load_structure_roundtrip(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result()
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        loaded = load_structure_cache(request.verse_path, str(tmp_path))
        assert loaded == structure

    def test_save_load_chunk_roundtrip(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result()
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        detail = _make_chunk_detail()
        save_chunk_cache(request.verse_path, 0, detail, str(tmp_path))
        loaded = load_chunk_cache(request.verse_path, 0, str(tmp_path))
        assert loaded == detail

    def test_load_nonexistent_returns_none(self, tmp_path):
        assert load_structure_cache("/books/quran:99:99", str(tmp_path)) is None
        assert load_chunk_cache("/books/quran:99:99", 0, str(tmp_path)) is None
        assert load_cache_metadata("/books/quran:99:99", str(tmp_path)) is None

    def test_save_creates_directories(self, tmp_path):
        deep_base = os.path.join(str(tmp_path), "a", "b", "c")
        request = _make_request()
        structure = _make_structure_result()
        path = save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=deep_base,
        )
        assert os.path.exists(path)
        assert os.path.exists(os.path.join(path, "structure.json"))


# ---------------------------------------------------------------------------
# Metadata tests
# ---------------------------------------------------------------------------

class TestMetadata:
    def test_metadata_saved_correctly(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=5)
        save_structure_cache(
            request.verse_path, request, structure, "opus-test",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        meta = load_cache_metadata(request.verse_path, str(tmp_path))
        assert meta["verse_path"] == request.verse_path
        assert meta["model"] == "opus-test"
        assert meta["chunk_count"] == 5
        assert meta["pipeline_version"] == PIPELINE_VERSION
        assert meta["structure_version"] == STRUCTURE_SCHEMA_VERSION
        assert meta["cache_format_version"] == CACHE_FORMAT_VERSION
        assert meta["arabic_text_hash"] == _hash_text(request.arabic_text)

    def test_chunk_timestamp_updated(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result()
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        meta_before = load_cache_metadata(request.verse_path, str(tmp_path))
        assert meta_before["chunk_timestamps"] == {}

        save_chunk_cache(request.verse_path, 1, _make_chunk_detail(), str(tmp_path))
        meta_after = load_cache_metadata(request.verse_path, str(tmp_path))
        assert "1" in meta_after["chunk_timestamps"]

    def test_metadata_language_keys_sorted(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result()
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        meta = load_cache_metadata(request.verse_path, str(tmp_path))
        assert meta["language_keys"] == sorted(VALID_LANGUAGE_KEYS)


# ---------------------------------------------------------------------------
# Staleness detection tests
# ---------------------------------------------------------------------------

class TestStaleness:
    def test_no_cache_is_stale(self, tmp_path):
        request = _make_request()
        result = check_cache_staleness(request, DUMMY_GLOSSARY, str(tmp_path))
        assert result.is_stale is True
        assert result.needs_structure is True
        assert result.needs_chunks is True
        assert "no cache exists" in result.reasons

    def test_fresh_cache_not_stale(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=2)
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        # Save all chunks
        for i in range(2):
            save_chunk_cache(request.verse_path, i, _make_chunk_detail(), str(tmp_path))

        result = check_cache_staleness(request, DUMMY_GLOSSARY, str(tmp_path))
        assert result.is_stale is False
        assert result.needs_structure is False
        assert result.needs_chunks is False
        assert result.stale_chunk_indices == []

    def test_arabic_changed_invalidates_all(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result()
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        # Change the Arabic text
        changed_request = _make_request(arabic_text="\u0642\u064f\u0644\u0652 \u0647\u064f\u0648\u064e \u0627\u0644\u0644\u0651\u064e\u0647\u064f \u0623\u064e\u062d\u064e\u062f\u064c")
        result = check_cache_staleness(changed_request, DUMMY_GLOSSARY, str(tmp_path))
        assert result.is_stale is True
        assert result.needs_structure is True
        assert result.needs_chunks is True
        assert "arabic text changed" in result.reasons

    def test_glossary_changed_invalidates_chunks(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=2)
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        for i in range(2):
            save_chunk_cache(request.verse_path, i, _make_chunk_detail(), str(tmp_path))

        # Check with a different glossary
        new_glossary = {"terms": [{"ar": "\u0635\u0644\u0627\u0629", "en": "prayer"}]}
        result = check_cache_staleness(request, new_glossary, str(tmp_path))
        assert result.is_stale is True
        assert result.needs_structure is False
        assert result.needs_chunks is True
        assert "glossary changed" in result.reasons

    def test_language_keys_changed_invalidates_chunks(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=1)
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        save_chunk_cache(request.verse_path, 0, _make_chunk_detail(), str(tmp_path))

        # Tamper with cached language keys to simulate a change
        meta_path = os.path.join(
            _cache_dir(request.verse_path, str(tmp_path)), "meta.json"
        )
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        meta["language_keys"] = ["en", "fr"]  # fewer than actual
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f)

        result = check_cache_staleness(request, DUMMY_GLOSSARY, str(tmp_path))
        assert result.is_stale is True
        assert result.needs_structure is False
        assert result.needs_chunks is True
        assert any("language keys changed" in r for r in result.reasons)

    def test_missing_chunk_file_reports_stale_index(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=3)
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        # Save only chunks 0 and 2 (missing chunk 1)
        save_chunk_cache(request.verse_path, 0, _make_chunk_detail(), str(tmp_path))
        save_chunk_cache(request.verse_path, 2, _make_chunk_detail(), str(tmp_path))

        result = check_cache_staleness(request, DUMMY_GLOSSARY, str(tmp_path))
        assert result.is_stale is True
        assert result.needs_structure is False
        assert result.needs_chunks is False
        assert result.stale_chunk_indices == [1]


# ---------------------------------------------------------------------------
# Invalidation tests
# ---------------------------------------------------------------------------

class TestInvalidation:
    def test_invalidate_full(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result()
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        assert invalidate_cache(request.verse_path, str(tmp_path)) is True
        assert load_structure_cache(request.verse_path, str(tmp_path)) is None
        # Second call returns False (already gone)
        assert invalidate_cache(request.verse_path, str(tmp_path)) is False

    def test_invalidate_chunks_only(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=2)
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        save_chunk_cache(request.verse_path, 0, _make_chunk_detail(), str(tmp_path))
        save_chunk_cache(request.verse_path, 1, _make_chunk_detail(), str(tmp_path))

        removed = invalidate_chunks(request.verse_path, base_dir=str(tmp_path))
        assert removed == 2
        # Structure still exists
        assert load_structure_cache(request.verse_path, str(tmp_path)) is not None
        # Chunks gone
        assert load_chunk_cache(request.verse_path, 0, str(tmp_path)) is None
        assert load_chunk_cache(request.verse_path, 1, str(tmp_path)) is None

    def test_invalidate_specific_chunks(self, tmp_path):
        request = _make_request()
        structure = _make_structure_result(chunk_count=3)
        save_structure_cache(
            request.verse_path, request, structure, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        for i in range(3):
            save_chunk_cache(request.verse_path, i, _make_chunk_detail(), str(tmp_path))

        removed = invalidate_chunks(
            request.verse_path, chunk_indices=[1], base_dir=str(tmp_path)
        )
        assert removed == 1
        assert load_chunk_cache(request.verse_path, 0, str(tmp_path)) is not None
        assert load_chunk_cache(request.verse_path, 1, str(tmp_path)) is None
        assert load_chunk_cache(request.verse_path, 2, str(tmp_path)) is not None


# ---------------------------------------------------------------------------
# get_cached_or_plan tests
# ---------------------------------------------------------------------------

class TestGetCachedOrPlan:
    def test_no_cache_returns_none(self, tmp_path):
        request = _make_request()
        structure, staleness = get_cached_or_plan(
            request, DUMMY_GLOSSARY, str(tmp_path)
        )
        assert structure is None
        assert staleness.is_stale is True
        assert staleness.needs_structure is True

    def test_fresh_cache_returns_structure(self, tmp_path):
        request = _make_request()
        original = _make_structure_result(chunk_count=2)
        save_structure_cache(
            request.verse_path, request, original, "test-model",
            glossary=DUMMY_GLOSSARY, base_dir=str(tmp_path),
        )
        for i in range(2):
            save_chunk_cache(request.verse_path, i, _make_chunk_detail(), str(tmp_path))

        structure, staleness = get_cached_or_plan(
            request, DUMMY_GLOSSARY, str(tmp_path)
        )
        assert structure == original
        assert staleness.is_stale is False
