"""Tests for app.words.corpus_extract."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from app.words.corpus_extract import (
    extract_corpus_surface_set,
    load_corpus_surface_set,
    summary_stats,
    tokenize_chunk_text,
    write_corpus_surface_set,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_responses_dir():
    """Create a temp dir with a few synthetic wrapper JSONs."""
    with tempfile.TemporaryDirectory() as td:
        # Wrapper 1 — al-kafi:1:1:1:1, v4 response
        wrapper1 = {
            "verse_path": "/books/al-kafi:1:1:1:1",
            "result": {
                "chunks": [
                    {
                        "chunk_type": "isnad",
                        "arabic_text": "عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ",
                    },
                    {
                        "chunk_type": "body",
                        "arabic_text": "قَالَ قَالَ أَبُو عَبْدِ اللَّهِ",
                    },
                ],
            },
        }
        # Wrapper 2 — al-kafi:1:1:1:2, also v4
        wrapper2 = {
            "verse_path": "/books/al-kafi:1:1:1:2",
            "result": {
                "chunks": [
                    {
                        "chunk_type": "body",
                        "arabic_text": "قَالَ النَّبِيُّ",
                    },
                ],
            },
        }
        # Wrapper 3 — v3 lean (word_analysis canonical, chunks may be stripped)
        wrapper3 = {
            "verse_path": "/books/al-kafi:1:1:1:3",
            "result": {
                "word_analysis": [
                    {"word": "قَالَ", "pos": "V"},
                    {"word": "النَّبِيُّ", "pos": "N"},
                ],
                "chunks": [
                    # v3 lean: chunks exist but arabic_text is stripped
                    {"chunk_type": "body", "word_start": 0, "word_end": 2},
                ],
            },
        }
        for name, w in [
            ("al-kafi_1_1_1_1.json", wrapper1),
            ("al-kafi_1_1_1_2.json", wrapper2),
            ("al-kafi_1_1_1_3.json", wrapper3),
        ]:
            with open(os.path.join(td, name), "w", encoding="utf-8") as f:
                json.dump(w, f, ensure_ascii=False)
        yield td


# ---------------------------------------------------------------------------
# tokenize_chunk_text
# ---------------------------------------------------------------------------

class TestTokenizeChunkText:
    def test_simple_split(self):
        result = tokenize_chunk_text("قَالَ النَّبِيُّ")
        assert result == ["قَالَ", "النَّبِيُّ"]

    def test_strips_punctuation(self):
        result = tokenize_chunk_text("قَالَ، النَّبِيُّ.")
        assert "قَالَ" in result
        assert "النَّبِيُّ" in result
        # Ensure punctuation is gone
        assert all("،" not in t and "." not in t for t in result)

    def test_empty_returns_empty(self):
        assert tokenize_chunk_text("") == []
        assert tokenize_chunk_text(None) == []  # type: ignore[arg-type]

    def test_whitespace_only_returns_empty(self):
        assert tokenize_chunk_text("    \n\t  ") == []

    def test_punctuation_only_tokens_dropped(self):
        result = tokenize_chunk_text(". , ، ؛ ؟")
        assert result == []


# ---------------------------------------------------------------------------
# extract_corpus_surface_set
# ---------------------------------------------------------------------------

class TestExtractCorpusSurfaceSet:
    def test_extracts_unique_surfaces(self, temp_responses_dir):
        result = extract_corpus_surface_set(temp_responses_dir)
        assert isinstance(result, dict)
        assert "قَالَ" in result
        # قَالَ appears: 2x in v4 verse1 + 1x in v4 verse2 + 1x in v3 verse3 = 4 total
        assert result["قَالَ"]["count"] == 4
        assert len(result["قَالَ"]["paths"]) == 3
        assert "/books/al-kafi:1:1:1:1" in result["قَالَ"]["paths"]
        assert "/books/al-kafi:1:1:1:2" in result["قَالَ"]["paths"]
        assert "/books/al-kafi:1:1:1:3" in result["قَالَ"]["paths"]

    def test_extracts_from_v3_word_analysis(self, temp_responses_dir):
        """v3 lean responses (word_analysis canonical, chunks stripped) must
        still contribute surfaces via word_analysis[*].word."""
        result = extract_corpus_surface_set(temp_responses_dir)
        # النَّبِيُّ appears in v4 verse 2 AND v3 verse 3 (from word_analysis)
        assert "النَّبِيُّ" in result
        # Should have path from v3 wrapper
        assert "/books/al-kafi:1:1:1:3" in result["النَّبِيُّ"]["paths"]

    def test_paths_sorted_and_deduped(self, temp_responses_dir):
        result = extract_corpus_surface_set(temp_responses_dir)
        for surface_data in result.values():
            paths = surface_data["paths"]
            assert paths == sorted(paths)
            assert len(paths) == len(set(paths))

    def test_unique_surface_appears_once(self, temp_responses_dir):
        result = extract_corpus_surface_set(temp_responses_dir)
        # عَلِيُّ should appear once (only in verse 1, chunk 1)
        if "عَلِيُّ" in result:
            assert result["عَلِيُّ"]["count"] == 1

    def test_missing_dir_returns_empty(self):
        result = extract_corpus_surface_set("/nonexistent/dir/path")
        assert result == {}

    def test_include_filter_restricts_paths(self, temp_responses_dir):
        # Filter to only verse 1
        result = extract_corpus_surface_set(
            temp_responses_dir, include_filter=["/books/al-kafi:1:1:1:1"]
        )
        # قَالَ appears twice in verse 1 only (filtered)
        assert result["قَالَ"]["count"] == 2
        assert result["قَالَ"]["paths"] == ["/books/al-kafi:1:1:1:1"]


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_write_and_load_round_trip(self, temp_responses_dir):
        original = extract_corpus_surface_set(temp_responses_dir)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tf:
            output_path = tf.name
        try:
            write_corpus_surface_set(original, output_path)
            loaded = load_corpus_surface_set(output_path)
            assert loaded == original
        finally:
            os.unlink(output_path)

    def test_writes_arabic_unescaped(self, temp_responses_dir):
        original = extract_corpus_surface_set(temp_responses_dir)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tf:
            output_path = tf.name
        try:
            write_corpus_surface_set(original, output_path)
            with open(output_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Arabic should appear as-is, not \uXXXX escape
            assert "قَالَ" in content
            assert "\\u0642" not in content
        finally:
            os.unlink(output_path)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class TestSummaryStats:
    def test_empty(self):
        stats = summary_stats({})
        assert stats == {"unique_surfaces": 0, "total_tokens": 0}

    def test_nonempty(self):
        s = {
            "a": {"count": 5, "paths": []},
            "b": {"count": 12, "paths": []},
            "c": {"count": 1, "paths": []},
        }
        stats = summary_stats(s)
        assert stats["unique_surfaces"] == 3
        assert stats["total_tokens"] == 18
        assert stats["max_freq"] == 12
        assert stats["min_freq"] == 1
        assert stats["surfaces_appearing_once"] == 1
        assert stats["surfaces_appearing_10_plus"] == 1
