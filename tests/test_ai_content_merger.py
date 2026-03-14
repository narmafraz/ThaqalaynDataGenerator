"""Tests for app.ai_content_merger module."""

import json
import os
import tempfile
from unittest.mock import patch

import pytest

from app.ai_content_merger import (
    AI_TRANSLATION_ENTRIES,
    AI_TRANSLATION_IDS,
    build_lean_ai_content,
    load_ai_responses,
    merge_ai_content,
    merge_ai_into_complete_file,
    merge_ai_into_file,
    merge_ai_into_verse,
    resolve_canonical_ids,
    update_translations_index,
    _collect_ai_translation_ids,
)
from app.lib_model import ProcessingReport


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _sample_ai_result():
    """Return a sample AI pipeline result dict."""
    return {
        "diacritics_status": "completed",
        "diacritics_changes": [],
        "diacritized_text": "أَخْبَرَنَا أَبُو جَعْفَرٍ",
        "word_analysis": [
            {"word": "أَخْبَرَنَا", "translation": {"en": "informed us", "fr": "nous a informés"}, "pos": "V"},
            {"word": "أَبُو", "translation": {"en": "father of", "fr": "père de"}, "pos": "N"},
            {"word": "جَعْفَرٍ", "translation": {"en": "Ja'far", "fr": "Ja'far"}, "pos": "N"},
        ],
        "tags": ["theology"],
        "content_type": "theological",
        "related_quran": [{"ref": "2:30", "relationship": "thematic"}],
        "isnad_matn": {"has_chain": True, "narrators": [], "isnad_ar": "chain", "matn_ar": "body"},
        "translations": {
            "en": {
                "summary": "English summary",
                "key_terms": {"العقل": "intellect"},
                "seo_question": "What about intellect?",
            },
            "fr": {
                "summary": "Résumé français",
                "key_terms": {"العقل": "intellect"},
                "seo_question": "Quoi de l'intellect?",
            },
        },
        "chunks": [
            {
                "chunk_type": "isnad",
                "arabic_text": "أَخْبَرَنَا أَبُو",
                "word_start": 0,
                "word_end": 2,
                "translations": {"en": "Informed us, Abu", "fr": "Nous a informés, Abu"},
            },
            {
                "chunk_type": "body",
                "arabic_text": "جَعْفَرٍ",
                "word_start": 2,
                "word_end": 3,
                "translations": {"en": "Ja'far", "fr": "Ja'far"},
            },
        ],
        "topics": ["reasoning"],
        "key_phrases": [{"phrase_ar": "test", "phrase_en": "test", "category": "theological_concept"}],
        "similar_content_hints": [{"description": "desc", "theme": "theme"}],
    }


def _sample_attribution():
    return {
        "model": "claude-opus-4-6-20260205",
        "generated_date": "2026-02-27",
        "pipeline_version": "2.0.0",
        "generation_method": "claude_code_direct",
    }


def _sample_wrapper(verse_path="/books/al-kafi:1:1:1:1"):
    return {
        "verse_path": verse_path,
        "ai_attribution": _sample_attribution(),
        "result": _sample_ai_result(),
    }


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ─── load_ai_responses ───────────────────────────────────────────────────────

class TestLoadAiResponses:
    def test_loads_valid_files(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        wrapper = _sample_wrapper()
        _write_json(str(resp_dir / "al-kafi_1_1_1_1.json"), wrapper)

        result = load_ai_responses(str(resp_dir))
        assert "/books/al-kafi:1:1:1:1" in result
        assert "result" in result["/books/al-kafi:1:1:1:1"]
        assert "ai_attribution" in result["/books/al-kafi:1:1:1:1"]

    def test_skips_malformed_json(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        (resp_dir / "bad.json").write_text("not json", encoding="utf-8")

        result = load_ai_responses(str(resp_dir))
        assert len(result) == 0

    def test_skips_missing_verse_path(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        _write_json(str(resp_dir / "no_path.json"), {"result": {}})

        result = load_ai_responses(str(resp_dir))
        assert len(result) == 0

    def test_skips_missing_result(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        _write_json(str(resp_dir / "no_result.json"), {"verse_path": "/books/test:1"})

        result = load_ai_responses(str(resp_dir))
        assert len(result) == 0

    def test_missing_directory_returns_empty(self, tmp_path):
        result = load_ai_responses(str(tmp_path / "nonexistent"))
        assert result == {}

    def test_empty_directory(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        result = load_ai_responses(str(resp_dir))
        assert result == {}

    def test_skips_non_json_files(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        (resp_dir / "readme.txt").write_text("not a json file", encoding="utf-8")

        result = load_ai_responses(str(resp_dir))
        assert len(result) == 0

    def test_multiple_files(self, tmp_path):
        resp_dir = tmp_path / "responses"
        resp_dir.mkdir()
        _write_json(str(resp_dir / "v1.json"), _sample_wrapper("/books/al-kafi:1:1:1:1"))
        _write_json(str(resp_dir / "v2.json"), _sample_wrapper("/books/al-kafi:1:1:1:2"))

        result = load_ai_responses(str(resp_dir))
        assert len(result) == 2


# ─── build_lean_ai_content ───────────────────────────────────────────────────

class TestBuildLeanAiContent:
    def test_strips_diacritized_text(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        assert "diacritized_text" not in lean

    def test_strips_arabic_text_from_chunks(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        for chunk in lean["chunks"]:
            assert "arabic_text" not in chunk

    def test_preserves_chunk_translations(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        assert lean["chunks"][0]["translations"]["en"] == "Informed us, Abu"

    def test_dissolves_translations_into_summaries(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        assert lean["summaries"]["en"] == "English summary"
        assert lean["summaries"]["fr"] == "Résumé français"

    def test_dissolves_translations_into_key_terms(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        assert lean["key_terms"]["en"]["العقل"] == "intellect"

    def test_dissolves_translations_into_seo_questions(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        assert lean["seo_questions"]["en"] == "What about intellect?"

    def test_no_top_level_translations(self):
        result = _sample_ai_result()
        lean = build_lean_ai_content(result, _sample_attribution())
        assert "translations" not in lean

    def test_includes_attribution(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert lean["ai_attribution"]["model"] == "claude-opus-4-6-20260205"
        assert lean["ai_attribution"]["pipeline_version"] == "2.0.0"
        # generation_method should be stripped
        assert "generation_method" not in lean["ai_attribution"]

    def test_preserves_word_analysis(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert len(lean["word_analysis"]) == 3
        assert lean["word_analysis"][0]["word"] == "أَخْبَرَنَا"

    def test_preserves_isnad_matn(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert lean["isnad_matn"]["has_chain"] is True

    def test_preserves_topics_tags(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert lean["topics"] == ["reasoning"]
        assert lean["tags"] == ["theology"]

    def test_preserves_content_type(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert lean["content_type"] == "theological"

    def test_preserves_related_quran(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert lean["related_quran"][0]["ref"] == "2:30"

    def test_preserves_key_phrases(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert len(lean["key_phrases"]) == 1

    def test_preserves_similar_content_hints(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert len(lean["similar_content_hints"]) == 1

    def test_preserves_diacritics_status(self):
        lean = build_lean_ai_content(_sample_ai_result(), _sample_attribution())
        assert lean["diacritics_status"] == "completed"

    def test_empty_result(self):
        lean = build_lean_ai_content({}, {})
        assert lean == {}

    def test_empty_translations(self):
        result = {"translations": {}}
        lean = build_lean_ai_content(result, {})
        assert "summaries" not in lean
        assert "key_terms" not in lean
        assert "seo_questions" not in lean


# ─── merge_ai_into_verse ────────────────────────────────────────────────────

class TestMergeAiIntoVerse:
    def test_matches_and_merges(self):
        verse = {"path": "/books/al-kafi:1:1:1:1", "text": ["arabic"]}
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": _sample_attribution(), "result": _sample_ai_result()}}
        assert merge_ai_into_verse(verse, lookup) is True
        assert "ai" in verse
        assert "word_analysis" in verse["ai"]

    def test_no_match(self):
        verse = {"path": "/books/al-kafi:1:1:1:99", "text": ["arabic"]}
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": _sample_attribution(), "result": _sample_ai_result()}}
        assert merge_ai_into_verse(verse, lookup) is False
        assert "ai" not in verse

    def test_verse_without_path(self):
        verse = {"text": ["arabic"]}
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": _sample_attribution(), "result": _sample_ai_result()}}
        assert merge_ai_into_verse(verse, lookup) is False


# ─── _collect_ai_translation_ids ─────────────────────────────────────────────

class TestCollectAiTranslationIds:
    def test_collects_from_chunks(self):
        verses = [{"path": "/books/al-kafi:1:1:1:1"}]
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": {}, "result": _sample_ai_result()}}
        ids = _collect_ai_translation_ids(verses, lookup)
        assert "en.ai" in ids
        assert "fr.ai" in ids

    def test_no_match_returns_empty(self):
        verses = [{"path": "/books/other:1"}]
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": {}, "result": _sample_ai_result()}}
        ids = _collect_ai_translation_ids(verses, lookup)
        assert ids == []


# ─── merge_ai_into_file ─────────────────────────────────────────────────────

class TestMergeAiIntoFile:
    def test_verse_list(self, tmp_path):
        doc = {
            "kind": "verse_list",
            "index": "al-kafi:1:1:1",
            "data": {
                "verse_translations": ["en.hubeali"],
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "index": 1, "local_index": 1, "text": ["arabic"], "translations": {"en.hubeali": ["English"]}},
                    {"path": "/books/al-kafi:1:1:1:2", "index": 2, "local_index": 2, "text": ["arabic2"], "translations": {"en.hubeali": ["English2"]}},
                ],
            },
        }
        fpath = str(tmp_path / "chapter.json")
        _write_json(fpath, doc)

        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": _sample_attribution(), "result": _sample_ai_result()}}
        count = merge_ai_into_file(fpath, lookup)
        assert count == 1

        written = _read_json(fpath)
        # Verse 1 should have ai
        assert "ai" in written["data"]["verses"][0]
        # Verse 2 should not
        assert "ai" not in written["data"]["verses"][1]
        # verse_translations should include AI IDs
        vt = written["data"]["verse_translations"]
        assert "en.ai" in vt
        assert "fr.ai" in vt
        # original translations preserved
        assert "en.hubeali" in vt

    def test_chapter_list_skipped(self, tmp_path):
        doc = {"kind": "chapter_list", "data": {"chapters": []}}
        fpath = str(tmp_path / "chapters.json")
        _write_json(fpath, doc)
        count = merge_ai_into_file(fpath, {})
        assert count == 0

    def test_no_matching_verses(self, tmp_path):
        doc = {
            "kind": "verse_list",
            "data": {
                "verse_translations": ["en.hubeali"],
                "verses": [{"path": "/books/al-kafi:9:9:9:9", "text": ["arabic"]}],
            },
        }
        fpath = str(tmp_path / "chapter.json")
        _write_json(fpath, doc)
        count = merge_ai_into_file(fpath, {"/books/al-kafi:1:1:1:1": {"ai_attribution": {}, "result": _sample_ai_result()}})
        assert count == 0

    def test_malformed_file(self, tmp_path):
        fpath = str(tmp_path / "bad.json")
        with open(fpath, "w") as f:
            f.write("not json")
        count = merge_ai_into_file(fpath, {})
        assert count == 0

    def test_does_not_add_ai_to_verse_translations(self, tmp_path):
        """AI translation text should NOT be in verse.translations, only in verse.ai."""
        doc = {
            "kind": "verse_list",
            "data": {
                "verse_translations": ["en.hubeali"],
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "translations": {"en.hubeali": ["English"]}},
                ],
            },
        }
        fpath = str(tmp_path / "chapter.json")
        _write_json(fpath, doc)
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": _sample_attribution(), "result": _sample_ai_result()}}
        merge_ai_into_file(fpath, lookup)
        written = _read_json(fpath)
        verse = written["data"]["verses"][0]
        # en.ai should NOT be a key in verse.translations
        assert "en.ai" not in verse.get("translations", {})
        # but should be in the chapter's verse_translations
        assert "en.ai" in written["data"]["verse_translations"]


# ─── merge_ai_into_complete_file ─────────────────────────────────────────────

class TestMergeAiIntoCompleteFile:
    def test_recursive_walk(self, tmp_path):
        doc = {
            "kind": "complete_book",
            "data": {
                "chapters": [
                    {
                        "chapters": [
                            {
                                "verse_translations": ["en.hubeali"],
                                "verses": [
                                    {"path": "/books/al-kafi:1:1:1:1", "text": ["arabic"]},
                                ],
                            }
                        ],
                    }
                ],
            },
        }
        fpath = str(tmp_path / "complete.json")
        _write_json(fpath, doc)
        lookup = {"/books/al-kafi:1:1:1:1": {"ai_attribution": _sample_attribution(), "result": _sample_ai_result()}}
        count = merge_ai_into_complete_file(fpath, lookup)
        assert count == 1

        written = _read_json(fpath)
        leaf = written["data"]["chapters"][0]["chapters"][0]
        assert "ai" in leaf["verses"][0]
        assert "en.ai" in leaf["verse_translations"]


# ─── update_translations_index ───────────────────────────────────────────────

class TestUpdateTranslationsIndex:
    def test_adds_ai_entries(self, tmp_path):
        index_dir = tmp_path / "index"
        index_dir.mkdir()
        existing = {"en.qarai": {"id": "en.qarai", "lang": "en", "name": "Qarai"}}
        _write_json(str(index_dir / "translations.json"), existing)

        update_translations_index(str(tmp_path))

        updated = _read_json(str(index_dir / "translations.json"))
        # All 11 AI entries present
        for tid in AI_TRANSLATION_IDS:
            assert tid in updated
            assert updated[tid]["source"] == "ai"
        # Original entry preserved
        assert "en.qarai" in updated
        assert updated["en.qarai"]["name"] == "Qarai"

    def test_idempotent(self, tmp_path):
        index_dir = tmp_path / "index"
        index_dir.mkdir()
        _write_json(str(index_dir / "translations.json"), {})

        update_translations_index(str(tmp_path))
        first = _read_json(str(index_dir / "translations.json"))

        update_translations_index(str(tmp_path))
        second = _read_json(str(index_dir / "translations.json"))

        assert first == second

    def test_missing_file_skipped(self, tmp_path):
        # Should not raise
        update_translations_index(str(tmp_path))


# ─── merge_ai_content (integration) ─────────────────────────────────────────

class TestMergeAiContentIntegration:
    def test_full_pipeline(self, tmp_path):
        # Set up source dir with AI responses
        source_dir = tmp_path / "source"
        resp_dir = source_dir / "ai-content" / "samples" / "responses"
        resp_dir.mkdir(parents=True)
        _write_json(str(resp_dir / "al-kafi_1_1_1_1.json"), _sample_wrapper())

        # Set up dest dir with a chapter file
        dest_dir = tmp_path / "dest"
        books_dir = dest_dir / "books" / "al-kafi" / "1" / "1"
        books_dir.mkdir(parents=True)
        chapter_doc = {
            "kind": "verse_list",
            "index": "al-kafi:1:1:1",
            "data": {
                "verse_translations": ["en.hubeali"],
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "index": 1, "text": ["arabic"], "translations": {"en.hubeali": ["English"]}},
                ],
            },
        }
        _write_json(str(books_dir / "1.json"), chapter_doc)

        # Set up translations index
        index_dir = dest_dir / "index"
        index_dir.mkdir()
        _write_json(str(index_dir / "translations.json"), {"en.qarai": {"id": "en.qarai", "lang": "en", "name": "Qarai"}})

        report = ProcessingReport()

        with patch.dict(os.environ, {
            "DESTINATION_DIR": str(dest_dir) + "/",
        }):
            with patch("app.ai_content_merger.AI_RESPONSES_DIR", str(resp_dir)):
                merge_ai_content(report)

        assert report.ai_verses_available == 1
        assert report.ai_verses_merged == 1
        assert report.ai_merge_errors == []

        # Verify chapter file was updated
        written = _read_json(str(books_dir / "1.json"))
        verse = written["data"]["verses"][0]
        assert "ai" in verse
        assert "en.ai" in written["data"]["verse_translations"]
        # Zero duplication checks
        assert "diacritized_text" not in verse["ai"]
        for chunk in verse["ai"]["chunks"]:
            assert "arabic_text" not in chunk
        assert "translations" not in verse["ai"]
        # Dissolved fields present
        assert verse["ai"]["summaries"]["en"] == "English summary"

        # Verify translations index
        trans = _read_json(str(index_dir / "translations.json"))
        assert "en.ai" in trans
        assert "en.qarai" in trans

    def test_no_ai_content_dir(self, tmp_path):
        """Should handle missing AI directory gracefully."""
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        report = ProcessingReport()
        with patch.dict(os.environ, {"DESTINATION_DIR": str(dest_dir) + "/"}):
            with patch("app.ai_content_merger.AI_RESPONSES_DIR", str(tmp_path / "nonexistent")):
                merge_ai_content(report)

        assert report.ai_verses_available == 0
        assert report.ai_verses_merged == 0

    def test_idempotent_merge(self, tmp_path):
        """Running merge twice produces same result."""
        source_dir = tmp_path / "source"
        resp_dir = source_dir / "ai-content" / "samples" / "responses"
        resp_dir.mkdir(parents=True)
        _write_json(str(resp_dir / "al-kafi_1_1_1_1.json"), _sample_wrapper())

        dest_dir = tmp_path / "dest"
        books_dir = dest_dir / "books" / "al-kafi" / "1" / "1"
        books_dir.mkdir(parents=True)
        chapter_doc = {
            "kind": "verse_list",
            "index": "al-kafi:1:1:1",
            "data": {
                "verse_translations": ["en.hubeali"],
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "index": 1, "text": ["arabic"]},
                ],
            },
        }
        _write_json(str(books_dir / "1.json"), chapter_doc)
        index_dir = dest_dir / "index"
        index_dir.mkdir()
        _write_json(str(index_dir / "translations.json"), {})

        with patch.dict(os.environ, {"DESTINATION_DIR": str(dest_dir) + "/"}):
            with patch("app.ai_content_merger.AI_RESPONSES_DIR", str(resp_dir)):
                merge_ai_content()
                first = _read_json(str(books_dir / "1.json"))
                merge_ai_content()
                second = _read_json(str(books_dir / "1.json"))

        assert first == second


# ─── resolve_canonical_ids ────────────────────────────────────────────────────

class TestResolveCanonicalIds:
    def _make_lookup(self, narrators):
        """Build an ai_lookup with one verse containing given narrators."""
        return {
            "/books/al-kafi:1:1:1:1": {
                "ai_attribution": {},
                "result": {
                    "isnad_matn": {
                        "has_chain": True,
                        "narrators": narrators,
                    },
                },
            },
        }

    def test_resolves_known_narrator(self):
        """Resolves canonical_id for a narrator found in registry."""
        lookup = self._make_lookup([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1},
        ])

        with patch("app.ai_content_merger.NarratorRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.narrator_count = 1
            instance.resolve.return_value = 1
            count = resolve_canonical_ids(lookup)

        assert count == 1
        narrator = lookup["/books/al-kafi:1:1:1:1"]["result"]["isnad_matn"]["narrators"][0]
        assert narrator["canonical_id"] == 1

    def test_skips_when_registry_empty(self):
        """No resolution when registry has zero entries."""
        lookup = self._make_lookup([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1},
        ])

        with patch("app.ai_content_merger.NarratorRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.narrator_count = 0
            count = resolve_canonical_ids(lookup)

        assert count == 0
        assert "canonical_id" not in lookup["/books/al-kafi:1:1:1:1"]["result"]["isnad_matn"]["narrators"][0]

    def test_skips_unknown_narrator(self):
        """No canonical_id set when registry returns None."""
        lookup = self._make_lookup([
            {"name_ar": "مجهول", "name_en": "Unknown", "position": 1},
        ])

        with patch("app.ai_content_merger.NarratorRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.narrator_count = 100
            instance.resolve.return_value = None
            count = resolve_canonical_ids(lookup)

        assert count == 0
        assert "canonical_id" not in lookup["/books/al-kafi:1:1:1:1"]["result"]["isnad_matn"]["narrators"][0]

    def test_passes_preceding_names(self):
        """Preceding names are accumulated for chain-context disambiguation."""
        lookup = self._make_lookup([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1},
            {"name_ar": "أَبِيهِ", "name_en": "his father", "position": 2},
        ])

        resolve_calls = []

        def track_resolve(name_ar, preceding_names=None):
            resolve_calls.append((name_ar, list(preceding_names or [])))
            return {"عَلِيّ": 10, "أَبِيهِ": 20}.get(name_ar)

        with patch("app.ai_content_merger.NarratorRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.narrator_count = 100
            instance.resolve.side_effect = track_resolve
            count = resolve_canonical_ids(lookup)

        assert count == 2
        assert resolve_calls[0] == ("عَلِيّ", [])
        assert resolve_calls[1] == ("أَبِيهِ", ["عَلِيّ"])

    def test_no_narrators_in_result(self):
        """Gracefully handles results with no narrators."""
        lookup = {
            "/books/quran:1:1": {
                "ai_attribution": {},
                "result": {
                    "isnad_matn": {"has_chain": False, "narrators": []},
                },
            },
        }

        with patch("app.ai_content_merger.NarratorRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.narrator_count = 100
            count = resolve_canonical_ids(lookup)

        assert count == 0

    def test_idempotent(self):
        """Running twice doesn't double-count (already-resolved narrators)."""
        lookup = self._make_lookup([
            {"name_ar": "عَلِيّ", "name_en": "Ali", "position": 1, "canonical_id": 10},
        ])

        with patch("app.ai_content_merger.NarratorRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.narrator_count = 100
            instance.resolve.return_value = 10  # same ID
            count = resolve_canonical_ids(lookup)

        # Already had the same canonical_id, so count should be 0
        assert count == 0
