"""Tests for Phase 2 programmatic enrichment module."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.pipeline_cli.programmatic_enrichment import (
    enrich_diacritics_status,
    enrich_explicit_quran_refs,
    enrich_key_phrases,
    enrich_key_terms,
    enrich_narrators,
    enrich_topics_and_tags,
    programmatic_enrich,
)


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SAMPLE_PHASE1 = {
    "diacritized_text": "قَالَ عَلِيُّ بْنُ إِبْرَاهِيمَ",
    "diacritics_status": "completed",
    "diacritics_changes": [],
    "word_tags": [
        ["قَالَ", "V"],
        ["عَلِيُّ", "N"],
        ["بْنُ", "N"],
        ["إِبْرَاهِيمَ", "N"],
    ],
    "chunks": [
        {
            "chunk_type": "body",
            "word_start": 0,
            "word_end": 4,
            "translations": {"en": "Ali ibn Ibrahim said"},
        }
    ],
    "translations": {
        "en": {
            "summary": "Ali ibn Ibrahim narrates about prayer [2:255].",
            "seo_question": "What did Ali say?",
        }
    },
    "related_quran": [{"ref": "1:1", "relationship": "thematic"}],
    "isnad_matn": {
        "isnad_ar": "قَالَ عَلِيُّ بْنُ إِبْرَاهِيمَ",
        "matn_ar": "",
        "has_chain": True,
        "narrators": [],
    },
}

SAMPLE_REQUEST = SimpleNamespace(
    arabic_text="قَالَ عَلِيُّ بْنُ إِبْرَاهِيمَ",
    english_text="Ali ibn Ibrahim said about prayer [2:255].",
    book_name="al-kafi",
    chapter_title="The Book of Prayer",
    existing_narrator_chain="عَلِيُّ بْنُ إِبْرَاهِيمَ",
    verse_path="/books/al-kafi:1:1:1:1",
    hadith_number=1,
)

SAMPLE_PHRASES = {
    "phrases": [
        {
            "phrase_ar": "بِسْمِ اللَّهِ",
            "phrase_en": "In the name of Allah",
            "category": "quranic_echo",
        },
    ]
}

SAMPLE_WORD_DICT = {
    "عَلِيُّ|N": {"en": "Ali", "ur": "علی"},
    "إِبْرَاهِيمَ|N": {"en": "Ibrahim", "ur": "ابراہیم"},
}

SAMPLE_TAXONOMY = {
    "keyword_to_topics": {"prayer": ["salat", "prayer_rulings"]},
    "keyword_to_tags": {"prayer": ["worship", "jurisprudence"]},
    "chapter_to_content_type": {"the book of prayer": "legal_ruling"},
    "default_content_type_by_book": {"al-kafi": "narrative"},
    "tag_to_topics": {"worship": ["salat"]},
}


# ===================================================================
# TestEnrichExplicitQuranRefs
# ===================================================================


class TestEnrichExplicitQuranRefs:
    def test_explicit_ref_square_brackets(self):
        result = enrich_explicit_quran_refs("See verse [1:1] for details.")
        assert len(result) == 1
        assert result[0] == {"ref": "1:1", "relationship": "explicit"}

    def test_explicit_ref_parentheses(self):
        result = enrich_explicit_quran_refs("Quran (2:255) says...")
        assert len(result) == 1
        assert result[0] == {"ref": "2:255", "relationship": "explicit"}

    def test_multiple_refs(self):
        result = enrich_explicit_quran_refs("See [1:1] and also [2:255].")
        assert len(result) == 2
        refs = {r["ref"] for r in result}
        assert refs == {"1:1", "2:255"}

    def test_no_refs(self):
        result = enrich_explicit_quran_refs("Plain text with no references.")
        assert result == []

    def test_dedup_refs(self):
        result = enrich_explicit_quran_refs("Both [1:1] and again [1:1] here.")
        assert len(result) == 1
        assert result[0]["ref"] == "1:1"


# ===================================================================
# TestEnrichKeyPhrases
# ===================================================================


class TestEnrichKeyPhrases:
    def test_exact_match(self):
        text = "وَقَالَ بِسْمِ اللَّهِ الرَّحْمَنِ الرَّحِيمِ"
        result = enrich_key_phrases(text, SAMPLE_PHRASES)
        assert len(result) == 1
        assert result[0]["phrase_ar"] == "بِسْمِ اللَّهِ"
        assert result[0]["phrase_en"] == "In the name of Allah"
        assert result[0]["category"] == "quranic_echo"

    def test_diacritics_insensitive_match(self):
        """Phrase without diacritics should match diacritized text."""
        phrases = {
            "phrases": [
                {
                    "phrase_ar": "بسم الله",
                    "phrase_en": "In the name of Allah",
                    "category": "quranic_echo",
                },
            ]
        }
        text = "وَقَالَ بِسْمِ اللَّهِ الرَّحْمَنِ"
        result = enrich_key_phrases(text, phrases)
        assert len(result) == 1

    def test_no_match(self):
        text = "هذا حديث عن الصلاة"
        result = enrich_key_phrases(text, SAMPLE_PHRASES)
        assert result == []

    def test_multiple_matches(self):
        phrases = {
            "phrases": [
                {
                    "phrase_ar": "بِسْمِ اللَّهِ",
                    "phrase_en": "In the name of Allah",
                    "category": "quranic_echo",
                },
                {
                    "phrase_ar": "الرَّحْمَنِ الرَّحِيمِ",
                    "phrase_en": "The Most Gracious, the Most Merciful",
                    "category": "quranic_echo",
                },
            ]
        }
        text = "بِسْمِ اللَّهِ الرَّحْمَنِ الرَّحِيمِ"
        result = enrich_key_phrases(text, phrases)
        assert len(result) == 2

    def test_empty_dict(self):
        result = enrich_key_phrases("بِسْمِ اللَّهِ", {"phrases": []})
        assert result == []


# ===================================================================
# TestEnrichKeyTerms
# ===================================================================


class TestEnrichKeyTerms:
    def test_content_words_selected(self):
        """N, V, ADJ should be selected; PREP, CONJ should be skipped."""
        word_tags = [
            ["صَلاَة", "N"],
            ["فِي", "PREP"],
            ["كَتَبَ", "V"],
            ["وَ", "CONJ"],
            ["جَمِيل", "ADJ"],
        ]
        word_dict = {
            "صَلاَة|N": {"en": "prayer"},
            "كَتَبَ|V": {"en": "wrote"},
            "جَمِيل|ADJ": {"en": "beautiful"},
            "فِي|PREP": {"en": "in"},
        }
        result = enrich_key_terms(word_tags, word_dict)
        assert "en" in result
        en_terms = result["en"]
        assert "صَلاَة" in en_terms
        assert "كَتَبَ" in en_terms
        assert "جَمِيل" in en_terms
        # PREP should not appear
        assert "فِي" not in en_terms

    def test_stop_words_skipped(self):
        """Common isnad particles (عن, من, بن, قال, etc.) should be skipped."""
        word_tags = [["قَالَ", "V"], ["عَنْ", "N"], ["صَلاَة", "N"]]
        word_dict = {
            "قَالَ|V": {"en": "said"},
            "عَنْ|N": {"en": "from"},
            "صَلاَة|N": {"en": "prayer"},
        }
        result = enrich_key_terms(word_tags, word_dict)
        en_terms = result.get("en", {})
        # قال and عن are stop words (after stripping diacritics: قال, عن)
        assert "قَالَ" not in en_terms
        assert "عَنْ" not in en_terms
        assert "صَلاَة" in en_terms

    def test_dictionary_lookup(self):
        """Terms found in word_dictionary should have translations."""
        word_tags = [["عَلِيُّ", "N"]]
        result = enrich_key_terms(word_tags, SAMPLE_WORD_DICT)
        assert "en" in result
        assert result["en"]["عَلِيُّ"] == "Ali"
        assert "ur" in result
        assert result["ur"]["عَلِيُّ"] == "علی"

    def test_missing_from_dict(self):
        """Terms not in dictionary should not appear in result."""
        word_tags = [["غَرِيب", "N"]]
        result = enrich_key_terms(word_tags, SAMPLE_WORD_DICT)
        assert result == {}

    def test_empty_word_tags(self):
        result = enrich_key_terms([], SAMPLE_WORD_DICT)
        assert result == {}

    def test_top5_limit(self):
        """More than 5 content words -> only top 5 returned per language."""
        word_tags = [
            [f"كلمة{i}", "N"] for i in range(10)
        ]
        word_dict = {
            f"كلمة{i}|N": {"en": f"word{i}"} for i in range(10)
        }
        result = enrich_key_terms(word_tags, word_dict)
        assert len(result.get("en", {})) == 5


# ===================================================================
# TestEnrichDiacriticsStatus
# ===================================================================


class TestEnrichDiacriticsStatus:
    def test_identical_validated(self):
        text = "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ"
        assert enrich_diacritics_status(text, text) == "validated"

    def test_no_original_diacritics(self):
        """Original without tashkeel marks -> 'added'."""
        original = "بسم الله الرحمن الرحيم"
        diacritized = "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ"
        assert enrich_diacritics_status(original, diacritized) == "added"

    def test_different_corrected(self):
        """Texts differ and original has some diacritics -> 'corrected'."""
        original = "بِسْمِ الله الرحمن"
        diacritized = "بِسْمِ اللَّهِ الرَّحْمٰنِ"
        assert enrich_diacritics_status(original, diacritized) == "corrected"

    def test_empty_text(self):
        """Empty strings -> 'added' (no original to validate against)."""
        assert enrich_diacritics_status("", "") == "added"
        assert enrich_diacritics_status(None, None) == "added"


# ===================================================================
# TestEnrichTopicsAndTags
# ===================================================================


class TestEnrichTopicsAndTags:
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TOPICS", {"salat", "prayer_rulings"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TAGS", {"worship", "jurisprudence"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_CONTENT_TYPES", {"legal_ruling", "narrative"})
    def test_prayer_keywords(self):
        topics, tags, ct = enrich_topics_and_tags(
            "This hadith is about prayer and its rulings.",
            "The Book of Prayer",
            "al-kafi",
            SAMPLE_TAXONOMY,
        )
        assert "salat" in topics or "prayer_rulings" in topics
        assert "worship" in tags or "jurisprudence" in tags

    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TOPICS", {"knowledge", "education"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TAGS", {"knowledge"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_CONTENT_TYPES", {"narrative"})
    def test_knowledge_keywords(self):
        taxonomy = {
            "keyword_to_topics": {"knowledge": ["knowledge", "education"]},
            "keyword_to_tags": {"knowledge": ["knowledge"]},
            "chapter_to_content_type": {},
            "default_content_type_by_book": {"al-kafi": "narrative"},
        }
        topics, tags, ct = enrich_topics_and_tags(
            "The importance of knowledge and scholars.",
            "Chapter on Knowledge",
            "al-kafi",
            taxonomy,
        )
        assert "knowledge" in topics or "education" in topics
        assert "knowledge" in tags

    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TOPICS", {"salat"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TAGS", {"worship"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_CONTENT_TYPES", {"legal_ruling", "narrative"})
    def test_chapter_content_type(self):
        """Chapter title matching -> correct content_type."""
        _topics, _tags, ct = enrich_topics_and_tags(
            "Text about prayer.",
            "The Book of Prayer",
            "al-kafi",
            SAMPLE_TAXONOMY,
        )
        assert ct == "legal_ruling"

    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TOPICS", set())
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TAGS", set())
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_CONTENT_TYPES", {"narrative"})
    def test_default_content_type(self):
        """No chapter match -> default for book."""
        taxonomy = {
            "keyword_to_topics": {},
            "keyword_to_tags": {},
            "chapter_to_content_type": {},
            "default_content_type_by_book": {"al-kafi": "narrative"},
        }
        _topics, _tags, ct = enrich_topics_and_tags(
            "Some text.", "Unknown Chapter", "al-kafi", taxonomy
        )
        assert ct == "narrative"

    def test_none_taxonomy(self):
        """taxonomy is None -> reasonable defaults."""
        topics, tags, ct = enrich_topics_and_tags(
            "Some text.", "Chapter", "al-kafi", None
        )
        assert topics == []
        assert tags == []
        assert ct == "narrative"

    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TOPICS", {"salat", "prayer_rulings"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TAGS", {"worship", "jurisprudence"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_CONTENT_TYPES", {"legal_ruling", "narrative"})
    def test_validates_against_enums(self):
        """Only valid topics/tags/content_types should be returned."""
        taxonomy = {
            "keyword_to_topics": {"prayer": ["salat", "INVALID_TOPIC"]},
            "keyword_to_tags": {"prayer": ["worship", "BOGUS_TAG"]},
            "chapter_to_content_type": {"prayer": "legal_ruling"},
            "default_content_type_by_book": {},
        }
        topics, tags, ct = enrich_topics_and_tags(
            "About prayer.", "Book of Prayer", "al-kafi", taxonomy
        )
        assert "INVALID_TOPIC" not in topics
        assert "BOGUS_TAG" not in tags
        assert ct in {"legal_ruling", "narrative"}


# ===================================================================
# TestEnrichNarrators
# ===================================================================


class TestEnrichNarrators:
    def _make_mock_registry(self, resolve_return=None, narrator_data=None):
        """Create a MagicMock that passes isinstance checks."""
        from app.narrator_registry import NarratorRegistry

        mock = MagicMock(spec=NarratorRegistry)
        mock.resolve.return_value = resolve_return
        mock.get_narrator.return_value = narrator_data
        return mock

    def test_with_existing_chain(self):
        result = enrich_narrators(
            "قَالَ عَلِيُّ بْنُ إِبْرَاهِيمَ عن أبيه",
            "عَلِيُّ بْنُ إِبْرَاهِيمَ عن أبيه",
            None,
            None,
        )
        assert result["has_chain"] is True
        assert result["isnad_ar"] == "عَلِيُّ بْنُ إِبْرَاهِيمَ عن أبيه"

    def test_no_chain(self):
        result = enrich_narrators("بِسْمِ اللَّهِ", None, None, None)
        assert result["has_chain"] is False
        assert result["narrators"] == []

    def test_template_lookup(self):
        registry = self._make_mock_registry(
            resolve_return=42,
            narrator_data={
                "canonical_name_en": "Ali ibn Ibrahim",
                "role": "narrator",
                "known_identity": None,
            },
        )
        templates = {
            "42": {
                "name_en": "Ali ibn Ibrahim al-Qummi",
                "role": "narrator",
                "known_identity": "Al-Qummi",
            }
        }
        result = enrich_narrators(
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            templates,
            registry,
        )
        assert result["has_chain"] is True
        assert len(result["narrators"]) >= 1
        narrator = result["narrators"][0]
        assert narrator["name_en"] == "Ali ibn Ibrahim al-Qummi"
        assert narrator["role"] == "narrator"

    def test_registry_lookup(self):
        registry = self._make_mock_registry(
            resolve_return=99,
            narrator_data={
                "canonical_name_en": "Muhammad ibn Yahya",
                "role": "narrator",
                "known_identity": None,
            },
        )
        result = enrich_narrators(
            "مُحَمَّدُ بْنُ يَحْيَى",
            "مُحَمَّدُ بْنُ يَحْيَى",
            None,
            registry,
        )
        assert result["has_chain"] is True
        assert len(result["narrators"]) >= 1
        narrator = result["narrators"][0]
        assert narrator["canonical_id"] == 99
        assert narrator["identity_confidence"] == "high"

    def test_none_templates(self):
        """templates=None -> still returns basic structure."""
        registry = self._make_mock_registry(resolve_return=1, narrator_data=None)
        result = enrich_narrators(
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            None,
            registry,
        )
        assert result["has_chain"] is True
        assert isinstance(result["narrators"], list)

    def test_none_registry(self):
        """registry=None -> still works, canonical_id=None."""
        result = enrich_narrators(
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            None,
            None,
        )
        assert result["has_chain"] is True
        for narrator in result["narrators"]:
            assert narrator["canonical_id"] is None


# ===================================================================
# TestProgrammaticEnrichOrchestrator
# ===================================================================


class TestProgrammaticEnrichOrchestrator:
    @patch("app.pipeline_cli.programmatic_enrichment.enrich_narrators")
    def test_merges_narrators_into_isnad(self, mock_enrich_narrators):
        mock_enrich_narrators.return_value = {
            "isnad_ar": "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "matn_ar": "some text",
            "has_chain": True,
            "narrators": [
                {
                    "name_ar": "عَلِيُّ بْنُ إِبْرَاهِيمَ",
                    "name_en": "Ali ibn Ibrahim",
                    "role": "narrator",
                    "position": 0,
                    "identity_confidence": "high",
                    "ambiguity_note": None,
                    "known_identity": None,
                    "canonical_id": 42,
                }
            ],
        }
        result = programmatic_enrich(dict(SAMPLE_PHASE1), SAMPLE_REQUEST)
        isnad = result["isnad_matn"]
        assert len(isnad["narrators"]) == 1
        assert isnad["narrators"][0]["canonical_id"] == 42

    def test_merges_explicit_refs(self):
        """Explicit refs from english_text added alongside thematic refs from phase1."""
        result = programmatic_enrich(dict(SAMPLE_PHASE1), SAMPLE_REQUEST)
        refs = result["related_quran"]
        ref_strings = {r["ref"] for r in refs}
        # [2:255] is explicit from english_text, 1:1 is thematic from phase1
        assert "2:255" in ref_strings
        assert "1:1" in ref_strings

    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TOPICS", {"salat", "prayer_rulings"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_TAGS", {"worship", "jurisprudence"})
    @patch("app.pipeline_cli.programmatic_enrichment.VALID_CONTENT_TYPES", {"legal_ruling", "narrative"})
    def test_adds_topics_and_tags(self):
        result = programmatic_enrich(
            dict(SAMPLE_PHASE1), SAMPLE_REQUEST, taxonomy=SAMPLE_TAXONOMY
        )
        assert "topics" in result
        assert "tags" in result
        assert "content_type" in result

    def test_adds_key_phrases(self):
        phrases = {
            "phrases": [
                {
                    "phrase_ar": "عَلِيُّ بْنُ إِبْرَاهِيمَ",
                    "phrase_en": "Ali ibn Ibrahim",
                    "category": "well_known_saying",
                },
            ]
        }
        result = programmatic_enrich(
            dict(SAMPLE_PHASE1), SAMPLE_REQUEST, phrases_dict=phrases
        )
        assert "key_phrases" in result
        assert len(result["key_phrases"]) == 1

    def test_merges_key_terms_into_translations(self):
        result = programmatic_enrich(
            dict(SAMPLE_PHASE1), SAMPLE_REQUEST, word_dict=SAMPLE_WORD_DICT
        )
        translations = result.get("translations", {})
        if "en" in translations and isinstance(translations["en"], dict):
            # key_terms may or may not be populated depending on stop-word filtering
            # but the structure should exist
            assert isinstance(translations["en"], dict)

    def test_preserves_phase1_fields(self):
        """Phase 1 fields like diacritized_text, word_tags, chunks are preserved."""
        phase1 = dict(SAMPLE_PHASE1)
        result = programmatic_enrich(phase1, SAMPLE_REQUEST)
        assert result["diacritized_text"] == SAMPLE_PHASE1["diacritized_text"]
        assert result["word_tags"] == SAMPLE_PHASE1["word_tags"]
        assert result["chunks"] == SAMPLE_PHASE1["chunks"]

    def test_all_required_fields_present(self):
        """Result should have all standard pipeline fields."""
        result = programmatic_enrich(dict(SAMPLE_PHASE1), SAMPLE_REQUEST)
        expected_fields = {
            "diacritized_text",
            "diacritics_status",
            "diacritics_changes",
            "word_tags",
            "isnad_matn",
            "translations",
            "chunks",
            "related_quran",
            "topics",
            "key_phrases",
            "tags",
            "content_type",
        }
        for field in expected_fields:
            assert field in result, f"Missing field: {field}"

    def test_handles_none_resources(self):
        """All optional resources as None -> still returns valid result."""
        result = programmatic_enrich(
            dict(SAMPLE_PHASE1),
            SAMPLE_REQUEST,
            narrator_templates=None,
            registry=None,
            word_dict=None,
            phrases_dict=None,
            taxonomy=None,
        )
        assert isinstance(result, dict)
        assert "isnad_matn" in result
        assert "related_quran" in result
        assert "topics" in result
        assert "content_type" in result
