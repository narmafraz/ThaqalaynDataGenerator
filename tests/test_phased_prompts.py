"""Tests for the Phase 1 phased prompts module (app.pipeline_cli.phased_prompts)."""

import pytest
from types import SimpleNamespace

from app.pipeline_cli.phased_prompts import (
    build_phase1_system_prompt,
    build_phase1_user_message,
    parse_phase1_response,
)
from app.ai_pipeline import build_system_prompt


MOCK_GLOSSARY = {
    "terms": [
        {"ar": "تَقْوَى", "en": "God-consciousness", "ur": "تقویٰ", "tr": "takva", "fa": "تقوا"},
        {"ar": "صَلَاة", "en": "ritual prayer", "ur": "نماز", "tr": "namaz", "fa": "نماز"},
    ]
}

MOCK_TAXONOMY = {
    "taxonomy": {
        "creed": {
            "topics": {
                "tawhid": {"en": "Monotheism"},
                "imamate": {"en": "Imamate"},
            }
        }
    }
}


def _make_request(**overrides):
    """Create a mock PipelineRequest using SimpleNamespace."""
    defaults = {
        "verse_path": "/books/al-kafi:1:1:1:1",
        "arabic_text": "حَدَّثَنَا عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ",
        "english_text": "Ali ibn Ibrahim narrated from his father",
        "book_name": "Al-Kafi",
        "chapter_title": "Book of Reason and Ignorance",
        "hadith_number": 1,
        "existing_narrator_chain": "Ali ibn Ibrahim > his father",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# TestBuildPhase1SystemPrompt
# ---------------------------------------------------------------------------


class TestBuildPhase1SystemPrompt:
    def test_contains_glossary(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY, topic_taxonomy=MOCK_TAXONOMY)
        assert "تَقْوَى" in prompt
        assert "God-consciousness" in prompt
        assert "صَلَاة" in prompt
        assert "ritual prayer" in prompt

    def test_includes_taxonomy(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY, topic_taxonomy=MOCK_TAXONOMY)
        assert "TOPIC TAXONOMY" in prompt
        assert "tawhid" in prompt

    def test_omits_key_phrases(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY, topic_taxonomy=MOCK_TAXONOMY)
        assert "KEY PHRASES REFERENCE" not in prompt

    def test_omits_examples(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY, topic_taxonomy=MOCK_TAXONOMY)
        assert "EXAMPLES" not in prompt

    def test_smaller_than_monolithic(self):
        phase1 = build_phase1_system_prompt(
            glossary=MOCK_GLOSSARY, topic_taxonomy=MOCK_TAXONOMY
        )
        monolithic = build_system_prompt(
            glossary=MOCK_GLOSSARY,
            few_shot_examples={"examples": []},
            word_dictionary=None,
            topic_taxonomy=MOCK_TAXONOMY,
            key_phrases_dict={"phrases": []},
        )
        assert len(phase1) < len(monolithic), (
            f"Phase 1 prompt ({len(phase1)} chars) should be shorter than "
            f"monolithic ({len(monolithic)} chars)"
        )

    def test_requires_diacritics(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY, topic_taxonomy=MOCK_TAXONOMY)
        assert "tashkeel" in prompt.lower() or "diacritics" in prompt.lower()


# ---------------------------------------------------------------------------
# TestBuildPhase1UserMessage
# ---------------------------------------------------------------------------


class TestBuildPhase1UserMessage:
    def test_contains_arabic_text(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert req.arabic_text in msg

    def test_contains_chapter_info(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "Al-Kafi" in msg
        assert "Book of Reason and Ignorance" in msg

    def test_requests_7_fields(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert '"has_chain"' in msg
        assert '"tags"' in msg
        assert '"content_type"' in msg
        assert '"chunks"' in msg
        assert '"translations"' in msg
        assert '"related_quran"' in msg
        assert '"topics"' in msg

    def test_chunks_have_arabic_text(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "arabic_text" in msg

    def test_chunks_no_word_start_end(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "word_start" not in msg
        assert "word_end" not in msg

    def test_has_chain_is_top_level(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "isnad_matn" not in msg

    def test_includes_key_terms(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "key_terms" in msg

    def test_omits_narrator_details(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "identity_confidence" not in msg
        assert "ambiguity_note" not in msg

    def test_omits_word_tags(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert "word_tags" not in msg

    def test_omits_diacritized_text_field(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert '"diacritized_text"' not in msg

    def test_omits_multilang_instructions(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        assert '"ur"' not in msg
        assert '"tr"' not in msg
        assert '"fa"' not in msg


# ---------------------------------------------------------------------------
# TestParsePhase1Response
# ---------------------------------------------------------------------------


class TestParsePhase1Response:
    def test_normalizes_response(self):
        raw = {
            "has_chain": True,
            "tags": ["theology", "ethics"],
            "content_type": "theological",
            "topics": ["tawhid", "imamate"],
            "chunks": [
                {
                    "chunk_type": "isnad",
                    "arabic_text": "حَدَّثَنَا عَلِيٌّ",
                    "translations": {"en": "Ali narrated to us"},
                }
            ],
            "translations": {
                "en": {
                    "summary": "A short hadith.",
                    "seo_question": "Who narrated?",
                    "key_terms": {"عَلِيٌّ": "Ali"},
                }
            },
            "related_quran": [{"ref": "2:255", "relationship": "thematic"}],
        }
        result = parse_phase1_response(raw)
        assert result["has_chain"] is True
        assert result["tags"] == ["theology", "ethics"]
        assert result["content_type"] == "theological"
        assert result["topics"] == ["tawhid", "imamate"]
        assert len(result["chunks"]) == 1
        assert result["chunks"][0]["arabic_text"] == "حَدَّثَنَا عَلِيٌّ"
        assert result["translations"]["en"]["summary"] == "A short hadith."
        assert result["translations"]["en"]["key_terms"] == {"عَلِيٌّ": "Ali"}
        assert result["related_quran"][0]["ref"] == "2:255"

    def test_strips_word_start_end_from_chunks(self):
        raw = {
            "chunks": [
                {
                    "chunk_type": "body",
                    "arabic_text": "بِسْمِ اللَّهِ",
                    "word_start": 0,
                    "word_end": 2,
                    "translations": {"en": "In the name of Allah"},
                }
            ],
        }
        result = parse_phase1_response(raw)
        assert "word_start" not in result["chunks"][0]
        assert "word_end" not in result["chunks"][0]

    def test_strips_non_en_translations(self):
        raw = {
            "translations": {
                "en": {"summary": "English summary", "seo_question": "Q?", "key_terms": {}},
                "ur": {"summary": "Urdu summary", "seo_question": "Q?"},
                "tr": {"summary": "Turkish summary", "seo_question": "Q?"},
            },
        }
        result = parse_phase1_response(raw)
        assert "en" in result["translations"]
        assert "ur" not in result["translations"]
        assert "tr" not in result["translations"]

    def test_preserves_key_terms(self):
        raw = {
            "translations": {
                "en": {
                    "summary": "S",
                    "seo_question": "Q?",
                    "key_terms": {"تَقْوَى": "God-consciousness"},
                }
            },
        }
        result = parse_phase1_response(raw)
        assert result["translations"]["en"]["key_terms"] == {"تَقْوَى": "God-consciousness"}

    def test_defaults_for_missing_fields(self):
        result = parse_phase1_response({})
        assert result["has_chain"] is False
        assert result["tags"] == []
        assert result["content_type"] == ""
        assert result["topics"] == []
        assert result["chunks"] == []
        assert result["translations"] == {
            "en": {"summary": "", "seo_question": "", "key_terms": {}}
        }
        assert result["related_quran"] == []
        # Phase 1 should NOT contain these fields:
        assert "diacritized_text" not in result
        assert "word_tags" not in result
        assert "isnad_matn" not in result
        assert "diacritics_status" not in result
        assert "diacritics_changes" not in result
