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
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY)
        assert "تَقْوَى" in prompt
        assert "God-consciousness" in prompt
        assert "صَلَاة" in prompt
        assert "ritual prayer" in prompt

    def test_includes_taxonomy(self):
        taxonomy = {
            "taxonomy": {
                "creed": {
                    "topics": {
                        "tawhid": {"en": "Monotheism"},
                    }
                }
            }
        }
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY, topic_taxonomy=taxonomy)
        assert "TOPIC TAXONOMY" in prompt
        assert "tawhid" in prompt

    def test_omits_key_phrases(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY)
        assert "KEY PHRASES REFERENCE" not in prompt

    def test_omits_examples(self):
        prompt = build_phase1_system_prompt(glossary=MOCK_GLOSSARY)
        assert "EXAMPLES" not in prompt

    def test_smaller_than_monolithic(self):
        mock_taxonomy = {
            "taxonomy": {
                "creed": {
                    "topics": {
                        "tawhid": {"en": "Monotheism"},
                        "imamate": {"en": "Imamate"},
                    }
                }
            }
        }
        phase1 = build_phase1_system_prompt(
            glossary=MOCK_GLOSSARY, topic_taxonomy=mock_taxonomy
        )
        monolithic = build_system_prompt(
            glossary=MOCK_GLOSSARY,
            few_shot_examples={"examples": []},
            word_dictionary=None,
            topic_taxonomy=mock_taxonomy,
            key_phrases_dict={"phrases": []},
        )
        assert len(phase1) < len(monolithic), (
            f"Phase 1 prompt ({len(phase1)} chars) should be shorter than "
            f"monolithic ({len(monolithic)} chars)"
        )


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

    def test_requests_9_fields(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        # The 9 Phase 1 fields should be mentioned
        assert "diacritized_text" in msg
        assert "word_tags" in msg
        assert "chunks" in msg
        assert "translations" in msg
        assert "related_quran" in msg
        assert "isnad_matn" in msg
        assert '"tags"' in msg
        assert '"content_type"' in msg
        assert '"topics"' in msg

    def test_omits_narrator_details(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        # Phase 1 should NOT ask for the narrators array
        assert "No narrators array needed" in msg or "narrators array" not in msg
        # Specifically, the detailed narrator schema instructions should be absent
        assert "identity_confidence" not in msg
        assert "ambiguity_note" not in msg

    def test_omits_multilang_instructions(self):
        req = _make_request()
        msg = build_phase1_user_message(req)
        # Phase 1 only asks for EN translations, not the other 10 languages
        # Check that the multi-language translation keys are not instructed
        assert '"ur"' not in msg
        assert '"tr"' not in msg
        assert '"fa"' not in msg


# ---------------------------------------------------------------------------
# TestParsePhase1Response
# ---------------------------------------------------------------------------


class TestParsePhase1Response:
    def test_normalizes_response(self):
        raw = {
            "diacritized_text": "حَدَّثَنَا عَلِيٌّ",
            "diacritics_status": "added",
            "diacritics_changes": [],
            "word_tags": [["حَدَّثَنَا", "V"], ["عَلِيٌّ", "N"]],
            "chunks": [
                {
                    "chunk_type": "isnad",
                    "word_start": 0,
                    "word_end": 2,
                    "translations": {"en": "Ali narrated to us"},
                }
            ],
            "translations": {"en": {"summary": "A short hadith.", "seo_question": "Who narrated?"}},
            "related_quran": [{"ref": "2:255", "relationship": "thematic"}],
            "isnad_matn": {"isnad_ar": "حدثنا علي", "matn_ar": "", "has_chain": True},
            "topics": ["tawhid", "imamate"],
            "tags": ["theology", "ethics"],
            "content_type": "theological",
        }
        result = parse_phase1_response(raw)
        assert result["diacritized_text"] == "حَدَّثَنَا عَلِيٌّ"
        assert result["diacritics_status"] == "added"
        assert len(result["word_tags"]) == 2
        assert len(result["chunks"]) == 1
        assert result["translations"]["en"]["summary"] == "A short hadith."
        assert result["related_quran"][0]["ref"] == "2:255"
        assert result["isnad_matn"]["has_chain"] is True
        assert result["topics"] == ["tawhid", "imamate"]
        assert result["tags"] == ["theology", "ethics"]
        assert result["content_type"] == "theological"

    def test_strips_non_en_translations(self):
        raw = {
            "translations": {
                "en": {"summary": "English summary", "seo_question": "Q?"},
                "ur": {"summary": "Urdu summary", "seo_question": "Q?"},
                "tr": {"summary": "Turkish summary", "seo_question": "Q?"},
                "fa": {"summary": "Farsi summary", "seo_question": "Q?"},
            },
        }
        result = parse_phase1_response(raw)
        assert "en" in result["translations"]
        assert "ur" not in result["translations"]
        assert "tr" not in result["translations"]
        assert "fa" not in result["translations"]

    def test_stubs_narrators(self):
        raw = {
            "isnad_matn": {
                "isnad_ar": "حدثنا",
                "matn_ar": "قال",
                "has_chain": True,
                "narrators": [
                    {"name_ar": "علي", "name_en": "Ali", "role": "narrator", "position": 1}
                ],
            },
        }
        result = parse_phase1_response(raw)
        # narrators should always be empty — Phase 2 handles them
        assert result["isnad_matn"]["narrators"] == []
        # Other isnad_matn fields should be preserved
        assert result["isnad_matn"]["has_chain"] is True
        assert result["isnad_matn"]["isnad_ar"] == "حدثنا"

    def test_defaults_for_missing_fields(self):
        result = parse_phase1_response({})
        assert result["diacritized_text"] == ""
        assert result["diacritics_status"] == "added"
        assert result["diacritics_changes"] == []
        assert result["word_tags"] == []
        assert result["chunks"] == []
        assert result["translations"] == {"en": {"summary": "", "seo_question": ""}}
        assert result["related_quran"] == []
        assert result["isnad_matn"]["isnad_ar"] == ""
        assert result["isnad_matn"]["matn_ar"] == ""
        assert result["isnad_matn"]["has_chain"] is False
        assert result["isnad_matn"]["narrators"] == []
        assert result["topics"] == []
        assert result["tags"] == []
        assert result["content_type"] == ""
