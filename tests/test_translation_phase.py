"""Tests for the Phase 4 translation module (app.pipeline_cli.translation_phase)."""

import asyncio
import copy
import json
import pytest
from unittest.mock import AsyncMock, patch

from app.pipeline_cli.translation_phase import (
    NON_EN_LANGUAGES,
    build_translation_prompt,
    merge_translations,
    translate_chunks,
)

SAMPLE_RESULT = {
    "chunks": [
        {
            "chunk_type": "body",
            "word_start": 0,
            "word_end": 5,
            "translations": {"en": "He said about knowledge"},
        },
    ],
    "translations": {
        "en": {
            "summary": "A hadith about knowledge",
            "seo_question": "What about knowledge?",
            "key_terms": {},
        }
    },
}

SAMPLE_TRANSLATION_RESPONSE = {
    "chunks": [
        {
            "translations": {
                "ur": "\u0627\u0633 \u0646\u06d2 \u0639\u0644\u0645 \u06a9\u06d2 \u0628\u0627\u0631\u06d2 \u0645\u06cc\u06ba \u06a9\u06c1\u0627",
                "tr": "\u0130lim hakk\u0131nda s\u00f6yledi",
                "fa": "\u0627\u0648 \u062f\u0631\u0628\u0627\u0631\u0647 \u0639\u0644\u0645 \u06af\u0641\u062a",
                "id": "Dia berkata tentang ilmu",
                "bn": "\u09a4\u09bf\u09a8\u09bf \u099c\u09cd\u099e\u09be\u09a8 \u09b8\u09ae\u09cd\u09aa\u09b0\u09cd\u0995\u09c7 \u09ac\u09b2\u09b2\u09c7\u09a8",
                "es": "Dijo sobre el conocimiento",
                "fr": "Il a dit \u00e0 propos du savoir",
                "de": "Er sagte \u00fcber das Wissen",
                "ru": "\u041e\u043d \u0441\u043a\u0430\u0437\u0430\u043b \u043e \u0437\u043d\u0430\u043d\u0438\u0438",
                "zh": "\u4ed6\u8c08\u5230\u4e86\u77e5\u8bc6",
            }
        }
    ],
    "summary": {
        "ur": "\u062d\u062f\u06cc\u062b \u0639\u0644\u0645 \u06a9\u06d2 \u0628\u0627\u0631\u06d2 \u0645\u06cc\u06ba",
        "tr": "\u0130lim hakk\u0131nda hadis",
        "fa": "\u062d\u062f\u06cc\u062b\u06cc \u062f\u0631\u0628\u0627\u0631\u0647 \u0639\u0644\u0645",
        "id": "Hadis tentang ilmu",
        "bn": "\u099c\u09cd\u099e\u09be\u09a8 \u09b8\u09ae\u09cd\u09aa\u09b0\u09cd\u0995\u09bf\u09a4 \u09b9\u09be\u09a6\u09bf\u09b8",
        "es": "Un hadiz sobre el conocimiento",
        "fr": "Un hadith sur le savoir",
        "de": "Ein Hadith \u00fcber Wissen",
        "ru": "\u0425\u0430\u0434\u0438\u0441 \u043e \u0437\u043d\u0430\u043d\u0438\u0438",
        "zh": "\u5173\u4e8e\u77e5\u8bc6\u7684\u5723\u8bad",
    },
    "seo_question": {
        "ur": "\u0639\u0644\u0645 \u06a9\u06d2 \u0628\u0627\u0631\u06d2 \u0645\u06cc\u06ba \u06a9\u06cc\u0627\u061f",
        "tr": "\u0130lim hakk\u0131nda ne?",
        "fa": "\u062f\u0631\u0628\u0627\u0631\u0647 \u0639\u0644\u0645 \u0686\u0647\u061f",
        "id": "Apa tentang ilmu?",
        "bn": "\u099c\u09cd\u099e\u09be\u09a8 \u09b8\u09ae\u09cd\u09aa\u09b0\u09cd\u0995\u09c7 \u0995\u09bf?",
        "es": "\u00bfQu\u00e9 sobre el conocimiento?",
        "fr": "Quoi sur le savoir?",
        "de": "Was \u00fcber Wissen?",
        "ru": "\u0427\u0442\u043e \u043e \u0437\u043d\u0430\u043d\u0438\u0438?",
        "zh": "\u5173\u4e8e\u77e5\u8bc6\uff1f",
    },
}


class TestBuildTranslationPrompt:
    """Tests for build_translation_prompt()."""

    def test_system_prompt_content(self):
        """System prompt mentions 10 languages."""
        system, _user = build_translation_prompt([], "", "")
        assert "10 languages" in system
        for lang_name in [
            "Urdu", "Turkish", "Farsi", "Indonesian", "Bengali",
            "Spanish", "French", "German", "Russian", "Chinese",
        ]:
            assert lang_name in system

    def test_user_message_contains_chunks(self):
        """Chunk EN translations appear in user message."""
        chunks = [{"chunk_type": "body", "translations": {"en": "He said about knowledge"}}]
        _system, user = build_translation_prompt(chunks, "summary", "question")
        assert "He said about knowledge" in user
        assert "Chunk 1 (body)" in user

    def test_user_message_contains_summary(self):
        """Summary text appears in user message."""
        _system, user = build_translation_prompt([], "A hadith about knowledge", "")
        assert "Summary: A hadith about knowledge" in user

    def test_user_message_contains_seo(self):
        """SEO question appears in user message."""
        _system, user = build_translation_prompt([], "", "What about knowledge?")
        assert "SEO Question: What about knowledge?" in user

    def test_includes_arabic_context(self):
        """Arabic text included when provided."""
        arabic = "\u0642\u0627\u0644 \u0639\u0646 \u0627\u0644\u0639\u0644\u0645"
        _system, user = build_translation_prompt([], "", "", arabic_text=arabic)
        assert arabic in user
        assert "Original Arabic" in user

    def test_no_arabic_when_empty(self):
        """Arabic section omitted when empty string."""
        _system, user = build_translation_prompt([], "", "", arabic_text="")
        assert "Original Arabic" not in user


class TestMergeTranslations:
    """Tests for merge_translations()."""

    def test_merges_chunk_translations(self):
        """Chunk translations for 10 languages merged in."""
        result = copy.deepcopy(SAMPLE_RESULT)
        merged = merge_translations(result, SAMPLE_TRANSLATION_RESPONSE)
        chunk_trans = merged["chunks"][0]["translations"]
        for lang in NON_EN_LANGUAGES:
            assert lang in chunk_trans, f"Missing language {lang} in chunk translations"
            assert chunk_trans[lang] != ""

    def test_merges_summary(self):
        """Summary translations merged into translations dict."""
        result = copy.deepcopy(SAMPLE_RESULT)
        merged = merge_translations(result, SAMPLE_TRANSLATION_RESPONSE)
        for lang in NON_EN_LANGUAGES:
            assert "summary" in merged["translations"][lang]
            assert merged["translations"][lang]["summary"] != ""

    def test_merges_seo_question(self):
        """seo_question translations merged."""
        result = copy.deepcopy(SAMPLE_RESULT)
        merged = merge_translations(result, SAMPLE_TRANSLATION_RESPONSE)
        for lang in NON_EN_LANGUAGES:
            assert "seo_question" in merged["translations"][lang]
            assert merged["translations"][lang]["seo_question"] != ""

    def test_preserves_en(self):
        """English translations not overwritten."""
        result = copy.deepcopy(SAMPLE_RESULT)
        merged = merge_translations(result, SAMPLE_TRANSLATION_RESPONSE)
        assert merged["translations"]["en"]["summary"] == "A hadith about knowledge"
        assert merged["translations"]["en"]["seo_question"] == "What about knowledge?"
        assert merged["chunks"][0]["translations"]["en"] == "He said about knowledge"

    def test_partial_response(self):
        """Missing languages handled gracefully."""
        result = copy.deepcopy(SAMPLE_RESULT)
        partial_response = {
            "chunks": [{"translations": {"ur": "ترجمہ", "tr": "Çeviri"}}],
            "summary": {"ur": "خلاصہ"},
            "seo_question": {},
        }
        merged = merge_translations(result, partial_response)
        # Provided languages are present
        assert merged["chunks"][0]["translations"]["ur"] == "ترجمہ"
        assert merged["translations"]["ur"]["summary"] == "خلاصہ"
        # Missing languages don't cause errors; they simply aren't set
        assert merged["translations"]["en"]["summary"] == "A hadith about knowledge"


class TestTranslateChunks:
    """Tests for translate_chunks() with mocked call_openai."""

    def test_calls_openai(self):
        """Verify call_openai called with correct model."""
        mock_call = AsyncMock(return_value={
            "result": json.dumps(SAMPLE_TRANSLATION_RESPONSE),
            "cost": 0.001,
            "output_tokens": 500,
        })
        with patch("app.pipeline_cli.openai_backend.call_openai", mock_call):
            result = copy.deepcopy(SAMPLE_RESULT)
            updated = asyncio.run(translate_chunks(result, model="gpt-5-mini"))

        mock_call.assert_called_once()
        call_args = mock_call.call_args
        assert call_args.kwargs.get("model") == "gpt-5-mini" or call_args[1].get("model") == "gpt-5-mini"
        # Translations should be merged
        assert "ur" in updated["chunks"][0]["translations"]
        assert updated["_phase4_cost"] == 0.001
        assert updated["_phase4_tokens"] == 500

    def test_handles_error(self):
        """Error response fills empty translations."""
        mock_call = AsyncMock(return_value={"error": "API rate limit exceeded"})
        with patch("app.pipeline_cli.openai_backend.call_openai", mock_call):
            result = copy.deepcopy(SAMPLE_RESULT)
            updated = asyncio.run(translate_chunks(result, model="gpt-5-mini"))

        # All non-EN languages should have empty fallback translations
        for lang in NON_EN_LANGUAGES:
            assert lang in updated["translations"]
            assert updated["translations"][lang]["summary"] == ""
            assert updated["translations"][lang]["seo_question"] == ""
            assert lang in updated["chunks"][0]["translations"]
