"""Tests for the AI content pipeline module."""

import json
import os
import pytest
import tempfile
from unittest.mock import patch

from app.ai_pipeline import (
    MAX_GENERATION_ATTEMPTS,
    QURAN_SURAH_AYAH_COUNTS,
    VALID_CHUNK_TYPES,
    VALID_DIACRITICS_STATUS,
    VALID_CONTENT_TYPES,
    VALID_IDENTITY_CONFIDENCE,
    VALID_LANGUAGE_KEYS,
    VALID_NARRATOR_ROLES,
    VALID_PHRASE_CATEGORIES,
    VALID_POS_TAGS,
    VALID_QURAN_RELATIONSHIPS,
    VALID_TAGS,
    VALID_TOPICS,
    PipelineConfig,
    PipelineRequest,
    build_system_prompt,
    build_user_message,
    compute_remaining,
    estimate_cost,
    extract_pipeline_request,
    load_few_shot_examples,
    load_glossary,
    load_key_phrases_dictionary,
    load_sample_verses,
    load_topic_taxonomy,
    merge_stats,
    parse_response,
    reconstruct_fields,
    strip_redundant_fields,
    validate_directory,
    validate_result,
    validate_wrapper,
    write_request_jsonl,
    write_verse_stats,
)


# ===================================================================
# Helper: build a valid pipeline result for testing
# ===================================================================

def _make_valid_result(**overrides):
    """Build a minimally valid pipeline result dict."""
    result = {
        "diacritized_text": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
        "diacritics_status": "validated",
        "diacritics_changes": [],
        "word_analysis": [
            {
                "word": "بِسْمِ",
                "translation": {lang: f"In the name of ({lang})" for lang in VALID_LANGUAGE_KEYS},
                "pos": "PREP",
            },
            {
                "word": "اللَّهِ",
                "translation": {lang: f"Allah ({lang})" for lang in VALID_LANGUAGE_KEYS},
                "pos": "N",
            },
        ],
        "tags": ["theology", "worship"],
        "content_type": "creedal",
        "related_quran": [],
        "isnad_matn": {
            "isnad_ar": "",
            "matn_ar": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
            "has_chain": False,
            "narrators": [],
        },
        "translations": {},
        "chunks": [
            {
                "chunk_type": "body",
                "arabic_text": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
                "word_start": 0,
                "word_end": 2,
                "translations": {lang: f"Body text ({lang})" for lang in VALID_LANGUAGE_KEYS},
            },
        ],
    }
    # Populate all 10 language translations
    for lang in VALID_LANGUAGE_KEYS:
        result["translations"][lang] = {
            "text": f"Translation in {lang}",
            "summary": f"Summary in {lang}",
            "key_terms": {"اللَّه": f"Allah in {lang}"},
            "seo_question": f"Question in {lang}?",
        }
    result.update(overrides)
    return result


def _make_valid_result_with_chain(**overrides):
    """Build a valid result with has_chain=True and narrator data.

    Internally consistent: word_analysis lists the same 8 words that the
    chain + matn cover, and chunks' word_start/word_end + arabic_text agree
    with that word_analysis so strip/reconstruct round-trips cleanly.
    """
    result = _make_valid_result()
    # Override word_analysis to actual chain + matn words so chunks align
    chain_words = [
        ("عَنْ", "PREP"),
        ("أَحْمَدَ", "N"),
        ("بْنِ", "N"),
        ("مُحَمَّدٍ", "N"),
        ("عَنْ", "PREP"),
        ("أَبِي", "N"),
        ("عَبْدِ", "N"),
        ("اللَّهِ", "N"),
    ]
    matn_words = [
        ("طَلَبُ", "N"),
        ("الْعِلْمِ", "N"),
        ("فَرِيضَةٌ", "N"),
    ]
    all_words = chain_words + matn_words
    result["word_analysis"] = [
        {
            "word": w,
            "translation": {lang: f"trans-{lang}-{i}" for lang in VALID_LANGUAGE_KEYS},
            "pos": pos,
        }
        for i, (w, pos) in enumerate(all_words)
    ]
    result["diacritized_text"] = " ".join(w for w, _ in all_words)
    result["isnad_matn"] = {
        "isnad_ar": "عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنْ أَبِي عَبْدِ اللَّهِ",
        "matn_ar": "طَلَبُ الْعِلْمِ فَرِيضَةٌ",
        "has_chain": True,
        "narrators": [
            {
                "name_ar": "أَحْمَدَ بْنِ مُحَمَّدٍ",
                "name_en": "Ahmad ibn Muhammad",
                "role": "narrator",
                "position": 1,
                "identity_confidence": "definite",
                "ambiguity_note": None,
                "known_identity": "Ahmad ibn Muhammad al-Barqi",
            },
            {
                "name_ar": "أَبِي عَبْدِ اللَّهِ",
                "name_en": "Abu Abdillah",
                "role": "imam",
                "position": 2,
                "identity_confidence": "definite",
                "ambiguity_note": None,
                "known_identity": "Imam Ja'far al-Sadiq (AS)",
            },
        ],
    }
    result["chunks"] = [
        {
            "chunk_type": "isnad",
            "arabic_text": " ".join(w for w, _ in chain_words),
            "word_start": 0,
            "word_end": len(chain_words),
            "translations": {lang: f"Isnad text in {lang} for the chain of narration" for lang in VALID_LANGUAGE_KEYS},
        },
        {
            "chunk_type": "body",
            "arabic_text": " ".join(w for w, _ in matn_words),
            "word_start": len(chain_words),
            "word_end": len(all_words),
            "translations": {lang: f"Body text in {lang}" for lang in VALID_LANGUAGE_KEYS},
        },
    ]
    # Update top-level translations.text to match the joined chunk text
    full_text = " ".join(c["translations"]["en"] for c in result["chunks"])
    for lang_data in result["translations"].values():
        if isinstance(lang_data, dict):
            lang_data["text"] = full_text
    result.update(overrides)
    return result


# ===================================================================
# Data file loading tests
# ===================================================================

class TestDataLoading:
    def test_load_glossary(self):
        data = load_glossary()
        assert "terms" in data
        assert len(data["terms"]) >= 40
        # Each term should have at least ar and en
        for term in data["terms"]:
            assert "ar" in term, f"Term missing 'ar': {term}"
            assert "en" in term, f"Term missing 'en': {term}"

    def test_load_glossary_has_all_languages(self):
        data = load_glossary()
        expected_langs = {"ar", "en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"}
        first_term = data["terms"][0]
        for lang in expected_langs:
            assert lang in first_term, f"First glossary term missing language: {lang}"

    def test_load_few_shot_examples(self):
        data = load_few_shot_examples()
        assert "examples" in data
        assert len(data["examples"]) == 3

    def test_few_shot_examples_have_input_output(self):
        data = load_few_shot_examples()
        for i, example in enumerate(data["examples"]):
            assert "input" in example, f"Example {i} missing 'input'"
            assert "output" in example, f"Example {i} missing 'output'"
            assert "arabic_text" in example["input"], f"Example {i} input missing 'arabic_text'"

    def test_few_shot_examples_outputs_validate(self):
        """Each few-shot example output should pass validation."""
        data = load_few_shot_examples()
        for i, example in enumerate(data["examples"]):
            errors = validate_result(example["output"])
            assert errors == [], f"Example {i} validation errors: {errors}"

    def test_load_sample_verses(self):
        data = load_sample_verses()
        assert "verses" in data
        assert len(data["verses"]) == 20
        for entry in data["verses"]:
            assert "path" in entry
            assert entry["path"].startswith("/books/")


# ===================================================================
# System prompt tests
# ===================================================================

class TestBuildSystemPrompt:
    def test_includes_glossary(self):
        glossary = load_glossary()
        prompt = build_system_prompt(glossary=glossary, few_shot_examples=load_few_shot_examples())
        # Should contain at least some glossary terms
        assert "صلاة" in prompt
        assert "salat" in prompt.lower() or "prayer" in prompt.lower()

    def test_includes_few_shot_examples(self):
        prompt = build_system_prompt()
        assert "Example 1" in prompt
        assert "Example 2" in prompt
        assert "Example 3" in prompt
        assert "Expected output:" in prompt

    def test_includes_rules(self):
        prompt = build_system_prompt()
        assert "IMPORTANT RULES" in prompt
        assert "honorifics" in prompt.lower()
        assert "valid JSON only" in prompt

    def test_includes_shia_context(self):
        prompt = build_system_prompt()
        assert "Shia" in prompt
        assert "Four Books" in prompt

    def test_word_dictionary_section_removed_in_v4(self):
        """v4: word dictionary section no longer included in system prompt."""
        from app.ai_pipeline import load_word_dictionary
        word_dict = load_word_dictionary()
        prompt = build_system_prompt(word_dictionary=word_dict)
        assert "COMMON WORD TRANSLATIONS" not in prompt

    def test_omits_word_dictionary_when_none(self):
        prompt = build_system_prompt(word_dictionary=None)
        # When explicitly None and file doesn't exist, section should be absent
        # But since the file now exists, it will be loaded. Test with empty dict instead.
        prompt_empty = build_system_prompt(
            word_dictionary={"words": []}
        )
        assert "COMMON WORD TRANSLATIONS" not in prompt_empty

    def test_word_dictionary_canonical_translations(self):
        from app.ai_pipeline import load_word_dictionary
        word_dict = load_word_dictionary()
        if word_dict is None:
            pytest.skip("word_dictionary.json not found")
        words = word_dict.get("words", [])
        assert len(words) >= 20, "Word dictionary should have at least 20 entries"
        # Each entry should have all required fields
        for entry in words:
            assert "ar" in entry, "Each word must have 'ar' field"
            assert "diacritized" in entry, "Each word must have 'diacritized' field"
            assert "pos" in entry, "Each word must have 'pos' field"
            assert "en" in entry, "Each word must have 'en' field"


# ===================================================================
# User message tests
# ===================================================================

class TestBuildUserMessage:
    def test_includes_arabic_text(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="طَلَبُ الْعِلْمِ فَرِيضَةٌ",
        )
        msg = build_user_message(req)
        assert "طَلَبُ الْعِلْمِ فَرِيضَةٌ" in msg

    def test_includes_english_when_present(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
            english_text="Seeking knowledge is an obligation.",
        )
        msg = build_user_message(req)
        assert "Seeking knowledge is an obligation." in msg

    def test_excludes_english_when_absent(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
            english_text="",
        )
        msg = build_user_message(req)
        assert "English reference translation" not in msg

    def test_includes_narrator_chain_when_present(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
            existing_narrator_chain="عن أحمد بن محمد قال",
        )
        msg = build_user_message(req)
        assert "عن أحمد بن محمد قال" in msg

    def test_shows_null_when_no_narrator_chain(self):
        req = PipelineRequest(
            verse_path="/books/quran:1:1",
            arabic_text="text",
            existing_narrator_chain=None,
        )
        msg = build_user_message(req)
        assert "null" in msg

    def test_includes_hadith_number(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:5",
            arabic_text="text",
            hadith_number=5,
        )
        msg = build_user_message(req)
        assert "Hadith number: 5" in msg

    def test_excludes_hadith_number_when_none(self):
        req = PipelineRequest(
            verse_path="/books/quran:1:1",
            arabic_text="text",
            hadith_number=None,
        )
        msg = build_user_message(req)
        assert "Hadith number" not in msg

    def test_includes_output_schema(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
        )
        msg = build_user_message(req)
        assert "diacritized_text" in msg
        assert "word_tags" in msg
        assert "translations" in msg
        assert "isnad_matn" in msg
        assert "chunks" in msg


# ===================================================================
# Validation tests — valid data
# ===================================================================

class TestValidateResultValid:
    def test_valid_result_passes(self):
        result = _make_valid_result()
        errors = validate_result(result)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_valid_result_with_chain_passes(self):
        result = _make_valid_result_with_chain()
        errors = validate_result(result)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_valid_quran_references(self):
        result = _make_valid_result(
            related_quran=[
                {"ref": "96:1", "relationship": "explicit"},
                {"ref": "20:114", "relationship": "thematic"},
            ]
        )
        errors = validate_result(result)
        assert errors == []


# ===================================================================
# Validation tests — invalid enums
# ===================================================================

class TestValidateResultInvalidEnums:
    def test_invalid_diacritics_status(self):
        result = _make_valid_result(diacritics_status="unknown")
        errors = validate_result(result)
        assert any("diacritics_status" in e for e in errors)

    def test_invalid_pos_tag(self):
        result = _make_valid_result()
        result["word_analysis"][0]["pos"] = "NOUN"  # should be "N"
        errors = validate_result(result)
        assert any("pos" in e and "NOUN" in e for e in errors)

    def test_invalid_tag(self):
        result = _make_valid_result(tags=["theology", "invalid_tag"])
        errors = validate_result(result)
        assert any("invalid tag" in e for e in errors)

    def test_invalid_content_type(self):
        result = _make_valid_result(content_type="unknown_type")
        errors = validate_result(result)
        assert any("content_type" in e for e in errors)

    def test_invalid_quran_relationship(self):
        result = _make_valid_result(
            related_quran=[{"ref": "1:1", "relationship": "direct"}]
        )
        errors = validate_result(result)
        assert any("quran relationship" in e for e in errors)

    def test_invalid_surah_number(self):
        result = _make_valid_result(
            related_quran=[{"ref": "115:1", "relationship": "thematic"}]
        )
        errors = validate_result(result)
        assert any("surah number" in e for e in errors)

    def test_invalid_ayah_number(self):
        """Al-Fatiha has 7 ayat — ayah 8 should fail."""
        result = _make_valid_result(
            related_quran=[{"ref": "1:8", "relationship": "thematic"}]
        )
        errors = validate_result(result)
        if QURAN_SURAH_AYAH_COUNTS:
            assert any("invalid ayah number" in e for e in errors)

    def test_valid_ayah_boundary(self):
        """Al-Baqarah has 286 ayat — ayah 286 should pass."""
        result = _make_valid_result(
            related_quran=[{"ref": "2:286", "relationship": "thematic"}]
        )
        errors = validate_result(result)
        assert not any("invalid ayah number" in e for e in errors)

    def test_invalid_quran_ref_format(self):
        result = _make_valid_result(
            related_quran=[{"ref": "abc", "relationship": "thematic"}]
        )
        errors = validate_result(result)
        assert any("quran ref format" in e for e in errors)

    def test_valid_quran_ref_with_word_ranges(self):
        """Explicit refs can have optional word_start/word_end."""
        result = _make_valid_result(
            related_quran=[{"ref": "96:1", "relationship": "explicit", "word_start": 0, "word_end": 2}]
        )
        errors = validate_result(result)
        assert not any("related_quran word" in e for e in errors)

    def test_invalid_quran_ref_word_end_before_start(self):
        result = _make_valid_result(
            related_quran=[{"ref": "96:1", "relationship": "explicit", "word_start": 2, "word_end": 1}]
        )
        errors = validate_result(result)
        assert any("word_end <= word_start" in e for e in errors)

    def test_invalid_quran_ref_word_exceeds_analysis(self):
        result = _make_valid_result(
            related_quran=[{"ref": "96:1", "relationship": "explicit", "word_start": 0, "word_end": 999}]
        )
        errors = validate_result(result)
        assert any("exceeds word_analysis length" in e for e in errors)

    def test_invalid_narrator_role(self):
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["role"] = "teacher"
        errors = validate_result(result)
        assert any("narrator role" in e for e in errors)

    def test_invalid_identity_confidence(self):
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["identity_confidence"] = "certain"
        errors = validate_result(result)
        assert any("identity_confidence" in e for e in errors)

    def test_missing_ambiguity_note_for_likely(self):
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["identity_confidence"] = "likely"
        result["isnad_matn"]["narrators"][0]["ambiguity_note"] = None
        errors = validate_result(result)
        assert any("ambiguity_note" in e for e in errors)

    def test_narrator_position_mismatch(self):
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["position"] = 5
        errors = validate_result(result)
        assert any("position mismatch" in e for e in errors)

    def test_valid_narrator_word_ranges(self):
        """Valid word_ranges passes validation."""
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["word_ranges"] = [
            {"word_start": 0, "word_end": 1}
        ]
        errors = validate_result(result)
        assert not any("word_ranges" in e for e in errors)

    def test_narrator_word_ranges_out_of_bounds(self):
        """word_end exceeding word_analysis length should fail."""
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["word_ranges"] = [
            {"word_start": 0, "word_end": 999}
        ]
        errors = validate_result(result)
        assert any("word_ranges" in e and "exceeds" in e for e in errors)

    def test_narrator_word_ranges_invalid_range(self):
        """word_end <= word_start should fail."""
        result = _make_valid_result_with_chain()
        result["isnad_matn"]["narrators"][0]["word_ranges"] = [
            {"word_start": 3, "word_end": 2}
        ]
        errors = validate_result(result)
        assert any("word_ranges" in e and "must be >" in e for e in errors)

    def test_invalid_language_key(self):
        result = _make_valid_result()
        result["translations"]["xx"] = {"text": "x", "summary": "x", "key_terms": {}, "seo_question": "x"}
        errors = validate_result(result)
        assert any("invalid language key" in e for e in errors)


# ===================================================================
# Validation tests — missing fields
# ===================================================================

class TestValidateResultMissingFields:
    def test_missing_required_field(self):
        # Remove both diacritized_text AND word_analysis so auto-reconstruct
        # cannot restore it — tests a genuinely missing required field.
        result = _make_valid_result()
        del result["diacritized_text"]
        del result["word_analysis"]
        errors = validate_result(result)
        assert any("missing required field" in e and "diacritized_text" in e for e in errors)

    def test_missing_diacritized_text_auto_reconstructed(self):
        """Stripped format (diacritized_text removed but word_analysis present) passes."""
        result = _make_valid_result()
        del result["diacritized_text"]
        errors = validate_result(result)
        assert not any("diacritized_text" in e for e in errors)

    def test_missing_all_languages(self):
        result = _make_valid_result()
        result["translations"] = {}
        errors = validate_result(result)
        assert any("missing languages" in e for e in errors)

    def test_missing_some_languages(self):
        result = _make_valid_result()
        del result["translations"]["ur"]
        del result["translations"]["zh"]
        errors = validate_result(result)
        assert any("missing languages" in e for e in errors)

    def test_missing_translation_subfield(self):
        result = _make_valid_result()
        del result["translations"]["ur"]["summary"]
        errors = validate_result(result)
        assert any("translations.ur" in e and "summary" in e for e in errors)

    def test_empty_translation_summary(self):
        # Catches Phase 4 silent-swallow where batch parse failure leaves
        # translation fields as empty strings instead of being absent.
        result = _make_valid_result()
        result["translations"]["fa"]["summary"] = ""
        errors = validate_result(result)
        assert any("translations.fa.summary is empty string" in e for e in errors)

    def test_has_chain_true_but_empty_isnad(self):
        result = _make_valid_result()
        result["isnad_matn"]["has_chain"] = True
        result["isnad_matn"]["isnad_ar"] = ""
        result["isnad_matn"]["narrators"] = []
        errors = validate_result(result)
        assert any("isnad_ar is empty" in e for e in errors)


# ===================================================================
# Validation tests — wrong types
# ===================================================================

class TestValidateResultWrongTypes:
    def test_diacritized_text_not_string(self):
        result = _make_valid_result(diacritized_text=123)
        errors = validate_result(result)
        assert any("diacritized_text must be string" in e for e in errors)

    def test_tags_not_array(self):
        result = _make_valid_result(tags="theology")
        errors = validate_result(result)
        assert any("tags must be array" in e for e in errors)

    def test_tags_wrong_count(self):
        result = _make_valid_result(tags=["theology"])  # only 1, need 2-5
        errors = validate_result(result)
        assert any("2-5 items" in e for e in errors)

    def test_word_missing_diacritics(self):
        result = _make_valid_result()
        result["word_analysis"][0]["word"] = "بسم"  # no tashkeel
        errors = validate_result(result)
        assert any("no diacritics" in e for e in errors)

    def test_word_with_diacritics_passes(self):
        result = _make_valid_result()
        # Default words already have diacritics (بِسْمِ, اللَّهِ)
        errors = validate_result(result)
        assert not any("no diacritics" in e for e in errors)

    def test_punctuation_word_skips_diacritics_check(self):
        """Punctuation-only words like (, ), . should not require diacritics."""
        result = _make_valid_result()
        # Insert a punctuation word in the middle
        result["word_analysis"].insert(1, {
            "word": "(",
            "translation": {lang: "(" for lang in VALID_LANGUAGE_KEYS},
            "pos": "PART",
        })
        # Adjust chunk word_end to match new word count
        result["chunks"][0]["word_end"] = 3
        errors = validate_result(result)
        assert not any("no diacritics" in e and "'('" in e for e in errors)

    def test_single_letter_abbreviation_skips_diacritics_check(self):
        """Single-letter Arabic abbreviations (ع, ص, ج) should not require diacritics."""
        result = _make_valid_result()
        # Test common hadith abbreviations: ع (alayhi al-salam), ص (sallallahu), ج (volume)
        for abbrev in ["\u0639", "\u0635", "\u062c", "\u0635\u0640", "\u0639."]:
            result["word_analysis"].insert(1, {
                "word": abbrev,
                "translation": {lang: "(AS)" for lang in VALID_LANGUAGE_KEYS},
                "pos": "PART",
            })
        result["chunks"][0]["word_end"] = len(result["word_analysis"])
        errors = validate_result(result)
        assert not any("no diacritics" in e for e in errors), (
            f"Abbreviation words should be exempt from diacritics check, got: "
            f"{[e for e in errors if 'no diacritics' in e]}"
        )

    def test_multi_letter_undiacritized_still_fails(self):
        """Multi-letter undiacritized Arabic words should still fail validation."""
        result = _make_valid_result()
        result["word_analysis"][0]["word"] = "\u0628\u0633\u0645"  # بسم — 3 letters, no diacritics
        errors = validate_result(result)
        assert any("no diacritics" in e for e in errors)


# ===================================================================
# Parse response tests
# ===================================================================

class TestParseResponse:
    def test_clean_json(self):
        data = {"key": "value"}
        result = parse_response(json.dumps(data))
        assert result == data

    def test_markdown_wrapped_json(self):
        data = {"key": "value"}
        text = f"```json\n{json.dumps(data)}\n```"
        result = parse_response(text)
        assert result == data

    def test_markdown_wrapped_no_language(self):
        data = {"key": "value"}
        text = f"```\n{json.dumps(data)}\n```"
        result = parse_response(text)
        assert result == data

    def test_json_with_surrounding_text(self):
        data = {"key": "value"}
        text = f"Here is the result:\n{json.dumps(data)}\nDone."
        result = parse_response(text)
        assert result == data

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Could not extract valid JSON"):
            parse_response("this is not json at all")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_response("")


# ===================================================================
# Extract pipeline request tests
# ===================================================================

class TestExtractPipelineRequest:
    def test_extract_from_chapter_file(self, tmp_path):
        """Test extracting a verse from a chapter JSON file."""
        # Create a mock chapter file
        chapter = {
            "index": "quran:1",
            "kind": "verse_list",
            "data": {
                "titles": {"en": "Al-Fatiha", "ar": "الفاتحة"},
                "verses": [
                    {
                        "path": "/books/quran:1:1",
                        "part_type": "Verse",
                        "local_index": 1,
                        "text": ["بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ"],
                        "translations": {
                            "en.qarai": ["In the Name of Allah, the All-beneficent, the All-merciful."]
                        },
                    }
                ],
            },
        }

        # Write to temp directory mimicking ThaqalaynData structure
        books_dir = tmp_path / "books" / "quran"
        books_dir.mkdir(parents=True)
        chapter_file = books_dir / "1.json"
        chapter_file.write_text(json.dumps(chapter, ensure_ascii=False), encoding="utf-8")

        result = extract_pipeline_request("/books/quran:1:1", data_dir=str(tmp_path))
        assert result is not None
        assert result.verse_path == "/books/quran:1:1"
        assert "بِسْمِ" in result.arabic_text
        assert "Name of Allah" in result.english_text
        assert result.book_name == "quran"
        assert result.chapter_title == "Al-Fatiha"

    def test_extract_with_narrator_chain(self, tmp_path):
        """Test extracting a hadith that has a narrator chain."""
        chapter = {
            "index": "al-kafi:1:1:1",
            "kind": "verse_list",
            "data": {
                "titles": {"en": "Chapter 1"},
                "verses": [
                    {
                        "path": "/books/al-kafi:1:1:1:1",
                        "part_type": "Hadith",
                        "local_index": 1,
                        "text": ["عِدَّةٌ مِنْ أَصْحَابِنَا"],
                        "narrator_chain": {
                            "parts": [
                                {"kind": "narrator", "text": "أَبُو جَعْفَرٍ"},
                                {"kind": "plain", "text": " عَنْ "},
                                {"kind": "narrator", "text": "أَحْمَدَ"},
                            ]
                        },
                        "translations": {
                            "en.hubeali": ["A number of our companions..."]
                        },
                    }
                ],
            },
        }

        books_dir = tmp_path / "books" / "al-kafi" / "1" / "1"
        books_dir.mkdir(parents=True)
        chapter_file = books_dir / "1.json"
        chapter_file.write_text(json.dumps(chapter, ensure_ascii=False), encoding="utf-8")

        result = extract_pipeline_request("/books/al-kafi:1:1:1:1", data_dir=str(tmp_path))
        assert result is not None
        assert result.existing_narrator_chain is not None
        assert "أَبُو جَعْفَرٍ" in result.existing_narrator_chain
        assert result.book_name == "al-kafi"
        assert result.hadith_number == 1

    def test_missing_chapter_returns_none(self, tmp_path):
        result = extract_pipeline_request("/books/nonexistent:1:1", data_dir=str(tmp_path))
        assert result is None


# ===================================================================
# Request JSONL writing tests
# ===================================================================

class TestWriteRequestJsonl:
    def test_writes_valid_jsonl(self, tmp_path):
        requests = [
            PipelineRequest(
                verse_path="/books/quran:1:1",
                arabic_text="بسم الله",
                english_text="In the name of God",
                book_name="quran",
                chapter_title="Al-Fatiha",
            ),
        ]
        output_path = str(tmp_path / "requests.jsonl")
        write_request_jsonl(requests, output_path)

        assert os.path.exists(output_path)
        with open(output_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1

        entry = json.loads(lines[0])
        assert entry["custom_id"] == "quran:1:1"
        assert entry["params"]["model"] == "claude-opus-4-6-20260205"
        assert entry["params"]["temperature"] == 0.5
        assert "بسم الله" in entry["params"]["messages"][0]["content"]


# ===================================================================
# Cost estimation tests
# ===================================================================

class TestEstimateCost:
    def test_estimate_returns_required_fields(self):
        cost = estimate_cost(100)
        assert "generation" in cost
        assert "validation" in cost
        assert "regeneration" in cost
        assert "total_cost_usd" in cost
        assert cost["num_verses"] == 100

    def test_estimate_scales_with_verses(self):
        cost_100 = estimate_cost(100)
        cost_1000 = estimate_cost(1000)
        assert cost_1000["total_cost_usd"] > cost_100["total_cost_usd"]

    def test_full_corpus_estimate(self):
        cost = estimate_cost(46857)
        # Should be around $4,400 (11 languages, root/is_proper_noun deferred to word dict)
        assert 3000 < cost["total_cost_usd"] < 5500


# ===================================================================
# Validate directory tests
# ===================================================================

class TestValidateDirectory:
    def test_validate_valid_files(self, tmp_path):
        result = _make_valid_result()
        filepath = tmp_path / "sample1.json"
        filepath.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")

        report = validate_directory(str(tmp_path))
        assert report["total"] == 1
        assert report["passed"] == 1
        assert report["failed"] == 0

    def test_validate_invalid_files(self, tmp_path):
        result = _make_valid_result(content_type="bad_type")
        filepath = tmp_path / "bad.json"
        filepath.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")

        report = validate_directory(str(tmp_path))
        assert report["total"] == 1
        assert report["failed"] == 1
        assert "bad.json" in report["errors_by_file"]

    def test_validate_missing_directory(self):
        report = validate_directory("/nonexistent/path")
        assert report["total"] == 0

    def test_validate_with_wrapper(self, tmp_path):
        """Files with a wrapper containing 'result' key should be unwrapped."""
        inner_result = _make_valid_result()
        wrapped = {
            "verse_path": "/books/quran:1:1",
            "result": inner_result,
        }
        filepath = tmp_path / "wrapped.json"
        filepath.write_text(json.dumps(wrapped, ensure_ascii=False), encoding="utf-8")

        report = validate_directory(str(tmp_path))
        assert report["passed"] == 1


# ===================================================================
# Validation tests — chunks
# ===================================================================

class TestValidateChunks:
    def test_valid_single_chunk(self):
        result = _make_valid_result()
        errors = validate_result(result)
        assert not any("chunks" in e for e in errors)

    def test_valid_multi_chunk(self):
        result = _make_valid_result_with_chain()
        errors = validate_result(result)
        assert not any("chunks" in e for e in errors)

    def test_missing_chunks_field(self):
        result = _make_valid_result()
        del result["chunks"]
        errors = validate_result(result)
        assert any("missing required field" in e and "chunks" in e for e in errors)

    def test_empty_chunks_array(self):
        result = _make_valid_result(chunks=[])
        errors = validate_result(result)
        assert any("at least 1 entry" in e for e in errors)

    def test_missing_chunk_field(self):
        result = _make_valid_result()
        del result["chunks"][0]["chunk_type"]
        errors = validate_result(result)
        assert any("chunks[0] missing field: chunk_type" in e for e in errors)

    def test_invalid_chunk_type(self):
        result = _make_valid_result()
        result["chunks"][0]["chunk_type"] = "paragraph"
        errors = validate_result(result)
        assert any("invalid chunk_type" in e for e in errors)

    def test_word_start_not_zero(self):
        result = _make_valid_result()
        result["chunks"][0]["word_start"] = 1
        result["chunks"][0]["word_end"] = 2
        errors = validate_result(result)
        assert any("word_start must be 0" in e for e in errors)

    def test_word_end_exceeds_word_count(self):
        result = _make_valid_result()
        result["chunks"][0]["word_end"] = 99
        errors = validate_result(result)
        assert any("exceeds word_analysis length" in e for e in errors)

    def test_word_end_not_greater_than_word_start(self):
        result = _make_valid_result()
        result["chunks"][0]["word_start"] = 0
        result["chunks"][0]["word_end"] = 0
        errors = validate_result(result)
        assert any("must be greater than word_start" in e for e in errors)

    def test_non_sequential_chunks(self):
        result = _make_valid_result_with_chain()
        result["chunks"][1]["word_start"] = 0  # gap: doesn't continue from chunk[0].word_end
        errors = validate_result(result)
        assert any("must equal" in e for e in errors)

    def test_last_chunk_coverage(self):
        result = _make_valid_result()
        result["chunks"][0]["word_end"] = 1  # doesn't cover all 2 words
        errors = validate_result(result)
        assert any("must equal" in e and "word_analysis length" in e for e in errors)

    def test_missing_chunk_language(self):
        result = _make_valid_result()
        del result["chunks"][0]["translations"]["zh"]
        errors = validate_result(result)
        assert any("chunks[0] translations missing languages" in e for e in errors)

    def test_chunk_translation_not_string(self):
        result = _make_valid_result()
        result["chunks"][0]["translations"]["en"] = {"text": "not a plain string"}
        errors = validate_result(result)
        assert any("chunks[0] translations.en must be string" in e for e in errors)

    def test_chunk_translation_length_phase4_silent_swallow(self):
        """Catches the Phase-4 silent-partial-output failure mode: a 1-word
        EN translation for a 30-word AR chunk. The empty-string check above
        only catches fully-empty translations; this catches truncations."""
        result = _make_v4_result()
        # Replace chunks[0] with a 30-word ar chunk and a 1-word en
        long_ar = " ".join(["كَلِمَةٌ"] * 30)
        result["chunks"][0]["arabic_text"] = long_ar
        result["chunks"][0]["word_end"] = 30
        result["chunks"][0]["translations"]["en"] = "yes"
        errors = validate_result(result)
        assert any("en translation length out of bounds" in e for e in errors), (
            f"Expected length-bound error, got: {errors}"
        )

    def test_chunk_translation_length_short_chunk_lenient(self):
        """Short chunks get loose bounds because honorific expansion
        legitimately inflates English. A 4-word AR chunk → 20-word EN
        (5x ratio) should NOT flag because 5x is within the 8x upper
        bound for short chunks."""
        result = _make_v4_result()
        result["chunks"][0]["arabic_text"] = "أَبِالنَّبِيِّ أَمْ بِالْوَصِيِّ تُكَذِّبَانِ"
        result["chunks"][0]["word_end"] = 4
        result["chunks"][0]["translations"]["en"] = (
            "Is it with the Prophet (peace be upon him and his family) "
            "or with the Successor (peace be upon him) that you both deny?"
        )
        errors = validate_result(result)
        # Should not include a length-bound error for this chunk
        assert not any("chunks[0] en translation length out of bounds" in e for e in errors), (
            f"Short-chunk honorific expansion should be tolerated: {errors}"
        )

    def test_chunk_type_body_starts_with_chain_verb(self):
        """Catches LLM mis-typing: a body chunk that starts with a chain
        transmission verb (حدثني/حدثنا/etc) should have been typed isnad."""
        result = _make_v4_result()
        # Add a second chunk that's mis-typed as body but starts with a chain verb
        result["chunks"] = [
            dict(result["chunks"][0]),
            {
                "chunk_type": "body",
                "arabic_text": "حَدَّثَنِي زُرَارَةُ بْنُ أَعْيَنَ قَالَ",
                "word_start": result["chunks"][0]["word_end"],
                "word_end": result["chunks"][0]["word_end"] + 4,
                "translations": {lang: f"Body text {lang}" for lang in VALID_LANGUAGE_KEYS},
            },
        ]
        errors = validate_result(result)
        assert any("starts with chain transmission verb" in e for e in errors), (
            f"Expected chain-opener error on body chunk: {errors}"
        )

    def test_chunk_type_body_with_wa_prefix_chain_verb(self):
        """Wa- prefix should be stripped before checking — 'وحدثني' should
        flag the same as 'حدثني'."""
        result = _make_v4_result()
        result["chunks"] = [
            dict(result["chunks"][0]),
            {
                "chunk_type": "body",
                "arabic_text": "وَحَدَّثَنِي زُرَارَةُ بْنُ أَعْيَنَ قَالَ",
                "word_start": result["chunks"][0]["word_end"],
                "word_end": result["chunks"][0]["word_end"] + 4,
                "translations": {lang: f"Body text {lang}" for lang in VALID_LANGUAGE_KEYS},
            },
        ]
        errors = validate_result(result)
        assert any("starts with chain transmission verb" in e for e in errors)

    def test_chunk_type_isnad_with_chain_verb_does_not_flag(self):
        """The chain-verb check fires only on non-isnad chunks. An isnad
        chunk starting with حَدَّثَنِي is normal."""
        result = _make_v4_result()
        result["chunks"][0]["chunk_type"] = "isnad"
        result["chunks"][0]["arabic_text"] = "حَدَّثَنِي زُرَارَةُ بْنُ أَعْيَنَ قَالَ"
        result["chunks"][0]["word_end"] = 4
        # has_chain must agree with isnad chunk presence
        result["isnad_matn"]["has_chain"] = True
        result["isnad_matn"]["narrators"] = [{
            "name_ar": "زُرَارَةُ بْنُ أَعْيَنَ", "name_en": "Zurara b. A'yan",
            "role": "narrator", "position": 1,
            "identity_confidence": "definite", "ambiguity_note": None,
        }]
        errors = validate_result(result)
        assert not any("starts with chain transmission verb" in e for e in errors)

    def test_isnad_chunk_without_narrator_surface_flags(self):
        """Catches narrator/chunk disagreement: chunk_type='isnad' but the
        narrators[] list contains names that don't appear in the chunk text.
        Real-world LLM error from the corpus measurement."""
        result = _make_v4_result()
        result["chunks"][0]["chunk_type"] = "isnad"
        result["chunks"][0]["arabic_text"] = "زُرَارَةُ بْنُ أَعْيَنَ"
        result["chunks"][0]["word_end"] = 3
        result["isnad_matn"]["has_chain"] = True
        # narrators[] has a different name than the chunk content
        result["isnad_matn"]["narrators"] = [{
            "name_ar": "ابْنُ بُكَيْرٍ", "name_en": "Ibn Bukayr",
            "role": "narrator", "position": 1,
            "identity_confidence": "definite", "ambiguity_note": None,
        }]
        errors = validate_result(result)
        assert any("contains no narrator surface" in e for e in errors), (
            f"Expected narrator-surface error: {errors}"
        )

    def test_isnad_chunk_with_matching_narrator_passes(self):
        """The narrator-surface check should accept a chunk whose text
        contains a normalized substring of any narrator's name_ar."""
        result = _make_v4_result()
        result["chunks"][0]["chunk_type"] = "isnad"
        result["chunks"][0]["arabic_text"] = "عَنْ زُرَارَةَ بْنِ أَعْيَنَ قَالَ"
        result["chunks"][0]["word_end"] = 4
        result["isnad_matn"]["has_chain"] = True
        result["isnad_matn"]["narrators"] = [{
            "name_ar": "زُرَارَةَ بْنِ أَعْيَنَ", "name_en": "Zurara b. A'yan",
            "role": "narrator", "position": 1,
            "identity_confidence": "definite", "ambiguity_note": None,
        }]
        errors = validate_result(result)
        assert not any("contains no narrator surface" in e for e in errors), (
            f"Should accept matching narrator: {errors}"
        )

    def test_isnad_chunk_alif_variant_normalization(self):
        """Alif-waṣla in chunk text vs plain alif in narrator name should
        normalize to a match — common pattern in v4 LLM output."""
        result = _make_v4_result()
        result["chunks"][0]["chunk_type"] = "isnad"
        # ٱلْحَسَنُ in chunk vs الحسن in narrator
        result["chunks"][0]["arabic_text"] = "ٱلْحَسَنُ بْنُ سَعِيدٍ"
        result["chunks"][0]["word_end"] = 3
        result["isnad_matn"]["has_chain"] = True
        result["isnad_matn"]["narrators"] = [{
            "name_ar": "الحَسَنُ بْنُ سَعِيدٍ", "name_en": "al-Hasan b. Sa'id",
            "role": "narrator", "position": 1,
            "identity_confidence": "definite", "ambiguity_note": None,
        }]
        errors = validate_result(result)
        assert not any("contains no narrator surface" in e for e in errors), (
            f"Alif normalization should let chunk and narrator match: {errors}"
        )

    def test_chunk_translation_empty_string(self):
        # Concrete Phase 4 batch-failure footprint: a verse that saved with
        # English present but other-language chunk translations as empty
        # strings (tahdhib-al-ahkam_10_14_20 in May 3 2026 retry).
        result = _make_valid_result()
        result["chunks"][0]["translations"]["fa"] = ""
        errors = validate_result(result)
        assert any("chunks[0] translations.fa is empty string" in e for e in errors)


# ===================================================================
# Enum constant completeness tests
# ===================================================================

class TestEnumConstants:
    def test_valid_pos_tags_complete(self):
        expected = {"N", "V", "ADJ", "ADV", "PREP", "CONJ", "PRON", "DET",
                    "PART", "INTJ", "REL", "DEM", "NEG", "COND", "INTERR"}
        assert VALID_POS_TAGS == expected

    def test_valid_tags_complete(self):
        assert len(VALID_TAGS) == 14

    def test_valid_content_types_complete(self):
        assert len(VALID_CONTENT_TYPES) == 12

    def test_valid_language_keys_complete(self):
        assert VALID_LANGUAGE_KEYS == {"en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"}

    def test_valid_narrator_roles_complete(self):
        assert VALID_NARRATOR_ROLES == {"narrator", "companion", "imam", "author"}

    def test_valid_identity_confidence_complete(self):
        assert VALID_IDENTITY_CONFIDENCE == {"definite", "likely", "ambiguous"}

    def test_valid_chunk_types_complete(self):
        assert VALID_CHUNK_TYPES == {"isnad", "opening", "body", "quran_quote", "closing"}

    def test_valid_phrase_categories_complete(self):
        assert VALID_PHRASE_CATEGORIES == {
            "theological_concept", "well_known_saying", "jurisprudential_term",
            "quranic_echo", "prophetic_formula",
        }

    def test_valid_topics_loaded(self):
        assert len(VALID_TOPICS) >= 80, f"Expected at least 80 topics, got {len(VALID_TOPICS)}"


# ===================================================================
# Data file loading tests — taxonomy and key phrases
# ===================================================================

class TestNewDataLoading:
    def test_load_topic_taxonomy(self):
        data = load_topic_taxonomy()
        assert data is not None
        assert "taxonomy" in data
        # Should have all 14 Level 1 categories
        assert len(data["taxonomy"]) == 14
        for category_key, category_data in data["taxonomy"].items():
            assert "en" in category_data, f"Category {category_key} missing 'en'"
            assert "ar" in category_data, f"Category {category_key} missing 'ar'"
            assert "topics" in category_data, f"Category {category_key} missing 'topics'"
            assert len(category_data["topics"]) >= 3, f"Category {category_key} has too few topics"

    def test_topic_taxonomy_topic_labels(self):
        data = load_topic_taxonomy()
        assert data is not None
        for category_key, category_data in data["taxonomy"].items():
            for topic_key, topic_data in category_data["topics"].items():
                assert "en" in topic_data, f"Topic {topic_key} in {category_key} missing 'en'"
                assert "ar" in topic_data, f"Topic {topic_key} in {category_key} missing 'ar'"

    def test_valid_topics_matches_taxonomy(self):
        """VALID_TOPICS should contain exactly the Level 2 keys from the taxonomy."""
        data = load_topic_taxonomy()
        assert data is not None
        expected_topics = set()
        for category_data in data["taxonomy"].values():
            for topic_key in category_data.get("topics", {}):
                expected_topics.add(topic_key)
        assert VALID_TOPICS == expected_topics

    def test_load_key_phrases_dictionary(self):
        data = load_key_phrases_dictionary()
        assert data is not None
        assert "phrases" in data
        assert len(data["phrases"]) >= 100, f"Expected at least 100 phrases, got {len(data['phrases'])}"

    def test_key_phrases_have_required_fields(self):
        data = load_key_phrases_dictionary()
        assert data is not None
        for i, phrase in enumerate(data["phrases"]):
            assert "phrase_ar" in phrase, f"Phrase {i} missing 'phrase_ar'"
            assert "phrase_en" in phrase, f"Phrase {i} missing 'phrase_en'"
            assert "category" in phrase, f"Phrase {i} missing 'category'"
            assert phrase["category"] in VALID_PHRASE_CATEGORIES, \
                f"Phrase {i} invalid category: {phrase['category']}"

    def test_key_phrases_are_multi_word(self):
        data = load_key_phrases_dictionary()
        assert data is not None
        for i, phrase in enumerate(data["phrases"]):
            word_count = len(phrase["phrase_ar"].strip().split())
            assert word_count >= 2, \
                f"Phrase {i} '{phrase['phrase_ar']}' is not multi-word ({word_count} words)"


# ===================================================================
# System prompt tests — new sections
# ===================================================================

class TestBuildSystemPromptNewFields:
    def test_includes_topic_taxonomy(self):
        prompt = build_system_prompt()
        assert "TOPIC TAXONOMY" in prompt
        assert "tawhid" in prompt
        assert "seeking_knowledge" in prompt

    def test_includes_key_phrases_reference(self):
        prompt = build_system_prompt()
        assert "KEY PHRASES REFERENCE" in prompt
        assert "theological_concept" in prompt

    def test_omits_taxonomy_when_none(self):
        prompt = build_system_prompt(topic_taxonomy={"taxonomy": {}})
        assert "TOPIC TAXONOMY" not in prompt

    def test_omits_phrases_when_none(self):
        prompt = build_system_prompt(key_phrases_dict={"phrases": []})
        assert "KEY PHRASES REFERENCE" not in prompt


# ===================================================================
# User message tests — new fields
# ===================================================================

class TestBuildUserMessageNewFields:
    def test_includes_topics_field(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
        )
        msg = build_user_message(req)
        assert '"topics"' in msg
        assert "Level 2 topic keys" in msg

    def test_includes_key_phrases_field(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
        )
        msg = build_user_message(req)
        assert '"key_phrases"' in msg
        assert "multi-word" in msg.lower()

    def test_similar_content_hints_removed_in_v4(self):
        """v4: similar_content_hints field removed from prompt."""
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
        )
        msg = build_user_message(req)
        assert '"similar_content_hints"' not in msg


# ===================================================================
# Validation tests — topics
# ===================================================================

class TestValidateTopics:
    def test_valid_topics_passes(self):
        result = _make_valid_result(topics=["tawhid", "seeking_knowledge"])
        errors = validate_result(result)
        assert not any("topics" in e for e in errors)

    def test_invalid_topic(self):
        result = _make_valid_result(topics=["tawhid", "made_up_topic"])
        errors = validate_result(result)
        assert any("invalid topic" in e for e in errors)

    def test_topics_too_many(self):
        result = _make_valid_result(topics=["tawhid", "patience", "honesty", "humility", "seeking_knowledge", "prayer_importance"])
        errors = validate_result(result)
        assert any("1-5 items" in e for e in errors)

    def test_topics_five_is_valid(self):
        """5 topics should be valid (expanded from 3)."""
        result = _make_valid_result(topics=["tawhid", "patience", "honesty", "humility", "seeking_knowledge"])
        errors = validate_result(result)
        assert not any("1-5 items" in e for e in errors)

    def test_topics_empty(self):
        result = _make_valid_result(topics=[])
        errors = validate_result(result)
        assert any("1-5 items" in e for e in errors)

    def test_topics_not_array(self):
        result = _make_valid_result(topics="tawhid")
        errors = validate_result(result)
        assert any("topics must be array" in e for e in errors)

    def test_result_without_topics_passes(self):
        """topics is optional — old results without it should still pass."""
        result = _make_valid_result()
        # Remove topics if present
        result.pop("topics", None)
        errors = validate_result(result)
        assert not any("topics" in e for e in errors)


# ===================================================================
# Validation tests — key_phrases
# ===================================================================

class TestValidateKeyPhrases:
    def test_valid_key_phrases_passes(self):
        result = _make_valid_result(key_phrases=[
            {
                "phrase_ar": "طَلَبُ الْعِلْمِ فَرِيضَةٌ",
                "phrase_en": "Seeking knowledge is an obligation",
                "category": "well_known_saying",
            }
        ])
        errors = validate_result(result)
        assert not any("key_phrases" in e for e in errors)

    def test_empty_key_phrases_passes(self):
        result = _make_valid_result(key_phrases=[])
        errors = validate_result(result)
        assert not any("key_phrases" in e for e in errors)

    def test_key_phrases_invalid_category(self):
        result = _make_valid_result(key_phrases=[
            {
                "phrase_ar": "طَلَبُ الْعِلْمِ",
                "phrase_en": "Seeking knowledge",
                "category": "invented_category",
            }
        ])
        errors = validate_result(result)
        assert any("invalid category" in e for e in errors)

    def test_key_phrases_missing_field(self):
        result = _make_valid_result(key_phrases=[
            {"phrase_ar": "طَلَبُ الْعِلْمِ", "phrase_en": "Seeking knowledge"}
        ])
        errors = validate_result(result)
        assert any("missing field: category" in e for e in errors)

    def test_key_phrases_single_word_rejected(self):
        result = _make_valid_result(key_phrases=[
            {
                "phrase_ar": "التَّوْحِيدُ",
                "phrase_en": "Monotheism",
                "category": "theological_concept",
            }
        ])
        errors = validate_result(result)
        assert any("multi-word" in e for e in errors)

    def test_key_phrases_too_many(self):
        result = _make_valid_result(key_phrases=[
            {"phrase_ar": f"عبارة {i} كلمة", "phrase_en": f"Phrase {i}", "category": "well_known_saying"}
            for i in range(6)
        ])
        errors = validate_result(result)
        assert any("0-5 items" in e for e in errors)

    def test_result_without_key_phrases_passes(self):
        """key_phrases is optional — old results without it should still pass."""
        result = _make_valid_result()
        result.pop("key_phrases", None)
        errors = validate_result(result)
        assert not any("key_phrases" in e for e in errors)


# ===================================================================
# Validation tests — similar_content_hints
# ===================================================================

class TestValidateSimilarContentHints:
    def test_valid_hints_passes(self):
        result = _make_valid_result(similar_content_hints=[
            {
                "description": "Similar hadith about seeking knowledge in other chapters",
                "theme": "seeking_knowledge",
            }
        ])
        errors = validate_result(result)
        assert not any("similar_content_hints" in e for e in errors)

    def test_empty_hints_passes(self):
        result = _make_valid_result(similar_content_hints=[])
        errors = validate_result(result)
        assert not any("similar_content_hints" in e for e in errors)

    def test_hints_missing_field(self):
        result = _make_valid_result(similar_content_hints=[
            {"description": "Some hint"}
        ])
        errors = validate_result(result)
        assert any("missing field: theme" in e for e in errors)

    def test_hints_too_many(self):
        result = _make_valid_result(similar_content_hints=[
            {"description": f"Hint {i}", "theme": f"theme_{i}"} for i in range(4)
        ])
        errors = validate_result(result)
        assert any("0-3 items" in e for e in errors)

    def test_result_without_hints_passes(self):
        """similar_content_hints is optional — old results without it should still pass."""
        result = _make_valid_result()
        result.pop("similar_content_hints", None)
        errors = validate_result(result)
        assert not any("similar_content_hints" in e for e in errors)


# ===================================================================
# Wrapper validation tests
# ===================================================================

class TestValidateWrapper:
    def _make_valid_wrapper(self, **overrides):
        wrapper = {
            "verse_path": "/books/al-kafi:1:1:1:1",
            "ai_attribution": {
                "model": "claude-opus-4-6-20260205",
                "generated_date": "2026-02-27",
                "pipeline_version": "2.0.0",
                "generation_method": "claude_code_direct",
            },
            "generation_attempts": 1,
            "result": _make_valid_result(),
        }
        wrapper.update(overrides)
        return wrapper

    def test_valid_wrapper_passes(self):
        errors = validate_wrapper(self._make_valid_wrapper())
        assert errors == []

    def test_missing_verse_path(self):
        wrapper = self._make_valid_wrapper()
        del wrapper["verse_path"]
        errors = validate_wrapper(wrapper)
        assert any("verse_path" in e for e in errors)

    def test_missing_ai_attribution_field(self):
        wrapper = self._make_valid_wrapper()
        del wrapper["ai_attribution"]["model"]
        errors = validate_wrapper(wrapper)
        assert any("model" in e for e in errors)

    def test_generation_attempts_exceeds_max(self):
        wrapper = self._make_valid_wrapper(generation_attempts=MAX_GENERATION_ATTEMPTS + 1)
        errors = validate_wrapper(wrapper)
        assert any("exceeds max" in e for e in errors)

    def test_generation_attempts_at_max_passes(self):
        wrapper = self._make_valid_wrapper(generation_attempts=MAX_GENERATION_ATTEMPTS)
        errors = validate_wrapper(wrapper)
        assert not any("generation_attempts" in e for e in errors)

    def test_wrapper_without_generation_attempts_passes(self):
        """generation_attempts is optional for backward compatibility."""
        wrapper = self._make_valid_wrapper()
        del wrapper["generation_attempts"]
        errors = validate_wrapper(wrapper)
        assert not any("generation_attempts" in e for e in errors)


# ===================================================================
# key_terms dict validation tests
# ===================================================================

class TestValidateKeyTerms:
    def test_key_terms_must_be_dict(self):
        """key_terms as a list (wrong schema) triggers validation error."""
        result = _make_valid_result()
        result["translations"]["en"]["key_terms"] = [
            {"term": "Allah", "definition": "God"}
        ]
        errors = validate_result(result)
        assert any("key_terms must be dict" in e for e in errors)

    def test_key_terms_keys_must_have_arabic(self):
        """key_terms with English transliteration keys triggers validation error."""
        result = _make_valid_result()
        result["translations"]["en"]["key_terms"] = {
            "al-ilm": "Knowledge",
            "tawhid": "Monotheism",
        }
        errors = validate_result(result)
        assert any("has no Arabic characters" in e for e in errors)

    def test_key_terms_valid_dict_passes(self):
        """key_terms as dict with Arabic keys passes validation."""
        result = _make_valid_result()
        # Already has valid key_terms from _make_valid_result
        errors = validate_result(result)
        assert not any("key_terms must be dict" in e for e in errors)
        assert not any("has no Arabic characters" in e for e in errors)


# ===================================================================
# diacritics cross-check tests
# ===================================================================

class TestValidateDiacriticsCrossCheck:
    def test_diacritics_status_added_with_changes_is_error(self):
        """status='added' with non-empty diacritics_changes triggers error."""
        result = _make_valid_result()
        result["diacritics_status"] = "added"
        result["diacritics_changes"] = [
            {"word": "بسم", "original": "بسم", "corrected": "بِسْمِ"}
        ]
        errors = validate_result(result)
        assert any("diacritics_status is 'added'" in e and "non-empty" in e for e in errors)

    def test_diacritics_status_validated_with_changes_is_error(self):
        """status='validated' with non-empty diacritics_changes triggers error."""
        result = _make_valid_result()
        result["diacritics_status"] = "validated"
        result["diacritics_changes"] = [
            {"word": "الله", "original": "الله", "corrected": "اللَّهِ"}
        ]
        errors = validate_result(result)
        assert any("diacritics_status is 'validated'" in e and "non-empty" in e for e in errors)

    def test_diacritics_status_completed_with_changes_is_ok(self):
        """status='completed' with non-empty changes is valid (no error)."""
        result = _make_valid_result()
        result["diacritics_status"] = "completed"
        result["diacritics_changes"] = [
            {"word": "بسم", "original": "بسم", "corrected": "بِسْمِ"}
        ]
        errors = validate_result(result)
        assert not any("diacritics_status" in e and "non-empty" in e for e in errors)

    def test_diacritics_status_added_with_empty_changes_is_ok(self):
        """status='added' with empty diacritics_changes is valid."""
        result = _make_valid_result()
        result["diacritics_status"] = "added"
        result["diacritics_changes"] = []
        errors = validate_result(result)
        assert not any("diacritics_status" in e and "non-empty" in e for e in errors)


# ===================================================================
# Strip/reconstruct redundant fields tests
# ===================================================================

class TestStripRedundantFields:
    """Tests for strip_redundant_fields() and reconstruct_fields()."""

    def test_strip_removes_expected_keys(self):
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        assert "diacritized_text" not in stripped
        for chunk in stripped["chunks"]:
            assert "arabic_text" not in chunk
        for lang, obj in stripped["translations"].items():
            assert "text" not in obj
        # isnad_matn.isnad_ar/matn_ar are kept (not reliably reconstructable)
        assert "isnad_ar" in stripped["isnad_matn"]
        assert "matn_ar" in stripped["isnad_matn"]

    def test_strip_preserves_essential_data(self):
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        # word_analysis, chunks (minus arabic_text), translations (minus text) preserved
        assert len(stripped["word_analysis"]) == len(result["word_analysis"])
        assert len(stripped["chunks"]) == len(result["chunks"])
        assert stripped["chunks"][0]["word_start"] == 0
        assert stripped["chunks"][0]["word_end"] == 2
        assert stripped["chunks"][0]["chunk_type"] == "body"
        assert set(stripped["translations"].keys()) == set(result["translations"].keys())
        for lang in VALID_LANGUAGE_KEYS:
            assert "summary" in stripped["translations"][lang]
            assert "key_terms" in stripped["translations"][lang]
            assert "seo_question" in stripped["translations"][lang]
        assert stripped["diacritics_status"] == "validated"
        assert stripped["tags"] == result["tags"]
        assert stripped["content_type"] == result["content_type"]

    def test_strip_does_not_mutate_original(self):
        result = _make_valid_result()
        original_text = result["diacritized_text"]
        strip_redundant_fields(result)
        assert result["diacritized_text"] == original_text

    def test_reconstruct_restores_diacritized_text(self):
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        reconstructed = reconstruct_fields(stripped)
        expected = " ".join(w["word"] for w in result["word_analysis"])
        assert reconstructed["diacritized_text"] == expected

    def test_reconstruct_restores_chunk_arabic(self):
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        reconstructed = reconstruct_fields(stripped)
        for chunk in reconstructed["chunks"]:
            assert "arabic_text" in chunk
            assert isinstance(chunk["arabic_text"], str)
            assert len(chunk["arabic_text"]) > 0

    def test_reconstruct_restores_translation_text(self):
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        reconstructed = reconstruct_fields(stripped)
        for lang in VALID_LANGUAGE_KEYS:
            assert "text" in reconstructed["translations"][lang]
            assert isinstance(reconstructed["translations"][lang]["text"], str)

    def test_reconstruct_restores_isnad_matn_fallback(self):
        """If isnad_ar/matn_ar are manually removed, reconstruct restores them."""
        result = _make_valid_result_with_chain()
        stripped = strip_redundant_fields(result)
        # Manually remove isnad_ar/matn_ar to test fallback reconstruction
        del stripped["isnad_matn"]["isnad_ar"]
        del stripped["isnad_matn"]["matn_ar"]
        reconstructed = reconstruct_fields(stripped)
        assert "isnad_ar" in reconstructed["isnad_matn"]
        assert "matn_ar" in reconstructed["isnad_matn"]
        # isnad_ar should be reconstructed from isnad-typed chunks
        assert len(reconstructed["isnad_matn"]["isnad_ar"]) > 0
        # matn_ar should be reconstructed from non-isnad chunks
        assert len(reconstructed["isnad_matn"]["matn_ar"]) > 0

    def test_strip_preserves_isnad_matn_fields(self):
        """strip_redundant_fields() keeps isnad_ar/matn_ar intact."""
        result = _make_valid_result_with_chain()
        stripped = strip_redundant_fields(result)
        assert stripped["isnad_matn"]["isnad_ar"] == result["isnad_matn"]["isnad_ar"]
        assert stripped["isnad_matn"]["matn_ar"] == result["isnad_matn"]["matn_ar"]

    def test_reconstruct_chinese_no_spaces(self):
        """Chinese translations should be joined without spaces."""
        result = _make_valid_result()
        # Give chunks Chinese text without spaces
        result["chunks"] = [
            {
                "chunk_type": "body",
                "arabic_text": result["word_analysis"][0]["word"],
                "word_start": 0,
                "word_end": 1,
                "translations": {lang: (f"前半" if lang == "zh" else f"Part1 ({lang})") for lang in VALID_LANGUAGE_KEYS},
            },
            {
                "chunk_type": "body",
                "arabic_text": result["word_analysis"][1]["word"],
                "word_start": 1,
                "word_end": 2,
                "translations": {lang: (f"后半" if lang == "zh" else f"Part2 ({lang})") for lang in VALID_LANGUAGE_KEYS},
            },
        ]
        stripped = strip_redundant_fields(result)
        reconstructed = reconstruct_fields(stripped)
        # Chinese: no space
        assert reconstructed["translations"]["zh"]["text"] == "前半后半"
        # English: space-joined
        assert reconstructed["translations"]["en"]["text"] == "Part1 (en) Part2 (en)"

    def test_strip_reconstruct_roundtrip(self):
        """Strip then reconstruct should produce a result that validates."""
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        reconstructed = reconstruct_fields(stripped)
        errors = validate_result(reconstructed)
        assert errors == [], f"Round-trip validation failed: {errors}"

    def test_strip_reconstruct_roundtrip_with_chain(self):
        """Round-trip works for results with narrator chains."""
        result = _make_valid_result_with_chain()
        stripped = strip_redundant_fields(result)
        reconstructed = reconstruct_fields(stripped)
        errors = validate_result(reconstructed)
        assert errors == [], f"Round-trip validation failed: {errors}"

    def test_validate_accepts_stripped_format(self):
        """validate_result() should auto-reconstruct and pass on stripped input."""
        result = _make_valid_result()
        stripped = strip_redundant_fields(result)
        # stripped is missing diacritized_text, etc. — validate should still pass
        errors = validate_result(stripped)
        assert errors == [], f"Stripped format validation failed: {errors}"

    def test_validate_accepts_stripped_format_with_chain(self):
        result = _make_valid_result_with_chain()
        stripped = strip_redundant_fields(result)
        errors = validate_result(stripped)
        assert errors == [], f"Stripped chain format validation failed: {errors}"

    def test_reconstruct_idempotent_on_full_format(self):
        """reconstruct_fields on full format should not change anything."""
        result = _make_valid_result()
        reconstructed = reconstruct_fields(result)
        assert reconstructed["diacritized_text"] == result["diacritized_text"]
        for i, chunk in enumerate(reconstructed["chunks"]):
            assert chunk["arabic_text"] == result["chunks"][i]["arabic_text"]
        for lang in VALID_LANGUAGE_KEYS:
            assert reconstructed["translations"][lang]["text"] == result["translations"][lang]["text"]


# ===================================================================
# Tests for per-verse stats infrastructure
# ===================================================================


class TestWriteVerseStats:
    """Tests for write_verse_stats()."""

    def test_writes_file_to_stats_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stats = {"verse_path": "/books/al-kafi:1:1:1:1", "validation_passed": True}
            path = write_verse_stats("al-kafi_1_1_1_1", stats, stats_dir=tmpdir)
            assert os.path.isfile(path)
            assert path.endswith("al-kafi_1_1_1_1.stats.json")
            with open(path, encoding="utf-8") as f:
                loaded = json.load(f)
            assert loaded["verse_id"] == "al-kafi_1_1_1_1"
            assert loaded["validation_passed"] is True
            assert "stats_recorded_at" in loaded

    def test_creates_stats_dir_if_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "sub", "stats")
            write_verse_stats("quran_1_1", {"x": 1}, stats_dir=nested_dir)
            assert os.path.isdir(nested_dir)

    def test_preserves_existing_verse_id(self):
        """If stats_dict already has verse_id, write_verse_stats should not overwrite it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stats = {"verse_id": "already_set"}
            path = write_verse_stats("al-kafi_1_1_1_1", stats, stats_dir=tmpdir)
            with open(path, encoding="utf-8") as f:
                loaded = json.load(f)
            # setdefault does NOT overwrite existing keys
            assert loaded["verse_id"] == "already_set"

    def test_overwrites_existing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_verse_stats("v1", {"attempt": 1}, stats_dir=tmpdir)
            write_verse_stats("v1", {"attempt": 2}, stats_dir=tmpdir)
            path = os.path.join(tmpdir, "v1.stats.json")
            with open(path, encoding="utf-8") as f:
                loaded = json.load(f)
            assert loaded["attempt"] == 2


class TestMergeStats:
    """Tests for merge_stats()."""

    def test_merges_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stats_dir = os.path.join(tmpdir, "stats")
            output_path = os.path.join(tmpdir, "generation_stats.json")

            write_verse_stats("v1", {"score": 1}, stats_dir=stats_dir)
            write_verse_stats("v2", {"score": 2}, stats_dir=stats_dir)
            write_verse_stats("v3", {"score": 3}, stats_dir=stats_dir)

            result = merge_stats(stats_dir=stats_dir, output_path=output_path)
            assert result["total_hadiths"] == 3
            assert "v1" in result["stats"]
            assert "v2" in result["stats"]
            assert "v3" in result["stats"]
            assert result["stats"]["v2"]["score"] == 2

            # Verify file was written
            with open(output_path, encoding="utf-8") as f:
                on_disk = json.load(f)
            assert on_disk["total_hadiths"] == 3

    def test_empty_stats_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stats_dir = os.path.join(tmpdir, "stats")
            os.makedirs(stats_dir)
            output_path = os.path.join(tmpdir, "generation_stats.json")

            result = merge_stats(stats_dir=stats_dir, output_path=output_path)
            assert result["total_hadiths"] == 0
            assert result["stats"] == {}

    def test_missing_stats_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stats_dir = os.path.join(tmpdir, "nonexistent")
            output_path = os.path.join(tmpdir, "generation_stats.json")

            result = merge_stats(stats_dir=stats_dir, output_path=output_path)
            assert result["total_hadiths"] == 0

    def test_skips_non_stats_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stats_dir = os.path.join(tmpdir, "stats")
            os.makedirs(stats_dir)
            output_path = os.path.join(tmpdir, "generation_stats.json")

            write_verse_stats("v1", {"score": 1}, stats_dir=stats_dir)
            # Write a non-stats file
            with open(os.path.join(stats_dir, "readme.txt"), "w") as f:
                f.write("not stats")
            with open(os.path.join(stats_dir, "other.json"), "w") as f:
                json.dump({"x": 1}, f)

            result = merge_stats(stats_dir=stats_dir, output_path=output_path)
            assert result["total_hadiths"] == 1

    def test_handles_corrupt_stats_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stats_dir = os.path.join(tmpdir, "stats")
            os.makedirs(stats_dir)
            output_path = os.path.join(tmpdir, "generation_stats.json")

            write_verse_stats("v1", {"score": 1}, stats_dir=stats_dir)
            # Write a corrupt stats file
            with open(os.path.join(stats_dir, "bad.stats.json"), "w") as f:
                f.write("{invalid json")

            result = merge_stats(stats_dir=stats_dir, output_path=output_path)
            assert result["total_hadiths"] == 1


class TestComputeRemaining:
    """Tests for compute_remaining()."""

    def test_all_remaining_when_no_responses(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.json")
            responses_dir = os.path.join(tmpdir, "responses")
            os.makedirs(responses_dir)

            manifest = {
                "total": 3,
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "book": "al-kafi"},
                    {"path": "/books/al-kafi:1:1:1:2", "book": "al-kafi"},
                    {"path": "/books/quran:1:1", "book": "quran"},
                ],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest, f)

            remaining = compute_remaining(manifest_path=manifest_path, responses_dir=responses_dir)
            assert len(remaining) == 3

    def test_skips_existing_responses(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.json")
            responses_dir = os.path.join(tmpdir, "responses")
            os.makedirs(responses_dir)

            manifest = {
                "total": 3,
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "book": "al-kafi"},
                    {"path": "/books/al-kafi:1:1:1:2", "book": "al-kafi"},
                    {"path": "/books/quran:1:1", "book": "quran"},
                ],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest, f)

            # Create response for first verse
            with open(os.path.join(responses_dir, "al-kafi_1_1_1_1.json"), "w") as f:
                json.dump({"result": {}}, f)

            remaining = compute_remaining(manifest_path=manifest_path, responses_dir=responses_dir)
            assert len(remaining) == 2
            paths = [e["path"] for e in remaining]
            assert "/books/al-kafi:1:1:1:1" not in paths
            assert "/books/al-kafi:1:1:1:2" in paths
            assert "/books/quran:1:1" in paths

    def test_empty_when_all_done(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.json")
            responses_dir = os.path.join(tmpdir, "responses")
            os.makedirs(responses_dir)

            manifest = {
                "total": 2,
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "book": "al-kafi"},
                    {"path": "/books/quran:1:1", "book": "quran"},
                ],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest, f)

            with open(os.path.join(responses_dir, "al-kafi_1_1_1_1.json"), "w") as f:
                json.dump({}, f)
            with open(os.path.join(responses_dir, "quran_1_1.json"), "w") as f:
                json.dump({}, f)

            remaining = compute_remaining(manifest_path=manifest_path, responses_dir=responses_dir)
            assert len(remaining) == 0

    def test_missing_manifest_returns_empty(self):
        remaining = compute_remaining(manifest_path="/nonexistent/manifest.json")
        assert remaining == []

    def test_missing_responses_dir_returns_all(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.json")
            manifest = {
                "total": 2,
                "verses": [
                    {"path": "/books/al-kafi:1:1:1:1", "book": "al-kafi"},
                    {"path": "/books/quran:1:1", "book": "quran"},
                ],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest, f)

            remaining = compute_remaining(
                manifest_path=manifest_path,
                responses_dir=os.path.join(tmpdir, "nonexistent"),
            )
            assert len(remaining) == 2

    def test_sort_by_word_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.json")
            responses_dir = os.path.join(tmpdir, "responses")
            os.makedirs(responses_dir)

            manifest = {
                "total": 3,
                "verses": [
                    {"path": "/books/quran:1:1", "book": "quran"},
                    {"path": "/books/al-kafi:1:1:1:1:1", "book": "al-kafi"},
                    {"path": "/books/al-kafi:1:1:1:1", "book": "al-kafi"},
                ],
            }
            with open(manifest_path, "w") as f:
                json.dump(manifest, f)

            remaining = compute_remaining(
                manifest_path=manifest_path,
                responses_dir=responses_dir,
                sort_by_word_count=True,
            )
            # Should be sorted by book then path length
            assert remaining[0]["book"] == "al-kafi"
            assert remaining[-1]["book"] == "quran"


# ===================================================================
# V4 word_tags tests
# ===================================================================

def _make_v4_result(**overrides):
    """Build a minimally valid v4 pipeline result.

    Internally consistent: chunks[].arabic_text whitespace-split count
    matches chunks[].word_end - word_start, and word_tags (when present
    for legacy reasons) has the same length.
    """
    result = {
        "diacritized_text": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
        "diacritics_status": "validated",
        "diacritics_changes": [],
        "tags": ["theology", "worship"],
        "content_type": "creedal",
        "related_quran": [],
        "isnad_matn": {
            "isnad_ar": "",
            "matn_ar": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
            "has_chain": False,
            "narrators": [],
        },
        "translations": {},
        "chunks": [
            {
                "chunk_type": "body",
                "arabic_text": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
                "word_start": 0,
                "word_end": 4,
                "translations": {lang: f"Body text ({lang})" for lang in VALID_LANGUAGE_KEYS},
            },
        ],
    }
    for lang in VALID_LANGUAGE_KEYS:
        result["translations"][lang] = {
            "summary": f"Summary in {lang}",
            "key_terms": {"اللَّه": f"Allah in {lang}"},
            "seo_question": f"Question in {lang}?",
        }
    result.update(overrides)
    return result


class TestV4WordTags:
    """Tests for v4 word_tags format validation."""

    def test_valid_v4_result_passes(self):
        result = _make_v4_result()
        errors = validate_result(result)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_word_tags_invalid_pos_not_flagged(self):
        """v4 word_tags POS is no longer validated — Phase 2 places a
        placeholder "N" tag, so a POS-enum check would be tautological.
        Invalid POS values must NOT raise errors here."""
        result = _make_v4_result(word_tags=[
            ["بِسْمِ", "INVALID_POS"],
            ["اللَّهِ", "ALSO_INVALID"],
        ])
        errors = validate_result(result)
        assert not any("invalid pos" in e.lower() for e in errors), (
            f"POS should not be validated for v4 word_tags: {errors}"
        )

    def test_v4_chunk_arabic_text_diacritics_required(self):
        """v4 routes the diacritics gate through chunks[].arabic_text — the
        Phase 1 LLM canonical. An undiacritized word in chunk text must
        raise an error."""
        result = _make_v4_result()
        # Replace the chunk's arabic_text with an undiacritized word
        result["chunks"][0]["arabic_text"] = "بسم اللَّهِ"
        errors = validate_result(result)
        assert any("no diacritics" in e for e in errors), (
            f"v4 chunks-level diacritics gate should fire on undiacritized word: {errors}"
        )

    def test_legacy_word_tags_not_list_of_pairs_still_validated(self):
        """Legacy v4 wrappers persisted before #5 may still carry word_tags.
        If present, structural sanity checks still apply."""
        result = _make_v4_result(word_tags=["بِسْمِ", "N"])
        errors = validate_result(result)
        assert len(errors) > 0

    def test_v4_translations_text_optional(self):
        """v4 results don't require translations.*.text."""
        result = _make_v4_result()
        # Verify no "text" key in translations
        for lang_data in result["translations"].values():
            assert "text" not in lang_data
        errors = validate_result(result)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_v3_translations_text_still_required(self):
        """v3 results still require translations.*.text."""
        result = _make_valid_result()
        # Remove text from one language
        result["translations"]["en"].pop("text")
        errors = validate_result(result)
        assert any("text" in e for e in errors)


class TestV4StripReconstruct:
    """Tests for v4 strip/reconstruct with the new (no word_tags) format
    and legacy (word_tags-bearing) format."""

    def test_strip_keeps_chunks_arabic_text_for_new_v4(self):
        """New-format v4 (no word_tags). Strip should preserve chunks[].arabic_text
        because it's Phase 1 LLM canonical."""
        result = _make_v4_result()
        stripped = strip_redundant_fields(result)
        assert "word_tags" not in stripped
        assert "word_analysis" not in stripped
        assert stripped["chunks"][0]["arabic_text"]

    def test_strip_removes_word_analysis_for_legacy_word_tags(self):
        """Legacy v4 wrappers may carry word_tags AND a word_analysis stub
        (e.g. injected by reconstruct). Strip should remove the stub but
        keep word_tags so validate_result on legacy data still works."""
        result = _make_v4_result(word_tags=[
            ["بِسْمِ", "N"],
            ["اللَّهِ", "N"],
            ["الرَّحْمٰنِ", "N"],
            ["الرَّحِيمِ", "N"],
        ])
        result["word_analysis"] = [{"word": "بِسْمِ", "pos": "PREP"}]
        stripped = strip_redundant_fields(result)
        assert "word_tags" in stripped
        assert "word_analysis" not in stripped

    def test_reconstruct_builds_word_analysis_from_legacy_word_tags(self):
        """Legacy responses still have word_tags. reconstruct_fields should
        synthesize word_analysis from them — preserving the legacy path."""
        result = _make_v4_result(word_tags=[
            ["بِسْمِ", "PREP"],
            ["اللَّهِ", "N"],
            ["الرَّحْمٰنِ", "N"],
            ["الرَّحِيمِ", "N"],
        ])
        # Strip word_analysis if present
        result.pop("word_analysis", None)
        reconstructed = reconstruct_fields(result)
        assert "word_analysis" in reconstructed
        assert len(reconstructed["word_analysis"]) == 4
        assert reconstructed["word_analysis"][0]["word"] == "بِسْمِ"
        assert reconstructed["word_analysis"][0]["pos"] == "PREP"

    def test_reconstruct_translations_text_from_chunks(self):
        result = _make_v4_result()
        reconstructed = reconstruct_fields(result)
        for lang in VALID_LANGUAGE_KEYS:
            assert "text" in reconstructed["translations"][lang]

    # ---- Round-trip property tests (v4 only) ----
    #
    # These lock the strip/reconstruct pair as a true contract. They guard
    # against regressions when tasks #1/#2/#3 widen what gets stripped: any
    # future change that makes strip remove a field reconstruct can't rebuild
    # will fail one of these properties.
    #
    # Defining property: for every v4 result x, the pair must satisfy
    #   1. strip is idempotent: strip(strip(x)) == strip(x)
    #   2. reconstruct(strip(x)) == reconstruct(x) — strip never loses
    #      information that reconstruct can rebuild
    #   3. strip(reconstruct(strip(x))) == strip(x) — feeding a
    #      reconstructed result back through strip lands at the canonical
    #      stripped form (catches e.g. v3-mode strip bugs triggered by
    #      synthetic word_analysis stubs reconstruct injects).

    @staticmethod
    def _deep_eq(a, b):
        return json.dumps(a, sort_keys=True, ensure_ascii=False) == json.dumps(b, sort_keys=True, ensure_ascii=False)

    def test_v4_strip_is_idempotent(self):
        x = _make_v4_result()
        once = strip_redundant_fields(x)
        twice = strip_redundant_fields(once)
        assert self._deep_eq(once, twice), "strip must be idempotent on v4"

    def test_v4_reconstruct_after_strip_matches_reconstruct(self):
        """strip then reconstruct must produce the same canonical full form
        as a direct reconstruct — i.e. strip never removes anything that
        reconstruct can't put back."""
        x = _make_v4_result()
        a = reconstruct_fields(strip_redundant_fields(x))
        b = reconstruct_fields(x)
        assert self._deep_eq(a, b), (
            "reconstruct(strip(x)) must equal reconstruct(x): strip is "
            "removing data reconstruct can't rebuild"
        )

    def test_v4_strip_reconstruct_strip_round_trip(self):
        """Round-tripping a stripped v4 through reconstruct and back through
        strip must land at the same stripped form. Catches the class of bug
        where a synthetic word_analysis stub injected by reconstruct trips
        strip into v3-mode behaviour and over-strips fields v4 needs to keep."""
        x = _make_v4_result()
        stripped = strip_redundant_fields(x)
        round_trip = strip_redundant_fields(reconstruct_fields(stripped))
        assert self._deep_eq(stripped, round_trip), (
            "strip(reconstruct(strip(x))) must equal strip(x): the strip "
            "logic is not consistent under reconstruct's stub injection"
        )


class TestWordDictionary:
    """Tests for word_dictionary.py module."""

    def test_extract_unique_words_from_v4(self):
        from app.pipeline_cli.word_dictionary import extract_unique_words
        with tempfile.TemporaryDirectory() as tmpdir:
            response = {
                "result": {
                    "word_tags": [["قَالَ", "V"], ["عَنْ", "PREP"], ["قَالَ", "V"]]
                }
            }
            with open(os.path.join(tmpdir, "test.json"), "w", encoding="utf-8") as f:
                json.dump(response, f)
            counts = extract_unique_words(tmpdir)
            assert counts["قَالَ|V"] == 2
            assert counts["عَنْ|PREP"] == 1

    def test_extract_unique_words_from_v3(self):
        from app.pipeline_cli.word_dictionary import extract_unique_words
        with tempfile.TemporaryDirectory() as tmpdir:
            response = {
                "result": {
                    "word_analysis": [
                        {"word": "قَالَ", "pos": "V", "translation": {}},
                        {"word": "عَنْ", "pos": "PREP", "translation": {}},
                    ]
                }
            }
            with open(os.path.join(tmpdir, "test.json"), "w", encoding="utf-8") as f:
                json.dump(response, f)
            counts = extract_unique_words(tmpdir)
            assert counts["قَالَ|V"] == 1
            assert counts["عَنْ|PREP"] == 1

    def test_assemble_word_analysis(self):
        from app.pipeline_cli.word_dictionary import assemble_word_analysis
        word_tags = [["قَالَ", "V"], ["عَنْ", "PREP"]]
        dictionary = {
            "قَالَ|V": {"en": "he said", "ur": "کہا"},
            "عَنْ|PREP": {"en": "from", "ur": "سے"},
        }
        result = assemble_word_analysis(word_tags, dictionary)
        assert len(result) == 2
        assert result[0]["word"] == "قَالَ"
        assert result[0]["pos"] == "V"
        assert result[0]["translation"]["en"] == "he said"

    def test_assemble_word_analysis_missing_entry(self):
        from app.pipeline_cli.word_dictionary import assemble_word_analysis
        word_tags = [["قَالَ", "V"]]
        dictionary = {}  # Empty dictionary
        result = assemble_word_analysis(word_tags, dictionary)
        assert len(result) == 1
        assert result[0]["translation"]["en"] == "???"

    def test_find_missing_words(self):
        from app.pipeline_cli.word_dictionary import find_missing_words
        with tempfile.TemporaryDirectory() as tmpdir:
            response = {
                "result": {
                    "word_tags": [["قَالَ", "V"], ["عَنْ", "PREP"]]
                }
            }
            with open(os.path.join(tmpdir, "test.json"), "w", encoding="utf-8") as f:
                json.dump(response, f)
            dictionary = {"قَالَ|V": {"en": "he said"}}
            missing = find_missing_words(tmpdir, dictionary)
            assert len(missing) == 1
            assert missing[0][0] == "عَنْ"
            assert missing[0][1] == "PREP"

    def test_build_translation_prompt(self):
        from app.pipeline_cli.word_dictionary import build_translation_prompt
        words = [("قَالَ", "V"), ("عَنْ", "PREP")]
        prompt = build_translation_prompt(words)
        assert "قَالَ" in prompt
        assert "عَنْ" in prompt
        assert "JSON" in prompt

    def test_save_and_load_dictionary(self):
        from app.pipeline_cli.word_dictionary import save_v4_dictionary, load_v4_dictionary
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "dict.json")
            words = {"قَالَ|V": {"en": "he said"}}
            save_v4_dictionary(words, path=path)
            loaded = load_v4_dictionary(path=path)
            assert loaded == words


class TestFixChunkBoundaries:
    """Test the deterministic chunk boundary fix for OpenAI models."""

    def test_zero_length_isnad_chunk(self):
        """Fix zero-length isnad chunk using isnad_ar word count."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {
            "word_tags": [["w", "N"]] * 10,
            "isnad_matn": {"isnad_ar": "حَدَّثَنَا مُحَمَّدُ بْنُ يَحْيَى"},
            "chunks": [
                {"chunk_type": "isnad", "word_start": 0, "word_end": 0},
                {"chunk_type": "matn", "word_start": 0, "word_end": 10},
            ],
        }
        fixes = fix_chunk_boundaries(result)
        assert len(fixes) >= 1
        assert result["chunks"][0]["word_end"] == 4  # 4 words in isnad_ar
        assert result["chunks"][1]["word_start"] == 4  # sequential

    def test_single_zero_length_chunk(self):
        """Single chunk with (0,0) should span entire text."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {
            "word_tags": [["w", "N"]] * 5,
            "chunks": [
                {"chunk_type": "matn", "word_start": 0, "word_end": 0},
            ],
        }
        fixes = fix_chunk_boundaries(result)
        assert result["chunks"][0]["word_end"] == 5

    def test_off_by_one_last_chunk(self):
        """Last chunk word_end should be array length, not last index."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {
            "word_tags": [["w", "N"]] * 10,
            "chunks": [
                {"chunk_type": "matn", "word_start": 0, "word_end": 9},
            ],
        }
        fixes = fix_chunk_boundaries(result)
        assert result["chunks"][0]["word_end"] == 10

    def test_sequential_gap_repair(self):
        """Enforce chunk[i+1].word_start == chunk[i].word_end."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {
            "word_tags": [["w", "N"]] * 10,
            "chunks": [
                {"chunk_type": "isnad", "word_start": 0, "word_end": 3},
                {"chunk_type": "matn", "word_start": 0, "word_end": 10},
            ],
        }
        fixes = fix_chunk_boundaries(result)
        assert result["chunks"][1]["word_start"] == 3

    def test_already_correct_noop(self):
        """Correct chunks should not be modified."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {
            "word_tags": [["w", "N"]] * 10,
            "chunks": [
                {"chunk_type": "isnad", "word_start": 0, "word_end": 3},
                {"chunk_type": "matn", "word_start": 3, "word_end": 10},
            ],
        }
        fixes = fix_chunk_boundaries(result)
        assert fixes == []

    def test_no_chunks_noop(self):
        """No chunks — should return empty fixes."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {"word_tags": [["w", "N"]] * 5, "chunks": []}
        fixes = fix_chunk_boundaries(result)
        assert fixes == []

    def test_isnad_ar_longer_than_text(self):
        """isnad_ar word count capped at total_words."""
        from app.pipeline_cli.verse_processor import fix_chunk_boundaries
        result = {
            "word_tags": [["w", "N"]] * 3,
            "isnad_matn": {"isnad_ar": "a b c d e f g h i j"},  # 10 words > 3 total
            "chunks": [
                {"chunk_type": "isnad", "word_start": 0, "word_end": 0},
                {"chunk_type": "matn", "word_start": 0, "word_end": 3},
            ],
        }
        fixes = fix_chunk_boundaries(result)
        assert result["chunks"][0]["word_end"] == 3  # capped at total_words
