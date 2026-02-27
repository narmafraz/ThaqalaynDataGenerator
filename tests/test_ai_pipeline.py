"""Tests for the AI content pipeline module."""

import json
import os
import pytest
import tempfile
from unittest.mock import patch

from app.ai_pipeline import (
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
    estimate_cost,
    extract_pipeline_request,
    load_few_shot_examples,
    load_glossary,
    load_key_phrases_dictionary,
    load_sample_verses,
    load_topic_taxonomy,
    parse_response,
    reconstruct_fields,
    strip_redundant_fields,
    validate_directory,
    validate_result,
    write_request_jsonl,
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
    """Build a valid result with has_chain=True and narrator data."""
    result = _make_valid_result()
    result["isnad_matn"] = {
        "isnad_ar": "عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنْ أَبِي عَبْدِ اللَّهِ عَلَيْهِ السَّلَامُ قَالَ",
        "matn_ar": "طَلَبُ الْعِلْمِ فَرِيضَةٌ",
        "has_chain": True,
        "narrators": [
            {
                "name_ar": "أَحْمَدُ بْنُ مُحَمَّدٍ",
                "name_en": "Ahmad ibn Muhammad",
                "role": "narrator",
                "position": 1,
                "identity_confidence": "definite",
                "ambiguity_note": None,
                "known_identity": "Ahmad ibn Muhammad al-Barqi",
            },
            {
                "name_ar": "أَبُو عَبْدِ اللَّهِ",
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
            "arabic_text": "عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ",
            "word_start": 0,
            "word_end": 1,
            "translations": {lang: f"Isnad ({lang})" for lang in VALID_LANGUAGE_KEYS},
        },
        {
            "chunk_type": "body",
            "arabic_text": "طَلَبُ الْعِلْمِ فَرِيضَةٌ",
            "word_start": 1,
            "word_end": 2,
            "translations": {lang: f"Body ({lang})" for lang in VALID_LANGUAGE_KEYS},
        },
    ]
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

    def test_includes_word_dictionary(self):
        from app.ai_pipeline import load_word_dictionary
        word_dict = load_word_dictionary()
        if word_dict is None:
            pytest.skip("word_dictionary.json not found")
        prompt = build_system_prompt(word_dictionary=word_dict)
        assert "COMMON WORD TRANSLATIONS" in prompt
        # Should contain some common Arabic particles
        assert "\u0648\u064e" in prompt  # wa (and)
        assert "\u0645\u0650\u0646\u0652" in prompt  # min (from)
        assert "CONJ" in prompt
        assert "PREP" in prompt

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
        assert "word_analysis" in msg
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

    def test_invalid_quran_ref_format(self):
        result = _make_valid_result(
            related_quran=[{"ref": "abc", "relationship": "thematic"}]
        )
        errors = validate_result(result)
        assert any("quran ref format" in e for e in errors)

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

    def test_includes_similar_content_hints_field(self):
        req = PipelineRequest(
            verse_path="/books/al-kafi:1:1:1:1",
            arabic_text="text",
        )
        msg = build_user_message(req)
        assert '"similar_content_hints"' in msg


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
        result = _make_valid_result(topics=["tawhid", "patience", "honesty", "humility"])
        errors = validate_result(result)
        assert any("1-3 items" in e for e in errors)

    def test_topics_empty(self):
        result = _make_valid_result(topics=[])
        errors = validate_result(result)
        assert any("1-3 items" in e for e in errors)

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
