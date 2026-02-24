"""Tests for the AI content pipeline module."""

import json
import os
import pytest
import tempfile
from unittest.mock import patch

from app.ai_pipeline import (
    VALID_DIACRITICS_STATUS,
    VALID_HADITH_TYPES,
    VALID_IDENTITY_CONFIDENCE,
    VALID_LANGUAGE_KEYS,
    VALID_NARRATOR_ROLES,
    VALID_POS_TAGS,
    VALID_QURAN_RELATIONSHIPS,
    VALID_TAGS,
    PipelineConfig,
    PipelineRequest,
    build_system_prompt,
    build_user_message,
    estimate_cost,
    extract_pipeline_request,
    load_few_shot_examples,
    load_glossary,
    load_sample_verses,
    parse_response,
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
                "translation_en": "In the name of",
                "root": "س م و",
                "pos": "PREP",
                "is_proper_noun": False,
            },
            {
                "word": "اللَّهِ",
                "translation_en": "Allah",
                "root": "أ ل ه",
                "pos": "N",
                "is_proper_noun": True,
            },
        ],
        "tags": ["theology", "worship"],
        "hadith_type": "creedal",
        "related_quran": [],
        "isnad_matn": {
            "isnad_ar": "",
            "matn_ar": "بِسْمِ اللَّهِ الرَّحْمٰنِ الرَّحِيمِ",
            "has_chain": False,
            "narrators": [],
        },
        "translations": {},
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

    def test_invalid_hadith_type(self):
        result = _make_valid_result(hadith_type="unknown_type")
        errors = validate_result(result)
        assert any("hadith_type" in e for e in errors)

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
        result = _make_valid_result()
        del result["diacritized_text"]
        errors = validate_result(result)
        assert any("missing required field" in e and "diacritized_text" in e for e in errors)

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

    def test_is_proper_noun_not_boolean(self):
        result = _make_valid_result()
        result["word_analysis"][0]["is_proper_noun"] = "yes"
        errors = validate_result(result)
        assert any("is_proper_noun must be boolean" in e for e in errors)


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
        # Should be around $3,900 per AI_CONTENT_PIPELINE.md
        assert 3000 < cost["total_cost_usd"] < 5000


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
        result = _make_valid_result(hadith_type="bad_type")
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
# Enum constant completeness tests
# ===================================================================

class TestEnumConstants:
    def test_valid_pos_tags_complete(self):
        expected = {"N", "V", "ADJ", "ADV", "PREP", "CONJ", "PRON", "DET",
                    "PART", "INTJ", "REL", "DEM", "NEG", "COND", "INTERR"}
        assert VALID_POS_TAGS == expected

    def test_valid_tags_complete(self):
        assert len(VALID_TAGS) == 14

    def test_valid_hadith_types_complete(self):
        assert len(VALID_HADITH_TYPES) == 10

    def test_valid_language_keys_complete(self):
        assert VALID_LANGUAGE_KEYS == {"ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"}

    def test_valid_narrator_roles_complete(self):
        assert VALID_NARRATOR_ROLES == {"narrator", "companion", "imam", "author"}

    def test_valid_identity_confidence_complete(self):
        assert VALID_IDENTITY_CONFIDENCE == {"definite", "likely", "ambiguous"}
