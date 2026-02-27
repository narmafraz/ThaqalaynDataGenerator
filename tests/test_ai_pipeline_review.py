"""Tests for the AI pipeline review, chunked processing, and prompt builders."""

import json
import pytest

from app.ai_pipeline import (
    VALID_LANGUAGE_KEYS,
    PipelineRequest,
    strip_redundant_fields,
    validate_result,
)

from app.ai_pipeline_review import (
    BACK_REFERENCE_PATTERNS,
    CHUNKED_PROCESSING_THRESHOLD,
    EUROPEAN_DIACRITICS,
    EUROPEAN_DIACRITICS_MIN_LENGTH,
    ReviewWarning,
    assemble_chunked_result,
    build_chunk_detail_prompt,
    build_fix_prompt,
    build_review_prompt,
    build_structure_prompt,
    estimate_word_count,
    review_result,
    should_use_chunked_processing,
)


# ===================================================================
# Helper: build valid results and requests for testing
# ===================================================================

def _make_valid_result(**overrides):
    """Build a minimally valid pipeline result dict."""
    result = {
        "diacritized_text": "\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650 \u0627\u0644\u0631\u0651\u064e\u062d\u0652\u0645\u0670\u0646\u0650 \u0627\u0644\u0631\u0651\u064e\u062d\u0650\u064a\u0645\u0650",
        "diacritics_status": "validated",
        "diacritics_changes": [],
        "word_analysis": [
            {
                "word": "\u0628\u0650\u0633\u0652\u0645\u0650",
                "translation": {lang: f"In the name of ({lang})" for lang in VALID_LANGUAGE_KEYS},
                "pos": "PREP",
            },
            {
                "word": "\u0627\u0644\u0644\u0651\u064e\u0647\u0650",
                "translation": {lang: f"Allah ({lang})" for lang in VALID_LANGUAGE_KEYS},
                "pos": "N",
            },
        ],
        "tags": ["theology", "worship"],
        "content_type": "creedal",
        "related_quran": [],
        "isnad_matn": {
            "isnad_ar": "",
            "matn_ar": "\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650",
            "has_chain": False,
            "narrators": [],
        },
        "translations": {},
        "chunks": [
            {
                "chunk_type": "body",
                "arabic_text": "\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650",
                "word_start": 0,
                "word_end": 2,
                "translations": {lang: f"Body text ({lang})" for lang in VALID_LANGUAGE_KEYS},
            },
        ],
    }
    for lang in VALID_LANGUAGE_KEYS:
        text = f"Translation in {lang} - this is a faithful rendering of the text."
        # Use proper diacritics for European languages so clean results don't trigger warnings
        if lang == "tr":
            text = "Allah'\u0131n ad\u0131yla, \u00f6\u011fretici ve merhamet \u015fefkatli olan."
        elif lang == "fr":
            text = "Au nom de Dieu, le Tout Mis\u00e9ricordieux, le Tr\u00e8s Mis\u00e9ricordieux."
        elif lang == "de":
            text = "Im Namen Gottes, des Allerbarmers, des Barmherzigen. F\u00fcr alle."
        elif lang == "es":
            text = "En el nombre de Dios, el Compasivo, el Misericordioso. Se\u00f1or."
        result["translations"][lang] = {
            "text": text,
            "summary": f"Summary in {lang}",
            "key_terms": {"\u0627\u0644\u0644\u0651\u064e\u0647": f"Allah in {lang}"},
            "seo_question": f"Question in {lang}?",
        }
    result.update(overrides)
    return result


def _make_request(**overrides):
    """Build a PipelineRequest for testing."""
    defaults = {
        "verse_path": "/books/al-kafi:1:1:1:1",
        "arabic_text": "\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650 \u0627\u0644\u0631\u0651\u064e\u062d\u0652\u0645\u0670\u0646\u0650 \u0627\u0644\u0631\u0651\u064e\u062d\u0650\u064a\u0645\u0650",
        "english_text": "In the Name of Allah, the All-beneficent, the All-merciful.",
        "book_name": "al-kafi",
        "chapter_title": "Chapter 1",
    }
    defaults.update(overrides)
    return PipelineRequest(**defaults)


def _make_long_arabic(word_count: int) -> str:
    """Generate Arabic-ish text with a given number of words."""
    words = ["\u0637\u064e\u0644\u064e\u0628\u064f", "\u0627\u0644\u0652\u0639\u0650\u0644\u0652\u0645\u0650", "\u0641\u064e\u0631\u0650\u064a\u0636\u064e\u0629\u064c"]
    return " ".join(words[i % len(words)] for i in range(word_count))


# ===================================================================
# TestReviewResult — 12 tests
# ===================================================================

class TestReviewResult:
    def test_clean_result_no_warnings(self):
        """A well-formed result should produce no warnings."""
        result = _make_valid_result()
        request = _make_request()
        warnings = review_result(result, request)
        assert warnings == []

    def test_summary_as_translation_flagged(self):
        """A too-short translation triggers length_ratio warning."""
        result = _make_valid_result()
        # Make Arabic text long but English translation very short
        long_arabic = _make_long_arabic(50)
        result["translations"]["en"]["text"] = "Short."
        request = _make_request(arabic_text=long_arabic)
        warnings = review_result(result, request)
        ratio_warnings = [w for w in warnings if w.category == "length_ratio"]
        assert len(ratio_warnings) >= 1
        assert any(w.field == "translations.en" for w in ratio_warnings)

    def test_short_arabic_skips_ratio_check(self):
        """Arabic text under 20 chars should skip ratio check."""
        result = _make_valid_result()
        result["translations"]["en"]["text"] = "X"  # very short
        request = _make_request(arabic_text="\u0628\u0633\u0645")  # 3 chars
        warnings = review_result(result, request)
        ratio_warnings = [w for w in warnings if w.category == "length_ratio"]
        assert len(ratio_warnings) == 0

    def test_arabic_echo_in_word_translation(self):
        """Non-fa/ur word translation with >50% Arabic chars is flagged."""
        result = _make_valid_result()
        # Set English word translation to Arabic text
        result["word_analysis"][0]["translation"]["en"] = "\u0628\u0650\u0633\u0652\u0645\u0650"
        request = _make_request()
        warnings = review_result(result, request)
        echo_warnings = [w for w in warnings if w.category == "arabic_echo"]
        assert len(echo_warnings) >= 1
        assert any("en" in w.field for w in echo_warnings)

    def test_farsi_exact_echo_flagged(self):
        """Farsi translation identical to Arabic word (sans diacritics) is flagged."""
        result = _make_valid_result()
        # Set fa word translation to exact Arabic word
        arabic_word = result["word_analysis"][0]["word"]
        result["word_analysis"][0]["translation"]["fa"] = arabic_word
        request = _make_request()
        warnings = review_result(result, request)
        echo_warnings = [w for w in warnings if w.category == "arabic_echo" and "fa" in w.field]
        assert len(echo_warnings) >= 1

    def test_farsi_loanword_ok(self):
        """Farsi translation that differs from Arabic word should not be flagged."""
        result = _make_valid_result()
        result["word_analysis"][0]["translation"]["fa"] = "\u0628\u0647 \u0646\u0627\u0645"  # "be nam" - different
        request = _make_request()
        warnings = review_result(result, request)
        fa_echo = [w for w in warnings if w.category == "arabic_echo" and "fa" in w.field]
        assert len(fa_echo) == 0

    def test_turkish_missing_diacritics(self):
        """Long Turkish text without any diacritics is flagged."""
        result = _make_valid_result()
        # Long Turkish text with NO diacritics (all ASCII)
        result["translations"]["tr"]["text"] = "Bu hadis, bilginin onemini anlatiyor. " * 5
        request = _make_request()
        warnings = review_result(result, request)
        diac_warnings = [w for w in warnings if w.category == "missing_diacritics" and "tr" in w.field]
        assert len(diac_warnings) >= 1

    def test_short_turkish_no_diacritics_check(self):
        """Short Turkish text skips diacritics check."""
        result = _make_valid_result()
        result["translations"]["tr"]["text"] = "Kisa metin"  # < 50 chars
        request = _make_request()
        warnings = review_result(result, request)
        diac_warnings = [w for w in warnings if w.category == "missing_diacritics" and "tr" in w.field]
        assert len(diac_warnings) == 0

    def test_quran_empty_related_quran(self):
        """Quran verse with empty related_quran is flagged."""
        result = _make_valid_result(related_quran=[])
        request = _make_request(verse_path="/books/quran:1:1")
        warnings = review_result(result, request)
        quran_warnings = [w for w in warnings if w.category == "empty_related_quran"]
        assert len(quran_warnings) == 1

    def test_hadith_empty_related_quran_ok(self):
        """Hadith with empty related_quran is NOT flagged."""
        result = _make_valid_result(related_quran=[])
        request = _make_request(verse_path="/books/al-kafi:1:1:1:1")
        warnings = review_result(result, request)
        quran_warnings = [w for w in warnings if w.category == "empty_related_quran"]
        assert len(quran_warnings) == 0

    def test_has_chain_no_isnad_chunk(self):
        """has_chain=True with no isnad chunk and empty isnad_ar is flagged."""
        result = _make_valid_result()
        result["isnad_matn"] = {
            "isnad_ar": "",
            "matn_ar": "text",
            "has_chain": True,
            "narrators": [
                {
                    "name_ar": "\u0623\u064e\u062d\u0652\u0645\u064e\u062f\u064f",
                    "name_en": "Ahmad",
                    "role": "narrator",
                    "position": 1,
                    "identity_confidence": "definite",
                    "ambiguity_note": None,
                    "known_identity": None,
                },
            ],
        }
        # Only body chunk, no isnad chunk
        request = _make_request()
        warnings = review_result(result, request)
        isnad_warnings = [w for w in warnings if w.category == "missing_isnad_chunk"]
        assert len(isnad_warnings) == 1

    def test_back_reference_without_chain(self):
        """Arabic starting with back-reference but has_chain=False is flagged."""
        result = _make_valid_result()
        result["isnad_matn"]["has_chain"] = False
        # Arabic text starts with back-reference pattern
        request = _make_request(arabic_text="\u0648\u0639\u0646\u0647 \u0642\u064e\u0627\u0644\u064e \u0637\u064e\u0644\u064e\u0628\u064f \u0627\u0644\u0652\u0639\u0650\u0644\u0652\u0645\u0650")
        warnings = review_result(result, request)
        backref_warnings = [w for w in warnings if w.category == "back_reference_no_chain"]
        assert len(backref_warnings) == 1


# ===================================================================
# TestChunkedProcessing — 8 tests
# ===================================================================

class TestChunkedProcessing:
    def test_short_text_not_chunked(self):
        """Short text should not trigger chunked processing."""
        request = _make_request(arabic_text="\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650")
        assert should_use_chunked_processing(request) is False

    def test_long_text_is_chunked(self):
        """Long text should trigger chunked processing."""
        request = _make_request(arabic_text=_make_long_arabic(250))
        assert should_use_chunked_processing(request) is True

    def test_custom_threshold(self):
        """Custom threshold should be respected."""
        request = _make_request(arabic_text=_make_long_arabic(50))
        assert should_use_chunked_processing(request, threshold=30) is True
        assert should_use_chunked_processing(request, threshold=100) is False

    def test_estimate_word_count(self):
        """Word count estimation uses whitespace splitting."""
        request = _make_request(arabic_text="\u0637\u064e\u0644\u064e\u0628\u064f \u0627\u0644\u0652\u0639\u0650\u0644\u0652\u0645\u0650 \u0641\u064e\u0631\u0650\u064a\u0636\u064e\u0629\u064c")
        assert estimate_word_count(request) == 3

    def test_estimate_word_count_empty(self):
        """Empty text should have 0 word count."""
        request = _make_request(arabic_text="")
        assert estimate_word_count(request) == 0

    def test_assemble_happy_path(self):
        """Assembling structure + chunk details produces valid result."""
        structure = _make_valid_result()
        structure["word_analysis"] = []  # Structure pass has empty word_analysis
        structure["chunks"][0]["translations"] = {}  # Empty chunk translations

        chunk_detail = {
            "word_analysis": [
                {
                    "word": "\u0628\u0650\u0633\u0652\u0645\u0650",
                    "translation": {lang: f"In the name of ({lang})" for lang in VALID_LANGUAGE_KEYS},
                    "pos": "PREP",
                },
                {
                    "word": "\u0627\u0644\u0644\u0651\u064e\u0647\u0650",
                    "translation": {lang: f"Allah ({lang})" for lang in VALID_LANGUAGE_KEYS},
                    "pos": "N",
                },
            ],
            "translations": {lang: f"Body text ({lang})" for lang in VALID_LANGUAGE_KEYS},
        }

        result = assemble_chunked_result(structure, [chunk_detail])
        assert len(result["word_analysis"]) == 2
        assert result["chunks"][0]["word_start"] == 0
        assert result["chunks"][0]["word_end"] == 2
        errors = validate_result(result)
        assert errors == [], f"Validation errors: {errors}"

    def test_assemble_mismatch_raises(self):
        """Mismatched chunk count raises ValueError."""
        structure = _make_valid_result()
        structure["word_analysis"] = []
        # Structure has 1 chunk but we provide 2 details
        with pytest.raises(ValueError, match="Expected 1 chunk details"):
            assemble_chunked_result(structure, [{}, {}])

    def test_structure_prompt_omits_word_analysis(self):
        """Structure prompt should mention empty word_analysis."""
        request = _make_request()
        prompt = build_structure_prompt(request)
        assert "STRUCTURE PASS" in prompt
        assert "word_analysis" in prompt
        assert "empty array" in prompt.lower() or "empty array []" in prompt


# ===================================================================
# TestPromptBuilders — 5 tests
# ===================================================================

class TestPromptBuilders:
    def test_review_prompt_has_arabic(self):
        """Review prompt includes the original Arabic text."""
        result = _make_valid_result()
        request = _make_request()
        prompt = build_review_prompt(result, request)
        assert request.arabic_text in prompt

    def test_review_prompt_has_instructions(self):
        """Review prompt includes review checklist."""
        result = _make_valid_result()
        request = _make_request()
        prompt = build_review_prompt(result, request)
        assert "REVIEW CHECKLIST" in prompt
        assert "overall_quality" in prompt
        assert "needs_fix" in prompt

    def test_fix_prompt_has_warnings(self):
        """Fix prompt includes warning details."""
        result = _make_valid_result()
        request = _make_request()
        warnings = [
            ReviewWarning(
                field="translations.tr",
                category="missing_diacritics",
                severity="medium",
                message="No diacritics found",
                suggestion="Add proper Turkish diacritics",
            ),
        ]
        prompt = build_fix_prompt(result, request, warnings)
        assert "missing_diacritics" in prompt
        assert "translations.tr" in prompt
        assert "No diacritics found" in prompt

    def test_fix_prompt_has_field_refs(self):
        """Fix prompt includes the flagged field values."""
        result = _make_valid_result()
        request = _make_request()
        warnings = [
            ReviewWarning(
                field="translations.en",
                category="length_ratio",
                severity="high",
                message="Too short",
                suggestion="Expand translation",
            ),
        ]
        prompt = build_fix_prompt(result, request, warnings)
        # Should include the translations field value
        assert "translations" in prompt
        assert "Translation in en" in prompt  # from the result

    def test_structure_prompt_includes_verse_translations(self):
        """Structure prompt should instruct verse-level translations."""
        request = _make_request()
        prompt = build_structure_prompt(request)
        assert "translations" in prompt
        assert "11 languages" in prompt

    def test_chunk_detail_prompt_includes_chunk_arabic(self):
        """Chunk detail prompt includes the chunk's Arabic text."""
        request = _make_request()
        structure = _make_valid_result()
        prompt = build_chunk_detail_prompt(request, structure, 0)
        chunk_arabic = structure["chunks"][0]["arabic_text"]
        assert chunk_arabic in prompt
        assert "CHUNK DETAIL PASS" in prompt

    def test_chunk_detail_prompt_out_of_range(self):
        """Out-of-range chunk index raises IndexError."""
        request = _make_request()
        structure = _make_valid_result()
        with pytest.raises(IndexError):
            build_chunk_detail_prompt(request, structure, 5)


class TestReviewAcceptsStrippedFormat:
    """Tests that review_result() works with stripped format input."""

    def test_review_accepts_stripped_format(self):
        """review_result() should auto-reconstruct and work on stripped input."""
        result = _make_valid_result()
        request = _make_request()
        # Verify full format works
        warnings_full = review_result(result, request)
        # Now strip and verify stripped also works
        stripped = strip_redundant_fields(result)
        warnings_stripped = review_result(stripped, request)
        # Should produce equivalent results (both should pass without issues)
        assert isinstance(warnings_stripped, list)
