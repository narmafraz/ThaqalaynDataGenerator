"""Comprehensive tests for the Arabic text normalization engine."""

import pytest

from app.arabic_normalization import (
    ComparisonResult,
    ComparisonTier,
    ValidationEntry,
    ValidationReport,
    compare_arabic,
    normalize_arabic,
    normalize_letters,
    normalize_preserve_diacritics,
    normalize_punctuation,
    normalize_whitespace,
    remove_tatweel,
    strip_tashkeel,
)


# ===================================================================
# strip_tashkeel tests
# ===================================================================

class TestStripTashkeel:
    def test_removes_fatha(self):
        assert strip_tashkeel("بَ") == "ب"

    def test_removes_damma(self):
        assert strip_tashkeel("بُ") == "ب"

    def test_removes_kasra(self):
        assert strip_tashkeel("بِ") == "ب"

    def test_removes_fathatan(self):
        assert strip_tashkeel("بً") == "ب"

    def test_removes_dammatan(self):
        assert strip_tashkeel("بٌ") == "ب"

    def test_removes_kasratan(self):
        assert strip_tashkeel("بٍ") == "ب"

    def test_removes_shadda(self):
        assert strip_tashkeel("بّ") == "ب"

    def test_removes_sukun(self):
        assert strip_tashkeel("بْ") == "ب"

    def test_removes_superscript_alef(self):
        """Dagger alef (superscript alef) should be removed."""
        assert strip_tashkeel("هٰذَا") == "هذا"

    def test_preserves_base_letters(self):
        """Base Arabic letters should be unchanged."""
        text = "بسم الله الرحمن الرحيم"
        assert strip_tashkeel(text) == text

    def test_fully_diacritized_text(self):
        """Remove all diacritics from fully vocalized text."""
        vocalized = "بِسْمِ اللَّهِ الرَّحْمَنِ الرَّحِيمِ"
        expected = "بسم الله الرحمن الرحيم"
        assert strip_tashkeel(vocalized) == expected

    def test_empty_string(self):
        assert strip_tashkeel("") == ""

    def test_non_arabic_text_unchanged(self):
        text = "Hello World 123"
        assert strip_tashkeel(text) == text

    def test_mixed_arabic_english(self):
        text = "مُحَمَّدٌ is a name"
        assert strip_tashkeel(text) == "محمد is a name"

    def test_multiple_diacritics_on_one_letter(self):
        """Letter with shadda + kasra (common combination)."""
        text = "اللِّهِ"  # lam with shadda + kasra
        result = strip_tashkeel(text)
        assert "ِّ" not in result
        # Should be just "الله"
        assert strip_tashkeel("اللِّهِ") == "الله"


# ===================================================================
# remove_tatweel tests
# ===================================================================

class TestRemoveTatweel:
    def test_removes_tatweel(self):
        assert remove_tatweel("كـتـاب") == "كتاب"

    def test_no_tatweel(self):
        text = "كتاب"
        assert remove_tatweel(text) == text

    def test_multiple_tatweels(self):
        assert remove_tatweel("ـــكـــ") == "ك"

    def test_empty_string(self):
        assert remove_tatweel("") == ""


# ===================================================================
# normalize_letters tests
# ===================================================================

class TestNormalizeLetters:
    def test_alef_with_hamza_above(self):
        """أ -> ا"""
        assert normalize_letters("أحمد") == "احمد"

    def test_alef_with_hamza_below(self):
        """إ -> ا"""
        assert normalize_letters("إبراهيم") == "ابراهيم"

    def test_alef_with_madda(self):
        """آ -> ا (note: ة -> ه also applies)"""
        assert normalize_letters("آية") == "ايه"

    def test_alef_wasla(self):
        """ٱ -> ا"""
        assert normalize_letters("ٱلرَّحْمَنِ") == "الرَّحْمَنِ"

    def test_teh_marbuta(self):
        """ة -> ه"""
        assert normalize_letters("رحمة") == "رحمه"

    def test_alef_maksura(self):
        """ى -> ي"""
        assert normalize_letters("على") == "علي"
        assert normalize_letters("موسى") == "موسي"

    def test_waw_with_hamza(self):
        """ؤ -> و"""
        assert normalize_letters("مؤمن") == "مومن"

    def test_yeh_with_hamza(self):
        """ئ -> ي"""
        assert normalize_letters("شيئ") == "شيي"

    def test_multiple_normalizations(self):
        """Multiple letter variants in one text."""
        text = "أَنَّ إِبْرَاهِيمَ آمَنَ بِرَحْمَةِ اللَّهِ عَلَى مُؤْمِنٍ"
        result = normalize_letters(text)
        assert "أ" not in result
        assert "إ" not in result
        assert "آ" not in result
        assert "ة" not in result
        assert "ؤ" not in result

    def test_preserves_plain_letters(self):
        """Letters that don't need normalization stay unchanged."""
        text = "بسم الله الرحمن الرحيم"
        assert normalize_letters(text) == text

    def test_empty_string(self):
        assert normalize_letters("") == ""


# ===================================================================
# normalize_punctuation tests
# ===================================================================

class TestNormalizePunctuation:
    def test_arabic_comma(self):
        assert normalize_punctuation("أحمد، محمد") == "أحمد, محمد"

    def test_arabic_semicolon(self):
        assert normalize_punctuation("قال؛ قال") == "قال; قال"

    def test_arabic_question_mark(self):
        assert normalize_punctuation("ما هذا؟") == "ما هذا?"

    def test_no_arabic_punctuation(self):
        text = "plain text"
        assert normalize_punctuation(text) == text


# ===================================================================
# normalize_whitespace tests
# ===================================================================

class TestNormalizeWhitespace:
    def test_collapses_multiple_spaces(self):
        assert normalize_whitespace("بسم   الله   الرحمن") == "بسم الله الرحمن"

    def test_strips_leading_trailing(self):
        assert normalize_whitespace("  بسم الله  ") == "بسم الله"

    def test_tabs_and_newlines(self):
        assert normalize_whitespace("بسم\tالله\nالرحمن") == "بسم الله الرحمن"

    def test_nbsp(self):
        assert normalize_whitespace("بسم\u00A0الله") == "بسم الله"

    def test_zero_width_chars(self):
        """ZWNJ, ZWJ, ZWSP, BOM should be treated as whitespace."""
        assert normalize_whitespace("بسم\u200Cالله") == "بسم الله"
        assert normalize_whitespace("بسم\u200Bالله") == "بسم الله"
        assert normalize_whitespace("بسم\uFEFFالله") == "بسم الله"

    def test_empty_string(self):
        assert normalize_whitespace("") == ""

    def test_only_whitespace(self):
        assert normalize_whitespace("   \t\n  ") == ""


# ===================================================================
# normalize_arabic (full pipeline) tests
# ===================================================================

class TestNormalizeArabic:
    def test_full_normalization(self):
        """All steps combined."""
        text = "بِسْـمِ اللَّهِ  الرَّحْمَنِ الرَّحِيمِ"
        expected = "بسم الله الرحمن الرحيم"
        assert normalize_arabic(text) == expected

    def test_hamza_and_tashkeel_combined(self):
        text = "أَخْبَرَنَا أَبُو جَعْفَرٍ"
        expected = "اخبرنا ابو جعفر"
        assert normalize_arabic(text) == expected

    def test_teh_marbuta_with_tashkeel(self):
        text = "رَحْمَةً"
        expected = "رحمه"
        assert normalize_arabic(text) == expected

    def test_real_hadith_opening(self):
        """Test normalization on a real hadith isnad.

        Note: يَحْيَى ends with alef maksura (ى) which normalizes to yeh (ي),
        giving يحيي (double yeh at end).
        """
        text = "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ"
        expected = "محمد بن يحيي عن احمد بن محمد"
        assert normalize_arabic(text) == expected

    def test_alef_maksura_in_name(self):
        text = "عِيسَى بْنِ مُوسَى"
        expected = "عيسي بن موسي"
        assert normalize_arabic(text) == expected

    def test_preserves_numbers(self):
        text = "الآية 42"
        result = normalize_arabic(text)
        assert "42" in result

    def test_empty_string(self):
        assert normalize_arabic("") == ""

    def test_idempotent(self):
        """Normalizing already-normalized text should be a no-op."""
        text = "بسم الله الرحمن الرحيم"
        assert normalize_arabic(normalize_arabic(text)) == normalize_arabic(text)


# ===================================================================
# normalize_preserve_diacritics tests
# ===================================================================

class TestNormalizePreserveDiacritics:
    def test_preserves_tashkeel(self):
        text = "بِسْمِ"
        result = normalize_preserve_diacritics(text)
        assert "ِ" in result  # kasra should still be there
        assert "ْ" in result  # sukun should still be there

    def test_removes_tatweel_but_keeps_tashkeel(self):
        text = "كِـتَـابٌ"
        result = normalize_preserve_diacritics(text)
        assert "\u0640" not in result  # tatweel removed
        assert "ِ" in result           # kasra preserved
        assert "َ" in result           # fatha preserved
        assert "ٌ" in result           # dammatan preserved

    def test_normalizes_letters(self):
        text = "أَحْمَدُ"
        result = normalize_preserve_diacritics(text)
        assert result.startswith("ا")  # hamza normalized
        assert "َ" in result           # fatha preserved


# ===================================================================
# compare_arabic tests — Tier 1 (Exact)
# ===================================================================

class TestComparisonTier1:
    def test_identical_texts(self):
        text = "بسم الله الرحمن الرحيم"
        result = compare_arabic(text, text)
        assert result.tier == ComparisonTier.EXACT
        assert result.confidence == 1.0

    def test_same_after_tashkeel_stripping(self):
        """Two texts that differ only in diacritics are Tier 2 (not Tier 1)."""
        a = "بِسْمِ اللَّهِ"
        b = "بِسْمِ اللّهِ"  # different shadda placement
        result = compare_arabic(a, b)
        # Same base text but different diacritics = Tier 2
        assert result.tier == ComparisonTier.DIACRITICS

    def test_hamza_variants_match(self):
        """أحمد and احمد should be exact match after normalization."""
        result = compare_arabic("أحمد", "احمد")
        assert result.tier == ComparisonTier.EXACT

    def test_teh_marbuta_heh_match(self):
        """رحمة and رحمه should be exact match after normalization."""
        result = compare_arabic("رحمة", "رحمه")
        assert result.tier == ComparisonTier.EXACT

    def test_tatweel_difference_is_exact(self):
        """كـتاب and كتاب should be exact match."""
        result = compare_arabic("كـتاب", "كتاب")
        assert result.tier == ComparisonTier.EXACT

    def test_whitespace_difference_is_exact(self):
        """Extra whitespace should not affect comparison."""
        result = compare_arabic("بسم  الله", "بسم الله")
        assert result.tier == ComparisonTier.EXACT

    def test_alef_maksura_yeh_match(self):
        """على and علي should be exact match."""
        result = compare_arabic("على", "علي")
        assert result.tier == ComparisonTier.EXACT


# ===================================================================
# compare_arabic tests — Tier 2 (Diacritics only)
# ===================================================================

class TestComparisonTier2:
    def test_different_diacritics_same_base(self):
        """Same consonantal text with different diacritics = Tier 2."""
        a = "كَتَبَ"  # kataba (he wrote)
        b = "كُتُبٌ"  # kutubun (books)
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.DIACRITICS
        assert 0 < result.confidence < 1.0
        assert "diacritics_differ" in result.differences

    def test_partial_vs_full_diacritics(self):
        """Partially diacritized vs fully diacritized = Tier 2."""
        a = "بِسْمِ اللَّهِ الرَّحْمَنِ الرَّحِيمِ"
        b = "بِسمِ اللهِ الرَحمنِ الرَحيمِ"
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.DIACRITICS

    def test_diacritized_vs_undiacritized(self):
        """Fully diacritized vs plain text = Tier 2."""
        a = "مُحَمَّدٌ"
        b = "محمد"
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.DIACRITICS


# ===================================================================
# compare_arabic tests — Tier 3 (Substantive)
# ===================================================================

class TestComparisonTier3:
    def test_different_words(self):
        """Completely different texts = Tier 3."""
        result = compare_arabic("بسم الله", "قال النبي")
        assert result.tier == ComparisonTier.SUBSTANTIVE
        assert result.confidence < 0.5

    def test_word_added(self):
        """Text with an extra word = Tier 3."""
        a = "محمد بن يحيى"
        b = "محمد بن يحيى العطار"
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.SUBSTANTIVE
        assert result.confidence > 0.5  # still mostly similar

    def test_word_swapped(self):
        """Word replacement = Tier 3."""
        a = "عن أحمد بن محمد"
        b = "عن علي بن محمد"
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.SUBSTANTIVE

    def test_completely_different(self):
        result = compare_arabic("الله أكبر", "لا إله إلا الله")
        assert result.tier == ComparisonTier.SUBSTANTIVE
        assert result.confidence < 0.5

    def test_empty_vs_text(self):
        result = compare_arabic("", "بسم الله")
        assert result.tier == ComparisonTier.SUBSTANTIVE
        assert result.confidence == 0.0

    def test_both_empty(self):
        result = compare_arabic("", "")
        assert result.tier == ComparisonTier.EXACT
        assert result.confidence == 1.0


# ===================================================================
# ComparisonResult serialization
# ===================================================================

class TestComparisonResultSerialization:
    def test_to_dict_exact(self):
        result = ComparisonResult(
            tier=ComparisonTier.EXACT,
            confidence=1.0,
        )
        d = result.to_dict()
        assert d["tier"] == 1
        assert d["tier_name"] == "exact"
        assert d["confidence"] == 1.0
        assert "differences" not in d

    def test_to_dict_with_differences(self):
        result = ComparisonResult(
            tier=ComparisonTier.SUBSTANTIVE,
            confidence=0.75,
            differences=["word_count: 5 vs 6"],
        )
        d = result.to_dict()
        assert d["tier"] == 3
        assert d["differences"] == ["word_count: 5 vs 6"]


# ===================================================================
# ValidationEntry and ValidationReport
# ===================================================================

class TestValidationReport:
    def test_report_summary(self):
        report = ValidationReport(
            book_slug="al-kafi",
            source_a_name="hubeali",
            source_b_name="thaqalayn_api",
        )
        report.entries = [
            ValidationEntry(
                path="/books/al-kafi:1:1:1:1",
                comparison=ComparisonResult(tier=ComparisonTier.EXACT, confidence=1.0),
            ),
            ValidationEntry(
                path="/books/al-kafi:1:1:1:2",
                comparison=ComparisonResult(tier=ComparisonTier.DIACRITICS, confidence=0.9),
            ),
            ValidationEntry(
                path="/books/al-kafi:1:1:1:3",
                comparison=ComparisonResult(tier=ComparisonTier.SUBSTANTIVE, confidence=0.7),
            ),
        ]

        summary = report.summary()
        assert summary["total"] == 3
        assert summary["exact"] == 1
        assert summary["diacritics_only"] == 1
        assert summary["substantive"] == 1

    def test_report_to_dict(self):
        report = ValidationReport(
            book_slug="test",
            source_a_name="a",
            source_b_name="b",
        )
        report.entries = [
            ValidationEntry(
                path="/books/test:1",
                comparison=ComparisonResult(tier=ComparisonTier.EXACT, confidence=1.0),
                source_a_name="a",
                source_b_name="b",
            ),
        ]
        d = report.to_dict()
        assert "summary" in d
        assert "entries" in d
        assert len(d["entries"]) == 1
        assert d["entries"][0]["path"] == "/books/test:1"

    def test_empty_report(self):
        report = ValidationReport(
            book_slug="test",
            source_a_name="a",
            source_b_name="b",
        )
        assert report.total == 0
        assert report.exact_count == 0


# ===================================================================
# Edge cases and real-world Arabic text
# ===================================================================

class TestEdgeCases:
    def test_quran_bismillah(self):
        """Test with the most common Quranic phrase."""
        vocalized = "بِسْمِ ٱللَّهِ ٱلرَّحْمَٰنِ ٱلرَّحِيمِ"
        plain = "بسم الله الرحمن الرحيم"
        result = compare_arabic(vocalized, plain)
        assert result.tier in (ComparisonTier.EXACT, ComparisonTier.DIACRITICS)

    def test_narrator_name_variants(self):
        """Different romanization/diacritization of same narrator."""
        a = "مُحَمَّدُ بْنُ يَعْقُوبَ"
        b = "محمد بن يعقوب"
        result = compare_arabic(a, b)
        assert result.tier in (ComparisonTier.EXACT, ComparisonTier.DIACRITICS)

    def test_standalone_hamza(self):
        """Standalone hamza (ء) should be preserved."""
        a = "شيء"
        b = "شيء"
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.EXACT

    def test_unicode_composed_vs_decomposed(self):
        """Handle both composed and decomposed Unicode forms.

        In practice both forms appear in different sources.
        The normalization should handle both.
        """
        # NFC form (composed)
        composed = "\u0628\u0650"  # ba + kasra as separate chars
        # The composed form is the same for Arabic combining marks
        assert strip_tashkeel(composed) == "\u0628"

    def test_long_text_comparison(self):
        """Test with a longer hadith text snippet."""
        a = (
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ عَنِ "
            "ابْنِ مَحْبُوبٍ عَنْ عَبْدِ اللَّهِ بْنِ سِنَانٍ عَنْ "
            "أَبِي عَبْدِ اللَّهِ"
        )
        b = (
            "محمد بن يحيى عن أحمد بن محمد عن "
            "ابن محبوب عن عبد الله بن سنان عن "
            "أبي عبد الله"
        )
        result = compare_arabic(a, b)
        # Same text, just one is vocalized
        assert result.tier in (ComparisonTier.EXACT, ComparisonTier.DIACRITICS)

    def test_confidence_score_range(self):
        """Confidence should always be between 0 and 1."""
        test_pairs = [
            ("بسم الله", "بسم الله"),
            ("بسم الله", "قال النبي"),
            ("", ""),
            ("أ", "ب"),
        ]
        for a, b in test_pairs:
            result = compare_arabic(a, b)
            assert 0.0 <= result.confidence <= 1.0, f"Bad confidence for ({a!r}, {b!r}): {result.confidence}"


# ===================================================================
# Levenshtein ratio edge cases (tested through compare_arabic)
# ===================================================================

class TestLevenshteinViaCompare:
    def test_single_char_diff(self):
        """Single character difference should have high confidence."""
        a = "محمد"
        b = "محمب"  # last char different
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.SUBSTANTIVE
        assert result.confidence > 0.5

    def test_very_different_lengths(self):
        """Very different lengths should have low confidence."""
        a = "م"
        b = "محمد بن يحيى العطار الكوفي"
        result = compare_arabic(a, b)
        assert result.tier == ComparisonTier.SUBSTANTIVE
        assert result.confidence < 0.3
