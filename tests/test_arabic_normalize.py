"""Tests for Arabic text normalization utilities."""

import pytest

from app.wikishia.arabic_normalize import (
    normalize_alef,
    normalize_alef_maksura,
    normalize_arabic,
    normalize_for_matching,
    normalize_hamza,
    normalize_teh_marbuta,
    normalize_whitespace,
    strip_diacritics,
    strip_honorifics,
    strip_tatweel,
)


class TestStripDiacritics:
    """Test diacritics removal from Arabic text."""

    def test_remove_fatha(self):
        """Fatha (short 'a') is removed."""
        assert strip_diacritics('\u0645\u064E') == '\u0645'  # meem + fatha -> meem

    def test_remove_damma(self):
        """Damma (short 'u') is removed."""
        assert strip_diacritics('\u0645\u064F') == '\u0645'

    def test_remove_kasra(self):
        """Kasra (short 'i') is removed."""
        assert strip_diacritics('\u0645\u0650') == '\u0645'

    def test_remove_shadda(self):
        """Shadda (doubling mark) is removed."""
        assert strip_diacritics('\u0645\u0651') == '\u0645'

    def test_remove_sukun(self):
        """Sukun (no-vowel mark) is removed."""
        assert strip_diacritics('\u0645\u0652') == '\u0645'

    def test_remove_tanwin_fathatan(self):
        """Fathatan (tanwin -an) is removed."""
        assert strip_diacritics('\u0645\u064B') == '\u0645'

    def test_full_name_diacritics(self):
        """Test stripping diacritics from a real narrator name."""
        name = 'مُحَمَّدُ بْنُ يَحْيَى'
        result = strip_diacritics(name)
        assert result == 'محمد بن يحيى'

    def test_preserves_base_letters(self):
        """Non-diacritic Arabic letters are preserved."""
        text = 'محمد'
        assert strip_diacritics(text) == 'محمد'

    def test_preserves_non_arabic(self):
        """Non-Arabic characters are preserved."""
        assert strip_diacritics('hello world') == 'hello world'

    def test_empty_string(self):
        assert strip_diacritics('') == ''


class TestNormalizeAlef:
    """Test alef variant normalization."""

    def test_alef_with_hamza_above(self):
        assert normalize_alef('\u0623') == '\u0627'

    def test_alef_with_hamza_below(self):
        assert normalize_alef('\u0625') == '\u0627'

    def test_alef_with_madda(self):
        assert normalize_alef('\u0622') == '\u0627'

    def test_alef_wasla(self):
        assert normalize_alef('\u0671') == '\u0627'

    def test_plain_alef_unchanged(self):
        assert normalize_alef('\u0627') == '\u0627'

    def test_mixed_text(self):
        """Alef variants in context are normalized."""
        text = '\u0623\u062D\u0645\u062F'  # Ahmad with hamza-alef
        result = normalize_alef(text)
        assert result[0] == '\u0627'  # Plain alef


class TestNormalizeTehMarbuta:
    """Test teh marbuta -> heh normalization."""

    def test_teh_marbuta_to_heh(self):
        assert normalize_teh_marbuta('\u0629') == '\u0647'

    def test_in_word(self):
        """Teh marbuta at end of word is normalized."""
        word = '\u0641\u0627\u0637\u0645\u0629'  # Fatima
        result = normalize_teh_marbuta(word)
        assert result[-1] == '\u0647'


class TestNormalizeHamza:
    """Test hamza-on-carrier normalization."""

    def test_waw_with_hamza(self):
        assert normalize_hamza('\u0624') == '\u0648'

    def test_ya_with_hamza(self):
        assert normalize_hamza('\u0626') == '\u064A'


class TestNormalizeAlefMaksura:
    """Test alef maksura -> yeh normalization."""

    def test_alef_maksura_to_yeh(self):
        assert normalize_alef_maksura('\u0649') == '\u064A'


class TestStripTatweel:
    """Test tatweel removal."""

    def test_remove_tatweel(self):
        assert strip_tatweel('\u0645\u0640\u062D') == '\u0645\u062D'

    def test_no_tatweel(self):
        text = '\u0645\u062D'
        assert strip_tatweel(text) == text


class TestStripHonorifics:
    """Test Islamic honorific removal from narrator names."""

    def test_strip_alayhi_salam(self):
        name = 'أَبِي جَعْفَرٍ ( عليه السلام )'
        result = strip_honorifics(name)
        assert '( عليه السلام )' not in result
        assert 'أَبِي جَعْفَرٍ' in result

    def test_strip_alayhim_salam(self):
        name = 'عَنْهُمْ ( عليهم السلام )'
        result = strip_honorifics(name)
        assert '( عليهم السلام )' not in result

    def test_strip_salla(self):
        name = 'رَسُولِ اللَّهِ ( صلى الله عليه وآله )'
        result = strip_honorifics(name)
        assert '( صلى الله عليه وآله )' not in result

    def test_no_honorific(self):
        name = 'مُحَمَّدُ بْنُ يَحْيَى'
        assert strip_honorifics(name) == name


class TestNormalizeWhitespace:
    """Test whitespace normalization."""

    def test_collapse_multiple_spaces(self):
        assert normalize_whitespace('a  b   c') == 'a b c'

    def test_strip_leading_trailing(self):
        assert normalize_whitespace('  abc  ') == 'abc'

    def test_normalize_tabs_and_newlines(self):
        assert normalize_whitespace('a\tb\nc') == 'a b c'


class TestNormalizeArabic:
    """Test the full normalization pipeline."""

    def test_full_normalization(self):
        """Full pipeline on a diacritized narrator name."""
        name = 'مُحَمَّدُ بْنُ يَحْيَى'
        result = normalize_arabic(name)
        # Alef maksura (ى) is normalized to yeh (ي)
        assert result == 'محمد بن يحيي'

    def test_alef_variants_normalized(self):
        """Alef variants are unified."""
        text = '\u0623\u062D\u0645\u062F \u0625\u0628\u0631\u0627\u0647\u064A\u0645'
        result = normalize_arabic(text)
        assert '\u0623' not in result
        assert '\u0625' not in result

    def test_idempotent(self):
        """Applying normalization twice gives the same result."""
        name = 'مُحَمَّدُ بْنُ يَحْيَى الْعَطَّارُ'
        once = normalize_arabic(name)
        twice = normalize_arabic(once)
        assert once == twice


class TestNormalizeForMatching:
    """Test matching-specific normalization (includes honorific removal)."""

    def test_strips_honorific_and_normalizes(self):
        name = 'أَبِي عَبْدِ اللَّهِ ( عليه السلام )'
        result = normalize_for_matching(name)
        assert '( عليه السلام )' not in result
        # Diacritics also removed
        assert '\u064E' not in result  # No fatha

    def test_normalizes_without_honorific(self):
        name = 'مُحَمَّدِ بْنِ مُسْلِمٍ'
        result = normalize_for_matching(name)
        assert result == 'محمد بن مسلم'

    def test_empty_after_honorific_removal(self):
        """Edge case: name that is just an honorific phrase."""
        result = normalize_for_matching('( عليه السلام )')
        assert result.strip() == ''
