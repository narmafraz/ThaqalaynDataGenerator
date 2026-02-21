"""Tests for Arabic-to-English transliteration of narrator names."""

import pytest

from app.wikishia.transliteration import (
    transliterate_arabic,
    transliterate_narrator_index,
    _transliterate_word,
)


class TestTransliterateKnownWords:
    """Test transliteration of words in the known-words dictionary."""

    def test_muhammad(self):
        """Common name Muhammad is recognized."""
        result = _transliterate_word("محمد")
        assert result == "Muhammad"

    def test_ali(self):
        result = _transliterate_word("علي")
        assert result == "Ali"

    def test_hasan(self):
        result = _transliterate_word("حسن")
        assert result == "Hasan"

    def test_husayn(self):
        result = _transliterate_word("حسين")
        assert result == "Husayn"

    def test_ibrahim(self):
        result = _transliterate_word("ابراهيم")
        assert result == "Ibrahim"

    def test_ibn(self):
        """'ibn' (son of) connector is recognized."""
        result = _transliterate_word("بن")
        assert result == "ibn"

    def test_abu(self):
        result = _transliterate_word("ابو")
        assert result == "Abu"

    def test_with_diacritics(self):
        """Words with diacritics are recognized after stripping."""
        result = _transliterate_word("مُحَمَّدُ")
        assert result == "Muhammad"


class TestTransliterateAlPrefix:
    """Test transliteration of words with al- prefix."""

    def test_al_known_word(self):
        """al- prefix with known word in dictionary."""
        result = _transliterate_word("الصادق")
        assert result == "al-Sadiq"

    def test_al_hasan(self):
        result = _transliterate_word("الحسن")
        assert result == "al-Hasan"


class TestTransliterateFullNames:
    """Test transliteration of complete narrator name strings."""

    def test_simple_name(self):
        result = transliterate_arabic("محمد بن يحيى")
        assert "Muhammad" in result
        assert "ibn" in result
        assert "Yahya" in result

    def test_name_with_diacritics(self):
        """Diacritized narrator name is transliterated."""
        result = transliterate_arabic("مُحَمَّدُ بْنُ يَحْيَى")
        assert "Muhammad" in result

    def test_name_with_honorific(self):
        """Honorific phrases are transliterated."""
        result = transliterate_arabic("أَبِي جَعْفَرٍ ( عليه السلام )")
        assert "(a)" in result

    def test_complex_name(self):
        """Multi-part narrator name."""
        result = transliterate_arabic("عَلِيُّ بْنُ إِبْرَاهِيمَ بْنِ هَاشِمٍ")
        assert "Ali" in result
        assert "Ibrahim" in result

    def test_empty_string(self):
        assert transliterate_arabic("") == ""

    def test_first_letter_capitalized(self):
        """Result should start with a capital letter."""
        result = transliterate_arabic("محمد")
        assert result[0].isupper()

    def test_sallahu_alayhi(self):
        """Prophet's honorific is handled."""
        result = transliterate_arabic("رَسُولِ اللَّهِ ( صلى الله عليه وآله )")
        assert "(s)" in result


class TestTransliterateNarratorIndex:
    """Test batch transliteration of narrator index."""

    def test_transliterate_index(self):
        index = {
            1: "مُحَمَّدُ بْنُ يَحْيَى",
            2: "أَحْمَدَ بْنِ مُحَمَّدٍ",
            3: "عَلِيُّ بْنُ إِبْرَاهِيمَ",
        }
        result = transliterate_narrator_index(index)
        assert len(result) == 3
        assert 1 in result
        assert 2 in result
        assert 3 in result
        assert "Muhammad" in result[1]
        assert "Ahmad" in result[2]
        assert "Ali" in result[3]

    def test_empty_index(self):
        result = transliterate_narrator_index({})
        assert result == {}

    def test_preserves_ids(self):
        """Narrator IDs are preserved as keys."""
        index = {42: "محمد", 99: "علي"}
        result = transliterate_narrator_index(index)
        assert 42 in result
        assert 99 in result
