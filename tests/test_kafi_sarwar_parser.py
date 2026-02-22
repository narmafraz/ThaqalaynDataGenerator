"""Tests for kafi_sarwar.py parser utility functions.

Tests pure functions: we_dont_care, sitepath_from_filepath,
add_chapter_content (with minimal HTML fixtures).
"""
import os
import pytest

from app.kafi_sarwar import (
    SARWAR_TRANSLATION_ID,
    V8_HADITH_CUMSUM,
    sitepath_from_filepath,
    we_dont_care,
)
from app.lib_model import ProcessingReport
from app.models import Chapter, PartType, Verse


class TestSarwarWeDontCare:
    """Tests for kafi_sarwar.we_dont_care()."""

    def test_body_tag_returns_true(self):
        assert we_dont_care("<body>") is True

    def test_body_close_tag_returns_true(self):
        assert we_dont_care("</body>") is True

    def test_body_in_content(self):
        assert we_dont_care("<html><body><p>text</p>") is True

    def test_normal_html_returns_false(self):
        assert we_dont_care("<p>Some hadith text</p>") is False

    def test_empty_string_returns_false(self):
        assert we_dont_care("") is False


class TestSitepathFromFilepath:
    """Tests for sitepath_from_filepath()."""

    def test_converts_forward_slash_path(self):
        result = sitepath_from_filepath("/data/thaqalayn_net/chapter/1/2/3/4.html")
        assert result == "1/2/3/4"

    def test_converts_backslash_path(self):
        result = sitepath_from_filepath(
            "C:\\data\\thaqalayn_net\\chapter\\1\\2\\3\\4.html"
        )
        assert result == "1/2/3/4"

    def test_strips_html_extension(self):
        result = sitepath_from_filepath("/some/path/chapter/1/2/3.html")
        assert not result.endswith(".html")

    def test_single_level_path(self):
        result = sitepath_from_filepath("/data/chapter/1.html")
        assert result == "1"


class TestSarwarConstants:
    """Tests for kafi_sarwar.py constants."""

    def test_sarwar_translation_id(self):
        assert SARWAR_TRANSLATION_ID == "en.sarwar"

    def test_v8_hadith_cumsum_length(self):
        # Volume 8 has 52 chapters
        assert len(V8_HADITH_CUMSUM) == 52

    def test_v8_hadith_cumsum_is_monotonically_increasing(self):
        for i in range(1, len(V8_HADITH_CUMSUM)):
            assert V8_HADITH_CUMSUM[i] >= V8_HADITH_CUMSUM[i - 1], (
                f"V8_HADITH_CUMSUM is not monotonically increasing at index {i}: "
                f"{V8_HADITH_CUMSUM[i-1]} -> {V8_HADITH_CUMSUM[i]}"
            )

    def test_v8_hadith_cumsum_starts_at_1(self):
        assert V8_HADITH_CUMSUM[0] == 1

    def test_v8_hadith_cumsum_ends_at_597(self):
        assert V8_HADITH_CUMSUM[-1] == 597


class TestAddChapterContent:
    """Tests for kafi_sarwar.add_chapter_content() with minimal fixtures."""

    def test_skips_zero_file(self, tmp_path):
        """Files ending in /0.html should be skipped."""
        from app.kafi_sarwar import add_chapter_content

        chapter = Chapter()
        chapter.path = "/books/al-kafi:1:1:1"
        chapter.titles = {"en": "Test", "ar": "اختبار"}
        chapter.verses = []
        chapter.verse_translations = ["en.hubeali"]

        zero_file = tmp_path / "0.html"
        zero_file.write_text("<p>content</p>", encoding="utf-8")

        report = ProcessingReport()
        add_chapter_content(chapter, str(zero_file), report=report)
        assert len(chapter.verses) == 0
        assert len(report.sequence_errors) > 0

    def test_adds_sarwar_translation_id(self, tmp_path):
        """Should add sarwar translation ID to verse_translations."""
        from app.kafi_sarwar import add_chapter_content

        # The parser splits on <hr>, then for each hadith section:
        # - finds all <p> tags
        # - reads RTL paragraphs (Arabic) while is_rtl_tag
        # - reads next paragraph (English translation)
        # is_rtl_tag checks for dir="rtl" attribute
        # First segment must contain <body> so we_dont_care skips it
        hadith_html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">محمد بن يحيى</p>'
            '<p>Muhammad ibn Yahya narrated...</p>'
            '<p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p>'
        )

        filepath = str(tmp_path / "chapter" / "1" / "2" / "3" / "1.html")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(hadith_html)

        # Create a chapter with matching verses
        chapter = Chapter()
        chapter.path = "/books/al-kafi:1:1:1"
        chapter.titles = {"en": "Test Chapter", "ar": "باب"}
        chapter.crumbs = []
        chapter.verse_translations = ["en.hubeali"]

        verse = Verse()
        verse.part_type = PartType.Hadith
        verse.text = ["محمد بن يحيى"]
        verse.translations = {"en.hubeali": ["English hubeali"]}
        chapter.verses = [verse]

        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        assert SARWAR_TRANSLATION_ID in chapter.verse_translations

    def test_doesnt_duplicate_sarwar_translation_id(self, tmp_path):
        """Should not add sarwar translation ID if already present."""
        from app.kafi_sarwar import add_chapter_content

        hadith_html = (
            '<body></body>'
            '<hr>'
            '<p dir="rtl">محمد بن يحيى</p>'
            '<p>Muhammad ibn Yahya narrated...</p>'
            '<p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p>'
        )

        filepath = str(tmp_path / "chapter" / "1" / "2" / "3" / "1.html")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(hadith_html)

        chapter = Chapter()
        chapter.path = "/books/al-kafi:1:1:1"
        chapter.titles = {"en": "Test"}
        chapter.crumbs = []
        chapter.verse_translations = ["en.hubeali", SARWAR_TRANSLATION_ID]

        verse = Verse()
        verse.part_type = PartType.Hadith
        verse.text = ["محمد بن يحيى"]
        verse.translations = {"en.hubeali": ["English"]}
        chapter.verses = [verse]

        report = ProcessingReport()
        add_chapter_content(chapter, filepath, report=report)
        sarwar_count = chapter.verse_translations.count(SARWAR_TRANSLATION_ID)
        assert sarwar_count == 1
