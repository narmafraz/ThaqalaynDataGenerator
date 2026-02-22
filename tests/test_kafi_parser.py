"""Tests for kafi.py parser utility functions.

Tests pure functions that don't require raw data files:
extract_headings, we_dont_care, table_of_contents, join_texts,
is_section_break_tag, is_book_title, is_chapter_title, is_newline,
add_hadith.
"""
import re

import pytest
from bs4 import BeautifulSoup, NavigableString, Tag

from app.kafi import (
    HUBEALI_TRANSLATION_ID,
    add_hadith,
    extract_headings,
    is_book_ending,
    is_book_title,
    is_chapter_title,
    is_newline,
    is_section_break_tag,
    join_texts,
    table_of_contents,
    we_dont_care,
)
from app.models import Chapter, PartType, Verse


def _make_tag(html: str) -> Tag:
    """Parse an HTML snippet and return the first tag."""
    soup = BeautifulSoup(html, "html.parser")
    return soup.find()


def _make_heading_tags(texts: list[str], css_class: str = "Heading1Center") -> list[Tag]:
    """Create heading Tag objects with given class and text."""
    tags = []
    for text in texts:
        html = f'<h1 class="{css_class}">{text}</h1>'
        soup = BeautifulSoup(html, "html.parser")
        tags.append(soup.find("h1"))
    return tags


class TestExtractHeadings:
    """Tests for extract_headings()."""

    def test_two_headings_arabic_and_english(self):
        headings = _make_heading_tags(["كتاب العقل", "The Book of Intellect"])
        result = extract_headings(headings)
        assert result["ar"] == "كتاب العقل"
        assert result["en"] == "The Book of Intellect"

    def test_single_heading_english_only(self):
        headings = _make_heading_tags(["The Book of Proof"])
        result = extract_headings(headings)
        assert result["en"] == "The Book of Proof"
        assert "ar" not in result

    def test_strips_parenthetical_numbering(self):
        headings = _make_heading_tags(["Chapter One (1)"])
        result = extract_headings(headings)
        assert result["en"] == "Chapter One"

    def test_strips_text(self):
        headings = _make_heading_tags(["  كتاب  ", "  Book  "])
        result = extract_headings(headings)
        assert result["ar"] == "كتاب"
        assert result["en"] == "Book"

    def test_empty_headings_raises(self):
        with pytest.raises(AssertionError):
            extract_headings([])


class TestWeDontCare:
    """Tests for we_dont_care() in kafi.py."""

    def test_none_returns_true(self):
        assert we_dont_care(None) is True

    def test_volume_heading_returns_true(self):
        tag = _make_tag('<h1 class="test">AL-KAFI VOLUME 3</h1>')
        assert we_dont_care(tag) is True

    def test_regular_heading_returns_false(self):
        tag = _make_tag('<h1 class="test">The Book of Intellect</h1>')
        assert we_dont_care(tag) is False

    def test_lowercase_volume_returns_false(self):
        tag = _make_tag('<h1 class="test">al-kafi volume 3</h1>')
        # we_dont_care uppercases before matching
        assert we_dont_care(tag) is True


class TestTableOfContents:
    """Tests for table_of_contents()."""

    def test_toc_heading_returns_true(self):
        tag = _make_tag('<h1 class="test">TABLE OF CONTENTS</h1>')
        assert table_of_contents(tag)

    def test_toc_mixed_case_returns_true(self):
        tag = _make_tag('<h1 class="test">Table of Contents</h1>')
        assert table_of_contents(tag)

    def test_regular_heading_returns_none(self):
        tag = _make_tag('<h1 class="test">The Book of Intellect</h1>')
        assert not table_of_contents(tag)


class TestJoinTexts:
    """Tests for join_texts()."""

    def test_joins_with_newlines(self):
        assert join_texts(["line1", "line2", "line3"]) == "line1\nline2\nline3"

    def test_single_text(self):
        assert join_texts(["single"]) == "single"

    def test_empty_list(self):
        assert join_texts([]) == ""


class TestIsSectionBreakTag:
    """Tests for is_section_break_tag()."""

    def test_section_break_class(self):
        tag = _make_tag('<div class="section-break">---</div>')
        assert is_section_break_tag(tag) is True

    def test_no_section_break_class(self):
        tag = _make_tag('<div class="normal">text</div>')
        assert is_section_break_tag(tag) is False

    def test_no_class_attribute(self):
        tag = _make_tag('<div>text</div>')
        assert is_section_break_tag(tag) is False


class TestIsBookTitle:
    """Tests for is_book_title()."""

    def test_book_title_style(self):
        tag = _make_tag(
            '<p style="font-size: x-large; font-weight: bold; '
            'text-align: center; text-decoration: underline">Title</p>'
        )
        assert is_book_title(tag) is True

    def test_xx_large_with_page_break(self):
        tag = _make_tag(
            '<p style="font-size: xx-large; font-weight: bold; '
            'text-align: center; page-break-before: always">Title</p>'
        )
        assert is_book_title(tag) is True

    def test_missing_bold_returns_false(self):
        tag = _make_tag(
            '<p style="font-size: x-large; text-align: center; '
            'text-decoration: underline">Title</p>'
        )
        assert is_book_title(tag) is False


class TestIsChapterTitle:
    """Tests for is_chapter_title()."""

    def test_chapter_title_style(self):
        tag = _make_tag(
            '<p style="font-weight: bold; text-decoration: underline">Chapter</p>'
        )
        assert is_chapter_title(tag) is True

    def test_missing_underline(self):
        tag = _make_tag('<p style="font-weight: bold">Chapter</p>')
        assert is_chapter_title(tag) is False


class TestIsBookEnding:
    """Tests for is_book_ending()."""

    def test_book_ending_style(self):
        tag = _make_tag(
            '<p style="font-weight: bold; text-align: center; text-indent: 0">End</p>'
        )
        assert is_book_ending(tag) is True

    def test_missing_center(self):
        tag = _make_tag('<p style="font-weight: bold; text-indent: 0">End</p>')
        assert is_book_ending(tag) is False


class TestIsNewline:
    """Tests for is_newline()."""

    def test_whitespace_string(self):
        ns = NavigableString("   \n  ")
        assert is_newline(ns)

    def test_empty_string(self):
        ns = NavigableString("")
        assert is_newline(ns)

    def test_text_content(self):
        ns = NavigableString("some text")
        assert not is_newline(ns)

    def test_tag_returns_false(self):
        tag = _make_tag("<p>   </p>")
        assert not is_newline(tag)


class TestAddHadith:
    """Tests for add_hadith()."""

    def test_adds_verse_to_chapter(self):
        chapter = Chapter()
        chapter.verses = []
        add_hadith(chapter, ["Arabic text"], ["English text"])
        assert len(chapter.verses) == 1

    def test_verse_has_arabic_text(self):
        chapter = Chapter()
        chapter.verses = []
        add_hadith(chapter, ["مُحَمَّدُ بْنُ يَحْيَى"], ["Muhammad ibn Yahya"])
        assert chapter.verses[0].text == ["مُحَمَّدُ بْنُ يَحْيَى"]

    def test_verse_has_english_translation(self):
        chapter = Chapter()
        chapter.verses = []
        add_hadith(chapter, ["Arabic"], ["English translation"])
        assert chapter.verses[0].translations[HUBEALI_TRANSLATION_ID] == [
            "English translation"
        ]

    def test_verse_has_hadith_part_type(self):
        chapter = Chapter()
        chapter.verses = []
        add_hadith(chapter, ["Arabic"], ["English"])
        assert chapter.verses[0].part_type == PartType.Hadith

    def test_custom_part_type(self):
        chapter = Chapter()
        chapter.verses = []
        add_hadith(chapter, ["Arabic"], ["English"], part_type=PartType.Heading)
        assert chapter.verses[0].part_type == PartType.Heading

    def test_cleans_up_footnote_references(self):
        chapter = Chapter()
        chapter.verses = []
        hadith_en = ['Some text<a id="fn1"></a><sup>[1]</sup>']
        add_hadith(chapter, ["Arabic"], hadith_en)
        translation = chapter.verses[0].translations[HUBEALI_TRANSLATION_ID][0]
        assert "<sup>" not in translation
        assert "[1]" not in translation

    def test_cleans_up_self_closing_anchor(self):
        chapter = Chapter()
        chapter.verses = []
        hadith_en = ['Text<a id="fn2"/><sup>[2]</sup>']
        add_hadith(chapter, ["Arabic"], hadith_en)
        translation = chapter.verses[0].translations[HUBEALI_TRANSLATION_ID][0]
        assert "<sup>" not in translation

    def test_multiple_hadiths_appended(self):
        chapter = Chapter()
        chapter.verses = []
        add_hadith(chapter, ["Arabic 1"], ["English 1"])
        add_hadith(chapter, ["Arabic 2"], ["English 2"])
        add_hadith(chapter, ["Arabic 3"], ["English 3"])
        assert len(chapter.verses) == 3


class TestKafiConstants:
    """Tests for kafi.py module-level constants and patterns."""

    def test_hubeali_translation_id(self):
        assert HUBEALI_TRANSLATION_ID == "en.hubeali"

    def test_volume_heading_pattern(self):
        from app.kafi import VOLUME_HEADING_PATTERN

        assert VOLUME_HEADING_PATTERN.match("AL-KAFI VOLUME 1")
        assert VOLUME_HEADING_PATTERN.match("AL-KAFI VOLUME 8")
        assert not VOLUME_HEADING_PATTERN.match("THE BOOK OF INTELLECT")

    def test_table_of_contents_pattern(self):
        from app.kafi import TABLE_OF_CONTENTS_PATTERN

        assert TABLE_OF_CONTENTS_PATTERN.match("TABLE OF CONTENTS")
        assert not TABLE_OF_CONTENTS_PATTERN.match("INTRODUCTION")

    def test_end_of_hadith_pattern(self):
        from app.kafi import END_OF_HADITH_PATTERN

        assert END_OF_HADITH_PATTERN.search("some text<sup>[1]</sup>")
        assert END_OF_HADITH_PATTERN.search("text<sup>[42]</sup>  ")
        assert not END_OF_HADITH_PATTERN.search("some text without footnote")

    def test_v8_hadith_title_pattern(self):
        from app.kafi import V8_HADITH_TITLE_PATTERN

        assert V8_HADITH_TITLE_PATTERN.match("H 1234")
        assert V8_HADITH_TITLE_PATTERN.match("H 5")
        assert not V8_HADITH_TITLE_PATTERN.match("Some other text")
