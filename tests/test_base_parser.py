"""Tests for base_parser.py shared utilities.

Tests make_chapter(), make_verse(), register_translation(),
publish_book(), and get_parser_raw_path().
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from app.base_parser import (
    get_parser_raw_path,
    make_chapter,
    make_verse,
    publish_book,
    register_translation,
)
from app.models import Chapter, Language, PartType, Translation, Verse


class TestMakeChapter:
    """Tests for make_chapter() factory function."""

    def test_creates_chapter_with_part_type(self):
        ch = make_chapter(PartType.Chapter, "/books/test:1")
        assert ch.part_type == PartType.Chapter

    def test_creates_chapter_with_path(self):
        ch = make_chapter(PartType.Book, "/books/quran")
        assert ch.path == "/books/quran"

    def test_creates_chapter_with_titles(self):
        titles = {"en": "The Opening", "ar": "الفاتحة"}
        ch = make_chapter(PartType.Chapter, "/books/quran:1", titles=titles)
        assert ch.titles == titles

    def test_creates_chapter_without_titles(self):
        ch = make_chapter(PartType.Chapter, "/books/test:1")
        assert ch.titles is None

    def test_creates_chapter_with_verse_translations(self):
        vt = ["en.qarai", "en.sarwar"]
        ch = make_chapter(PartType.Chapter, "/books/test:1", verse_translations=vt)
        assert ch.verse_translations == vt

    def test_creates_chapter_without_verse_translations(self):
        ch = make_chapter(PartType.Chapter, "/books/test:1")
        assert ch.verse_translations is None

    def test_returns_chapter_instance(self):
        ch = make_chapter(PartType.Volume, "/books/al-kafi:1")
        assert isinstance(ch, Chapter)

    def test_supports_all_part_types(self):
        for pt in [PartType.Book, PartType.Volume, PartType.Chapter, PartType.Section]:
            ch = make_chapter(pt, f"/books/test")
            assert ch.part_type == pt

    def test_chapter_fields_default_to_none(self):
        ch = make_chapter(PartType.Chapter, "/books/test:1")
        assert ch.verses is None
        assert ch.chapters is None
        assert ch.crumbs is None
        assert ch.index is None
        assert ch.nav is None


class TestMakeVerse:
    """Tests for make_verse() factory function."""

    def test_creates_verse_with_part_type(self):
        v = make_verse(PartType.Hadith)
        assert v.part_type == PartType.Hadith

    def test_creates_verse_with_text(self):
        text = ["بِسْمِ اللَّهِ الرَّحْمَنِ الرَّحِيمِ"]
        v = make_verse(PartType.Verse, text=text)
        assert v.text == text

    def test_creates_verse_without_text(self):
        v = make_verse(PartType.Hadith)
        assert v.text is None

    def test_creates_verse_with_translations(self):
        translations = {"en.qarai": ["In the name of Allah"]}
        v = make_verse(PartType.Verse, translations=translations)
        assert v.translations == translations

    def test_creates_verse_with_gradings(self):
        gradings = ["Sahih - Majlisi"]
        v = make_verse(PartType.Hadith, gradings=gradings)
        assert v.gradings == gradings

    def test_creates_verse_with_source_url(self):
        url = "https://thaqalayn.net/hadith/1/1/1/1"
        v = make_verse(PartType.Hadith, source_url=url)
        assert v.source_url == url

    def test_returns_verse_instance(self):
        v = make_verse(PartType.Verse)
        assert isinstance(v, Verse)

    def test_verse_fields_default_to_none(self):
        v = make_verse(PartType.Verse)
        assert v.index is None
        assert v.path is None
        assert v.narrator_chain is None
        assert v.relations is None

    def test_creates_verse_with_all_fields(self):
        v = make_verse(
            PartType.Hadith,
            text=["Arabic text"],
            translations={"en.qarai": ["English"]},
            gradings=["Sahih"],
            source_url="https://example.com",
        )
        assert v.text == ["Arabic text"]
        assert v.translations == {"en.qarai": ["English"]}
        assert v.gradings == ["Sahih"]
        assert v.source_url == "https://example.com"


class TestRegisterTranslation:
    """Tests for register_translation()."""

    @patch("app.base_parser.add_translation")
    def test_creates_translation_with_correct_id(self, mock_add):
        t = register_translation("en.qarai", Language.EN, "Ali Quli Qarai")
        assert t.id == "en.qarai"

    @patch("app.base_parser.add_translation")
    def test_creates_translation_with_correct_language(self, mock_add):
        t = register_translation("fa.makarem", Language.FA, "Naser Makarem Shirazi")
        assert t.lang == "fa"

    @patch("app.base_parser.add_translation")
    def test_creates_translation_with_correct_name(self, mock_add):
        t = register_translation("en.sarwar", Language.EN, "Muhammad Sarwar")
        assert t.name == "Muhammad Sarwar"

    @patch("app.base_parser.add_translation")
    def test_calls_add_translation(self, mock_add):
        t = register_translation("en.qarai", Language.EN, "Ali Quli Qarai")
        mock_add.assert_called_once_with(t)

    @patch("app.base_parser.add_translation")
    def test_returns_translation_instance(self, mock_add):
        t = register_translation("en.qarai", Language.EN, "Qarai")
        assert isinstance(t, Translation)


class TestPublishBook:
    """Tests for publish_book() pipeline."""

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_calls_set_index(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        mock_collect.return_value = {}
        book = Chapter()
        book.path = "/books/test-book"
        publish_book(book)
        mock_set_index.assert_called_once()

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_calls_insert_chapter(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        mock_collect.return_value = {}
        book = Chapter()
        book.path = "/books/test-book"
        publish_book(book)
        mock_insert.assert_called_once_with(book)

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_writes_complete_book_file(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        mock_collect.return_value = {}
        book = Chapter()
        book.path = "/books/al-kafi"
        publish_book(book)
        # Should write to /books/complete/al-kafi
        call_args = mock_write.call_args
        assert call_args[0][0] == "/books/complete/al-kafi"

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_complete_book_has_correct_kind(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        mock_collect.return_value = {}
        book = Chapter()
        book.path = "/books/quran"
        publish_book(book)
        call_args = mock_write.call_args
        data = call_args[0][1]
        assert data["kind"] == "complete_book"
        assert data["index"] == "quran"

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_calls_update_index_files(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        index_maps = {"en": {"/books/test": {"title": "Test"}}}
        mock_collect.return_value = index_maps
        book = Chapter()
        book.path = "/books/test"
        publish_book(book)
        mock_update.assert_called_once_with(index_maps)

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_passes_report_to_set_index(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        from app.lib_model import ProcessingReport

        mock_collect.return_value = {}
        report = ProcessingReport()
        book = Chapter()
        book.path = "/books/test"
        publish_book(book, report=report)
        args = mock_set_index.call_args
        assert args[0][3] == report

    @patch("app.base_parser.update_index_files")
    @patch("app.base_parser.collect_indexes")
    @patch("app.base_parser.write_file")
    @patch("app.base_parser.insert_chapter")
    @patch("app.base_parser.set_index")
    def test_pipeline_order(
        self, mock_set_index, mock_insert, mock_write, mock_collect, mock_update
    ):
        """Verify publish_book calls functions in the correct order."""
        call_order = []
        mock_set_index.side_effect = lambda *a, **kw: call_order.append("set_index")
        mock_insert.side_effect = lambda *a, **kw: call_order.append("insert_chapter")
        mock_write.side_effect = lambda *a, **kw: call_order.append("write_file")
        mock_collect.side_effect = lambda *a, **kw: (
            call_order.append("collect_indexes"),
            {},
        )[1]
        mock_update.side_effect = lambda *a, **kw: call_order.append(
            "update_index_files"
        )

        book = Chapter()
        book.path = "/books/test"
        publish_book(book)

        assert call_order == [
            "set_index",
            "insert_chapter",
            "write_file",
            "collect_indexes",
            "update_index_files",
        ]


class TestGetParserRawPath:
    """Tests for get_parser_raw_path()."""

    def test_returns_path_relative_to_parser(self):
        # Use this test file as the parser reference
        result = get_parser_raw_path(__file__, "some_dir", "file.xml")
        expected = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "raw", "some_dir", "file.xml"
        )
        assert result == expected

    def test_returns_raw_subdirectory(self):
        result = get_parser_raw_path(__file__, "tanzil_net", "quran-data.xml")
        assert os.path.join("raw", "tanzil_net", "quran-data.xml") in result

    def test_single_file_part(self):
        result = get_parser_raw_path(__file__, "file.txt")
        assert result.endswith(os.path.join("raw", "file.txt"))

    def test_no_extra_parts(self):
        result = get_parser_raw_path(__file__)
        assert result.endswith("raw")

    def test_uses_absolute_path(self):
        result = get_parser_raw_path(__file__, "test.xml")
        assert os.path.isabs(result)
