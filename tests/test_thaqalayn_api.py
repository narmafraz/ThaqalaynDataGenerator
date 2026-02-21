"""Tests for the ThaqalaynAPI JSON transformer."""

import json
import os
from collections import OrderedDict

import pytest
from fastapi.encoders import jsonable_encoder

from app.book_registry import BookConfig
from app.models import Chapter, Verse, Language, PartType
from app.models.enums import PartType as PartTypeEnum
from app.thaqalayn_api import (
    build_chapter_from_hadiths,
    build_verse,
    get_category_title,
    get_chapter_title,
    group_hadiths,
    has_multiple_categories,
    has_multiple_volumes,
    make_translator_id,
    transform_book,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _sample_hadith(
    hadith_id=1,
    volume=1,
    category="Content",
    category_id="1",
    chapter="Chapter One",
    chapter_in_category_id=1,
    arabic="نص عربي",
    english="English text",
    french="",
    majlisi="",
    mohseni="",
    behbudi="",
    url="",
    author="Author Name",
    translator="Translator Name",
):
    return {
        "id": hadith_id,
        "bookId": "test-book",
        "book": "Test Book",
        "category": category,
        "categoryId": category_id,
        "chapter": chapter,
        "chapterInCategoryId": chapter_in_category_id,
        "author": author,
        "translator": translator,
        "arabicText": arabic,
        "englishText": english,
        "frenchText": french,
        "thaqalaynSanad": "",
        "thaqalaynMatn": "",
        "majlisiGrading": majlisi,
        "mohseniGrading": mohseni,
        "behbudiGrading": behbudi,
        "gradingsFull": [],
        "volume": volume,
        "URL": url,
    }


def _make_book_config(**kwargs):
    defaults = dict(
        slug="test-book",
        index=99,
        path="/books/test-book",
        titles={Language.EN.value: "Test Book"},
    )
    defaults.update(kwargs)
    return BookConfig(**defaults)


@pytest.fixture
def simple_hadiths():
    """Three hadiths in one chapter."""
    return [
        _sample_hadith(hadith_id=1, chapter="Chapter One", chapter_in_category_id=1),
        _sample_hadith(hadith_id=2, chapter="Chapter One", chapter_in_category_id=1),
        _sample_hadith(hadith_id=3, chapter="Chapter One", chapter_in_category_id=1),
    ]


@pytest.fixture
def multi_chapter_hadiths():
    """Hadiths spanning two chapters."""
    return [
        _sample_hadith(hadith_id=1, chapter="Ch 1", chapter_in_category_id=1),
        _sample_hadith(hadith_id=2, chapter="Ch 1", chapter_in_category_id=1),
        _sample_hadith(hadith_id=3, chapter="Ch 2", chapter_in_category_id=2),
    ]


@pytest.fixture
def multi_volume_hadiths():
    """Hadiths in two volumes."""
    return [
        _sample_hadith(hadith_id=1, volume=1, chapter="V1 Ch1", chapter_in_category_id=1),
        _sample_hadith(hadith_id=2, volume=2, chapter="V2 Ch1", chapter_in_category_id=1),
    ]


@pytest.fixture
def multi_category_hadiths():
    """Hadiths in two categories within same volume."""
    return [
        _sample_hadith(hadith_id=1, category="Introduction", category_id="0", chapter="Intro Ch", chapter_in_category_id=1),
        _sample_hadith(hadith_id=2, category="Content", category_id="1", chapter="Content Ch", chapter_in_category_id=1),
    ]


@pytest.fixture
def hadiths_with_gradings():
    """Hadiths with grading fields."""
    return [
        _sample_hadith(
            hadith_id=1,
            majlisi="Sahih",
            mohseni="Mu'tabar",
            behbudi="Sahih",
            url="https://thaqalayn.net/hadith/25/1/1/1",
        ),
    ]


@pytest.fixture
def hadiths_with_french():
    """Hadiths with French translations."""
    return [
        _sample_hadith(
            hadith_id=1,
            french="Texte français",
        ),
    ]


# ---------------------------------------------------------------------------
# Tests: make_translator_id
# ---------------------------------------------------------------------------

class TestMakeTranslatorId:

    def test_simple_name(self):
        assert make_translator_id("Badr Shahin") == "en.badr-shahin"

    def test_with_lang(self):
        assert make_translator_id("Badr Shahin", "fr") == "fr.badr-shahin"

    def test_removes_dots(self):
        assert make_translator_id("Dr. Smith") == "en.dr-smith"


# ---------------------------------------------------------------------------
# Tests: group_hadiths
# ---------------------------------------------------------------------------

class TestGroupHadiths:

    def test_single_group(self, simple_hadiths):
        grouped = group_hadiths(simple_hadiths)
        assert len(grouped) == 1  # one volume
        assert 1 in grouped
        assert "1" in grouped[1]  # one category
        assert 1 in grouped[1]["1"]  # one chapter
        assert len(grouped[1]["1"][1]) == 3

    def test_multi_chapter(self, multi_chapter_hadiths):
        grouped = group_hadiths(multi_chapter_hadiths)
        chapters = grouped[1]["1"]
        assert len(chapters) == 2
        assert len(chapters[1]) == 2  # Ch 1 has 2 hadiths
        assert len(chapters[2]) == 1  # Ch 2 has 1 hadith

    def test_multi_volume(self, multi_volume_hadiths):
        grouped = group_hadiths(multi_volume_hadiths)
        assert len(grouped) == 2
        assert 1 in grouped
        assert 2 in grouped

    def test_multi_category(self, multi_category_hadiths):
        grouped = group_hadiths(multi_category_hadiths)
        vol = grouped[1]
        assert len(vol) == 2  # two categories
        assert "0" in vol
        assert "1" in vol

    def test_preserves_order(self):
        hadiths = [
            _sample_hadith(hadith_id=1, category_id="3", chapter_in_category_id=2),
            _sample_hadith(hadith_id=2, category_id="1", chapter_in_category_id=1),
            _sample_hadith(hadith_id=3, category_id="3", chapter_in_category_id=1),
        ]
        grouped = group_hadiths(hadiths)
        cat_ids = list(grouped[1].keys())
        assert cat_ids == ["3", "1"]  # first-seen order


# ---------------------------------------------------------------------------
# Tests: build_verse
# ---------------------------------------------------------------------------

class TestBuildVerse:

    def test_basic_verse(self):
        h = _sample_hadith(arabic="نص عربي", english="English text")
        v = build_verse(h, "en.test")

        assert v.part_type == PartType.Hadith
        assert v.text == ["نص عربي"]
        assert v.translations["en.test"] == ["English text"]

    def test_verse_with_gradings(self, hadiths_with_gradings):
        v = build_verse(hadiths_with_gradings[0], "en.test")
        assert v.gradings["majlisi"] == "Sahih"
        assert v.gradings["mohseni"] == "Mu'tabar"
        assert v.gradings["behbudi"] == "Sahih"

    def test_verse_with_source_url(self, hadiths_with_gradings):
        v = build_verse(hadiths_with_gradings[0], "en.test")
        assert v.source_url == "https://thaqalayn.net/hadith/25/1/1/1"

    def test_verse_without_gradings(self, simple_hadiths):
        v = build_verse(simple_hadiths[0], "en.test")
        assert v.gradings is None

    def test_verse_without_source_url(self, simple_hadiths):
        v = build_verse(simple_hadiths[0], "en.test")
        assert v.source_url is None

    def test_verse_with_french(self, hadiths_with_french):
        v = build_verse(hadiths_with_french[0], "en.test", fr_translator_id="fr.test")
        assert v.translations["en.test"] == ["English text"]
        assert v.translations["fr.test"] == ["Texte français"]

    def test_verse_empty_french_ignored(self, simple_hadiths):
        v = build_verse(simple_hadiths[0], "en.test", fr_translator_id="fr.test")
        assert "fr.test" not in (v.translations or {})

    def test_verse_partial_gradings(self):
        h = _sample_hadith(majlisi="Sahih")  # only majlisi set
        v = build_verse(h, "en.test")
        assert v.gradings == {"majlisi": "Sahih"}
        assert "mohseni" not in v.gradings
        assert "behbudi" not in v.gradings


# ---------------------------------------------------------------------------
# Tests: build_chapter_from_hadiths
# ---------------------------------------------------------------------------

class TestBuildChapter:

    def test_chapter_structure(self, simple_hadiths):
        ch = build_chapter_from_hadiths(simple_hadiths, "Chapter One", "en.test")
        assert ch.part_type == PartType.Chapter
        assert ch.titles[Language.EN.value] == "Chapter One"
        assert len(ch.verses) == 3

    def test_chapter_verses_are_hadiths(self, simple_hadiths):
        ch = build_chapter_from_hadiths(simple_hadiths, "Test", "en.test")
        for v in ch.verses:
            assert v.part_type == PartType.Hadith


# ---------------------------------------------------------------------------
# Tests: helper functions
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_has_multiple_volumes(self, multi_volume_hadiths):
        grouped = group_hadiths(multi_volume_hadiths)
        assert has_multiple_volumes(grouped) is True

    def test_has_single_volume(self, simple_hadiths):
        grouped = group_hadiths(simple_hadiths)
        assert has_multiple_volumes(grouped) is False

    def test_has_multiple_categories(self, multi_category_hadiths):
        grouped = group_hadiths(multi_category_hadiths)
        assert has_multiple_categories(grouped[1]) is True

    def test_has_single_category(self, simple_hadiths):
        grouped = group_hadiths(simple_hadiths)
        assert has_multiple_categories(grouped[1]) is False

    def test_get_category_title(self):
        hadiths = [_sample_hadith(category="Introduction")]
        assert get_category_title(hadiths) == "Introduction"

    def test_get_chapter_title(self):
        hadiths = [_sample_hadith(chapter="My Chapter")]
        assert get_chapter_title(hadiths) == "My Chapter"


# ---------------------------------------------------------------------------
# Tests: transform_book (integration, needs temp dir + raw data fixture)
# ---------------------------------------------------------------------------

class TestTransformBook:

    @pytest.fixture
    def raw_data_dir(self, tmp_path, monkeypatch):
        """Set up temporary raw data and destination directories."""
        # Destination dir for output
        dest_dir = tmp_path / "data"
        dest_dir.mkdir()
        monkeypatch.setenv("DESTINATION_DIR", str(dest_dir) + "/")

        # Raw data dir
        raw_dir = tmp_path / "raw" / "thaqalayn_api" / "test-book"
        raw_dir.mkdir(parents=True)

        # Monkey-patch get_raw_path to use our temp dir
        import app.thaqalayn_api as tapi
        monkeypatch.setattr(tapi, "get_raw_path", lambda folder: str(tmp_path / "raw" / "thaqalayn_api" / folder))

        return raw_dir

    def _write_hadiths(self, raw_dir, hadiths):
        data = {
            "source": "test",
            "total_hadiths": len(hadiths),
            "hadiths": hadiths,
        }
        with open(raw_dir / "hadiths.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def test_simple_book_structure(self, raw_data_dir):
        """Single volume, single category, one chapter."""
        hadiths = [
            _sample_hadith(hadith_id=i, chapter="Chapter One", chapter_in_category_id=1)
            for i in range(1, 4)
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        assert book.part_type == PartType.Book
        assert book.titles[Language.EN.value] == "Test Book"
        # Single volume + single category = chapters directly under book
        assert len(book.chapters) == 1
        assert book.chapters[0].part_type == PartType.Chapter
        assert len(book.chapters[0].verses) == 3

    def test_multi_chapter_book(self, raw_data_dir):
        """Single volume, single category, two chapters."""
        hadiths = [
            _sample_hadith(hadith_id=1, chapter="Ch 1", chapter_in_category_id=1),
            _sample_hadith(hadith_id=2, chapter="Ch 1", chapter_in_category_id=1),
            _sample_hadith(hadith_id=3, chapter="Ch 2", chapter_in_category_id=2),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        assert len(book.chapters) == 2
        assert book.chapters[0].titles[Language.EN.value] == "Ch 1"
        assert book.chapters[1].titles[Language.EN.value] == "Ch 2"
        assert len(book.chapters[0].verses) == 2
        assert len(book.chapters[1].verses) == 1

    def test_multi_volume_book(self, raw_data_dir):
        """Two volumes, each with one chapter."""
        hadiths = [
            _sample_hadith(hadith_id=1, volume=1, chapter="V1Ch1", chapter_in_category_id=1),
            _sample_hadith(hadith_id=2, volume=2, chapter="V2Ch1", chapter_in_category_id=1),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        assert len(book.chapters) == 2
        assert book.chapters[0].part_type == PartType.Volume
        assert book.chapters[0].titles[Language.EN.value] == "Volume 1"
        assert book.chapters[1].titles[Language.EN.value] == "Volume 2"
        # Each volume has one chapter
        assert len(book.chapters[0].chapters) == 1
        assert len(book.chapters[1].chapters) == 1

    def test_multi_category_book(self, raw_data_dir):
        """Single volume, two categories."""
        hadiths = [
            _sample_hadith(hadith_id=1, category="Introduction", category_id="0", chapter="Intro", chapter_in_category_id=1),
            _sample_hadith(hadith_id=2, category="Content", category_id="1", chapter="Main", chapter_in_category_id=1),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        assert len(book.chapters) == 2
        assert book.chapters[0].part_type == PartType.Section
        assert book.chapters[0].titles[Language.EN.value] == "Introduction"
        assert book.chapters[1].titles[Language.EN.value] == "Content"

    def test_indexes_assigned(self, raw_data_dir):
        """set_index assigns paths and indexes to all levels."""
        hadiths = [
            _sample_hadith(hadith_id=1, chapter="Ch 1", chapter_in_category_id=1),
            _sample_hadith(hadith_id=2, chapter="Ch 2", chapter_in_category_id=2),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        # Chapters should have paths assigned by set_index
        assert book.chapters[0].path is not None
        assert book.chapters[1].path is not None
        # Verses should have paths
        assert book.chapters[0].verses[0].path is not None
        assert book.chapters[0].verses[0].index is not None

    def test_verse_translations_set(self, raw_data_dir):
        """verse_translations is set on book and chapters."""
        hadiths = [_sample_hadith(hadith_id=1)]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        assert "en.test-translator" in book.verse_translations
        assert "en.test-translator" in book.chapters[0].verse_translations

    def test_gradings_preserved(self, raw_data_dir):
        """Gradings from API are preserved in verse objects."""
        hadiths = [
            _sample_hadith(hadith_id=1, majlisi="Sahih", mohseni="Mu'tabar"),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        verse = book.chapters[0].verses[0]
        assert verse.gradings["majlisi"] == "Sahih"
        assert verse.gradings["mohseni"] == "Mu'tabar"

    def test_french_translations(self, raw_data_dir):
        """French translations from frenchText field are included."""
        hadiths = [
            _sample_hadith(hadith_id=1, french="Texte français"),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(
            config, "test-book", "Test Translator",
            fr_translator_name="French Translator",
        )

        verse = book.chapters[0].verses[0]
        assert "fr.french-translator" in verse.translations
        assert verse.translations["fr.french-translator"] == ["Texte français"]
        assert "fr.french-translator" in book.verse_translations

    def test_book_metadata(self, raw_data_dir):
        """Book metadata (author, source_url) is set from config."""
        hadiths = [_sample_hadith(hadith_id=1)]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config(
            author={Language.EN.value: "Test Author"},
            source_url="https://example.com",
        )
        book = transform_book(config, "test-book", "Test Translator")

        assert book.author[Language.EN.value] == "Test Author"
        assert book.source_url == "https://example.com"

    def test_serialization_round_trip(self, raw_data_dir):
        """Transformed book serializes to JSON and matches expected schema."""
        hadiths = [
            _sample_hadith(hadith_id=1, majlisi="Sahih", url="https://example.com/1"),
            _sample_hadith(hadith_id=2, chapter="Ch 2", chapter_in_category_id=2),
        ]
        self._write_hadiths(raw_data_dir, hadiths)

        config = _make_book_config()
        book = transform_book(config, "test-book", "Test Translator")

        json_data = jsonable_encoder(book)

        # Verify structure
        assert json_data["part_type"] == "Book"
        assert len(json_data["chapters"]) == 2

        # Verify first verse
        v = json_data["chapters"][0]["verses"][0]
        assert v["text"] == ["نص عربي"]
        assert v["part_type"] == "Hadith"
        assert v["gradings"]["majlisi"] == "Sahih"
        assert v["source_url"] == "https://example.com/1"

    def test_empty_hadiths_raises(self, raw_data_dir):
        """Empty hadiths list raises ValueError."""
        self._write_hadiths(raw_data_dir, [])

        config = _make_book_config()
        with pytest.raises(ValueError, match="No hadiths found"):
            transform_book(config, "test-book", "Test Translator")
