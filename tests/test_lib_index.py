import json
import os

from app.lib_index import collect_indexes, update_index_files, add_translation
from app.models import Chapter, PartType
from app.models.translation import Translation


class TestCollectIndexes:
    """Test index collection from chapter hierarchy"""

    def test_single_chapter_index(self):
        """Test collecting index from a single chapter"""
        chapter = Chapter()
        chapter.titles = {"en": "Test Chapter", "ar": "باب"}
        chapter.path = "/books/test:1"
        chapter.local_index = 1
        chapter.part_type = PartType.Chapter

        index_maps = collect_indexes(chapter)
        assert "en" in index_maps
        assert "/books/test:1" in index_maps["en"]
        assert index_maps["en"]["/books/test:1"]["title"] == "Test Chapter"

    def test_nested_chapters_index(self):
        """Test index collection from nested structure"""
        book = Chapter()
        book.titles = {"en": "Book"}
        book.path = "/books/test"
        book.local_index = None
        book.part_type = PartType.Book

        ch1 = Chapter()
        ch1.titles = {"en": "Chapter 1", "ar": "باب الأول"}
        ch1.path = "/books/test:1"
        ch1.local_index = 1
        ch1.part_type = PartType.Chapter

        ch2 = Chapter()
        ch2.titles = {"en": "Chapter 2"}
        ch2.path = "/books/test:2"
        ch2.local_index = 2
        ch2.part_type = PartType.Chapter

        book.chapters = [ch1, ch2]

        index_maps = collect_indexes(book)

        # Should have entries for book + 2 chapters in English
        assert "/books/test" in index_maps["en"]
        assert "/books/test:1" in index_maps["en"]
        assert "/books/test:2" in index_maps["en"]

        # Arabic should have book entry and ch1
        assert "/books/test:1" in index_maps["ar"]

    def test_chapter_without_titles_skipped(self):
        """Test that chapters without titles are skipped"""
        chapter = Chapter()
        chapter.titles = None
        chapter.path = "/books/test"
        chapter.part_type = PartType.Chapter

        index_maps = collect_indexes(chapter)
        assert index_maps == {}

    def test_index_includes_part_type(self):
        """Test that part_type is included in index entry"""
        chapter = Chapter()
        chapter.titles = {"en": "Test"}
        chapter.path = "/books/test"
        chapter.local_index = 1
        chapter.part_type = PartType.Volume

        index_maps = collect_indexes(chapter)
        assert index_maps["en"]["/books/test"]["part_type"] == PartType.Volume


class TestUpdateIndexFiles:
    """Test update_index_files writes merged index JSON to disk"""

    def test_creates_new_index_file(self, temp_destination_dir):
        """Test that update_index_files creates a new index file"""
        index_maps = {
            "en": {
                "/books/test": {"title": "Test Book", "local_index": 1, "part_type": "Book"}
            }
        }
        update_index_files(index_maps)

        outfile = temp_destination_dir / "index" / "books.en.json"
        assert outfile.exists()
        with open(outfile, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "/books/test" in data
        assert data["/books/test"]["title"] == "Test Book"

    def test_merges_with_existing_index(self, temp_destination_dir):
        """Test that update_index_files merges with pre-existing index data"""
        # Write a pre-existing index file at the DESTINATION_DIR location
        index_dir = temp_destination_dir / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        existing = {"/books/old": {"title": "Old Book"}}
        with open(index_dir / "books.en.json", "w", encoding="utf-8") as f:
            json.dump(existing, f)

        index_maps = {
            "en": {
                "/books/new": {"title": "New Book", "local_index": 1, "part_type": "Book"}
            }
        }
        update_index_files(index_maps)

        outfile = temp_destination_dir / "index" / "books.en.json"
        assert outfile.exists()
        with open(outfile, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "/books/new" in data
        assert "/books/old" in data, "Existing entries must be preserved when merging"

    def test_multiple_languages(self, temp_destination_dir):
        """Test that separate files are created per language"""
        index_maps = {
            "en": {"/books/test": {"title": "English Title", "local_index": 1, "part_type": "Book"}},
            "ar": {"/books/test": {"title": "عنوان عربي", "local_index": 1, "part_type": "Book"}},
        }
        update_index_files(index_maps)

        en_file = temp_destination_dir / "index" / "books.en.json"
        ar_file = temp_destination_dir / "index" / "books.ar.json"
        assert en_file.exists()
        assert ar_file.exists()

        with open(ar_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["/books/test"]["title"] == "عنوان عربي"


    def test_sequential_updates_preserve_all_books(self, temp_destination_dir):
        """Regression test: calling update_index_files for two books sequentially
        must preserve entries from the first book when writing the second.
        This was the root cause of Quran breadcrumbs being missing — Quran
        entries were written first, then overwritten by Al-Kafi because the
        existing-file check used a raw path instead of DESTINATION_DIR."""
        quran_indexes = {
            "en": {
                "/books/quran": {"title": "The Holy Quran", "local_index": None, "part_type": "Book"},
                "/books/quran:1": {"title": "The Opening", "local_index": 1, "part_type": "Chapter"},
                "/books/quran:2": {"title": "The Cow", "local_index": 2, "part_type": "Chapter"},
            }
        }
        update_index_files(quran_indexes)

        kafi_indexes = {
            "en": {
                "/books/al-kafi": {"title": "Al-Kafi", "local_index": None, "part_type": "Book"},
                "/books/al-kafi:1": {"title": "Volume One", "local_index": 1, "part_type": "Volume"},
            }
        }
        update_index_files(kafi_indexes)

        outfile = temp_destination_dir / "index" / "books.en.json"
        with open(outfile, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Both books must be present
        assert "/books/quran" in data, "Quran root must be in index"
        assert "/books/quran:1" in data, "Quran chapters must be in index"
        assert "/books/quran:2" in data, "Quran chapters must be in index"
        assert "/books/al-kafi" in data, "Al-Kafi root must be in index"
        assert "/books/al-kafi:1" in data, "Al-Kafi volumes must be in index"


class TestAddTranslation:
    """Test add_translation writes to translations index"""

    def test_adds_new_translation(self, temp_destination_dir):
        """Test adding a new translation to index"""
        t = Translation(name="Test Translator", id="en.test", lang="en")
        add_translation(t)

        outfile = temp_destination_dir / "index" / "translations.json"
        assert outfile.exists()
        with open(outfile, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "en.test" in data
        assert data["en.test"]["name"] == "Test Translator"
        assert data["en.test"]["lang"] == "en"

    def test_adds_multiple_translations(self, temp_destination_dir):
        """Test adding multiple translations builds up the index"""
        t1 = Translation(name="Translator A", id="en.a", lang="en")
        t2 = Translation(name="Translator B", id="fa.b", lang="fa")

        add_translation(t1)
        add_translation(t2)

        outfile = temp_destination_dir / "index" / "translations.json"
        with open(outfile, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "en.a" in data
        assert "fa.b" in data
