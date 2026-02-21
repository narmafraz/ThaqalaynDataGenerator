from app.lib_index import collect_indexes
from app.models import Chapter, PartType


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
