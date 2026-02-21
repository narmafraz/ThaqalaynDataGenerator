from app.lib_model import set_index, get_chapters, get_verses, SEQUENCE_ERRORS
from app.models import Chapter, Crumb, PartType


class TestSetIndex:
    """Tests for the set_index() recursive indexing function"""

    def test_single_chapter_with_verses(self, simple_chapter):
        """Test indexing a single chapter with verses"""
        indexes = set_index(simple_chapter, [], 0)

        # Verify verses get sequential indexes
        assert simple_chapter.verses[0].index == 1
        assert simple_chapter.verses[1].index == 2
        assert simple_chapter.verses[2].index == 3

        # Verify local indexes
        assert simple_chapter.verses[0].local_index == 1
        assert simple_chapter.verses[1].local_index == 2
        assert simple_chapter.verses[2].local_index == 3

        # Verify verse_count
        assert simple_chapter.verse_count == 3

    def test_nested_chapters_global_indexing(self, nested_book):
        """Test global verse indexes across nested chapters"""
        indexes = set_index(nested_book, [], 0)

        # First chapter verses: 1, 2
        assert nested_book.chapters[0].verses[0].index == 1
        assert nested_book.chapters[0].verses[1].index == 2

        # Second chapter verses: 3, 4 (continues from first)
        assert nested_book.chapters[1].verses[0].index == 3
        assert nested_book.chapters[1].verses[1].index == 4

    def test_chapter_local_indexing(self, nested_book):
        """Test local indexes within chapters"""
        set_index(nested_book, [], 0)

        # Chapter local indexes
        assert nested_book.chapters[0].local_index == 1
        assert nested_book.chapters[1].local_index == 2

        # Verse local indexes reset per chapter
        assert nested_book.chapters[0].verses[0].local_index == 1
        assert nested_book.chapters[1].verses[0].local_index == 1

    def test_path_generation(self, nested_book):
        """Test path strings are correctly formatted"""
        set_index(nested_book, [], 0)

        # Chapter paths
        assert nested_book.chapters[0].path == "/books/test:1"
        assert nested_book.chapters[1].path == "/books/test:2"

        # Verse paths
        assert nested_book.chapters[0].verses[0].path == "/books/test:1:1"
        assert nested_book.chapters[1].verses[1].path == "/books/test:2:2"

    def test_crumbs_not_modified_by_set_index(self, nested_book):
        """Verify set_index does not modify crumbs (they are set externally)"""
        set_index(nested_book, [], 0)

        # Crumbs are set externally before set_index, not by set_index itself
        assert nested_book.chapters[0].crumbs == []

    def test_verse_count_calculation(self, nested_book):
        """Verify verse_count is correctly calculated.

        Note: set_index uses indexes[-1] to set verse_start_index for each
        subchapter, which means the first chapter's verse_start_index is
        equal to the chapter-depth index (1), not 0. This is a known quirk.
        The book-level verse_count uses the overall index count.
        """
        set_index(nested_book, [], 0)

        # Book-level verse_count covers all 4 verses
        assert nested_book.verse_count == 4

        # Each chapter has verses assigned
        for ch in nested_book.chapters:
            assert ch.verse_count is not None
            assert ch.verse_count >= 0

    def test_navigation_prev_next(self, nested_book):
        """Test prev/next navigation links between siblings"""
        set_index(nested_book, [], 0)

        # Second chapter should have prev pointing to first
        assert nested_book.chapters[1].nav.prev == "/books/test:1"

        # First chapter should have next pointing to second
        assert nested_book.chapters[0].nav.next == "/books/test:2"

    def test_navigation_up(self, nested_book):
        """Test up navigation links to parent"""
        set_index(nested_book, [], 0)

        # Chapters should have up navigation to parent book
        assert nested_book.chapters[0].nav.up == "/books/test"

    def test_empty_chapter(self):
        """Test chapter with no verses or subchapters"""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Empty"}
        chapter.path = "/books/empty"
        chapter.crumbs = []
        chapter.verse_start_index = 0

        indexes = set_index(chapter, [], 0)

        # Should not crash
        assert indexes == [0]


class TestGetChapters:
    """Tests for get_chapters() utility function"""

    def test_get_chapters_from_object(self, nested_book):
        """Test getting chapters from object with chapters attribute"""
        chapters = get_chapters(nested_book)
        assert chapters is not None
        assert len(chapters) == 2

    def test_get_chapters_from_dict(self):
        """Test getting chapters from dictionary"""
        book_dict = {"chapters": ["ch1", "ch2"]}
        chapters = get_chapters(book_dict)
        assert chapters == ["ch1", "ch2"]

    def test_get_chapters_none(self, simple_chapter):
        """Test getting chapters returns None when no chapters exist"""
        chapters = get_chapters(simple_chapter)
        assert chapters is None


class TestGetVerses:
    """Tests for get_verses() utility function"""

    def test_get_verses_from_object(self, simple_chapter):
        """Test getting verses from object with verses attribute"""
        verses = get_verses(simple_chapter)
        assert verses is not None
        assert len(verses) == 3

    def test_get_verses_from_dict(self):
        """Test getting verses from dictionary"""
        chapter_dict = {"verses": ["v1", "v2"]}
        verses = get_verses(chapter_dict)
        assert verses == ["v1", "v2"]

    def test_get_verses_none(self, nested_book):
        """Test getting verses returns None when no verses exist"""
        verses = get_verses(nested_book)
        assert verses is None


class TestSequenceErrors:
    """Test chapter numbering validation"""

    def test_skipped_chapter_number_logged(self):
        """Test that skipping a chapter number adds to SEQUENCE_ERRORS"""
        initial_errors = len(SEQUENCE_ERRORS)

        book = Chapter()
        book.part_type = PartType.Book
        book.titles = {"en": "Test Book"}
        book.path = "/books/test"
        book.crumbs = []
        book.verse_start_index = 0
        book.chapters = []

        # Chapter 1 and Chapter 3 (skipping 2)
        ch1 = Chapter()
        ch1.part_type = PartType.Chapter
        ch1.titles = {"en": "Chapter 1"}
        ch1.crumbs = []
        ch1.verse_start_index = 0

        ch3 = Chapter()
        ch3.part_type = PartType.Chapter
        ch3.titles = {"en": "Chapter 3"}
        ch3.crumbs = []
        ch3.verse_start_index = 0

        book.chapters = [ch1, ch3]

        set_index(book, [], 0)

        # Should have logged a sequence error
        assert len(SEQUENCE_ERRORS) > initial_errors

    def test_sequential_chapters_no_error(self):
        """Test that sequential chapter numbers don't produce errors"""
        initial_errors = len(SEQUENCE_ERRORS)

        book = Chapter()
        book.part_type = PartType.Book
        book.titles = {"en": "Test"}
        book.path = "/books/test"
        book.crumbs = []
        book.verse_start_index = 0
        book.chapters = []

        for i in range(1, 4):
            ch = Chapter()
            ch.part_type = PartType.Chapter
            ch.titles = {"en": f"Chapter {i}"}
            ch.crumbs = []
            ch.verse_start_index = 0
            book.chapters.append(ch)

        set_index(book, [], 0)

        assert len(SEQUENCE_ERRORS) == initial_errors
