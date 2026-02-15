from app.lib_model import set_index, get_chapters, get_verses
from app.models import Chapter, Crumb, PartType


class TestSetIndex:
    """Tests for the set_index() recursive indexing function"""

    def test_single_chapter_with_verses(self, simple_chapter):
        """Test indexing a single chapter with verses"""
        master_index = {}
        indexes = set_index(simple_chapter, [], 0, master_index)

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
        master_index = {}
        indexes = set_index(nested_book, [], 0, master_index)

        # First chapter verses: 1, 2
        assert nested_book.chapters[0].verses[0].index == 1
        assert nested_book.chapters[0].verses[1].index == 2

        # Second chapter verses: 3, 4 (continues from first)
        assert nested_book.chapters[1].verses[0].index == 3
        assert nested_book.chapters[1].verses[1].index == 4

    def test_chapter_local_indexing(self, nested_book):
        """Test local indexes within chapters"""
        master_index = {}
        set_index(nested_book, [], 0, master_index)

        # Chapter local indexes
        assert nested_book.chapters[0].local_index == 1
        assert nested_book.chapters[1].local_index == 2

        # Verse local indexes reset per chapter
        assert nested_book.chapters[0].verses[0].local_index == 1
        assert nested_book.chapters[1].verses[0].local_index == 1

    def test_path_generation(self, nested_book):
        """Test path strings are correctly formatted"""
        master_index = {}
        set_index(nested_book, [], 0, master_index)

        # Chapter paths
        assert nested_book.chapters[0].path == "/books/test:1"
        assert nested_book.chapters[1].path == "/books/test:2"

        # Verse paths
        assert nested_book.chapters[0].verses[0].path == "/books/test:1:1"
        assert nested_book.chapters[1].verses[1].path == "/books/test:2:2"

    def test_breadcrumb_generation(self, nested_book):
        """Verify breadcrumbs contain proper hierarchy"""
        master_index = {}
        set_index(nested_book, [], 0, master_index)

        # First chapter should have 1 crumb (itself)
        assert len(nested_book.chapters[0].crumbs) == 1
        assert nested_book.chapters[0].crumbs[0].path == "/books/test:1"

    def test_verse_count_calculation(self, nested_book):
        """Verify verse_count is correctly calculated"""
        master_index = {}
        set_index(nested_book, [], 0, master_index)

        # Each chapter has 2 verses
        assert nested_book.chapters[0].verse_count == 2
        assert nested_book.chapters[1].verse_count == 2

        # Book has total of 4 verses
        assert nested_book.verse_count == 4

    def test_navigation_prev_next(self, nested_book):
        """Test prev/next navigation links between siblings"""
        master_index = {}
        set_index(nested_book, [], 0, master_index)

        # Second chapter should have prev pointing to first
        assert nested_book.chapters[1].nav.prev.path == "/books/test:1"

        # First chapter should have next pointing to second
        assert nested_book.chapters[0].nav.next.path == "/books/test:2"

    def test_navigation_up(self, nested_book):
        """Test up navigation links to parent"""
        # Add parent crumb to book
        parent_crumb = Crumb()
        parent_crumb.path = "/books"
        parent_crumb.titles = {"en": "Books"}
        nested_book.crumbs = [parent_crumb]

        master_index = {}
        set_index(nested_book, [], 0, master_index)

        # Chapters should have up navigation to parent
        assert nested_book.chapters[0].nav.up.path == "/books"

    def test_empty_chapter(self):
        """Test chapter with no verses or subchapters"""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Empty"}
        chapter.path = "/books/empty"
        chapter.crumbs = []
        chapter.verse_start_index = 0

        master_index = {}
        indexes = set_index(chapter, [], 0, master_index)

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
