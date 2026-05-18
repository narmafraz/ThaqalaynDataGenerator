from app.lib_model import set_index, get_chapters, get_verses, ProcessingReport, SEQUENCE_ERRORS
from app.models import Chapter, Crumb, PartType, Verse


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
        """Verify verse_count is correctly calculated."""
        set_index(nested_book, [], 0)

        # Book has 2 chapters × 2 verses = 4
        assert nested_book.verse_count == 4
        # First chapter: starts at 0, has 2 verses
        assert nested_book.chapters[0].verse_start_index == 0
        assert nested_book.chapters[0].verse_count == 2
        # Second chapter: starts after first, has 2 verses
        assert nested_book.chapters[1].verse_start_index == 2
        assert nested_book.chapters[1].verse_count == 2

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
        """Test that skipping a chapter number adds to report.sequence_errors"""
        report = ProcessingReport()

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

        set_index(book, [], 0, report)

        # Should have logged a sequence error
        assert len(report.sequence_errors) == 1
        assert "Chapter 2" in report.sequence_errors[0]

    def test_sequential_chapters_no_error(self):
        """Test that sequential chapter numbers don't produce errors"""
        report = ProcessingReport()

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

        set_index(book, [], 0, report)

        assert len(report.sequence_errors) == 0


def _make_book(slug: str = "test") -> Chapter:
    """Helper: build a top-level book chapter for use in indexing tests."""
    book = Chapter()
    book.part_type = PartType.Book
    book.titles = {"en": "Test"}
    book.path = f"/books/{slug}"
    book.crumbs = []
    book.verse_start_index = 0
    book.chapters = []
    return book


def _make_chapter(title: str, num_verses: int, part_type: PartType = PartType.Chapter) -> Chapter:
    """Helper: leaf chapter with N hadith verses."""
    ch = Chapter()
    ch.part_type = part_type
    ch.titles = {"en": title}
    ch.crumbs = []
    ch.verse_start_index = 0
    ch.verses = []
    for _ in range(num_verses):
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["text"]
        ch.verses.append(v)
    return ch


def _make_intermediate(title: str) -> Chapter:
    """Helper: chapter that contains sub-chapters (no direct verses)."""
    ch = Chapter()
    ch.part_type = PartType.Chapter
    ch.titles = {"en": title}
    ch.crumbs = []
    ch.verse_start_index = 0
    ch.chapters = []
    return ch


class TestVerseStartIndexCorrectness:
    """Tests for verse_start_index + verse_count correctness across the
    full set of structural cases the corpus contains:

    - leaf chapter (verses only)
    - parent → leaf chapters
    - parent → intermediate → leaf (e.g. al-kafi: vol → book → chapter → verses)
    - **mixed depth** (e.g. man-la-yahduruhu-al-faqih: some chapters have
      verses directly while their siblings have sub-chapter children)

    Before the 2026-05-18 fix, the first subchapter at each level was given
    an incorrect verse_start_index because the code used `indexes[-1]` as a
    proxy for the cumulative-verse counter — which only works once recursion
    has descended to the verse depth. Mixed-depth trees broke this assumption
    in subtler ways (a verse counter at one depth got reused as a chapter
    counter at the same depth in a sibling subtree).

    The fix introduces a dedicated `verse_counter` distinct from `indexes`.
    These tests pin the corrected behaviour.
    """

    def test_first_chapter_starts_at_zero(self):
        """The first subchapter's verse_start_index is 0 — no verses came before it."""
        book = _make_book()
        book.chapters.append(_make_chapter("Chapter 1", 3))

        set_index(book, [], 0)

        assert book.chapters[0].verse_start_index == 0
        assert book.chapters[0].verse_count == 3
        assert book.verse_count == 3

    def test_two_leaf_chapters_chain_cleanly(self):
        """Second chapter starts where the first one ended."""
        book = _make_book()
        book.chapters.append(_make_chapter("Chapter 1", 3))
        book.chapters.append(_make_chapter("Chapter 2", 5))

        set_index(book, [], 0)

        assert book.chapters[0].verse_start_index == 0
        assert book.chapters[0].verse_count == 3
        assert book.chapters[1].verse_start_index == 3
        assert book.chapters[1].verse_count == 5
        assert book.verse_count == 8

    def test_intermediate_chapter_aggregates_sub_counts(self):
        """A chapter containing sub-chapters has verse_count = sum of sub verses."""
        book = _make_book()
        section = _make_intermediate("Section 1")
        section.chapters.append(_make_chapter("Chapter A", 2))
        section.chapters.append(_make_chapter("Chapter B", 4))
        book.chapters.append(section)

        set_index(book, [], 0)

        assert section.chapters[0].verse_start_index == 0
        assert section.chapters[0].verse_count == 2
        assert section.chapters[1].verse_start_index == 2
        assert section.chapters[1].verse_count == 4
        # Intermediate aggregates direct + recursive
        assert section.verse_start_index == 0
        assert section.verse_count == 6
        assert book.verse_count == 6

    def test_mixed_depth_book_first_deeper_subtree_starts_correctly(self):
        """Reproduction of the man-la-yahduruhu-al-faqih bug.

        Vol 1 has chapters with verses directly (depth=2 leaves).
        Vol 2 has chapters whose children are chapters with verses (depth=3 leaves).
        The first chapter of vol 2's first sub-chapter should have
        verse_start_index = (verses in vol 1), not the chapter counter.
        """
        book = _make_book("faqih")

        vol1 = _make_intermediate("Volume 1")
        vol1.chapters.append(_make_chapter("Prelude", 1))
        vol1.chapters.append(_make_chapter("Knowledge", 36))
        # vol 1 total = 37 verses
        book.chapters.append(vol1)

        vol2 = _make_intermediate("Volume 2")
        zakat = _make_intermediate("Book of Zakat")
        zakat.chapters.append(_make_chapter("Chapter on Shyness", 1))
        zakat.chapters.append(_make_chapter("Chapter on Categories", 43))
        # zakat total = 44 verses
        vol2.chapters.append(zakat)
        book.chapters.append(vol2)

        set_index(book, [], 0)

        # Volume 1 inner cross-checks
        assert vol1.chapters[0].verse_start_index == 0
        assert vol1.chapters[0].verse_count == 1
        assert vol1.chapters[1].verse_start_index == 1
        assert vol1.chapters[1].verse_count == 36
        assert vol1.verse_start_index == 0
        assert vol1.verse_count == 37

        # Volume 2 — this is the previously buggy region
        assert vol2.verse_start_index == 37
        assert zakat.verse_start_index == 37
        # The first chapter under zakat is the one that was reading "1570 / -1569"
        # in production data; here it should be the cumulative 37 + count 1.
        assert zakat.chapters[0].verse_start_index == 37
        assert zakat.chapters[0].verse_count == 1
        assert zakat.chapters[1].verse_start_index == 38
        assert zakat.chapters[1].verse_count == 43
        assert zakat.verse_count == 44
        assert vol2.verse_count == 44

        assert book.verse_count == 81  # 37 + 44

    def test_verse_index_is_globally_monotonic_in_mixed_depth_book(self):
        """Across the whole book, verse.index increments 1, 2, 3, ... regardless
        of where in the tree the verse lives."""
        book = _make_book("mixed")
        vol1 = _make_intermediate("V1")
        vol1.chapters.append(_make_chapter("Prelude", 1))
        vol1.chapters.append(_make_chapter("Ch", 2))
        book.chapters.append(vol1)
        vol2 = _make_intermediate("V2")
        zakat = _make_intermediate("Zakat")
        zakat.chapters.append(_make_chapter("Sub", 3))
        vol2.chapters.append(zakat)
        book.chapters.append(vol2)

        set_index(book, [], 0)

        # Flatten verses in tree-order
        verses_in_order = (
            vol1.chapters[0].verses
            + vol1.chapters[1].verses
            + zakat.chapters[0].verses
        )
        for i, v in enumerate(verses_in_order, start=1):
            assert v.index == i, f"verse {i} got index {v.index}"

    def test_heading_verses_do_not_count(self):
        """PartType.Heading verses are skipped by the counter — they're navigational
        anchors, not countable units."""
        book = _make_book()
        ch = _make_chapter("Mixed", 0)  # no auto-generated verses
        # Add a heading then two hadith
        h = Verse()
        h.part_type = PartType.Heading
        h.text = ["a section header"]
        ch.verses.append(h)
        for _ in range(2):
            v = Verse()
            v.part_type = PartType.Hadith
            v.text = ["text"]
            ch.verses.append(v)
        book.chapters.append(ch)

        set_index(book, [], 0)

        assert ch.verse_start_index == 0
        assert ch.verse_count == 2  # only the 2 hadith
        assert book.verse_count == 2

    def test_empty_first_chapter_does_not_offset_siblings(self):
        """An empty chapter (no verses) shouldn't shift the counter for its siblings."""
        book = _make_book()
        book.chapters.append(_make_chapter("Empty", 0))
        book.chapters.append(_make_chapter("Five", 5))

        set_index(book, [], 0)

        assert book.chapters[0].verse_start_index == 0
        assert book.chapters[0].verse_count == 0
        assert book.chapters[1].verse_start_index == 0
        assert book.chapters[1].verse_count == 5
        assert book.verse_count == 5

    def test_three_level_kafi_shape(self):
        """Mimic the al-kafi shape: Volume → Book → Chapter → verses (depth 4)."""
        book = _make_book("al-kafi")
        vol = _make_intermediate("Volume 1")
        usul = _make_intermediate("Book of Knowledge")
        usul.chapters.append(_make_chapter("Chapter on Intellect", 36))
        usul.chapters.append(_make_chapter("Chapter on Knowledge", 24))
        vol.chapters.append(usul)
        book.chapters.append(vol)

        set_index(book, [], 0)

        assert usul.chapters[0].verse_start_index == 0
        assert usul.chapters[0].verse_count == 36
        assert usul.chapters[1].verse_start_index == 36
        assert usul.chapters[1].verse_count == 24
        assert usul.verse_count == 60
        assert vol.verse_count == 60
        assert book.verse_count == 60

    def test_three_volumes_chain_cleanly(self):
        """Top-level volumes (sibling subtrees) accumulate across the whole book."""
        book = _make_book("multi")
        for i, n in enumerate([10, 20, 5], start=1):
            vol = _make_intermediate(f"Volume {i}")
            vol.chapters.append(_make_chapter("only chapter", n))
            book.chapters.append(vol)

        set_index(book, [], 0)

        assert book.chapters[0].verse_start_index == 0
        assert book.chapters[0].verse_count == 10
        assert book.chapters[1].verse_start_index == 10
        assert book.chapters[1].verse_count == 20
        assert book.chapters[2].verse_start_index == 30
        assert book.chapters[2].verse_count == 5
        assert book.verse_count == 35

    def test_set_index_is_idempotent_with_fresh_counter(self):
        """Calling set_index a second time on the same tree (with a fresh
        counter) must produce the same shape — no leaked state in default args."""
        book = _make_book()
        book.chapters.append(_make_chapter("A", 3))
        book.chapters.append(_make_chapter("B", 2))

        set_index(book, [], 0)
        first_starts = [c.verse_start_index for c in book.chapters]
        first_counts = [c.verse_count for c in book.chapters]

        # Reset chapter state to simulate fresh invocation
        for c in book.chapters:
            c.verse_start_index = 0
            c.verse_count = None
            c.index = None
            c.local_index = None
            c.path = None
            for v in c.verses:
                v.index = None
                v.local_index = None
                v.path = None

        set_index(book, [], 0)
        second_starts = [c.verse_start_index for c in book.chapters]
        second_counts = [c.verse_count for c in book.chapters]

        assert first_starts == second_starts == [0, 3]
        assert first_counts == second_counts == [3, 2]


class TestProcessingReport:
    """Test the ProcessingReport class for error accumulation."""

    def test_fresh_report_is_empty(self):
        """A new report has no errors and zero counter."""
        report = ProcessingReport()
        assert report.sequence_errors == []
        assert report.narrations_without_narrators == 0

    def test_add_sequence_error(self):
        """Errors are accumulated in order."""
        report = ProcessingReport()
        report.add_sequence_error("error 1")
        report.add_sequence_error("error 2")
        assert report.sequence_errors == ["error 1", "error 2"]

    def test_reports_are_isolated(self):
        """Two report instances do not share state."""
        r1 = ProcessingReport()
        r2 = ProcessingReport()
        r1.add_sequence_error("only in r1")
        r1.narrations_without_narrators = 5
        assert r2.sequence_errors == []
        assert r2.narrations_without_narrators == 0
