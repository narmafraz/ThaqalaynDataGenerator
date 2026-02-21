from app.link_quran_kafi import (
    process_translation_text, QURAN_QUOTE, update_refs,
    process_chapter_verses, process_chapter,
)
from app.models import Chapter, Verse, PartType


class TestQuranKafiLinking:
    """Test Quran-Kafi cross-referencing"""

    def test_quran_quote_pattern_matches(self):
        """Test regex pattern matches Quran references"""
        text = "See [2:255] and (3:18)"
        matches = QURAN_QUOTE.findall(text)
        assert matches == [('2', '255'), ('3', '18')]

    def test_quran_quote_pattern_various_formats(self):
        """Test pattern matches both bracket types"""
        text1 = "Reference [5:10] here"
        text2 = "Reference (5:10) here"
        assert QURAN_QUOTE.findall(text1) == [('5', '10')]
        assert QURAN_QUOTE.findall(text2) == [('5', '10')]

    def test_process_translation_text_replaces(self):
        """Test references are replaced with HTML links"""
        text = ["This refers to [2:255] in the Quran"]
        refs = set()
        process_translation_text(text, refs)

        assert '/#/books/quran:2#h255' in text[0]
        assert (2, 255) in refs

    def test_process_translation_text_multiple_refs(self):
        """Test multiple references in same text"""
        text = ["Verses [2:1] and [2:2] are important"]
        refs = set()
        process_translation_text(text, refs)

        assert (2, 1) in refs
        assert (2, 2) in refs

    def test_process_translation_text_preserves_brackets(self):
        """Test that links are formatted correctly"""
        text = ["See [3:5]"]
        refs = set()
        process_translation_text(text, refs)

        # Should have link format with brackets preserved in display
        assert '<a href="/#/books/quran:3#h5">[3:5]</a>' in text[0]

    def test_update_refs_adds_relation_to_verse(self):
        """Test that Quran verse gets 'Mentioned In' relation"""
        # Create simple Quran structure
        quran = Chapter()
        quran.chapters = []

        sura = Chapter()
        sura.verses = []
        verse = Verse()
        verse.part_type = PartType.Verse
        verse.path = "/books/quran:2:255"
        sura.verses.append(verse)
        quran.chapters.append(sura)

        # Create hadith
        hadith = Verse()
        hadith.path = "/books/al-kafi:1:1:1"

        # Update refs
        quran_refs = {(1, 1)}  # First sura, first verse
        update_refs(quran, hadith, quran_refs)

        # Verify verse has relation
        assert verse.relations is not None
        assert "Mentioned In" in verse.relations
        assert hadith.path in verse.relations["Mentioned In"]

    def test_update_refs_adds_relation_to_hadith(self):
        """Test that hadith gets 'Mentions' relation"""
        # Create simple Quran structure
        quran = Chapter()
        quran.chapters = []

        sura = Chapter()
        sura.verses = []
        verse = Verse()
        verse.part_type = PartType.Verse
        sura.verses.append(verse)
        quran.chapters.append(sura)

        # Create hadith
        hadith = Verse()
        hadith.path = "/books/al-kafi:1:1:1"

        # Update refs
        quran_refs = {(1, 1)}
        update_refs(quran, hadith, quran_refs)

        # Verify hadith has relation
        assert hadith.relations is not None
        assert "Mentions" in hadith.relations
        assert "/books/quran:1:1" in hadith.relations["Mentions"]

    def test_update_refs_handles_invalid_reference(self):
        """Test graceful handling of invalid Quran references"""
        # Create simple Quran structure with only 1 verse
        quran = Chapter()
        quran.chapters = []

        sura = Chapter()
        sura.verses = []
        verse = Verse()
        verse.part_type = PartType.Verse
        sura.verses.append(verse)
        quran.chapters.append(sura)

        # Create hadith
        hadith = Verse()
        hadith.path = "/books/al-kafi:1:1:1"

        # Try to reference non-existent verse
        quran_refs = {(1, 999)}  # Invalid verse number
        update_refs(quran, hadith, quran_refs)

        # Should not crash, hadith should have no relations
        # (or empty Mentions if there were valid refs)
        if hadith.relations:
            assert len(hadith.relations.get("Mentions", set())) == 0


def _make_quran_with_suras(sura_verse_counts):
    """Helper to build a minimal Quran structure for testing."""
    quran = Chapter()
    quran.chapters = []
    for sura_idx, verse_count in enumerate(sura_verse_counts, 1):
        sura = Chapter()
        sura.verses = []
        for v_idx in range(1, verse_count + 1):
            v = Verse()
            v.part_type = PartType.Verse
            v.path = f"/books/quran:{sura_idx}:{v_idx}"
            sura.verses.append(v)
        quran.chapters.append(sura)
    return quran


class TestProcessChapterVerses:
    """Test process_chapter_verses which processes hadiths in a chapter"""

    def test_processes_hubeali_translation(self):
        """Test that en.hubeali translations are scanned for Quran refs"""
        quran = _make_quran_with_suras([5])

        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.text = ["Arabic text"]
        hadith.translations = {
            "en.hubeali": ["This mentions [1:3] in the text"]
        }
        chapter.verses.append(hadith)

        process_chapter_verses(quran, chapter)

        # Hadith should have Mentions relation
        assert hadith.relations is not None
        assert "/books/quran:1:3" in hadith.relations["Mentions"]
        # Quran verse should have Mentioned In relation
        assert quran.chapters[0].verses[2].relations is not None

    def test_processes_sarwar_translation(self):
        """Test that en.sarwar translations are also scanned"""
        quran = _make_quran_with_suras([3])

        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.text = ["Arabic text"]
        hadith.translations = {
            "en.sarwar": ["Reference (1:2) here"]
        }
        chapter.verses.append(hadith)

        process_chapter_verses(quran, chapter)

        assert hadith.relations is not None
        assert "/books/quran:1:2" in hadith.relations["Mentions"]

    def test_skips_headings(self):
        """Test that Heading-type verses are skipped"""
        quran = _make_quran_with_suras([5])

        chapter = Chapter()
        chapter.verses = []
        heading = Verse()
        heading.part_type = PartType.Heading
        heading.path = "/books/al-kafi:1:1:1:1"
        heading.text = ["Heading text"]
        heading.translations = {
            "en.hubeali": ["This mentions [1:1]"]
        }
        chapter.verses.append(heading)

        process_chapter_verses(quran, chapter)

        # Heading should NOT have relations
        assert heading.relations is None

    def test_hadith_without_translations_is_noop(self):
        """Test hadith without relevant translations gets no relations"""
        quran = _make_quran_with_suras([5])

        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.text = ["Arabic text"]
        hadith.translations = {
            "en.other": ["Some text [1:1]"]
        }
        chapter.verses.append(hadith)

        process_chapter_verses(quran, chapter)

        # No refs found since 'en.other' is not checked
        assert hadith.relations is None

    def test_refs_from_both_translations_merged(self):
        """Test refs from both hubeali and sarwar are combined"""
        quran = _make_quran_with_suras([5])

        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.text = ["Arabic"]
        hadith.translations = {
            "en.hubeali": ["See [1:1]"],
            "en.sarwar": ["Also (1:3)"],
        }
        chapter.verses.append(hadith)

        process_chapter_verses(quran, chapter)

        assert "/books/quran:1:1" in hadith.relations["Mentions"]
        assert "/books/quran:1:3" in hadith.relations["Mentions"]


class TestProcessChapter:
    """Test recursive process_chapter traversal"""

    def test_processes_nested_chapters(self):
        """Test that process_chapter recurses into subchapters"""
        quran = _make_quran_with_suras([5])

        # Build nested kafi structure: book -> chapter -> hadith
        book = Chapter()
        book.chapters = []

        ch = Chapter()
        ch.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-kafi:1:1:1:1"
        hadith.text = ["Arabic"]
        hadith.translations = {"en.hubeali": ["See [1:2]"]}
        ch.verses.append(hadith)
        book.chapters.append(ch)

        process_chapter(quran, book)

        # Should have processed the hadith in the nested chapter
        assert hadith.relations is not None
        assert "/books/quran:1:2" in hadith.relations["Mentions"]

    def test_handles_empty_chapter(self):
        """Test that empty chapters (no verses or subchapters) don't crash"""
        quran = _make_quran_with_suras([1])

        empty = Chapter()
        # No chapters, no verses
        process_chapter(quran, empty)
        # Should not raise
