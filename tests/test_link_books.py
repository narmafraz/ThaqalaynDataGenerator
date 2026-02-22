"""Tests for the generalized cross-reference linker (link_books.py)."""

from app.link_books import (
    QURAN_QUOTE,
    _count_relations,
    _process_chapter,
    _process_chapter_verses,
    _process_translation_text,
    _update_refs,
)
from app.models import Chapter, PartType, Verse


def _make_quran(sura_verse_counts):
    """Build a minimal Quran structure for testing."""
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


class TestQuranQuotePattern:
    def test_bracket_format(self):
        assert QURAN_QUOTE.findall("[2:255]") == [("2", "255")]

    def test_parenthesis_format(self):
        assert QURAN_QUOTE.findall("(3:18)") == [("3", "18")]

    def test_multiple_refs(self):
        text = "See [2:255] and (3:18) and [112:1]"
        assert QURAN_QUOTE.findall(text) == [("2", "255"), ("3", "18"), ("112", "1")]

    def test_no_match(self):
        assert QURAN_QUOTE.findall("No references here") == []


class TestProcessTranslationText:
    def test_replaces_with_html_link(self):
        text = ["See [2:255]"]
        refs = set()
        _process_translation_text(text, refs)
        assert '<a href="/#/books/quran:2#h255">[2:255]</a>' in text[0]
        assert (2, 255) in refs

    def test_multiple_refs_collected(self):
        text = ["Verse [1:1] and [2:3]"]
        refs = set()
        _process_translation_text(text, refs)
        assert (1, 1) in refs
        assert (2, 3) in refs

    def test_empty_list(self):
        refs = set()
        _process_translation_text([], refs)
        assert len(refs) == 0


class TestUpdateRefs:
    def test_quran_verse_gets_mentioned_in(self):
        quran = _make_quran([5])
        hadith = Verse()
        hadith.path = "/books/al-tawhid:1:2:3"
        _update_refs(quran, hadith, {(1, 3)})
        assert "Mentioned In" in quran.chapters[0].verses[2].relations
        assert hadith.path in quran.chapters[0].verses[2].relations["Mentioned In"]

    def test_hadith_gets_mentions(self):
        quran = _make_quran([5])
        hadith = Verse()
        hadith.path = "/books/nahj-al-balagha:1:1:1"
        _update_refs(quran, hadith, {(1, 1)})
        assert "Mentions" in hadith.relations
        assert "/books/quran:1:1" in hadith.relations["Mentions"]

    def test_invalid_ref_handled_gracefully(self):
        quran = _make_quran([3])
        hadith = Verse()
        hadith.path = "/books/test:1:1"
        _update_refs(quran, hadith, {(1, 999)})
        # Should not crash; hadith should have no relations
        assert hadith.relations is None or len(hadith.relations.get("Mentions", [])) == 0

    def test_multiple_hadiths_same_quran_verse(self):
        quran = _make_quran([5])
        h1 = Verse()
        h1.path = "/books/al-kafi:1:1:1:1"
        h2 = Verse()
        h2.path = "/books/al-tawhid:1:1:1"
        _update_refs(quran, h1, {(1, 1)})
        _update_refs(quran, h2, {(1, 1)})
        mentioned = quran.chapters[0].verses[0].relations["Mentioned In"]
        assert h1.path in mentioned
        assert h2.path in mentioned

    def test_preserves_existing_mentions(self):
        quran = _make_quran([5])
        hadith = Verse()
        hadith.path = "/books/test:1:1"
        hadith.relations = {"Mentions": ["/books/quran:1:1"]}
        _update_refs(quran, hadith, {(1, 2)})
        assert "/books/quran:1:1" in hadith.relations["Mentions"]
        assert "/books/quran:1:2" in hadith.relations["Mentions"]


class TestProcessChapterVerses:
    def test_scans_all_translations(self):
        """Any translation ID should be scanned, not just hubeali/sarwar."""
        quran = _make_quran([5])
        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-tawhid:1:1:1"
        hadith.text = ["Arabic"]
        hadith.translations = {"en.badr-shahin": ["See [1:3]"]}
        chapter.verses.append(hadith)

        _process_chapter_verses(quran, chapter)
        assert hadith.relations is not None
        assert "/books/quran:1:3" in hadith.relations["Mentions"]

    def test_skips_headings(self):
        quran = _make_quran([5])
        chapter = Chapter()
        chapter.verses = []
        heading = Verse()
        heading.part_type = PartType.Heading
        heading.path = "/books/test:1:1"
        heading.translations = {"en.test": ["See [1:1]"]}
        chapter.verses.append(heading)

        _process_chapter_verses(quran, chapter)
        assert heading.relations is None

    def test_handles_no_translations(self):
        quran = _make_quran([5])
        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1:1"
        hadith.text = ["Arabic only"]
        chapter.verses.append(hadith)

        _process_chapter_verses(quran, chapter)
        assert hadith.relations is None

    def test_multiple_translations_merged(self):
        quran = _make_quran([5])
        chapter = Chapter()
        chapter.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/test:1:1"
        hadith.translations = {
            "en.trans-a": ["See [1:1]"],
            "en.trans-b": ["Also [1:3]"],
        }
        chapter.verses.append(hadith)

        _process_chapter_verses(quran, chapter)
        assert "/books/quran:1:1" in hadith.relations["Mentions"]
        assert "/books/quran:1:3" in hadith.relations["Mentions"]


class TestProcessChapter:
    def test_recurses_into_subchapters(self):
        quran = _make_quran([5, 5])
        book = Chapter()
        book.chapters = []
        ch = Chapter()
        ch.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/al-amali-saduq:1:1:1"
        hadith.translations = {"en.ali-peiravi": ["Quran says [2:3]"]}
        ch.verses.append(hadith)
        book.chapters.append(ch)

        _process_chapter(quran, book)
        assert hadith.relations is not None
        assert "/books/quran:2:3" in hadith.relations["Mentions"]

    def test_handles_deeply_nested(self):
        quran = _make_quran([5])
        root = Chapter()
        root.chapters = []
        level1 = Chapter()
        level1.chapters = []
        level2 = Chapter()
        level2.verses = []
        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.path = "/books/deep:1:1:1:1"
        hadith.translations = {"en.test": ["[1:5]"]}
        level2.verses.append(hadith)
        level1.chapters.append(level2)
        root.chapters.append(level1)

        _process_chapter(quran, root)
        assert "/books/quran:1:5" in hadith.relations["Mentions"]

    def test_handles_empty_chapter(self):
        quran = _make_quran([1])
        empty = Chapter()
        _process_chapter(quran, empty)  # Should not raise


class TestCountRelations:
    def test_counts_verses_with_mentions(self):
        chapter = Chapter()
        chapter.verses = []
        v1 = Verse()
        v1.relations = {"Mentions": ["/books/quran:1:1"]}
        v2 = Verse()
        v2.relations = None
        v3 = Verse()
        v3.relations = {"Mentions": ["/books/quran:2:1"]}
        chapter.verses = [v1, v2, v3]
        assert _count_relations(chapter) == 2

    def test_recurses_into_subchapters(self):
        root = Chapter()
        root.chapters = []
        ch = Chapter()
        ch.verses = []
        v = Verse()
        v.relations = {"Mentions": ["/books/quran:1:1"]}
        ch.verses.append(v)
        root.chapters.append(ch)
        assert _count_relations(root) == 1

    def test_empty_chapter(self):
        assert _count_relations(Chapter()) == 0
