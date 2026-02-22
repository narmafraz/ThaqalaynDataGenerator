"""Tests for the generalized cross-reference linker (link_books.py)."""

import json
import os

from app.link_books import (
    QURAN_QUOTE,
    _collect_verse_updates,
    _count_relations,
    _get_leaf_chapter_paths,
    _patch_modular_file,
    _process_chapter,
    _process_chapter_verses,
    _process_translation_text,
    _propagate_to_modular_files,
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
        assert '<a href="/books/quran:2#h255">[2:255]</a>' in text[0]
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


class TestCollectVerseUpdates:
    def test_collects_verses_with_relations(self):
        chapter = Chapter()
        chapter.path = "/books/test:1"
        chapter.verses = []
        v1 = Verse()
        v1.path = "/books/test:1:1"
        v1.relations = {"Mentions": {"/books/quran:1:1"}}
        v1.translations = {"en.test": ["See [1:1]"]}
        v2 = Verse()
        v2.path = "/books/test:1:2"
        chapter.verses = [v1, v2]

        updates = {}
        _collect_verse_updates(chapter, updates)
        assert "/books/test:1:1" in updates
        # v2 has no relations or translations, but still included if translations exist
        # Actually v2 has neither, so should not be included
        assert "/books/test:1:2" not in updates

    def test_recurses_into_subchapters(self):
        root = Chapter()
        root.chapters = []
        ch = Chapter()
        ch.path = "/books/test:1"
        ch.verses = []
        v = Verse()
        v.path = "/books/test:1:1"
        v.relations = {"Mentions": {"/books/quran:2:1"}}
        v.translations = {"en.t": ["ref [2:1]"]}
        ch.verses.append(v)
        root.chapters.append(ch)

        updates = {}
        _collect_verse_updates(root, updates)
        assert "/books/test:1:1" in updates


class TestGetLeafChapterPaths:
    def test_returns_leaf_paths(self):
        root = Chapter()
        root.path = "/books/test"
        root.chapters = []
        ch = Chapter()
        ch.path = "/books/test:1"
        ch.verses = [Verse()]
        root.chapters.append(ch)

        paths = _get_leaf_chapter_paths(root)
        assert "/books/test:1" in paths

    def test_skips_non_leaf_chapters(self):
        root = Chapter()
        root.path = "/books/test"
        root.chapters = []
        mid = Chapter()
        mid.path = "/books/test:1"
        mid.chapters = []
        leaf = Chapter()
        leaf.path = "/books/test:1:1"
        leaf.verses = [Verse()]
        mid.chapters.append(leaf)
        root.chapters.append(mid)

        paths = _get_leaf_chapter_paths(root)
        assert "/books/test:1:1" in paths
        assert "/books/test:1" not in paths


class TestPatchModularFile:
    def test_patches_verse_list(self, tmp_path, monkeypatch):
        """Patch a verse_list file with updated relations."""
        # Create a mock verse_list JSON file
        verse_list = {
            "index": "test:1",
            "kind": "verse_list",
            "data": {
                "path": "/books/test:1",
                "verses": [
                    {"path": "/books/test:1:1", "text": ["Arabic"]},
                    {"path": "/books/test:1:2", "text": ["More Arabic"]},
                ]
            }
        }
        dest_file = tmp_path / "books" / "test" / "1.json"
        dest_file.parent.mkdir(parents=True)
        dest_file.write_text(json.dumps(verse_list), encoding="utf-8")

        monkeypatch.setattr("app.link_books.get_dest_path", lambda p: str(dest_file))

        updates = {
            "/books/test:1:1": {
                "relations": {"Mentions": ["/books/quran:1:1"]},
                "translations": {"en.test": ["See <a>link</a>"]},
            }
        }
        patched = _patch_modular_file("/books/test:1", updates)
        assert patched == 1

        result = json.loads(dest_file.read_text(encoding="utf-8"))
        v1 = result["data"]["verses"][0]
        assert v1["relations"] == {"Mentions": ["/books/quran:1:1"]}
        assert v1["translations"] == {"en.test": ["See <a>link</a>"]}
        # v2 should be unchanged
        v2 = result["data"]["verses"][1]
        assert "relations" not in v2

    def test_patches_verse_detail(self, tmp_path, monkeypatch):
        """Patch a verse_detail file with updated relations."""
        verse_detail = {
            "index": "test:1:1",
            "kind": "verse_detail",
            "data": {
                "verse": {"path": "/books/test:1:1", "text": ["Arabic"]},
                "chapter_path": "/books/test:1",
                "nav": {},
            }
        }
        dest_file = tmp_path / "books" / "test" / "1" / "1.json"
        dest_file.parent.mkdir(parents=True)
        dest_file.write_text(json.dumps(verse_detail), encoding="utf-8")

        monkeypatch.setattr("app.link_books.get_dest_path", lambda p: str(dest_file))

        updates = {
            "/books/test:1:1": {
                "relations": {"Mentions": ["/books/quran:2:3"]},
            }
        }
        patched = _patch_modular_file("/books/test:1:1", updates)
        assert patched == 1

        result = json.loads(dest_file.read_text(encoding="utf-8"))
        assert result["data"]["verse"]["relations"] == {"Mentions": ["/books/quran:2:3"]}

    def test_returns_zero_for_missing_file(self, monkeypatch):
        monkeypatch.setattr("app.link_books.get_dest_path", lambda p: "/nonexistent/path.json")
        assert _patch_modular_file("/books/fake:1", {}) == 0

    def test_returns_zero_when_no_matching_updates(self, tmp_path, monkeypatch):
        verse_list = {
            "index": "test:1",
            "kind": "verse_list",
            "data": {"verses": [{"path": "/books/test:1:1"}]}
        }
        dest_file = tmp_path / "test.json"
        dest_file.write_text(json.dumps(verse_list), encoding="utf-8")

        monkeypatch.setattr("app.link_books.get_dest_path", lambda p: str(dest_file))

        # No matching verse path in updates
        updates = {"/books/other:1:1": {"relations": {"Mentions": ["/books/quran:1:1"]}}}
        assert _patch_modular_file("/books/test:1", updates) == 0
