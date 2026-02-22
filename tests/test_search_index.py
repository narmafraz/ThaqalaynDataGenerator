"""Tests for search index generation (app/search_index.py)."""

import json
import os
import pytest

from app.search_index import (
    strip_html,
    build_titles_index,
    extract_verse_docs,
    build_book_docs,
    generate_search_indexes,
    write_search_json,
    _is_generic_chapter_title,
)


# ---------------------------------------------------------------------------
# strip_html tests
# ---------------------------------------------------------------------------

class TestStripHtml:
    def test_removes_sup_tags(self):
        assert strip_html("text<sup>asws</sup> more") == "textasws more"

    def test_removes_span_tags(self):
        assert strip_html('<span class="iTxt">Aql</span>') == "Aql"

    def test_removes_footnote_references(self):
        assert strip_html("some text[1] more[23]") == "some text more"

    def test_preserves_plain_text(self):
        assert strip_html("Hello world") == "Hello world"

    def test_handles_empty_string(self):
        assert strip_html("") == ""

    def test_strips_whitespace(self):
        assert strip_html("  hello  ") == "hello"

    def test_nested_tags(self):
        assert strip_html("<div><span>text</span></div>") == "text"

    def test_self_closing_tags(self):
        assert strip_html('before<a id="_ftnref1"></a>after') == "beforeafter"


# ---------------------------------------------------------------------------
# _is_generic_chapter_title tests
# ---------------------------------------------------------------------------

class TestIsGenericChapterTitle:
    def test_generic_chapter(self):
        assert _is_generic_chapter_title("Chapter 1", "Chapter") is True

    def test_generic_chapter_large_number(self):
        assert _is_generic_chapter_title("Chapter 123", "Chapter") is True

    def test_descriptive_chapter(self):
        assert _is_generic_chapter_title(
            "Chapter 1 \u2013 The necessity of knowledge", "Chapter"
        ) is False

    def test_non_chapter_type(self):
        assert _is_generic_chapter_title("Chapter 1", "Book") is False

    def test_volume_type(self):
        assert _is_generic_chapter_title("Volume 1", "Volume") is False

    def test_empty_title(self):
        assert _is_generic_chapter_title("", "Chapter") is False


# ---------------------------------------------------------------------------
# build_titles_index tests
# ---------------------------------------------------------------------------

class TestBuildTitlesIndex:
    @pytest.fixture
    def titles_data_dir(self, tmp_path):
        """Create a temporary data dir with index/books.en.json and books.ar.json."""
        index_dir = tmp_path / "index"
        index_dir.mkdir()

        en_data = {
            "/books/quran": {"title": "Quran"},
            "/books/quran:1": {
                "local_index": 1,
                "part_type": "Chapter",
                "title": "The Opening",
            },
            "/books/al-kafi": {"title": "Al-Kafi"},
            "/books/al-kafi:1:1:1": {
                "local_index": 1,
                "part_type": "Chapter",
                "title": "Chapter 1",
            },
        }
        ar_data = {
            "/books/quran": {"title": "\u0627\u0644\u0642\u0631\u0622\u0646"},
            "/books/quran:1": {
                "local_index": 1,
                "part_type": "Chapter",
                "title": "\u0627\u0644\u0641\u0627\u062a\u062d\u0629",
            },
            "/books/al-kafi": {"title": "\u0627\u0644\u0643\u0627\u0641\u064a"},
        }

        with open(index_dir / "books.en.json", "w", encoding="utf-8") as f:
            json.dump(en_data, f, ensure_ascii=False)
        with open(index_dir / "books.ar.json", "w", encoding="utf-8") as f:
            json.dump(ar_data, f, ensure_ascii=False)

        return str(tmp_path)

    def test_builds_titles(self, titles_data_dir):
        docs = build_titles_index(titles_data_dir)
        assert len(docs) >= 3  # quran, quran:1, al-kafi

    def test_title_has_compact_keys(self, titles_data_dir):
        docs = build_titles_index(titles_data_dir)
        for doc in docs:
            assert "p" in doc
            assert "en" in doc
            assert "ar" in doc
            assert "arn" in doc
            assert "pt" in doc

    def test_arabic_normalization_applied(self, titles_data_dir):
        docs = build_titles_index(titles_data_dir)
        quran_title = next(d for d in docs if d["p"] == "/books/quran")
        # Arabic title should be present
        assert quran_title["ar"] != ""
        # Normalized version should also be present
        assert quran_title["arn"] != ""

    def test_skips_generic_chapters_without_arabic(self, titles_data_dir):
        docs = build_titles_index(titles_data_dir)
        # "Chapter 1" at /books/al-kafi:1:1:1 has no Arabic title -> skipped
        paths = [d["p"] for d in docs]
        assert "/books/al-kafi:1:1:1" not in paths

    def test_keeps_chapters_with_arabic_title(self, titles_data_dir):
        docs = build_titles_index(titles_data_dir)
        # /books/quran:1 has both English and Arabic titles -> kept
        paths = [d["p"] for d in docs]
        assert "/books/quran:1" in paths

    def test_strips_html_from_titles(self, tmp_path):
        """Test that HTML in Arabic titles is stripped."""
        index_dir = tmp_path / "index"
        index_dir.mkdir()

        ar_data = {
            "/books/test:1": {
                "part_type": "Chapter",
                "title": '<span class="first">b</span>ab test',
            },
        }
        en_data = {
            "/books/test:1": {
                "part_type": "Chapter",
                "title": "Chapter 1 - Test",
            },
        }

        with open(index_dir / "books.ar.json", "w", encoding="utf-8") as f:
            json.dump(ar_data, f, ensure_ascii=False)
        with open(index_dir / "books.en.json", "w", encoding="utf-8") as f:
            json.dump(en_data, f, ensure_ascii=False)

        docs = build_titles_index(str(tmp_path))
        doc = next(d for d in docs if d["p"] == "/books/test:1")
        assert "<span" not in doc["ar"]
        assert doc["ar"] == "bab test"


# ---------------------------------------------------------------------------
# extract_verse_docs tests
# ---------------------------------------------------------------------------

class TestExtractVerseDocs:
    def _make_chapter_json(self, verses=None, titles=None, path="/books/quran:1",
                            default_trans=None):
        """Helper to create a chapter JSON structure."""
        if verses is None:
            verses = [
                {
                    "index": 1,
                    "local_index": 1,
                    "path": f"{path}:1",
                    "text": ["\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650"],
                    "translations": {
                        "en.qarai": ["In the Name of Allah"],
                    },
                },
            ]
        return {
            "kind": "verse_list",
            "data": {
                "path": path,
                "titles": titles or {"en": "The Opening", "ar": "\u0627\u0644\u0641\u0627\u062a\u062d\u0629"},
                "verses": verses,
                "default_verse_translation_ids": default_trans or {"en": "en.qarai"},
            },
        }

    def test_basic_extraction(self):
        chapter = self._make_chapter_json()
        docs = extract_verse_docs(chapter, "quran")
        assert len(docs) == 1
        assert docs[0]["p"] == "/books/quran:1:1"

    def test_compact_keys(self):
        chapter = self._make_chapter_json()
        docs = extract_verse_docs(chapter, "quran")
        doc = docs[0]
        assert set(doc.keys()) == {"p", "t", "ar", "en", "i"}

    def test_arabic_normalized(self):
        chapter = self._make_chapter_json()
        docs = extract_verse_docs(chapter, "quran")
        doc = docs[0]
        # Normalized Arabic should have no diacritics
        assert "\u064e" not in doc["ar"]  # no fatha
        assert "\u0650" not in doc["ar"]  # no kasra
        assert "\u0652" not in doc["ar"]  # no sukun

    def test_english_text_extracted(self):
        chapter = self._make_chapter_json()
        docs = extract_verse_docs(chapter, "quran")
        assert docs[0]["en"] == "In the Name of Allah"

    def test_html_stripped_from_english(self):
        verses = [
            {
                "index": 1,
                "local_index": 1,
                "path": "/books/test:1:1",
                "text": ["test"],
                "translations": {
                    "en.hubeali": ["Text<sup>asws</sup> with<a id=\"ref\"></a> tags[1]"],
                },
            },
        ]
        chapter = self._make_chapter_json(
            verses=verses, path="/books/test:1",
            default_trans={"en": "en.hubeali"},
        )
        docs = extract_verse_docs(chapter, "test")
        assert docs[0]["en"] == "Textasws with tags"

    def test_chapter_title_included(self):
        chapter = self._make_chapter_json()
        docs = extract_verse_docs(chapter, "quran")
        assert docs[0]["t"] == "The Opening"

    def test_local_index_included(self):
        chapter = self._make_chapter_json()
        docs = extract_verse_docs(chapter, "quran")
        assert docs[0]["i"] == 1

    def test_empty_verses(self):
        chapter = self._make_chapter_json(verses=[])
        docs = extract_verse_docs(chapter, "quran")
        assert docs == []

    def test_verse_without_path_skipped(self):
        verses = [
            {"index": 1, "local_index": 1, "text": ["test"], "translations": {}},
        ]
        chapter = self._make_chapter_json(verses=verses)
        docs = extract_verse_docs(chapter, "quran")
        assert docs == []

    def test_verse_without_text(self):
        verses = [
            {
                "index": 1,
                "local_index": 1,
                "path": "/books/test:1:1",
                "translations": {"en.qarai": ["Some text"]},
            },
        ]
        chapter = self._make_chapter_json(
            verses=verses, path="/books/test:1",
        )
        docs = extract_verse_docs(chapter, "test")
        assert docs[0]["ar"] == ""

    def test_fallback_to_first_english_translation(self):
        """When no default translation is set, use first en.* translation."""
        verses = [
            {
                "index": 1,
                "local_index": 1,
                "path": "/books/test:1:1",
                "text": ["test"],
                "translations": {
                    "en.sarwar": ["Sarwar translation"],
                    "en.transliteration": ["ignore this"],
                },
            },
        ]
        chapter = self._make_chapter_json(
            verses=verses, path="/books/test:1",
            default_trans={},
        )
        docs = extract_verse_docs(chapter, "test")
        assert docs[0]["en"] == "Sarwar translation"

    def test_transliteration_excluded_from_fallback(self):
        """en.transliteration should not be used as fallback English text."""
        verses = [
            {
                "index": 1,
                "local_index": 1,
                "path": "/books/test:1:1",
                "text": ["test"],
                "translations": {
                    "en.transliteration": ["Bismi Allahi"],
                },
            },
        ]
        chapter = self._make_chapter_json(
            verses=verses, path="/books/test:1",
            default_trans={},
        )
        docs = extract_verse_docs(chapter, "test")
        # No usable English translation
        assert docs[0]["en"] == ""

    def test_multiple_verses(self):
        verses = [
            {
                "index": i,
                "local_index": i,
                "path": f"/books/test:1:{i}",
                "text": [f"text {i}"],
                "translations": {"en.test": [f"english {i}"]},
            }
            for i in range(1, 4)
        ]
        chapter = self._make_chapter_json(
            verses=verses, path="/books/test:1",
            default_trans={"en": "en.test"},
        )
        docs = extract_verse_docs(chapter, "test")
        assert len(docs) == 3
        assert [d["i"] for d in docs] == [1, 2, 3]


# ---------------------------------------------------------------------------
# build_book_docs tests
# ---------------------------------------------------------------------------

class TestBuildBookDocs:
    @pytest.fixture
    def book_data_dir(self, tmp_path):
        """Create a temporary data dir with book JSON files."""
        book_dir = tmp_path / "books" / "test-book"
        book_dir.mkdir(parents=True)

        # Chapter list (should be skipped)
        chapter_list = {
            "kind": "chapter_list",
            "data": {"path": "/books/test-book", "chapters": []},
        }
        with open(book_dir / "test-book.json", "w", encoding="utf-8") as f:
            json.dump(chapter_list, f)

        # Verse list (should be processed)
        verse_list = {
            "kind": "verse_list",
            "data": {
                "path": "/books/test-book:1",
                "titles": {"en": "Chapter One"},
                "default_verse_translation_ids": {"en": "en.test"},
                "verses": [
                    {
                        "index": 1,
                        "local_index": 1,
                        "path": "/books/test-book:1:1",
                        "text": ["\u0628\u0650\u0633\u0652\u0645\u0650"],
                        "translations": {"en.test": ["In the name"]},
                    },
                    {
                        "index": 2,
                        "local_index": 2,
                        "path": "/books/test-book:1:2",
                        "text": ["\u0627\u0644\u0644\u0651\u064e\u0647\u0650"],
                        "translations": {"en.test": ["of Allah"]},
                    },
                ],
            },
        }
        with open(book_dir / "1.json", "w", encoding="utf-8") as f:
            json.dump(verse_list, f, ensure_ascii=False)

        return str(tmp_path)

    def test_builds_docs_from_verse_lists(self, book_data_dir):
        docs = build_book_docs(book_data_dir, "test-book")
        assert len(docs) == 2

    def test_skips_chapter_list_files(self, book_data_dir):
        docs = build_book_docs(book_data_dir, "test-book")
        # Should only have docs from verse_list, not chapter_list
        paths = [d["p"] for d in docs]
        assert "/books/test-book" not in paths

    def test_nonexistent_book(self, tmp_path):
        docs = build_book_docs(str(tmp_path), "nonexistent")
        assert docs == []


# ---------------------------------------------------------------------------
# write_search_json tests
# ---------------------------------------------------------------------------

class TestWriteSearchJson:
    def test_writes_json_file(self, tmp_path):
        data_dir = str(tmp_path)
        docs = [{"p": "/books/test", "en": "Test"}]
        filepath = write_search_json(data_dir, "test.json", docs)
        assert os.path.exists(filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        assert loaded == docs

    def test_creates_search_directory(self, tmp_path):
        data_dir = str(tmp_path)
        write_search_json(data_dir, "test.json", [])
        assert os.path.isdir(os.path.join(data_dir, "index", "search"))

    def test_preserves_arabic_text(self, tmp_path):
        data_dir = str(tmp_path)
        docs = [{"ar": "\u0627\u0644\u0641\u0627\u062a\u062d\u0629"}]
        filepath = write_search_json(data_dir, "test.json", docs)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        # Should not have escaped unicode
        assert "\\u" not in content
        assert "\u0627\u0644\u0641\u0627\u062a\u062d\u0629" in content


# ---------------------------------------------------------------------------
# generate_search_indexes integration test
# ---------------------------------------------------------------------------

class TestGenerateSearchIndexes:
    @pytest.fixture
    def full_data_dir(self, tmp_path):
        """Create a complete temporary data directory."""
        # Index files
        index_dir = tmp_path / "index"
        index_dir.mkdir()

        en_index = {
            "/books/quran": {"title": "Quran"},
            "/books/quran:1": {"title": "The Opening", "part_type": "Chapter", "local_index": 1},
        }
        ar_index = {
            "/books/quran": {"title": "\u0627\u0644\u0642\u0631\u0622\u0646"},
            "/books/quran:1": {"title": "\u0627\u0644\u0641\u0627\u062a\u062d\u0629", "part_type": "Chapter", "local_index": 1},
        }
        with open(index_dir / "books.en.json", "w", encoding="utf-8") as f:
            json.dump(en_index, f, ensure_ascii=False)
        with open(index_dir / "books.ar.json", "w", encoding="utf-8") as f:
            json.dump(ar_index, f, ensure_ascii=False)

        # Quran book data
        quran_dir = tmp_path / "books" / "quran"
        quran_dir.mkdir(parents=True)
        verse_list = {
            "kind": "verse_list",
            "data": {
                "path": "/books/quran:1",
                "titles": {"en": "The Opening", "ar": "\u0627\u0644\u0641\u0627\u062a\u062d\u0629"},
                "default_verse_translation_ids": {"en": "en.qarai"},
                "verses": [
                    {
                        "index": 1,
                        "local_index": 1,
                        "path": "/books/quran:1:1",
                        "text": ["\u0628\u0650\u0633\u0652\u0645\u0650 \u0627\u0644\u0644\u0651\u064e\u0647\u0650"],
                        "translations": {"en.qarai": ["In the Name of Allah"]},
                    },
                ],
            },
        }
        with open(quran_dir / "1.json", "w", encoding="utf-8") as f:
            json.dump(verse_list, f, ensure_ascii=False)

        # Al-Kafi (empty dir)
        kafi_dir = tmp_path / "books" / "al-kafi"
        kafi_dir.mkdir(parents=True)

        return str(tmp_path)

    def test_generates_all_files(self, full_data_dir):
        results = generate_search_indexes(full_data_dir)
        assert "titles.json" in results
        assert "quran-docs.json" in results
        assert "al-kafi-docs.json" in results

    def test_titles_file_created(self, full_data_dir):
        generate_search_indexes(full_data_dir)
        filepath = os.path.join(full_data_dir, "index", "search", "titles.json")
        assert os.path.exists(filepath)

    def test_quran_docs_created(self, full_data_dir):
        generate_search_indexes(full_data_dir)
        filepath = os.path.join(full_data_dir, "index", "search", "quran-docs.json")
        assert os.path.exists(filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            docs = json.load(f)
        assert len(docs) == 1
        assert docs[0]["p"] == "/books/quran:1:1"

    def test_metadata_file_created(self, full_data_dir):
        generate_search_indexes(full_data_dir)
        filepath = os.path.join(full_data_dir, "index", "search", "search-meta.json")
        assert os.path.exists(filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            meta = json.load(f)
        assert meta["language"] == "arabic"
        assert "titles" in meta["schemas"]
        assert "book" in meta["schemas"]

    def test_empty_book_produces_empty_docs(self, full_data_dir):
        results = generate_search_indexes(full_data_dir)
        assert results["al-kafi-docs.json"] == 0
