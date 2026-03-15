import json
import os
from fastapi.encoders import jsonable_encoder
from app.lib_db import (
    index_from_path, get_dest_path, clean_nones,
    write_file, load_json, load_chapter, ensure_dir,
    insert_chapter, insert_verse_details, delete_file, delete_folder,
    shellify_complete_books, _shellify_node,
)
from app.models import Chapter, Verse, PartType


class TestPathFunctions:
    """Test path transformation utilities"""

    def test_index_from_path(self):
        """Test path to index conversion"""
        assert index_from_path("/books/quran:1:5") == "quran:1:5"
        assert index_from_path("/books/al-kafi:1:2:3") == "al-kafi:1:2:3"

    def test_get_dest_path_colon_to_slash(self, temp_destination_dir):
        """Test : converted to / in paths"""
        path = get_dest_path("/books/quran:1:5")
        assert "books/quran/1/5.json" in path

    def test_get_dest_path_leading_slash_removed(self, temp_destination_dir):
        """Test leading slash is removed"""
        path = get_dest_path("/books/test")
        # Should not have double slash
        assert "//" not in path


class TestCleanNones:
    """Test recursive None removal from data structures"""

    def test_clean_nones_simple_dict(self):
        """Test None removal from flat dictionary"""
        input_dict = {"a": 1, "b": None, "c": 3}
        expected = {"a": 1, "c": 3}
        assert clean_nones(input_dict) == expected

    def test_clean_nones_nested_dict(self):
        """Test recursive None cleaning in nested dicts"""
        input_dict = {
            "a": 1,
            "b": None,
            "c": {
                "d": None,
                "e": 2,
                "f": {"g": None, "h": 3}
            }
        }
        expected = {
            "a": 1,
            "c": {
                "e": 2,
                "f": {"h": 3}
            }
        }
        assert clean_nones(input_dict) == expected

    def test_clean_nones_list(self):
        """Test None removal from lists"""
        input_list = [1, None, 2, None, 3]
        expected = [1, 2, 3]
        assert clean_nones(input_list) == expected

    def test_clean_nones_nested_list(self):
        """Test None removal from nested lists"""
        input_list = [1, [2, None, 3], None, [None, 4]]
        expected = [1, [2, 3], [4]]
        assert clean_nones(input_list) == expected

    def test_clean_nones_mixed_structures(self):
        """Test None cleaning in mixed dict/list structures"""
        input_data = {
            "a": [1, None, 2],
            "b": None,
            "c": {
                "items": [None, {"x": 1, "y": None}, None]
            }
        }
        expected = {
            "a": [1, 2],
            "c": {
                "items": [{"x": 1}]
            }
        }
        assert clean_nones(input_data) == expected

    def test_clean_nones_preserves_zero(self):
        """Zero should not be removed"""
        assert clean_nones({"a": 0, "b": None}) == {"a": 0}

    def test_clean_nones_preserves_empty_string(self):
        """Empty string should not be removed"""
        assert clean_nones({"a": "", "b": None}) == {"a": ""}

    def test_clean_nones_preserves_false(self):
        """False should not be removed"""
        assert clean_nones({"a": False, "b": None}) == {"a": False}

    def test_clean_nones_empty_dict_removed(self):
        """Empty dicts after cleaning should remain"""
        # Empty dict is not None, so it stays
        assert clean_nones({"a": {}, "b": None}) == {"a": {}}


class TestFileOperations:
    """Test file I/O operations"""

    def test_write_file_creates_json(self, temp_destination_dir, simple_chapter):
        """Test JSON file is created correctly"""
        obj = {
            "index": "test:1",
            "kind": "chapter_list",
            "data": jsonable_encoder(simple_chapter)
        }
        result = write_file("/books/test:1", obj)

        assert result.index == "test:1"
        # Verify file exists
        import os
        assert os.path.exists(result.id)

    def test_write_file_utf8_encoding(self, temp_destination_dir):
        """Test Arabic text is preserved with UTF-8"""
        obj = {
            "index": "test",
            "data": {
                "text": "مُحَمَّدُ بْنُ يَحْيَى",
                "title": "الباب الأول"
            }
        }
        result = write_file("/books/test", obj)

        # Read back and verify
        with open(result.id, 'r', encoding='utf-8') as f:
            loaded = json.load(f)
            assert loaded["data"]["text"] == "مُحَمَّدُ بْنُ يَحْيَى"

    def test_write_file_sorted_keys(self, temp_destination_dir):
        """Test JSON keys are sorted for consistent diffs"""
        obj = {
            "index": "test",
            "data": {"z": 1, "a": 2, "m": 3}
        }
        result = write_file("/books/test", obj)

        with open(result.id, 'r') as f:
            content = f.read()
            # Keys should be sorted
            assert content.index('"a"') < content.index('"m"') < content.index('"z"')

    def test_ensure_dir_creates_nested(self, temp_destination_dir):
        """Test nested directory creation"""
        import os
        path = os.path.join(str(temp_destination_dir), "a", "b", "c", "file.json")
        result = ensure_dir(path)

        # Directory should exist
        assert os.path.exists(os.path.dirname(result))

    def test_load_json_with_data_wrapper(self, temp_destination_dir):
        """Test loading JSON with data wrapper"""
        obj = {"index": "test", "kind": "chapter", "data": {"title": "Test"}}
        write_file("/books/test", obj)

        loaded = load_json("/books/test")
        assert loaded["data"]["title"] == "Test"

    def test_load_chapter_returns_chapter(self, temp_destination_dir):
        """Test load_chapter deserializes into Chapter model"""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Test Chapter"}
        chapter.path = "/books/test"
        obj = {
            "index": "test",
            "kind": "chapter_list",
            "data": jsonable_encoder(chapter)
        }
        write_file("/books/test", obj)

        loaded = load_chapter("/books/test")
        assert isinstance(loaded, Chapter)
        assert loaded.titles["en"] == "Test Chapter"

    def test_delete_file_removes_json(self, temp_destination_dir):
        """Test delete_file removes the generated JSON file"""
        obj = {"index": "test", "data": {"x": 1}}
        result = write_file("/books/test", obj)
        assert os.path.exists(result.id)

        delete_file("/books/test")
        assert not os.path.exists(result.id)

    def test_delete_file_nonexistent_is_noop(self, temp_destination_dir):
        """Test delete_file on nonexistent path does not raise"""
        delete_file("/books/nonexistent")  # Should not raise

    def test_delete_folder_removes_directory(self, temp_destination_dir):
        """Test delete_folder removes entire directory tree"""
        obj = {"index": "test", "data": {"x": 1}}
        write_file("/books/test:1:1", obj)
        write_file("/books/test:1:2", obj)

        folder_path = os.path.join(str(temp_destination_dir), "books", "test")
        assert os.path.exists(folder_path)

        delete_folder("/books/test")
        assert not os.path.exists(folder_path)

    def test_delete_folder_nonexistent_is_noop(self, temp_destination_dir):
        """Test delete_folder on nonexistent path does not raise"""
        delete_folder("/books/nonexistent")  # Should not raise

    def test_write_file_nones_stripped(self, temp_destination_dir):
        """Test that None values are stripped from output JSON"""
        obj = {
            "index": "test",
            "data": {"title": "Test", "description": None}
        }
        result = write_file("/books/test", obj)

        with open(result.id, 'r', encoding='utf-8') as f:
            loaded = json.load(f)
        assert "description" not in loaded["data"]


class TestInsertChapter:
    """Test recursive chapter insertion"""

    def test_insert_chapter_with_verses(self, temp_destination_dir):
        """Test inserting a leaf chapter produces shell format with verse_refs"""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Chapter 1"}
        chapter.path = "/books/test:1"

        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Test text"]
        v.index = 1
        v.local_index = 1
        v.path = "/books/test:1:1"
        chapter.verses = [v]

        insert_chapter(chapter)

        loaded = load_json("/books/test:1")
        assert loaded["kind"] == "verse_list"
        assert loaded["index"] == "test:1"
        # Shell format: no verses, has verse_refs
        assert "verses" not in loaded["data"]
        assert "verse_refs" in loaded["data"]
        assert len(loaded["data"]["verse_refs"]) == 1
        ref = loaded["data"]["verse_refs"][0]
        assert ref["local_index"] == 1
        assert ref["part_type"] == "Hadith"
        assert ref["path"] == "/books/test:1:1"
        assert "inline" not in ref

    def test_insert_chapter_shell_with_heading(self, temp_destination_dir):
        """Test headings are inlined in verse_refs"""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Chapter 1"}
        chapter.path = "/books/test:1"

        heading = Verse()
        heading.part_type = PartType.Heading
        heading.text = ["Section Title"]
        heading.local_index = 0

        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Test text"]
        v.index = 1
        v.local_index = 1
        v.path = "/books/test:1:1"
        chapter.verses = [heading, v]

        insert_chapter(chapter)

        loaded = load_json("/books/test:1")
        refs = loaded["data"]["verse_refs"]
        assert len(refs) == 2
        # Heading is inlined
        assert refs[0]["part_type"] == "Heading"
        assert "inline" in refs[0]
        assert refs[0]["inline"]["text"] == ["Section Title"]
        assert "path" not in refs[0]
        # Hadith has path, no inline
        assert refs[1]["part_type"] == "Hadith"
        assert refs[1]["path"] == "/books/test:1:1"
        assert "inline" not in refs[1]

    def test_insert_chapter_with_subchapters(self, temp_destination_dir):
        """Test inserting a chapter with nested subchapters"""
        book = Chapter()
        book.part_type = PartType.Book
        book.titles = {"en": "Test Book"}
        book.path = "/books/test"

        ch = Chapter()
        ch.part_type = PartType.Chapter
        ch.titles = {"en": "Chapter 1"}
        ch.path = "/books/test:1"

        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Verse text"]
        v.index = 1
        v.local_index = 1
        v.path = "/books/test:1:1"
        ch.verses = [v]
        book.chapters = [ch]

        insert_chapter(book)

        # Parent chapter_list should be created
        loaded_book = load_json("/books/test")
        assert loaded_book["kind"] == "chapter_list"

        # Child verse_list should be created
        loaded_ch = load_json("/books/test:1")
        assert loaded_ch["kind"] == "verse_list"


class TestInsertVerseDetails:
    """Test per-verse detail file generation."""

    def _make_chapter_with_verses(self, num_verses=3):
        """Helper to create a chapter with hadiths."""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Test Chapter", "ar": "\u0628\u0627\u0628 \u0627\u0644\u0627\u062e\u062a\u0628\u0627\u0631"}
        chapter.path = "/books/test:1:1"
        chapter.verses = []

        for i in range(1, num_verses + 1):
            v = Verse()
            v.part_type = PartType.Hadith
            v.text = [f"Arabic text {i}"]
            v.translations = {"en.hubeali": [f"English translation {i}"]}
            v.index = i
            v.local_index = i
            v.path = f"/books/test:1:1:{i}"
            chapter.verses.append(v)

        return chapter

    def test_verse_detail_files_created(self, temp_destination_dir):
        """insert_verse_details writes a JSON file for each verse."""
        chapter = self._make_chapter_with_verses(3)
        insert_verse_details(chapter)

        for i in range(1, 4):
            loaded = load_json(f"/books/test:1:1:{i}")
            assert loaded["kind"] == "verse_detail"
            assert loaded["index"] == f"test:1:1:{i}"

    def test_verse_detail_contains_verse_data(self, temp_destination_dir):
        """Verse detail includes the verse object."""
        chapter = self._make_chapter_with_verses(1)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        data = loaded["data"]
        assert data["verse"]["text"] == ["Arabic text 1"]
        assert data["verse"]["translations"]["en.hubeali"] == ["English translation 1"]
        assert data["verse"]["part_type"] == "Hadith"

    def test_verse_detail_contains_chapter_context(self, temp_destination_dir):
        """Verse detail includes chapter_path and chapter_title."""
        chapter = self._make_chapter_with_verses(1)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        data = loaded["data"]
        assert data["chapter_path"] == "/books/test:1:1"
        assert data["chapter_title"]["en"] == "Test Chapter"
        assert data["chapter_title"]["ar"] == "\u0628\u0627\u0628 \u0627\u0644\u0627\u062e\u062a\u0628\u0627\u0631"

    def test_verse_detail_navigation_first(self, temp_destination_dir):
        """First verse has next and up but no prev."""
        chapter = self._make_chapter_with_verses(3)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        nav = loaded["data"]["nav"]
        assert "prev" not in nav
        assert nav["next"] == "/books/test:1:1:2"
        assert nav["up"] == "/books/test:1:1"

    def test_verse_detail_navigation_middle(self, temp_destination_dir):
        """Middle verse has prev, next, and up."""
        chapter = self._make_chapter_with_verses(3)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:2")
        nav = loaded["data"]["nav"]
        assert nav["prev"] == "/books/test:1:1:1"
        assert nav["next"] == "/books/test:1:1:3"
        assert nav["up"] == "/books/test:1:1"

    def test_verse_detail_navigation_last(self, temp_destination_dir):
        """Last verse has prev and up but no next."""
        chapter = self._make_chapter_with_verses(3)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:3")
        nav = loaded["data"]["nav"]
        assert nav["prev"] == "/books/test:1:1:2"
        assert "next" not in nav
        assert nav["up"] == "/books/test:1:1"

    def test_verse_detail_single_verse(self, temp_destination_dir):
        """Single verse has only up navigation."""
        chapter = self._make_chapter_with_verses(1)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        nav = loaded["data"]["nav"]
        assert "prev" not in nav
        assert "next" not in nav
        assert nav["up"] == "/books/test:1:1"

    def test_verse_detail_with_gradings(self, temp_destination_dir):
        """Verse with gradings includes them in detail."""
        chapter = self._make_chapter_with_verses(1)
        chapter.verses[0].gradings = {
            "majlisi": "Sahih",
            "mohseni": "Mu'tabar",
        }
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        assert loaded["data"]["gradings"]["majlisi"] == "Sahih"
        assert loaded["data"]["gradings"]["mohseni"] == "Mu'tabar"

    def test_verse_detail_with_source_url(self, temp_destination_dir):
        """Verse with source_url includes it in detail."""
        chapter = self._make_chapter_with_verses(1)
        chapter.verses[0].source_url = "https://thaqalayn.net/hadith/1/1/1/1"
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        assert loaded["data"]["source_url"] == "https://thaqalayn.net/hadith/1/1/1/1"

    def test_verse_detail_omits_empty_gradings(self, temp_destination_dir):
        """Verse without gradings does not include gradings key."""
        chapter = self._make_chapter_with_verses(1)
        insert_verse_details(chapter)

        loaded = load_json("/books/test:1:1:1")
        assert "gradings" not in loaded["data"]
        assert "source_url" not in loaded["data"]

    def test_verse_detail_skips_headings(self, temp_destination_dir):
        """Heading part_type verses are not given detail files."""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Test"}
        chapter.path = "/books/test:1"
        chapter.verses = []

        heading = Verse()
        heading.part_type = PartType.Heading
        heading.text = ["Heading text"]
        heading.path = "/books/test:1:h1"
        chapter.verses.append(heading)

        hadith = Verse()
        hadith.part_type = PartType.Hadith
        hadith.text = ["Hadith text"]
        hadith.index = 1
        hadith.local_index = 1
        hadith.path = "/books/test:1:1"
        chapter.verses.append(hadith)

        insert_verse_details(chapter)

        # Hadith should have detail file
        loaded = load_json("/books/test:1:1")
        assert loaded["kind"] == "verse_detail"

        # Heading should NOT have detail file
        assert not os.path.exists(get_dest_path("/books/test:1:h1"))

    def test_insert_chapter_creates_verse_details(self, temp_destination_dir):
        """insert_chapter automatically creates verse detail files."""
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": "Chapter 1"}
        chapter.path = "/books/test:1"
        chapter.verses = []

        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Test text"]
        v.index = 1
        v.local_index = 1
        v.path = "/books/test:1:1"
        chapter.verses = [v]

        insert_chapter(chapter)

        # Shell chapter should exist with verse_refs (no verses)
        chapter_json = load_json("/books/test:1")
        assert chapter_json["kind"] == "verse_list"
        assert "verses" not in chapter_json["data"]
        assert "verse_refs" in chapter_json["data"]

        # Individual verse_detail should also exist
        verse_json = load_json("/books/test:1:1")
        assert verse_json["kind"] == "verse_detail"


class TestShellifyCompleteBooks:
    """Test conversion of complete book files to shell format."""

    def test_shellify_node_converts_verses_to_refs(self):
        """_shellify_node replaces verses with verse_refs."""
        node = {
            "verses": [
                {"local_index": 1, "part_type": "Hadith", "path": "/books/test:1:1", "text": ["arabic"]},
                {"local_index": 2, "part_type": "Hadith", "path": "/books/test:1:2", "text": ["arabic2"]},
            ],
            "verse_translations": ["en.hubeali"],
        }
        count = _shellify_node(node)
        assert count == 1
        assert "verses" not in node
        assert len(node["verse_refs"]) == 2
        assert node["verse_refs"][0]["path"] == "/books/test:1:1"
        assert node["verse_refs"][0]["part_type"] == "Hadith"
        assert "text" not in node["verse_refs"][0]

    def test_shellify_node_inlines_headings(self):
        """_shellify_node inlines Heading verses."""
        node = {
            "verses": [
                {"local_index": 0, "part_type": "Heading", "text": ["Title"]},
                {"local_index": 1, "part_type": "Hadith", "path": "/books/test:1:1"},
            ],
        }
        _shellify_node(node)
        assert node["verse_refs"][0]["part_type"] == "Heading"
        assert node["verse_refs"][0]["inline"]["text"] == ["Title"]
        assert "path" not in node["verse_refs"][0]
        assert node["verse_refs"][1]["path"] == "/books/test:1:1"

    def test_shellify_node_recursive(self):
        """_shellify_node recurses into chapters."""
        node = {
            "chapters": [
                {
                    "chapters": [
                        {
                            "verses": [
                                {"local_index": 1, "part_type": "Hadith", "path": "/books/test:1:1:1"},
                            ],
                        },
                    ],
                },
            ],
        }
        count = _shellify_node(node)
        assert count == 1
        leaf = node["chapters"][0]["chapters"][0]
        assert "verses" not in leaf
        assert leaf["verse_refs"][0]["path"] == "/books/test:1:1:1"

    def test_shellify_node_no_verses(self):
        """_shellify_node is a no-op for nodes without verses."""
        node = {"chapters": [{"chapters": []}]}
        count = _shellify_node(node)
        assert count == 0

    def test_shellify_complete_books(self, temp_destination_dir):
        """shellify_complete_books converts on-disk complete book files."""
        complete_dir = os.path.join(str(temp_destination_dir), "books", "complete")
        os.makedirs(complete_dir)

        doc = {
            "kind": "complete_book",
            "index": "test",
            "data": {
                "chapters": [
                    {
                        "verses": [
                            {"local_index": 1, "part_type": "Hadith", "path": "/books/test:1:1", "text": ["arabic"]},
                        ],
                        "verse_translations": ["en.hubeali"],
                    },
                ],
            },
        }
        with open(os.path.join(complete_dir, "test.json"), "w", encoding="utf-8") as f:
            json.dump(doc, f)

        count = shellify_complete_books()
        assert count == 1

        with open(os.path.join(complete_dir, "test.json"), "r", encoding="utf-8") as f:
            result = json.load(f)

        leaf = result["data"]["chapters"][0]
        assert "verses" not in leaf
        assert len(leaf["verse_refs"]) == 1
        assert leaf["verse_refs"][0]["path"] == "/books/test:1:1"
