import json
import os
from fastapi.encoders import jsonable_encoder
from app.lib_db import (
    index_from_path, get_dest_path, clean_nones,
    write_file, load_json, load_chapter, ensure_dir,
    insert_chapter, delete_file, delete_folder,
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
        """Test inserting a leaf chapter with verses"""
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

        # Should create the JSON file
        loaded = load_json("/books/test:1")
        assert loaded["kind"] == "verse_list"
        assert loaded["index"] == "test:1"

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
