import os
import pytest
from pathlib import Path
from app.models import Chapter, Verse, PartType, Language


@pytest.fixture
def temp_destination_dir(tmp_path, monkeypatch):
    """Set up temporary destination directory for file I/O tests"""
    dest_dir = tmp_path / "data"
    dest_dir.mkdir()
    monkeypatch.setenv("DESTINATION_DIR", str(dest_dir) + "/")
    return dest_dir


@pytest.fixture
def simple_verse():
    """Create a basic verse for testing"""
    verse = Verse()
    verse.part_type = PartType.Hadith
    verse.text = ["مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ"]
    verse.translations = {}
    verse.index = 1
    verse.local_index = 1
    verse.path = "/books/test:1:1"
    return verse


@pytest.fixture
def simple_chapter():
    """Create chapter with verses for testing indexing"""
    chapter = Chapter()
    chapter.part_type = PartType.Chapter
    chapter.titles = {"en": "Chapter 1", "ar": "باب الأول"}
    chapter.path = "/books/test"
    chapter.crumbs = []
    chapter.verse_start_index = 0

    # Add 3 verses
    chapter.verses = []
    for i in range(1, 4):
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = [f"Test hadith {i}"]
        chapter.verses.append(v)

    return chapter


@pytest.fixture
def nested_book():
    """Create nested book structure: Book -> 2 Chapters -> 2 verses each"""
    book = Chapter()
    book.part_type = PartType.Book
    book.titles = {"en": "Test Book"}
    book.path = "/books/test"
    book.crumbs = []
    book.verse_start_index = 0
    book.chapters = []

    # Add 2 chapters with 2 verses each
    for ch_num in range(1, 3):
        chapter = Chapter()
        chapter.part_type = PartType.Chapter
        chapter.titles = {"en": f"Chapter {ch_num}"}
        chapter.crumbs = []
        chapter.verse_start_index = 0
        chapter.verses = []

        for v_num in range(1, 3):
            verse = Verse()
            verse.part_type = PartType.Hadith
            verse.text = [f"Verse {ch_num}.{v_num}"]
            chapter.verses.append(verse)

        book.chapters.append(chapter)

    return book
