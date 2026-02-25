"""Tests for the Quran parser (quran.py).

These tests use the actual raw source files from ThaqalaynDataSources.
Tests are skipped if the source files are not available.
"""
import os

import pytest

from app.config import get_raw_path
from app.models import PartType

# Check if source files exist
_raw_dir = get_raw_path("tanzil_net")
_has_quran_text = os.path.isfile(os.path.join(_raw_dir, "quran_simple.txt"))
_has_quran_xml = os.path.isfile(os.path.join(_raw_dir, "quran-data.xml"))

pytestmark = pytest.mark.skipif(
    not (_has_quran_text and _has_quran_xml),
    reason="Requires raw Quran source files in app/raw/tanzil_net/",
)


class TestBuildVerses:
    """Test quran.build_verses() parses the Quran text file."""

    def test_builds_correct_number_of_verses(self):
        from app.quran import build_verses, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        # The Quran has 6236 verses
        assert len(verses) == 6236

    def test_first_verse_is_bismillah(self):
        from app.quran import build_verses, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        # First verse should be Bismillah
        assert "بِسْمِ" in verses[0].text[0]

    def test_all_verses_have_text(self):
        from app.quran import build_verses, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        for i, verse in enumerate(verses):
            assert len(verse.text) > 0, f"Verse {i} has no text"
            assert len(verse.text[0].strip()) > 0, f"Verse {i} has empty text"

    def test_all_verses_have_verse_part_type(self):
        from app.quran import build_verses, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        for i, verse in enumerate(verses):
            assert verse.part_type == PartType.Verse, \
                f"Verse {i} has part_type {verse.part_type}, expected Verse"

    def test_verses_have_empty_translations(self):
        from app.quran import build_verses, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        assert verses[0].translations == {}


class TestBuildChapters:
    """Test quran.build_chapters() parses quran-data.xml."""

    def test_builds_114_chapters(self):
        from app.quran import build_verses, build_chapters, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        chapters = build_chapters(
            get_path("tanzil_net/quran-data.xml"), verses, []
        )
        assert len(chapters) == 114

    def test_first_chapter_is_fatiha(self):
        from app.quran import build_verses, build_chapters, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        chapters = build_chapters(
            get_path("tanzil_net/quran-data.xml"), verses, []
        )
        fatiha = chapters[0]
        assert fatiha.titles["en"] == "The Opening"
        assert "الفاتحة" in fatiha.titles["ar"]

    def test_fatiha_has_7_verses(self):
        from app.quran import build_verses, build_chapters, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        chapters = build_chapters(
            get_path("tanzil_net/quran-data.xml"), verses, []
        )
        assert len(chapters[0].verses) == 7

    def test_last_chapter_is_an_nas(self):
        from app.quran import build_verses, build_chapters, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        chapters = build_chapters(
            get_path("tanzil_net/quran-data.xml"), verses, []
        )
        an_nas = chapters[113]
        assert an_nas.titles["en"] == "Mankind"

    def test_chapters_have_reveal_type(self):
        from app.quran import build_verses, build_chapters, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        chapters = build_chapters(
            get_path("tanzil_net/quran-data.xml"), verses, []
        )
        for ch in chapters:
            assert ch.reveal_type in ("Meccan", "Medinan"), \
                f"Invalid reveal_type: {ch.reveal_type}"

    def test_chapters_have_part_type_chapter(self):
        from app.quran import build_verses, build_chapters, get_path
        verses = build_verses(get_path("tanzil_net/quran_simple.txt"))
        chapters = build_chapters(
            get_path("tanzil_net/quran-data.xml"), verses, []
        )
        for ch in chapters:
            assert ch.part_type == PartType.Chapter


class TestGetSajdaData:
    """Test sajda (prostration) position extraction."""

    def test_sajda_data_extracted(self):
        import xml.etree.ElementTree
        from app.quran import get_sajda_data, get_path
        quran = xml.etree.ElementTree.parse(
            get_path("tanzil_net/quran-data.xml")
        ).getroot()
        sajdas = get_sajda_data(quran)
        # The Quran has 14 sajda positions (some recommended, some obligatory)
        assert len(sajdas) >= 14

    def test_sajda_types_valid(self):
        import xml.etree.ElementTree
        from app.quran import get_sajda_data, get_path
        quran = xml.etree.ElementTree.parse(
            get_path("tanzil_net/quran-data.xml")
        ).getroot()
        sajdas = get_sajda_data(quran)
        for (sura, aya), stype in sajdas.items():
            assert stype in ("recommended", "obligatory"), \
                f"Invalid sajda type '{stype}' at ({sura}, {aya})"

    def test_sajda_positions_have_valid_ranges(self):
        import xml.etree.ElementTree
        from app.quran import get_sajda_data, get_path
        quran = xml.etree.ElementTree.parse(
            get_path("tanzil_net/quran-data.xml")
        ).getroot()
        sajdas = get_sajda_data(quran)
        for (sura, aya) in sajdas:
            assert 1 <= sura <= 114, f"Invalid sura number {sura}"
            assert aya >= 1, f"Invalid aya number {aya}"
