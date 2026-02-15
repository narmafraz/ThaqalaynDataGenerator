from app.link_quran_kafi import process_translation_text, QURAN_QUOTE, update_refs
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
