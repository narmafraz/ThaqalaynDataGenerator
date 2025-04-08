import unittest
from app.models.quran import Chapter, Verse
from app.link_quran_kafi import update_refs

class TestUpdateRefs(unittest.TestCase):
    def test_update_refs_sorting(self):
        # Set up dummy Quran structure
        # Chapter 1 with 2 verses and Chapter 2 with 1 verse 
        verse1 = Verse(relations=None)
        verse2 = Verse(relations=None)
        sura1 = Chapter(verses=[verse1, verse2])
        verse3 = Verse(relations=None)
        sura2 = Chapter(verses=[verse3])
        quran = Chapter(chapters=[sura1, sura2])
        
        # Set up a dummy hadith with a unique path
        hadith = Verse(path="hadith1", translations={}, relations=None)
        
        # Create unsorted set of Quran references (sura_no, verse_no)
        quran_refs = {(2, 1), (1, 2), (1, 1)}
        
        # Execute update_refs to update both the Quran verses and the hadith
        update_refs(quran, hadith, quran_refs)
        
        # Verify that the hadith.relations has sorted Mentions correctly
        expected_mentions = ['/books/quran:1:1', '/books/quran:1:2', '/books/quran:2:1']
        self.assertEqual(hadith.relations, {'Mentions': expected_mentions})
        
        # Verify that each matching Quran verse has been updated with the hadith path
        self.assertEqual(verse1.relations, {"Mentioned In": ["hadith1"]})
        self.assertEqual(verse2.relations, {"Mentioned In": ["hadith1"]})
        self.assertEqual(verse3.relations, {"Mentioned In": ["hadith1"]})

if __name__ == "__main__":
    unittest.main()
