import unittest
from app.lib_db import load_chapter

class TestKafiPaths(unittest.TestCase):
    def test_paths_not_none(self):
        kafi = load_chapter("/books/complete/al-kafi")
        
        def check_chapter(chapter):
            self.assertIsNotNone(chapter.path, f"Chapter path is None for chapter: {chapter}")
            if chapter.verses:
                for verse in chapter.verses:
                    self.assertIsNotNone(verse.path, f"Verse path is None for verse: {verse}")
            if chapter.chapters:
                for subchapter in chapter.chapters:
                    check_chapter(subchapter)
        
        check_chapter(kafi)

if __name__ == '__main__':
    unittest.main()
