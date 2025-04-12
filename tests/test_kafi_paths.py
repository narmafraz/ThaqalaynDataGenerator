import unittest
from app.lib_db import load_chapter
from app.models.enums import PartType

class TestKafiPaths(unittest.TestCase):
    def test_paths_not_none(self):
        kafi = load_chapter("/books/complete/al-kafi")
        
        errors = []
        
        def check_chapter(chapter, parent_info="root", index_info=""):
            if chapter.path is None:
                errors.append(f"{parent_info} - chapter at index {index_info} (object: {chapter}) has path None")
            if chapter.verses:
                for i, verse in enumerate(chapter.verses):
                    if verse.part_type is not PartType.Heading and verse.path is None:
                        errors.append(f"{chapter.path or parent_info} - verse at index {i} has path None")
            if chapter.chapters:
                for i, subchapter in enumerate(chapter.chapters):
                    check_chapter(subchapter, parent_info=(chapter.path or parent_info), index_info=i)
        
        check_chapter(kafi)
        if errors:
            self.fail("Entities with missing path detected:\n" + "\n".join(errors))

if __name__ == '__main__':
    unittest.main()
