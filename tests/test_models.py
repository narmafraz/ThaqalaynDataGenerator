from fastapi.encoders import jsonable_encoder
from app.models import Chapter, Verse, PartType, Language


class TestModelSerialization:
    """Test Pydantic model serialization"""

    def test_chapter_serialization(self, simple_chapter):
        """Test Chapter serializes to JSON"""
        json_data = jsonable_encoder(simple_chapter)
        assert json_data["titles"]["en"] == "Chapter 1"
        assert json_data["part_type"] == PartType.Chapter.value

    def test_verse_serialization(self, simple_verse):
        """Test Verse serializes correctly"""
        json_data = jsonable_encoder(simple_verse)
        assert json_data["text"][0] == "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ"

    def test_nested_chapter_serialization(self, nested_book):
        """Test nested chapter structure serializes"""
        json_data = jsonable_encoder(nested_book)
        assert json_data["titles"]["en"] == "Test Book"
        assert len(json_data["chapters"]) == 2
        assert len(json_data["chapters"][0]["verses"]) == 2

    def test_verse_with_translations(self):
        """Test verse with translations serializes"""
        verse = Verse()
        verse.part_type = PartType.Hadith
        verse.text = ["Arabic text"]
        verse.translations = {
            "en.test": ["English translation"]
        }

        json_data = jsonable_encoder(verse)
        assert "translations" in json_data
        assert "en.test" in json_data["translations"]
        assert json_data["translations"]["en.test"] == ["English translation"]
