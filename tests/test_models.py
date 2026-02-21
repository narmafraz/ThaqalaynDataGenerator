from fastapi.encoders import jsonable_encoder
from app.models import Chapter, Verse, PartType, Language, Navigation, Crumb
from app.models.translation import Translation
from app.models.people import Narrator, ChainVerses, NarratorIndex
from app.models.quran import NarratorChain, SpecialText


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


class TestModelRoundTrip:
    """Test that models can serialize and deserialize without data loss."""

    def test_chapter_round_trip(self):
        """Chapter -> JSON -> Chapter preserves fields."""
        ch = Chapter()
        ch.part_type = PartType.Chapter
        ch.titles = {"en": "Test Chapter", "ar": "باب اختبار"}
        ch.path = "/books/test:1"
        ch.index = 1
        ch.local_index = 1
        ch.verse_count = 5
        ch.verse_start_index = 0

        json_data = jsonable_encoder(ch)
        restored = Chapter(**json_data)

        assert restored.titles == ch.titles
        assert restored.path == ch.path
        assert restored.part_type == ch.part_type
        assert restored.index == ch.index
        assert restored.verse_count == ch.verse_count

    def test_verse_round_trip(self):
        """Verse -> JSON -> Verse preserves all fields."""
        v = Verse()
        v.part_type = PartType.Hadith
        v.index = 42
        v.local_index = 3
        v.path = "/books/al-kafi:1:1:1:3"
        v.text = ["Arabic hadith text here"]
        v.translations = {
            "en.hubeali": ["English translation text"],
            "en.sarwar": ["Sarwar translation"],
        }

        json_data = jsonable_encoder(v)
        restored = Verse(**json_data)

        assert restored.index == v.index
        assert restored.local_index == v.local_index
        assert restored.path == v.path
        assert restored.text == v.text
        assert restored.translations == v.translations
        assert restored.part_type == v.part_type

    def test_verse_with_narrator_chain_round_trip(self):
        """Verse with narrator_chain survives round-trip."""
        v = Verse()
        v.part_type = PartType.Hadith
        v.path = "/books/test:1"
        v.text = ["text"]

        chain = NarratorChain()
        chain.text = "narrator chain text"
        chain.parts = []

        part1 = SpecialText()
        part1.kind = "narrator"
        part1.text = "مُحَمَّدُ بْنُ يَحْيَى"
        part1.path = "/people/narrators/1"
        chain.parts.append(part1)

        part2 = SpecialText()
        part2.kind = "plain"
        part2.text = " عَنْ "
        chain.parts.append(part2)

        v.narrator_chain = chain

        json_data = jsonable_encoder(v)
        restored = Verse(**json_data)

        assert restored.narrator_chain is not None
        assert restored.narrator_chain.text == "narrator chain text"
        assert len(restored.narrator_chain.parts) == 2
        assert restored.narrator_chain.parts[0].kind == "narrator"
        assert restored.narrator_chain.parts[0].path == "/people/narrators/1"

    def test_translation_model(self):
        """Translation model serializes correctly."""
        t = Translation(name="Test Author", id="en.test", lang="en")
        json_data = jsonable_encoder(t)
        assert json_data["name"] == "Test Author"
        assert json_data["id"] == "en.test"
        assert json_data["lang"] == "en"

        restored = Translation(**json_data)
        assert restored.name == t.name
        assert restored.id == t.id
        assert restored.lang == t.lang

    def test_narrator_model_round_trip(self):
        """Narrator model serializes and deserializes."""
        n = Narrator()
        n.index = 1
        n.path = "/people/narrators/1"
        n.titles = {"ar": "أَبُو جَعْفَرٍ"}
        n.verse_paths = {"/books/al-kafi:1:1:1:1", "/books/al-kafi:1:1:1:2"}
        n.subchains = {}

        cv = ChainVerses()
        cv.narrator_ids = [1, 2]
        cv.verse_paths = {"/books/al-kafi:1:1:1:1"}
        n.subchains["1-2"] = cv

        json_data = jsonable_encoder(n)
        assert json_data["index"] == 1
        assert json_data["titles"]["ar"] == "أَبُو جَعْفَرٍ"
        assert len(json_data["verse_paths"]) == 2
        assert "1-2" in json_data["subchains"]

    def test_navigation_model(self):
        """Navigation model holds prev/next/up as strings."""
        nav = Navigation()
        nav.prev = "/books/quran:1"
        nav.next = "/books/quran:3"
        nav.up = "/books/quran"

        json_data = jsonable_encoder(nav)
        assert json_data["prev"] == "/books/quran:1"
        assert json_data["next"] == "/books/quran:3"
        assert json_data["up"] == "/books/quran"

    def test_crumb_model(self):
        """Crumb model holds titles and path."""
        c = Crumb()
        c.titles = {"en": "Volume One", "ar": "الجزء الأول"}
        c.path = "/books/al-kafi:1"
        c.indexed_titles = {"en": "Volume One"}

        json_data = jsonable_encoder(c)
        assert json_data["titles"]["en"] == "Volume One"
        assert json_data["path"] == "/books/al-kafi:1"

    def test_chapter_with_navigation_round_trip(self):
        """Chapter with nav serializes and restores."""
        ch = Chapter()
        ch.part_type = PartType.Chapter
        ch.titles = {"en": "Chapter 2"}
        ch.path = "/books/test:2"
        ch.nav = Navigation()
        ch.nav.prev = "/books/test:1"
        ch.nav.next = "/books/test:3"
        ch.nav.up = "/books/test"

        json_data = jsonable_encoder(ch)
        restored = Chapter(**json_data)

        assert restored.nav is not None
        assert restored.nav.prev == "/books/test:1"
        assert restored.nav.next == "/books/test:3"
        assert restored.nav.up == "/books/test"
