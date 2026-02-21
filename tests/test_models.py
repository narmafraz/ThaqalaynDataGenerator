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


class TestSchemaEvolution:
    """Test Phase 3B schema additions."""

    def test_verse_gradings_dict(self):
        """Gradings field accepts Dict[str, str] for grading systems."""
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Arabic text"]
        v.gradings = {
            "majlisi": "Sahih",
            "mohseni": "Mu'tabar",
            "behbudi": "Sahih",
        }

        json_data = jsonable_encoder(v)
        assert json_data["gradings"]["majlisi"] == "Sahih"
        assert json_data["gradings"]["mohseni"] == "Mu'tabar"
        assert json_data["gradings"]["behbudi"] == "Sahih"

        restored = Verse(**json_data)
        assert restored.gradings == v.gradings

    def test_verse_source_url(self):
        """Verse source_url links back to source site."""
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Arabic text"]
        v.source_url = "https://thaqalayn.net/hadith/1/1/1/1"

        json_data = jsonable_encoder(v)
        assert json_data["source_url"] == "https://thaqalayn.net/hadith/1/1/1/1"

        restored = Verse(**json_data)
        assert restored.source_url == v.source_url

    def test_verse_gradings_none_by_default(self):
        """Gradings field is None by default and omitted from JSON."""
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["text"]

        assert v.gradings is None

    def test_verse_source_url_none_by_default(self):
        """source_url is None by default."""
        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["text"]

        assert v.source_url is None

    def test_section_part_type(self):
        """Section PartType for ThaqalaynAPI books with sections."""
        ch = Chapter()
        ch.part_type = PartType.Section
        ch.titles = {"en": "Section 1"}
        ch.path = "/books/test:1"

        json_data = jsonable_encoder(ch)
        assert json_data["part_type"] == "Section"

        restored = Chapter(**json_data)
        assert restored.part_type == PartType.Section

    def test_french_language(self):
        """FR language enum for French translations."""
        assert Language.FR.value == "fr"

        v = Verse()
        v.part_type = PartType.Hadith
        v.text = ["Arabic text"]
        v.translations = {
            "fr.test": ["Traduction francaise"]
        }

        json_data = jsonable_encoder(v)
        assert json_data["translations"]["fr.test"] == ["Traduction francaise"]

    def test_chapter_book_metadata(self):
        """Chapter supports author, translator, source_url metadata."""
        ch = Chapter()
        ch.part_type = PartType.Book
        ch.titles = {"en": "Al-Kafi", "ar": "\u0627\u0644\u0643\u0627\u0641\u064a"}
        ch.path = "/books/al-kafi"
        ch.author = {
            "en": "Shaykh al-Kulayni",
            "ar": "\u0627\u0644\u0634\u064a\u062e \u0627\u0644\u0643\u0644\u064a\u0646\u064a",
        }
        ch.translator = {
            "en": "HubeAli.com",
        }
        ch.source_url = "https://thaqalayn.net/"

        json_data = jsonable_encoder(ch)
        assert json_data["author"]["en"] == "Shaykh al-Kulayni"
        assert json_data["translator"]["en"] == "HubeAli.com"
        assert json_data["source_url"] == "https://thaqalayn.net/"

        restored = Chapter(**json_data)
        assert restored.author == ch.author
        assert restored.translator == ch.translator
        assert restored.source_url == ch.source_url

    def test_chapter_metadata_none_by_default(self):
        """Book metadata fields are None by default."""
        ch = Chapter()
        assert ch.author is None
        assert ch.translator is None
        assert ch.source_url is None
