from app.book_registry import BOOK_REGISTRY, BookConfig, get_book_config, get_next_book_index
from app.models.enums import Language


class TestBookRegistry:
    """Test the declarative book registry."""

    def test_registry_has_quran_and_kafi(self):
        """Registry contains the two existing books."""
        slugs = [b.slug for b in BOOK_REGISTRY]
        assert "quran" in slugs
        assert "al-kafi" in slugs

    def test_quran_config(self):
        """Quran config has correct values."""
        cfg = get_book_config("quran")
        assert cfg is not None
        assert cfg.index == 1
        assert cfg.path == "/books/quran"
        assert cfg.titles[Language.EN.value] == "The Holy Quran"
        assert cfg.titles[Language.AR.value] == "\u0627\u0644\u0642\u0631\u0622\u0646 \u0627\u0644\u0643\u0631\u064a\u0645"

    def test_kafi_config(self):
        """Al-Kafi config has correct values."""
        cfg = get_book_config("al-kafi")
        assert cfg is not None
        assert cfg.index == 2
        assert cfg.path == "/books/al-kafi"
        assert cfg.titles[Language.EN.value] == "Al-Kafi"
        assert cfg.author is not None
        assert cfg.author[Language.EN.value] == "Shaykh al-Kulayni"

    def test_get_book_config_not_found(self):
        """get_book_config returns None for unknown slugs."""
        assert get_book_config("nonexistent") is None

    def test_get_next_book_index(self):
        """Next index is one greater than the max existing index."""
        expected = max(b.index for b in BOOK_REGISTRY) + 1
        assert get_next_book_index() == expected

    def test_book_config_defaults(self):
        """BookConfig has sensible defaults for optional fields."""
        cfg = BookConfig(slug="test", index=99, path="/books/test")
        assert cfg.author is None
        assert cfg.translator is None
        assert cfg.source_url is None
        assert cfg.descriptions == {}
        assert cfg.titles == {}

    def test_registry_unique_indexes(self):
        """All books have unique index values."""
        indexes = [b.index for b in BOOK_REGISTRY]
        assert len(indexes) == len(set(indexes))

    def test_registry_unique_slugs(self):
        """All books have unique slugs."""
        slugs = [b.slug for b in BOOK_REGISTRY]
        assert len(slugs) == len(set(slugs))

    def test_registry_unique_paths(self):
        """All books have unique paths."""
        paths = [b.path for b in BOOK_REGISTRY]
        assert len(paths) == len(set(paths))
