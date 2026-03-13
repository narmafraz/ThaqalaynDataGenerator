"""Tests for the canonical narrator registry."""

import json
import os
import tempfile

import pytest

from app.narrator_registry import NarratorRegistry


def _create_registry_file(narrators: dict, version="1.0.0", last_id=None) -> str:
    """Helper: write a temp registry JSON file, return its path."""
    if last_id is None:
        last_id = max((int(k) for k in narrators), default=0)
    data = {
        "version": version,
        "last_id": last_id,
        "narrators": narrators,
    }
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return path


@pytest.fixture
def sample_registry_path():
    """Create a sample registry with a few narrators."""
    narrators = {
        "1": {
            "canonical_name_ar": "جعفر بن محمد الصادق",
            "canonical_name_en": "Imam Ja'far al-Sadiq (AS)",
            "role": "imam",
            "variants_ar": [
                "أَبُو عَبْدِ اللَّهِ",
                "أَبِي عَبْدِ اللَّهِ",
                "الصَّادِقُ",
            ],
            "disambiguation_context": None,
            "old_ids": [42, 187],
        },
        "2": {
            "canonical_name_ar": "إبراهيم بن هاشم القمي",
            "canonical_name_en": "Ibrahim ibn Hashim al-Qummi",
            "role": "narrator",
            "variants_ar": ["أَبِيهِ"],
            "disambiguation_context": "When preceded by عَلِيُّ بْنُ إِبْرَاهِيمَ in the chain",
            "old_ids": [55, 230],
        },
        "3": {
            "canonical_name_ar": "علي بن إبراهيم",
            "canonical_name_en": "Ali ibn Ibrahim al-Qummi",
            "role": "narrator",
            "variants_ar": ["عَلِيُّ بْنُ إِبْرَاهِيمَ"],
            "disambiguation_context": None,
            "old_ids": [10],
        },
        "4": {
            "canonical_name_ar": "محمد بن يحيى",
            "canonical_name_en": "Muhammad ibn Yahya",
            "role": "narrator",
            "variants_ar": ["مُحَمَّدُ بْنُ يَحْيَى"],
            "disambiguation_context": None,
            "old_ids": [20],
        },
    }
    path = _create_registry_file(narrators, last_id=4)
    yield path
    os.unlink(path)


class TestRegistryLoading:
    """Test loading the registry from JSON."""

    def test_load_basic(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.narrator_count == 4
        assert registry.last_id == 4
        assert registry.version == "1.0.0"

    def test_load_nonexistent_file(self):
        registry = NarratorRegistry("/nonexistent/path.json")
        assert registry.narrator_count == 0
        assert registry.last_id == 0

    def test_load_empty_registry(self):
        path = _create_registry_file({}, last_id=0)
        try:
            registry = NarratorRegistry(path)
            assert registry.narrator_count == 0
        finally:
            os.unlink(path)


class TestExactLookup:
    """Test exact Arabic name lookups."""

    def test_lookup_canonical_name(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.lookup_exact("جعفر بن محمد الصادق") == 1

    def test_lookup_variant(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.lookup_exact("أَبُو عَبْدِ اللَّهِ") == 1
        assert registry.lookup_exact("أَبِي عَبْدِ اللَّهِ") == 1

    def test_lookup_missing(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.lookup_exact("nonexistent") is None


class TestNormalizedLookup:
    """Test normalized Arabic name lookups."""

    def test_lookup_returns_list(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        results = registry.lookup_normalized("جعفر بن محمد الصادق")
        assert isinstance(results, list)
        assert 1 in results

    def test_lookup_diacritized_variant(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        # The diacritized variant should normalize to the same form
        results = registry.lookup_normalized("عَلِيُّ بْنُ إِبْرَاهِيمَ")
        assert 3 in results

    def test_lookup_missing(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        results = registry.lookup_normalized("totally unknown")
        assert results == []


class TestResolve:
    """Test context-aware disambiguation."""

    def test_resolve_exact_match(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.resolve("أَبُو عَبْدِ اللَّهِ") == 1

    def test_resolve_with_disambiguation(self, sample_registry_path):
        """أَبِيهِ preceded by Ali ibn Ibrahim should resolve to Ibrahim ibn Hashim."""
        registry = NarratorRegistry(sample_registry_path)
        result = registry.resolve(
            "أَبِيهِ",
            preceding_names=["عَلِيُّ بْنُ إِبْرَاهِيمَ"],
        )
        assert result == 2

    def test_resolve_no_context(self, sample_registry_path):
        """أَبِيهِ without context should still resolve (exact match exists)."""
        registry = NarratorRegistry(sample_registry_path)
        result = registry.resolve("أَبِيهِ")
        # Should resolve via exact match on variant
        assert result == 2

    def test_resolve_unknown(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.resolve("completely unknown name") is None


class TestGetNarrator:
    """Test getting full narrator entries."""

    def test_get_existing(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        entry = registry.get_narrator(1)
        assert entry is not None
        assert entry["canonical_name_ar"] == "جعفر بن محمد الصادق"
        assert entry["role"] == "imam"

    def test_get_nonexistent(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.get_narrator(999) is None

    def test_get_name_ar(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.get_name_ar(1) == "جعفر بن محمد الصادق"

    def test_get_name_en(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        assert registry.get_name_en(1) == "Imam Ja'far al-Sadiq (AS)"


class TestRegisterVariant:
    """Test adding new variants."""

    def test_register_new_variant(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        registry.register_variant(1, "الإمام الصادق")
        assert registry.lookup_exact("الإمام الصادق") == 1

    def test_register_duplicate_variant(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        registry.register_variant(1, "أَبُو عَبْدِ اللَّهِ")  # Already exists
        # Should not crash, entry should still work
        assert registry.lookup_exact("أَبُو عَبْدِ اللَّهِ") == 1

    def test_register_variant_invalid_id(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        with pytest.raises(ValueError):
            registry.register_variant(999, "test")


class TestAllIds:
    """Test all_ids method."""

    def test_returns_sorted(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        ids = registry.all_ids()
        assert ids == [1, 2, 3, 4]


class TestSaveRegistry:
    """Test saving registry back to disk."""

    def test_save_and_reload(self, sample_registry_path):
        registry = NarratorRegistry(sample_registry_path)
        registry.register_variant(1, "new_variant_test")

        # Save to new file
        fd, save_path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            registry.save(save_path)

            # Reload and verify
            registry2 = NarratorRegistry(save_path)
            assert registry2.narrator_count == 4
            assert registry2.lookup_exact("new_variant_test") == 1
        finally:
            os.unlink(save_path)
