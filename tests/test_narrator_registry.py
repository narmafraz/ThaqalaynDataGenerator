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


class TestCanonicalLookupKey:
    """Tests for the new canonical_lookup_key + lookup_canonical_key fallback.

    These cover the AI-vs-registry honorific format mismatch and the
    leading chain-verb prefix that occasionally ends up in extracted names.
    """

    def test_strips_inline_diacritized_honorific(self):
        from app.narrator_registry import canonical_lookup_key
        # AI emits inline form with full diacritics
        ai_form = "أَبِي جَعْفَرٍ عَلَيْهِ السَّلَامُ"
        # Registry has parenthetical form
        registry_form = "أَبِي جَعْفَرٍ ( عليه السلام )"
        assert canonical_lookup_key(ai_form) == canonical_lookup_key(registry_form)

    def test_strips_parenthetical_no_space_form(self):
        from app.narrator_registry import canonical_lookup_key
        a = "أَبِي جَعْفَرٍ ( عليه السلام )"
        b = "أَبِي جَعْفَرٍ (عليه السلام)"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_strips_prophet_salawat(self):
        from app.narrator_registry import canonical_lookup_key
        a = "محمد صلى الله عليه وآله وسلم"
        b = "محمد ( صلى الله عليه وآله وسلم )"
        assert canonical_lookup_key(a) == canonical_lookup_key(b) == "محمد"

    def test_strips_leading_verb_rawa(self):
        from app.narrator_registry import canonical_lookup_key
        # Verb prefix should be stripped — "روى ابن بكير" -> "ابن بكير"
        with_verb = "رَوَى اِبْنُ بُكَيْرٍ"
        without_verb = "اِبْنُ بُكَيْرٍ"
        assert canonical_lookup_key(with_verb) == canonical_lookup_key(without_verb)

    def test_strips_leading_verb_haddathana(self):
        from app.narrator_registry import canonical_lookup_key
        a = "حدثنا محمد بن يعقوب"
        b = "محمد بن يعقوب"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_does_not_strip_qaala(self):
        """قال is too common as a real attribution; do not auto-strip."""
        from app.narrator_registry import canonical_lookup_key, _LEADING_VERB_PREFIXES
        assert "قال " not in _LEADING_VERB_PREFIXES
        assert "وقال " not in _LEADING_VERB_PREFIXES

    def test_does_not_collapse_to_empty(self):
        """A name that is JUST a leading verb should keep the verb (no name to extract)."""
        from app.narrator_registry import canonical_lookup_key
        # The strip is guarded: if stripping produces an empty result, the
        # original is kept. Post-normalize "روى" becomes "روي" (alef-maksura
        # → yeh under normalize_arabic). The leading-verb prefix list contains
        # "روي " (with trailing space), which doesn't match a bare "روي" with
        # no following text — so no strip happens here either way.
        result = canonical_lookup_key("روى")
        assert result  # non-empty
        assert "روي" in result

    def test_lookup_canonical_key_resolves_imam_baqir(self):
        """The exact case the user reported: AI-emitted Abi Jafar inline form
        should resolve to a canonical Abi Jafar entry."""
        from app.narrator_registry import NarratorRegistry
        narrators = {
            "9": {
                "canonical_name_ar": "أَبِي جَعْفَرٍ ( عليه السلام )",
                "canonical_name_en": "",
                "role": "imam",
                "variants_ar": [],
                "disambiguation_context": None,
            },
        }
        path = _create_registry_file(narrators)
        try:
            r = NarratorRegistry(path)
            # AI form should now resolve via canonical_key fallback
            ai_form = "أَبِي جَعْفَرٍ عَلَيْهِ السَّلَامُ"
            assert r.resolve(ai_form) == 9
            # Canonical-key lookup directly returns the same thing
            assert 9 in r.lookup_canonical_key(ai_form)
        finally:
            os.unlink(path)

    def test_lookup_canonical_key_resolves_with_verb_prefix(self):
        """A leading verb in the AI-emitted name should not block resolution."""
        from app.narrator_registry import NarratorRegistry
        narrators = {
            "100": {
                "canonical_name_ar": "اِبْنُ بُكَيْرٍ",
                "canonical_name_en": "Ibn Bukayr",
                "role": "narrator",
                "variants_ar": [],
                "disambiguation_context": None,
            },
        }
        path = _create_registry_file(narrators)
        try:
            r = NarratorRegistry(path)
            # AI emitted "رَوَى اِبْنُ بُكَيْرٍ" — verb prefix should be stripped
            assert r.resolve("رَوَى اِبْنُ بُكَيْرٍ") == 100
        finally:
            os.unlink(path)


class TestTrailingJunkStripping:
    """Trailing punctuation, particles, and layered honorifics — Class 3 fixes.

    The splitter's NARRATORS_TEXT_PATTERN can leak trailing particles/colons
    into the last extracted name (e.g. "...عليه السلام :"). Stripping these
    in canonical_lookup_key lets such names resolve to the right canonical id.
    """

    def test_strips_trailing_colon(self):
        from app.narrator_registry import canonical_lookup_key
        a = "أَبِي عَبْدِ اللَّهِ عَلَيْهِ السَّلَامُ :"
        b = "أَبِي عَبْدِ اللَّهِ"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_strips_trailing_arabic_comma(self):
        # ARABIC COMMA (،) becomes ASCII , via normalize_arabic, then stripped.
        from app.narrator_registry import canonical_lookup_key
        a = "ابن أبي عمير،"
        b = "ابن أبي عمير"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_strips_trailing_anna_hu(self):
        from app.narrator_registry import canonical_lookup_key
        a = "أَبِي عَبْدِ اللَّهِ عَلَيْهِ السَّلَامُ أَنَّهُ"
        b = "أَبِي عَبْدِ اللَّهِ"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_strips_trailing_qala(self):
        from app.narrator_registry import canonical_lookup_key
        a = "أبي عبد الله عليه السلام قال"
        b = "أبي عبد الله"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_does_not_strip_qala_in_middle(self):
        """`قال` only strips when it's the trailing token, not when it's in
        the middle of a name."""
        from app.narrator_registry import canonical_lookup_key
        # Hypothetical name with "قال" not at end — should be untouched.
        result = canonical_lookup_key("قال علي بن زيد")
        assert "قال علي بن زيد" in result

    def test_strips_layered_decorations(self):
        """Loop should peel each tail layer in turn:
        "( عليه السلام ) : أنه" → "(عليه السلام) :" → "(عليه السلام)" → ""
        """
        from app.narrator_registry import canonical_lookup_key
        layered = "أَبِي جَعْفَرٍ ( عليه السلام ) : أنه"
        clean = "أَبِي جَعْفَرٍ"
        assert canonical_lookup_key(layered) == canonical_lookup_key(clean)

    def test_does_not_strip_inline_ayyadahu_allah(self):
        """Tahdhib's "may Allah strengthen him" must NOT be stripped at the
        resolver layer — stripping it from "الشيخ أيده الله تعالى" produces
        ckey "الشيخ", which collides with a different registry entry (an
        Imam called "the Sheikh" in al-Kafi). This phrase is handled
        instead by the per-book preamble strip in narrator_linker."""
        from app.narrator_registry import canonical_lookup_key
        a = "اَلشَّيْخُ أَيَّدَهُ اللَّهُ تَعَالَى"
        b = "الشيخ"
        # Different ckeys — a retains the honorific.
        assert canonical_lookup_key(a) != canonical_lookup_key(b)
        # And ckey for a still includes "ايده الله" tokens.
        assert "ايده الله" in canonical_lookup_key(a)

    def test_strips_abbreviated_paren_honorifics(self):
        """Abbreviated forms (ع), (ره), (ص) common in older typesetting."""
        from app.narrator_registry import canonical_lookup_key
        for ab in ("(ع)", "(ره)", "(ص)"):
            a = f"محمد بن علي {ab}"
            b = "محمد بن علي"
            assert canonical_lookup_key(a) == canonical_lookup_key(b), ab

    def test_strips_inline_rahimahu_allah_taala(self):
        from app.narrator_registry import canonical_lookup_key
        a = "أبي رحمه الله تعالى"
        b = "أبي"
        assert canonical_lookup_key(a) == canonical_lookup_key(b)

    def test_persian_yeh_normalises(self):
        """Persian yeh ی and Arabic yeh ي should produce the same key."""
        from app.narrator_registry import canonical_lookup_key
        persian = "حدثنی محمد بن الحسن"
        arabic = "حدثني محمد بن الحسن"
        assert canonical_lookup_key(persian) == canonical_lookup_key(arabic)

    def test_persian_kaf_normalises(self):
        from app.narrator_registry import canonical_lookup_key
        persian = "محمد بن یعقوب الکلینی"
        arabic = "محمد بن يعقوب الكليني"
        assert canonical_lookup_key(persian) == canonical_lookup_key(arabic)

    def test_generic_ckey_blocks_fallback(self):
        """Entries whose canonical_lookup_key falls in _GENERIC_CKEYS must
        not be resolvable via the canonical-key fallback. They remain
        resolvable via exact and normalized lookups.

        Regression: bare "الشيخ" was mis-linking to a registry entry whose
        canonical form is "الشَّيْخِ ( عليه السلام )" — a real but more
        specific narrator. Adding "الشيخ" to _GENERIC_CKEYS blocks the
        fallback while preserving the exact-match path."""
        from app.narrator_registry import NarratorRegistry, _GENERIC_CKEYS
        narrators = {
            "9999": {
                "canonical_name_ar": "الشَّيْخِ ( عليه السلام )",
                "canonical_name_en": "the Sheikh (peace be upon him)",
                "role": "narrator",
                "variants_ar": [],
                "disambiguation_context": None,
            },
        }
        path = _create_registry_file(narrators)
        try:
            r = NarratorRegistry(path)
            assert "الشيخ" in _GENERIC_CKEYS
            # Exact form resolves fine
            assert r.resolve("الشَّيْخِ ( عليه السلام )") == 9999
            # Bare forms must NOT match
            assert r.resolve("الشيخ") is None
            assert r.resolve("الشَّيْخُ") is None
            assert r.resolve("أَخْبَرَنِي اَلشَّيْخُ") is None
            assert r.resolve("الشيخ أيده الله تعالى") is None
        finally:
            os.unlink(path)

    def test_resolve_handles_imam_with_trailing_colon(self):
        """End-to-end: a registry entry stored under the canonical form should
        resolve when the chain emits "...:"."""
        from app.narrator_registry import NarratorRegistry
        narrators = {
            "13": {
                "canonical_name_ar": "أَبِي عَبْدِ اللَّهِ ( عليه السلام )",
                "canonical_name_en": "Imam Ja'far al-Sadiq",
                "role": "imam",
                "variants_ar": [],
                "disambiguation_context": None,
            },
        }
        path = _create_registry_file(narrators)
        try:
            r = NarratorRegistry(path)
            assert r.resolve("أَبِي عَبْدِ اللَّهِ عَلَيْهِ السَّلَامُ :") == 13
            assert r.resolve("أَبِي عَبْدِ اللَّهِ عَلَيْهِ السَّلَامُ أَنَّهُ") == 13
        finally:
            os.unlink(path)


class TestDisambiguationBooks:
    """Book-scope disambiguation: an entry's ``disambiguation_books`` field
    constrains it to only resolve when the chain comes from a listed book.

    Use case: al-Mufid is conventionally referred to as "الشيخ" in Tahdhib
    al-Ahkam and al-Istibsar (Tusi's editorial references to his teacher),
    but bare "الشيخ" elsewhere should not resolve to him.
    """

    @pytest.fixture
    def book_registry_path(self):
        """Registry with: an unscoped Imam ref + a book-scoped al-Mufid
        sharing the variant 'الشيخ'."""
        narrators = {
            "100": {
                "canonical_name_ar": "الشَّيْخِ ( عليه السلام )",
                "canonical_name_en": "the Sheikh (an Imam reference)",
                "role": "imam",
                "variants_ar": [],
                "disambiguation_context": None,
            },
            "200": {
                "canonical_name_ar": "الشَّيْخُ الْمُفِيدُ",
                "canonical_name_en": "al-Shaykh al-Mufid",
                "role": "scholar",
                "variants_ar": ["الشَّيْخُ", "أَخْبَرَنِي الشَّيْخُ"],
                "disambiguation_context": None,
                "disambiguation_books": ["tahdhib-al-ahkam", "al-istibsar"],
            },
        }
        path = _create_registry_file(narrators)
        yield path
        os.unlink(path)

    def test_resolves_in_listed_book(self, book_registry_path):
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry(book_registry_path)
        # Bare الشيخ in Tahdhib → al-Mufid
        assert r.resolve("الشَّيْخُ", book_slug="tahdhib-al-ahkam") == 200
        assert r.resolve("الشيخ", book_slug="tahdhib-al-ahkam") == 200
        # Same in al-Istibsar
        assert r.resolve("الشَّيْخُ", book_slug="al-istibsar") == 200

    def test_does_not_resolve_in_other_book(self, book_registry_path):
        """Outside listed books, bare الشيخ must NOT resolve to al-Mufid.
        The other candidate (id 100) is the Imam reference which uses the
        parenthetical form — bare الشيخ shouldn't match it either."""
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry(book_registry_path)
        assert r.resolve("الشَّيْخُ", book_slug="al-kafi") is None
        assert r.resolve("الشيخ", book_slug="al-kafi") is None
        assert r.resolve("الشيخ", book_slug="man-la-yahduruhu-al-faqih") is None

    def test_parenthetical_imam_form_still_resolves_everywhere(self, book_registry_path):
        """Entry 100 (the Imam ref) has no disambiguation_books, so its
        exact-match form resolves regardless of book."""
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry(book_registry_path)
        for book in ("tahdhib-al-ahkam", "al-istibsar", "al-kafi", None):
            assert r.resolve("الشَّيْخِ ( عليه السلام )", book_slug=book) == 100, book

    def test_verbal_form_resolves_in_listed_book(self, book_registry_path):
        """The "أخبرني الشيخ" variant (declared explicitly) resolves
        in-scope; the bare form (via normalised match on الشيخ variant)
        does too."""
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry(book_registry_path)
        assert r.resolve("أَخْبَرَنِي الشَّيْخُ", book_slug="tahdhib-al-ahkam") == 200
        assert r.resolve("أَخْبَرَنِي الشَّيْخُ", book_slug="al-kafi") is None

    def test_no_book_context_skips_filter(self, book_registry_path):
        """Passing book_slug=None keeps the existing behaviour — entries
        with disambiguation_books are NOT filtered out. This preserves
        backward compatibility for callers that don't yet thread the book
        context (and matters for tests / one-off scripts)."""
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry(book_registry_path)
        # With no book context, the al-Mufid match wins (only candidate
        # for bare الشيخ).
        assert r.resolve("الشَّيْخُ") == 200

    def test_filter_returns_none_when_all_scoped_out(self, book_registry_path):
        """If the only candidate is book-scoped and the current book is
        not in scope, resolve returns None (rather than picking the
        out-of-scope candidate as a default)."""
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry(book_registry_path)
        assert r.resolve("الشَّيْخُ", book_slug="nahj-al-balagha") is None

    def test_real_registry_al_mufid_entry(self):
        """Verify the actual al-Mufid entry (id 4630) we added to the
        live canonical_narrators.json behaves correctly."""
        from app.narrator_registry import NarratorRegistry
        r = NarratorRegistry()  # default path = real registry
        # In Tahdhib / Istibsar: bare الشيخ → 4630
        assert r.resolve("الشَّيْخُ", book_slug="tahdhib-al-ahkam") == 4630
        assert r.resolve("الشَّيْخُ", book_slug="al-istibsar") == 4630
        # Elsewhere: None
        assert r.resolve("الشَّيْخُ", book_slug="al-kafi") is None
        assert r.resolve("الشَّيْخُ", book_slug="man-la-yahduruhu-al-faqih") is None
        # The al-Kafi-style parenthetical form still resolves to 2709
        assert r.resolve("الشَّيْخِ ( عليه السلام )", book_slug="al-kafi") == 2709
        # Tahdhib's editorial phrasing
        assert r.resolve("اَلشَّيْخُ أَيَّدَهُ اَللَّهُ تَعَالَى",
                         book_slug="tahdhib-al-ahkam") == 4630
        # Mid-chain verbal form
        assert r.resolve("أَخْبَرَنِي اَلشَّيْخُ",
                         book_slug="tahdhib-al-ahkam") == 4630

