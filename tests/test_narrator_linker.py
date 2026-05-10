"""Tests for the generic narrator linker module."""

import json
import os
import tempfile

import pytest

from app.models.quran import NarratorChain, SpecialText, Verse
from app.narrator_linker import (
    _book_slug_from_path,
    _looks_like_isnad,
    _strip_book_preamble,
    build_chain_parts,
    extract_isnad_text,
    link_verse_narrators,
    resolve_narrators,
    split_narrator_names,
    strip_html,
)
from app.narrator_registry import NarratorRegistry


# ── Test helpers ────────────────────────────────────────────────────────


def _make_verse(text: str) -> Verse:
    """Create a Verse with the given text as first line."""
    v = Verse()
    v.text = [text]
    v.path = "/books/test:1:1:1"
    return v


def _create_registry_file(narrators: dict) -> str:
    """Create a temporary registry file."""
    data = {
        "version": "1.0.0",
        "last_id": max((int(k) for k in narrators), default=0),
        "narrators": narrators,
    }
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return path


@pytest.fixture
def registry():
    """Create a test registry with common Al-Kafi narrators."""
    narrators = {
        "1": {
            "canonical_name_ar": "مُحَمَّدُ بْنُ يَحْيَى",
            "canonical_name_en": "Muhammad ibn Yahya",
            "role": "narrator",
            "variants_ar": [],
            "disambiguation_context": None,
            "old_ids": [],
        },
        "2": {
            "canonical_name_ar": "أَحْمَدَ بْنِ مُحَمَّدٍ",
            "canonical_name_en": "Ahmad ibn Muhammad",
            "role": "narrator",
            "variants_ar": [],
            "disambiguation_context": None,
            "old_ids": [],
        },
        "3": {
            "canonical_name_ar": "عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "canonical_name_en": "Ali ibn Ibrahim",
            "role": "narrator",
            "variants_ar": [],
            "disambiguation_context": None,
            "old_ids": [],
        },
        "4": {
            "canonical_name_ar": "أَبِيهِ",
            "canonical_name_en": "his father",
            "role": "narrator",
            "variants_ar": [],
            "disambiguation_context": "When preceded by عَلِيُّ بْنُ إِبْرَاهِيمَ",
            "old_ids": [],
        },
        "5": {
            "canonical_name_ar": "أَبِي عَبْدِ اللَّهِ ( عليه السلام )",
            "canonical_name_en": "Imam al-Sadiq (AS)",
            "role": "imam",
            "variants_ar": [],
            "disambiguation_context": None,
            "old_ids": [],
        },
    }
    path = _create_registry_file(narrators)
    reg = NarratorRegistry(path)
    yield reg
    os.unlink(path)


# ── strip_html tests ───────────────────────────────────────────────────


class TestStripHtml:
    def test_strips_span_tags(self):
        assert strip_html('<span class="x">text</span>') == "text"

    def test_no_tags(self):
        assert strip_html("plain text") == "plain text"


# ── extract_isnad_text tests ───────────────────────────────────────────


class TestExtractIsnadText:
    def test_basic_extraction(self):
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ test text"
        )
        result = extract_isnad_text(verse)
        assert result is not None
        assert "مُحَمَّدُ بْنُ يَحْيَى" in result
        assert "أَحْمَدَ بْنِ مُحَمَّدٍ" in result
        # Verse text should be modified to remove the chain
        assert verse.text[0].strip().startswith("test") or verse.text[0].strip() == ""

    def test_no_match(self):
        verse = _make_verse("plain text without any narrator patterns")
        result = extract_isnad_text(verse)
        assert result is None

    def test_empty_text(self):
        verse = Verse()
        verse.text = []
        result = extract_isnad_text(verse)
        assert result is None

    def test_failover_pattern(self):
        verse = _make_verse(
            "أَبُو عَبْدِ اللَّهِ الْأَشْعَرِيُّ عَنْ بَعْضِ أَصْحَابِنَا "
            "رَفَعَهُ عَنْ هِشَامِ بْنِ الْحَكَمِ قَالَ text"
        )
        result = extract_isnad_text(verse)
        assert result is not None

    def test_sets_narrator_chain(self):
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ text"
        )
        extract_isnad_text(verse)
        assert verse.narrator_chain is not None
        assert verse.narrator_chain.text is not None
        assert verse.narrator_chain.parts is not None

    def test_idempotent_re_extracts_from_parts(self):
        """When the chain has already been extracted on a prior run
        (verse.text[0] is matn-only, narrator_chain.parts contains the
        chain), reconstruct the chain text from parts instead of returning
        None.

        Without this, a re-run of process_all_narrators on processed
        data extracts zero chains and (combined with the pre-extraction
        folder delete) silently destroys all narrator profile pages.
        """
        # Simulate "already processed" state:
        # - verse.text[0] is just the matn
        # - narrator_chain.parts holds the chain text in segments
        verse = Verse()
        verse.text = ["just the matn here"]
        verse.narrator_chain = NarratorChain()
        p1 = SpecialText()
        p1.kind = "narrator"
        p1.text = "مُحَمَّدُ بْنُ يَحْيَى"
        p1.path = "/people/narrators/4"
        p2 = SpecialText()
        p2.kind = "plain"
        p2.text = " عَنْ "
        p3 = SpecialText()
        p3.kind = "narrator"
        p3.text = "أَحْمَدَ بْنِ مُحَمَّدٍ"
        p3.path = "/people/narrators/5"
        verse.narrator_chain.parts = [p1, p2, p3]

        result = extract_isnad_text(verse)

        assert result is not None
        # Reconstructed chain text concatenates parts
        assert "مُحَمَّدُ بْنُ يَحْيَى" in result
        assert "عَنْ" in result
        assert "أَحْمَدَ بْنِ مُحَمَّدٍ" in result
        # verse.text[0] is NOT re-modified (already truncated)
        assert verse.text[0] == "just the matn here"

    def test_idempotent_skipped_when_no_parts(self):
        """When narrator_chain has no parts AND text[0] has no chain,
        return None (don't fabricate a result)."""
        verse = Verse()
        verse.text = ["just the matn here"]
        verse.narrator_chain = NarratorChain()
        verse.narrator_chain.parts = []
        result = extract_isnad_text(verse)
        assert result is None


# ── split_narrator_names tests ─────────────────────────────────────────


class TestSplitNarratorNames:
    def test_basic_split(self):
        text = "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ"
        result = split_narrator_names(text)
        assert len(result) >= 2
        assert "مُحَمَّدُ بْنُ يَحْيَى" in result
        assert "أَحْمَدَ بْنِ مُحَمَّدٍ" in result

    def test_multiple_connectors(self):
        text = "عَلِيُّ بْنُ إِبْرَاهِيمَ عَنْ أَبِيهِ عَنِ ابْنِ أَبِي عُمَيْرٍ"
        result = split_narrator_names(text)
        assert len(result) == 3

    def test_single_narrator(self):
        text = "مُحَمَّدُ بْنُ يَحْيَى"
        result = split_narrator_names(text)
        assert len(result) == 1

    def test_prefix_stripped(self):
        text = "أَخْبَرَنَا أَبُو جَعْفَرٍ مُحَمَّدُ بْنُ يَعْقُوبَ"
        result = split_narrator_names(text)
        # The prefix "أَخْبَرَنَا" should be stripped
        assert len(result) >= 1


# ── resolve_narrators tests ────────────────────────────────────────────


class TestResolveNarrators:
    def test_resolve_known_narrators(self, registry):
        names = ["مُحَمَّدُ بْنُ يَحْيَى", "أَحْمَدَ بْنِ مُحَمَّدٍ"]
        resolved = resolve_narrators(names, registry)
        assert len(resolved) == 2
        assert resolved[0] == ("مُحَمَّدُ بْنُ يَحْيَى", 1)
        assert resolved[1] == ("أَحْمَدَ بْنِ مُحَمَّدٍ", 2)

    def test_resolve_unknown_narrator(self, registry):
        names = ["totally unknown narrator"]
        resolved = resolve_narrators(names, registry)
        assert len(resolved) == 1
        assert resolved[0][1] is None

    def test_resolve_mixed(self, registry):
        names = ["مُحَمَّدُ بْنُ يَحْيَى", "unknown_person"]
        resolved = resolve_narrators(names, registry)
        assert resolved[0][1] == 1
        assert resolved[1][1] is None

    def test_disambiguation_context(self, registry):
        """أَبِيهِ after Ali ibn Ibrahim should resolve to his father."""
        names = ["عَلِيُّ بْنُ إِبْرَاهِيمَ", "أَبِيهِ"]
        resolved = resolve_narrators(names, registry)
        assert resolved[0][1] == 3  # Ali ibn Ibrahim
        assert resolved[1][1] == 4  # his father (Ibrahim ibn Hashim)


# ── build_chain_parts tests ────────────────────────────────────────────


class TestBuildChainParts:
    def test_basic_parts(self):
        resolved = [("narrator1", 1), ("narrator2", 2)]
        parts = build_chain_parts("narrator1 عَنْ narrator2 end", resolved)

        narrator_parts = [p for p in parts if p.kind == "narrator"]
        assert len(narrator_parts) == 2
        assert narrator_parts[0].path == "/people/narrators/1"
        assert narrator_parts[1].path == "/people/narrators/2"

    def test_unlinked_narrator(self):
        resolved = [("narrator1", 1), ("unknown", None)]
        parts = build_chain_parts("narrator1 عَنْ unknown end", resolved)

        narrator_parts = [p for p in parts if p.kind == "narrator"]
        plain_parts = [p for p in parts if p.kind == "plain"]
        assert len(narrator_parts) == 1  # Only the known one
        # The unknown one should be plain text
        assert any("unknown" in p.text for p in plain_parts)

    def test_connector_text_preserved(self):
        resolved = [("أ", 1), ("ب", 2)]
        parts = build_chain_parts("أ عَنْ ب end", resolved)

        plain_parts = [p for p in parts if p.kind == "plain"]
        assert any(" عَنْ " in p.text for p in plain_parts)

    def test_trailing_text(self):
        resolved = [("name", 1)]
        parts = build_chain_parts("name trailing text", resolved)
        assert parts[-1].kind == "plain"
        assert "trailing" in parts[-1].text


# ── link_verse_narrators end-to-end tests ──────────────────────────────


class TestLinkVerseNarrators:
    def test_full_chain(self, registry):
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ test text"
        )
        canonical_ids = link_verse_narrators(verse, registry)

        assert 1 in canonical_ids  # Muhammad ibn Yahya
        assert 2 in canonical_ids  # Ahmad ibn Muhammad

        # Narrator chain should have parts
        assert verse.narrator_chain is not None
        assert verse.narrator_chain.parts is not None

        narrator_parts = [p for p in verse.narrator_chain.parts if p.kind == "narrator"]
        assert len(narrator_parts) == 2

        # Chain text should be cleared (optimization)
        assert verse.narrator_chain.text is None

    def test_no_chain(self, registry):
        verse = _make_verse("plain text without narrator patterns")
        canonical_ids = link_verse_narrators(verse, registry)
        assert canonical_ids == []

    def test_empty_verse(self, registry):
        verse = Verse()
        verse.text = []
        verse.path = "/test"
        canonical_ids = link_verse_narrators(verse, registry)
        assert canonical_ids == []

    def test_partial_resolution(self, registry):
        """Some narrators resolve, some don't."""
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ unknown_person قَالَ test"
        )
        canonical_ids = link_verse_narrators(verse, registry)
        # Only the known narrator should be in the list
        assert 1 in canonical_ids
        # Unknown narrator gets skipped (None filtered out)
        assert len(canonical_ids) == 1


class TestLooksLikeIsnad:
    """The Class 2 pre-filter: detect non-isnad verses before trying to
    extract a chain from them.

    Permissive — any one chain signal in the leading window passes.
    """

    def test_real_kafi_chain_passes(self):
        line = "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ matn"
        assert _looks_like_isnad(line)

    def test_saduq_father_chain_passes(self):
        # Saduq's "my father (peace upon him) said: X told me ..." style.
        line = "أَبِي رَحِمَهُ اَللَّهُ قَالَ حَدَّثَنِي مُحَمَّدُ بْنُ يَحْيَى"
        assert _looks_like_isnad(line)

    def test_haddathana_alone_passes(self):
        line = "حَدَّثَنَا أَحْمَدُ بْنُ مُحَمَّدٍ matn"
        assert _looks_like_isnad(line)

    def test_faqih_narrative_quote_rejected(self):
        # Real Faqih opener — narrative, no chain.
        line = "مَا كَانَ فِي اَلْكِتَابِ مِنْ ذِكْرِ اَلصَّلاَةِ"
        assert not _looks_like_isnad(line)

    def test_back_reference_rejected(self):
        # "And with this isnad he said..." — back-reference to prior chain,
        # has no chain of its own.
        line = "وَبِهَذَا اَلْإِسْنَادِ قَالَ كَذَا وَكَذَا"
        assert not _looks_like_isnad(line)

    def test_prophet_quote_rejected(self):
        # "And the Messenger of Allah said..." — direct attribution, no chain.
        line = "وَقَالَ رَسُولُ اَللَّهِ صَلَّى اَللَّهُ عَلَيْهِ وَآلِهِ"
        assert not _looks_like_isnad(line)

    def test_empty_rejected(self):
        assert not _looks_like_isnad("")

    def test_undiacritized_chain_passes_via_fallback(self):
        # Diacritized signal patterns alone wouldn't match plain "حدثني" —
        # the strip_tashkeel fallback covers this.
        line = "حدثني محمد بن الحسن قال matn"
        assert _looks_like_isnad(line)


class TestStripBookPreamble:
    """Per-book leading-preamble strip used to peel meta-prose wrappers
    before chain extraction (Tahdhib's "the Sheikh, may Allah strengthen
    him, told me..." formula being the canonical example)."""

    def test_tahdhib_full_preamble(self):
        line = "مَا أَخْبَرَنِي بِهِ اَلشَّيْخُ أَيَّدَهُ اَللَّهُ تَعَالَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ matn"
        peeled = _strip_book_preamble(line, "tahdhib-al-ahkam")
        assert peeled.startswith("عَنْ"), peeled
        assert len(peeled) < len(line)

    def test_tahdhib_short_preamble(self):
        line = "أَخْبَرَنِي بِهِ اَلشَّيْخُ أَيَّدَهُ اَللَّهُ تَعَالَى عَنْ مُحَمَّدِ بْنِ يَحْيَى قَالَ matn"
        peeled = _strip_book_preamble(line, "tahdhib-al-ahkam")
        assert peeled.startswith("عَنْ"), peeled

    def test_tahdhib_standalone_sheikh_ayyadahu(self):
        """The bare "الشيخ أيده الله تعالى" (no "ما أخبرني به" prefix) must
        also be peeled — without this, it slips through to the splitter
        and the resolver mis-links it to a different "الشيخ" registry
        entry (the al-Kafi Imam reference)."""
        line = "اَلشَّيْخُ أَيَّدَهُ اَللَّهُ تَعَالَى عَنْ مُحَمَّدِ بْنِ يَحْيَى قَالَ matn"
        peeled = _strip_book_preamble(line, "tahdhib-al-ahkam")
        assert peeled.startswith("عَنْ"), peeled

    def test_istibsar_standalone_sheikh_ayyadahu(self):
        """al-Istibsar uses the same Tusi formula as Tahdhib."""
        line = "اَلشَّيْخُ أَيَّدَهُ اَللَّهُ تَعَالَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ قَالَ matn"
        peeled = _strip_book_preamble(line, "al-istibsar")
        assert peeled.startswith("عَنْ"), peeled

    def test_istibsar_meta_isnad(self):
        line = "فَأَمَّا مَا رَوَاهُ اَلْحُسَيْنُ بْنُ سَعِيدٍ عَنْ matn"
        peeled = _strip_book_preamble(line, "al-istibsar")
        assert peeled.startswith("اَلْحُسَيْنُ") or peeled.startswith("الْحُسَيْنُ"), peeled

    def test_unknown_book_passthrough(self):
        line = "anything goes here"
        assert _strip_book_preamble(line, "some-unknown-book") == line

    def test_no_preamble_match_passthrough(self):
        line = "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ قَالَ"
        assert _strip_book_preamble(line, "tahdhib-al-ahkam") == line

    def test_none_book_slug_passthrough(self):
        line = "مَا أَخْبَرَنِي بِهِ اَلشَّيْخُ"
        assert _strip_book_preamble(line, None) == line


class TestBookSlugFromPath:
    def test_basic_path(self):
        assert _book_slug_from_path("/books/tahdhib-al-ahkam:1:2:3") == "tahdhib-al-ahkam"

    def test_kafi_path(self):
        assert _book_slug_from_path("/books/al-kafi:1:2:3:4") == "al-kafi"

    def test_path_with_no_colon(self):
        assert _book_slug_from_path("/books/quran") == "quran"

    def test_none_path(self):
        assert _book_slug_from_path(None) is None

    def test_bare_path(self):
        assert _book_slug_from_path("/") is None
        assert _book_slug_from_path("") is None
        assert _book_slug_from_path("/no-books-prefix:1:2") is None


class TestExtractIsnadGuards:
    """End-to-end checks that the new guards in extract_isnad_text fire."""

    def test_narrative_verse_returns_none(self):
        verse = _make_verse("مَا كَانَ فِي اَلْكِتَابِ مِنْ ذِكْرِ اَلصَّلاَةِ")
        assert extract_isnad_text(verse) is None
        # verse.text[0] preserved (no chain extracted)
        assert verse.text[0].startswith("مَا كَانَ")

    def test_back_reference_returns_none(self):
        verse = _make_verse("وَبِهَذَا اَلْإِسْنَادِ قَالَ كَذَا")
        assert extract_isnad_text(verse) is None

    def test_tahdhib_preamble_peeled_then_chain_extracted(self):
        verse = Verse()
        verse.path = "/books/tahdhib-al-ahkam:1:2:3"
        verse.text = [
            "مَا أَخْبَرَنِي بِهِ اَلشَّيْخُ أَيَّدَهُ اَللَّهُ تَعَالَى عَنْ مُحَمَّدِ بْنِ يَحْيَى قَالَ matn-text"
        ]
        chain_text = extract_isnad_text(verse)
        assert chain_text is not None
        # Preamble peeled — no longer in chain text
        assert "أَخْبَرَنِي بِهِ" not in chain_text
        # Chain text should include the actual narrator
        assert "مُحَمَّدِ بْنِ يَحْيَى" in chain_text

    def test_idempotent_path_skips_guards(self):
        """Re-running extract on a verse whose chain is already in
        narrator_chain.parts must reconstruct the chain text without
        being rejected by _looks_like_isnad (which would see a matn,
        not an isnad, in verse.text[0])."""
        verse = Verse()
        verse.path = "/books/al-kafi:1:1:1:1"
        verse.text = ["just-the-matn"]  # chain already moved out
        verse.narrator_chain = NarratorChain()
        p = SpecialText()
        p.kind = "narrator"
        p.text = "مُحَمَّدُ بْنُ يَحْيَى"
        verse.narrator_chain.parts = [p]
        result = extract_isnad_text(verse)
        assert result == "مُحَمَّدُ بْنُ يَحْيَى"


class TestEndingPhraseLongestMatch:
    """The chain-end regex must prefer the longer alternative when one is a
    prefix of another, otherwise the trailing characters leak into the last
    extracted narrator name.

    Regression: "أَنَّهُ" used to match as "أَنَّ" + leftover "هُ", baking
    the trailing هُ into the previous narrator.
    """

    def test_anna_hu_consumed_whole(self):
        """For text ending in "...أَنَّهُ ...", extraction should stop at
        the full "أَنَّهُ" boundary, not at "أَنَّ"."""
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ أَنَّهُ matn-text-here"
        )
        chain_text = extract_isnad_text(verse)
        assert chain_text is not None
        # The "هُ" must NOT remain as a leading character of the matn.
        assert not verse.text[0].lstrip().startswith("هُ"), (
            f"verse.text[0] starts with leftover 'هُ': {verse.text[0]!r}"
        )

    def test_fi_qawlihi_consumed_before_fi(self):
        """Sanity check that the existing "فِي قَوْلِهِ" before "فِي"
        ordering still wins (we reordered the same group)."""
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ فِي قَوْلِهِ tafsir-text"
        )
        chain_text = extract_isnad_text(verse)
        assert chain_text is not None
        # Matn should start with "tafsir", not with leftover Arabic from
        # a too-early "فِي" stop.
        assert verse.text[0].lstrip().startswith("tafsir"), verse.text[0]

    def test_anna_still_terminates_chain(self):
        """The shorter "أَنَّ" alternative still works when no هُ follows."""
        verse = _make_verse(
            "مُحَمَّدُ بْنُ يَحْيَى عَنْ أَحْمَدَ بْنِ مُحَمَّدٍ أَنَّ matn-text"
        )
        chain_text = extract_isnad_text(verse)
        assert chain_text is not None
        assert "أَنَّ" in chain_text
