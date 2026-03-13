"""Tests for the generic narrator linker module."""

import json
import os
import tempfile

import pytest

from app.models.quran import NarratorChain, SpecialText, Verse
from app.narrator_linker import (
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
