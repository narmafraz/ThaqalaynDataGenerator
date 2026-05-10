"""Tests for app.words.normalize.

The fixture `fixtures/surface_forms.json` is the contract locked between
Python and the TypeScript twin. Each entry's `input` field is the raw
surface form (as it appears in chunk arabic_text); `nfc` is the expected
output of slug(). The fixture is replayed both here (Python) and in the
Angular Karma test for `word-normalize.ts` (when that lands in Phase 8).
"""
from __future__ import annotations

import json
import os
import unicodedata
from pathlib import Path

import pytest

from app.words.normalize import normalize_for_match, slug

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "surface_forms.json"


def _load_fixture():
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


class TestSlug:
    def test_empty_string_returns_empty(self):
        assert slug("") == ""

    def test_none_input_returns_empty(self):
        assert slug(None) == ""  # type: ignore[arg-type]

    def test_strips_whitespace(self):
        assert slug("  قَالَ  ") == "قَالَ"

    def test_preserves_diacritics(self):
        """slug() must NOT strip tashkeel — that would collapse
        inflectional distinctions we want to keep distinct."""
        assert "َ" in slug("قَالَ")
        assert slug("قَالَ") != slug("قَالُ")

    def test_idempotent(self):
        """slug(slug(x)) == slug(x) for any x."""
        for sample in ["قَالَ", "وَبِالْعَهْدِ", "أَبُو"]:
            once = slug(sample)
            twice = slug(once)
            assert once == twice, f"Not idempotent: {sample!r}"

    def test_homographs_with_different_diacritization_are_distinct(self):
        """Different inflections of the same rasm → different slugs."""
        a = slug("قَدَرَ")  # "he was able"
        b = slug("قَدَّرَ")  # "he estimated" (shadda)
        assert a != b

    def test_nfc_normalization_lam_alif_ligature(self):
        """The lam-alif ligature codepoint vs ل + ا sequence should
        normalize to the same NFC form. Pick whichever is canonical."""
        ligature = "ﻻ"  # U+FEFB ARABIC LIGATURE LAM WITH ALEF ISOLATED FORM
        sequence = "لا"  # U+0644 + U+0627
        # NFC normalization may or may not decompose this; the test
        # asserts that whatever slug returns is itself NFC-normalized
        # (idempotent under normalization).
        for s in (ligature, sequence):
            result = slug(s)
            assert result == unicodedata.normalize("NFC", result), (
                f"slug output {result!r} is not NFC-normalized"
            )

    # ------------------------------------------------------------------
    # Fixture replay — the Python side of the cross-language contract
    # ------------------------------------------------------------------

    def test_fixture_loads(self):
        fixture = _load_fixture()
        assert len(fixture) >= 100, f"Fixture too small: {len(fixture)}"
        assert all("input" in e and "nfc" in e for e in fixture)

    def test_fixture_replay_all_entries(self):
        """Every fixture entry: slug(input) == expected nfc.

        This is the same fixture the TypeScript twin will replay (with
        s.normalize('NFC')). Both languages must produce the same
        output for every entry. Failure here would be a Python
        regression in the normalization contract.
        """
        fixture = _load_fixture()
        failures = []
        for entry in fixture:
            actual = slug(entry["input"])
            if actual != entry["nfc"]:
                failures.append((entry["input"], entry["nfc"], actual))
        assert not failures, (
            f"{len(failures)} fixture entries failed (first 3): "
            f"{failures[:3]}"
        )

    def test_fixture_round_trip_idempotent(self):
        """slug(slug(input)) == slug(input) for every fixture entry."""
        fixture = _load_fixture()
        for entry in fixture:
            once = slug(entry["input"])
            twice = slug(once)
            assert once == twice, f"Not idempotent on fixture: {entry['input']!r}"


class TestNormalizeForMatch:
    def test_strips_diacritics(self):
        assert normalize_for_match("قَالَ") == "قال"

    def test_unifies_alif_variants(self):
        assert normalize_for_match("أَحْمَدُ") == "احمد"
        assert normalize_for_match("إِبْرَاهِيمُ") == "ابراهيم"
        assert normalize_for_match("آيَةٌ") == "ايه"

    def test_unifies_alif_wasla(self):
        """Alif-wasla (ٱ) -> regular alif (ا) — common in classical text
        before sun letters / definite article concatenation."""
        assert normalize_for_match("ٱلْحَسَنُ") == "الحسن"

    def test_unifies_ya_variants(self):
        """Alif-maksura (ى) -> ya (ي)."""
        assert normalize_for_match("هُدًى") == "هدي"

    def test_strips_tatweel(self):
        assert normalize_for_match("صَــلَاة") == "صلاه"

    def test_empty_returns_empty(self):
        assert normalize_for_match("") == ""
        assert normalize_for_match(None) == ""  # type: ignore[arg-type]

    def test_idempotent(self):
        for sample in ["قَالَ", "ٱلْحَسَنُ", "أَبُو"]:
            once = normalize_for_match(sample)
            twice = normalize_for_match(once)
            assert once == twice

    def test_different_from_slug(self):
        """normalize_for_match is lossy (drops diacritics); slug is not.
        Confirms they're intentionally different functions for different
        purposes."""
        assert slug("قَالَ") != normalize_for_match("قَالَ")
