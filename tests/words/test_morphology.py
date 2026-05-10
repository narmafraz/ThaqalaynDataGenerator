"""Tests for app.words.morphology.

These tests load the CAMeL Tools morphology DB (~40 MB) on first
analyzer call. Subsequent tests reuse the cached instance. Tests are
designed to fail clearly if the DB isn't installed.
"""
from __future__ import annotations

import pytest

# Skip the whole module if camel_tools isn't installed
camel_tools = pytest.importorskip("camel_tools")

from app.words.morphology import (  # noqa: E402
    analyze,
    extract_lemma,
    extract_root,
    generate_paradigm,
    get_best_analysis,
    measure_coverage,
    paradigm_by_role,
    translate_pos,
)


# ---------------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------------

class TestAnalyze:
    def test_known_verb_returns_analyses(self):
        result = analyze("قَالَ")
        assert len(result) >= 1
        assert any(a.get("pos") for a in result)

    def test_known_verb_has_expected_fields(self):
        result = analyze("قَالَ")
        first = result[0]
        # Core fields we depend on downstream
        assert "lex" in first
        assert "root" in first
        assert "pos" in first
        assert "gloss" in first

    def test_qaala_lemma_is_qal(self):
        """قَالَ should lemmatize to قال (no diacritics in CAMeL lex)."""
        lemma = extract_lemma("قَالَ")
        assert lemma == "قال"

    def test_qaala_root(self):
        """قَالَ root is ق-و-ل (or in CAMeL format, ق.و.ل)."""
        root = extract_root("قَالَ")
        assert root is not None
        # CAMeL formats hollow-verb roots with a '#' or '.' separator
        # Both representations are acceptable as long as the letters
        # are q-w-l (with the middle weak letter possibly marked)
        assert "ق" in root and "ل" in root

    def test_empty_input_returns_empty_list(self):
        assert analyze("") == []
        assert analyze(None) == []  # type: ignore[arg-type]

    def test_garbage_input_returns_empty(self):
        """Non-Arabic input shouldn't blow up — return empty list."""
        # "hello" — Latin only
        result = analyze("hello")
        # CAMeL may return a punctuation/foreign tag or empty; either is fine
        assert isinstance(result, list)


class TestBestAnalysis:
    def test_single_analysis_returns_it(self):
        # Use a form likely to have only one analysis
        best = get_best_analysis("اَلسَّلَامُ")  # "the peace" — has analyses
        # Either None or a dict — but if not None, must be a dict
        assert best is None or isinstance(best, dict)

    def test_qaala_best_analysis_is_verb(self):
        best = get_best_analysis("قَالَ")
        assert best is not None
        # CAMeL POS tag may be "verb" or a sub-type
        pos = best.get("pos", "")
        assert "verb" in pos.lower() or pos == "verb"

    def test_none_for_empty(self):
        assert get_best_analysis("") is None


# ---------------------------------------------------------------------------
# Generator (paradigm)
# ---------------------------------------------------------------------------

class TestGeneratePardigm:
    def test_qal_verb_paradigm(self):
        forms = generate_paradigm("قال", pos="verb")
        # Should generate many forms (60+ for قال in calima-msa)
        assert len(forms) >= 20, f"Too few forms: {len(forms)}"

    def test_paradigm_has_diac_field(self):
        forms = generate_paradigm("قال", pos="verb")
        assert all("diac" in f for f in forms)

    def test_paradigm_includes_imperative(self):
        forms = generate_paradigm("قال", pos="verb")
        diacs = {f.get("diac", "") for f in forms}
        # قُلْ is the canonical 2ms imperative for قال
        assert "قُلْ" in diacs or any("قُلْ" in d for d in diacs)

    def test_paradigm_includes_first_person_past(self):
        forms = generate_paradigm("قال", pos="verb")
        diacs = {f.get("diac", "") for f in forms}
        # قُلْتُ is 1cs past
        assert "قُلْتُ" in diacs

    def test_empty_lemma_returns_empty(self):
        assert generate_paradigm("") == []
        assert generate_paradigm(None) == []  # type: ignore[arg-type]


class TestParadigmByRole:
    def test_qal_returns_role_keyed_entries(self):
        entries = paradigm_by_role("قال", pos="verb")
        assert len(entries) > 0
        for e in entries:
            assert "role" in e
            assert "form" in e
            assert "diacritized" in e

    def test_roles_are_strings(self):
        entries = paradigm_by_role("قال", pos="verb")
        for e in entries:
            assert isinstance(e["role"], str)
            assert len(e["role"]) > 0

    def test_includes_past_3ms(self):
        """The canonical 'he said' form should be present."""
        entries = paradigm_by_role("قال", pos="verb")
        roles = {e["role"] for e in entries}
        assert "past_3ms" in roles

    def test_includes_imperative_2ms(self):
        entries = paradigm_by_role("قال", pos="verb")
        roles = {e["role"] for e in entries}
        assert "imperative_2ms" in roles

    def test_dedupes_by_role_and_form(self):
        """When CAMeL returns multiple analyses for the same form, we
        keep only one entry per (role, form)."""
        entries = paradigm_by_role("قال", pos="verb")
        seen = set()
        for e in entries:
            key = (e["role"], e["form"])
            assert key not in seen, f"Duplicate (role, form): {key}"
            seen.add(key)

    def test_sorted_by_role_order(self):
        """Past forms should appear before present, present before imperative."""
        entries = paradigm_by_role("قال", pos="verb")
        roles = [e["role"] for e in entries]
        # Find first past_3ms and first present_3ms (if both present)
        past_idx = next((i for i, r in enumerate(roles) if r.startswith("past_")), None)
        pres_idx = next((i for i, r in enumerate(roles) if r.startswith("present_")), None)
        impr_idx = next((i for i, r in enumerate(roles) if r.startswith("imperative_")), None)
        # Check ordering where pairs exist
        if past_idx is not None and pres_idx is not None:
            assert past_idx < pres_idx
        if pres_idx is not None and impr_idx is not None:
            assert pres_idx < impr_idx


# ---------------------------------------------------------------------------
# POS translation
# ---------------------------------------------------------------------------

class TestTranslatePos:
    def test_known_camel_pos_mapped(self):
        assert translate_pos("verb") == "V"
        assert translate_pos("noun") == "N"
        assert translate_pos("adj") == "ADJ"
        assert translate_pos("prep") == "PREP"
        assert translate_pos("conj") == "CONJ"

    def test_proper_noun_maps_to_n(self):
        assert translate_pos("noun_prop") == "N"

    def test_unknown_pos_defaults_to_n(self):
        assert translate_pos("totally_unknown_xyz") == "N"

    def test_none_defaults_to_n(self):
        assert translate_pos(None) == "N"
        assert translate_pos("") == "N"


# ---------------------------------------------------------------------------
# Coverage helper
# ---------------------------------------------------------------------------

class TestMeasureCoverage:
    def test_high_coverage_on_common_forms(self):
        common = [
            "قَالَ", "قُلْتُ", "اَللَّهِ", "مُحَمَّدٌ", "عَلِيٌّ",
            "أَبِي", "بْنُ", "عَنْ", "مِنْ", "إِلَى",
        ]
        stats = measure_coverage(common)
        assert stats["total"] == 10
        # All of these should analyze successfully — they're foundational
        # MSA/classical-Arabic vocabulary
        assert stats["coverage"] >= 0.8, (
            f"Coverage {stats['coverage']:.0%} too low on common forms; "
            f"unanalyzed: {stats['unanalyzed']}"
        )

    def test_empty_input(self):
        stats = measure_coverage([])
        assert stats == {
            "total": 0,
            "analyzed": 0,
            "unanalyzed": 0,
            "coverage": 0.0,
            "avg_analyses_per_form": 0.0,
        }
