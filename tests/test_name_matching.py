"""Tests for the 5-step name matching pipeline."""

import json
import os
import pytest

from app.wikishia.name_matching import (
    MatchResult,
    NameMatcher,
    levenshtein_distance,
    similarity_ratio,
)


class TestLevenshteinDistance:
    """Test Levenshtein edit distance computation."""

    def test_identical_strings(self):
        assert levenshtein_distance("abc", "abc") == 0

    def test_empty_strings(self):
        assert levenshtein_distance("", "") == 0

    def test_one_empty(self):
        assert levenshtein_distance("abc", "") == 3
        assert levenshtein_distance("", "abc") == 3

    def test_single_insertion(self):
        assert levenshtein_distance("abc", "abcd") == 1

    def test_single_deletion(self):
        assert levenshtein_distance("abcd", "abc") == 1

    def test_single_substitution(self):
        assert levenshtein_distance("abc", "axc") == 1

    def test_complex_edit(self):
        assert levenshtein_distance("kitten", "sitting") == 3

    def test_arabic_strings(self):
        """Test with Arabic text."""
        # Same word without/with one letter difference
        s1 = "محمد"
        s2 = "محمود"
        distance = levenshtein_distance(s1, s2)
        assert distance > 0

    def test_symmetric(self):
        """Distance is symmetric: d(a,b) == d(b,a)."""
        assert levenshtein_distance("abc", "xyz") == levenshtein_distance("xyz", "abc")


class TestSimilarityRatio:
    """Test similarity ratio computation."""

    def test_identical(self):
        assert similarity_ratio("abc", "abc") == 1.0

    def test_completely_different(self):
        assert similarity_ratio("abc", "xyz") == pytest.approx(0.0, abs=0.01)

    def test_partial_match(self):
        ratio = similarity_ratio("abcd", "abce")
        assert 0.5 < ratio < 1.0

    def test_empty_strings(self):
        assert similarity_ratio("", "") == 1.0

    def test_one_empty(self):
        assert similarity_ratio("abc", "") == 0.0
        assert similarity_ratio("", "abc") == 0.0

    def test_arabic_similar_names(self):
        """Similar Arabic names should have high similarity."""
        s1 = "محمد بن يحيى"
        s2 = "محمد بن يحيي"  # Slightly different ya form
        ratio = similarity_ratio(s1, s2)
        assert ratio > 0.8


class TestMatchResult:
    """Test MatchResult serialization."""

    def test_to_dict_unmatched(self):
        result = MatchResult(1, "test name")
        d = result.to_dict()
        assert d["narrator_id"] == 1
        assert d["narrator_name"] == "test name"
        assert "matched_title" not in d

    def test_to_dict_matched(self):
        result = MatchResult(1, "test name")
        result.matched_title = "WikiShia Title"
        result.match_step = "exact"
        result.confidence = 1.0
        d = result.to_dict()
        assert d["matched_title"] == "WikiShia Title"
        assert d["match_step"] == "exact"
        assert d["confidence"] == 1.0

    def test_to_dict_with_candidates(self):
        result = MatchResult(1, "test")
        result.candidates = [("Title A", 0.9), ("Title B", 0.8)]
        d = result.to_dict()
        assert len(d["candidates"]) == 2
        assert d["candidates"][0]["title"] == "Title A"


class TestNameMatcherStep1ExactMatch:
    """Test Step 1: Exact match."""

    def test_exact_match_found(self):
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "محمد بن يعقوب"})
        matcher.load_wikishia_titles(["محمد بن يعقوب", "Other Title"])

        result = matcher.match_narrator(1, "محمد بن يعقوب")
        assert result.matched_title == "محمد بن يعقوب"
        assert result.match_step == "exact"
        assert result.confidence == 1.0

    def test_exact_match_not_found(self):
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "محمد بن يعقوب"})
        matcher.load_wikishia_titles(["Other Title"])

        result = matcher._step1_exact_match(1, "محمد بن يعقوب")
        assert result is None


class TestNameMatcherStep2NormalizedMatch:
    """Test Step 2: Normalized match."""

    def test_normalized_match_diacritics(self):
        """Names differing only in diacritics should match."""
        matcher = NameMatcher()
        # Name with diacritics in our system
        matcher.load_narrator_names({1: "مُحَمَّدُ بْنُ يَحْيَى"})
        # WikiShia title without diacritics
        matcher.load_wikishia_titles(["محمد بن يحيى"])

        result = matcher._step2_normalized_match(1, "مُحَمَّدُ بْنُ يَحْيَى")
        assert result is not None
        assert result.match_step == "normalized"
        assert result.confidence == 0.95

    def test_normalized_match_alef_variants(self):
        """Alef variants should not prevent matching."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "\u0623\u062D\u0645\u062F"})  # with hamza-alef
        matcher.load_wikishia_titles(["\u0627\u062D\u0645\u062F"])    # plain alef

        result = matcher._step2_normalized_match(1, "\u0623\u062D\u0645\u062F")
        assert result is not None

    def test_normalized_no_match(self):
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "محمد"})
        matcher.load_wikishia_titles(["علي"])

        result = matcher._step2_normalized_match(1, "محمد")
        assert result is None


class TestNameMatcherStep3FuzzyMatch:
    """Test Step 3: Fuzzy match."""

    def test_fuzzy_match_close_names(self):
        """Names with small differences should fuzzy-match."""
        matcher = NameMatcher(fuzzy_threshold=0.7)
        matcher.load_narrator_names({1: "محمد بن يحيا"})
        matcher.load_wikishia_titles(["محمد بن يحيى"])

        result = matcher._step3_fuzzy_match(1, "محمد بن يحيا")
        assert result is not None
        assert result.match_step == "fuzzy"
        assert result.confidence >= 0.7

    def test_fuzzy_match_too_different(self):
        """Very different names should not fuzzy-match."""
        matcher = NameMatcher(fuzzy_threshold=0.8)
        matcher.load_narrator_names({1: "محمد"})
        matcher.load_wikishia_titles(["ابراهيم بن سعيد"])

        result = matcher._step3_fuzzy_match(1, "محمد")
        assert result is None

    def test_fuzzy_match_returns_candidates(self):
        """Fuzzy match should return ranked candidates."""
        matcher = NameMatcher(fuzzy_threshold=0.5)
        matcher.load_narrator_names({1: "محمد بن علي"})
        matcher.load_wikishia_titles([
            "محمد بن علي الباقر",
            "محمد بن علي الرضا",
            "عيسى بن موسى",
        ])

        result = matcher._step3_fuzzy_match(1, "محمد بن علي")
        assert result is not None
        # Should have candidates ranked by score
        assert len(result.candidates) >= 1
        # Best candidate should have highest score
        scores = [s for _, s in result.candidates]
        assert scores == sorted(scores, reverse=True)


class TestNameMatcherStep4ManualMapping:
    """Test Step 4: Manual mapping."""

    def test_manual_mapping_found(self):
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "أَبِي جَعْفَرٍ ( عليه السلام )"})
        matcher.load_manual_mapping({
            "أَبِي جَعْفَرٍ ( عليه السلام )": "Imam al-Baqir"
        })

        result = matcher._step4_manual_mapping(
            1, "أَبِي جَعْفَرٍ ( عليه السلام )"
        )
        assert result is not None
        assert result.matched_title == "Imam al-Baqir"
        assert result.match_step == "manual"

    def test_manual_mapping_normalized_key(self):
        """Manual mapping should work even if key has different diacritics."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "مُحَمَّدٌ"})
        matcher.load_manual_mapping({"محمد": "Muhammad"})

        result = matcher._step4_manual_mapping(1, "مُحَمَّدٌ")
        assert result is not None
        assert result.matched_title == "Muhammad"

    def test_manual_mapping_not_found(self):
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "unknown name"})
        matcher.load_manual_mapping({})

        result = matcher._step4_manual_mapping(1, "unknown name")
        assert result is None

    def test_load_manual_mapping_file(self, tmp_path):
        """Test loading manual mapping from a JSON file."""
        mapping = {"key1": "value1", "key2": "value2"}
        filepath = tmp_path / "mappings.json"
        filepath.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")

        matcher = NameMatcher()
        matcher.load_manual_mapping_file(str(filepath))
        assert matcher.manual_mapping == mapping

    def test_load_nonexistent_file(self):
        """Loading from nonexistent file should not crash."""
        matcher = NameMatcher()
        matcher.load_manual_mapping_file("/nonexistent/path.json")
        assert matcher.manual_mapping == {}


class TestNameMatcherStep5AI:
    """Test Step 5: AI-assisted matching."""

    def test_ai_placeholder_returns_none(self):
        """AI step placeholder returns None (unimplemented)."""
        matcher = NameMatcher()
        result = matcher._step5_ai_assisted(1, "test name")
        assert result is None


class TestNameMatcherFullPipeline:
    """Test the full 5-step pipeline."""

    def test_pipeline_prioritizes_exact(self):
        """Exact match is returned even when other steps would also match."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "محمد بن يعقوب"})
        matcher.load_wikishia_titles(["محمد بن يعقوب"])
        matcher.load_manual_mapping({"محمد بن يعقوب": "Different Title"})

        result = matcher.match_narrator(1, "محمد بن يعقوب")
        assert result.match_step == "exact"

    def test_pipeline_falls_through_to_normalized(self):
        """When exact match fails, normalized match is used."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "مُحَمَّدُ"})
        matcher.load_wikishia_titles(["محمد"])  # No diacritics

        result = matcher.match_narrator(1, "مُحَمَّدُ")
        assert result.match_step == "normalized"

    def test_pipeline_manual_before_fuzzy(self):
        """Manual mapping takes precedence over fuzzy matching."""
        matcher = NameMatcher(fuzzy_threshold=0.5)
        matcher.load_narrator_names({1: "أبي جعفر"})
        matcher.load_wikishia_titles(["ابي جعفر الباقر"])
        matcher.load_manual_mapping({"أبي جعفر": "Imam al-Baqir"})

        result = matcher.match_narrator(1, "أبي جعفر")
        assert result.match_step == "manual"

    def test_pipeline_unmatched(self):
        """Completely unmatched narrators return empty MatchResult."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "some unique name"})
        matcher.load_wikishia_titles(["completely different"])

        result = matcher.match_narrator(1, "some unique name")
        assert result.matched_title is None
        assert result.match_step is None

    def test_run_pipeline_processes_all(self):
        """run_pipeline() processes all narrators."""
        matcher = NameMatcher()
        matcher.load_narrator_names({
            1: "محمد بن يعقوب",
            2: "مُحَمَّدُ بْنُ يَحْيَى",
            3: "totally unknown",
        })
        matcher.load_wikishia_titles(["محمد بن يعقوب", "محمد بن يحيى"])

        results = matcher.run_pipeline()
        assert len(results) == 3
        assert results[1].matched_title is not None  # Exact match
        assert results[2].matched_title is not None  # Normalized match
        assert results[3].matched_title is None      # No match

    def test_get_unmatched(self):
        """get_unmatched returns only unmatched results."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "matched", 2: "unmatched"})
        matcher.load_wikishia_titles(["matched"])

        results = matcher.run_pipeline()
        unmatched = matcher.get_unmatched(results)
        assert len(unmatched) == 1
        assert unmatched[0].narrator_id == 2

    def test_export_results(self, tmp_path):
        """export_results writes valid JSON."""
        matcher = NameMatcher()
        matcher.load_narrator_names({1: "محمد بن يعقوب", 2: "شخص مجهول تماما"})
        matcher.load_wikishia_titles(["محمد بن يعقوب"])

        results = matcher.run_pipeline()
        output_path = str(tmp_path / "results.json")
        matcher.export_results(results, output_path)

        assert os.path.exists(output_path)
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["total_narrators"] == 2
        assert data["matched"] == 1
        assert data["unmatched"] == 1
        assert "1" in data["results"]
        assert "2" in data["results"]
