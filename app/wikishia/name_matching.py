"""5-step name matching pipeline for linking narrator names to WikiShia articles.

Pipeline steps:
1. Exact match - Arabic names in our system vs WikiShia article titles
2. Normalized match - Strip diacritics, normalize letter variants
3. Fuzzy match - Levenshtein distance with configurable threshold
4. Manual mapping - JSON file for known exceptions
5. AI-assisted - For remaining ambiguous cases (placeholder)

Usage:
    from app.wikishia.name_matching import NameMatcher

    matcher = NameMatcher()
    matcher.load_narrator_index(narrator_index_data)
    matcher.load_wikishia_titles(wikishia_titles)
    results = matcher.run_pipeline()
"""

import json
import logging
import os
from typing import Dict, List, Optional, Set, Tuple

from app.wikishia.arabic_normalize import normalize_arabic, normalize_for_matching

logger = logging.getLogger(__name__)

# Default threshold for fuzzy matching (0.0 = no match, 1.0 = exact)
DEFAULT_FUZZY_THRESHOLD = 0.80


class MatchResult:
    """Result of matching a narrator name to a WikiShia article."""

    def __init__(self, narrator_id: int, narrator_name: str):
        self.narrator_id: int = narrator_id
        self.narrator_name: str = narrator_name
        self.matched_title: Optional[str] = None
        self.match_step: Optional[str] = None  # "exact", "normalized", "fuzzy", "manual", "ai"
        self.confidence: float = 0.0
        self.candidates: List[Tuple[str, float]] = []  # (title, score) pairs for review

    def to_dict(self) -> dict:
        result = {
            "narrator_id": self.narrator_id,
            "narrator_name": self.narrator_name,
        }
        if self.matched_title:
            result["matched_title"] = self.matched_title
            result["match_step"] = self.match_step
            result["confidence"] = self.confidence
        if self.candidates:
            result["candidates"] = [
                {"title": t, "score": s} for t, s in self.candidates
            ]
        return result


def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute the Levenshtein edit distance between two strings.

    Uses the Wagner-Fischer dynamic programming algorithm.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Integer edit distance.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))

    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            cost = 0 if c1 == c2 else 1
            curr_row.append(min(
                curr_row[j] + 1,       # Insertion
                prev_row[j + 1] + 1,   # Deletion
                prev_row[j] + cost     # Substitution
            ))
        prev_row = curr_row

    return prev_row[-1]


def similarity_ratio(s1: str, s2: str) -> float:
    """Compute similarity ratio between two strings using Levenshtein distance.

    Returns a float in [0.0, 1.0] where 1.0 means identical strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Float similarity ratio.
    """
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    max_len = max(len(s1), len(s2))
    distance = levenshtein_distance(s1, s2)
    return 1.0 - (distance / max_len)


class NameMatcher:
    """5-step name matching pipeline for narrator-WikiShia article linking."""

    def __init__(self, fuzzy_threshold: float = DEFAULT_FUZZY_THRESHOLD):
        self.fuzzy_threshold = fuzzy_threshold
        # Our narrator data: {id: arabic_name}
        self.narrator_names: Dict[int, str] = {}
        # WikiShia article titles (the target set to match against)
        self.wikishia_titles: Set[str] = set()
        # Manual mapping overrides: {narrator_arabic_name: wikishia_title}
        self.manual_mapping: Dict[str, str] = {}
        # Pre-computed normalized forms
        self._normalized_narrators: Dict[int, str] = {}
        self._normalized_wikishia: Dict[str, str] = {}  # normalized -> original title

    def load_narrator_names(self, id_name_map: Dict[int, str]):
        """Load narrator names from the narrator index.

        Args:
            id_name_map: Dict mapping narrator ID to Arabic name.
        """
        self.narrator_names = dict(id_name_map)
        self._normalized_narrators = {
            nid: normalize_for_matching(name)
            for nid, name in self.narrator_names.items()
        }

    def load_wikishia_titles(self, titles: List[str]):
        """Load WikiShia article titles as the target matching set.

        Args:
            titles: List of WikiShia article titles.
        """
        self.wikishia_titles = set(titles)
        self._normalized_wikishia = {}
        for title in titles:
            normalized = normalize_for_matching(title)
            # Keep the first title for each normalized form
            if normalized not in self._normalized_wikishia:
                self._normalized_wikishia[normalized] = title

    def load_manual_mapping(self, mapping: Dict[str, str]):
        """Load manual mapping overrides.

        Args:
            mapping: Dict mapping narrator Arabic name to WikiShia article title.
        """
        self.manual_mapping = dict(mapping)

    def load_manual_mapping_file(self, filepath: str):
        """Load manual mapping from a JSON file.

        Args:
            filepath: Path to JSON file with {narrator_name: wikishia_title} mapping.
        """
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                self.manual_mapping = json.load(f)
            logger.info("Loaded %d manual mappings from %s",
                        len(self.manual_mapping), filepath)

    def _step1_exact_match(self, narrator_id: int, name: str) -> Optional[MatchResult]:
        """Step 1: Exact match against WikiShia titles."""
        if name in self.wikishia_titles:
            result = MatchResult(narrator_id, name)
            result.matched_title = name
            result.match_step = "exact"
            result.confidence = 1.0
            return result
        return None

    def _step2_normalized_match(self, narrator_id: int, name: str) -> Optional[MatchResult]:
        """Step 2: Normalized match (strip diacritics + normalize letter variants)."""
        normalized_name = self._normalized_narrators.get(narrator_id)
        if not normalized_name:
            normalized_name = normalize_for_matching(name)

        if normalized_name in self._normalized_wikishia:
            result = MatchResult(narrator_id, name)
            result.matched_title = self._normalized_wikishia[normalized_name]
            result.match_step = "normalized"
            result.confidence = 0.95
            return result
        return None

    def _step3_fuzzy_match(self, narrator_id: int, name: str) -> Optional[MatchResult]:
        """Step 3: Fuzzy match using Levenshtein distance."""
        normalized_name = self._normalized_narrators.get(narrator_id)
        if not normalized_name:
            normalized_name = normalize_for_matching(name)

        best_score = 0.0
        best_title = None
        candidates = []

        for norm_title, original_title in self._normalized_wikishia.items():
            score = similarity_ratio(normalized_name, norm_title)
            if score >= self.fuzzy_threshold:
                candidates.append((original_title, score))
                if score > best_score:
                    best_score = score
                    best_title = original_title

        if best_title:
            result = MatchResult(narrator_id, name)
            result.matched_title = best_title
            result.match_step = "fuzzy"
            result.confidence = best_score
            result.candidates = sorted(candidates, key=lambda x: -x[1])[:5]
            return result
        return None

    def _step4_manual_mapping(self, narrator_id: int, name: str) -> Optional[MatchResult]:
        """Step 4: Manual mapping lookup."""
        if name in self.manual_mapping:
            result = MatchResult(narrator_id, name)
            result.matched_title = self.manual_mapping[name]
            result.match_step = "manual"
            result.confidence = 1.0
            return result

        # Also try normalized name against manual mapping
        normalized = normalize_for_matching(name)
        for map_name, map_title in self.manual_mapping.items():
            if normalize_for_matching(map_name) == normalized:
                result = MatchResult(narrator_id, name)
                result.matched_title = map_title
                result.match_step = "manual"
                result.confidence = 0.95
                return result

        return None

    def _step5_ai_assisted(self, narrator_id: int, name: str) -> Optional[MatchResult]:
        """Step 5: AI-assisted matching (placeholder).

        In production, this would call an LLM to evaluate ambiguous matches.
        For now, it returns None (unmatched) and logs the name for review.
        """
        logger.info("AI-assisted matching needed for narrator %d: %s",
                     narrator_id, name)
        return None

    def match_narrator(self, narrator_id: int, name: str) -> MatchResult:
        """Run the full 5-step matching pipeline for a single narrator.

        Steps are tried in order. The first successful match wins.
        Manual mapping (step 4) is checked before fuzzy (step 3) since
        manual overrides should take precedence over algorithmic matches.

        Args:
            narrator_id: Narrator ID from our system.
            name: Arabic narrator name.

        Returns:
            MatchResult with match details (may be unmatched if all steps fail).
        """
        # Step 1: Exact match
        result = self._step1_exact_match(narrator_id, name)
        if result:
            return result

        # Step 4: Manual mapping (checked early since it's an authoritative override)
        result = self._step4_manual_mapping(narrator_id, name)
        if result:
            return result

        # Step 2: Normalized match
        result = self._step2_normalized_match(narrator_id, name)
        if result:
            return result

        # Step 3: Fuzzy match
        result = self._step3_fuzzy_match(narrator_id, name)
        if result:
            return result

        # Step 5: AI-assisted (placeholder)
        result = self._step5_ai_assisted(narrator_id, name)
        if result:
            return result

        # No match found
        return MatchResult(narrator_id, name)

    def run_pipeline(self) -> Dict[int, MatchResult]:
        """Run the matching pipeline for all narrators.

        Returns:
            Dict mapping narrator ID to MatchResult.
        """
        results = {}
        matched_count = 0
        step_counts = {"exact": 0, "normalized": 0, "fuzzy": 0, "manual": 0, "ai": 0}

        for narrator_id, name in self.narrator_names.items():
            result = self.match_narrator(narrator_id, name)
            results[narrator_id] = result

            if result.matched_title:
                matched_count += 1
                step_counts[result.match_step] += 1

        total = len(self.narrator_names)
        logger.info(
            "Matching complete: %d/%d narrators matched (%.1f%%)",
            matched_count, total,
            (matched_count / total * 100) if total > 0 else 0
        )
        for step, count in step_counts.items():
            if count > 0:
                logger.info("  Step '%s': %d matches", step, count)

        return results

    def get_unmatched(self, results: Dict[int, MatchResult]) -> List[MatchResult]:
        """Get list of unmatched narrators from pipeline results.

        Args:
            results: Pipeline results from run_pipeline().

        Returns:
            List of MatchResult objects that had no match.
        """
        return [r for r in results.values() if not r.matched_title]

    def export_results(self, results: Dict[int, MatchResult], filepath: str):
        """Export matching results to a JSON file for review.

        Args:
            results: Pipeline results.
            filepath: Output file path.
        """
        data = {
            "total_narrators": len(results),
            "matched": sum(1 for r in results.values() if r.matched_title),
            "unmatched": sum(1 for r in results.values() if not r.matched_title),
            "by_step": {},
            "results": {},
        }

        step_counts = {}
        for r in results.values():
            if r.match_step:
                step_counts[r.match_step] = step_counts.get(r.match_step, 0) + 1
        data["by_step"] = step_counts

        for nid, result in sorted(results.items()):
            data["results"][str(nid)] = result.to_dict()

        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info("Exported matching results to %s", filepath)
