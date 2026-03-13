"""Canonical narrator registry with fast lookups and disambiguation.

Loads the canonical_narrators.json file and provides O(1) lookups by
exact Arabic name, normalized name, and context-aware disambiguation.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

from app.arabic_normalization import normalize_arabic, strip_tashkeel
from app.config import AI_PIPELINE_DATA_DIR

logger = logging.getLogger(__name__)

REGISTRY_FILENAME = "canonical_narrators.json"


class NarratorRegistry:
    """Canonical narrator registry with fast lookups.

    Provides:
    - Exact Arabic name lookup (O(1))
    - Normalized name lookup (O(1))
    - Context-aware disambiguation for ambiguous names
    """

    def __init__(self, path: Optional[str] = None):
        if path is None:
            path = os.path.join(AI_PIPELINE_DATA_DIR, REGISTRY_FILENAME)
        self._path = path
        self._narrators: Dict[int, dict] = {}
        self._last_id: int = 0
        self._version: str = ""

        # Fast lookup indexes
        self._by_exact_ar: Dict[str, int] = {}
        self._by_normalized: Dict[str, List[int]] = {}

        self._load(path)

    def _load(self, path: str):
        """Load registry from JSON file and build indexes."""
        if not os.path.isfile(path):
            logger.warning("Narrator registry not found at %s", path)
            return

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._version = data.get("version", "")
        self._last_id = data.get("last_id", 0)

        narrators = data.get("narrators", {})
        for id_str, entry in narrators.items():
            canonical_id = int(id_str)
            self._narrators[canonical_id] = entry

            # Index canonical name
            canonical_ar = entry.get("canonical_name_ar", "")
            if canonical_ar:
                self._index_variant(canonical_ar, canonical_id)

            # Index all variants
            for variant in entry.get("variants_ar", []):
                self._index_variant(variant, canonical_id)

    def _index_variant(self, name_ar: str, canonical_id: int):
        """Add a name variant to both exact and normalized indexes."""
        # Exact lookup
        if name_ar not in self._by_exact_ar:
            self._by_exact_ar[name_ar] = canonical_id

        # Normalized lookup
        normalized = normalize_arabic(name_ar)
        if normalized not in self._by_normalized:
            self._by_normalized[normalized] = []
        if canonical_id not in self._by_normalized[normalized]:
            self._by_normalized[normalized].append(canonical_id)

    @property
    def version(self) -> str:
        return self._version

    @property
    def last_id(self) -> int:
        return self._last_id

    @property
    def narrator_count(self) -> int:
        return len(self._narrators)

    def lookup_exact(self, name_ar: str) -> Optional[int]:
        """Look up canonical ID by exact Arabic name match. O(1)."""
        return self._by_exact_ar.get(name_ar)

    def lookup_normalized(self, name_ar: str) -> List[int]:
        """Look up canonical IDs by normalized Arabic name. Returns list (may be ambiguous)."""
        normalized = normalize_arabic(name_ar)
        return self._by_normalized.get(normalized, [])

    def resolve(self, name_ar: str, preceding_names: Optional[List[str]] = None) -> Optional[int]:
        """Resolve a narrator name to canonical ID with context-aware disambiguation.

        Strategy:
        1. Try exact match first (fastest, most precise)
        2. Try normalized match — if unique, return it
        3. If multiple normalized matches, use disambiguation_context
        4. Return None if truly ambiguous

        Args:
            name_ar: Arabic narrator name to resolve
            preceding_names: List of preceding narrator names in the chain (for disambiguation)
        """
        # Step 1: Exact match
        exact = self.lookup_exact(name_ar)
        if exact is not None:
            return exact

        # Step 2: Normalized match
        candidates = self.lookup_normalized(name_ar)
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) == 0:
            return None

        # Step 3: Disambiguation via context
        if preceding_names:
            for cid in candidates:
                entry = self._narrators.get(cid, {})
                context = entry.get("disambiguation_context")
                if context and self._matches_context(context, preceding_names):
                    return cid

        # Step 4: Default to most common (first in list, which is ordered by narration count)
        # Return first candidate as default — bootstrap orders by frequency
        return candidates[0]

    def _matches_context(self, context: str, preceding_names: List[str]) -> bool:
        """Check if disambiguation context matches the chain context.

        Context strings like:
        - "When preceded by عَلِيُّ بْنُ إِبْرَاهِيمَ in the chain"
        - "After محمد بن يحيى"
        """
        if not context or not preceding_names:
            return False

        # Extract the Arabic name from the context string
        # Look for Arabic text in the context
        context_normalized = normalize_arabic(context)
        for name in preceding_names:
            name_normalized = normalize_arabic(name)
            if name_normalized and name_normalized in context_normalized:
                return True
        return False

    def get_narrator(self, canonical_id: int) -> Optional[dict]:
        """Get full narrator entry by canonical ID."""
        return self._narrators.get(canonical_id)

    def get_name_ar(self, canonical_id: int) -> Optional[str]:
        """Get canonical Arabic name for an ID."""
        entry = self._narrators.get(canonical_id)
        if entry:
            return entry.get("canonical_name_ar")
        return None

    def get_name_en(self, canonical_id: int) -> Optional[str]:
        """Get canonical English name for an ID."""
        entry = self._narrators.get(canonical_id)
        if entry:
            return entry.get("canonical_name_en")
        return None

    def register_variant(self, canonical_id: int, name_ar: str):
        """Register a new Arabic variant for an existing canonical narrator.

        Does not persist to disk — call save() for that.
        """
        entry = self._narrators.get(canonical_id)
        if entry is None:
            raise ValueError(f"Canonical ID {canonical_id} not found in registry")

        variants = entry.get("variants_ar", [])
        if name_ar not in variants:
            variants.append(name_ar)
            entry["variants_ar"] = variants

        self._index_variant(name_ar, canonical_id)

    def all_ids(self) -> List[int]:
        """Return all canonical IDs in the registry."""
        return sorted(self._narrators.keys())

    def save(self, path: Optional[str] = None):
        """Save registry back to JSON file."""
        if path is None:
            path = self._path

        data = {
            "version": self._version,
            "last_id": self._last_id,
            "narrators": {str(k): v for k, v in sorted(self._narrators.items())},
        }

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
