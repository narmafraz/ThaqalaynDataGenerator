"""Canonical narrator registry with fast lookups and disambiguation.

Loads the canonical_narrators.json file and provides O(1) lookups by
exact Arabic name, normalized name, and context-aware disambiguation.
"""

import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

from app.arabic_normalization import normalize_arabic, strip_tashkeel
from app.config import AI_PIPELINE_DATA_DIR

logger = logging.getLogger(__name__)

REGISTRY_FILENAME = "canonical_narrators.json"


# Canonical-keys that are too underspecified to be safe as a fallback lookup.
# When canonical_lookup_key(name) yields one of these, we skip canonical-key
# indexing for that variant — exact and normalized matches still apply, but
# any honorific-stripped or verb-stripped candidate that collapses down to
# this generic form returns None (kind="plain") instead of mis-linking to a
# real-but-different narrator.
#
# "الشيخ" — common in Tahdhib + al-Istibsar where Tusi refers editorially
# to his teacher al-Mufid (not in the registry). Until al-Mufid has his own
# entry, bare "الشيخ" candidates should not resolve to entry 2709 (a
# different narrator referred to as "الشَّيْخِ ( عليه السلام )" in al-Kafi).
# That al-Kafi reference still resolves via exact match on the parenthetical
# form, since the form has tokens beyond just "الشيخ".
_GENERIC_CKEYS = {
    "الشيخ",
}


# Honorific suffixes to strip from a narrator name when building the canonical
# lookup key. The registry was built from legacy parsing (parenthetical form)
# while the AI pipeline emits the inline form with full diacritics. After
# normalize_arabic strips tashkeel + letter variants, both forms collapse to
# patterns matched here.
#
# CRITICAL: these patterns operate on the *post-normalize_arabic* string, which
# has applied:
#   - tashkeel stripped
#   - ى (alef maksura) → ي (yeh)
#   - آ أ إ ٱ → ا (all hamza-alef variants → plain alef)
#   - ة → ه (teh marbuta → heh)
#   - ؤ → و, ئ → ي
#   - Persian yeh ی → ي, Persian kaf ک → ك
#   - Arabic punctuation (، ؛ ؟) → ASCII (, ; ?)
# So patterns here use the COLLAPSED forms: "صلي" not "صلى", "اله" not "آله",
# "ابي" not "أبي", "," not "،". Building patterns with the original Arabic
# forms WILL NOT match against post-normalize text.
#
# Patterns are anchored at end-of-string with optional leading whitespace.
# _strip_honorific_suffix loops over this list until no pattern fires, so a
# layered tail like " (عليه السلام) : أنه" is peeled one decoration per pass.
_HONORIFIC_SUFFIX_PATTERNS = [
    re.compile(p) for p in (
        # Parenthetical forms — registry's traditional style (post-normalize)
        r"\s*\(\s*ع\.?\s*\)\s*$",
        r"\s*\(\s*ره\s*\)\s*$",
        r"\s*\(\s*ص\s*\)\s*$",
        r"\s*\(\s*عليه السلام\s*\)\s*$",
        r"\s*\(\s*عليها السلام\s*\)\s*$",
        r"\s*\(\s*عليهم السلام\s*\)\s*$",
        r"\s*\(\s*عليهما السلام\s*\)\s*$",
        r"\s*\(\s*صلي الله عليه واله(?:\s+وسلم)?\s*\)\s*$",
        r"\s*\(\s*صلي الله عليه وسلم\s*\)\s*$",
        r"\s*\(\s*صلوات الله عليه\s*\)\s*$",
        r"\s*\(\s*صلوات الله عليها\s*\)\s*$",
        r"\s*\(\s*صلوات الله عليهم\s*\)\s*$",
        r"\s*\(\s*صلوات الله عليهما\s*\)\s*$",
        r"\s*\(\s*رضي الله عنه\s*\)\s*$",
        r"\s*\(\s*رضي الله عنها\s*\)\s*$",
        r"\s*\(\s*رحمه الله\s*\)\s*$",
        # Inline forms — AI pipeline's style (post-normalize)
        r"\s+عليه السلام\s*$",
        r"\s+عليها السلام\s*$",
        r"\s+عليهم السلام\s*$",
        r"\s+عليهما السلام\s*$",
        r"\s+صلي الله عليه واله(?:\s+وسلم)?\s*$",
        r"\s+صلي الله عليه وسلم\s*$",
        r"\s+صلوات الله عليه\s*$",
        r"\s+صلوات الله عليها\s*$",
        r"\s+صلوات الله عليهم\s*$",
        r"\s+صلوات الله عليهما\s*$",
        r"\s+رضي الله عنه\s*$",
        r"\s+رضي الله عنها\s*$",
        r"\s+رحمه الله\s*$",
        r"\s+رحمه الله تعالي\s*$",
        # Note: "أيده الله [تعالى]" is NOT stripped here, even though it
        # looks like an honorific. Removing it from "الشيخ أيده الله
        # تعالى" produces ckey "الشيخ" which collides with a different
        # registry entry — a real narrator referred to as "الشيخ
        # (عليه السلام)" in al-Kafi. Tahdhib's standalone "الشيخ أيده
        # الله تعالى" preamble is handled instead by the per-book
        # preamble strip in narrator_linker._BOOK_PREAMBLE_PATTERNS,
        # which peels the entire phrase before chain extraction so it
        # never reaches the resolver as a candidate name.
        # Trailing standalone "ع"/"ره"/"ص" abbreviations (after parenthetical strip)
        r"\s+ع\s*$",
        r"\s+ره\s*$",
        r"\s+ص\s*$",
        # Trailing particles that the splitter's NARRATORS_TEXT_PATTERN can
        # leak into a name — strict whitespace anchoring keeps this off the
        # interior of real names.  "قال" deliberately stripped only at end of
        # string (the start-of-string variant is kept off the leading-verb list
        # because "قال علي" could be a real attribution; trailing "قال" after
        # a name is not).
        r"\s+انه\s*$",
        r"\s+قال\s*$",
        # Trailing punctuation. Arabic ، ؛ ؟ have already become , ; ? via
        # ARABIC_PUNCTUATION_MAP in normalize_arabic.
        r"\s*[:,;.?\-–—]\s*$",
    )
]


# Leading chain-glue verbs/connectors that occasionally end up baked into a
# narrator name when the AI pipeline extracts isnad (e.g. "روى ابن بكير").
# Matched against the *post-normalize* string. Note: "روى" → "روي" after
# normalize_arabic's alef-maksura → yeh conversion, so prefix uses "روي".
#
# Each entry is checked as a leading-prefix; the first match strips the prefix
# and any whitespace after.
#
# Note: these are deliberately conservative. We don't strip "عن" (from) — that
# IS often part of a kunya (e.g., "عن أبيه" — "from his father", which we want
# to preserve as the link between narrators in the chain, not as part of one
# narrator's identity).
_LEADING_VERB_PREFIXES = (
    "وروي ",   # "وروى" post-normalize: alef-maksura → yeh
    "روي ",    # "روى"
    "وحدثني ",
    "حدثني ",
    "وحدثنا ",
    "حدثنا ",
    "واخبرنا ",
    "اخبرنا ",
    "واخبرني ",
    "اخبرني ",
    "وسمعت ",
    "سمعت ",
    # NOTE: "قال " is NOT stripped — it's too common as a sub-token in real
    # narrator names ("قال علي" could be a real attribution, and stripping it
    # blindly would alias many distinct narrators).
)


def _strip_honorific_suffix(text: str) -> str:
    """Remove a trailing honorific suffix from a normalized narrator name.

    Handles both parenthetical (registry) and inline (AI pipeline) forms.
    Returns the input unchanged if no honorific is present. Operates on
    the *post-normalize_arabic* string (no tashkeel, normalized letters).

    Loops over the pattern list until a full pass produces no change, so
    layered tails such as "(عليه السلام) : انه" peel one decoration per
    iteration. All patterns are end-anchored with optional leading
    whitespace, so iteration converges.
    """
    if not text:
        return text
    result = text
    # Hard cap on iterations as a defensive bound — in practice we converge
    # within a handful of passes (typical depth is 1-3).
    for _ in range(10):
        prev = result
        for pattern in _HONORIFIC_SUFFIX_PATTERNS:
            new_result = pattern.sub("", result)
            if new_result != result:
                result = new_result.rstrip()
                break
        if result == prev:
            break
    return result


def _strip_leading_chain_verb(text: str) -> str:
    """Remove a leading chain-glue verb from a normalized narrator name.

    Examples (after normalize_arabic):
        "روى ابن بكير" -> "ابن بكير"
        "حدثنا محمد بن يعقوب" -> "محمد بن يعقوب"
        "ابن بكير" -> "ابن بكير" (unchanged)
    """
    if not text:
        return text
    for prefix in _LEADING_VERB_PREFIXES:
        if text.startswith(prefix):
            stripped = text[len(prefix):].lstrip()
            # Refuse to return an empty/single-word stub — if the verb was
            # the entire content, there's no narrator name to extract.
            if stripped:
                return stripped
    return text


def canonical_lookup_key(name_ar: str) -> str:
    """Build a maximally-stripped lookup key for a narrator name.

    Pipeline:
    1. normalize_arabic() — strip tashkeel, normalize letter variants, etc.
    2. Strip trailing honorific suffix (parenthetical OR inline form).
    3. Strip leading chain verb (روى, حدثنا, etc.).
    4. Re-collapse any whitespace introduced.

    Returns empty string if the input collapses to nothing useful.
    """
    if not name_ar:
        return ""
    normalized = normalize_arabic(name_ar)
    stripped = _strip_honorific_suffix(normalized)
    stripped = _strip_leading_chain_verb(stripped)
    return stripped.strip()


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
        # Looser key: post-normalize + honorific-stripped + leading-verb-stripped.
        # Used as a fallback when the AI emits a name that differs from the
        # registry's canonical form only in honorific style or has a chain-verb
        # prefix. Kept separate from _by_normalized so we don't conflate them.
        self._by_canonical_key: Dict[str, List[int]] = {}

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
        """Add a name variant to exact, normalized, and canonical-key indexes."""
        # Exact lookup
        if name_ar not in self._by_exact_ar:
            self._by_exact_ar[name_ar] = canonical_id

        # Normalized lookup
        normalized = normalize_arabic(name_ar)
        if normalized not in self._by_normalized:
            self._by_normalized[normalized] = []
        if canonical_id not in self._by_normalized[normalized]:
            self._by_normalized[normalized].append(canonical_id)

        # Canonical-key lookup (honorific-stripped + leading-verb-stripped).
        # Always register, even when ckey == normalized — needed so AI-emitted
        # names with verb-prefix or honorific-suffix can resolve to a registry
        # entry whose own canonical form happens to be a "clean" name (no
        # honorific or verb to strip).
        #
        # Skip the canonical-key index for ckeys in _GENERIC_CKEYS — those
        # are too underspecified to be safe as a fallback.
        ckey = canonical_lookup_key(name_ar)
        if ckey and ckey not in _GENERIC_CKEYS:
            if ckey not in self._by_canonical_key:
                self._by_canonical_key[ckey] = []
            if canonical_id not in self._by_canonical_key[ckey]:
                self._by_canonical_key[ckey].append(canonical_id)

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

    def lookup_canonical_key(self, name_ar: str) -> List[int]:
        """Look up canonical IDs by the looser canonical key (honorific-stripped
        + leading-verb-stripped). Returns list (may be ambiguous).

        Use this as a fallback when normalized lookup fails — it handles the
        registry-vs-AI honorific format mismatch and chain-verb prefixes.
        """
        ckey = canonical_lookup_key(name_ar)
        if not ckey:
            return []
        return self._by_canonical_key.get(ckey, [])

    def resolve(
        self,
        name_ar: str,
        preceding_names: Optional[List[str]] = None,
        book_slug: Optional[str] = None,
    ) -> Optional[int]:
        """Resolve a narrator name to canonical ID with context-aware disambiguation.

        Strategy:
        1. Try exact match first (fastest, most precise)
        2. Try normalized match
        3. Fall back to canonical-key match (honorific/verb-stripped)
        4. Apply per-stage book-scope filter (disambiguation_books)
        5. Disambiguate by preceding-names context if multiple candidates remain
        6. Default to first candidate (registry-frequency ordered)

        Args:
            name_ar: Arabic narrator name to resolve.
            preceding_names: List of preceding narrator names in the chain
                (used for preceding-context disambiguation).
            book_slug: Slug of the book the chain is from (e.g. "tahdhib-al-ahkam").
                When supplied, candidates with a ``disambiguation_books`` entry
                are filtered: a candidate matches only if its book list contains
                this slug. Candidates without a ``disambiguation_books`` field
                are always allowed.

        The book filter applies at every stage. For stage 1 (exact match),
        passing the filter is required — if the only exact-match candidate
        fails the book scope, exact match yields no result and we fall through
        to stage 2.
        """
        # Step 1: Exact match (single candidate)
        exact = self.lookup_exact(name_ar)
        if exact is not None:
            filtered = self._filter_by_book([exact], book_slug)
            if filtered:
                return filtered[0]
            # Exact candidate rejected by book filter — fall through to next stages

        # Step 2: Normalized match
        candidates = self.lookup_normalized(name_ar)
        candidates = self._filter_by_book(candidates, book_slug)
        if len(candidates) == 0:
            # Step 2b: Fall back to canonical-key match (handles honorific
            # format mismatch + chain-verb prefix).
            candidates = self.lookup_canonical_key(name_ar)
            candidates = self._filter_by_book(candidates, book_slug)
            if len(candidates) == 0:
                return None

        if len(candidates) == 1:
            return candidates[0]

        # Step 3: Disambiguation via preceding-names context
        if preceding_names:
            for cid in candidates:
                entry = self._narrators.get(cid, {})
                context = entry.get("disambiguation_context")
                if context and self._matches_context(context, preceding_names):
                    return cid

        # Step 4: Default to most common (first in list, which is ordered by narration count)
        # Return first candidate as default — bootstrap orders by frequency
        return candidates[0]

    def _filter_by_book(
        self,
        candidates: List[int],
        book_slug: Optional[str],
    ) -> List[int]:
        """Apply per-entry ``disambiguation_books`` constraint to candidates.

        An entry that declares ``disambiguation_books`` matches only when the
        provided ``book_slug`` is in that list. Entries without the field are
        always allowed. When ``book_slug`` is None, no entry is filtered out —
        the field is ignored entirely (backward-compatible for callers that
        don't yet pass the book context).
        """
        if not candidates:
            return candidates
        if book_slug is None:
            return candidates
        result = []
        for cid in candidates:
            entry = self._narrators.get(cid, {})
            books = entry.get("disambiguation_books")
            if not books:
                # No book scope declared — always allow
                result.append(cid)
            elif book_slug in books:
                result.append(cid)
            # Else: this entry is book-scoped and the current book doesn't match — drop
        return result

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
