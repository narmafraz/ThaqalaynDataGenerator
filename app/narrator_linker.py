"""Generic narrator extraction and linking for all books.

Extracts reusable logic from kafi_narrators.py into a generic module
that works across all books (Al-Kafi, Thaqalayn API books, GHBook books).

Provides:
- extract_isnad_text(): Extract narrator chain text from hadith first line
- split_narrator_names(): Split chain text into individual narrator names
- resolve_narrators(): Resolve names against NarratorRegistry
- build_chain_parts(): Build SpecialText parts for narrator_chain
- link_verse_narrators(): Orchestrator combining all steps
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

from app.arabic_normalization import normalize_arabic, strip_tashkeel
from app.models.quran import NarratorChain, SpecialText, Verse
from app.narrator_registry import NarratorRegistry

logger = logging.getLogger(__name__)

# ── Regex patterns (ported from kafi_narrators.py) ──────────────────────

SPAN_PATTERN = re.compile(r"</?span[^>]*>")

# Diacritized patterns (precise, for Al-Kafi and well-diacritized sources)
NARRATOR_SPLIT_PATTERN = re.compile(
    r" (?:عَمَّنْ سَمِعَ|وَ سَمِعْتُ|وَ|جَمِيعاً عَنْ|جَمِيعاً عَنِ|"
    r"عَنْ|عَنِ|إِلَى|قَالَ حَدَّثَنِي|عَمَّنْ|مِمَّنْ|مِنْهُمْ|"
    r"رَفَعَهُ عَنْ|رَفَعَهُ إِلَى|فِي حَدِيثِ|رَفَعَهُ أَنَّ|رَفَعَهُ) "
)

# Leading prefix to strip before narrator splitting. Anything that's chain-glue
# at the very start of the isnad — verbs of attribution, the connector "wa-",
# numeric prefixes from indexed sources — and not part of any narrator's name.
#
# Adding these (May 2026): رَوَى, وَرَوَى, حَدَّثَنِي/وَحَدَّثَنِي,
# حَدَّثَنَا/وَحَدَّثَنَا, أَخْبَرَنِي/وَأَخْبَرَنَا/وَأَخْبَرَنِي,
# سَمِعْتُ/وَسَمِعْتُ. Without these, the AI-generated isnad text often started
# with e.g. "رَوَى اِبْنُ بُكَيْرٍ ..." and the leading verb baked into the first
# extracted narrator name, blocking canonical_id resolution.
SKIP_PREFIX_PATTERN = re.compile(
    r"^([\d\s-]*|"
    r"أخْبَرَنَا|أَخْبَرَنَا|أَخْبَرَنِي|"
    r"وَأَخْبَرَنَا|وَأَخْبَرَنِي|"
    r"حَدَّثَنَا|حَدَّثَنِي|وَحَدَّثَنَا|وَحَدَّثَنِي|"
    r"رَوَى|وَرَوَى|"
    r"سَمِعْتُ|وَسَمِعْتُ|"
    r"وَ)* "
)

NARRATORS_TEXT_PATTERN = re.compile(
    # Longer alternatives must come first — Python's re takes the leftmost
    # alternation match, not the longest. Without this ordering, "أَنَّهُ"
    # would match as "أَنَّ" + leftover "هُ", and the trailing هُ would be
    # baked into the last narrator name. Same logic for the "فِي قَوْلِهِ"
    # vs "فِي قَوْلِ" vs "فِي" triplet which was already ordered correctly.
    #
    # The "وَبِهَذَا الْإِسْنَادِ" / "وَفِي رِوَايَةِ" markers terminate the
    # chain early when a verse references a previously-stated isnad with a
    # back-reference; otherwise the splitter swallows the meta-prose into
    # the chain and produces non-narrator candidates.
    r"(.*?) (قَالَ|فِي هَذِهِ الْآيَةِ|يَرْفَعُهُ قَالَ|رَفَعَهُ قَالَ|"
    r"فَكَانَ مِنْ سُؤَالِهِ أَنْ قَالَ|فِي قَوْلِهِ|فِي قَوْلِ|فِي|"
    r"وَبِهَذَا الْإِسْنَادِ|وَفِي رِوَايَةِ|"
    r"أَنَّهُ|أَنَّ|يَقُولُ|فَقَالَ|مِثْلَهُ)"
)

NARRATORS_TEXT_FAILOVER_PATTERN = re.compile(r"(.*?\( عليهم? السلام \))")

NARRATORS_TEXT_CONTINUE_PATTERN = re.compile(r"\s*(حَدَّثَنِي)\s")

# Undiacritized patterns (for less-diacritized sources like thaqalayn_api, ghbook)
_UNDIACRITIZED_SPLIT_WORDS = [
    "عمن سمع", "و سمعت", "و", "جميعا عن", "جميعا عن",
    "عن", "عن", "الى", "قال حدثني", "عمن", "ممن", "منهم",
    "رفعه عن", "رفعه الى", "في حديث", "رفعه ان", "رفعه",
]
NARRATOR_SPLIT_PATTERN_UNDIACRITIZED = re.compile(
    " (?:" + "|".join(re.escape(w) for w in _UNDIACRITIZED_SPLIT_WORDS) + ") "
)

NARRATORS_TEXT_PATTERN_UNDIACRITIZED = re.compile(
    # See NARRATORS_TEXT_PATTERN for the alternation-ordering rule. Note
    # that "ان" / "انه" share the same first two letters; "انه" must come
    # first so it consumes the trailing ه instead of leaving it on the name.
    r"(.*?) (قال|في هذه الاية|يرفعه قال|رفعه قال|"
    r"فكان من سؤاله ان قال|في قوله|في قول|في|"
    r"وبهذا الاسناد|وفي رواية|"
    r"انه|ان|يقول|فقال|مثله)"
)

NARRATORS_TEXT_FAILOVER_PATTERN_UNDIACRITIZED = re.compile(
    r"(.*?\( عليهم? السلام \))"
)

SKIP_PREFIX_PATTERN_UNDIACRITIZED = re.compile(
    r"^([\d\s-]*|"
    r"اخبرنا|اخبرني|واخبرنا|واخبرني|"
    r"حدثنا|حدثني|وحدثنا|وحدثني|"
    r"روى|وروى|"
    r"سمعت|وسمعت|"
    r"و)* "
)


# Chain-signal regex used by _looks_like_isnad. A first line that contains
# none of these in its leading window almost certainly has no isnad chain —
# it's a narrative quote, a back-reference, or a heading. Running the chain
# extractor on such text produces non-narrator candidates (Class 2).
#
# Permissive: any single signal qualifies. Operates on the raw (diacritized)
# string, with a strip_tashkeel fallback for less-diacritized sources.
#
# Signals:
#   - "بن" / "بْنِ" / "بنِ" — patronymic, present in almost every isnad name
#   - chain-formal verbs: حدثنا, حدثني, اخبرنا, اخبرني, سمعت, روى
#   - chain connectors: عن, جميعا عن
_ISNAD_SIGNAL_PATTERN = re.compile(
    r"بْن[ُِ]|\bبن\b|\bعَنْ\b|\bعَنِ\b|\bعن\b|"
    r"حَدَّثَنَا|حَدَّثَنِي|أَخْبَرَنَا|أَخْبَرَنِي|سَمِعْتُ|رَوَى|"
    r"حدثنا|حدثني|اخبرنا|اخبرني|سمعت|روى"
)
_ISNAD_SIGNAL_WINDOW = 120  # chars from start of first_line to scan


def _looks_like_isnad(first_line: str) -> bool:
    """Return True if the first line plausibly opens with an isnad chain.

    Used as a Class 2 pre-filter: when the function returns False, the
    extractor skips chain extraction entirely and reports "no chain"
    rather than slicing narrative prose into garbage candidate names.

    Heuristic is deliberately permissive — any one of the standard
    chain signals (patronymic "بن", a chain-formal verb like حدثنا /
    اخبرنا / سمعت / روى, or a chain connector like عن) within the
    first ~120 chars passes. This admits unusual chains while still
    filtering out narrative entries that have none of these markers.
    """
    if not first_line:
        return False
    window = first_line[:_ISNAD_SIGNAL_WINDOW]
    if _ISNAD_SIGNAL_PATTERN.search(window):
        return True
    # Tashkeel-stripped fallback for sources with inconsistent diacritization
    return bool(_ISNAD_SIGNAL_PATTERN.search(strip_tashkeel(window)))


# Per-book leading preambles to peel off the first line before chain
# extraction. These are book-specific narrative wrappers around an actual
# isnad — Tahdhib's "what the Sheikh told me about" formula is the canonical
# example. Without peeling, the chain-extractor either misses the real
# chain or bakes the preamble's words into the first narrator name.
#
# Each entry: post-strip_tashkeel regex anchored at start, applied after
# strip_tashkeel for stability across diacritization variants.
_BOOK_PREAMBLE_PATTERNS = {
    "tahdhib-al-ahkam": [
        re.compile(r"^\s*ما أخبرني به الشيخ أيده الله(?:\s+تعالى)?\s+"),
        re.compile(r"^\s*أخبرني به الشيخ أيده الله(?:\s+تعالى)?\s+"),
        # Standalone "الشيخ أيده الله [تعالى]" without the "ما أخبرني به"
        # prefix — Tahdhib's editorial reference to al-Mufid as the source
        # of the chain. Must be peeled here, not via honorific-suffix strip
        # in the resolver: stripping "أيده الله تعالى" alone collapses the
        # candidate to ckey "الشيخ" which collides with a different
        # registered narrator (an Imam referred to as "the Sheikh" in
        # al-Kafi).
        re.compile(r"^\s*الشيخ أيده الله(?:\s+تعالى)?\s+"),
        re.compile(r"^\s*فأما ما رواه\s+"),
        re.compile(r"^\s*ما رواه\s+"),
    ],
    "al-istibsar": [
        re.compile(r"^\s*ما أخبرني به الشيخ أيده الله(?:\s+تعالى)?\s+"),
        re.compile(r"^\s*أخبرني به الشيخ أيده الله(?:\s+تعالى)?\s+"),
        re.compile(r"^\s*الشيخ أيده الله(?:\s+تعالى)?\s+"),
        re.compile(r"^\s*فأما ما رواه\s+"),
        re.compile(r"^\s*ما رواه\s+"),
    ],
}


def _strip_book_preamble(first_line: str, book_slug: Optional[str]) -> str:
    """Strip a known per-book preamble from the start of the first line.

    Operates on the diacritized string by mapping match positions through
    strip_tashkeel — the patterns are written against the post-strip form
    so they work regardless of how heavily the source was diacritized.

    Returns first_line unchanged if book_slug is unknown or no preamble
    matches.
    """
    if not first_line or not book_slug:
        return first_line
    patterns = _BOOK_PREAMBLE_PATTERNS.get(book_slug)
    if not patterns:
        return first_line
    stripped = strip_tashkeel(first_line)
    for pattern in patterns:
        m = pattern.match(stripped)
        if not m:
            continue
        # Map the post-strip end_index back to the original-string position.
        # strip_tashkeel only removes combining marks, so character-by-character
        # alignment finds where the preamble ends in the original.
        end = m.end(0)
        stripped_pos = 0
        original_pos = 0
        while stripped_pos < end and original_pos < len(first_line):
            if first_line[original_pos] == stripped[stripped_pos]:
                stripped_pos += 1
            original_pos += 1
        return first_line[original_pos:]
    return first_line


def _book_slug_from_path(path: Optional[str]) -> Optional[str]:
    """Extract a book slug from a verse path like /books/tahdhib-al-ahkam:1:2:3."""
    if not path or not path.startswith("/books/"):
        return None
    rest = path[len("/books/"):]
    slug, _, _ = rest.partition(":")
    return slug or None


def strip_html(text: str) -> str:
    """Remove HTML span tags from text."""
    return SPAN_PATTERN.sub("", text)


def _reconstruct_chain_text_from_parts(verse: Verse) -> Optional[str]:
    """Reconstruct chain text from a verse's existing narrator_chain.parts.

    Used by extract_isnad_text as the idempotent path: when a verse's
    chain was extracted on a prior run, it lives in narrator_chain.parts
    (with verse.text[0] already truncated to matn). Concatenating the
    parts' text fields recovers the original chain text.

    Returns None if no usable parts are present (i.e. fresh data — the
    caller should fall through to the regex path).
    """
    chain = getattr(verse, "narrator_chain", None)
    if chain is None:
        return None
    parts = getattr(chain, "parts", None)
    if not parts:
        return None
    pieces = []
    for p in parts:
        text = getattr(p, "text", None) or ""
        if text:
            pieces.append(text)
    if not pieces:
        return None
    return "".join(pieces)


def extract_isnad_text(verse: Verse, use_undiacritized: bool = False) -> Optional[str]:
    """Extract narrator chain text from the first line of a verse.

    Modifies verse.text[0] to remove the extracted chain text.
    Sets verse.narrator_chain.text to the extracted text.

    Returns the extracted chain text, or None if no chain found.

    Idempotent: if the chain has already been extracted on a prior run
    (verse.text[0] is matn-only, but verse.narrator_chain.parts contains
    the previously-extracted chain), reconstructs the chain text from
    those parts and returns it without re-modifying verse.text[0]. Without
    this fallback, a re-run of process_all_narrators on already-processed
    data would silently yield zero narrators and (in combination with the
    pre-extraction folder delete) destroy all narrator profile pages.

    Class 2 guards (applied only on fresh extraction, not on the idempotent
    re-read path):
    - _strip_book_preamble peels known per-book wrappers (e.g. Tahdhib's
      "ما أخبرني به الشيخ أيده الله تعالى") before pattern matching.
    - _looks_like_isnad bails early when the first line shows no chain
      signal (no بن, no chain-formal verb, no عن within the leading window),
      so narrative prose isn't sliced into garbage candidate names.
    """
    if not verse.text or len(verse.text) < 1:
        # Even with no text, the chain may have been moved into parts
        # on a prior run. Try reconstructing from parts before giving up.
        reconstructed = _reconstruct_chain_text_from_parts(verse)
        return reconstructed

    # Idempotent path: if narrator_chain already has parts (chain was
    # extracted on a previous run), reconstruct from there. The pattern
    # match below would fail because verse.text[0] is now the matn —
    # without this short-circuit the function is destructive on re-run.
    # IMPORTANT: this short-circuit must come before the Class 2 guards
    # below so re-runs of process_all_narrators don't drop chains that
    # _looks_like_isnad would reject when applied to a chain-stripped
    # verse.text[0] (which is a matn, not an isnad).
    reconstructed = _reconstruct_chain_text_from_parts(verse)
    if reconstructed:
        return reconstructed

    first_line = verse.text[0]

    # Class 2 step: peel known per-book preambles. Updates verse.text[0]
    # so the peeled portion is dropped from both chain and matn — these
    # preambles are meta-prose, not content the user needs to read.
    book_slug = _book_slug_from_path(getattr(verse, "path", None))
    peeled = _strip_book_preamble(first_line, book_slug)
    if peeled is not first_line and len(peeled) < len(first_line):
        verse.text[0] = peeled
        first_line = peeled

    # Class 2 step: bail when no isnad signal is present. Returning None
    # here means "no chain", which downstream link_verse_narrators handles
    # the same as a regex non-match — no narrator parts produced.
    if not _looks_like_isnad(first_line):
        return None

    # Try diacritized patterns first (more precise)
    text_pattern = NARRATORS_TEXT_PATTERN
    failover_pattern = NARRATORS_TEXT_FAILOVER_PATTERN
    continue_pattern = NARRATORS_TEXT_CONTINUE_PATTERN

    narrators_text_match = text_pattern.match(first_line)
    if not narrators_text_match:
        narrators_text_match = failover_pattern.match(first_line)

    # If diacritized didn't match and undiacritized is allowed, try that
    if not narrators_text_match and use_undiacritized:
        stripped = strip_tashkeel(first_line)
        text_pattern_u = NARRATORS_TEXT_PATTERN_UNDIACRITIZED
        failover_pattern_u = NARRATORS_TEXT_FAILOVER_PATTERN_UNDIACRITIZED

        narrators_text_match = text_pattern_u.match(stripped)
        if not narrators_text_match:
            narrators_text_match = failover_pattern_u.match(stripped)

        if narrators_text_match:
            # Map position back to original text (same char count since we only removed combining marks)
            # For undiacritized match, extract from original text using the match positions
            end_index = narrators_text_match.end(0)
            # The stripped text has fewer chars, so we need to map back
            # Count tashkeel chars before the match end to adjust
            stripped_pos = 0
            original_pos = 0
            while stripped_pos < end_index and original_pos < len(first_line):
                if first_line[original_pos] == stripped[stripped_pos]:
                    stripped_pos += 1
                original_pos += 1
            end_index = original_pos

            narrators_text = first_line[:end_index]
            hadith_text = first_line[end_index:]
            verse.text[0] = hadith_text

            if not verse.narrator_chain:
                verse.narrator_chain = NarratorChain()
                verse.narrator_chain.parts = []
            verse.narrator_chain.text = narrators_text
            return narrators_text

    if not narrators_text_match:
        return None

    # Process the match (diacritized path)
    while narrators_text_match:
        if len(narrators_text_match.groups()) > 1:
            ending_phrase_len = len(narrators_text_match.groups()[-1]) + 1
        else:
            ending_phrase_len = 0
        end_index = narrators_text_match.end(0)
        if continue_pattern.match(first_line, end_index):
            narrators_text_match = text_pattern.match(first_line, end_index)
        else:
            break

    narrators_text = first_line[:end_index]
    hadith_text = first_line[end_index:]
    verse.text[0] = hadith_text

    if not verse.narrator_chain:
        verse.narrator_chain = NarratorChain()
        verse.narrator_chain.parts = []
    verse.narrator_chain.text = narrators_text

    return narrators_text


def _trim_ending_phrase(isnad_text: str) -> str:
    """Trim ending action phrase (e.g., قَالَ) from isnad text before splitting.

    The original extract_narrators captures text up to an action verb.
    The chain text includes the verb, but splitting should exclude it.
    """
    # Try diacritized pattern
    match = NARRATORS_TEXT_PATTERN.match(isnad_text)
    if match and len(match.groups()) > 1:
        ending_phrase_len = len(match.groups()[-1]) + 1
        # Handle continuation patterns
        while True:
            end_index = match.end(0)
            cont_match = NARRATORS_TEXT_CONTINUE_PATTERN.match(isnad_text, end_index)
            if cont_match:
                match = NARRATORS_TEXT_PATTERN.match(isnad_text, end_index)
                if match and len(match.groups()) > 1:
                    ending_phrase_len = len(match.groups()[-1]) + 1
                    continue
            break
        if ending_phrase_len > 0:
            return isnad_text[:-ending_phrase_len]

    # Try failover pattern (no ending phrase to trim)
    match = NARRATORS_TEXT_FAILOVER_PATTERN.match(isnad_text)
    if match:
        return isnad_text

    return isnad_text


def split_narrator_names(
    isnad_text: str,
    use_undiacritized: bool = False,
) -> List[str]:
    """Split narrator chain text into individual narrator names.

    Args:
        isnad_text: The extracted chain text (may include ending action phrase)
        use_undiacritized: If True, try undiacritized patterns as fallback

    Returns:
        List of narrator name strings
    """
    # Trim ending phrase (e.g., "قَالَ") before splitting
    trimmed = _trim_ending_phrase(isnad_text)

    # First trim prefix, then split
    narrators_without_prefix = SKIP_PREFIX_PATTERN.sub("", trimmed)
    narrators = NARRATOR_SPLIT_PATTERN.split(narrators_without_prefix)

    if len(narrators) <= 1 and use_undiacritized:
        # Try undiacritized
        stripped = strip_tashkeel(trimmed)
        narrators_without_prefix = SKIP_PREFIX_PATTERN_UNDIACRITIZED.sub("", stripped)
        narrators = NARRATOR_SPLIT_PATTERN_UNDIACRITIZED.split(narrators_without_prefix)

    return [n for n in narrators if n.strip()]


def resolve_narrators(
    names: List[str],
    registry: NarratorRegistry,
    book_slug: Optional[str] = None,
) -> List[Tuple[str, Optional[int]]]:
    """Resolve narrator names against the canonical registry.

    Returns list of (name_text, canonical_id_or_None).
    Uses chain context (preceding names) for disambiguation, and the
    optional ``book_slug`` to scope candidates that declare a
    ``disambiguation_books`` constraint (e.g. al-Mufid for Tahdhib/Istibsar
    chains referring to "الشيخ").
    """
    resolved = []
    preceding = []

    for name in names:
        canonical_id = registry.resolve(
            name, preceding_names=preceding, book_slug=book_slug
        )
        resolved.append((name, canonical_id))
        preceding.append(name)

    return resolved


def build_chain_parts(
    isnad_text: str,
    resolved: List[Tuple[str, Optional[int]]],
) -> List[SpecialText]:
    """Build narrator_chain.parts from resolved narrator names.

    Creates alternating plain/narrator SpecialText objects.
    Narrators with canonical_id get kind="narrator" and path="/people/narrators/{id}".
    Narrators without canonical_id get kind="plain" (unlinked).
    """
    parts = []
    remaining = isnad_text

    for name_text, canonical_id in resolved:
        split_result = remaining.split(name_text, 1)
        if len(split_result) != 2:
            logger.warning(
                "Could not split chain text by narrator '%s' in '%s'",
                name_text, remaining[:80]
            )
            continue

        before, remaining = split_result

        if before:
            part = SpecialText()
            part.kind = "plain"
            part.text = before
            parts.append(part)

        narrator_part = SpecialText()
        if canonical_id is not None:
            narrator_part.kind = "narrator"
            narrator_part.text = name_text
            narrator_part.path = f"/people/narrators/{canonical_id}"
        else:
            narrator_part.kind = "plain"
            narrator_part.text = name_text
        parts.append(narrator_part)

    # Trailing text
    if remaining:
        part = SpecialText()
        part.kind = "plain"
        part.text = remaining
        parts.append(part)

    return parts


def link_verse_narrators(
    verse: Verse,
    registry: NarratorRegistry,
    use_undiacritized: bool = False,
) -> List[int]:
    """Orchestrator: extract narrator chain, split, resolve, build parts.

    Modifies verse in-place (sets narrator_chain.parts, removes chain from text[0]).
    Returns list of canonical IDs found (may be empty).

    The verse's book slug is derived from its path and threaded through to
    the resolver as ``book_slug`` — registry entries with a
    ``disambiguation_books`` constraint (e.g. al-Mufid for Tahdhib/Istibsar)
    use it to scope themselves to the right books.
    """
    # Step 1: Extract isnad text
    isnad_text = extract_isnad_text(verse, use_undiacritized=use_undiacritized)
    if not isnad_text:
        return []

    # Step 2: Split into names
    names = split_narrator_names(isnad_text, use_undiacritized=use_undiacritized)
    if not names:
        return []

    # Step 3: Resolve against registry, with book context for disambiguation
    book_slug = _book_slug_from_path(getattr(verse, "path", None))
    resolved = resolve_narrators(names, registry, book_slug=book_slug)

    # Step 4: Build chain parts
    parts = build_chain_parts(isnad_text, resolved)
    if verse.narrator_chain:
        verse.narrator_chain.parts = parts
        verse.narrator_chain.text = None  # Optimization: drop redundant text

    # Return canonical IDs
    return [cid for _, cid in resolved if cid is not None]
