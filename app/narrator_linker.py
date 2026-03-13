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

SKIP_PREFIX_PATTERN = re.compile(r"^([\d\s-]*|أخْبَرَنَا|أَخْبَرَنَا|وَ)* ")

NARRATORS_TEXT_PATTERN = re.compile(
    r"(.*?) (قَالَ|فِي هَذِهِ الْآيَةِ|يَرْفَعُهُ قَالَ|رَفَعَهُ قَالَ|"
    r"فَكَانَ مِنْ سُؤَالِهِ أَنْ قَالَ|فِي قَوْلِ|فِي قَوْلِهِ|فِي|"
    r"أَنَّ|يَقُولُ|فَقَالَ|مِثْلَهُ)"
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
    r"(.*?) (قال|في هذه الاية|يرفعه قال|رفعه قال|"
    r"فكان من سؤاله ان قال|في قول|في قوله|في|"
    r"ان|يقول|فقال|مثله)"
)

NARRATORS_TEXT_FAILOVER_PATTERN_UNDIACRITIZED = re.compile(
    r"(.*?\( عليهم? السلام \))"
)

SKIP_PREFIX_PATTERN_UNDIACRITIZED = re.compile(r"^([\d\s-]*|اخبرنا|و)* ")


def strip_html(text: str) -> str:
    """Remove HTML span tags from text."""
    return SPAN_PATTERN.sub("", text)


def extract_isnad_text(verse: Verse, use_undiacritized: bool = False) -> Optional[str]:
    """Extract narrator chain text from the first line of a verse.

    Modifies verse.text[0] to remove the extracted chain text.
    Sets verse.narrator_chain.text to the extracted text.

    Returns the extracted chain text, or None if no chain found.
    """
    if not verse.text or len(verse.text) < 1:
        return None

    first_line = verse.text[0]

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
) -> List[Tuple[str, Optional[int]]]:
    """Resolve narrator names against the canonical registry.

    Returns list of (name_text, canonical_id_or_None).
    Uses chain context (preceding names) for disambiguation.
    """
    resolved = []
    preceding = []

    for name in names:
        canonical_id = registry.resolve(name, preceding_names=preceding)
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
    """
    # Step 1: Extract isnad text
    isnad_text = extract_isnad_text(verse, use_undiacritized=use_undiacritized)
    if not isnad_text:
        return []

    # Step 2: Split into names
    names = split_narrator_names(isnad_text, use_undiacritized=use_undiacritized)
    if not names:
        return []

    # Step 3: Resolve against registry
    resolved = resolve_narrators(names, registry)

    # Step 4: Build chain parts
    parts = build_chain_parts(isnad_text, resolved)
    if verse.narrator_chain:
        verse.narrator_chain.parts = parts
        verse.narrator_chain.text = None  # Optimization: drop redundant text

    # Return canonical IDs
    return [cid for _, cid in resolved if cid is not None]
