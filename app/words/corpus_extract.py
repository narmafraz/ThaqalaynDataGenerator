"""Walk v4 corpus, extract unique surface forms with counts + paths.

This is Phase A of the Words pipeline. Produces the
``corpus_surface_set``: a dict mapping every NFC-normalized Arabic
surface form found in any chunk to its frequency and the list of
verse paths where it appears.

The output drives:
- Surface page generation (one page per unique surface, with
  ``occurrence_paths`` populated from this dict)
- Lemma page generation (``in_corpus`` + ``count`` flags on
  paradigm entries are O(1) lookups into this dict)
- Coverage measurement (Phase 1 uses a sample of this dict)

This module reads from `ai-content/corpus/responses/` so the user's
parallel pipeline run only affects the next extraction run — current
extractions snapshot the data at read time.
"""
from __future__ import annotations

import json
import logging
import os
import string
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from .normalize import slug

logger = logging.getLogger(__name__)

# Punctuation stripped from token edges before normalization. Includes
# Arabic comma/semicolon/question mark plus the common Latin set that
# appears in chunk arabic_text (LLM sometimes punctuates).
_TRIM_CHARS = "،؛؟" + string.punctuation + "«»“”‘’"


def _iter_chunks_in_response(response_path: Path) -> List[Dict]:
    """Read a response file and yield its chunks list.

    Returns empty list if the file is unreadable or has no chunks.
    """
    try:
        with open(response_path, "r", encoding="utf-8") as f:
            wrapper = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Skipping unreadable response %s: %s", response_path, e)
        return []
    result = wrapper.get("result") or {}
    if not isinstance(result, dict):
        return []
    chunks = result.get("chunks") or []
    if not isinstance(chunks, list):
        return []
    return chunks


def _verse_path_for_response(wrapper: Dict, response_filename: str) -> str:
    """Derive the verse_path used in URLs from a response wrapper.

    Falls back to constructing from the filename if the wrapper lacks
    an explicit verse_path field.
    """
    vp = wrapper.get("verse_path")
    if vp:
        return vp
    # Filenames like "al-kafi_1_2_3_4.json" → "/books/al-kafi:1:2:3:4"
    base = Path(response_filename).stem
    parts = base.split("_")
    if len(parts) < 2:
        return f"/books/{base}"
    book = parts[0]
    idx = ":".join(parts[1:])
    return f"/books/{book}:{idx}"


def tokenize_chunk_text(arabic_text: str) -> List[str]:
    """Split a chunk's arabic_text into surface tokens.

    Whitespace-split + edge-punctuation strip. Each token is then
    NFC-normalized via :func:`slug`. Empty tokens are filtered.

    Returns the cleaned list of surface forms in order.
    """
    if not arabic_text:
        return []
    out = []
    for tok in arabic_text.split():
        cleaned = tok.strip(_TRIM_CHARS)
        if not cleaned:
            continue
        normalized = slug(cleaned)
        if normalized:
            out.append(normalized)
    return out


def _extract_surfaces_from_result(result: Dict) -> List[str]:
    """Extract surface forms from a result wrapper.

    Handles both formats:
    - **v4 (chunks-canonical):** chunks[].arabic_text is the source;
      split on whitespace, trim punctuation, NFC-normalize.
    - **v3 (word_analysis-canonical, lean-stripped):** word_analysis[*].word
      is the source; each entry's word IS already a surface form.

    Selection rule: if chunks have any non-empty ``arabic_text``, use
    chunks (v4 canonical). Otherwise fall back to word_analysis.
    Returns NFC-normalized surface forms in order, de-dup is the
    caller's responsibility.
    """
    chunks = result.get("chunks") or []
    # Prefer v4 chunks-canonical when arabic_text is populated
    v4_text_available = False
    if isinstance(chunks, list) and chunks:
        for c in chunks:
            if isinstance(c, dict) and (c.get("arabic_text") or "").strip():
                v4_text_available = True
                break

    if v4_text_available:
        out = []
        for c in chunks:
            if not isinstance(c, dict):
                continue
            text = c.get("arabic_text") or ""
            out.extend(tokenize_chunk_text(text))
        return out

    # Fall back to word_analysis (v3 canonical)
    word_analysis = result.get("word_analysis") or []
    if isinstance(word_analysis, list):
        out = []
        for entry in word_analysis:
            if not isinstance(entry, dict):
                continue
            word = entry.get("word") or ""
            cleaned = word.strip(_TRIM_CHARS)
            if not cleaned:
                continue
            normalized = slug(cleaned)
            if normalized:
                out.append(normalized)
        return out

    return []


def extract_corpus_surface_set(
    responses_dir: str,
    *,
    include_filter: Optional[List[str]] = None,
) -> Dict[str, Dict]:
    """Walk all corpus responses and produce the canonical surface set.

    Handles both v3 (word_analysis canonical) and v4 (chunks.arabic_text
    canonical) response formats — selection happens per-file based on
    which field has populated content.

    Args:
        responses_dir: Path to a directory containing wrapper JSONs
            (typically ``ThaqalaynDataSources/ai-content/corpus/responses``).
        include_filter: Optional list of verse_path prefixes to include
            (e.g. ``["/books/al-kafi:1:1"]``). Default: include all.

    Returns:
        Dict ``{surface_form: {"count": int, "paths": [verse_path, ...]}}``.
        Surface forms are NFC-normalized. Paths are sorted and de-duped
        per surface.
    """
    if not os.path.isdir(responses_dir):
        logger.warning("Responses dir not found: %s", responses_dir)
        return {}

    surface_to_counts: Dict[str, int] = defaultdict(int)
    surface_to_paths: Dict[str, set] = defaultdict(set)

    processed = 0
    no_surfaces = 0

    for fname in sorted(os.listdir(responses_dir)):
        if not fname.endswith(".json"):
            continue
        path = Path(responses_dir) / fname
        try:
            with open(path, "r", encoding="utf-8") as f:
                wrapper = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping unreadable response %s: %s", path, e)
            continue

        result = wrapper.get("result") or {}
        if not isinstance(result, dict):
            continue

        verse_path = _verse_path_for_response(wrapper, fname)

        if include_filter and not any(verse_path.startswith(p) for p in include_filter):
            continue

        surfaces = _extract_surfaces_from_result(result)
        if not surfaces:
            no_surfaces += 1
            continue

        for surface in surfaces:
            surface_to_counts[surface] += 1
            surface_to_paths[surface].add(verse_path)

        processed += 1

    logger.info(
        "Extracted from %d responses (%d with no surfaces); %d unique surfaces",
        processed,
        no_surfaces,
        len(surface_to_counts),
    )

    # Materialize result with sorted-paths lists
    return {
        surface: {
            "count": surface_to_counts[surface],
            "paths": sorted(surface_to_paths[surface]),
        }
        for surface in sorted(surface_to_counts.keys())
    }


def write_corpus_surface_set(
    surface_set: Dict[str, Dict],
    output_path: str,
) -> None:
    """Persist a corpus_surface_set to JSON.

    Args:
        surface_set: Output of :func:`extract_corpus_surface_set`.
        output_path: Where to write the JSON. Parent dir created if
            missing.
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(surface_set, f, ensure_ascii=False, indent=2, sort_keys=True)


def load_corpus_surface_set(path: str) -> Dict[str, Dict]:
    """Read a corpus_surface_set previously written by
    :func:`write_corpus_surface_set`."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def summary_stats(surface_set: Dict[str, Dict]) -> Dict[str, int]:
    """Compute summary stats from a corpus_surface_set."""
    if not surface_set:
        return {"unique_surfaces": 0, "total_tokens": 0}
    return {
        "unique_surfaces": len(surface_set),
        "total_tokens": sum(s["count"] for s in surface_set.values()),
        "max_freq": max(s["count"] for s in surface_set.values()),
        "min_freq": min(s["count"] for s in surface_set.values()),
        "surfaces_appearing_once": sum(1 for s in surface_set.values() if s["count"] == 1),
        "surfaces_appearing_10_plus": sum(1 for s in surface_set.values() if s["count"] >= 10),
    }
