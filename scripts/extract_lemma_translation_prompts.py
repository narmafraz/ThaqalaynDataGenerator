"""Walk ThaqalaynWords/lemmas/*.json and emit translation-prompt JSONL.

Output line shape (one per lemma):

  {"slug": "قَالَ", "lemma_ar": "قَالَ", "pos": "V", "pos_label": "Verb",
   "pos_camel": "verb", "en_gloss": "to say, speak",
   "lane_body": "concatenated readable text…",
   "freq": 8421}

The output is what feeds `app.words.spark_translation.run_lemma_batch`.
Persisted to `ThaqalaynWordSources/translation/lemma_prompts.jsonl` so
each run can re-tune the prompt without rebuilding the source corpus.

Usage:
    python scripts/extract_lemma_translation_prompts.py
    python scripts/extract_lemma_translation_prompts.py --words-dir ../ThaqalaynWords \\
        --out ../ThaqalaynWordSources/translation/lemma_prompts.jsonl
    python scripts/extract_lemma_translation_prompts.py --pilot-set path/to/pilot.json
        # restrict to slugs listed in pilot_set.json["lemmas"]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("extract_lemma_translation_prompts")


# ────────────────── POS-aligned gloss picker ──────────────────
#
# Mirrors `scripts/build_word_indexes.py::_pick_aligned_gloss` so the
# en_gloss field sent to Spark matches what the UI already displays as
# the Path C English gloss. Kept in sync deliberately — refactor to a
# shared module if either side grows.

_POS_FAMILIES: Dict[str, set] = {
    "verb":       {"verb"},
    "noun":       {"noun", "name", "proper noun"},
    "noun_prop":  {"noun", "name", "proper noun"},
    "noun_quant": {"noun", "num"},
    "noun_num":   {"noun", "num"},
    "adj":        {"adj", "adjective"},
    "adj.act":    {"adj", "adjective", "verb"},
    "adj.pass":   {"adj", "adjective", "verb"},
    "adv":        {"adv", "adverb"},
    "prep":       {"prep", "preposition"},
    "conj":       {"conj", "conjunction"},
    "part":       {"particle", "part"},
    "particle":   {"particle", "part"},
    "pron":       {"pron", "pronoun"},
    "det":        {"det", "determiner", "article"},
    "intj":       {"intj", "interjection"},
    "fut_part":   {"particle", "part"},
    "neg_part":   {"particle", "part"},
    "interrog_part": {"particle", "part"},
    "focus_part": {"particle", "part"},
    "prog_part":  {"particle", "part"},
    "voc_part":   {"particle", "part"},
}
_CONTENT_POS = {
    "verb", "noun", "noun_prop", "noun_quant", "noun_num",
    "adj", "adj.act", "adj.pass", "adv",
}


def pick_aligned_gloss(pos_camel: str, senses: List[Dict]) -> str:
    """Return the first POS-aligned Wiktextract sense gloss, or "" if none."""
    if not senses:
        return ""
    accepted = _POS_FAMILIES.get(pos_camel or "", set())
    for s in senses:
        sp = (s.get("pos") or "").lower()
        if accepted and sp in accepted:
            g = s.get("gloss") or ""
            return g.strip()
    if pos_camel in _CONTENT_POS:
        return (senses[0].get("gloss") or "").strip()
    return ""


# ────────────────── Lane's body renderer ──────────────────


def render_classical_definitions(cd: Optional[dict], max_entries: int = 3, max_chars: int = 3000) -> str:
    """Summarise `lemmas/{slug}.json#classical_definitions` for prompt use.

    Strips HTML tags, joins the top `max_entries` lexicon entries with the
    lexicon name as a heading. Caps total length at `max_chars` so the
    input-token budget stays bounded even for high-coverage lemmas (some
    have 19+ entries / 70 KB body_html in aggregate).

    Round 1 prompt does NOT use this output. Round 2 may A/B-test enabling
    it. Captured here so the JSONL has the data ready without re-extracting.
    """
    if not cd:
        return ""
    entries = cd.get("entries") or []
    if not entries:
        return ""

    import re
    tag_re = re.compile(r"<[^>]+>")

    parts: List[str] = []
    for entry in entries[:max_entries]:
        lex = entry.get("lexicon_en") or entry.get("lexicon_ar") or "Classical lexicon"
        body_html = entry.get("body_html") or ""
        body_text = tag_re.sub(" ", body_html)
        body_text = " ".join(body_text.split())  # collapse whitespace
        if not body_text:
            continue
        parts.append(f"[{lex}] {body_text}")
        if sum(len(p) for p in parts) >= max_chars:
            break

    out = "\n".join(parts)
    return out[:max_chars]


def render_lanes_body(entries: List[Dict]) -> str:
    """Concatenate `lanes_definition.entries[*].body` to readable text.

    Segment kinds we handle:
      • text       → raw text, kept as-is
      • italic_en  → English meaning, kept as-is (style markers dropped)
      • arabic     → use the Arabic NFC form (text_ar), fall back to BW
      • quote      → "quoted text" with single quotes
      • page_break → dropped

    Multiple Lane's entries (rare — a lemma may have two senses) are
    separated by '|' between entries.
    """
    if not entries:
        return ""
    chunks: List[str] = []
    for entry in entries:
        body = entry.get("body") or []
        parts: List[str] = []
        for seg in body:
            kind = seg.get("kind")
            if kind in ("text", "italic_en"):
                t = (seg.get("text") or "").strip()
                if t:
                    parts.append(t)
            elif kind == "arabic":
                ar = seg.get("text_ar") or seg.get("text_bw") or ""
                if ar:
                    parts.append(ar)
            elif kind == "quote":
                t = (seg.get("text") or "").strip()
                if t:
                    parts.append(f"'{t}'")
            # page_break and unknown kinds intentionally skipped.
        joined = " ".join(parts)
        if joined:
            chunks.append(joined)
    return " | ".join(chunks)


# ────────────────── POS label normalisation ──────────────────

_POS_HUMAN_LABELS: Dict[str, str] = {
    "verb": "Verb",
    "noun": "Noun",
    "noun_prop": "Proper Noun",
    "noun_quant": "Noun (quantifier)",
    "noun_num": "Noun (numeral)",
    "adj": "Adjective",
    "adj.act": "Active Participle",
    "adj.pass": "Passive Participle",
    "adv": "Adverb",
    "prep": "Preposition",
    "conj": "Conjunction",
    "pron": "Pronoun",
    "det": "Determiner",
    "intj": "Interjection",
    "part": "Particle",
    "particle": "Particle",
    "fut_part": "Future particle",
    "neg_part": "Negative particle",
    "interrog_part": "Interrogative particle",
    "focus_part": "Focus particle",
    "prog_part": "Progressive particle",
    "voc_part": "Vocative particle",
    "digit": "Digit/Numeral",
}


def humanize_pos(pos_camel: str) -> str:
    return _POS_HUMAN_LABELS.get(pos_camel or "", pos_camel or "unknown")


# ────────────────── walker ──────────────────


def extract_lemma_item(data: dict) -> Optional[dict]:
    """Build the JSONL row for one lemma. Returns None to skip."""
    slug = data.get("slug")
    if not slug:
        return None

    pos_camel = data.get("pos_camel") or ""
    senses = (data.get("definition") or {}).get("senses") or []
    lanes_entries = (data.get("lanes_definition") or {}).get("entries") or []

    return {
        "slug": slug,
        "lemma_ar": data.get("lemma") or slug,
        "pos": data.get("pos") or "",
        "pos_camel": pos_camel,
        "pos_label": humanize_pos(pos_camel),
        "en_gloss": pick_aligned_gloss(pos_camel, senses),
        "lane_body": render_lanes_body(lanes_entries),
        # Captured but NOT consumed by the Round 1 prompt. Round 2 may
        # opt in via build_lemma_user_message(include_classical=True).
        "classical_summary": render_classical_definitions(
            data.get("classical_definitions")
        ),
        "freq": data.get("frequency_in_corpus", 0),
    }


def walk_lemmas(
    words_dir: Path, slug_filter: Optional[set] = None
) -> List[dict]:
    lemmas_dir = words_dir / "lemmas"
    if not lemmas_dir.is_dir():
        raise FileNotFoundError(f"No lemmas dir at {lemmas_dir}")
    items: List[dict] = []
    skipped = 0
    for p in lemmas_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("skipping %s: %s", p.name, e)
            skipped += 1
            continue
        if slug_filter is not None and (data.get("slug") not in slug_filter):
            continue
        item = extract_lemma_item(data)
        if item is None:
            skipped += 1
            continue
        items.append(item)
    if skipped:
        logger.warning("skipped %d lemma files", skipped)
    return items


# ────────────────── CLI ──────────────────


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")  # Arabic-safe console output
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--words-dir",
        type=Path,
        default=Path("../ThaqalaynWords"),
        help="Path to the ThaqalaynWords repo (default: ../ThaqalaynWords)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("../ThaqalaynWordSources/translation/lemma_prompts.jsonl"),
        help="Output JSONL path",
    )
    parser.add_argument(
        "--pilot-set",
        type=Path,
        default=None,
        help="Optional pilot_set.json restricting which lemma slugs to emit",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    slug_filter: Optional[set] = None
    if args.pilot_set is not None:
        with open(args.pilot_set, "r", encoding="utf-8") as f:
            pilot = json.load(f)
        slug_filter = set(pilot.get("lemmas") or [])
        logger.info("pilot filter active: %d lemma slugs", len(slug_filter))

    items = walk_lemmas(args.words_dir, slug_filter=slug_filter)
    logger.info("walked %d lemma items", len(items))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    logger.info("wrote %s", args.out)

    # Light reporting
    with_en = sum(1 for i in items if i["en_gloss"])
    with_lane = sum(1 for i in items if i["lane_body"])
    logger.info(
        "coverage: %d/%d have en_gloss (%.1f%%); %d/%d have lane_body (%.1f%%)",
        with_en, len(items), 100 * with_en / max(1, len(items)),
        with_lane, len(items), 100 * with_lane / max(1, len(items)),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
