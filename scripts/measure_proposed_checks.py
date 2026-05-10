#!/usr/bin/env python3
"""Measure flag rate of proposed review checks #15 and #17 against the corpus.

Before adding new validation gates that could quarantine real content,
measure how often each check would fire on existing accepted responses.
A high flag rate means the check has many false positives (the existing
corpus is already accepted, so a fresh flag is FP-suspect).

Checks measured:

  #15 Chunk-translation length sanity
      For each chunk:
        - If Arabic word count N >= 3, the chunk's en translation should
          have between 0.4*N and 4*N words.
        - For shorter chunks, looser bounds: 0.25*N to 6*N.
      Skip chunks with empty arabic_text or empty translation.
      We only run this on English (Phase 4 generates other languages
      from English, so en is the canonical source-of-quality).

  #17 Chunk-type plausibility
      For each chunk:
        a. If chunk_type == "isnad": at least one narrator's name_ar
           (or its first whitespace-separated word) must appear within
           the chunk's arabic_text. Otherwise flag.
        b. If chunk_type != "isnad" AND it's NOT the verse's only chunk:
           chunk's arabic_text should NOT start with a chain verb
           (روى, حدثني, أخبرني, عن, …). If it does, flag.

      Caveats:
        - We strip diacritics for comparison (LLM may have different
          diacritization between narrators[].name_ar and chunks[].arabic_text).
        - Single-chunk verses with the chain inline are not flagged for
          chain-verb-in-non-isnad.

Usage:
  .venv/Scripts/python.exe scripts/measure_proposed_checks.py [--sample N] [--show K]

Defaults: full corpus, show 5 samples per failure category.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RESPONSES_DIR = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-content" / "corpus" / "responses"

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Diacritic stripping (matches the existing review utilities)
# ---------------------------------------------------------------------------

_DIACRITIC_MARKS = set("ًٌٍَُِّْٰـ")

# Alif variants — needed because chunks often use ALEF WASLA (ٱ) where
# narrator names use plain ALEF (ا). Without normalization we get false
# positives on the substring match.
_ALIF_VARIANTS = "أإآٱا"
_YA_VARIANTS = "يى"


def normalize_arabic(s: str) -> str:
    """Strip diacritics + normalize alif/ya variants for surface comparison."""
    out = []
    for ch in s:
        if ch in _DIACRITIC_MARKS:
            continue
        if ch in _ALIF_VARIANTS:
            out.append("ا")
        elif ch in _YA_VARIANTS:
            out.append("ي")
        else:
            out.append(ch)
    return "".join(out)


def strip_diacritics(s: str) -> str:
    """Backwards-compat alias kept for any callers; just calls normalize."""
    return normalize_arabic(s)


# ---------------------------------------------------------------------------
# Check #15 — chunk translation length sanity
# ---------------------------------------------------------------------------

def check_chunk_translation_length(
    chunks: List[dict],
) -> List[dict]:
    """Flag chunks whose English translation length is out of range.

    Returns a list of flag dicts: {chunk_idx, ar_words, en_words, ratio,
    bound_low, bound_high, ar_text, en_text}.
    """
    flags = []
    for i, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            continue
        ar = (chunk.get("arabic_text") or "").strip()
        if not ar:
            continue
        translations = chunk.get("translations") or {}
        en = (translations.get("en") or "").strip()
        if not en:
            continue  # empty handled by existing review check; skip here

        ar_words = len(ar.split())
        en_words = len(en.split())
        if ar_words == 0 or en_words == 0:
            continue

        # Bounds: tighter for long chunks, looser for short ones
        if ar_words >= 3:
            low, high = 0.4 * ar_words, 4.0 * ar_words
        else:
            low, high = 0.25 * ar_words, 6.0 * ar_words

        if en_words < low or en_words > high:
            flags.append({
                "chunk_idx": i,
                "chunk_type": chunk.get("chunk_type"),
                "ar_words": ar_words,
                "en_words": en_words,
                "ratio": round(en_words / ar_words, 2),
                "bounds": (round(low, 1), round(high, 1)),
                "direction": "short" if en_words < low else "long",
                "ar_text": ar[:80],
                "en_text": en[:80],
            })
    return flags


# ---------------------------------------------------------------------------
# Check #17 — chunk-type plausibility
# ---------------------------------------------------------------------------

# Chain-verb tokens that typically open an isnad. Stripped of diacritics.
# Includes both standalone verbs and prepositions used to chain narrators.
_CHAIN_OPENERS = {
    "روى", "رواه", "روي",
    "حدثنا", "حدثني", "اخبرنا", "اخبرني",
    "عن", "عنه",
    "قال",  # ambiguous — common in matn too, treat carefully
}

_CHAIN_OPENERS_STRICT = {
    "روى", "رواه", "روي",
    "حدثنا", "حدثني", "اخبرنا", "اخبرني",
}


def check_chunk_type_plausibility(
    chunks: List[dict],
    narrators: List[dict],
) -> Tuple[List[dict], List[dict]]:
    """Return (isnad_no_narrator_flags, matn_starts_with_chain_flags).

    isnad_no_narrator: chunk_type=="isnad" but no narrator name_ar appears.
    matn_starts_with_chain: chunk_type!="isnad" but text starts with a
        strict chain opener (روى/حدثنا/اخبرنا family).
    """
    # Build narrator surface-form set: name_ar + first word of name_ar.
    # Both diacritic-stripped for comparison.
    narrator_surfaces: set = set()
    for n in narrators:
        if not isinstance(n, dict):
            continue
        name = (n.get("name_ar") or "").strip()
        if not name:
            continue
        bare = strip_diacritics(name)
        narrator_surfaces.add(bare)
        first = bare.split()[0] if bare.split() else ""
        if len(first) >= 3:  # avoid single-letter or very short tokens
            narrator_surfaces.add(first)

    isnad_flags: List[dict] = []
    matn_flags: List[dict] = []

    for i, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            continue
        text = (chunk.get("arabic_text") or "").strip()
        if not text:
            continue
        bare_text = strip_diacritics(text)
        ctype = chunk.get("chunk_type")

        if ctype == "isnad":
            # Must contain at least one narrator surface
            if narrator_surfaces:
                hit = any(surf in bare_text for surf in narrator_surfaces)
                if not hit:
                    isnad_flags.append({
                        "chunk_idx": i,
                        "ar_text": text[:80],
                        "narrator_surfaces": sorted(narrator_surfaces)[:5],
                    })
            # if no narrators known, can't check — skip
        else:
            # Non-isnad chunks shouldn't start with strict chain openers.
            # Strip leading واو/conjunctions before checking.
            first_word = bare_text.split()[0] if bare_text.split() else ""
            # Drop a leading wa- prefix so "وروى" still flags as "روى"
            if first_word.startswith("و") and len(first_word) > 1:
                first_word = first_word[1:]
            if first_word in _CHAIN_OPENERS_STRICT:
                matn_flags.append({
                    "chunk_idx": i,
                    "chunk_type": ctype,
                    "first_word": first_word,
                    "ar_text": text[:80],
                })
    return isnad_flags, matn_flags


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def list_response_files() -> List[Path]:
    if not RESPONSES_DIR.is_dir():
        return []
    return sorted(RESPONSES_DIR.glob("*.json"))


def process_file(path: Path) -> dict:
    summary = {
        "path": path.name,
        "skipped": None,
        "len_flags": [],
        "isnad_no_narrator": [],
        "matn_starts_with_chain": [],
    }
    try:
        with open(path, "r", encoding="utf-8") as f:
            wrapper = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        summary["skipped"] = f"unreadable: {e}"
        return summary

    result = wrapper.get("result")
    if not isinstance(result, dict):
        summary["skipped"] = "no result dict"
        return summary

    chunks = result.get("chunks") or []
    if not chunks:
        summary["skipped"] = "no chunks"
        return summary

    summary["len_flags"] = check_chunk_translation_length(chunks)

    narrators = (result.get("isnad_matn") or {}).get("narrators") or []
    in_flags, mn_flags = check_chunk_type_plausibility(chunks, narrators)
    summary["isnad_no_narrator"] = in_flags
    summary["matn_starts_with_chain"] = mn_flags

    summary["chunk_count"] = len(chunks)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=None,
                        help="Sample N random files instead of full corpus")
    parser.add_argument("--show", type=int, default=5,
                        help="Show K sample failures per category (default 5)")
    args = parser.parse_args()

    files = list_response_files()
    print(f"Total response files in corpus: {len(files)}")
    if args.sample and args.sample < len(files):
        random.seed(42)
        files = random.sample(files, args.sample)
        print(f"Sampling {len(files)} files (seed=42)")
    print()

    total_files = 0
    skipped = Counter()
    files_with_len_flag = 0
    files_with_isnad_flag = 0
    files_with_matn_flag = 0
    total_chunks = 0

    len_flag_sample: List[Tuple[str, dict]] = []
    isnad_flag_sample: List[Tuple[str, dict]] = []
    matn_flag_sample: List[Tuple[str, dict]] = []

    len_flag_dir_counts = Counter()  # short / long

    for fp in files:
        s = process_file(fp)
        if s["skipped"]:
            skipped[s["skipped"]] += 1
            continue
        total_files += 1
        total_chunks += s.get("chunk_count", 0)

        if s["len_flags"]:
            files_with_len_flag += 1
            for f in s["len_flags"]:
                len_flag_dir_counts[f["direction"]] += 1
            if len(len_flag_sample) < args.show:
                len_flag_sample.append((s["path"], s["len_flags"][0]))

        if s["isnad_no_narrator"]:
            files_with_isnad_flag += 1
            if len(isnad_flag_sample) < args.show:
                isnad_flag_sample.append((s["path"], s["isnad_no_narrator"][0]))

        if s["matn_starts_with_chain"]:
            files_with_matn_flag += 1
            if len(matn_flag_sample) < args.show:
                matn_flag_sample.append((s["path"], s["matn_starts_with_chain"][0]))

    print("=== Summary ===")
    print(f"Files analyzed:     {total_files}")
    print(f"Total chunks:       {total_chunks}")
    if skipped:
        print("Skipped:")
        for r, n in skipped.most_common():
            print(f"  {n:>5}  {r}")
    print()
    print("=== Check #15: chunk translation length (English) ===")
    pct_len = (files_with_len_flag / total_files * 100) if total_files else 0
    print(f"Files with at least one flag: {files_with_len_flag} / {total_files} ({pct_len:.1f}%)")
    if len_flag_dir_counts:
        for direction, n in len_flag_dir_counts.most_common():
            print(f"  {n:>5}  chunks flagged as too {direction}")
    print()
    if len_flag_sample:
        print(f"Sample {len(len_flag_sample)} flagged chunks:")
        for path, f in len_flag_sample:
            print(f"  {path}")
            print(f"    chunk[{f['chunk_idx']}] type={f['chunk_type']!r}  ar={f['ar_words']}w  en={f['en_words']}w  ratio={f['ratio']}  bounds={f['bounds']}  direction={f['direction']}")
            print(f"    ar: {f['ar_text']}")
            print(f"    en: {f['en_text']}")
            print()

    print("=== Check #17a: isnad chunk without narrator surface ===")
    pct_isnad = (files_with_isnad_flag / total_files * 100) if total_files else 0
    print(f"Files with at least one flag: {files_with_isnad_flag} / {total_files} ({pct_isnad:.1f}%)")
    if isnad_flag_sample:
        print(f"Sample {len(isnad_flag_sample)} flagged chunks:")
        for path, f in isnad_flag_sample:
            print(f"  {path}")
            print(f"    chunk[{f['chunk_idx']}] ar: {f['ar_text']}")
            print(f"    expected one of: {f['narrator_surfaces']}")
            print()

    print("=== Check #17b: non-isnad chunk starting with chain opener ===")
    pct_matn = (files_with_matn_flag / total_files * 100) if total_files else 0
    print(f"Files with at least one flag: {files_with_matn_flag} / {total_files} ({pct_matn:.1f}%)")
    if matn_flag_sample:
        print(f"Sample {len(matn_flag_sample)} flagged chunks:")
        for path, f in matn_flag_sample:
            print(f"  {path}")
            print(f"    chunk[{f['chunk_idx']}] type={f['chunk_type']!r}  first_word={f['first_word']!r}")
            print(f"    ar: {f['ar_text']}")
            print()


if __name__ == "__main__":
    main()
