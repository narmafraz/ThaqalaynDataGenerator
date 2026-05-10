#!/usr/bin/env python3
"""Audit enrich_key_terms behaviour against the live corpus.

Question: enrich_key_terms (programmatic_enrichment.py:288) filters
word_tags by POS to pick "content words". Phase 2 always writes the
placeholder "N" so every word passes the POS filter — the function
degenerates to "first N non-stop-word words that have a dictionary
entry under (word, 'N')". This script measures what it actually
produces in production and compares to Phase 1's LLM-emitted key_terms.

What we want to know:
  1. How often does Phase 2's enrich_key_terms add terms to the
     final merged key_terms (i.e., how often is the LLM's output
     incomplete and Phase 2 fills gaps)?
  2. When it does add, are the additions semantically useful or
     just frequent-word noise?
  3. What fraction of corpus key_terms are LLM-emitted vs Phase 2
     supplemented?

Approach: sample N v4 responses. For each, recreate the word_tags
that would have been built at generation time (whitespace-split on
chunks[].arabic_text with placeholder "N"), then run enrich_key_terms
against the canonical word dictionary. Compare against the response's
existing Phase 1 LLM key_terms in translations.en.key_terms.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.pipeline_cli.programmatic_enrichment import enrich_key_terms  # noqa: E402
from app.ai_pipeline import load_word_dictionary  # noqa: E402

RESPONSES_DIR = PROJECT_ROOT / ".." / "ThaqalaynDataSources" / "ai-content" / "corpus" / "responses"


def adapt_loaded_dict_to_lookup_shape(loaded):
    """The production word_dictionary.json is a list-of-objects:
        {"words": [{"ar": "وَ", "pos": "CONJ", "en": "and", "ur": "اور", ...}, ...]}
    But enrich_key_terms expects a flat dict keyed by "word|POS":
        {"وَ|CONJ": {"en": "and", "ur": "اور", ...}, ...}
    This converts the former to the latter. Without this conversion the
    function silently returns {} for every verse — which is what production
    has been doing for the entire phased-pipeline lifetime.
    """
    if not isinstance(loaded, dict):
        return {}
    raw = loaded.get("words")
    if not isinstance(raw, list):
        return loaded if isinstance(raw, dict) else {}
    out = {}
    metadata_keys = {"ar", "diacritized", "pos", "notes"}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        ar = entry.get("ar") or entry.get("diacritized")
        pos = entry.get("pos")
        if not ar or not pos:
            continue
        translations = {k: v for k, v in entry.items()
                        if k not in metadata_keys and isinstance(v, str)}
        out[f"{ar}|{pos}"] = translations
        # Also key the diacritized form if different (so word-tag lookups match)
        if entry.get("diacritized") and entry["diacritized"] != ar:
            out[f"{entry['diacritized']}|{pos}"] = translations
    return out

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def reconstruct_word_tags(chunks):
    """Whitespace-split chunks' arabic_text and tag every word as 'N'.

    Mirrors what Phase 2's reconstruct_from_chunks does. We use this
    instead of the persisted word_tags because that field has been
    stripped from most DataSources responses post-#5.
    """
    out = []
    for c in chunks:
        if not isinstance(c, dict):
            continue
        text = c.get("arabic_text") or ""
        for w in text.split():
            out.append([w, "N"])
    return out


def is_v4(result):
    return "word_analysis" not in result and "chunks" in result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=30,
                        help="Number of v4 responses to sample (default 30)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    raw_dict = load_word_dictionary()
    word_dict = adapt_loaded_dict_to_lookup_shape(raw_dict)
    print(f"Raw dict has 'words' as: {type((raw_dict or {}).get('words')).__name__}")
    print(f"Adapted dict (word|POS keys): {len(word_dict)} entries")
    print()

    if not RESPONSES_DIR.is_dir():
        print(f"Responses dir not found: {RESPONSES_DIR}")
        sys.exit(1)

    files = sorted(RESPONSES_DIR.glob("*.json"))
    random.seed(args.seed)
    random.shuffle(files)

    # Cumulative counters
    v4_examined = 0
    files_phase2_added_anything = 0
    files_phase2_added_useful = 0  # (subjective — needs manual review)
    phase1_term_count_total = 0
    phase2_term_count_total = 0  # what enrich_key_terms WOULD return
    phase2_added_after_merge_total = 0

    examples_added = []  # tuples: (path, phase1_terms, phase2_terms, phase2_added_after_merge)
    examples_overlap = []  # phase 2 terms that were already in phase 1
    examples_empty_phase1 = []  # rare cases where Phase 1 emitted nothing

    for fp in files:
        if v4_examined >= args.sample:
            break
        try:
            with open(fp, "r", encoding="utf-8") as f:
                wrapper = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        result = wrapper.get("result")
        if not isinstance(result, dict):
            continue
        if not is_v4(result):
            continue
        chunks = result.get("chunks") or []
        if not chunks:
            continue

        v4_examined += 1

        word_tags = reconstruct_word_tags(chunks)
        phase2_terms = enrich_key_terms(word_tags, word_dict)
        phase2_en = phase2_terms.get("en", {})

        translations = result.get("translations") or {}
        en_block = translations.get("en") or {}
        phase1_en = en_block.get("key_terms") or {}

        phase1_term_count_total += len(phase1_en)
        phase2_term_count_total += len(phase2_en)

        # Simulate the merger logic (programmatic_enrich):
        # if phase1 already has key_terms, phase2 only fills gaps
        added_by_phase2 = {}
        if phase1_en:
            for ar_term, trans in phase2_en.items():
                if ar_term not in phase1_en:
                    added_by_phase2[ar_term] = trans
        else:
            added_by_phase2 = dict(phase2_en)

        phase2_added_after_merge_total += len(added_by_phase2)

        if added_by_phase2:
            files_phase2_added_anything += 1
            if len(examples_added) < 8:
                examples_added.append((fp.name, dict(phase1_en), dict(phase2_en), added_by_phase2))

        if not phase1_en and v4_examined and len(examples_empty_phase1) < 3:
            examples_empty_phase1.append((fp.name, dict(phase2_en)))

        if phase1_en and phase2_en:
            overlap = set(phase1_en.keys()) & set(phase2_en.keys())
            if overlap and len(examples_overlap) < 5:
                examples_overlap.append((fp.name, sorted(overlap)[:5]))

    print(f"=== Sampled v4 responses: {v4_examined} ===")
    print()
    print(f"Phase 1 LLM key_terms (en) per verse: avg {phase1_term_count_total / max(v4_examined, 1):.2f}")
    print(f"Phase 2 enrich_key_terms (en) per verse: avg {phase2_term_count_total / max(v4_examined, 1):.2f}")
    print(f"After merge (Phase 1 + Phase 2 gap fill): Phase 2 added {phase2_added_after_merge_total} new terms total")
    print(f"  files where Phase 2 added at least one new term: {files_phase2_added_anything} / {v4_examined} ({files_phase2_added_anything / max(v4_examined, 1) * 100:.1f}%)")
    print()
    print("=== Sample: verses where Phase 2 added something not already in Phase 1 ===")
    for path, p1, p2, added in examples_added:
        print(f"  {path}")
        print(f"    Phase 1 ({len(p1)}): {list(p1.items())[:5]}")
        print(f"    Phase 2 ({len(p2)}): {list(p2.items())[:5]}")
        print(f"    Added by Phase 2 ({len(added)}): {list(added.items())[:5]}")
        print()
    print("=== Sample: verses where Phase 1 emitted no key_terms (Phase 2 fills) ===")
    for path, p2 in examples_empty_phase1:
        print(f"  {path}")
        print(f"    Phase 2 ({len(p2)}): {list(p2.items())[:5]}")
        print()
    print("=== Sample: Phase 1 / Phase 2 term overlaps ===")
    for path, terms in examples_overlap:
        print(f"  {path}: {terms}")


if __name__ == "__main__":
    main()
