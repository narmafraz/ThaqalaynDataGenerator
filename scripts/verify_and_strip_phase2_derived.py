#!/usr/bin/env python3
"""Verify-and-strip Phase 2 derived fields from v4 AI response files.

The phased pipeline's Phase 2 (programmatic_enrichment.py) generates several
fields by deterministic derivation from Phase 1's chunks. Persisting them in
ThaqalaynDataSources/ai-content/responses/ duplicates data that the merger
can re-derive at merge time — it bloats DataSources and the merged
ThaqalaynData.

This script verifies, **per file**, that each candidate field can be
exactly reconstructed from the LLM-generated chunks[].arabic_text. Only
when the stored value matches the reconstructed value byte-for-byte
does the field get removed.

Candidate fields (v4 only — v3's word_analysis is left untouched):

    diacritized_text       == " ".join(chunks[i].arabic_text for i in range(N))
    word_tags              == [[w, "N"] for c in chunks for w in c.arabic_text.split()]
    isnad_matn.isnad_ar    == concat of chunks where chunk_type == "isnad"
    isnad_matn.matn_ar     == concat of chunks where chunk_type != "isnad"

What's INTENTIONALLY left in:

  - word_analysis (v3 only; rich per-word translations from LLM)
  - isnad_matn.narrators (depends on registry state at generation time)
  - key_terms (depends on word dictionary, not pure derivation)
  - topics (depends on POS tags)
  - diacritics_changes (Phase 1 LLM output, not Phase 2 derivation)
  - chunks[].arabic_text (the canonical source — what we reconstruct FROM)

Usage:
    # Dry run on a single file (verify only — see what would happen)
    .venv/Scripts/python.exe scripts/verify_and_strip_phase2_derived.py \\
        --single ai-content/corpus/responses/man-la-yahduruhu-al-faqih_3_2_21_88.json

    # Dry run on a random sample of N files
    .venv/Scripts/python.exe scripts/verify_and_strip_phase2_derived.py --sample 10

    # Dry run across the entire corpus (no changes)
    .venv/Scripts/python.exe scripts/verify_and_strip_phase2_derived.py

    # Apply changes (writes modified files in-place)
    .venv/Scripts/python.exe scripts/verify_and_strip_phase2_derived.py --apply

The default mode is always dry-run. --apply is required to write anything.
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
# Reconstruction functions — single source of truth for what each derived
# field should look like, given chunks[].arabic_text as the canonical input.
# ---------------------------------------------------------------------------

def reconstruct_diacritized_text(chunks: List[dict]) -> str:
    """Phase 2's logic in programmatic_enrichment.reconstruct_from_chunks()
    builds diacritized_text by joining all word_tags entries with spaces:

        diacritized_text = " ".join(wt[0] for wt in all_words)

    where all_words is the concatenation of every chunk's words. So:

        diacritized_text = " ".join(w for c in chunks for w in c.arabic_text.split())

    This collapses any internal whitespace differences in chunks (e.g. a
    chunk might have "word1  word2" with a double space) since split()
    treats any whitespace run as one separator. That's the same behavior
    as Phase 2's reconstruct_from_chunks. Match against this exactly.
    """
    words = []
    for c in chunks:
        text = c.get("arabic_text", "") or ""
        words.extend(text.split())
    return " ".join(words)


def reconstruct_word_tags(chunks: List[dict]) -> List[list]:
    """Phase 2 builds word_tags as [[word, "N"], ...] over all chunk words.

    Per programmatic_enrichment.reconstruct_from_chunks: each chunk's
    arabic_text.split() gets [word, "N"] pairs appended, and chunk.word_start
    / chunk.word_end are set to the global offsets.
    """
    out = []
    for c in chunks:
        text = c.get("arabic_text", "") or ""
        for w in text.split():
            out.append([w, "N"])
    return out


def reconstruct_isnad_ar(chunks: List[dict]) -> str:
    """Concatenation of arabic_text from chunks where chunk_type == 'isnad'.

    Per programmatic_enrichment.reconstruct_isnad_matn:
        isnad_parts = [c.arabic_text for c in chunks if c.chunk_type == "isnad"]
        isnad_ar = " ".join(isnad_parts) if multiple, else ""

    The actual implementation joins parts with a space when concatenating
    multiple isnad chunks. Match exactly.
    """
    parts = [
        (c.get("arabic_text") or "")
        for c in chunks
        if c.get("chunk_type") == "isnad"
    ]
    return " ".join(p for p in parts if p)


def reconstruct_matn_ar(chunks: List[dict]) -> str:
    """Concatenation of arabic_text from chunks where chunk_type != 'isnad'."""
    parts = [
        (c.get("arabic_text") or "")
        for c in chunks
        if c.get("chunk_type") != "isnad"
    ]
    return " ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Verification logic
# ---------------------------------------------------------------------------

def is_v4_response(result: dict) -> bool:
    """v4 has word_tags but not word_analysis. v3 has word_analysis."""
    return "word_tags" in result and "word_analysis" not in result


def verify_field(
    stored: object,
    reconstructed: object,
    field_name: str,
) -> Tuple[bool, Optional[str]]:
    """Return (matches, reason_if_not).

    A non-match means we should NOT strip — there's content the
    reconstruction can't produce, so something is non-derived.
    """
    if stored == reconstructed:
        return True, None
    # Not a byte-exact match — diagnose
    if isinstance(stored, str) and isinstance(reconstructed, str):
        if stored.strip() == reconstructed.strip():
            return False, f"{field_name}: differs only in leading/trailing whitespace"
        if len(stored) != len(reconstructed):
            return False, f"{field_name}: length differs ({len(stored)} stored vs {len(reconstructed)} reconstructed)"
        # Find first diff index
        for i, (a, b) in enumerate(zip(stored, reconstructed)):
            if a != b:
                ctx = max(0, i - 10)
                return False, (
                    f"{field_name}: differs at index {i}, "
                    f"stored=...{stored[ctx:i+10]!r}, "
                    f"reconstructed=...{reconstructed[ctx:i+10]!r}"
                )
        return False, f"{field_name}: differs (string equality fail, no per-char diff found)"
    if isinstance(stored, list) and isinstance(reconstructed, list):
        if len(stored) != len(reconstructed):
            return False, f"{field_name}: length differs ({len(stored)} vs {len(reconstructed)})"
        for i, (a, b) in enumerate(zip(stored, reconstructed)):
            if a != b:
                return False, f"{field_name}: differs at index {i}: stored={a!r}, reconstructed={b!r}"
        return False, f"{field_name}: list equality fail"
    return False, f"{field_name}: type mismatch ({type(stored).__name__} vs {type(reconstructed).__name__})"


def verify_response(result: dict) -> Dict[str, Tuple[bool, Optional[str]]]:
    """Return a dict mapping field_name -> (match_ok, reason_if_not).

    Only reports on fields we'd consider stripping. Caller decides what
    to do based on the booleans.
    """
    chunks = result.get("chunks") or []
    out: Dict[str, Tuple[bool, Optional[str]]] = {}

    if "diacritized_text" in result:
        out["diacritized_text"] = verify_field(
            result["diacritized_text"],
            reconstruct_diacritized_text(chunks),
            "diacritized_text",
        )

    if "word_tags" in result:
        out["word_tags"] = verify_field(
            result["word_tags"],
            reconstruct_word_tags(chunks),
            "word_tags",
        )

    isnad_matn = result.get("isnad_matn") or {}
    if isinstance(isnad_matn, dict):
        if "isnad_ar" in isnad_matn:
            out["isnad_matn.isnad_ar"] = verify_field(
                isnad_matn["isnad_ar"],
                reconstruct_isnad_ar(chunks),
                "isnad_matn.isnad_ar",
            )
        if "matn_ar" in isnad_matn:
            out["isnad_matn.matn_ar"] = verify_field(
                isnad_matn["matn_ar"],
                reconstruct_matn_ar(chunks),
                "isnad_matn.matn_ar",
            )

    return out


def strip_verified_fields(result: dict, verifications: Dict[str, Tuple[bool, Optional[str]]]) -> List[str]:
    """Mutate result in-place, removing fields whose verification passed.

    Returns the list of field names that were stripped.
    """
    stripped = []
    if verifications.get("diacritized_text", (False,))[0]:
        result.pop("diacritized_text", None)
        stripped.append("diacritized_text")
    if verifications.get("word_tags", (False,))[0]:
        result.pop("word_tags", None)
        stripped.append("word_tags")
    isnad_matn = result.get("isnad_matn")
    if isinstance(isnad_matn, dict):
        if verifications.get("isnad_matn.isnad_ar", (False,))[0]:
            isnad_matn.pop("isnad_ar", None)
            stripped.append("isnad_matn.isnad_ar")
        if verifications.get("isnad_matn.matn_ar", (False,))[0]:
            isnad_matn.pop("matn_ar", None)
            stripped.append("isnad_matn.matn_ar")
    return stripped


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def list_response_files() -> List[Path]:
    if not RESPONSES_DIR.is_dir():
        return []
    return sorted(RESPONSES_DIR.glob("*.json"))


def process_file(path: Path, apply: bool, verbose: bool) -> Dict[str, object]:
    """Process one file. Returns a summary dict."""
    summary: Dict[str, object] = {
        "path": path.name,
        "skipped_reason": None,
        "verified": [],
        "mismatched": [],
        "stripped": [],
    }
    try:
        with open(path, "r", encoding="utf-8") as f:
            wrapper = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        summary["skipped_reason"] = f"unreadable: {e}"
        return summary

    result = wrapper.get("result")
    if not isinstance(result, dict):
        summary["skipped_reason"] = "no result dict"
        return summary

    if not is_v4_response(result):
        summary["skipped_reason"] = "v3 (word_analysis) — left untouched"
        return summary

    verifications = verify_response(result)
    if not verifications:
        summary["skipped_reason"] = "no candidate fields present"
        return summary

    for field, (ok, reason) in verifications.items():
        if ok:
            summary["verified"].append(field)
        else:
            summary["mismatched"].append({"field": field, "reason": reason})

    if verbose:
        print(f"  {path.name}: verified={summary['verified']}, mismatched={[m['field'] for m in summary['mismatched']]}")
        for m in summary["mismatched"]:
            print(f"    ✗ {m['reason']}")

    if apply and summary["verified"]:
        stripped = strip_verified_fields(result, verifications)
        summary["stripped"] = stripped
        if stripped:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(wrapper, f, ensure_ascii=False, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--single", type=Path,
                        help="Process a single response file (path relative to repo root or absolute).")
    parser.add_argument("--sample", type=int, default=None,
                        help="Process N random files instead of all.")
    parser.add_argument("--apply", action="store_true",
                        help="Write changes to disk. Default: dry run.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Per-file output (otherwise just a summary).")
    args = parser.parse_args()

    print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"Responses dir: {RESPONSES_DIR.resolve()}")

    if args.single:
        candidates = [args.single]
        if not args.single.is_absolute():
            # Try a few sensible bases for relative paths
            candidates.extend([
                (PROJECT_ROOT / args.single).resolve(),
                (PROJECT_ROOT / ".." / args.single).resolve(),
                (RESPONSES_DIR / args.single.name).resolve(),
            ])
        path = next((c for c in candidates if c.is_file()), None)
        if not path:
            print(f"ERROR: file not found. Tried: {[str(c) for c in candidates]}")
            sys.exit(1)
        files = [path]
    else:
        files = list_response_files()
        if args.sample and args.sample < len(files):
            random.seed(42)
            files = random.sample(files, args.sample)

    print(f"Processing {len(files)} file(s).\n")

    summaries = [process_file(f, apply=args.apply, verbose=args.verbose) for f in files]

    # Aggregate
    skipped: Counter = Counter()
    verified_counts: Counter = Counter()
    mismatch_counts: Counter = Counter()
    stripped_counts: Counter = Counter()
    files_with_strips = 0

    for s in summaries:
        if s["skipped_reason"]:
            skipped[s["skipped_reason"]] += 1
            continue
        for f in s["verified"]:
            verified_counts[f] += 1
        for m in s["mismatched"]:
            mismatch_counts[m["field"]] += 1
        if s["stripped"]:
            files_with_strips += 1
            for f in s["stripped"]:
                stripped_counts[f] += 1

    print("=== Summary ===")
    if skipped:
        print("Skipped:")
        for reason, n in skipped.most_common():
            print(f"  {n:5}  {reason}")
    print()
    print("Verified (would strip on --apply):")
    for f, n in verified_counts.most_common():
        print(f"  {n:5}  {f}")
    print()
    if mismatch_counts:
        print("Mismatched (will NOT strip — needs review):")
        for f, n in mismatch_counts.most_common():
            print(f"  {n:5}  {f}")
        print()
        # Print first few mismatch reasons for triage
        print("Sample mismatch reasons:")
        seen = 0
        for s in summaries:
            for m in s["mismatched"]:
                print(f"  {s['path']}  {m['reason'][:120]}")
                seen += 1
                if seen >= 8:
                    break
            if seen >= 8:
                break
        print()
    if args.apply:
        print(f"Files modified: {files_with_strips}")
        print("Stripped fields (totals across files):")
        for f, n in stripped_counts.most_common():
            print(f"  {n:5}  {f}")
    else:
        print("(dry-run — nothing written to disk)")


if __name__ == "__main__":
    main()
