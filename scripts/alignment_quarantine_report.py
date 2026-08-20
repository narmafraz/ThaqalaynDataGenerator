"""Summarize align-scraped artifacts: pass rates + quarantine-reason taxonomy.

Usage:  python scripts/alignment_quarantine_report.py [--dir PATH] [--samples N]
"""
import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "..", "ThaqalaynDataSources", "ai-content", "corpus", "chunk_alignment",
)


def classify(reason: str) -> str:
    r = reason.lower()
    if "api error" in r:
        return "api-error"
    if "parse fail" in r:
        return "json-parse-fail"
    if "wrong part count" in r:
        return "wrong-part-count"
    if "not verbatim" in r:
        # sub-classify: missing chars vs same-length reorder
        m = re.search(r"orig (\d+) chars, got (\d+)", reason)
        if m:
            o, g = int(m.group(1)), int(m.group(2))
            if o == g:
                return "not-verbatim: same-length (reorder/substitution)"
            return "not-verbatim: " + ("chars dropped" if g < o else "chars added")
        return "not-verbatim: other"
    if "strict revalidation" in r:
        return "legacy-strict-revalidation"
    if "exhausted" in r:
        return "exhausted"
    return "other: " + reason[:40]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=DEFAULT_DIR)
    ap.add_argument("--samples", type=int, default=3,
                    help="example reasons to print per category")
    args = ap.parse_args()

    files = aligned = quarantined = 0
    per_book_ok = Counter()
    per_book_bad = Counter()
    per_tid_ok = Counter()
    per_tid_bad = Counter()
    cats = Counter()
    examples = defaultdict(list)

    for fname in sorted(os.listdir(args.dir)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(args.dir, fname), encoding="utf-8") as f:
            art = json.load(f)
        files += 1
        book = (art.get("verse_path") or "/books/?").split("/")[2].split(":")[0]
        for tid in (art.get("aligned") or {}):
            aligned += 1
            per_book_ok[book] += 1
            per_tid_ok[tid] += 1
        for tid, reason in (art.get("quarantined") or {}).items():
            quarantined += 1
            per_book_bad[book] += 1
            per_tid_bad[tid] += 1
            cat = classify(reason)
            cats[cat] += 1
            if len(examples[cat]) < args.samples:
                examples[cat].append(f"{fname} [{tid}]: {reason[:160]}")

    total = aligned + quarantined
    print(f"artifacts: {files} verses | translation-ids: {total} "
          f"({aligned} aligned = {aligned / max(1, total):.1%}, "
          f"{quarantined} quarantined)")

    print("\nper book (ok / quarantined):")
    for b in sorted(set(per_book_ok) | set(per_book_bad)):
        print(f"  {b:24s} {per_book_ok[b]:5d} / {per_book_bad[b]:d}")

    print("\nper translation id (ok / quarantined):")
    for t in sorted(set(per_tid_ok) | set(per_tid_bad)):
        print(f"  {t:36s} {per_tid_ok[t]:5d} / {per_tid_bad[t]:d}")

    if cats:
        print("\nquarantine taxonomy:")
        for cat, n in cats.most_common():
            print(f"  {n:5d}  {cat}")
            for ex in examples[cat]:
                print(f"         e.g. {ex}")


if __name__ == "__main__":
    main()
