#!/usr/bin/env python3
"""Audit ThaqalaynData verse files for unlinked narrators in narrator chains.

Walks every verse_detail JSON file in $DESTINATION_DIR/books, reconstructs each
verse's chain text from narrator_chain.parts, re-splits it through the same
splitter narrator_linker uses, runs the registry resolver against the current
canonical_narrators.json, and aggregates the names that fail to resolve.

The output is a frequency-ranked list of unique unresolved names with one
example verse path per name. That list is the working set for adding new
variants to the registry.

Usage (from ThaqalaynDataGenerator):
    .venv/Scripts/python.exe scripts/audit_unlinked_narrators.py \\
        --book man-la-yahduruhu-al-faqih --top 50

    .venv/Scripts/python.exe scripts/audit_unlinked_narrators.py \\
        --output unlinked_narrators.tsv

Read-only: does not touch the registry or any verse JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.narrator_linker import (  # noqa: E402
    _book_slug_from_path,
    _looks_like_isnad,
    _strip_book_preamble,
    split_narrator_names,
)
from app.narrator_registry import NarratorRegistry, canonical_lookup_key  # noqa: E402


def reconstruct_chain_text(parts: List[dict]) -> str:
    pieces = []
    for p in parts:
        text = p.get("text") or ""
        if text:
            pieces.append(text)
    return "".join(pieces)


def iter_verse_files(books_root: Path, book_filter: Optional[str]):
    """Yield every verse_detail json file under books_root.

    book_filter, if set, restricts to the matching top-level slug.
    Skips books/complete and books/*.json (book metadata files).
    """
    if book_filter:
        roots = [books_root / book_filter]
    else:
        roots = [
            d for d in books_root.iterdir()
            if d.is_dir() and d.name != "complete"
        ]

    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*.json"):
            yield path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data-dir",
        default=os.environ.get("DESTINATION_DIR", "../ThaqalaynData/"),
        help="Root of generated JSON data (default: $DESTINATION_DIR or ../ThaqalaynData/)",
    )
    ap.add_argument("--book", default=None, help="Restrict to one book slug")
    ap.add_argument("--top", type=int, default=50, help="Show top N unique names (default 50)")
    ap.add_argument("--min-words", type=int, default=2,
                    help="Drop unresolved tokens shorter than this many words (default 2)")
    ap.add_argument("--output", default=None,
                    help="Write full TSV report to this path")
    ap.add_argument("--undiacritized", action="store_true",
                    help="Use undiacritized splitter fallback (for non-Kafi books)")
    ap.add_argument(
        "--simulate-class2",
        action="store_true",
        help=(
            "Simulate the Class 2 extractor guards (_looks_like_isnad + per-book "
            "preamble strip) on reconstructed chain text. Approximates what the "
            "audit would report after process_all_narrators is re-run with the "
            "new extractor — without actually re-running it."
        ),
    )
    args = ap.parse_args()

    books_root = Path(args.data_dir).resolve() / "books"
    if not books_root.is_dir():
        print(f"[error] no books directory at {books_root}", file=sys.stderr)
        return 2

    registry = NarratorRegistry()
    if registry.narrator_count == 0:
        print("[error] empty narrator registry", file=sys.stderr)
        return 2
    print(f"[info] registry size: {registry.narrator_count} canonical narrators")

    # Aggregations
    counter: Counter[str] = Counter()             # by canonical_lookup_key
    name_examples: Dict[str, str] = {}            # ckey -> example surface form
    example_paths: Dict[str, str] = {}            # ckey -> example verse path
    by_book: Counter[str] = Counter()             # ckey -> book slug counts (top book per name)
    book_per_ckey: Dict[str, Counter] = defaultdict(Counter)

    verses_seen = 0
    chains_seen = 0
    unresolved_total = 0
    rejected_class2 = 0

    for path in iter_verse_files(books_root, args.book):
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        if doc.get("kind") != "verse_detail":
            continue
        verse = (doc.get("data") or {}).get("verse") or {}
        chain = verse.get("narrator_chain") or {}
        parts = chain.get("parts") or []
        if not parts:
            continue

        verses_seen += 1
        chain_text = reconstruct_chain_text(parts)
        if not chain_text.strip():
            continue

        verse_path = verse.get("path")
        verse_book_slug = _book_slug_from_path(verse_path)

        if args.simulate_class2:
            # Approximate the new extractor's behaviour: peel the per-book
            # preamble (if any), then drop the verse if no chain signal
            # remains. This mirrors what extract_isnad_text would do on a
            # fresh re-run.
            chain_text = _strip_book_preamble(chain_text, verse_book_slug)
            if not _looks_like_isnad(chain_text):
                rejected_class2 += 1
                continue

        chains_seen += 1

        # Re-split & resolve. We deliberately re-run the splitter rather than
        # trusting the persisted "plain" parts as candidate names — the splitter
        # is the source of truth for what's a name vs. chain glue. book_slug
        # is threaded so book-scoped entries (e.g. al-Mufid for Tahdhib /
        # al-Istibsar) resolve correctly.
        names = split_narrator_names(chain_text, use_undiacritized=args.undiacritized)
        if not names:
            continue
        preceding: List[str] = []
        for name in names:
            cid = registry.resolve(
                name, preceding_names=preceding, book_slug=verse_book_slug
            )
            preceding.append(name)
            if cid is not None:
                continue
            # Filter very short tokens — likely splitter glue or a stray particle
            if len(name.split()) < args.min_words:
                continue
            ckey = canonical_lookup_key(name) or name
            counter[ckey] += 1
            unresolved_total += 1
            if ckey not in name_examples:
                name_examples[ckey] = name
                rel = str(path.relative_to(books_root.parent)).replace("\\", "/")
                example_paths[ckey] = rel
            book_slug_for_count = path.relative_to(books_root).parts[0]
            book_per_ckey[ckey][book_slug_for_count] += 1

    print(f"[info] scanned: verses_with_chain={verses_seen} chains_split={chains_seen} "
          f"unresolved_occurrences={unresolved_total} unique_names={len(counter)}")
    if args.simulate_class2:
        print(f"[info] class2-simulated: {rejected_class2} verses rejected "
              f"(no chain signal after preamble strip)")
    print()

    # Top N to console
    print(f"== top {args.top} unresolved narrator names (by occurrence) ==")
    print(f"{'count':>5}  {'top-book':<25}  {'name':<60}  example")
    for ckey, count in counter.most_common(args.top):
        top_book = book_per_ckey[ckey].most_common(1)[0][0] if book_per_ckey[ckey] else ""
        surface = name_examples.get(ckey, "")
        path = example_paths.get(ckey, "")
        print(f"{count:>5}  {top_book:<25}  {surface[:60]:<60}  {path}")

    if args.output:
        out_path = Path(args.output).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("count\tckey\texample_surface\texample_path\ttop_book\n")
            for ckey, count in counter.most_common():
                top_book = book_per_ckey[ckey].most_common(1)[0][0] if book_per_ckey[ckey] else ""
                surface = name_examples.get(ckey, "")
                path = example_paths.get(ckey, "")
                f.write(f"{count}\t{ckey}\t{surface}\t{path}\t{top_book}\n")
        print(f"\n[info] wrote full report to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
