"""Per-lemma lexicon coverage report.

Walks `ThaqalaynWords/lemmas/*.json` and reports how many lemmas have
each lexicon source populated:

- `lanes_definition` — Lane's Lexicon entry text (HTML/plain)
- `classical_definitions` — Hawramani classical-dictionary entries
- `cross_references.qac.found` — QAC v0.4 morphology coverage
- `cross_references.wiktextract.found` — Wiktionary headword presence
- `cross_references.lanes.found` — Lane's matched entry IDs
- `translations` — Path B Spark 11-language gloss

For each metric, prints overall coverage % and the top-N highest-corpus-frequency
lemmas where the field is null (these are the gaps that matter most).

Run after `regen_words.ps1` finishes. Output is summary only; redirect to file
if you want the gap lists for analysis:

    python scripts/report_lemma_coverage.py --top 50 > lemma_coverage.txt
    python scripts/report_lemma_coverage.py --top 0   # only headline percentages
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEFAULT_WORDS_DIR = Path(__file__).resolve().parent.parent.parent / "ThaqalaynWords"


def _non_empty(v) -> bool:
    """Truthy + reject empty dicts/lists/strings."""
    if v is None:
        return False
    if isinstance(v, (list, dict, str)):
        return bool(v)
    return True


def _has_translations(lemma: Dict) -> bool:
    """Translations may be `null` (Path C single gloss) or a Path-B map.
    Treat any non-empty dict / non-empty string as covered."""
    t = lemma.get("translations") or lemma.get("glosses")
    return _non_empty(t)


METRICS: List[Tuple[str, Callable[[Dict], bool]]] = [
    ("lanes_definition", lambda l: _non_empty(l.get("lanes_definition"))),
    ("classical_definitions (hawramani)", lambda l: _non_empty(l.get("classical_definitions"))),
    ("qac (cross_ref)", lambda l: bool(l.get("cross_references", {}).get("qac", {}).get("found"))),
    ("wiktextract (cross_ref)", lambda l: bool(l.get("cross_references", {}).get("wiktextract", {}).get("found"))),
    ("lanes (cross_ref entry_ids)", lambda l: bool(l.get("cross_references", {}).get("lanes", {}).get("found"))),
    ("translations (Path B)", _has_translations),
    ("ipa", lambda l: _non_empty(l.get("ipa"))),
    ("etymology", lambda l: _non_empty(l.get("etymology"))),
    ("definition", lambda l: _non_empty(l.get("definition"))),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--words-dir", default=str(DEFAULT_WORDS_DIR),
                        help="Path to ThaqalaynWords (default: %(default)s)")
    parser.add_argument("--top", type=int, default=20,
                        help="Show top-N missing lemmas per metric by corpus "
                             "frequency. 0 disables the gap list. (default: %(default)s)")
    args = parser.parse_args()

    lemmas_dir = Path(args.words_dir) / "lemmas"
    if not lemmas_dir.is_dir():
        logger.error("Not a directory: %s", lemmas_dir)
        return 2

    files = sorted(lemmas_dir.glob("*.json"))
    if not files:
        logger.error("No lemma files found in %s (did regen_words run?)", lemmas_dir)
        return 2

    print(f"Loaded {len(files)} lemma files from {lemmas_dir}\n")

    # One pass: load each lemma, evaluate every metric, accumulate.
    covered = {name: 0 for name, _ in METRICS}
    gaps: Dict[str, List[Tuple[int, str]]] = {name: [] for name, _ in METRICS}

    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                lemma = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping %s: %s", fp.name, e)
            continue
        freq = lemma.get("frequency_in_corpus") or 0
        slug = lemma.get("slug") or fp.stem
        for name, check in METRICS:
            if check(lemma):
                covered[name] += 1
            elif args.top > 0:
                gaps[name].append((freq, slug))

    total = len(files)
    print(f"{'Metric':<40}  {'Covered':>8}  %")
    print("-" * 70)
    for name, _ in METRICS:
        c = covered[name]
        pct = 100.0 * c / total if total else 0
        print(f"{name:<40}  {c:>8}  {pct:5.1f}%")
    print()

    if args.top > 0:
        for name, _ in METRICS:
            missing = sorted(gaps[name], reverse=True)[: args.top]
            if not missing:
                continue
            print(f"Top {len(missing)} gaps in '{name}' (by corpus frequency):")
            for freq, slug in missing:
                print(f"  {freq:>6}  {slug}")
            print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
