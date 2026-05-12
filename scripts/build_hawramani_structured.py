"""Build a structured hawramani classical-lexicon entry index from the
raw HTML pages dumped by ``scripts/scrape_hawramani.py``.

Walks every ``.html`` file in
``ThaqalaynWordSources/sources/hawramani-classical/raw/``, runs each
through :func:`app.words.hawramani.parse_hawramani_page`, and writes
the aggregate result to ``hawramani_entries.json`` next to the raw
dumps.

The structured output is keyed by ``fetched_slug`` (the URL slug, which
matches the diacritic-stripped form of one or more lemmas). The page
builder later looks up entries for each lemma using ``strip_diacritics``
on the lemma's slug.

Output schema:

    {
      "fetched_slug": {
        "fetched_slug": "قال",
        "url": "https://arabiclexicon.hawramani.com/قال/",
        "headwords": [...]
      },
      ...
    }

Usage:
    python scripts/build_hawramani_structured.py
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

from app.words.hawramani import (  # noqa: E402
    LEXICON_LEGEND,
    parse_hawramani_dir,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = (
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" /
    "hawramani-classical" / "raw"
).resolve()
DEFAULT_OUT = (
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" /
    "hawramani-classical" / "hawramani_entries.json"
).resolve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR,
                        help="Directory of dumped HTML files")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="Output JSON path")
    args = parser.parse_args()

    if not args.raw_dir.is_dir():
        logger.error("Raw dir not found: %s", args.raw_dir)
        sys.exit(1)

    logger.info("Parsing dumped HTML in %s ...", args.raw_dir)
    entries = parse_hawramani_dir(args.raw_dir)
    logger.info("  parsed %d pages", len(entries))

    # Stats
    total_lex_entries = 0
    lex_counts: Counter = Counter()
    empty_pages = 0
    for d in entries.values():
        if not d.get("headwords"):
            empty_pages += 1
            continue
        for hw in d["headwords"]:
            for e in hw["entries"]:
                total_lex_entries += 1
                lex_counts[e["lexicon_id"]] += 1
    logger.info("  total per-lexicon entries: %d", total_lex_entries)
    logger.info("  unique lexicons: %d", len(lex_counts))
    logger.info("  empty pages: %d", empty_pages)
    if lex_counts:
        logger.info("  top lexicons by count:")
        for lid, c in lex_counts.most_common(10):
            name = LEXICON_LEGEND.get(lid, {}).get("en", "")[:60]
            logger.info("    %s: %5d  %s", lid, c, name)
        # Warn about unknown lexicon IDs.
        unknown = [lid for lid in lex_counts if lid not in LEXICON_LEGEND]
        if unknown:
            logger.warning("  %d lexicon IDs not in LEXICON_LEGEND: %s",
                           len(unknown), unknown[:5])

    # Write output
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, separators=(",", ":"))
    size_mb = args.out.stat().st_size / 1_000_000
    logger.info("Wrote %s (%.1f MB)", args.out, size_mb)
    if size_mb > 95:
        logger.warning(
            "  %.1f MB approaches GitHub's 100 MB per-file limit — "
            "consider gitignoring this file and regenerating on demand.",
            size_mb,
        )


if __name__ == "__main__":
    main()
