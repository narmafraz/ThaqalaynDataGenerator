"""Corpus surface → words project coverage report.

Asks two questions:

1. **Surface page coverage**: of all unique surface forms in
   `ThaqalaynWordSources/extracted/corpus_surface_set.json`, how many have a
   `ThaqalaynWords/surfaces/{slug}.json` page?

2. **Lemma link resolution**: for surfaces that have a page, how many
   `lemma_link` paths point to an existing
   `ThaqalaynWords/lemmas/{slug}.json` page?

Both are sanity checks on the words pipeline. Drift would mean:

- (1) missing surface pages → `build_word_pages.py --full` either skipped
  surfaces or excluded a subset (`--top-n` / `--sample` mistakes).
- (2) dangling lemma_link → the lemma was filtered out (CAMeL had no
  analysis for it) while the surface page was still emitted.

Run after `regen_words.ps1` finishes:

    python scripts/report_surface_coverage.py
    python scripts/report_surface_coverage.py --top 30
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_WORDS_DIR = PROJECT_ROOT / "ThaqalaynWords"
DEFAULT_CORPUS_SET = PROJECT_ROOT / "ThaqalaynWordSources" / "extracted" / "corpus_surface_set.json"


def _safe_filename(slug: str) -> str:
    """Mirror of build_word_pages.py's safe_filename."""
    bad = '<>:"/\\|?*\x00'
    return "".join("_" if c in bad else c for c in slug)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--words-dir", default=str(DEFAULT_WORDS_DIR))
    parser.add_argument("--corpus-surface-set", default=str(DEFAULT_CORPUS_SET))
    parser.add_argument("--top", type=int, default=20,
                        help="Show top-N gaps by occurrence count (0 disables)")
    args = parser.parse_args()

    words = Path(args.words_dir)
    surfaces_dir = words / "surfaces"
    lemmas_dir = words / "lemmas"
    corpus_path = Path(args.corpus_surface_set)

    if not surfaces_dir.is_dir() or not lemmas_dir.is_dir():
        logger.error("Missing words tree: %s", words)
        return 2
    if not corpus_path.is_file():
        logger.error("Missing corpus_surface_set: %s", corpus_path)
        return 2

    with open(corpus_path, "r", encoding="utf-8") as f:
        corpus_surfaces: Dict[str, Dict] = json.load(f)
    logger.info("Loaded %d unique surfaces from %s", len(corpus_surfaces), corpus_path.name)

    # Pre-index lemma filenames (set lookup is O(1) per surface)
    lemma_filenames = {p.name for p in lemmas_dir.glob("*.json")}
    logger.info("Loaded %d lemma files\n", len(lemma_filenames))

    missing_surface: List[Tuple[int, str]] = []
    dangling_lemma_link: List[Tuple[int, str, str]] = []
    surfaces_with_page = 0
    surfaces_with_resolved_lemma = 0

    for surface, meta in corpus_surfaces.items():
        count = (meta or {}).get("count", 0) if isinstance(meta, dict) else 0
        sp = surfaces_dir / f"{_safe_filename(surface)}.json"
        if not sp.exists():
            missing_surface.append((count, surface))
            continue
        surfaces_with_page += 1
        try:
            with open(sp, "r", encoding="utf-8") as f:
                surface_doc = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        lemma_link = surface_doc.get("lemma_link") or ""
        # lemma_link like "/words/lemmas/{slug}" — strip prefix
        if lemma_link.startswith("/words/lemmas/"):
            target_slug = lemma_link[len("/words/lemmas/"):]
        else:
            target_slug = lemma_link
        if not target_slug:
            # surfaces with no lemma_link (no morphology) — common, not an error
            continue
        target_file = f"{_safe_filename(target_slug)}.json"
        if target_file in lemma_filenames:
            surfaces_with_resolved_lemma += 1
        else:
            dangling_lemma_link.append((count, surface, lemma_link))

    total = len(corpus_surfaces)
    logger.info("%-40s  %8s  %s", "Metric", "Count", "%")
    logger.info("-" * 70)
    logger.info("%-40s  %8d  %5.1f%%", "Surfaces with a surface page",
                surfaces_with_page, 100.0 * surfaces_with_page / total if total else 0)
    logger.info("%-40s  %8d  %5.1f%%", "  ... whose lemma_link resolves",
                surfaces_with_resolved_lemma, 100.0 * surfaces_with_resolved_lemma / total if total else 0)
    logger.info("%-40s  %8d  %5.1f%%", "Missing surface pages",
                len(missing_surface), 100.0 * len(missing_surface) / total if total else 0)
    logger.info("%-40s  %8d", "Dangling lemma_link", len(dangling_lemma_link))
    logger.info("")

    if args.top > 0 and missing_surface:
        top = sorted(missing_surface, reverse=True)[: args.top]
        logger.info("Top %d missing surface pages (by corpus occurrence count):", len(top))
        for count, surface in top:
            logger.info("  %6d  %s", count, surface)
        logger.info("")

    if args.top > 0 and dangling_lemma_link:
        top = sorted(dangling_lemma_link, reverse=True)[: args.top]
        logger.info("Top %d dangling lemma_links:", len(top))
        for count, surface, link in top:
            logger.info("  %6d  %s -> %s", count, surface, link)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
