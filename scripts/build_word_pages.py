"""Build per-word JSON pages for the ThaqalaynWords project.

Reads the corpus surface set + external source indexes, builds surface
and lemma pages for either a small sample (PoC) or the full corpus.

This is the **deterministic** phase — no LLM calls. LLM-synthesized
fields (translations, definitions, etymology) are left as null in the
output; a separate phase fills them in.

Output layout (to ``ThaqalaynWords/``):

    surfaces/{slug}.json
    lemmas/{slug}.json

Slugs are diacritized Arabic NFC. URLs use percent-encoded UTF-8.

Idempotent: re-running overwrites the files.

Usage:
    # 100-surface sample (PoC validation)
    python scripts/build_word_pages.py --sample 100

    # Top-N most-frequent surfaces (sanity check pages for common words)
    python scripts/build_word_pages.py --top-n 1000

    # Full corpus build (long-running — ~102K surfaces, ~10-30 min)
    python scripts/build_word_pages.py --full
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Make app.* importable without env-var fiddling for one-shot script use.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

from app.words.builders import (  # noqa: E402  (sys.path setup must precede)
    WordPageBuilder,
    build_lanes_arabic_index,
    canonical_diacritized_lemma,
)
from app.words.morphology import get_best_analysis  # noqa: E402
from app.words.normalize import slug  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

WORD_SOURCES = (PROJECT_ROOT / ".." / "ThaqalaynWordSources").resolve()
WORDS_OUT = (PROJECT_ROOT / ".." / "ThaqalaynWords").resolve()

CORPUS_PATH = WORD_SOURCES / "extracted" / "corpus_surface_set.json"
QAC_PATH = WORD_SOURCES / "sources" / "quranic-arabic-corpus" / "lemma_index.json"
WIKT_PATH = WORD_SOURCES / "sources" / "wiktextract-arabic" / "summary_index.json"
LANES_ORTH_PATH = WORD_SOURCES / "sources" / "lanes-lexicon" / "orth_index.json"


def load_sources() -> WordPageBuilder:
    """Load all source indexes and instantiate a builder."""
    logger.info("Loading source indexes ...")
    with open(CORPUS_PATH, encoding="utf-8") as f:
        corpus = json.load(f)
    with open(QAC_PATH, encoding="utf-8") as f:
        qac = json.load(f)
    with open(WIKT_PATH, encoding="utf-8") as f:
        wikt = json.load(f)
    with open(LANES_ORTH_PATH, encoding="utf-8") as f:
        lanes_orth = json.load(f)
    logger.info(
        "  corpus=%d, qac=%d, wikt=%d, lanes=%d (bw)",
        len(corpus), len(qac), len(wikt), len(lanes_orth),
    )
    logger.info("Building Arabic-keyed Lane's index ...")
    lanes_ar = build_lanes_arabic_index(lanes_orth)
    logger.info("  lanes (Arabic)=%d", len(lanes_ar))
    return WordPageBuilder(corpus, qac, wikt, lanes_ar)


def pick_surfaces(
    builder: WordPageBuilder,
    *,
    sample: Optional[int] = None,
    top_n: Optional[int] = None,
    full: bool = False,
) -> List[str]:
    """Pick the surface forms to build pages for."""
    all_surfaces = list(builder.corpus_surfaces.keys())
    if full:
        logger.info("Selecting all %d surfaces", len(all_surfaces))
        return all_surfaces
    if top_n:
        # Sort by frequency descending.
        ranked = sorted(
            all_surfaces,
            key=lambda s: builder.corpus_surfaces[s].get("count", 0),
            reverse=True,
        )
        logger.info("Selecting top %d by frequency", top_n)
        return ranked[:top_n]
    if sample:
        import random
        random.seed(0)
        sampled = random.sample(all_surfaces, k=min(sample, len(all_surfaces)))
        logger.info("Selecting random sample of %d", len(sampled))
        return sampled
    return []


def safe_filename(slug_text: str) -> str:
    """Return a filesystem-safe filename for a slug.

    Windows is fine with Arabic chars in NTFS but trips on certain
    characters even when Arabic surrounds them. Sanitize the few
    chars that POSIX/NTFS share as forbidden.
    """
    # Forbidden on Windows / POSIX-portable subset.
    bad = '<>:"/\\|?*\x00'
    return "".join("_" if c in bad else c for c in slug_text)


def write_page(out_dir: Path, slug_text: str, data: Dict) -> None:
    """Write a JSON page to disk."""
    fname = safe_filename(slug_text) + ".json"
    path = out_dir / fname
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--sample", type=int, default=100,
                   help="Random sample of N surfaces (default 100)")
    g.add_argument("--top-n", type=int,
                   help="Top-N most-frequent surfaces")
    g.add_argument("--full", action="store_true",
                   help="Build pages for every surface (long-running)")
    parser.add_argument("--out", type=Path, default=WORDS_OUT,
                        help="Output directory (default: ../ThaqalaynWords)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write files, just compute + log stats")
    args = parser.parse_args()

    builder = load_sources()

    surfaces = pick_surfaces(
        builder,
        sample=args.sample if not args.top_n and not args.full else None,
        top_n=args.top_n,
        full=args.full,
    )
    logger.info("Will build pages for %d surfaces", len(surfaces))

    out_dir = args.out
    if not args.dry_run:
        (out_dir / "surfaces").mkdir(parents=True, exist_ok=True)
        (out_dir / "lemmas").mkdir(parents=True, exist_ok=True)

    # Stats
    written_surfaces = 0
    written_lemmas = 0
    skipped_lemmas: Set[str] = set()
    seen_lemmas: Set[str] = set()
    no_morph = 0
    qac_hits = 0
    wikt_hits = 0
    lanes_hits = 0

    for surface in surfaces:
        page = builder.build_surface(surface)
        if page.get("morphology") is None:
            no_morph += 1
        if not args.dry_run:
            write_page(out_dir / "surfaces", page["slug"], page)
        written_surfaces += 1

        morph = page.get("morphology")
        if not morph:
            continue
        lemma_slug = morph.get("lemma_slug")
        if not lemma_slug:
            continue
        if lemma_slug in seen_lemmas:
            continue
        seen_lemmas.add(lemma_slug)

        # Pass POS hint to build_lemma so the paradigm generator gets
        # the right base POS.
        pos_camel = morph.get("pos_camel") or "verb"
        lemma_page = builder.build_lemma(lemma_slug, pos_hint=pos_camel)

        refs = lemma_page.get("cross_references", {})
        if refs.get("qac", {}).get("found"):
            qac_hits += 1
        if refs.get("wiktextract", {}).get("found"):
            wikt_hits += 1
        if refs.get("lanes", {}).get("found"):
            lanes_hits += 1

        if not args.dry_run:
            write_page(out_dir / "lemmas", lemma_slug, lemma_page)
        written_lemmas += 1

    logger.info("---")
    logger.info("Done.")
    logger.info("  Surfaces written: %d (no_morph: %d)", written_surfaces, no_morph)
    logger.info("  Unique lemmas written: %d", written_lemmas)
    logger.info("  Cross-ref hits:")
    if written_lemmas:
        logger.info("    QAC: %d (%.1f%%)", qac_hits, 100 * qac_hits / written_lemmas)
        logger.info("    Wiktextract: %d (%.1f%%)", wikt_hits, 100 * wikt_hits / written_lemmas)
        logger.info("    Lane's: %d (%.1f%%)", lanes_hits, 100 * lanes_hits / written_lemmas)


if __name__ == "__main__":
    main()
