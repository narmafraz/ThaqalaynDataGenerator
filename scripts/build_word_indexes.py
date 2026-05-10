"""Build browse/search indexes for the ThaqalaynWords output.

After ``build_word_pages.py`` writes per-surface and per-lemma JSON
files, this script walks those directories and produces two small
JSON index files used by the UI for the words list / search / browse
features:

- ``index/surfaces.json`` — flat list of every surface with its
  occurrence count and lemma link. Used to power autocompletion and
  the alphabetical surface browser.
- ``index/lemmas.json`` — flat list of every lemma with root, POS,
  aggregate frequency, paradigm coverage, and cross-ref presence
  flags. Used to power the lemma browser.

Both are sorted by descending frequency (most useful first when the
list is truncated for paging or when an unsorted UI defaults to it).

This script is deterministic (no LLM calls).

Usage:
    python scripts/build_word_indexes.py
    python scripts/build_word_indexes.py --words-dir ../ThaqalaynWords
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_WORDS_DIR = (PROJECT_ROOT / ".." / "ThaqalaynWords").resolve()


def build_surfaces_index(words_dir: Path) -> List[Dict]:
    """Walk surfaces/ and produce a flat index entry per file."""
    surfaces_dir = words_dir / "surfaces"
    if not surfaces_dir.is_dir():
        raise FileNotFoundError(f"No surfaces dir at {surfaces_dir}")
    entries: List[Dict] = []
    for p in surfaces_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("Skipping %s: %s", p.name, e)
            continue
        morph = data.get("morphology") or {}
        entries.append({
            "slug": data.get("slug"),
            "count": data.get("occurrence_count", 0),
            "lemma": morph.get("lemma_slug"),
            "pos": morph.get("pos"),
        })
    # Sort by descending frequency, then slug for stable ordering.
    entries.sort(key=lambda e: (-(e["count"] or 0), e["slug"] or ""))
    return entries


def build_lemmas_index(words_dir: Path) -> List[Dict]:
    """Walk lemmas/ and produce a flat index entry per file."""
    lemmas_dir = words_dir / "lemmas"
    if not lemmas_dir.is_dir():
        raise FileNotFoundError(f"No lemmas dir at {lemmas_dir}")
    entries: List[Dict] = []
    for p in lemmas_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("Skipping %s: %s", p.name, e)
            continue
        paradigm = data.get("paradigm") or []
        in_corpus_count = sum(1 for p in paradigm if p.get("in_corpus"))
        refs = data.get("cross_references") or {}
        entries.append({
            "slug": data.get("slug"),
            "root": data.get("root"),
            "pos": data.get("pos"),
            "frequency": data.get("frequency_in_corpus", 0),
            "paradigm_size": len(paradigm),
            "in_corpus_forms": in_corpus_count,
            "has_qac": bool(refs.get("qac", {}).get("found")),
            "has_wiktextract": bool(refs.get("wiktextract", {}).get("found")),
            "has_lanes": bool(refs.get("lanes", {}).get("found")),
        })
    entries.sort(key=lambda e: (-(e["frequency"] or 0), e["slug"] or ""))
    return entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--words-dir", type=Path, default=DEFAULT_WORDS_DIR,
                        help="Path to the ThaqalaynWords output directory")
    args = parser.parse_args()
    words_dir: Path = args.words_dir

    logger.info("Walking %s", words_dir)

    surfaces = build_surfaces_index(words_dir)
    lemmas = build_lemmas_index(words_dir)
    logger.info("  surfaces: %d", len(surfaces))
    logger.info("  lemmas: %d", len(lemmas))

    index_dir = words_dir / "index"
    index_dir.mkdir(parents=True, exist_ok=True)

    surfaces_out = index_dir / "surfaces.json"
    lemmas_out = index_dir / "lemmas.json"
    with open(surfaces_out, "w", encoding="utf-8") as f:
        json.dump({"total": len(surfaces), "surfaces": surfaces},
                  f, ensure_ascii=False, separators=(",", ":"))
    with open(lemmas_out, "w", encoding="utf-8") as f:
        json.dump({"total": len(lemmas), "lemmas": lemmas},
                  f, ensure_ascii=False, separators=(",", ":"))
    logger.info("Wrote %s (%d KB)",
                surfaces_out, surfaces_out.stat().st_size // 1024)
    logger.info("Wrote %s (%d KB)",
                lemmas_out, lemmas_out.stat().st_size // 1024)


if __name__ == "__main__":
    main()
