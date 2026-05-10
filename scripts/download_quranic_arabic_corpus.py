"""Download Quranic Arabic Corpus v0.4 morphology data.

Source: https://github.com/mustafa0x/quran-morphology (a GitHub fork of
the official Quranic Arabic Corpus v0.4 from corpus.quran.com — the
original site requires email signup; the fork is freely downloadable).

Format: tab-separated, one token per row, columns:
    location <TAB> surface_form <TAB> pos_tag <TAB> features

Where features are pipe-separated annotations like:
    ROOT:علم|LEM:عالَم|MP|GEN     (noun: root علم, lemma عالَم, masc plural, genitive)
    P|PREF|LEM:ال                  (particle: prefix definite article)

Idempotent: skips download if file already present and size matches.

Persists to ThaqalaynWordSources/sources/quranic-arabic-corpus/.

Usage:
    python scripts/download_quranic_arabic_corpus.py [--force]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_DEST = (
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" / "quranic-arabic-corpus"
).resolve()

SOURCE_URLS = {
    "quran-morphology.txt": "https://raw.githubusercontent.com/mustafa0x/quran-morphology/master/quran-morphology.txt",
    "morphology-terms-ar.json": "https://raw.githubusercontent.com/mustafa0x/quran-morphology/master/morphology-terms-ar.json",
    "README.md": "https://raw.githubusercontent.com/mustafa0x/quran-morphology/master/README.md",
}


def download_file(url: str, dest: Path, *, force: bool = False) -> bool:
    """Download a single file. Returns True if downloaded, False if skipped."""
    if dest.exists() and not force:
        logger.info("  exists: %s (skip; use --force to redownload)", dest.name)
        return False
    logger.info("  downloading %s ...", url)
    req = urllib.request.Request(url, headers={"User-Agent": "ThaqalaynWords/1.0"})
    with urllib.request.urlopen(req, timeout=60) as response:
        data = response.read()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        f.write(data)
    logger.info("  wrote %s (%d bytes)", dest.name, len(data))
    return True


def parse_qac_row(line: str) -> Optional[Dict]:
    """Parse one row of quran-morphology.txt.

    Returns a dict with keys:
        location: "surah:ayah:word:morpheme"
        surah, ayah, word, morpheme: int components
        surface: Arabic surface form
        pos: POS code (single char or two-char)
        features: dict of all features (root, lemma, case, etc.)

    Returns None for blank lines or unparseable rows.
    """
    line = line.rstrip("\n").rstrip("\r")
    if not line or line.startswith("#"):
        return None
    parts = line.split("\t")
    if len(parts) < 4:
        return None

    location, surface, pos, features_str = parts[0], parts[1], parts[2], parts[3]

    # Parse location
    loc_parts = location.split(":")
    if len(loc_parts) != 4:
        return None
    try:
        surah, ayah, word, morpheme = (int(p) for p in loc_parts)
    except ValueError:
        return None

    # Parse features (pipe-separated; key:value or flag-only)
    features: Dict[str, object] = {}
    flags: List[str] = []
    for f in features_str.split("|"):
        f = f.strip()
        if not f:
            continue
        if ":" in f:
            key, _, value = f.partition(":")
            features[key.strip()] = value.strip()
        else:
            flags.append(f)
    if flags:
        features["flags"] = flags

    return {
        "location": location,
        "surah": surah,
        "ayah": ayah,
        "word": word,
        "morpheme": morpheme,
        "surface": surface,
        "pos": pos,
        "features": features,
    }


def build_lemma_index(rows: List[Dict]) -> Dict[str, Dict]:
    """Group rows by lemma. Returns dict {lemma: {root, occurrences[], pos}}.

    Each occurrence contains the location + surface form for cross-reference.
    """
    by_lemma: Dict[str, Dict] = {}
    for row in rows:
        lemma = row["features"].get("LEM")
        if not lemma:
            continue
        root = row["features"].get("ROOT", "")
        entry = by_lemma.setdefault(
            lemma,
            {"lemma": lemma, "root": root, "pos": row["pos"], "occurrences": []},
        )
        entry["occurrences"].append(
            {
                "location": row["location"],
                "surface": row["surface"],
            }
        )
    return by_lemma


def build_root_index(rows: List[Dict]) -> Dict[str, List[str]]:
    """Group rows by root. Returns dict {root: [lemma, ...]}.

    A root maps to all lemmas derived from it.
    """
    by_root: Dict[str, set] = {}
    for row in rows:
        root = row["features"].get("ROOT")
        lemma = row["features"].get("LEM")
        if not root or not lemma:
            continue
        by_root.setdefault(root, set()).add(lemma)
    return {root: sorted(lemmas) for root, lemmas in sorted(by_root.items())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dest",
        type=Path,
        default=DEFAULT_DEST,
        help="Destination directory",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files exist",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip downloads; just parse existing files into indexes",
    )
    args = parser.parse_args()

    dest_dir: Path = args.dest
    logger.info("Destination: %s", dest_dir)

    # ----- Download phase -----
    if not args.parse_only:
        logger.info("Downloading files:")
        for filename, url in SOURCE_URLS.items():
            dest = dest_dir / filename
            download_file(url, dest, force=args.force)

    morphology_path = dest_dir / "quran-morphology.txt"
    if not morphology_path.exists():
        logger.error("quran-morphology.txt missing at %s", morphology_path)
        sys.exit(1)

    # ----- Parse phase -----
    logger.info("Parsing %s ...", morphology_path)
    rows: List[Dict] = []
    with open(morphology_path, "r", encoding="utf-8") as f:
        for line in f:
            parsed = parse_qac_row(line)
            if parsed:
                rows.append(parsed)
    logger.info("Parsed %d morpheme rows", len(rows))

    # Build derived indexes
    lemma_index = build_lemma_index(rows)
    root_index = build_root_index(rows)
    logger.info("  %d unique lemmas", len(lemma_index))
    logger.info("  %d unique roots", len(root_index))

    # Write parsed indexes alongside the raw file
    lemma_out = dest_dir / "lemma_index.json"
    root_out = dest_dir / "root_index.json"
    rows_out = dest_dir / "parsed_rows.json"

    with open(lemma_out, "w", encoding="utf-8") as f:
        json.dump(lemma_index, f, ensure_ascii=False, indent=2, sort_keys=True)
    logger.info("Wrote %s (%d lemmas)", lemma_out, len(lemma_index))

    with open(root_out, "w", encoding="utf-8") as f:
        json.dump(root_index, f, ensure_ascii=False, indent=2, sort_keys=True)
    logger.info("Wrote %s (%d roots)", root_out, len(root_index))

    with open(rows_out, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, separators=(",", ":"))
    logger.info("Wrote %s (%d rows)", rows_out, len(rows))


if __name__ == "__main__":
    main()
