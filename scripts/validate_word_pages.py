"""Validate the integrity of the ThaqalaynWords output.

Walks the per-page JSONs and asserts:

1. **Schema** — every surface/lemma file has the required fields.
2. **Link integrity** — every surface's ``lemma_link`` points to an
   existing lemma file (the cross-link is the most important contract
   between the two tiers).
3. **Slug ↔ filename** — the ``slug`` field equals the filename stem
   after our filesystem-safe transform.
4. **Frequency consistency** — for each lemma, its paradigm's
   ``count`` sum equals ``frequency_in_corpus``.
5. **Cross-reference sanity** — when a cross-ref says ``found=True``,
   the payload has the expected sub-fields.

Reports counts of each problem class. Exits non-zero if any failure
class is non-empty, so this can be wired into CI later.

Usage:
    python scripts/validate_word_pages.py
    python scripts/validate_word_pages.py --words-dir ../ThaqalaynWords
    python scripts/validate_word_pages.py --strict   # exit non-zero on any
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_WORDS_DIR = (PROJECT_ROOT / ".." / "ThaqalaynWords").resolve()

SURFACE_REQUIRED_FIELDS = {
    "surface", "slug", "occurrence_count", "occurrence_paths",
    "morphology", "lemma_link",
}
LEMMA_REQUIRED_FIELDS = {
    "lemma", "slug", "root", "root_slug", "root_link",
    "pos", "pos_camel", "paradigm",
    "frequency_in_corpus", "cross_references",
    "translations", "definition", "etymology", "ipa",
    "lanes_definition",
}
ROOT_REQUIRED_FIELDS = {
    "root", "slug", "lemmas", "lemma_count", "total_frequency",
    "translations", "definition", "etymology",
}


# ---------------------------------------------------------------------------
# Filename helpers
# ---------------------------------------------------------------------------

def safe_filename(slug_text: str) -> str:
    """Mirror of build_word_pages.py's safe_filename — keep in sync."""
    bad = '<>:"/\\|?*\x00'
    return "".join("_" if c in bad else c for c in slug_text)


# ---------------------------------------------------------------------------
# Per-file checks
# ---------------------------------------------------------------------------

def check_surface_file(p: Path) -> List[str]:
    """Return a list of issue codes for one surface file."""
    issues: List[str] = []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return ["bad_json"]
    missing = SURFACE_REQUIRED_FIELDS - data.keys()
    if missing:
        issues.append(f"missing_fields:{','.join(sorted(missing))}")
    # Slug ↔ filename
    slug = data.get("slug")
    if slug and safe_filename(slug) + ".json" != p.name:
        issues.append("slug_filename_mismatch")
    return issues


def check_lemma_file(p: Path) -> List[str]:
    """Return a list of issue codes for one lemma file."""
    issues: List[str] = []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return ["bad_json"]
    missing = LEMMA_REQUIRED_FIELDS - data.keys()
    if missing:
        issues.append(f"missing_fields:{','.join(sorted(missing))}")
    slug = data.get("slug")
    if slug and safe_filename(slug) + ".json" != p.name:
        issues.append("slug_filename_mismatch")
    # Frequency = sum of in_corpus form counts
    paradigm = data.get("paradigm") or []
    total = sum(p.get("count") or 0 for p in paradigm if p.get("in_corpus"))
    declared = data.get("frequency_in_corpus", 0)
    if total != declared:
        issues.append(f"frequency_mismatch:declared={declared}_computed={total}")
    # Cross-ref payload sanity
    refs = data.get("cross_references") or {}
    for src in ("qac", "wiktextract", "lanes"):
        ref = refs.get(src) or {}
        if ref.get("found"):
            if src == "lanes" and not ref.get("entry_ids"):
                issues.append("lanes_found_but_no_entry_ids")
            # QAC roots aren't required — some particles/proper nouns
            # legitimately have no root in QAC's schema. Don't flag.
    return issues


# ---------------------------------------------------------------------------
# Cross-file checks
# ---------------------------------------------------------------------------

def check_link_integrity(words_dir: Path) -> Tuple[int, int, int]:
    """Verify every surface's lemma_link points to a real lemma file.

    Returns (checked, broken, no_link).
    """
    surfaces_dir = words_dir / "surfaces"
    lemmas_dir = words_dir / "lemmas"

    # Build a fast set of existing lemma filenames (without .json suffix).
    existing_lemmas = {p.stem for p in lemmas_dir.glob("*.json")}

    checked = 0
    broken = 0
    no_link = 0
    for p in surfaces_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        checked += 1
        link = data.get("lemma_link")
        if not link:
            no_link += 1
            continue
        # link like "/words/lemmas/قالَ" — last segment is the slug
        slug = link.rsplit("/", 1)[-1]
        if safe_filename(slug) not in existing_lemmas:
            broken += 1
    return checked, broken, no_link


def check_root_link_integrity(words_dir: Path) -> Tuple[int, int, int]:
    """Verify every lemma's root_link points to a real root file.

    Returns (checked, broken, no_link).
    """
    lemmas_dir = words_dir / "lemmas"
    roots_dir = words_dir / "roots"

    if not roots_dir.is_dir():
        return 0, 0, 0
    existing_roots = {p.stem for p in roots_dir.glob("*.json")}

    checked = 0
    broken = 0
    no_link = 0
    for p in lemmas_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        checked += 1
        link = data.get("root_link")
        if not link:
            no_link += 1
            continue
        slug = link.rsplit("/", 1)[-1]
        if safe_filename(slug) not in existing_roots:
            broken += 1
    return checked, broken, no_link


def check_root_file(p: Path) -> List[str]:
    """Return a list of issue codes for one root file."""
    issues: List[str] = []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return ["bad_json"]
    missing = ROOT_REQUIRED_FIELDS - data.keys()
    if missing:
        issues.append(f"missing_fields:{','.join(sorted(missing))}")
    slug = data.get("slug")
    if slug and safe_filename(slug) + ".json" != p.name:
        issues.append("slug_filename_mismatch")
    # lemma_count must match len(lemmas)
    declared = data.get("lemma_count", 0)
    actual = len(data.get("lemmas") or [])
    if declared != actual:
        issues.append("lemma_count_mismatch")
    # total_frequency must match sum
    total = sum(l.get("frequency", 0) or 0 for l in (data.get("lemmas") or []))
    if total != data.get("total_frequency", 0):
        issues.append("total_frequency_mismatch")
    return issues


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def walk_and_check(
    dir_path: Path, check_fn,
) -> Tuple[int, Counter]:
    """Walk ``dir_path`` running ``check_fn`` on each .json file.

    Returns (file_count, issue_counter).
    """
    counter: Counter = Counter()
    count = 0
    for p in dir_path.glob("*.json"):
        count += 1
        for issue in check_fn(p):
            # Normalize sub-detail to base issue class for counting.
            base = issue.split(":", 1)[0]
            counter[base] += 1
    return count, counter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--words-dir", type=Path, default=DEFAULT_WORDS_DIR)
    parser.add_argument("--strict", action="store_true",
                        help="Exit non-zero when any issue is found")
    args = parser.parse_args()
    words_dir: Path = args.words_dir

    surfaces_dir = words_dir / "surfaces"
    lemmas_dir = words_dir / "lemmas"

    if not surfaces_dir.is_dir() or not lemmas_dir.is_dir():
        logger.error("Expected surfaces/ + lemmas/ in %s", words_dir)
        sys.exit(2)

    logger.info("Walking %s ...", words_dir)
    s_count, s_issues = walk_and_check(surfaces_dir, check_surface_file)
    logger.info("  surfaces: %d files", s_count)
    for issue, n in s_issues.most_common():
        logger.info("    %s: %d", issue, n)

    l_count, l_issues = walk_and_check(lemmas_dir, check_lemma_file)
    logger.info("  lemmas: %d files", l_count)
    for issue, n in l_issues.most_common():
        logger.info("    %s: %d", issue, n)

    roots_dir = words_dir / "roots"
    r_count = 0
    r_issues: Counter = Counter()
    if roots_dir.is_dir():
        r_count, r_issues = walk_and_check(roots_dir, check_root_file)
        logger.info("  roots: %d files", r_count)
        for issue, n in r_issues.most_common():
            logger.info("    %s: %d", issue, n)

    logger.info("Checking surface→lemma link integrity ...")
    s_checked, s_broken, s_no_link = check_link_integrity(words_dir)
    logger.info("  checked: %d, broken: %d, no_link: %d",
                s_checked, s_broken, s_no_link)

    logger.info("Checking lemma→root link integrity ...")
    rl_checked, rl_broken, rl_no_link = check_root_link_integrity(words_dir)
    logger.info("  checked: %d, broken: %d, no_link: %d",
                rl_checked, rl_broken, rl_no_link)

    total_issues = (
        sum(s_issues.values())
        + sum(l_issues.values())
        + sum(r_issues.values())
        + s_broken
        + rl_broken
    )
    logger.info("---")
    logger.info("Total issue rows: %d", total_issues)

    if args.strict and total_issues > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
