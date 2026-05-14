"""Merge Path B Spark translations into the per-page ThaqalaynWords JSONs.

Reads:
  - ThaqalaynWordSources/translation/lemma_responses/{slug}.json
  - ThaqalaynWordSources/translation/surface_responses/{slug}.json
    (or any subdir specified with --round-subdir)

For each parsed response:
  - **lemma**: populates `lemmas/{slug}.json.translations` (was null)
  - **surface**: adds `surfaces/{slug}.json.translations` (new field)
  - **both**: adds a `translations_attribution` block with model + date +
    pipeline version

Idempotent: skips responses with empty `parsed` or non-empty `issues`.
Existing translations are NOT overwritten unless `--overwrite` is passed
(so a partial re-run doesn't clobber a known-good prior result).

Whitespace handling: `.strip()` applied to every gloss before persist
(absorbs the leading-space leak observed on a few Round 1 outputs).

Usage:
    # Merge lemma responses from the top-level dir (production path)
    python scripts/merge_translations_into_pages.py --pass lemma

    # Merge from a specific round (e.g. when piloting)
    python scripts/merge_translations_into_pages.py --pass lemma --round-subdir round-2

    # Surface
    python scripts/merge_translations_into_pages.py --pass surface --round-subdir round-4

    # Both passes back-to-back
    python scripts/merge_translations_into_pages.py --pass both \\
        --lemma-round-subdir round-2 --surface-round-subdir round-4
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("merge_translations_into_pages")

PIPELINE_VERSION = "words.translation.v1.spark"


def make_attribution(model_name: str) -> dict:
    return {
        "model": model_name,
        "generated_date": datetime.date.today().isoformat(),
        "pipeline_version": PIPELINE_VERSION,
    }


def load_responses(
    base_dir: Path, round_subdir: Optional[str] = None
) -> Dict[str, dict]:
    """Walk the response dir and return {slug: {parsed, issues, meta}}.

    `round_subdir` (e.g. "round-2") restricts to a specific experiment
    subdir. None reads from the top-level (production path).
    """
    dir_ = base_dir / round_subdir if round_subdir else base_dir
    if not dir_.is_dir():
        logger.warning("no responses dir at %s", dir_)
        return {}
    out: Dict[str, dict] = {}
    for p in dir_.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                r = json.load(f)
        except Exception:
            continue
        slug = r.get("slug") or p.stem
        out[slug] = r
    return out


def is_valid_response(r: dict) -> bool:
    """Has parsed glosses + no validation issues."""
    if not r.get("parsed"):
        return False
    if r.get("issues"):
        return False
    glosses = (r.get("parsed") or {}).get("glosses") or {}
    if not isinstance(glosses, dict) or not glosses:
        return False
    return True


def cleaned_glosses(parsed: dict) -> dict:
    """Strip whitespace from each gloss value."""
    glosses = (parsed or {}).get("glosses") or {}
    return {k: (v or "").strip() for k, v in glosses.items() if isinstance(v, str)}


def merge_into_page(
    page_path: Path,
    glosses: dict,
    attribution: dict,
    *,
    overwrite: bool,
) -> str:
    """Fold glosses + attribution into one page JSON. Returns 'updated',
    'skipped_existing', or 'missing_page'."""
    if not page_path.exists():
        return "missing_page"
    try:
        with open(page_path, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return "missing_page"

    existing = d.get("translations")
    if existing and not overwrite:
        return "skipped_existing"

    d["translations"] = glosses
    d["translations_attribution"] = attribution

    with open(page_path, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
    return "updated"


def merge_pass(
    *,
    pass_: str,
    word_sources_dir: Path,
    words_dir: Path,
    round_subdir: Optional[str],
    model_name: str,
    overwrite: bool,
) -> Dict[str, int]:
    """Merge either lemma or surface responses into pages.

    Returns a tally dict: {updated, skipped_existing, skipped_invalid,
    missing_page}.
    """
    base = word_sources_dir / "translation" / f"{pass_}_responses"
    responses = load_responses(base, round_subdir=round_subdir)
    logger.info("[%s] loaded %d response files", pass_, len(responses))

    attribution = make_attribution(model_name)
    pages_dir = words_dir / f"{pass_}s"

    tally = {"updated": 0, "skipped_existing": 0,
             "skipped_invalid": 0, "missing_page": 0}
    for slug, r in responses.items():
        if not is_valid_response(r):
            tally["skipped_invalid"] += 1
            continue
        glosses = cleaned_glosses(r["parsed"])
        result = merge_into_page(
            pages_dir / f"{slug}.json",
            glosses,
            attribution,
            overwrite=overwrite,
        )
        tally[result] += 1
    return tally


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pass", dest="pass_", choices=["lemma", "surface", "both"],
        default="both",
    )
    parser.add_argument(
        "--words-dir", type=Path, default=Path("../ThaqalaynWords"),
    )
    parser.add_argument(
        "--word-sources-dir", type=Path,
        default=Path("../ThaqalaynWordSources"),
    )
    parser.add_argument(
        "--round-subdir", default=None,
        help='Apply to both passes (alias for --lemma-round-subdir and '
             '--surface-round-subdir when both are unset)',
    )
    parser.add_argument("--lemma-round-subdir", default=None)
    parser.add_argument("--surface-round-subdir", default=None)
    parser.add_argument(
        "--model-name", default="qwen36-35b-heretic",
        help="Model identity for the translations_attribution block",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Replace existing translations on pages (default: skip)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    lemma_sub = args.lemma_round_subdir or args.round_subdir
    surface_sub = args.surface_round_subdir or args.round_subdir

    grand_total = {"updated": 0, "skipped_existing": 0,
                   "skipped_invalid": 0, "missing_page": 0}
    if args.pass_ in ("lemma", "both"):
        t = merge_pass(
            pass_="lemma",
            word_sources_dir=args.word_sources_dir,
            words_dir=args.words_dir,
            round_subdir=lemma_sub,
            model_name=args.model_name,
            overwrite=args.overwrite,
        )
        logger.info("[lemma] tally: %s", t)
        for k, v in t.items():
            grand_total[k] += v
    if args.pass_ in ("surface", "both"):
        t = merge_pass(
            pass_="surface",
            word_sources_dir=args.word_sources_dir,
            words_dir=args.words_dir,
            round_subdir=surface_sub,
            model_name=args.model_name,
            overwrite=args.overwrite,
        )
        logger.info("[surface] tally: %s", t)
        for k, v in t.items():
            grand_total[k] += v

    logger.info("grand total: %s", grand_total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
