"""Walk ThaqalaynWords/surfaces/*.json and emit translation-prompt JSONL.

Each surface row joins the surface's morphology + clitics with the
lemma's translation output from Phase A (the lemma pass). The surface
prompt anchors its output to the lemma's translations so every form
of one lemma shares root vocabulary across the 11 languages.

Output line shape (one per surface):

  {"slug": "وَبِالْعَهْدِ", "surface_ar": "وَبِالْعَهْدِ",
   "pos": "N", "pos_label": "Noun",
   "lemma_ar": "عَهْد",
   "clitic_breakdown": "proclitics: wa- 'and' + bi- 'with/by' + al- 'the'",
   "lemma_translations": {"en": "pact, covenant", "fa": "پیمان", ...},
   "en_gloss": "pact, covenant",
   "lane_body": "concatenated text…",
   "occurrence_count": 47,
   "occurrence_paths": ["/books/al-kafi:1:1:1", ...]}

Reads from:
  - ThaqalaynWords/surfaces/*.json
  - ThaqalaynWords/lemmas/{slug}.json (for lane_body fallback)
  - ThaqalaynWordSources/translation/lemma_responses/{slug}.json
    (for lemma_translations from Phase A; if missing, the row is still
    emitted with empty lemma_translations so the run can proceed —
    Spark will lose its consistency anchor but will still produce output)

Usage:
    python scripts/extract_surface_translation_prompts.py
    python scripts/extract_surface_translation_prompts.py --pilot-set ../path/to/pilot.json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("extract_surface_translation_prompts")


# Reuse the rendering helpers from the lemma extractor. Importing via
# absolute path so this script can be run from the project root or any
# working directory.
def _import_lemma_helpers():
    """Late import so this script doesn't fail when CAMeL Tools / corpus
    paths are not present in the import time environment."""
    import importlib.util

    here = Path(__file__).resolve()
    target = here.parent / "extract_lemma_translation_prompts.py"
    spec = importlib.util.spec_from_file_location(
        "_lemma_extractor", str(target)
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ────────────────── per-surface item builder ──────────────────


def build_clitic_breakdown(clitics: dict) -> str:
    """Render `morphology.clitics` to a prompt-readable string."""
    from app.words.clitic_labels import render_clitics
    return render_clitics(clitics or {})


def load_lemma_translations_map(
    word_sources_dir: Path,
    *,
    round_subdir: Optional[str] = None,
) -> Dict[str, dict]:
    """Walk lemma_responses/*.json, return {slug: glosses_dict}.

    `round_subdir` (e.g. "round-2") restricts the walk to a specific
    experiment-round subdir. Used during the pilot phase to anchor
    surface prompts against a specific lemma-prompt variant. The
    production merge step uses the top-level dir (None).
    """
    base = word_sources_dir / "translation" / "lemma_responses"
    responses_dir = base / round_subdir if round_subdir else base
    if not responses_dir.is_dir():
        logger.warning(
            "no lemma_responses dir at %s — surface prompts will have empty "
            "anchors (Phase A must run before Phase B for best quality)",
            responses_dir,
        )
        return {}
    out: Dict[str, dict] = {}
    skipped_no_parse = 0
    for p in responses_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                resp = json.load(f)
        except Exception:
            continue
        parsed = resp.get("parsed")
        if not parsed:
            skipped_no_parse += 1
            continue
        glosses = (parsed or {}).get("glosses") or {}
        if glosses:
            out[resp.get("slug") or p.stem] = glosses
    logger.info(
        "loaded %d lemma translations (skipped %d unparsed)",
        len(out), skipped_no_parse,
    )
    return out


def load_lemma_context(
    words_dir: Path, lemma_slug: str, helpers
) -> dict:
    """Pull the lemma's en_gloss + lane_body for surface-prompt context.

    Returns {} when the lemma file is missing or unreadable.
    """
    path = words_dir / "lemmas" / f"{lemma_slug}.json"
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return {}
    pos_camel = d.get("pos_camel") or ""
    senses = (d.get("definition") or {}).get("senses") or []
    lanes_entries = (d.get("lanes_definition") or {}).get("entries") or []
    return {
        "en_gloss": helpers.pick_aligned_gloss(pos_camel, senses),
        "lane_body": helpers.render_lanes_body(lanes_entries),
    }


def build_surface_item(
    data: dict,
    *,
    lemma_translations_map: Dict[str, dict],
    lemma_context_cache: Dict[str, dict],
    words_dir: Path,
    helpers,
) -> Optional[dict]:
    slug = data.get("slug")
    if not slug:
        return None
    morph = data.get("morphology") or {}
    lemma_slug = morph.get("lemma_slug") or ""
    pos = morph.get("pos") or ""
    pos_camel = morph.get("pos_camel") or ""
    clitics = morph.get("clitics") or {}

    # Lemma context cached by slug so the same lemma isn't reloaded for
    # every one of its surface forms.
    ctx = lemma_context_cache.get(lemma_slug)
    if ctx is None and lemma_slug:
        ctx = load_lemma_context(words_dir, lemma_slug, helpers)
        lemma_context_cache[lemma_slug] = ctx
    ctx = ctx or {}

    return {
        "slug": slug,
        "surface_ar": data.get("surface") or slug,
        "lemma_ar": lemma_slug,
        "pos": pos,
        "pos_camel": pos_camel,
        "pos_label": helpers.humanize_pos(pos_camel),
        "clitic_breakdown": build_clitic_breakdown(clitics),
        "lemma_translations": lemma_translations_map.get(lemma_slug) or {},
        "en_gloss": ctx.get("en_gloss") or "",
        "lane_body": ctx.get("lane_body") or "",
        "occurrence_count": data.get("occurrence_count", 0),
        # Bounded slice of occurrence_paths — corpus_contexts (round 4+) will
        # query these for ±10-word windows, but the surface row itself only
        # needs the first 5 as a lightweight pointer.
        "occurrence_paths": (data.get("occurrence_paths") or [])[:5],
    }


# ────────────────── walker ──────────────────


def walk_surfaces(
    words_dir: Path,
    *,
    lemma_translations_map: Dict[str, dict],
    helpers,
    slug_filter: Optional[set] = None,
) -> List[dict]:
    surfaces_dir = words_dir / "surfaces"
    if not surfaces_dir.is_dir():
        raise FileNotFoundError(f"No surfaces dir at {surfaces_dir}")
    items: List[dict] = []
    skipped = 0
    cache: Dict[str, dict] = {}
    for p in surfaces_dir.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception as e:
            logger.warning("skipping %s: %s", p.name, e)
            skipped += 1
            continue
        if slug_filter is not None and (d.get("slug") not in slug_filter):
            continue
        item = build_surface_item(
            d,
            lemma_translations_map=lemma_translations_map,
            lemma_context_cache=cache,
            words_dir=words_dir,
            helpers=helpers,
        )
        if item is None:
            skipped += 1
            continue
        items.append(item)
    if skipped:
        logger.warning("skipped %d surface files", skipped)
    return items


# ────────────────── CLI ──────────────────


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--words-dir", type=Path, default=Path("../ThaqalaynWords"),
    )
    parser.add_argument(
        "--word-sources-dir", type=Path,
        default=Path("../ThaqalaynWordSources"),
    )
    parser.add_argument(
        "--out", type=Path,
        default=Path("../ThaqalaynWordSources/translation/surface_prompts.jsonl"),
    )
    parser.add_argument(
        "--pilot-set", type=Path, default=None,
        help="Optional pilot_set.json restricting which surface slugs to emit",
    )
    parser.add_argument(
        "--round-subdir", default=None,
        help='Pull lemma anchors from a specific round subdir (e.g. "round-2"). '
             'Defaults to the top-level lemma_responses dir.',
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    helpers = _import_lemma_helpers()

    slug_filter: Optional[set] = None
    if args.pilot_set is not None:
        with open(args.pilot_set, "r", encoding="utf-8") as f:
            pilot = json.load(f)
        slug_filter = set(pilot.get("surfaces") or [])
        logger.info("pilot filter active: %d surface slugs", len(slug_filter))

    lemma_translations = load_lemma_translations_map(
        args.word_sources_dir, round_subdir=args.round_subdir,
    )

    items = walk_surfaces(
        args.words_dir,
        lemma_translations_map=lemma_translations,
        helpers=helpers,
        slug_filter=slug_filter,
    )
    logger.info("walked %d surface items", len(items))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    logger.info("wrote %s", args.out)

    # Light reporting
    anchored = sum(1 for i in items if i["lemma_translations"])
    with_lane = sum(1 for i in items if i["lane_body"])
    with_clitics = sum(1 for i in items if i["clitic_breakdown"])
    logger.info(
        "coverage: anchored=%d/%d (%.1f%%); lane_body=%d/%d (%.1f%%); "
        "with_clitics=%d/%d (%.1f%%)",
        anchored, len(items), 100 * anchored / max(1, len(items)),
        with_lane, len(items), 100 * with_lane / max(1, len(items)),
        with_clitics, len(items), 100 * with_clitics / max(1, len(items)),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
