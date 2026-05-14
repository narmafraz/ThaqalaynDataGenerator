"""Extract ±10-word corpus context windows for each pilot surface.

Round 4 of the Path B experiment plan tests whether anchoring the
Spark prompt with real corpus usage windows improves translation
quality on polysemous / clitic-heavy surfaces (e.g. resolving the
"appointed" vs "turned away" ambiguity on وَلَّى).

For each surface in the pilot set:
  1. Load the surface JSON to read its occurrence_paths
  2. For up to 3 paths, load the verse_detail from ThaqalaynData
  3. Tokenize verse.text on whitespace
  4. Find tokens whose NFC-normalized form matches the surface NFC slug
  5. Slice ±10 tokens around the match (truncated at chunk boundaries)
  6. Persist {path, window} for the surface

Output: `ThaqalaynWordSources/translation/surface_contexts.json`
{ slug → [{path, window}, …]  }

Consumed by `extract_surface_translation_prompts.py` via the new
`--include-contexts` flag (passed through to the per-item builder).
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("extract_corpus_contexts")


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s or "")


# Token splitter: split on whitespace, drop bracket/parenthesis content,
# strip leading/trailing punctuation. Keep diacritics on the word itself.
_TOKEN_SPLIT_RE = re.compile(r"\s+")
_BRACKETS_RE = re.compile(r"\([^)]*\)|\[[^\]]*\]|\{[^}]*\}")
_LEADING_PUNCT = re.compile(r"^[،.؟!:؛()]+")
_TRAILING_PUNCT = re.compile(r"[،.؟!:؛()]+$")


def tokenize(text: str) -> List[str]:
    text = _BRACKETS_RE.sub(" ", text)
    out: List[str] = []
    for raw in _TOKEN_SPLIT_RE.split(text):
        if not raw:
            continue
        raw = _LEADING_PUNCT.sub("", raw)
        raw = _TRAILING_PUNCT.sub("", raw)
        if raw:
            out.append(raw)
    return out


def path_to_filesystem(path: str, data_dir: Path) -> Path:
    """`/books/al-kafi:1:1:1:1` → `data_dir/books/al-kafi/1/1/1/1.json`."""
    rel = path.lstrip("/")
    # Replace : with / in the index portion only (not in the books/ prefix)
    if rel.startswith("books/"):
        prefix, rest = rel.split("/", 1)
        rest = rest.replace(":", "/")
        rel = f"{prefix}/{rest}"
    return data_dir / f"{rel}.json"


def load_verse_text(path: str, data_dir: Path) -> Optional[str]:
    fs = path_to_filesystem(path, data_dir)
    if not fs.exists():
        return None
    try:
        with open(fs, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return None
    verse = (d.get("data") or {}).get("verse") or {}
    text = verse.get("text")
    if isinstance(text, list):
        text = " ".join(t for t in text if isinstance(t, str))
    return text or None


def slice_window(tokens: List[str], match_idx: int, radius: int = 10) -> str:
    lo = max(0, match_idx - radius)
    hi = min(len(tokens), match_idx + radius + 1)
    return " ".join(tokens[lo:hi])


def extract_for_surface(
    surface_slug: str,
    occurrence_paths: List[str],
    data_dir: Path,
    *,
    max_windows: int = 3,
    radius: int = 10,
) -> List[Dict[str, str]]:
    """Return up to `max_windows` {path, window} dicts for the surface."""
    target = nfc(surface_slug)
    out: List[Dict[str, str]] = []
    for path in occurrence_paths:
        if len(out) >= max_windows:
            break
        text = load_verse_text(path, data_dir)
        if not text:
            continue
        tokens = tokenize(text)
        # Find first matching index
        match_idx = None
        for i, tok in enumerate(tokens):
            if nfc(tok) == target:
                match_idx = i
                break
        if match_idx is None:
            # Try suffix-trim (Arabic verses sometimes have trailing diacritics)
            for i, tok in enumerate(tokens):
                if nfc(tok).rstrip("ٌٍَُِّْ") == target.rstrip("ٌٍَُِّْ"):
                    match_idx = i
                    break
        if match_idx is None:
            continue
        out.append({
            "path": path,
            "window": slice_window(tokens, match_idx, radius=radius),
        })
    return out


# ────────────────── CLI ──────────────────


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--words-dir", type=Path, default=Path("../ThaqalaynWords"),
    )
    parser.add_argument(
        "--data-dir", type=Path, default=Path("../ThaqalaynData"),
        help="ThaqalaynData repo (the merged shipped corpus the UI consumes)",
    )
    parser.add_argument(
        "--word-sources-dir", type=Path,
        default=Path("../ThaqalaynWordSources"),
    )
    parser.add_argument(
        "--out", type=Path,
        default=Path("../ThaqalaynWordSources/translation/surface_contexts.json"),
    )
    parser.add_argument(
        "--pilot-set", type=Path, default=None,
        help="Optional pilot_set.json; defaults to extracting for every "
             "surface in ThaqalaynWords/surfaces/",
    )
    parser.add_argument("--max-windows", type=int, default=3)
    parser.add_argument("--radius", type=int, default=10)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.pilot_set is not None:
        with open(args.pilot_set, "r", encoding="utf-8") as f:
            pilot = json.load(f)
        surface_slugs = pilot.get("surfaces") or []
    else:
        surfaces_dir = args.words_dir / "surfaces"
        surface_slugs = [p.stem for p in surfaces_dir.glob("*.json")]
    logger.info("extracting contexts for %d surfaces", len(surface_slugs))

    result: Dict[str, List[Dict[str, str]]] = {}
    matched = 0
    for slug in surface_slugs:
        # Load the surface JSON to get its occurrence_paths
        surf_file = args.words_dir / "surfaces" / f"{slug}.json"
        if not surf_file.exists():
            continue
        try:
            with open(surf_file, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        paths = d.get("occurrence_paths") or []
        windows = extract_for_surface(
            slug, paths, args.data_dir,
            max_windows=args.max_windows, radius=args.radius,
        )
        if windows:
            result[slug] = windows
            matched += 1

    logger.info("%d/%d surfaces got at least one context window",
                matched, len(surface_slugs))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info("wrote %s", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
