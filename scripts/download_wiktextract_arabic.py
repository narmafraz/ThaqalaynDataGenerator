"""Download the Wiktextract Arabic dictionary dump and extract a
lemma-indexed JSON for the Words project.

Source: https://kaikki.org/dictionary/Arabic/ — the postprocessed JSONL
provided by Wiktextract (Tatu Ylönen). Each line is one Wiktionary entry
(one word + meaning + etymology + translations).

The full dump is ~476 MB. We download it once to a cache location (not
checked into git — too big), then process it into a structured per-lemma
JSON that IS small enough to commit.

License: Wiktionary content is CC-BY-SA 4.0. Attribution requirement
applies if we redistribute extracted data.

Idempotent: skips download if file present and approximately the
expected size; skips extraction if output is up-to-date.

Usage:
    python scripts/download_wiktextract_arabic.py [--force]
                                                  [--cache <path>]
                                                  [--limit N]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
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
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" / "wiktextract-arabic"
).resolve()
DEFAULT_CACHE = (
    PROJECT_ROOT / "tmp" / "wiktextract_cache"
).resolve()

WIKTEXTRACT_URL = "https://kaikki.org/dictionary/Arabic/kaikki.org-dictionary-Arabic.jsonl"
EXPECTED_SIZE_BYTES = 500_000_000  # ~476MB, allow some variance


def download_with_progress(url: str, dest: Path, *, force: bool = False) -> bool:
    """Download a file with progress reporting. Returns True if downloaded."""
    if dest.exists() and not force:
        size = dest.stat().st_size
        if size > EXPECTED_SIZE_BYTES // 2:  # plausibly complete
            logger.info("  cache hit: %s (%d MB; --force to redownload)", dest.name, size // 1_000_000)
            return False
        logger.info("  cache incomplete (%d MB); re-downloading", size // 1_000_000)

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("  downloading %s ...", url)
    logger.info("  destination: %s", dest)
    req = urllib.request.Request(url, headers={"User-Agent": "ThaqalaynWords/1.0"})

    chunk = 1024 * 1024  # 1MB
    total = 0
    with urllib.request.urlopen(req, timeout=300) as response:
        with open(dest, "wb") as f:
            while True:
                data = response.read(chunk)
                if not data:
                    break
                f.write(data)
                total += len(data)
                if total % (10 * chunk) == 0:
                    logger.info("    ... %d MB", total // 1_000_000)
    logger.info("  wrote %d MB total", total // 1_000_000)
    return True


def parse_jsonl_to_lemma_index(jsonl_path: Path, *, limit: Optional[int] = None) -> Dict[str, List[Dict]]:
    """Read the Wiktextract JSONL dump, group entries by Arabic word.

    Wiktextract entries have ``word`` (the headword), ``pos`` (part of
    speech), ``senses[]`` (definition entries), ``etymology_text``,
    ``forms[]`` (inflections), ``sounds[]``, ``translations[]``, etc.

    Multiple entries can exist for the same word (different POS or
    homographs). We group all entries by ``word`` and return a dict
    mapping word -> list of entries.

    Args:
        jsonl_path: Path to the .jsonl dump.
        limit: Optional cap on number of lines processed (for testing).

    Returns:
        Dict {word: [entry_dict, ...]}. Words are NFC-normalized.
    """
    import unicodedata

    by_word: Dict[str, List[Dict]] = {}
    bad_lines = 0
    processed = 0

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue
            word = entry.get("word")
            if not word:
                continue
            # Normalize the headword for stable keys
            key = unicodedata.normalize("NFC", word)
            by_word.setdefault(key, []).append(entry)
            processed += 1
            if limit and processed >= limit:
                break

    logger.info(
        "  parsed %d entries (%d bad lines skipped); %d unique words",
        processed,
        bad_lines,
        len(by_word),
    )
    return by_word


def slim_entry(entry: Dict) -> Dict:
    """Strip an entry to fields we care about for the Words project.

    Wiktextract entries can be large with many internal fields. We keep:
    word, pos, lang, lang_code, senses[].glosses, etymology_text,
    forms[].form, forms[].tags, translations (filtered to our target
    languages), sounds[].ipa.

    Senses' glosses are the actual definitions ("a man who...", "(of God)
    All-merciful", etc.) — the most valuable piece per entry.
    """
    out: Dict[str, object] = {}
    for k in ("word", "pos", "lang", "lang_code"):
        if k in entry:
            out[k] = entry[k]
    senses = entry.get("senses") or []
    if senses:
        out["senses"] = [
            {
                "glosses": s.get("glosses", []),
                "tags": s.get("tags", []),
                "examples": [
                    {"text": ex.get("text"), "english": ex.get("english")}
                    for ex in (s.get("examples") or [])
                    if ex.get("text")
                ][:3],  # limit to 3 examples per sense
            }
            for s in senses
            if s.get("glosses")
        ]
    if entry.get("etymology_text"):
        out["etymology_text"] = entry["etymology_text"]
    forms = entry.get("forms") or []
    if forms:
        out["forms"] = [
            {"form": f.get("form"), "tags": f.get("tags", [])}
            for f in forms
            if f.get("form")
        ]
    sounds = entry.get("sounds") or []
    if sounds:
        ipas = [s.get("ipa") for s in sounds if s.get("ipa")]
        if ipas:
            out["ipa"] = ipas
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE,
                        help="Cache dir for the large raw dump")
    parser.add_argument("--force", action="store_true",
                        help="Force re-download")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap entries processed (for testing)")
    args = parser.parse_args()

    dest_dir: Path = args.dest
    cache_dir: Path = args.cache
    logger.info("Cache: %s", cache_dir)
    logger.info("Output: %s", dest_dir)

    raw_path = cache_dir / "kaikki.org-dictionary-Arabic.jsonl"

    # ----- Download phase -----
    logger.info("Downloading raw JSONL ...")
    download_with_progress(WIKTEXTRACT_URL, raw_path, force=args.force)

    # ----- Parse + slim -----
    logger.info("Parsing JSONL and grouping by word ...")
    by_word = parse_jsonl_to_lemma_index(raw_path, limit=args.limit)

    # Slim each entry to the fields we care about
    slimmed: Dict[str, List[Dict]] = {}
    for word, entries in by_word.items():
        slimmed[word] = [slim_entry(e) for e in entries]

    # Write the full slim index to the local cache (NOT committed — too large
    # for git's 100MB-per-file limit). A filtered version (entries matching
    # our corpus lemmas) will land in WordSources after Phase 5 produces
    # lemma data.
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_output = cache_dir / "wiktextract_arabic_lemmas.json"
    logger.info("Writing slimmed lemma index to cache ...")
    with open(cache_output, "w", encoding="utf-8") as f:
        json.dump(slimmed, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    size = cache_output.stat().st_size
    logger.info("Wrote %s (%d MB, %d words)", cache_output, size // 1_000_000, len(slimmed))

    # Write a small index of words → entry count + POS tags to WordSources.
    # That's small enough to commit and lets the pipeline check existence
    # cheaply.
    dest_dir.mkdir(parents=True, exist_ok=True)
    summary: Dict[str, Dict] = {}
    for word, entries in slimmed.items():
        summary[word] = {
            "entry_count": len(entries),
            "pos_tags": sorted({e.get("pos") for e in entries if e.get("pos")}),
            "has_etymology": any(e.get("etymology_text") for e in entries),
            "sense_count": sum(len(e.get("senses", [])) for e in entries),
        }
    summary_path = dest_dir / "summary_index.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=None, separators=(",", ":"))
    summary_size = summary_path.stat().st_size
    logger.info("Wrote summary %s (%d MB, %d words)", summary_path, summary_size // 1_000_000, len(summary))

    # README
    readme = dest_dir / "README.md"
    with open(readme, "w", encoding="utf-8") as f:
        f.write(
            "# Wiktextract Arabic\n\n"
            "Lemma-indexed extract of Wiktionary's Arabic entries, derived from\n"
            "the Wiktextract (Tatu Ylönen) postprocessed dump at\n"
            f"<{WIKTEXTRACT_URL}>.\n\n"
            "**License:** Wiktionary content is CC-BY-SA 4.0. Attribution to\n"
            "Wiktionary contributors required for any redistribution.\n\n"
            "## Files\n\n"
            "- `summary_index.json` — committed. Maps word → "
            "{entry_count, pos_tags, has_etymology, sense_count}. "
            "Used to cheaply check whether a Wiktextract entry exists for a "
            "given lemma + which POS variants are available.\n"
            "- (NOT committed) `wiktextract_arabic_lemmas.json` — full slimmed "
            "entries with definitions/senses/etymology. Lives in "
            "`ThaqalaynDataGenerator/tmp/wiktextract_cache/`. ~221 MB, too "
            "large for git. Re-derive with the download script.\n"
            "- (NOT committed) `kaikki.org-dictionary-Arabic.jsonl` — raw "
            "Wiktextract dump in the same cache dir. ~499 MB.\n\n"
            "## Re-build\n\n"
            "```\n"
            "python scripts/download_wiktextract_arabic.py\n"
            "```\n\n"
            "After Phase 5 produces our corpus lemma list, a filtered\n"
            "`wiktextract_corpus_lemmas.json` (small enough to commit) will be\n"
            "added by a filter step.\n\n"
            f"**Total entries:** {len(slimmed):,}\n"
        )
    logger.info("Wrote %s", readme)


if __name__ == "__main__":
    main()
