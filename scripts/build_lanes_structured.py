"""Build a structured Lane's Lexicon entry index from the raw TEI XML.

Reads the 36 Perseus TEI XML files in
``ThaqalaynWordSources/sources/lanes-lexicon/`` and produces:

- ``lanes_entries.json`` — every entry by ``id``, with body segments
  (typed: italic_en / arabic / text / quote / page_break) and the
  source-citation codes that appear in the body.

Output goes to the same source directory so it lives next to the raw
XML it was derived from. If the resulting JSON exceeds GitHub's 100 MB
per-file limit, this script will log a warning and the file should be
gitignored (similar to the corpus-filtered Wiktextract slim).

Idempotent: re-running overwrites the output.

Usage:
    python scripts/build_lanes_structured.py
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "app"))

from app.words.lanes import build_lanes_entries_index  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SRC = (
    PROJECT_ROOT / ".." / "ThaqalaynWordSources" / "sources" / "lanes-lexicon"
).resolve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", type=Path, default=DEFAULT_SRC,
                        help="Path to lanes-lexicon dir holding TEI XML files")
    args = parser.parse_args()

    if not args.src.is_dir():
        logger.error("Lanes XML dir not found: %s", args.src)
        sys.exit(1)

    logger.info("Building structured Lane's entry index from %s ...", args.src)
    entries = build_lanes_entries_index(args.src)
    logger.info("  parsed %d entries", len(entries))

    out_path = args.src / "lanes_entries.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, separators=(",", ":"))
    size_mb = out_path.stat().st_size / 1_000_000
    logger.info("Wrote %s (%.1f MB)", out_path, size_mb)
    if size_mb > 95:
        logger.warning(
            "  %.1f MB approaches GitHub's 100 MB per-file limit — "
            "consider gitignoring this file and regenerating on demand.",
            size_mb,
        )


if __name__ == "__main__":
    main()
