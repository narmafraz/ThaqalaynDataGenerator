"""CLI wrapper around `app.verse_counts.write_manifest`.

Use this when you only want to rebuild the verse-counts manifest without
running the full `add_data` pipeline (e.g. after a one-off data fix).

Usage:
    py scripts/build_verse_counts.py
    py scripts/build_verse_counts.py --data-root ../ThaqalaynData --out custom.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

# Allow running this script directly without PYTHONPATH set
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.verse_counts import build, write_manifest  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent / "ThaqalaynData",
        help="Path to ThaqalaynData repository root",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output path (default: <data-root>/index/verse-counts.json)",
    )
    args = p.parse_args()

    out_path = write_manifest(args.data_root, args.out)

    # Re-load just to print a summary
    with open(out_path, encoding="utf-8") as f:
        manifest = json.load(f)
    total_verses = sum(b["total"] for b in manifest.values())
    total_chapters = sum(len(b["by_chapter"]) for b in manifest.values())
    print(f"Wrote {out_path}")
    print(f"  {len(manifest)} books, {total_verses} countable verses, "
          f"{total_chapters} chapters")


if __name__ == "__main__":
    main()
