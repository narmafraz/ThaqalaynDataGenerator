"""CLI wrapper around `app.narrator_analysis.build`.

Writes a `{chapter}.narrators.json` sidecar next to every `verse_list` chapter
shell in ThaqalaynData. These power the opt-in "Narrator insights" panel on
chapter pages. Run after a data regen or a one-off data fix.

Usage:
    py scripts/build_narrator_analysis.py
    py scripts/build_narrator_analysis.py --data-root ../ThaqalaynData
    py scripts/build_narrator_analysis.py --book al-kafi   # limit to one book
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running this script directly without PYTHONPATH set
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import narrator_analysis as na  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent / "ThaqalaynData",
        help="Path to ThaqalaynData repository root",
    )
    p.add_argument(
        "--book",
        default=None,
        help="Limit sidecar generation to a single book slug (profile is still "
             "built corpus-wide for accurate source/placeholder classification)",
    )
    args = p.parse_args()

    data_root = args.data_root.resolve()
    if not (data_root / "books").is_dir():
        raise SystemExit(f"ThaqalaynData/books not found under {data_root}")

    print("Pass A: building corpus-wide narrator role profile...")
    profile = na.build_profile(data_root)
    n_sources = sum(1 for nid in profile.role_counts if profile.is_source(nid))
    print(f"  classified {len(profile.names_ar)} narrators "
          f"({n_sources} source-Imams, {len(profile.placeholder_ids)} placeholders)")

    print("Pass B: writing per-chapter sidecars...")
    written = na.build_chapter_sidecars(data_root, profile, only_book=args.book)
    print(f"Wrote {len(written)} narrator-analysis sidecars")


if __name__ == "__main__":
    main()
