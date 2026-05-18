"""Targeted regen for the man-la-yahduruhu-al-faqih book only.

Used once after fixing the set_index parser bug (chapter 1 of every section
inheriting the cumulative counter as verse_start_index). Runs the same
ThaqalaynAPI ingestion pipeline that `add_data` uses, but for one slug,
so we don't have to spin a full corpus regen alongside other in-flight work.

Usage:
    py scripts/regen_faqih.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Make `app.*` importable when invoked directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Mirror what add_data.ps1 sets — caller can still override with env vars.
os.environ.setdefault("DESTINATION_DIR", "../ThaqalaynData/")
os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

# Fix Windows console encoding for Arabic text output
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from app.book_registry import get_book_config  # noqa: E402
from app.thaqalayn_api import (  # noqa: E402
    THAQALAYN_API_BOOKS,
    init_thaqalayn_api_book,
    load_hadiths_multi,
)


SLUG = "man-la-yahduruhu-al-faqih"


def main() -> None:
    config = THAQALAYN_API_BOOKS.get(SLUG)
    if config is None:
        raise SystemExit(f"No THAQALAYN_API_BOOKS entry for {SLUG}")

    book_config = get_book_config(SLUG)
    if book_config is None:
        raise SystemExit(f"No book registry entry for {SLUG}")

    source_folders = config["source_folders"]
    translator_name = config["translator_name"]
    fr_translator_name = config.get("fr_translator_name")

    if len(source_folders) == 1:
        init_thaqalayn_api_book(
            book_config, source_folders[0], translator_name,
            fr_translator_name=fr_translator_name,
        )
    else:
        all_hadiths = load_hadiths_multi(source_folders)
        if not all_hadiths:
            raise SystemExit(f"No raw data found across {len(source_folders)} source folders")
        init_thaqalayn_api_book(
            book_config, source_folders[0], translator_name,
            fr_translator_name=fr_translator_name,
            hadiths=all_hadiths,
        )

    print(f"Done. Rebuilt {SLUG} into {os.environ['DESTINATION_DIR']}")


if __name__ == "__main__":
    main()
