"""Convert scraped altafsir.com data to ThaqalaynTafsirData format.

Reads JSON files from ThaqalaynDataSources/scraped/altafsir_com/{tafsir_id}/
and writes per-surah files to ThaqalaynTafsirData/{edition_id}/
following the same block-reference format as tafsir_converter.py.

Also updates editions.json to include the altafsir-sourced editions.

Usage:
    python app/altafsir_converter.py
    python app/altafsir_converter.py --tafsir 38    # Only al-Qummi
    python app/altafsir_converter.py --dry-run
"""

import argparse
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

from config import SOURCE_DATA_DIR, DEFAULT_DESTINATION_DIR, JSON_ENSURE_ASCII, JSON_INDENT
from scrapers.scrape_altafsir import SHIA_TAFSIRS

ALTAFSIR_SOURCE_DIR = os.path.join(SOURCE_DATA_DIR, "scraped", "altafsir_com")
# DESTINATION_DIR is the tafsir repo root (ThaqalaynTafsirData/). Files go:
#   {DESTINATION_DIR}/editions.json
#   {DESTINATION_DIR}/{edition_id}/{surah}.json
DESTINATION_DIR = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)
TAFSIR_OUTPUT_DIR = DESTINATION_DIR
EDITIONS_FILE = os.path.join(TAFSIR_OUTPUT_DIR, "editions.json")


def text_to_html(text: str) -> str:
    """Convert plain Arabic commentary text to HTML with RTL wrapping.

    altafsir scraper output is already plain text (HTML stripped during scraping).
    We wrap it in a proper RTL div and convert double-newlines to paragraph breaks.
    """
    if not text:
        return ""
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    wrapped = "".join(f'<p dir="rtl" lang="ar">{p}</p>' for p in paragraphs)
    return f'<div class="tafsir-arabic" lang="ar" dir="rtl">{wrapped}</div>'


def convert_tafsir(tafsir_id: int, tafsir_info: dict, dry_run: bool = False) -> dict:
    """Convert one tafsir's scraped JSON files to ThaqalaynData format.

    Returns stats dict.
    """
    source_dir = os.path.join(ALTAFSIR_SOURCE_DIR, str(tafsir_id))
    if not os.path.isdir(source_dir):
        print(f"  SKIP {tafsir_id}: no source dir at {source_dir}")
        return {"surah_count": 0, "ayah_count": 0, "block_count": 0}

    edition_id = tafsir_info["edition_id"]
    output_dir = os.path.join(TAFSIR_OUTPUT_DIR, edition_id)

    total_ayahs = 0
    total_blocks = 0
    surah_count = 0

    # Source JSON files are named {surah}.json
    for filename in sorted(os.listdir(source_dir),
                           key=lambda n: int(n.split(".")[0]) if n.endswith(".json") else 999):
        if not filename.endswith(".json"):
            continue
        surah_file = os.path.join(source_dir, filename)
        try:
            with open(surah_file, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"    SKIP {filename}: {e}")
            continue

        if not data.get("ayahs"):
            continue

        # Convert plain text blocks to HTML
        html_blocks = [text_to_html(b) for b in data.get("blocks", [])]

        out_data = {
            "edition": edition_id,
            "surah": data["surah"],
            "blocks": html_blocks,
            "ayahs": data["ayahs"],
        }

        if not dry_run:
            os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(output_dir, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out_data, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)

        total_ayahs += len(data["ayahs"])
        total_blocks += len(html_blocks)
        surah_count += 1

    return {
        "surah_count": surah_count,
        "ayah_count": total_ayahs,
        "block_count": total_blocks,
    }


def update_editions_index(processed_editions: list[tuple[int, dict]],
                          dry_run: bool = False) -> None:
    """Merge altafsir editions into the existing editions.json."""
    existing = []
    if os.path.exists(EDITIONS_FILE):
        with open(EDITIONS_FILE, encoding="utf-8") as f:
            existing = json.load(f)

    existing_ids = {e["id"] for e in existing}

    for tafsir_id, info in processed_editions:
        eid = info["edition_id"]
        if eid in existing_ids:
            continue
        language = "ar"  # altafsir.com only has Arabic
        existing.append({
            "id": eid,
            "name": info["name_ar"],
            "name_en": info["name_en"],
            "author": info["author_ar"],
            "author_en": info["author_en"],
            "language": language,
            "source": "altafsir.com",
            "death": info.get("death", ""),
        })
        existing_ids.add(eid)

    # Sort by language then name
    existing.sort(key=lambda e: (e["language"], e["name_en"]))

    if not dry_run:
        with open(EDITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)
        print(f"  Updated {EDITIONS_FILE} ({len(existing)} editions total)")


def main():
    parser = argparse.ArgumentParser(description="Convert altafsir JSON to ThaqalaynData tafsir format")
    parser.add_argument("--tafsir", type=int, help="Specific tafsir ID")
    parser.add_argument("--dry-run", action="store_true", help="Don't write files")
    args = parser.parse_args()

    tafsirs = {args.tafsir: SHIA_TAFSIRS[args.tafsir]} if args.tafsir else SHIA_TAFSIRS

    print(f"Altafsir Converter")
    print(f"  Source: {ALTAFSIR_SOURCE_DIR}")
    print(f"  Output: {TAFSIR_OUTPUT_DIR}")
    print(f"  Tafsirs: {len(tafsirs)}")
    if args.dry_run:
        print(f"  DRY RUN")
    print()

    processed = []
    for tid, info in tafsirs.items():
        source_path = os.path.join(ALTAFSIR_SOURCE_DIR, str(tid))
        if not os.path.isdir(source_path):
            print(f"  [{tid}] {info['name_en']} — no source dir, skipping")
            continue

        print(f"  [{tid}] {info['name_en']} ({info['edition_id']})...")
        stats = convert_tafsir(tid, info, dry_run=args.dry_run)
        print(f"    {stats['surah_count']} surahs, {stats['ayah_count']} ayahs, "
              f"{stats['block_count']} blocks")
        if stats["surah_count"] > 0:
            processed.append((tid, info))

    if processed:
        update_editions_index(processed, dry_run=args.dry_run)

    print(f"\nDone. Processed {len(processed)} editions.")


if __name__ == "__main__":
    main()
