"""Convert app-furqan SQLite tafsir databases to per-surah JSON files.

Reads SQLite databases from ThaqalaynDataSources/tafsir/app-furqan/
and generates JSON files in ThaqalaynData/tafsir/{edition_id}/{surah}.json

Usage:
    python app/tafsir_converter.py
    python app/tafsir_converter.py --dry-run
    python app/tafsir_converter.py --editions ar.mizan fa.nemooneh
"""

import argparse
import json
import os
import re
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8")

import markdown

from config import SOURCE_DATA_DIR, DEFAULT_DESTINATION_DIR, JSON_ENSURE_ASCII, JSON_INDENT


def md_to_html(text: str) -> str:
    """Convert Markdown tafsir text to HTML.

    Handles the app-furqan format which uses ```arabic and ```english code blocks.
    These are converted to styled divs instead of <code> blocks.
    """
    # Convert ```arabic ... ``` blocks to HTML divs before markdown processing
    text = re.sub(
        r'```arabic\s*\n(.*?)\n```',
        r'<div class="tafsir-arabic" lang="ar" dir="rtl">\1</div>',
        text,
        flags=re.DOTALL,
    )
    text = re.sub(
        r'```english\s*\n(.*?)\n```',
        r'<div class="tafsir-english">\1</div>',
        text,
        flags=re.DOTALL,
    )
    text = re.sub(
        r'```urdu\s*\n(.*?)\n```',
        r'<div class="tafsir-urdu" lang="ur" dir="rtl">\1</div>',
        text,
        flags=re.DOTALL,
    )
    # Convert remaining markdown to HTML (no fenced_code — we already handled code blocks)
    html = markdown.markdown(text, extensions=["tables"])
    return html.strip()

TAFSIR_SOURCE_DIR = os.path.join(SOURCE_DATA_DIR, "tafsir", "app-furqan")
# DESTINATION_DIR is the tafsir repo root (ThaqalaynTafsirData/). Files go:
#   {DESTINATION_DIR}/editions.json
#   {DESTINATION_DIR}/{edition_id}/{surah}.json
DESTINATION_DIR = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)
TAFSIR_OUTPUT_DIR = DESTINATION_DIR

# Edition definitions: maps edition_id to DB info
# Each entry: (db_filename, content_column, metadata)
EDITION_DEFS = {
    "ar.mizan": {
        "db": "tafsir_almizan_ar.db",
        "column": "content",
        "name": "الميزان في تفسير القرآن",
        "name_en": "Al-Mizan fi Tafsir al-Quran",
        "author": "العلامة الطباطبائي",
        "author_en": "Allamah Tabatabai",
        "language": "ar",
    },
    "en.mizan": {
        "db": "tafsir_almizan_en.db",
        "column": "content",
        "name": "Al-Mizan: An Exegesis of the Quran",
        "name_en": "Al-Mizan: An Exegesis of the Quran",
        "author": "Allamah Tabatabai",
        "author_en": "Allamah Tabatabai",
        "language": "en",
    },
    "fa.mizan": {
        "db": os.path.join("database", "tafsir_almizan_fa.db"),
        "column": "content",
        "name": "المیزان فی تفسیر القرآن",
        "name_en": "Al-Mizan (Farsi Translation)",
        "author": "علامه طباطبایی",
        "author_en": "Allamah Tabatabai",
        "language": "fa",
    },
    "fa.nemooneh": {
        "db": "tafsir_namouneh.db",
        "column": "content",
        "name": "تفسیر نمونه",
        "name_en": "Tafsir Nemooneh",
        "author": "آیت‌الله مکارم شیرازی",
        "author_en": "Ayatollah Makarem Shirazi",
        "language": "fa",
    },
    "en.nemooneh": {
        "db": "tafsir_namouneh.db",
        "column": "content_en",
        "name": "Tafsir Nemooneh (English)",
        "name_en": "Tafsir Nemooneh (English)",
        "author": "Ayatollah Makarem Shirazi",
        "author_en": "Ayatollah Makarem Shirazi",
        "language": "en",
    },
    "fa.noor": {
        "db": "tafsir-noor.db",
        "column": "content_fa",
        "name": "تفسیر نور",
        "name_en": "Tafsir Noor",
        "author": "محسن قرائتی",
        "author_en": "Mohsen Gharaati",
        "language": "fa",
    },
    "en.noor": {
        "db": "tafsir-noor.db",
        "column": "content_en",
        "name": "Tafsir Noor (English)",
        "name_en": "Tafsir Noor (English)",
        "author": "Mohsen Gharaati",
        "author_en": "Mohsen Gharaati",
        "language": "en",
    },
    "ar.safi": {
        "db": "tafsir_safi_ar.db",
        "column": "content",
        "name": "تفسير الصافي",
        "name_en": "Tafsir as-Safi",
        "author": "الفيض الكاشاني",
        "author_en": "Mulla Mohsin Fayz Kashani",
        "language": "ar",
    },
    "en.safi": {
        "db": "tafsir_safi_ar.db",
        "column": "content_en",
        "name": "Tafsir as-Safi (English)",
        "name_en": "Tafsir as-Safi (English)",
        "author": "Mulla Mohsin Fayz Kashani",
        "author_en": "Mulla Mohsin Fayz Kashani",
        "language": "en",
    },
}


def get_db_path(db_filename: str) -> str:
    return os.path.join(TAFSIR_SOURCE_DIR, db_filename)


def extract_edition(edition_id: str, edition_def: dict, dry_run: bool = False) -> dict:
    """Extract one tafsir edition from SQLite to per-surah JSON files.

    Returns stats dict with surah_count, ayah_count, empty_count.
    """
    db_path = get_db_path(edition_def["db"])
    if not os.path.exists(db_path):
        print(f"  SKIP {edition_id}: DB not found at {db_path}")
        return {"surah_count": 0, "ayah_count": 0, "empty_count": 0}

    column = edition_def["column"]
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Verify the column exists
    cols = [c[1] for c in cur.execute("PRAGMA table_info(content)").fetchall()]
    if column not in cols:
        print(f"  SKIP {edition_id}: column '{column}' not in content table (has: {cols})")
        conn.close()
        return {"surah_count": 0, "ayah_count": 0, "empty_count": 0}

    # Query: join ayah_mapping with content, preserving content_id for dedup
    query = f"""
        SELECT am.surah_number, am.ayah_number, am.content_id, c.{column}
        FROM ayah_mapping am
        JOIN content c ON am.content_id = c.content_id
        ORDER BY am.surah_number, am.ayah_number
    """
    rows = cur.execute(query).fetchall()
    conn.close()

    # Group by surah, deduplicating shared content blocks
    # Multiple ayahs can share the same content_id (e.g., al-Mizan discusses verse groups).
    # We store each content block once in a "blocks" array and reference by index.
    surahs: dict[int, dict] = {}  # surah_num -> {"ayahs": [...], "blocks": [...]}
    empty_count = 0

    for surah_num, ayah_num, content_id, text in rows:
        if text is None or text.strip() == "":
            empty_count += 1
            continue
        if surah_num not in surahs:
            surahs[surah_num] = {"ayahs": [], "blocks": [], "_block_ids": {}}

        s = surahs[surah_num]
        if content_id not in s["_block_ids"]:
            html = md_to_html(text.strip())
            block_idx = len(s["blocks"])
            s["blocks"].append(html)
            s["_block_ids"][content_id] = block_idx

        block_ref = s["_block_ids"][content_id]
        # Avoid duplicate ayah entries
        if not any(a["ayah"] == ayah_num for a in s["ayahs"]):
            s["ayahs"].append({"ayah": ayah_num, "block": block_ref})

    # Write per-surah JSON files
    output_dir = os.path.join(TAFSIR_OUTPUT_DIR, edition_id)
    total_ayahs = 0

    for surah_num in sorted(surahs.keys()):
        s = surahs[surah_num]
        s["ayahs"].sort(key=lambda a: a["ayah"])
        total_ayahs += len(s["ayahs"])

        surah_data = {
            "edition": edition_id,
            "surah": surah_num,
            "blocks": s["blocks"],
            "ayahs": s["ayahs"],
        }

        if not dry_run:
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, f"{surah_num}.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(surah_data, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)

    stats = {
        "surah_count": len(surahs),
        "ayah_count": total_ayahs,
        "empty_count": empty_count,
    }
    return stats


def generate_editions_index(edition_ids: list[str], dry_run: bool = False):
    """Generate the editions.json index file."""
    editions = []
    for eid in edition_ids:
        ed = EDITION_DEFS[eid]
        editions.append({
            "id": eid,
            "name": ed["name"],
            "name_en": ed["name_en"],
            "author": ed["author"],
            "author_en": ed["author_en"],
            "language": ed["language"],
            "source": "app-furqan",
        })

    if not dry_run:
        os.makedirs(TAFSIR_OUTPUT_DIR, exist_ok=True)
        filepath = os.path.join(TAFSIR_OUTPUT_DIR, "editions.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(editions, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT)
        print(f"  Written {filepath} ({len(editions)} editions)")


def main():
    parser = argparse.ArgumentParser(description="Convert tafsir SQLite DBs to JSON")
    parser.add_argument("--dry-run", action="store_true", help="Don't write files")
    parser.add_argument("--editions", nargs="*", help="Specific editions to convert")
    args = parser.parse_args()

    edition_ids = args.editions or list(EDITION_DEFS.keys())

    # Validate edition IDs
    for eid in edition_ids:
        if eid not in EDITION_DEFS:
            print(f"ERROR: Unknown edition '{eid}'. Available: {list(EDITION_DEFS.keys())}")
            sys.exit(1)

    print(f"Tafsir Converter")
    print(f"  Source: {TAFSIR_SOURCE_DIR}")
    print(f"  Output: {TAFSIR_OUTPUT_DIR}")
    print(f"  Editions: {len(edition_ids)}")
    if args.dry_run:
        print(f"  DRY RUN — no files will be written")
    print()

    successful = []
    for eid in edition_ids:
        ed = EDITION_DEFS[eid]
        print(f"  [{eid}] {ed['name_en']} ({ed['language']})...")
        stats = extract_edition(eid, ed, dry_run=args.dry_run)
        if stats["surah_count"] > 0:
            print(f"    {stats['surah_count']} surahs, {stats['ayah_count']} ayahs"
                  + (f", {stats['empty_count']} empty" if stats["empty_count"] else ""))
            successful.append(eid)
        print()

    if successful:
        generate_editions_index(successful, dry_run=args.dry_run)

    print(f"\nDone. {len(successful)}/{len(edition_ids)} editions converted.")


if __name__ == "__main__":
    main()
