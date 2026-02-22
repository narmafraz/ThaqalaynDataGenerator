"""Cross-validate Arabic text between HubeAli and thaqalayn.net sources.

For each Al-Kafi chapter that has both HubeAli Arabic text (primary source)
and thaqalayn.net Arabic text (Sarwar pages), compares the Arabic text
per hadith using the 3-tier comparison system from arabic_normalization.py.

Generates validation JSON files in ThaqalaynData/validation/.
"""
import glob
import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from app.arabic_normalization import (
    ComparisonTier,
    ValidationEntry,
    ValidationReport,
    compare_arabic,
)
from app.lib_bs4 import get_contents, is_rtl_tag
from app.lib_db import get_dest_path, get_destination_dir

logger = logging.getLogger(__name__)

# Pattern to strip hadith numbering prefix like "1ـ " or "2 ـ "
HADITH_NUMBER_PREFIX = re.compile(r"^\d+\s*[ـ\-–]\s*")

# Path to the thaqalayn.net mirror chapter files
THAQALAYN_NET_CHAPTER_DIR = os.path.join(
    os.path.dirname(__file__),
    "raw",
    "thaqalayn_net",
    "Thaqalayn",
    "thaqalayn.net",
    "chapter",
)


def _load_generated_chapter(chapter_path: str) -> Optional[dict]:
    """Load a generated chapter JSON from ThaqalaynData."""
    dest_path = get_dest_path(chapter_path)
    if not os.path.exists(dest_path):
        return None
    with open(dest_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "data" in data:
        return data["data"]
    return data


def _extract_arabic_from_thaqalayn_net(filepath: str) -> List[str]:
    """Extract Arabic text from a thaqalayn.net chapter HTML file.

    The HTML contains hadiths separated by <hr> tags. Each hadith section
    has RTL paragraphs (Arabic) followed by LTR paragraphs (English).
    """
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()

    hadith_sections = re.split(r"<hr/?>", html)
    arabic_texts = []

    for section in hadith_sections:
        # Skip body/header sections
        if "<body>" in section or "</body>" in section:
            continue

        soup = BeautifulSoup(section, "html.parser")
        all_paras = soup.find_all("p")
        if not all_paras:
            continue

        # Collect RTL (Arabic) paragraphs
        arabic_parts = []
        for para in all_paras:
            if is_rtl_tag(para):
                arabic_parts.append(get_contents(para))
            else:
                break

        if arabic_parts:
            combined = " ".join(arabic_parts)
            # Strip hadith number prefix (e.g. "1ـ ")
            combined = HADITH_NUMBER_PREFIX.sub("", combined)
            arabic_texts.append(combined)

    return arabic_texts


def _find_thaqalayn_net_file(vol: int, book: int, chapter: int) -> Optional[str]:
    """Find the thaqalayn.net HTML file for a given Al-Kafi chapter."""
    filepath = os.path.join(
        THAQALAYN_NET_CHAPTER_DIR,
        str(vol),
        str(book),
        str(chapter),
    )
    # Could be a direct file or a directory with numbered hadith files
    if os.path.isdir(filepath):
        # Volume 8 has per-hadith files; find all and merge
        html_files = sorted(
            glob.glob(os.path.join(filepath, "*.html")),
            key=lambda f: int(
                os.path.splitext(os.path.basename(f))[0]
            ) if os.path.splitext(os.path.basename(f))[0].isdigit() else 0,
        )
        return html_files if html_files else None

    # Try with .html extension
    html_path = filepath + ".html"
    if os.path.exists(html_path):
        return html_path

    return None


def _get_arabic_texts_from_generated(chapter_data: dict) -> List[str]:
    """Extract full Arabic text from generated chapter verses.

    Combines narrator_chain.text (if present) with verse.text to produce
    the complete Arabic text, matching the format of thaqalayn.net source.
    """
    texts = []
    for verse in chapter_data.get("verses", []):
        part_type = verse.get("part_type")
        if part_type == "Heading":
            continue

        parts = []

        # Include narrator chain text (separated in HubeAli data)
        nc = verse.get("narrator_chain")
        if nc and nc.get("text"):
            parts.append(nc["text"].strip())

        # Include the hadith body text
        text_list = verse.get("text", [])
        if text_list:
            parts.append(" ".join(text_list).strip())

        if parts:
            texts.append(" ".join(parts))
    return texts


def validate_chapter(
    vol: int,
    book: int,
    chapter: int,
) -> Optional[ValidationReport]:
    """Cross-validate a single Al-Kafi chapter between sources.

    Returns None if one or both sources are unavailable.
    """
    chapter_path = f"/books/al-kafi:{vol}:{book}:{chapter}"

    # Load generated data (HubeAli source)
    generated = _load_generated_chapter(chapter_path)
    if not generated:
        return None

    hubeali_texts = _get_arabic_texts_from_generated(generated)
    if not hubeali_texts:
        return None

    # Load thaqalayn.net data
    thaqalayn_file = _find_thaqalayn_net_file(vol, book, chapter)
    if not thaqalayn_file:
        return None

    # Extract Arabic text from thaqalayn.net
    if isinstance(thaqalayn_file, list):
        # Multiple files (volume 8 per-hadith)
        thaqalayn_texts = []
        for f in thaqalayn_file:
            texts = _extract_arabic_from_thaqalayn_net(f)
            thaqalayn_texts.extend(texts)
    else:
        thaqalayn_texts = _extract_arabic_from_thaqalayn_net(thaqalayn_file)

    if not thaqalayn_texts:
        return None

    # Compare hadith by hadith (positional alignment)
    report = ValidationReport(
        book_slug=f"al-kafi:{vol}:{book}:{chapter}",
        source_a_name="hubeali",
        source_b_name="thaqalayn.net",
    )

    count = min(len(hubeali_texts), len(thaqalayn_texts))
    for i in range(count):
        result = compare_arabic(hubeali_texts[i], thaqalayn_texts[i])
        entry = ValidationEntry(
            path=f"{chapter_path}:{i + 1}",
            comparison=result,
            source_a_name="hubeali",
            source_b_name="thaqalayn.net",
        )
        report.entries.append(entry)

    return report


def cross_validate_all_kafi() -> dict:
    """Cross-validate all available Al-Kafi chapters.

    Iterates through all generated Al-Kafi chapters and compares
    with thaqalayn.net source where available.

    Returns summary statistics.
    """
    output_dir = os.path.join(get_destination_dir(), "validation", "cross-validation")
    os.makedirs(output_dir, exist_ok=True)

    all_reports = []
    totals = {
        "chapters_compared": 0,
        "verses_compared": 0,
        "exact": 0,
        "diacritics_only": 0,
        "substantive": 0,
        "chapters_skipped": 0,
    }

    # Al-Kafi has 8 volumes
    for vol in range(1, 9):
        vol_dir = os.path.join(THAQALAYN_NET_CHAPTER_DIR, str(vol))
        if not os.path.isdir(vol_dir):
            continue

        for book_name in sorted(os.listdir(vol_dir)):
            book_path = os.path.join(vol_dir, book_name)
            if not os.path.isdir(book_path):
                continue
            book_num = int(book_name) if book_name.isdigit() else 0
            if book_num == 0:
                continue

            for chapter_name in sorted(os.listdir(book_path)):
                chapter_path = os.path.join(book_path, chapter_name)
                # Could be a .html file or a directory
                if chapter_name.endswith(".html"):
                    ch_num = int(chapter_name.replace(".html", ""))
                elif os.path.isdir(chapter_path):
                    ch_num = int(chapter_name) if chapter_name.isdigit() else 0
                else:
                    continue

                if ch_num == 0:
                    continue

                report = validate_chapter(vol, book_num, ch_num)
                if report and report.total > 0:
                    all_reports.append(report)
                    totals["chapters_compared"] += 1
                    totals["verses_compared"] += report.total
                    totals["exact"] += report.exact_count
                    totals["diacritics_only"] += report.diacritics_count
                    totals["substantive"] += report.substantive_count

                    # Write per-chapter validation file
                    chapter_file = os.path.join(
                        output_dir,
                        f"al-kafi-{vol}-{book_num}-{ch_num}.json",
                    )
                    with open(chapter_file, "w", encoding="utf-8") as f:
                        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
                else:
                    totals["chapters_skipped"] += 1

    # Write summary file
    summary = {
        "kind": "validation_summary",
        "book": "al-kafi",
        "source_a": "hubeali",
        "source_b": "thaqalayn.net",
        "totals": totals,
    }
    summary_path = os.path.join(output_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(
        "Cross-validation complete: %d chapters, %d verses "
        "(%d exact, %d diacritics-only, %d substantive)",
        totals["chapters_compared"],
        totals["verses_compared"],
        totals["exact"],
        totals["diacritics_only"],
        totals["substantive"],
    )

    return totals
