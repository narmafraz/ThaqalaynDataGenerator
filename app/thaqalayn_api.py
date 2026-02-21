"""ThaqalaynAPI JSON transformer.

Transforms scraped ThaqalaynAPI JSON (from raw/thaqalayn_api/) into our
Chapter/Verse hierarchy for output to ThaqalaynData.

Each scraped book is a flat list of hadiths with metadata fields like
volume, categoryId, chapterInCategoryId, chapter (title). This transformer
reconstructs the hierarchy: Book -> [Volume ->] Section -> Chapter -> Hadith.
"""

import json
import logging
import os
from collections import OrderedDict
from typing import Dict, List, Optional

import fastapi

from app.book_registry import BOOK_REGISTRY, BookConfig, get_book_config
from app.lib_db import insert_chapter, write_file
from app.lib_index import add_translation, collect_indexes, update_index_files
from app.lib_model import set_index
from app.models import Chapter, Crumb, Language, PartType, Translation, Verse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_raw_path(folder: str) -> str:
    """Get the path to a raw data folder."""
    return os.path.join(os.path.dirname(__file__), "raw", "thaqalayn_api", folder)


def load_hadiths(folder: str) -> List[dict]:
    """Load hadiths from a scraped JSON file."""
    filepath = os.path.join(get_raw_path(folder), "hadiths.json")
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("hadiths", [])


def make_translator_id(translator_name: str, lang: str = "en") -> str:
    """Generate a translation ID from translator name.

    E.g. "Badr Shahin" -> "en.badr-shahin"
    """
    slug = translator_name.lower().replace(" ", "-").replace(".", "")
    return f"{lang}.{slug}"


def group_hadiths(hadiths: List[dict]) -> OrderedDict:
    """Group flat hadith list into hierarchy by volume/category/chapter.

    Returns:
        OrderedDict[volume, OrderedDict[categoryId, OrderedDict[chapterInCategoryId, list]]]

    All dicts maintain insertion order (first-seen) to preserve the
    original ordering from the API.
    """
    grouped = OrderedDict()

    for h in hadiths:
        vol = h.get("volume", 1)
        cat_id = h.get("categoryId", "1")
        ch_id = h.get("chapterInCategoryId", 1)

        if vol not in grouped:
            grouped[vol] = OrderedDict()
        if cat_id not in grouped[vol]:
            grouped[vol][cat_id] = OrderedDict()
        if ch_id not in grouped[vol][cat_id]:
            grouped[vol][cat_id][ch_id] = []

        grouped[vol][cat_id][ch_id].append(h)

    return grouped


def build_verse(hadith: dict, translator_id: str, fr_translator_id: Optional[str] = None) -> Verse:
    """Build a Verse from a single ThaqalaynAPI hadith."""
    verse = Verse()
    verse.part_type = PartType.Hadith

    arabic = hadith.get("arabicText", "").strip()
    verse.text = [arabic] if arabic else []

    translations = {}
    english = hadith.get("englishText", "").strip()
    if english:
        translations[translator_id] = [english]

    french = hadith.get("frenchText", "").strip()
    if french and fr_translator_id:
        translations[fr_translator_id] = [french]

    if translations:
        verse.translations = translations

    # Gradings
    gradings = {}
    for key, label in [("majlisiGrading", "majlisi"), ("mohseniGrading", "mohseni"), ("behbudiGrading", "behbudi")]:
        val = hadith.get(key, "").strip()
        if val:
            gradings[label] = val
    if gradings:
        verse.gradings = gradings

    # Source URL
    url = hadith.get("URL", "").strip()
    if url:
        verse.source_url = url

    return verse


def build_chapter_from_hadiths(
    chapter_hadiths: List[dict],
    chapter_title: str,
    translator_id: str,
    fr_translator_id: Optional[str] = None,
) -> Chapter:
    """Build a leaf Chapter (with verses) from a group of hadiths."""
    chapter = Chapter()
    chapter.part_type = PartType.Chapter
    chapter.titles = {Language.EN.value: chapter_title}
    chapter.verses = []
    chapter.verse_start_index = 0

    for h in chapter_hadiths:
        verse = build_verse(h, translator_id, fr_translator_id)
        chapter.verses.append(verse)

    return chapter


def has_multiple_volumes(grouped: OrderedDict) -> bool:
    """Check if the book has multiple volumes."""
    return len(grouped) > 1


def has_multiple_categories(volume_data: OrderedDict) -> bool:
    """Check if a volume has multiple categories (sections)."""
    return len(volume_data) > 1


def get_category_title(hadiths: List[dict]) -> str:
    """Extract the category title from the first hadith in a category group."""
    if hadiths:
        return hadiths[0].get("category", "Content")
    return "Content"


def get_chapter_title(hadiths: List[dict]) -> str:
    """Extract the chapter title from the first hadith in a chapter group."""
    if hadiths:
        return hadiths[0].get("chapter", "")
    return ""


def transform_book(
    book_config: BookConfig,
    source_folder: str,
    translator_name: str,
    translator_lang: str = "en",
    fr_translator_name: Optional[str] = None,
    hadiths: Optional[List[dict]] = None,
) -> Chapter:
    """Transform scraped ThaqalaynAPI JSON into our Chapter hierarchy.

    Args:
        book_config: BookConfig from the registry
        source_folder: Folder name under raw/thaqalayn_api/
        translator_name: English translator display name
        translator_lang: Language code for translator (default "en")
        fr_translator_name: French translator name if French translations exist
        hadiths: Pre-loaded hadith list (if None, loads from source_folder)
    """
    if hadiths is None:
        hadiths = load_hadiths(source_folder)
    if not hadiths:
        raise ValueError(f"No hadiths found in {source_folder}")

    # Register translator
    translator_id = make_translator_id(translator_name, translator_lang)
    translation = Translation(name=translator_name, id=translator_id, lang=translator_lang)
    add_translation(translation)

    # Register French translator if applicable
    fr_translator_id = None
    if fr_translator_name:
        fr_translator_id = make_translator_id(fr_translator_name, "fr")
        fr_translation = Translation(name=fr_translator_name, id=fr_translator_id, lang="fr")
        add_translation(fr_translation)

    verse_translations = [translator_id]
    if fr_translator_id:
        verse_translations.append(fr_translator_id)

    grouped = group_hadiths(hadiths)
    multi_vol = has_multiple_volumes(grouped)

    # Build book root
    book = Chapter()
    book.index = book_config.index
    book.path = book_config.path
    book.part_type = PartType.Book
    book.titles = book_config.titles
    book.verse_start_index = 0
    book.verse_translations = verse_translations
    book.chapters = []

    if book_config.descriptions:
        book.descriptions = {k: [v] for k, v in book_config.descriptions.items()}
    if book_config.author:
        book.author = book_config.author
    if book_config.source_url:
        book.source_url = book_config.source_url
    if book_config.default_verse_translation_ids:
        book.default_verse_translation_ids = book_config.default_verse_translation_ids

    crumb = Crumb()
    crumb.titles = book.titles
    crumb.indexed_titles = book.titles
    crumb.path = book.path
    book.crumbs = [crumb]

    for vol_num, vol_data in grouped.items():
        multi_cat = has_multiple_categories(vol_data)

        if multi_vol:
            # Create volume-level chapter
            vol_chapter = Chapter()
            vol_chapter.part_type = PartType.Volume
            vol_chapter.titles = {Language.EN.value: f"Volume {vol_num}"}
            vol_chapter.verse_start_index = 0
            vol_chapter.verse_translations = verse_translations
            vol_chapter.chapters = []
            parent = vol_chapter
        else:
            parent = book

        for cat_id, cat_data in vol_data.items():
            if multi_cat:
                # Get category title from first hadith in first chapter
                first_chapter_hadiths = next(iter(cat_data.values()))
                cat_title = get_category_title(first_chapter_hadiths)

                cat_chapter = Chapter()
                cat_chapter.part_type = PartType.Section
                cat_chapter.titles = {Language.EN.value: cat_title}
                cat_chapter.verse_start_index = 0
                cat_chapter.verse_translations = verse_translations
                cat_chapter.chapters = []
                section_parent = cat_chapter
            else:
                section_parent = parent

            for ch_id, ch_hadiths in cat_data.items():
                ch_title = get_chapter_title(ch_hadiths)
                leaf = build_chapter_from_hadiths(
                    ch_hadiths, ch_title, translator_id, fr_translator_id
                )
                leaf.verse_translations = verse_translations
                if book_config.default_verse_translation_ids:
                    leaf.default_verse_translation_ids = book_config.default_verse_translation_ids
                section_parent.chapters.append(leaf)

            if multi_cat:
                parent.chapters.append(cat_chapter)

        if multi_vol:
            book.chapters.append(vol_chapter)

    # Assign indexes and paths
    set_index(book, [0, 0], 0)

    return book


def init_thaqalayn_api_book(
    book_config: BookConfig,
    source_folder: str,
    translator_name: str,
    translator_lang: str = "en",
    fr_translator_name: Optional[str] = None,
    hadiths: Optional[List[dict]] = None,
):
    """Full pipeline: transform, index, write, collect indexes.

    Args:
        hadiths: Pre-loaded hadith list. If None, loads from source_folder.
    """
    if hadiths is None:
        raw_path = get_raw_path(source_folder)
        hadiths_file = os.path.join(raw_path, "hadiths.json")
        if not os.path.exists(hadiths_file):
            logger.warning("Skipping %s: no raw data at %s", book_config.slug, hadiths_file)
            return

    logger.info("Processing ThaqalaynAPI book: %s", book_config.slug)
    book = transform_book(
        book_config, source_folder, translator_name, translator_lang,
        fr_translator_name, hadiths=hadiths,
    )
    insert_chapter(book)
    write_file(
        f"/books/complete/{book_config.slug}",
        fastapi.encoders.jsonable_encoder(book),
    )
    index_maps = collect_indexes(book)
    update_index_files(index_maps)
    logger.info("Completed ThaqalaynAPI book: %s", book_config.slug)


# ---------------------------------------------------------------------------
# ThaqalaynAPI source configuration
# Maps book slugs to their raw data source folders and translator info.
# Multi-volume books list all source folders; single-volume books have one.
# ---------------------------------------------------------------------------

THAQALAYN_API_BOOKS = {
    # The Four Books (Man La Yahduruhu — 5 volumes in separate source folders)
    "man-la-yahduruhu-al-faqih": {
        "source_folders": [
            "man-la-yahduruhu-al-faqih-v1",
            "man-la-yahduruhu-al-faqih-v2",
            "man-la-yahduruhu-al-faqih-v3",
            "man-la-yahduruhu-al-faqih-v4",
            "man-la-yahduruhu-al-faqih-v5",
        ],
        "translator_name": "Bab Ul Qaim Publications",
    },

    # Primary Collections
    "nahj-al-balagha": {
        "source_folders": ["nahj-al-balagha"],
        "translator_name": "Sayed Ali Raza",
    },
    "al-amali-mufid": {
        "source_folders": ["al-amali-mufid"],
        "translator_name": "Mulla Asgharali M M Jaffer",
    },
    "al-amali-saduq": {
        "source_folders": ["al-amali-saduq"],
        "translator_name": "Bilal Muhammad",
    },
    "kamil-al-ziyarat": {
        "source_folders": ["kamil-al-ziyarat"],
        "translator_name": "Sayyid Mohsen Al-Husayni Al-Milani",
    },
    "kitab-al-ghayba-numani": {
        "source_folders": ["kitab-al-ghayba-numani"],
        "translator_name": "Abdullah al-Shahin",
    },
    "kitab-al-ghayba-tusi": {
        "source_folders": ["kitab-al-ghayba-tusi"],
        "translator_name": "Sayyid Athar Husain S. H. Rizvi",
    },
    "kitab-al-mumin": {
        "source_folders": ["kitab-al-mumin"],
        "translator_name": "Muhajir b. Ali",
    },

    # Additional Collections
    "al-tawhid": {
        "source_folders": ["al-tawhid-saduq"],
        "translator_name": "Sayed Ali Raza Rizvi",
    },
    "uyun-akhbar-al-rida": {
        "source_folders": ["uyun-akhbar-al-rida-v1", "uyun-akhbar-al-rida-v2"],
        "translator_name": "Dr. Ali Peiravi",
    },
    "al-khisal": {
        "source_folders": ["al-khisal"],
        "translator_name": "Dr. Ali Peiravi",
    },
    "maani-al-akhbar": {
        "source_folders": ["maani-al-akhbar"],
        "translator_name": "Basel Kadem",
    },
    # NOTE: kamal-al-din has 659 hadiths in the API but 0 Arabic and 0 English
    # text. The translator field is missing. Kept as placeholder for future data.
    "kamal-al-din": {
        "source_folders": ["kamal-al-din"],
        "translator_name": "Unknown",
    },
    "thawab-al-amal": {
        "source_folders": ["thawab-al-amal"],
        "translator_name": "Sayed Athar Husain Rizvi & Sayed Maqsood Athar",
    },
    "kitab-al-zuhd": {
        "source_folders": ["kitab-al-zuhd"],
        "translator_name": "Shaykh Tahir Ridha Jaffer",
    },
    "risalat-al-huquq": {
        "source_folders": ["risalat-al-huquq"],
        "translator_name": "William C. Chittick",
    },
    "fadail-al-shia": {
        "source_folders": ["fadail-al-shia"],
        "translator_name": "Badr Shahin",
    },
    "sifat-al-shia": {
        "source_folders": ["sifat-al-shia"],
        "translator_name": "Badr Shahin",
    },
    "kitab-al-duafa": {
        "source_folders": ["kitab-al-duafa"],
        "translator_name": "Tashayyu",
    },
    "mujam-al-ahadith-al-mutabara": {
        "source_folders": ["mujam-al-ahadith-al-mutabara"],
        "translator_name": "Ammaar Muslim",
    },
}


def load_hadiths_multi(folders: List[str]) -> List[dict]:
    """Load and merge hadiths from multiple source folders (for multi-volume books).

    Each folder's hadiths are loaded in order. Volume numbers are preserved
    from the source data.
    """
    all_hadiths = []
    for folder in folders:
        raw_path = get_raw_path(folder)
        hadiths_file = os.path.join(raw_path, "hadiths.json")
        if not os.path.exists(hadiths_file):
            logger.warning("Source folder missing: %s", hadiths_file)
            continue
        with open(hadiths_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        all_hadiths.extend(data.get("hadiths", []))
    return all_hadiths


def init_all_thaqalayn_api_books():
    """Process all ThaqalaynAPI books that have raw data available."""
    for slug, api_config in THAQALAYN_API_BOOKS.items():
        book_config = get_book_config(slug)
        if book_config is None:
            logger.warning("No book registry entry for slug: %s", slug)
            continue

        source_folders = api_config["source_folders"]
        translator_name = api_config["translator_name"]
        fr_translator_name = api_config.get("fr_translator_name")

        if len(source_folders) == 1:
            # Single source folder: init_thaqalayn_api_book handles file check
            init_thaqalayn_api_book(
                book_config, source_folders[0], translator_name,
                fr_translator_name=fr_translator_name,
            )
        else:
            # Multi-volume: merge all source folders then pass hadiths directly
            all_hadiths = load_hadiths_multi(source_folders)
            if not all_hadiths:
                logger.info("Skipping %s: no raw data across %d folders", slug, len(source_folders))
                continue

            init_thaqalayn_api_book(
                book_config, source_folders[0], translator_name,
                fr_translator_name=fr_translator_name,
                hadiths=all_hadiths,
            )
