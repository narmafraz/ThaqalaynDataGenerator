"""Inject scraped + AI-generated chapter title translations into a chapter tree.

Two source files per book, both keyed by canonical chapter path:

    scraped/thaqalayn_net/arabic_chapter_titles/{slug}.json
        { "/books/al-khisal:1:5": "بَابُ ..." }

    ai-pipeline-data/chapter_translations/{slug}.json
        { "/books/al-khisal:1:5": {"fa": "...", "ur": "...", ...} }

Parsers call ``inject_translations(book, load_translations(slug))`` after
``set_index()`` has assigned paths but before ``insert_chapter()`` writes
files, so both per-chapter files and the books.{lang}.json indexes pick up
the merged titles in a single regen.
"""

import json
import os
from typing import Dict

from app import config
from app.models import Chapter, Language

SCRAPED_AR_DIR = os.path.join(
    config.RAW_DIR, "thaqalayn_net", "arabic_chapter_titles"
)
AI_TRANSLATIONS_DIR = os.path.join(
    config.AI_PIPELINE_DATA_DIR, "chapter_translations"
)


def load_translations(slug: str) -> Dict[str, Dict[str, str]]:
    """Load all available chapter title translations for a book.

    Returns a dict keyed by canonical chapter path, with per-language
    title values merged from both source files.
    """
    translations: Dict[str, Dict[str, str]] = {}

    scraped_path = os.path.join(SCRAPED_AR_DIR, f"{slug}.json")
    if os.path.exists(scraped_path):
        with open(scraped_path, "r", encoding="utf-8") as f:
            ar_titles = json.load(f)
        for path, ar in ar_titles.items():
            if ar:
                translations.setdefault(path, {})[Language.AR.value] = ar

    ai_path = os.path.join(AI_TRANSLATIONS_DIR, f"{slug}.json")
    if os.path.exists(ai_path):
        with open(ai_path, "r", encoding="utf-8") as f:
            lang_map = json.load(f)
        for path, langs in lang_map.items():
            entry = translations.setdefault(path, {})
            for lang, title in langs.items():
                if title:
                    entry[lang] = title

    return translations


def inject_translations(
    chapter: Chapter, translations: Dict[str, Dict[str, str]]
) -> int:
    """Walk the chapter tree, merging translations into chapter.titles.

    Parser-supplied titles always win — translations only fill empty slots.
    Returns the number of (chapter, language) pairs added.
    """
    added = 0
    if chapter.path and chapter.path in translations:
        if chapter.titles is None:
            chapter.titles = {}
        for lang, title in translations[chapter.path].items():
            if not chapter.titles.get(lang):
                chapter.titles[lang] = title
                added += 1
    if chapter.chapters:
        for sub in chapter.chapters:
            added += inject_translations(sub, translations)
    return added
