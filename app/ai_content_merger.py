"""Merge AI-generated content into the static JSON data files.

This module runs as a post-processing step after all other data generation.
It reads validated AI response files from SOURCE_DATA_DIR/ai-content/{subdir}/responses/,
transforms them into a lean zero-duplication format, and injects them into the
generated JSON files in DESTINATION_DIR.

Zero duplication means:
- diacritized_text NOT stored (reconstruct from word_analysis[].word)
- chunks[].arabic_text NOT stored (reconstruct from word_analysis[word_start:word_end])
- translations[lang].text NOT stored (reconstruct by concatenating chunks[].translations[lang])
- NO AI text in verse.translations (Angular reads from ai.chunks directly)
"""

import json
import logging
import os
from typing import Dict, List, Optional

from app.config import (
    AI_RESPONSES_DIR,
    DEFAULT_DESTINATION_DIR,
    JSON_ENCODING,
    JSON_ENSURE_ASCII,
    JSON_INDENT,
    SOURCE_DATA_DIR,
)

logger = logging.getLogger(__name__)

AI_LANGUAGES = ["en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]

AI_TRANSLATION_ENTRIES = {
    "en.ai": {"id": "en.ai", "lang": "en", "name": "AI Translation", "source": "ai", "model": "claude-opus-4-6"},
    "ur.ai": {"id": "ur.ai", "lang": "ur", "name": "\u0627\u06d2 \u0622\u0626\u06cc \u062a\u0631\u062c\u0645\u06c1", "source": "ai", "model": "claude-opus-4-6"},
    "tr.ai": {"id": "tr.ai", "lang": "tr", "name": "Yapay Zeka \u00c7evirisi", "source": "ai", "model": "claude-opus-4-6"},
    "fa.ai": {"id": "fa.ai", "lang": "fa", "name": "\u062a\u0631\u062c\u0645\u0647 \u0647\u0648\u0634 \u0645\u0635\u0646\u0648\u0639\u06cc", "source": "ai", "model": "claude-opus-4-6"},
    "id.ai": {"id": "id.ai", "lang": "id", "name": "Terjemahan AI", "source": "ai", "model": "claude-opus-4-6"},
    "bn.ai": {"id": "bn.ai", "lang": "bn", "name": "\u098f\u0986\u0987 \u0985\u09a8\u09c1\u09ac\u09be\u09a6", "source": "ai", "model": "claude-opus-4-6"},
    "es.ai": {"id": "es.ai", "lang": "es", "name": "Traducci\u00f3n IA", "source": "ai", "model": "claude-opus-4-6"},
    "fr.ai": {"id": "fr.ai", "lang": "fr", "name": "Traduction IA", "source": "ai", "model": "claude-opus-4-6"},
    "de.ai": {"id": "de.ai", "lang": "de", "name": "KI-\u00dcbersetzung", "source": "ai", "model": "claude-opus-4-6"},
    "ru.ai": {"id": "ru.ai", "lang": "ru", "name": "\u0418\u0418-\u043f\u0435\u0440\u0435\u0432\u043e\u0434", "source": "ai", "model": "claude-opus-4-6"},
    "zh.ai": {"id": "zh.ai", "lang": "zh", "name": "AI\u7ffb\u8bd1", "source": "ai", "model": "claude-opus-4-6"},
}

AI_TRANSLATION_IDS = sorted(AI_TRANSLATION_ENTRIES.keys())


def load_ai_responses(responses_dir: Optional[str] = None) -> Dict[str, dict]:
    """Load all AI response files from the responses directory.

    Returns dict mapping verse_path -> {"ai_attribution": ..., "result": ...}.
    Skips malformed files with warnings.
    """
    if responses_dir is None:
        responses_dir = AI_RESPONSES_DIR

    lookup = {}
    if not os.path.isdir(responses_dir):
        logger.info("AI responses directory not found: %s — skipping AI merge", responses_dir)
        return lookup

    for filename in sorted(os.listdir(responses_dir)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(responses_dir, filename)
        try:
            with open(filepath, "r", encoding=JSON_ENCODING) as f:
                wrapper = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping malformed AI response file %s: %s", filename, e)
            continue

        verse_path = wrapper.get("verse_path")
        if not verse_path or "result" not in wrapper:
            logger.warning("Skipping AI response file %s: missing verse_path or result", filename)
            continue

        attribution = wrapper.get("ai_attribution", {})
        lookup[verse_path] = {
            "ai_attribution": attribution,
            "result": wrapper["result"],
        }

    logger.info("Loaded %d AI response files from %s", len(lookup), responses_dir)
    return lookup


def build_lean_ai_content(result: dict, attribution: dict) -> dict:
    """Restructure AI pipeline result into lean zero-duplication format.

    Transformations:
    - Remove: diacritized_text (reconstruct from word_analysis)
    - Remove: chunks[].arabic_text (reconstruct from word_analysis)
    - Dissolve translations[lang] into: summaries[lang], key_terms[lang], seo_questions[lang]
    - Remove: top-level translations dict entirely
    - Add: ai_attribution

    Returns the lean ai dict ready for injection into a verse.
    """
    ai = {}

    # Add attribution
    clean_attribution = {
        k: v for k, v in attribution.items()
        if k in ("model", "generated_date", "pipeline_version")
    }
    if clean_attribution:
        ai["ai_attribution"] = clean_attribution

    # Copy word_analysis as-is
    if "word_analysis" in result:
        ai["word_analysis"] = result["word_analysis"]

    # Copy chunks, stripping arabic_text from each
    if "chunks" in result:
        lean_chunks = []
        for chunk in result["chunks"]:
            lean_chunk = {k: v for k, v in chunk.items() if k != "arabic_text"}
            lean_chunks.append(lean_chunk)
        ai["chunks"] = lean_chunks

    # Copy isnad_matn as-is
    if "isnad_matn" in result:
        ai["isnad_matn"] = result["isnad_matn"]

    # Copy related_quran as-is
    if "related_quran" in result:
        ai["related_quran"] = result["related_quran"]

    # Copy topics as-is
    if "topics" in result:
        ai["topics"] = result["topics"]

    # Copy tags as-is
    if "tags" in result:
        ai["tags"] = result["tags"]

    # Copy content_type as-is
    if "content_type" in result:
        ai["content_type"] = result["content_type"]

    # Copy key_phrases as-is
    if "key_phrases" in result:
        ai["key_phrases"] = result["key_phrases"]

    # Copy similar_content_hints as-is
    if "similar_content_hints" in result:
        ai["similar_content_hints"] = result["similar_content_hints"]

    # Dissolve translations into summaries, key_terms, seo_questions
    translations = result.get("translations", {})
    if translations:
        summaries = {}
        key_terms = {}
        seo_questions = {}
        for lang, entry in translations.items():
            if isinstance(entry, dict):
                if "summary" in entry:
                    summaries[lang] = entry["summary"]
                if "key_terms" in entry:
                    key_terms[lang] = entry["key_terms"]
                if "seo_question" in entry:
                    seo_questions[lang] = entry["seo_question"]
        if summaries:
            ai["summaries"] = summaries
        if key_terms:
            ai["key_terms"] = key_terms
        if seo_questions:
            ai["seo_questions"] = seo_questions

    # Copy diacritics fields
    if "diacritics_status" in result:
        ai["diacritics_status"] = result["diacritics_status"]
    if "diacritics_changes" in result:
        ai["diacritics_changes"] = result["diacritics_changes"]

    # NOTE: diacritized_text is intentionally NOT copied (zero duplication)

    return ai


def merge_ai_into_verse(verse: dict, ai_lookup: Dict[str, dict]) -> bool:
    """If verse's path matches ai_lookup, set verse['ai'] to lean content.

    Returns True if AI content was merged.
    """
    verse_path = verse.get("path")
    if not verse_path:
        return False

    ai_data = ai_lookup.get(verse_path)
    if not ai_data:
        return False

    lean_ai = build_lean_ai_content(ai_data["result"], ai_data["ai_attribution"])
    verse["ai"] = lean_ai
    return True


def _collect_ai_translation_ids(verses: list, ai_lookup: Dict[str, dict]) -> List[str]:
    """Determine which AI translation IDs should be added to verse_translations.

    Returns sorted list of AI translation IDs that have content in at least one verse.
    """
    ai_langs_present = set()
    for verse in verses:
        verse_path = verse.get("path", "")
        ai_data = ai_lookup.get(verse_path)
        if not ai_data:
            continue
        chunks = ai_data["result"].get("chunks", [])
        for chunk in chunks:
            chunk_translations = chunk.get("translations", {})
            ai_langs_present.update(chunk_translations.keys())

    return sorted(f"{lang}.ai" for lang in ai_langs_present if lang in AI_LANGUAGES)


def merge_ai_into_file(file_path: str, ai_lookup: Dict[str, dict]) -> int:
    """Read a JSON data file, merge AI into verses, re-write if changed.

    Handles different file kinds:
    - verse_list: iterate data.verses[], update data.verse_translations
    - verse_detail: merge into data directly (single verse)
    - chapter_list: skip (no verses)

    Returns the number of verses merged.
    """
    try:
        with open(file_path, "r", encoding=JSON_ENCODING) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read %s: %s", file_path, e)
        return 0

    kind = doc.get("kind", "")
    data = doc.get("data", {})
    merge_count = 0

    if kind == "verse_list":
        verses = data.get("verses", [])
        for verse in verses:
            if merge_ai_into_verse(verse, ai_lookup):
                merge_count += 1

        # Update verse_translations to include AI IDs
        if merge_count > 0:
            ai_ids = _collect_ai_translation_ids(verses, ai_lookup)
            if ai_ids:
                existing = data.get("verse_translations", [])
                for ai_id in ai_ids:
                    if ai_id not in existing:
                        existing.append(ai_id)
                data["verse_translations"] = existing

    elif kind == "verse_detail":
        # Single verse wrapped in data
        verse = data.get("verse", data)
        if merge_ai_into_verse(verse, ai_lookup):
            merge_count += 1

    elif kind == "chapter_list":
        # No verses to merge
        return 0

    else:
        # Unknown kind — try both patterns
        verses = data.get("verses", [])
        if verses:
            for verse in verses:
                if merge_ai_into_verse(verse, ai_lookup):
                    merge_count += 1

    if merge_count > 0:
        with open(file_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(doc, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    return merge_count


def _walk_complete_book(node: dict, ai_lookup: Dict[str, dict]) -> int:
    """Recursively walk a complete book structure, merging AI into verses.

    Complete books have nested chapters containing either more chapters or verses.
    """
    merge_count = 0

    # Check for verses at this level
    verses = node.get("verses", [])
    for verse in verses:
        if merge_ai_into_verse(verse, ai_lookup):
            merge_count += 1

    # Update verse_translations at this level if needed
    if merge_count > 0 and "verse_translations" in node:
        ai_ids = _collect_ai_translation_ids(verses, ai_lookup)
        for ai_id in ai_ids:
            if ai_id not in node["verse_translations"]:
                node["verse_translations"].append(ai_id)

    # Recurse into sub-chapters
    chapters = node.get("chapters", [])
    for chapter in chapters:
        merge_count += _walk_complete_book(chapter, ai_lookup)

    return merge_count


def merge_ai_into_complete_file(file_path: str, ai_lookup: Dict[str, dict]) -> int:
    """Merge AI content into a complete book file (e.g., complete/al-kafi.json).

    These files have a recursive chapter structure inside data.
    """
    try:
        with open(file_path, "r", encoding=JSON_ENCODING) as f:
            doc = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read complete file %s: %s", file_path, e)
        return 0

    data = doc.get("data", {})
    merge_count = _walk_complete_book(data, ai_lookup)

    if merge_count > 0:
        with open(file_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(doc, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)

    return merge_count


def update_translations_index(dest_dir: Optional[str] = None):
    """Add AI translation entries to index/translations.json.

    Adds 11 entries (en.ai, ur.ai, etc.) with source='ai' metadata.
    Preserves all existing entries.
    """
    if dest_dir is None:
        dest_dir = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)

    translations_path = os.path.join(dest_dir, "index", "translations.json")

    if not os.path.isfile(translations_path):
        logger.warning("translations.json not found at %s — skipping update", translations_path)
        return

    with open(translations_path, "r", encoding=JSON_ENCODING) as f:
        translations = json.load(f)

    added = 0
    for tid, entry in AI_TRANSLATION_ENTRIES.items():
        if tid not in translations:
            translations[tid] = entry
            added += 1

    if added > 0:
        with open(translations_path, "w", encoding=JSON_ENCODING) as f:
            json.dump(translations, f, ensure_ascii=JSON_ENSURE_ASCII, indent=JSON_INDENT, sort_keys=True)
        logger.info("Added %d AI translation entries to translations.json", added)


def merge_ai_content(report=None):
    """Main entry point: merge AI content into generated JSON files.

    Steps:
    1. Load AI responses from SOURCE_DATA_DIR
    2. Walk DESTINATION_DIR/books/ for all JSON files
    3. Merge AI content into each file
    4. Handle complete book files separately
    5. Update translations.json index
    6. Log stats to report
    """
    dest_dir = os.environ.get("DESTINATION_DIR", DEFAULT_DESTINATION_DIR)

    # Step 1: Load AI responses
    ai_lookup = load_ai_responses()
    if not ai_lookup:
        logger.info("No AI content to merge")
        if report is not None:
            report.ai_verses_available = 0
            report.ai_verses_merged = 0
        return

    if report is not None:
        report.ai_verses_available = len(ai_lookup)

    total_merged = 0
    errors = []

    # Step 2-3: Walk books/ directory for regular JSON files
    books_dir = os.path.join(dest_dir, "books")
    if os.path.isdir(books_dir):
        for root, _dirs, files in os.walk(books_dir):
            # Skip complete/ subdirectory (handled separately)
            if os.path.basename(root) == "complete":
                continue
            for filename in files:
                if not filename.endswith(".json"):
                    continue
                file_path = os.path.join(root, filename)
                try:
                    merged = merge_ai_into_file(file_path, ai_lookup)
                    total_merged += merged
                except Exception as e:
                    error_msg = f"Error merging AI into {file_path}: {e}"
                    logger.warning(error_msg)
                    errors.append(error_msg)

    # Step 4: Handle complete book files
    complete_dir = os.path.join(dest_dir, "books", "complete")
    if os.path.isdir(complete_dir):
        for filename in os.listdir(complete_dir):
            if not filename.endswith(".json"):
                continue
            file_path = os.path.join(complete_dir, filename)
            try:
                merged = merge_ai_into_complete_file(file_path, ai_lookup)
                total_merged += merged
            except Exception as e:
                error_msg = f"Error merging AI into complete file {file_path}: {e}"
                logger.warning(error_msg)
                errors.append(error_msg)

    # Step 5: Update translations index
    update_translations_index(dest_dir)

    # Step 6: Report
    logger.info(
        "AI content merge complete: %d verses merged (%d available, %d errors)",
        total_merged,
        len(ai_lookup),
        len(errors),
    )

    if report is not None:
        report.ai_verses_merged = total_merged
        report.ai_merge_errors = errors
