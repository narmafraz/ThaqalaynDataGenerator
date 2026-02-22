"""AI translation pipeline for generating multi-language hadith translations.

This module provides infrastructure for batch-translating Islamic scripture
using the Anthropic Claude API (Haiku model via Batch API for cost efficiency).

The pipeline:
1. Reads existing verse data from ThaqalaynData JSON files
2. Extracts Arabic text and English translations
3. Generates translation requests for the Claude Batch API
4. Processes batch results and outputs per-verse translation files
5. Ingests translations back into existing verse data

Target languages (priority order):
- Tier 1: Urdu (ur), Turkish (tr), Farsi (fa), Indonesian (id), Bengali (bn)
- Tier 2: Spanish (es), French (fr), German (de), Russian (ru), Chinese (zh)

All AI translations are marked with an "AI-generated" disclaimer in the
translator metadata.

Usage:
    # Generate batch request file (JSONL for Claude Batch API)
    python -m app.ai_translation generate --book al-kafi --lang ur tr fa

    # Process batch results and write translation files
    python -m app.ai_translation ingest --input results.jsonl

    # Generate sample translations (no API call needed)
    python -m app.ai_translation sample --book al-kafi --lang ur --count 5
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from app.lib_db import get_dest_path, load_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Language configuration
# ---------------------------------------------------------------------------

@dataclass
class LanguageConfig:
    """Configuration for a target translation language."""
    code: str
    name: str
    native_name: str
    translation_notes: str = ""


SUPPORTED_LANGUAGES: Dict[str, LanguageConfig] = {
    # Tier 1: Priority languages (large Muslim populations)
    "ur": LanguageConfig("ur", "Urdu", "اردو",
        "Use formal Urdu appropriate for religious texts. Preserve Islamic terminology."),
    "tr": LanguageConfig("tr", "Turkish", "Turkce",
        "Use formal Turkish. Preserve Arabic Islamic terms (hadis, imam, etc.)."),
    "fa": LanguageConfig("fa", "Farsi", "فارسی",
        "Use formal Persian. Preserve Arabic Islamic terminology where conventional."),
    "id": LanguageConfig("id", "Indonesian", "Bahasa Indonesia",
        "Use formal Indonesian. Preserve Arabic terms common in Indonesian Islam."),
    "bn": LanguageConfig("bn", "Bengali", "বাংলা",
        "Use formal Bengali. Preserve Arabic Islamic terms transliterated into Bengali script."),

    # Tier 2: Additional languages
    "es": LanguageConfig("es", "Spanish", "Espanol",
        "Use formal Spanish. Transliterate Arabic names; explain Islamic concepts."),
    "fr": LanguageConfig("fr", "French", "Francais",
        "Use formal French. Transliterate Arabic names; explain Islamic concepts."),
    "de": LanguageConfig("de", "German", "Deutsch",
        "Use formal German. Transliterate Arabic names; explain Islamic concepts."),
    "ru": LanguageConfig("ru", "Russian", "Русский",
        "Use formal Russian. Transliterate Arabic names into Cyrillic."),
    "zh": LanguageConfig("zh", "Chinese", "中文",
        "Use formal Simplified Chinese. Transliterate Arabic names; provide context."),
}


# ---------------------------------------------------------------------------
# Translation request/response models
# ---------------------------------------------------------------------------

@dataclass
class TranslationRequest:
    """A single verse translation request for the batch API."""
    custom_id: str         # e.g. "al-kafi:1:1:1:1__ur"
    verse_path: str        # e.g. "/books/al-kafi:1:1:1:1"
    target_lang: str       # e.g. "ur"
    arabic_text: str
    english_text: str
    context: str = ""      # chapter title, book name for context

    def to_batch_request(self) -> dict:
        """Convert to Anthropic Batch API JSONL format.

        Uses claude-haiku for cost efficiency:
        - Input: ~$0.25 / 1M tokens
        - Output: ~$1.25 / 1M tokens
        """
        lang_config = SUPPORTED_LANGUAGES.get(self.target_lang)
        if not lang_config:
            raise ValueError(f"Unsupported language: {self.target_lang}")

        system_prompt = (
            f"You are a professional translator specializing in Islamic religious texts. "
            f"Translate the following hadith/verse into {lang_config.name} ({lang_config.native_name}). "
            f"{lang_config.translation_notes} "
            f"Translate accurately and faithfully. Do not add commentary. "
            f"Output ONLY the translation text, nothing else."
        )

        user_message = f"Arabic original:\n{self.arabic_text}"
        if self.english_text:
            user_message += f"\n\nEnglish translation (for reference):\n{self.english_text}"
        if self.context:
            user_message += f"\n\nContext: {self.context}"

        return {
            "custom_id": self.custom_id,
            "params": {
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 2048,
                "system": system_prompt,
                "messages": [
                    {"role": "user", "content": user_message}
                ],
            }
        }


@dataclass
class TranslationResult:
    """A completed translation from the batch API."""
    custom_id: str
    verse_path: str
    target_lang: str
    translated_text: str
    success: bool = True
    error: Optional[str] = None

    @staticmethod
    def parse_custom_id(custom_id: str) -> Tuple[str, str]:
        """Parse custom_id back into (verse_path, target_lang).

        Format: "al-kafi:1:1:1:1__ur" -> ("/books/al-kafi:1:1:1:1", "ur")
        """
        parts = custom_id.rsplit("__", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid custom_id format: {custom_id}")
        verse_index, lang = parts
        return f"/books/{verse_index}", lang


# ---------------------------------------------------------------------------
# Verse extraction from existing data
# ---------------------------------------------------------------------------

def extract_verses_from_chapter(chapter_path: str) -> List[dict]:
    """Load a chapter JSON and extract verse data for translation.

    Returns list of dicts with keys: path, arabic_text, english_text, chapter_title.
    """
    try:
        data = load_json(chapter_path)
    except FileNotFoundError:
        logger.warning("Chapter file not found: %s", chapter_path)
        return []

    chapter_data = data.get("data", data)
    verses = chapter_data.get("verses", [])
    chapter_title = ""
    titles = chapter_data.get("titles", {})
    if titles:
        chapter_title = titles.get("en", titles.get("ar", ""))

    results = []
    for verse in verses:
        part_type = verse.get("part_type")
        if part_type not in ("Hadith", "Verse"):
            continue

        path = verse.get("path", "")
        if not path:
            continue

        arabic_lines = verse.get("text", [])
        arabic_text = "\n".join(arabic_lines) if arabic_lines else ""

        translations = verse.get("translations", {})
        english_text = ""
        for tid, lines in translations.items():
            if tid.startswith("en."):
                english_text = "\n".join(lines) if isinstance(lines, list) else str(lines)
                break

        if not arabic_text and not english_text:
            continue

        results.append({
            "path": path,
            "arabic_text": arabic_text,
            "english_text": english_text,
            "chapter_title": chapter_title,
        })

    return results


def walk_book_chapters(book_path: str) -> List[str]:
    """Walk a book hierarchy and return all leaf chapter paths.

    Recursively descends through the book structure to find chapters
    that contain verses (leaf nodes).
    """
    try:
        data = load_json(book_path)
    except FileNotFoundError:
        logger.warning("Book file not found: %s", book_path)
        return []

    chapter_data = data.get("data", data)
    chapters = chapter_data.get("chapters", [])
    verses = chapter_data.get("verses", [])

    if verses:
        return [book_path]

    leaf_paths = []
    for ch in chapters:
        ch_path = ch.get("path", "")
        if ch_path:
            leaf_paths.extend(walk_book_chapters(ch_path))

    return leaf_paths


# ---------------------------------------------------------------------------
# Batch request generation
# ---------------------------------------------------------------------------

def generate_batch_requests(
    book_slug: str,
    target_langs: List[str],
    max_verses: Optional[int] = None,
) -> List[TranslationRequest]:
    """Generate translation requests for a book.

    Args:
        book_slug: Book identifier (e.g. "al-kafi")
        target_langs: List of language codes to translate into
        max_verses: Optional limit on number of verses (for testing)

    Returns:
        List of TranslationRequest objects ready for batch submission.
    """
    book_path = f"/books/{book_slug}"
    chapter_paths = walk_book_chapters(book_path)
    logger.info("Found %d leaf chapters for %s", len(chapter_paths), book_slug)

    requests = []
    verse_count = 0

    for chapter_path in chapter_paths:
        verses = extract_verses_from_chapter(chapter_path)
        for verse in verses:
            if max_verses and verse_count >= max_verses:
                return requests

            verse_index = verse["path"][7:]  # strip "/books/"
            for lang in target_langs:
                if lang not in SUPPORTED_LANGUAGES:
                    logger.warning("Skipping unsupported language: %s", lang)
                    continue

                request = TranslationRequest(
                    custom_id=f"{verse_index}__{lang}",
                    verse_path=verse["path"],
                    target_lang=lang,
                    arabic_text=verse["arabic_text"],
                    english_text=verse["english_text"],
                    context=f"Book: {book_slug}, Chapter: {verse['chapter_title']}",
                )
                requests.append(request)

            verse_count += 1

    logger.info("Generated %d translation requests for %d verses", len(requests), verse_count)
    return requests


def write_batch_file(requests: List[TranslationRequest], output_path: str):
    """Write translation requests as JSONL for the Anthropic Batch API.

    Each line is a valid JSON object matching the Batch API format.
    """
    with open(output_path, "w", encoding="utf-8") as f:
        for req in requests:
            json.dump(req.to_batch_request(), f, ensure_ascii=False)
            f.write("\n")
    logger.info("Wrote %d requests to %s", len(requests), output_path)


# ---------------------------------------------------------------------------
# Translation ingestion
# ---------------------------------------------------------------------------

def make_translator_id(lang: str) -> str:
    """Generate the translation ID for AI translations.

    Format: "{lang}.ai" (e.g. "ur.ai", "tr.ai")
    """
    return f"{lang}.ai"


def make_translator_metadata(lang: str) -> dict:
    """Generate translator metadata for AI translations."""
    lang_config = SUPPORTED_LANGUAGES.get(lang)
    if not lang_config:
        raise ValueError(f"Unsupported language: {lang}")

    return {
        "name": f"AI Translation ({lang_config.name})",
        "id": make_translator_id(lang),
        "lang": lang,
        "ai_generated": True,
        "disclaimer": "This translation was generated by AI and may contain errors.",
    }


def parse_batch_results(results_path: str) -> List[TranslationResult]:
    """Parse results from the Anthropic Batch API.

    Each line is a JSON object with:
    - custom_id: Our original request ID
    - result.type: "succeeded" or "errored"
    - result.message.content[0].text: The translated text
    """
    results = []
    with open(results_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            custom_id = data["custom_id"]
            verse_path, target_lang = TranslationResult.parse_custom_id(custom_id)

            result_data = data.get("result", {})
            if result_data.get("type") == "succeeded":
                message = result_data.get("message", {})
                content = message.get("content", [])
                text = content[0].get("text", "") if content else ""
                results.append(TranslationResult(
                    custom_id=custom_id,
                    verse_path=verse_path,
                    target_lang=target_lang,
                    translated_text=text.strip(),
                ))
            else:
                error_msg = result_data.get("error", {}).get("message", "Unknown error")
                results.append(TranslationResult(
                    custom_id=custom_id,
                    verse_path=verse_path,
                    target_lang=target_lang,
                    translated_text="",
                    success=False,
                    error=error_msg,
                ))

    logger.info("Parsed %d results from %s", len(results), results_path)
    return results


def ingest_translations(
    results: List[TranslationResult],
    dry_run: bool = False,
) -> Dict[str, int]:
    """Ingest translated text into existing verse data files.

    For each successful translation, updates the verse's chapter JSON
    to include the new translation under the AI translator ID.

    Returns dict of counters: {"ingested": N, "skipped": N, "errors": N}
    """
    counters = {"ingested": 0, "skipped": 0, "errors": 0}

    # Group results by chapter path for efficient file I/O
    by_chapter: Dict[str, List[TranslationResult]] = {}
    for result in results:
        if not result.success:
            counters["errors"] += 1
            continue

        # Derive chapter path from verse path (strip last :N)
        parts = result.verse_path.rsplit(":", 1)
        if len(parts) != 2:
            logger.warning("Cannot derive chapter path from: %s", result.verse_path)
            counters["errors"] += 1
            continue

        chapter_path = parts[0]
        if chapter_path not in by_chapter:
            by_chapter[chapter_path] = []
        by_chapter[chapter_path].append(result)

    for chapter_path, chapter_results in by_chapter.items():
        try:
            chapter_data = load_json(chapter_path)
        except FileNotFoundError:
            logger.warning("Chapter not found: %s", chapter_path)
            counters["errors"] += len(chapter_results)
            continue

        data = chapter_data.get("data", chapter_data)
        verses = data.get("verses", [])

        # Build path->verse lookup
        verse_by_path = {}
        for v in verses:
            vpath = v.get("path", "")
            if vpath:
                verse_by_path[vpath] = v

        modified = False
        for result in chapter_results:
            verse = verse_by_path.get(result.verse_path)
            if not verse:
                logger.warning("Verse not found in chapter: %s", result.verse_path)
                counters["skipped"] += 1
                continue

            translator_id = make_translator_id(result.target_lang)
            translations = verse.get("translations", {})
            translations[translator_id] = [result.translated_text]
            verse["translations"] = translations
            modified = True
            counters["ingested"] += 1

        if modified and not dry_run:
            # Update verse_translations list in chapter if needed
            for result in chapter_results:
                translator_id = make_translator_id(result.target_lang)
                vt = data.get("verse_translations", [])
                if translator_id not in vt:
                    vt.append(translator_id)
                    data["verse_translations"] = vt

            # Write back
            dest_path = get_dest_path(chapter_path)
            with open(dest_path, "w", encoding="utf-8") as f:
                json.dump(chapter_data, f, ensure_ascii=False, indent=2, sort_keys=True)

    logger.info("Ingestion complete: %s", counters)
    return counters


# ---------------------------------------------------------------------------
# Sample translation generation (for testing without API)
# ---------------------------------------------------------------------------

# Representative sample translations for demonstration purposes.
# In production, these would come from the Claude Batch API.
SAMPLE_TRANSLATIONS = {
    "ur": {
        "bismillah": "اللہ کے نام سے جو بہت مہربان، نہایت رحم والا ہے",
        "hadith_prefix": "ہم سے بیان کیا",
    },
    "tr": {
        "bismillah": "Rahman ve Rahim olan Allah'in adiyla",
        "hadith_prefix": "Bize rivayet etti",
    },
    "fa": {
        "bismillah": "به نام خداوند بخشنده مهربان",
        "hadith_prefix": "از ما روایت شده",
    },
    "id": {
        "bismillah": "Dengan nama Allah Yang Maha Pengasih, Maha Penyayang",
        "hadith_prefix": "Diriwayatkan kepada kami",
    },
    "bn": {
        "bismillah": "পরম করুণাময় অতি দয়ালু আল্লাহর নামে",
        "hadith_prefix": "আমাদের কাছে বর্ণিত হয়েছে",
    },
    "es": {
        "bismillah": "En el nombre de Dios, el Compasivo, el Misericordioso",
        "hadith_prefix": "Nos fue narrado",
    },
    "fr": {
        "bismillah": "Au nom de Dieu, le Tout Misericordieux, le Tres Misericordieux",
        "hadith_prefix": "Il nous a ete rapporte",
    },
    "de": {
        "bismillah": "Im Namen Gottes, des Allerbarmers, des Barmherzigen",
        "hadith_prefix": "Es wurde uns berichtet",
    },
    "ru": {
        "bismillah": "Во имя Аллаха, Милостивого, Милосердного",
        "hadith_prefix": "Нам было передано",
    },
    "zh": {
        "bismillah": "奉至仁至慈的安拉之名",
        "hadith_prefix": "据我们传述",
    },
}


def generate_sample_translations(
    book_slug: str,
    target_langs: List[str],
    count: int = 5,
) -> List[TranslationResult]:
    """Generate sample translations for testing the pipeline.

    Creates representative translations without calling the API.
    In production, use generate_batch_requests() + Batch API instead.
    """
    book_path = f"/books/{book_slug}"
    chapter_paths = walk_book_chapters(book_path)

    results = []
    verse_count = 0

    for chapter_path in chapter_paths:
        verses = extract_verses_from_chapter(chapter_path)
        for verse in verses:
            if verse_count >= count:
                return results

            verse_index = verse["path"][7:]  # strip "/books/"
            for lang in target_langs:
                sample = SAMPLE_TRANSLATIONS.get(lang, {})
                prefix = sample.get("hadith_prefix", "[Translation]")

                # Create a sample translation combining the prefix with
                # a note about the source
                sample_text = f"{prefix}: [{verse.get('chapter_title', 'Unknown')}]"

                results.append(TranslationResult(
                    custom_id=f"{verse_index}__{lang}",
                    verse_path=verse["path"],
                    target_lang=lang,
                    translated_text=sample_text,
                ))

            verse_count += 1

    logger.info("Generated %d sample translations for %d verses", len(results), verse_count)
    return results


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------

def estimate_batch_cost(requests: List[TranslationRequest]) -> dict:
    """Estimate the cost of running a batch translation job.

    Claude Haiku pricing (Batch API, 50% discount):
    - Input: $0.40 / 1M tokens -> $0.20 / 1M tokens with batch
    - Output: $2.00 / 1M tokens -> $1.00 / 1M tokens with batch

    Rough token estimates:
    - Average hadith Arabic text: ~100 tokens
    - Average English translation: ~150 tokens
    - System prompt: ~100 tokens
    - Average output: ~200 tokens
    """
    num_requests = len(requests)

    # Rough token estimates per request
    avg_input_tokens = 350   # system + arabic + english + context
    avg_output_tokens = 200  # translation output

    total_input = num_requests * avg_input_tokens
    total_output = num_requests * avg_output_tokens

    # Batch API pricing (50% of standard)
    input_cost = (total_input / 1_000_000) * 0.20
    output_cost = (total_output / 1_000_000) * 1.00

    return {
        "num_requests": num_requests,
        "estimated_input_tokens": total_input,
        "estimated_output_tokens": total_output,
        "estimated_input_cost_usd": round(input_cost, 2),
        "estimated_output_cost_usd": round(output_cost, 2),
        "estimated_total_cost_usd": round(input_cost + output_cost, 2),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    """CLI entry point for the translation pipeline."""
    if len(sys.argv) < 2:
        print("Usage: python -m app.ai_translation <command> [options]")
        print()
        print("Commands:")
        print("  generate  Generate batch request JSONL file")
        print("  ingest    Ingest batch results into verse data")
        print("  sample    Generate sample translations (no API)")
        print("  estimate  Estimate batch job cost")
        print("  langs     List supported languages")
        sys.exit(1)

    command = sys.argv[1]

    if command == "langs":
        print("Supported languages:")
        for code, config in SUPPORTED_LANGUAGES.items():
            print(f"  {code}: {config.name} ({config.native_name})")
        return

    if command == "estimate":
        book = _get_arg("--book", "al-kafi")
        langs = _get_args("--lang", ["ur"])
        requests = generate_batch_requests(book, langs, max_verses=None)
        cost = estimate_batch_cost(requests)
        print(json.dumps(cost, indent=2))
        return

    if command == "generate":
        book = _get_arg("--book", "al-kafi")
        langs = _get_args("--lang", ["ur"])
        max_v = int(_get_arg("--max", "0")) or None
        output = _get_arg("--output", f"translations/batch_{book}.jsonl")
        requests = generate_batch_requests(book, langs, max_verses=max_v)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        write_batch_file(requests, output)
        cost = estimate_batch_cost(requests)
        print(f"Generated {len(requests)} requests. Estimated cost: ${cost['estimated_total_cost_usd']}")
        return

    if command == "sample":
        book = _get_arg("--book", "al-kafi")
        langs = _get_args("--lang", ["ur"])
        count = int(_get_arg("--count", "5"))
        results = generate_sample_translations(book, langs, count)
        for r in results:
            print(f"{r.custom_id}: {r.translated_text}")
        return

    if command == "ingest":
        input_path = _get_arg("--input", "")
        if not input_path:
            print("Error: --input is required")
            sys.exit(1)
        dry_run = "--dry-run" in sys.argv
        results = parse_batch_results(input_path)
        counters = ingest_translations(results, dry_run=dry_run)
        print(json.dumps(counters, indent=2))
        return

    print(f"Unknown command: {command}")
    sys.exit(1)


def _get_arg(flag: str, default: str) -> str:
    """Get a single CLI argument value."""
    try:
        idx = sys.argv.index(flag)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return default


def _get_args(flag: str, default: List[str]) -> List[str]:
    """Get multiple CLI argument values (all values after flag until next flag)."""
    try:
        idx = sys.argv.index(flag)
        values = []
        for i in range(idx + 1, len(sys.argv)):
            if sys.argv[i].startswith("--"):
                break
            values.append(sys.argv[i])
        return values if values else default
    except ValueError:
        return default


if __name__ == "__main__":
    main()
