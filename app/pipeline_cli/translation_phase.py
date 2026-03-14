"""Phase 4: Multi-language translation using cheap LLM model.

Translates EN chunk translations and metadata to 10 other languages.
Uses GPT-5-mini (or similar cheap model) via OpenAI API.
"""

import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

NON_EN_LANGUAGES = ["ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]

LANGUAGE_NAMES = {
    "ur": "Urdu", "tr": "Turkish", "fa": "Farsi/Persian",
    "id": "Indonesian", "bn": "Bengali", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese (Simplified)",
}


def build_translation_prompt(
    chunks: List[dict],
    en_summary: str,
    en_seo_question: str,
    arabic_text: str = "",
) -> tuple:
    """Build system and user prompts for multi-language translation.

    Args:
        chunks: List of chunk dicts, each with translations.en
        en_summary: English summary from translations.en.summary
        en_seo_question: English SEO question
        arabic_text: Original Arabic text for context

    Returns:
        (system_prompt, user_message) tuple
    """
    system = """You are a professional translator specializing in Islamic religious texts.
Translate the provided English passages to 10 languages: Urdu, Turkish, Farsi, Indonesian, Bengali, Spanish, French, German, Russian, Chinese (Simplified).

RULES:
- Preserve Islamic terminology (salat, wudu, zakat) unless the target language has established equivalents
- Transliterate narrator names — do not translate proper nouns
- Preserve honorifics (peace be upon him, etc.) in each language's convention
- Be faithful — do not add commentary
- For Chinese: do not use spaces between words
- Output valid JSON only"""

    # Build user message with chunks and metadata
    user_parts = []

    if arabic_text:
        user_parts.append(f"Original Arabic (for context, do NOT translate from Arabic — translate from English):\n{arabic_text}\n")

    user_parts.append("Translate the following English texts to 10 languages.\n")
    user_parts.append("Output a JSON object with this structure:")
    user_parts.append("""{
  "chunks": [
    {"translations": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}},
    ...
  ],
  "summary": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."},
  "seo_question": {"ur": "...", "tr": "...", "fa": "...", "id": "...", "bn": "...", "es": "...", "fr": "...", "de": "...", "ru": "...", "zh": "..."}
}""")

    user_parts.append("\n--- Texts to translate ---\n")

    for i, chunk in enumerate(chunks):
        en_text = ""
        if isinstance(chunk.get("translations"), dict):
            en_text = chunk["translations"].get("en", "")
        user_parts.append(f"Chunk {i+1} ({chunk.get('chunk_type', 'body')}): {en_text}")

    user_parts.append(f"\nSummary: {en_summary}")
    user_parts.append(f"SEO Question: {en_seo_question}")

    return system, "\n".join(user_parts)


def merge_translations(result: dict, translation_response: dict) -> dict:
    """Merge Phase 4 translation response into the pipeline result.

    Args:
        result: Current pipeline result dict (has EN-only translations)
        translation_response: Parsed JSON from translation LLM call

    Returns:
        Updated result dict with all 11 languages
    """
    # Merge chunk translations
    chunks_translations = translation_response.get("chunks", [])
    for i, chunk in enumerate(result.get("chunks", [])):
        if i < len(chunks_translations):
            chunk_trans = chunks_translations[i].get("translations", {})
            if "translations" not in chunk:
                chunk["translations"] = {}
            for lang in NON_EN_LANGUAGES:
                if lang in chunk_trans:
                    chunk["translations"][lang] = chunk_trans[lang]

    # Merge verse-level translations (summary, seo_question)
    summary_trans = translation_response.get("summary", {})
    seo_trans = translation_response.get("seo_question", {})

    translations = result.get("translations", {})
    for lang in NON_EN_LANGUAGES:
        if lang not in translations:
            translations[lang] = {}
        if lang in summary_trans:
            translations[lang]["summary"] = summary_trans[lang]
        if lang in seo_trans:
            translations[lang]["seo_question"] = seo_trans[lang]
        # key_terms for non-EN languages: empty dict (Phase 2 handles EN key_terms)
        if "key_terms" not in translations[lang]:
            translations[lang]["key_terms"] = {}

    result["translations"] = translations
    return result


def _fill_empty_translations(result: dict) -> None:
    """Fill all non-EN languages with empty strings as fallback.

    Mutates result in-place. Used when the translation call fails
    or the response cannot be parsed, so downstream code always sees
    all 11 languages present.
    """
    translations = result.get("translations", {})
    for lang in NON_EN_LANGUAGES:
        if lang not in translations:
            translations[lang] = {"summary": "", "seo_question": "", "key_terms": {}}
        else:
            translations[lang].setdefault("summary", "")
            translations[lang].setdefault("seo_question", "")
            translations[lang].setdefault("key_terms", {})
        for chunk in result.get("chunks", []):
            if "translations" not in chunk:
                chunk["translations"] = {}
            if lang not in chunk["translations"]:
                chunk["translations"][lang] = ""
    result["translations"] = translations


def _strip_code_fences(raw: str) -> str:
    """Strip markdown code fences from LLM output."""
    raw = raw.strip()
    if raw.startswith("```"):
        first_nl = raw.index("\n") if "\n" in raw else 3
        raw = raw[first_nl + 1:]
        if raw.rstrip().endswith("```"):
            raw = raw.rstrip()[:-3].rstrip()
    return raw


async def translate_chunks(
    result: dict,
    model: str = "gpt-5-mini",
    arabic_text: str = "",
) -> dict:
    """Run Phase 4 translation on a pipeline result.

    Calls the OpenAI API to translate EN chunk translations to 10 languages.
    Merges the response into the result dict.

    Args:
        result: Pipeline result with EN-only translations
        model: OpenAI model to use for translation
        arabic_text: Original Arabic text for context

    Returns:
        Updated result dict with all 11 languages, plus cost metadata
    """
    from app.pipeline_cli.openai_backend import call_openai

    chunks = result.get("chunks", [])
    en_trans = result.get("translations", {}).get("en", {})
    en_summary = en_trans.get("summary", "")
    en_seo = en_trans.get("seo_question", "")

    system, user = build_translation_prompt(chunks, en_summary, en_seo, arabic_text)

    cr = await call_openai(system, user, model=model)

    if "error" in cr:
        logger.error("Phase 4 translation failed: %s", cr["error"])
        _fill_empty_translations(result)
        return result

    # Parse translation response
    try:
        raw = _strip_code_fences(cr.get("result", ""))
        trans_data = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error("Phase 4 translation JSON parse failed: %s", e)
        _fill_empty_translations(result)
        return result

    result = merge_translations(result, trans_data)

    # Attach cost metadata
    result["_phase4_cost"] = cr.get("cost", 0)
    result["_phase4_tokens"] = cr.get("output_tokens", 0)

    return result
