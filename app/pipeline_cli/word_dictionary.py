"""Word dictionary management for v4 pipeline. [DORMANT INFRASTRUCTURE]

Extracts unique (word, POS) pairs from corpus responses, translates them
once via LLM, and assembles full word_analysis from word_tags + dictionary.

This eliminates per-hadith word translation — the biggest cost driver in v3.

STATUS (2026-05): the corpus translation step (step 3 in V4_PIPELINE_PLAN.md
Phase 7) was never executed because Phase 1 prompt was simplified to drop
word_tags emission for cost reasons. As a result:

  - extract_unique_words(): WORKS — but Phase 2's reconstructed word_tags
    all carry placeholder POS "N", so the resulting (word|N) keys lose the
    discrimination that real POS would provide. Pre-#5 corpus data with
    real POS in word_tags still yields good (word|POS) keys.
  - load_v4_dictionary(): WORKS — but word_translations_dict_v4.json
    doesn't exist on disk yet so it returns {}.
  - assemble_word_analysis(): WORKS — but is only invoked from tests
    (production never assembles word_analysis for v4 verses; Angular
    has no v4 word-by-word UI as a result).
  - find_missing_words(), build_translation_prompt(),
    save_v4_dictionary(): WORK — these would be the resurrection path:
    extract → find_missing → build_prompt → call LLM → save dict.

To resurrect (planned for a future session):

  1. Build the corpus dictionary by translating every (word, POS) once
     via LLM batch. Estimate ~$60-400 depending on the model.
  2. Wire `assemble_word_analysis(word_tags, dict)` into the merger so
     v4 responses get per-word translations attached at merge time
     (zero per-verse cost).
  3. Update Angular to expose word-by-word hover translations for v4
     (the v3 UI already does this via word_analysis).
  4. Optionally re-enable enrich_key_terms gap-fill in
     programmatic_enrichment.py (see its docstring for caveats).

Currently this module is preserved as the contract for that work.
The CLI `python -m app.pipeline_cli.pipeline word-dict ...`
subcommands still wire into these functions for ad-hoc dictionary
building.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

LANGUAGES = ["en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]
DICT_FILENAME = "word_translations_dict_v4.json"


def extract_unique_words(responses_dir: str) -> Dict[str, int]:
    """Scan all responses, collect unique 'word|POS' keys with frequency.

    Source priority (highest to lowest):
    1. word_tags (legacy v4 wrappers persisted before #5)
    2. word_analysis (v3)
    3. chunks[].arabic_text (current v4 — chunks are the LLM canonical;
       POS becomes a placeholder "N" since chunks don't carry per-word POS)

    Args:
        responses_dir: Path to directory containing response JSON files.

    Returns:
        Dict mapping 'word|POS' -> occurrence count, sorted by frequency desc.
    """
    counts: Dict[str, int] = {}

    for fname in os.listdir(responses_dir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(responses_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        result = data.get("result", {})

        # Legacy v4 format: word_tags
        word_tags = result.get("word_tags", [])
        if word_tags:
            for entry in word_tags:
                if isinstance(entry, list) and len(entry) >= 2:
                    key = f"{entry[0]}|{entry[1]}"
                    counts[key] = counts.get(key, 0) + 1
            continue

        # v3 format: word_analysis
        word_analysis = result.get("word_analysis", [])
        if word_analysis:
            for entry in word_analysis:
                if isinstance(entry, dict):
                    word = entry.get("word", "")
                    pos = entry.get("pos", "")
                    if word and pos:
                        key = f"{word}|{pos}"
                        counts[key] = counts.get(key, 0) + 1
            continue

        # New v4 format: source from chunks[].arabic_text. POS is unknown
        # at the chunk level — use the same "N" placeholder Phase 2 used
        # for word_tags so existing dictionary keys remain consistent.
        chunks = result.get("chunks", [])
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            text = chunk.get("arabic_text") or ""
            for word in text.split():
                if word:
                    key = f"{word}|N"
                    counts[key] = counts.get(key, 0) + 1

    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def load_v4_dictionary(path: Optional[str] = None) -> Dict[str, dict]:
    """Load the v4 word dictionary.

    Args:
        path: Path to dictionary JSON file. If None, uses default location.

    Returns:
        Dict mapping 'word|POS' -> {'en': ..., 'ur': ..., ...}
    """
    if path is None:
        from app.config import AI_PIPELINE_DATA_DIR
        path = os.path.join(AI_PIPELINE_DATA_DIR, DICT_FILENAME)

    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("words", {})


def save_v4_dictionary(words: Dict[str, dict], path: Optional[str] = None) -> None:
    """Save the v4 word dictionary.

    Args:
        words: Dict mapping 'word|POS' -> {'en': ..., 'ur': ..., ...}
        path: Path to dictionary JSON file. If None, uses default location.
    """
    if path is None:
        from app.config import AI_PIPELINE_DATA_DIR
        path = os.path.join(AI_PIPELINE_DATA_DIR, DICT_FILENAME)

    data = {
        "version": "4.0.0",
        "total_entries": len(words),
        "words": words,
    }

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info("Saved %d word entries to %s", len(words), path)


def assemble_word_analysis(word_tags: list, dictionary: dict) -> list:
    """Convert word_tags + dictionary into full word_analysis format.

    For each [word, POS] pair, looks up translations in dictionary.
    Missing entries get '???' placeholder translations.

    Args:
        word_tags: List of [word, POS] pairs.
        dictionary: Dict mapping 'word|POS' -> {'en': ..., 'ur': ..., ...}

    Returns:
        List of standard word_analysis dicts with word, pos, translation.
    """
    placeholder = {lang: "???" for lang in LANGUAGES}
    result = []

    for entry in word_tags:
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        word, pos = entry[0], entry[1]
        key = f"{word}|{pos}"
        translations = dictionary.get(key, placeholder)
        result.append({
            "word": word,
            "pos": pos,
            "translation": translations,
        })

    return result


def find_missing_words(
    responses_dir: str, dictionary: dict
) -> List[Tuple[str, str, int]]:
    """Find (word, POS) pairs in corpus not yet in dictionary.

    Args:
        responses_dir: Path to response files.
        dictionary: Current dictionary (word|POS -> translations).

    Returns:
        List of (word, POS, count) sorted by frequency descending.
    """
    corpus_words = extract_unique_words(responses_dir)
    missing = []
    for key, count in corpus_words.items():
        if key not in dictionary:
            parts = key.split("|", 1)
            if len(parts) == 2:
                missing.append((parts[0], parts[1], count))
    return sorted(missing, key=lambda x: -x[2])


def build_translation_prompt(words: List[Tuple[str, str]], languages: Optional[List[str]] = None) -> str:
    """Build a prompt to translate a batch of (word, POS) pairs.

    Args:
        words: List of (word, POS) tuples to translate.
        languages: Target languages. Defaults to all 11.

    Returns:
        Prompt string for LLM word translation.
    """
    if languages is None:
        languages = LANGUAGES

    lang_str = ", ".join(languages)
    word_list = "\n".join(f"- {word} ({pos})" for word, pos in words)

    return f"""Translate each Arabic word below into these languages: {lang_str}

Context: These are words from classical Islamic hadith texts (Shia tradition).
Use hadith-appropriate translations. For proper nouns, transliterate.
For particles/prepositions, give the most common meaning in hadith context.

Words:
{word_list}

Output a JSON object where each key is "word|POS" and each value is an object
with language keys mapping to translations. Example:
{{
  "قَالَ|V": {{"en": "he said", "ur": "کہا", "tr": "dedi", "fa": "گفت", ...}},
  "عَنْ|PREP": {{"en": "from/about", "ur": "سے", "tr": "-den/-dan", "fa": "از", ...}}
}}

Output ONLY valid JSON, no explanation."""
