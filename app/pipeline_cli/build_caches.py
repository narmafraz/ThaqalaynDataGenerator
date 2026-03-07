"""Build word translation dictionary and narrator templates from existing AI responses.

Scans all response JSON files and extracts:
1. Word translation cache: high-frequency words with consistent translations
2. Narrator templates: common narrator profiles for consistent transliteration

Usage:
    cd ThaqalaynDataGenerator
    source .venv/Scripts/activate
    PYTHONPATH="$PWD:$PWD/app" SOURCE_DATA_DIR="../ThaqalaynDataSources/" python -m app.pipeline_cli.build_caches
"""

import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.config import AI_CONTENT_DIR, AI_PIPELINE_DATA_DIR

LANGUAGE_KEYS = ["en", "ur", "tr", "fa", "id", "bn", "es", "fr", "de", "ru", "zh"]

# Word dictionary thresholds
MIN_WORD_OCCURRENCES = 10
MIN_CONSISTENCY = 0.90  # 90% of occurrences must agree on translation

# Narrator template thresholds
MIN_NARRATOR_OCCURRENCES = 3


def scan_response_files(responses_dir: Optional[str] = None) -> List[dict]:
    """Load all response JSON files from the responses directory.

    Scans both samples/ and corpus/ subdirectories.
    """
    if responses_dir is None:
        responses_dir = AI_CONTENT_DIR

    results = []
    for subdir in ["samples/responses", "corpus/responses"]:
        dir_path = os.path.join(responses_dir, subdir)
        if not os.path.isdir(dir_path):
            continue
        for fname in os.listdir(dir_path):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(dir_path, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if "result" in data and "word_analysis" in data["result"]:
                    results.append(data)
            except (json.JSONDecodeError, KeyError, OSError):
                continue
    return results


def build_word_dictionary(responses: List[dict]) -> dict:
    """Build word translation cache from response files.

    For each (word, pos) pair, counts how many times each translation
    appears per language. If a translation achieves >= MIN_CONSISTENCY
    agreement across >= MIN_WORD_OCCURRENCES, it becomes the canonical entry.

    Returns dict ready to write as JSON.
    """
    # word_key -> lang -> Counter(translation -> count)
    word_stats: Dict[str, Dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    # word_key -> total count
    word_counts: Counter = Counter()

    for resp in responses:
        word_analysis = resp["result"]["word_analysis"]
        for entry in word_analysis:
            word = entry.get("word", "")
            pos = entry.get("pos", "")
            if not word or not pos:
                continue

            key = f"{word}|{pos}"
            word_counts[key] += 1

            translations = entry.get("translation", {})
            for lang in LANGUAGE_KEYS:
                t = translations.get(lang, "")
                if t:
                    word_stats[key][lang][t] += 1

    # Build dictionary entries for qualifying words
    words = {}
    for key, count in word_counts.items():
        if count < MIN_WORD_OCCURRENCES:
            continue

        word_part, pos_part = key.rsplit("|", 1)
        translations = {}
        all_consistent = True

        for lang in LANGUAGE_KEYS:
            lang_counter = word_stats[key].get(lang, Counter())
            if not lang_counter:
                all_consistent = False
                continue
            most_common_val, most_common_count = lang_counter.most_common(1)[0]
            total = sum(lang_counter.values())
            consistency = most_common_count / total
            if consistency >= MIN_CONSISTENCY:
                translations[lang] = most_common_val
            else:
                all_consistent = False

        # Only include if at least English is consistent
        if "en" in translations:
            words[key] = {
                "word": word_part,
                "pos": pos_part,
                "occurrences": count,
                "translations": translations,
                "all_consistent": all_consistent,
            }

    return {
        "version": "1.0.0",
        "built_from": len(responses),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "min_occurrences": MIN_WORD_OCCURRENCES,
        "min_consistency": MIN_CONSISTENCY,
        "total_unique_words": len(word_counts),
        "dictionary_entries": len(words),
        "words": dict(sorted(words.items(), key=lambda x: -x[1]["occurrences"])),
    }


def build_narrator_templates(responses: List[dict]) -> dict:
    """Build narrator profile templates from response files.

    Groups narrators by Arabic name, picks the most common English
    transliteration, role, and identity info.

    Returns dict ready to write as JSON.
    """
    # name_ar -> list of narrator dicts
    narrator_data: Dict[str, List[dict]] = defaultdict(list)

    for resp in responses:
        result = resp["result"]
        isnad_matn = result.get("isnad_matn", {})
        narrators = isnad_matn.get("narrators", [])
        for n in narrators:
            name_ar = n.get("name_ar", "").strip()
            if not name_ar:
                continue
            narrator_data[name_ar].append(n)

    # Build templates for narrators appearing enough times
    templates = {}
    for name_ar, entries in narrator_data.items():
        if len(entries) < MIN_NARRATOR_OCCURRENCES:
            continue

        # Most common English name
        en_names = Counter(e.get("name_en", "") for e in entries if e.get("name_en"))
        if not en_names:
            continue
        name_en = en_names.most_common(1)[0][0]

        # Most common role
        roles = Counter(e.get("role", "") for e in entries if e.get("role"))
        role = roles.most_common(1)[0][0] if roles else "narrator"

        # Most common confidence
        confs = Counter(e.get("identity_confidence", "") for e in entries if e.get("identity_confidence"))
        confidence = confs.most_common(1)[0][0] if confs else "likely"

        # Most common known_identity (excluding None/null)
        identities = Counter(
            e.get("known_identity", "") for e in entries
            if e.get("known_identity")
        )
        known_identity = identities.most_common(1)[0][0] if identities else None

        templates[name_ar] = {
            "name_en": name_en,
            "role": role,
            "identity_confidence": confidence,
            "known_identity": known_identity,
            "occurrences": len(entries),
            "en_name_variants": dict(en_names.most_common(5)),
        }

    return {
        "version": "1.0.0",
        "built_from": len(responses),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "min_occurrences": MIN_NARRATOR_OCCURRENCES,
        "total_unique_narrators": len(narrator_data),
        "template_entries": len(templates),
        "narrators": dict(sorted(templates.items(), key=lambda x: -x[1]["occurrences"])),
    }


def print_summary(word_dict: dict, narrator_templates: dict) -> None:
    """Print summary statistics."""
    print(f"Responses scanned: {word_dict['built_from']}")
    print()
    print(f"Word Dictionary:")
    print(f"  Unique word|POS pairs: {word_dict['total_unique_words']}")
    print(f"  Dictionary entries (>={MIN_WORD_OCCURRENCES} occurrences, >={MIN_CONSISTENCY*100:.0f}% consistent): {word_dict['dictionary_entries']}")
    if word_dict["words"]:
        top5 = list(word_dict["words"].items())[:5]
        print(f"  Top 5 by frequency:")
        for key, val in top5:
            print(f"    {key}: {val['occurrences']}x -> en=\"{val['translations'].get('en', '?')}\"")
    print()
    print(f"Narrator Templates:")
    print(f"  Unique narrators: {narrator_templates['total_unique_narrators']}")
    print(f"  Template entries (>={MIN_NARRATOR_OCCURRENCES} occurrences): {narrator_templates['template_entries']}")
    if narrator_templates["narrators"]:
        top5 = list(narrator_templates["narrators"].items())[:5]
        print(f"  Top 5 by frequency:")
        for name_ar, val in top5:
            print(f"    {name_ar} -> {val['name_en']} ({val['role']}, {val['occurrences']}x)")


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    print("Scanning response files...")
    responses = scan_response_files()
    print(f"Found {len(responses)} response files with word_analysis.")
    print()

    if not responses:
        print("No responses found. Check AI_CONTENT_DIR and response file paths.")
        return

    print("Building word dictionary...")
    word_dict = build_word_dictionary(responses)

    print("Building narrator templates...")
    narrator_templates = build_narrator_templates(responses)

    print()
    print_summary(word_dict, narrator_templates)

    # Write outputs
    os.makedirs(AI_PIPELINE_DATA_DIR, exist_ok=True)

    word_dict_path = os.path.join(AI_PIPELINE_DATA_DIR, "word_translations_cache.json")
    with open(word_dict_path, "w", encoding="utf-8") as f:
        json.dump(word_dict, f, ensure_ascii=False, indent=2)
    print(f"\nWord dictionary written to: {word_dict_path}")

    narrator_path = os.path.join(AI_PIPELINE_DATA_DIR, "narrator_templates.json")
    with open(narrator_path, "w", encoding="utf-8") as f:
        json.dump(narrator_templates, f, ensure_ascii=False, indent=2)
    print(f"Narrator templates written to: {narrator_path}")


if __name__ == "__main__":
    main()
